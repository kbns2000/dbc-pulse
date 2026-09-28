"""Solami integration: provider switch and Blur event handling (no network)."""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dbc_pulse.solami import provider_urls, solami_urls, BlurTap


def test_provider_switch(monkeypatch):
    monkeypatch.setenv("SOLANA_RPC_URL", "https://example-rpc.invalid/?k=1")
    monkeypatch.delenv("SOLAMI_API_KEY", raising=False); monkeypatch.delenv("DBC_PROVIDER", raising=False)
    rpc, ws, name = provider_urls()
    assert name == "custom" and rpc.startswith("https://example-rpc") and ws.startswith("wss://example-rpc")
    monkeypatch.setenv("SOLAMI_API_KEY", "TESTKEY"); monkeypatch.setenv("DBC_PROVIDER", "solami"); monkeypatch.setenv("DBC_SOLAMI_UNTIL", "2099-01-01T00:00:00+00:00")
    rpc, ws, name = provider_urls()
    assert name == "solami" and rpc == "https://rpc.solami.dev/sol?api_key=TESTKEY" and ws == "wss://ws.solami.dev/ws/sol?api_key=TESTKEY"
    assert solami_urls("K")["blur"] == "wss://ws.solami.dev/data/subscribe?chain=solana&api_key=K"


def test_blur_handle_counts_and_agreement():
    ours = {"POOL1": 1_000_000_000}
    tap = BlurTap("K", lambda: ["POOL1", "POOL2", None], reserve_fn=lambda p: ours.get(p))
    f = tap._filter(); assert f["filter"]["types"] and f["filter"]["pools"] == ["POOL1", "POOL2"] and tap.stats["filter_pools"] == 2
    now = 1_000_000.0
    swap = {"type": "swap", "signature": "s", "slot": 1, "block_time": now - 0.5, "dex": "meteora-dbc", "pool": "POOL1", "quote_reserve": 1_010_000_000, "side": "buy", "fee_amount": 10}
    row = tap.handle(swap, now=now)
    assert row["pool"] == "POOL1" and row["recv"] == now
    tap.handle({"type": "liquidity", "kind": "remove", "pool": "POOL2", "block_time": now - 1}, now=now)
    tap.handle({"type": "metadata", "mint": "M"}, now=now)            # ignored
    h = tap.health()
    assert h["events"] == 2 and h["by_type"] == {"swap": 1, "liquidity": 1} and h["dex_seen"] == {"meteora-dbc": 1}
    assert h["agree_n"] == 0 and h["liquidity_remove"] == 1 and 400 <= h["lag_ms_avg"] <= 1100   # not yet quiet: no comparison
    tap._settle(now + 10); assert tap.health()["reserve_agreement"] == 1.0 and tap.health()["agree_n"] == 1
    tap.handle({"type": "swap", "pool": "POOL1", "quote_reserve": 2_000_000_000, "block_time": now + 20}, now=now + 20)   # 50% off
    tap._settle(now + 30); assert tap.health()["reserve_agreement"] == 0.5


def test_blur_keeps_provider_wallet_and_exact_agreement():
    ours = {"P": 589_751_510}   # reserve + unclaimed fees = vault balance (Blur's definition)
    tap = BlurTap("K", lambda: ["P"], reserve_fn=lambda p: ours.get(p)); now = 1_000_000.0
    liq = {"type": "liquidity", "kind": "add", "pool": "P", "provider": "WALLET1", "quote_usd": 12.5, "indexed_at": now - 0.2, "block_time": now - 1}
    row = tap.handle(liq, now=now)
    assert row["provider"] == "WALLET1" and row["quote_usd"] == 12.5 and row["indexed_at"] == now - 0.2
    tap.handle({"type": "swap", "pool": "P", "quote_reserve": 589_751_510, "block_time": now}, now=now)
    tap._settle(now + 9); h = tap.health(); assert h["agree_exact"] == 1 and h["reserve_agreement"] == 1.0


def test_quiet_window_needs_later_poll():
    polls = {"P": (100, 1_000.0)}                     # our last poll BEFORE the Blur swap
    tap = BlurTap("K", lambda: ["P"], reserve_fn=lambda p: polls.get(p))
    tap.handle({"type": "swap", "pool": "P", "quote_reserve": 105, "block_time": 1_005.0}, now=1_005.5)
    tap.handle({"type": "swap", "pool": "P", "quote_reserve": 110, "block_time": 1_006.0}, now=1_006.5)   # replaces the older one
    tap._settle(1_020.0); assert tap.health()["agree_n"] == 0            # stale poll: wait
    polls["P"] = (110, 1_016.0); tap._settle(1_021.0)
    h = tap.health(); assert h["agree_n"] == 1 and h["agree_exact"] == 1


def test_poll_keys_cover_busy_and_rotate():
    from dbc_pulse.stream import Stream
    st = Stream.__new__(Stream)
    class U: pass
    st.uni = U(); st.uni.pools = {f"P{i}": {"first_seen": i, "last_activity": None} for i in range(1500)}
    st.uni.pools["P1499"]["last_activity"] = 10_000   # newest + busiest pool, last in insertion order
    seen = set()
    for _ in range(4):
        ks = st._poll_keys(); assert len(ks) == 1000 and "P1499" in ks; seen |= set(ks)
    assert len(seen) == 1500   # every pool read within a few cycles


def test_orientation_mismatch_not_compared():
    tap = BlurTap("K", lambda: ["P"], reserve_fn=lambda p: (283_172_114_466, 2_000.0, "SOLMINT"))
    tap.handle({"type": "swap", "pool": "P", "quote_reserve": 968_973_530_653_117_210, "quote_mint": "TOKENMINT", "block_time": 1_000.0}, now=1_000.5)
    tap._settle(1_020.0); h = tap.health()
    assert h["agree_n"] == 0 and h["agree_orient_skip"] == 1


def test_slot_order_required_when_known():
    polls = {"P": (110, 5_000.0, None, 99)}          # poll slot 99 < swap slot 100: stale even though wall-clock is later
    tap = BlurTap("K", lambda: ["P"], reserve_fn=lambda p: polls.get(p))
    tap.handle({"type": "swap", "pool": "P", "quote_reserve": 110, "slot": 100, "block_time": 1_000.0}, now=1_000.5)
    tap._settle(1_020.0); assert tap.health()["agree_n"] == 0
    polls["P"] = (110, 5_001.0, None, 100); tap._settle(1_021.0)
    h = tap.health(); assert h["agree_n"] == 1 and h["agree_exact"] == 1


def test_solami_window_falls_back(monkeypatch):
    monkeypatch.setenv("SOLANA_RPC_URL", "https://example-rpc.invalid/?k=1"); monkeypatch.setenv("SOLAMI_API_KEY", "TESTKEY"); monkeypatch.setenv("DBC_PROVIDER", "solami")
    monkeypatch.setenv("DBC_SOLAMI_UNTIL", "2000-01-01T00:00:00+00:00")
    assert provider_urls()[2] == "custom"
