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
    monkeypatch.setenv("SOLAMI_API_KEY", "TESTKEY"); monkeypatch.setenv("DBC_PROVIDER", "solami")
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
    assert h["reserve_agreement"] == 1.0 and h["agree_n"] == 1 and h["liquidity_remove"] == 1 and 400 <= h["lag_ms_avg"] <= 1100
    tap.handle({"type": "swap", "pool": "POOL1", "quote_reserve": 2_000_000_000, "block_time": now}, now=now)   # 50% off → disagreement
    assert tap.health()["reserve_agreement"] == 0.5
