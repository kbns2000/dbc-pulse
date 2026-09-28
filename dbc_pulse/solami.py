"""Solami integration for dbc-pulse: RPC/WebSocket provider switch + Blur decoded-market-data tap.

Why Blur next to our own decoder
  dbc-pulse decodes the DBC program itself (IDL event-CPI + account polling). Blur (Solami's decoded market data)
  independently decodes swaps, liquidity adds/removes, pool creations, graduations and launchpad curve progress.
  Subscribing Blur to exactly the pools dbc-pulse tracks gives:
    - liquidity movement in/out per DAMM v2 pool with the provider wallet (not visible from pool state alone)
    - per-trade fees and trader wallets on migrated pools without per-transaction fetches
    - a cross-check: Blur's reported quote reserve vs our polled reserve for the same pool (decoder agreement)
    - stream health: event lag (receive time − block_time) and events per minute

Environment (dbc_pulse/.env, never committed)
  SOLAMI_API_KEY   your Solami key (needs DataApi for Blur; any standard key for RPC/WS)
  DBC_PROVIDER     "solami" to use Solami RPC + WebSocket for the whole stream (default: SOLANA_RPC_URL)
  DBC_BLUR         "1" to run the Blur tap inside the stream (default on when SOLAMI_API_KEY is set)
Endpoints (docs: solami.dev/docs/endpoints)
  RPC   https://rpc.solami.dev/sol?api_key=KEY
  WS    wss://ws.solami.dev/ws/sol?api_key=KEY
  Blur  wss://ws.solami.dev/data/subscribe?chain=solana&api_key=KEY
"""
from __future__ import annotations
import asyncio, json, os, time
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"
BLUR_TYPES = ["swap", "liquidity", "pool_create", "graduation", "meme"]


def solami_key() -> str | None:
    k = (os.environ.get("SOLAMI_API_KEY") or "").strip()
    return k or None


def solami_urls(key: str) -> dict:
    return {"rpc": f"https://rpc.solami.dev/sol?api_key={key}", "ws": f"wss://ws.solami.dev/ws/sol?api_key={key}",
            "blur": f"wss://ws.solami.dev/data/subscribe?chain=solana&api_key={key}"}


def provider_urls() -> tuple[str, str, str]:
    """(rpc_url, ws_url, provider_name). Solami when DBC_PROVIDER=solami and a key is set, else SOLANA_RPC_URL."""
    key = solami_key()
    if key and (os.environ.get("DBC_PROVIDER", "").lower() == "solami"):
        u = solami_urls(key); return u["rpc"], u["ws"], "solami"
    url = os.environ.get("SOLANA_RPC_URL") or ""
    return url, url.replace("https://", "wss://", 1), "custom"


def _f(x):
    try: return float(x)
    except (TypeError, ValueError): return None


class BlurTap:
    """Streams Blur events for the pools dbc-pulse tracks and keeps simple health/agreement counters.

    pools_fn() -> iterable of pool addresses to follow (DBC virtual pools + DAMM v2 pools). The filter is refreshed
    every `refresh_s` by sending a text frame, so newly migrated pools are followed without reconnecting.
    reserve_fn(pool) -> our latest quote reserve for that pool (raw units) or None, for the agreement check.
    """

    def __init__(self, key: str, pools_fn, reserve_fn=None, refresh_s: float = 60.0):
        self.url = solami_urls(key)["blur"]; self.pools_fn = pools_fn; self.reserve_fn = reserve_fn; self.refresh_s = refresh_s
        self.stats = {"events": 0, "by_type": {}, "dex_seen": {}, "lag_ms_sum": 0.0, "lag_n": 0, "agree_n": 0, "agree_ok": 0,
                      "liq_add": 0, "liq_remove": 0, "reconnects": 0, "last_event_ts": None, "filter_pools": 0}

    def _filter(self) -> dict:
        pools = sorted({p for p in (self.pools_fn() or []) if p})[:5000]
        self.stats["filter_pools"] = len(pools)
        return {"filter": {"types": BLUR_TYPES, "pools": pools}}

    def handle(self, msg: dict, now: float | None = None) -> dict | None:
        """Update counters for one decoded Blur event; returns the compact row written to disk (or None to skip)."""
        now = now or time.time(); t = msg.get("type")
        if not t or t == "metadata": return None
        s = self.stats; s["events"] += 1; s["by_type"][t] = s["by_type"].get(t, 0) + 1; s["last_event_ts"] = now
        dex = msg.get("dex") or msg.get("launchpad")
        if dex: s["dex_seen"][dex] = s["dex_seen"].get(dex, 0) + 1
        bt = msg.get("block_time")
        if isinstance(bt, (int, float)) and bt > 0:
            s["lag_ms_sum"] += max(0.0, now - bt) * 1000; s["lag_n"] += 1
        if t == "liquidity":
            k = (msg.get("kind") or "").lower()
            if k == "add": s["liq_add"] += 1
            elif k == "remove": s["liq_remove"] += 1
        if t == "swap" and self.reserve_fn and msg.get("pool"):
            ours = self.reserve_fn(msg["pool"]); theirs = msg.get("quote_reserve")
            if ours and isinstance(theirs, (int, float)) and theirs > 0:
                s["agree_n"] += 1
                if abs(ours - theirs) / theirs <= 0.02: s["agree_ok"] += 1   # within 2%: same state (poll lag allowed)
        keep = ("type", "signature", "slot", "block_time", "dex", "launchpad", "pool", "mint", "quote_mint", "trader", "side", "kind",
                "base_amount", "quote_amount", "base_reserve", "quote_reserve", "fee_amount", "fee_mint", "price", "volume_usd", "progress_pct", "graduated")
        row = {k: msg[k] for k in keep if k in msg}; row["recv"] = round(now, 3)
        return row

    def health(self) -> dict:
        s = self.stats
        return dict(events=s["events"], by_type=s["by_type"], dex_seen=s["dex_seen"], filter_pools=s["filter_pools"],
                    lag_ms_avg=round(s["lag_ms_sum"] / s["lag_n"], 1) if s["lag_n"] else None,
                    reserve_agreement=round(s["agree_ok"] / s["agree_n"], 4) if s["agree_n"] else None, agree_n=s["agree_n"],
                    liquidity_add=s["liq_add"], liquidity_remove=s["liq_remove"], reconnects=s["reconnects"], last_event_ts=s["last_event_ts"])

    async def run(self):
        import websockets
        while True:
            try:
                async with websockets.connect(self.url, max_size=2 ** 23, ping_interval=20) as ws:
                    await ws.send(json.dumps(self._filter())); last_refresh = time.time()
                    async for raw in ws:
                        try: msg = json.loads(raw)
                        except Exception: continue
                        row = self.handle(msg)
                        if row:
                            with (DATA / f"blur_{time.strftime('%Y%m%d', time.gmtime())}.jsonl").open("a", encoding="utf-8") as f:
                                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                        if time.time() - last_refresh > self.refresh_s:
                            await ws.send(json.dumps(self._filter())); last_refresh = time.time()
                            try: (DATA / "blur_health.json").write_text(json.dumps(dict(ts=time.time(), **self.health()), ensure_ascii=False), encoding="utf-8")
                            except Exception: pass
            except Exception as e:
                self.stats["reconnects"] += 1; print(f"blur error {type(e).__name__}: {e}", flush=True); await asyncio.sleep(5)
