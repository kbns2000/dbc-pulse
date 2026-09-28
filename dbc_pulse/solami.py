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
    QUIET_S = 8.0
    """Streams Blur events for the pools dbc-pulse tracks and keeps simple health/agreement counters.

    pools_fn() -> iterable of pool addresses dbc-pulse tracks (DBC virtual pools + DAMM v2 pools). The server-side filter is
    the connect URL (types + the two Meteora venues); the tracked set, refreshed every `refresh_s`, decides which swaps are written.
    reserve_fn(pool) -> our latest quote reserve for that pool (raw units) or None, for the agreement check. Blur reports the quote
    VAULT balance for DBC curves = VirtualPool.quote_reserve + unclaimed protocol/partner/creator quote fees (measured 2026-09-28 on
    26 live pools: 14 exact matches on the vault definition, 0 on the bare reserve), so reserve_fn must return reserve + unclaimed fees.
    """

    def __init__(self, key: str, pools_fn, reserve_fn=None, refresh_s: float = 60.0):
        # Server-side filter goes in the connect URL (measured 2026-09-28: a 3,334-pool text-frame filter was ignored and the
        # whole firehose arrived). Only the two Meteora venues: meteora_dbc (curves) and meteora_damm2 (graduated pools).
        # Swaps are written to disk only for pools dbc-pulse tracks; everything else is counted.
        self.url = solami_urls(key)["blur"] + "&type=" + ",".join(BLUR_TYPES) + "&dex=meteora_dbc,meteora_damm2&metadata=false"
        self.pools_fn = pools_fn; self.reserve_fn = reserve_fn; self.refresh_s = refresh_s; self._tracked: set = set()
        self.activity_fn = None   # optional callback(pool) on every swap: lets the stream poll the pools that are actually trading
        self._pending: dict = {}; self._last_settle = 0.0
        self.stats = {"events": 0, "by_type": {}, "dex_seen": {}, "lag_ms_sum": 0.0, "lag_n": 0, "agree_n": 0, "agree_ok": 0,
                      "liq_add": 0, "liq_remove": 0, "reconnects": 0, "last_event_ts": None, "filter_pools": 0, "swaps_total": 0, "swaps_tracked": 0}

    def _filter(self) -> dict:
        pools = sorted({p for p in (self.pools_fn() or []) if p})[:5000]
        self.stats["filter_pools"] = len(pools)
        return {"filter": {"types": BLUR_TYPES, "pools": pools}}

    def handle(self, msg: dict, now: float | None = None) -> dict | None:
        """Update counters for one decoded Blur event; returns the compact row written to disk (or None to skip)."""
        now = now or time.time(); t = msg.get("type")
        if not t or t in ("metadata", "connected", "replay_end", "pong"): return None
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
            theirs = msg.get("quote_reserve")
            if isinstance(theirs, (int, float)) and theirs > 0:
                self._pending[msg["pool"]] = (theirs, bt if isinstance(bt, (int, float)) and bt > 0 else now)   # newer swap replaces older
        if now - self._last_settle >= 2.0: self._settle(now)
        # "provider" = the LP wallet on liquidity events (measured 2026-09-28: raw liquidity messages carry provider, base/quote_usd,
        # decimals and indexed_at; an earlier keep-list dropped provider, so rows before this fix have no wallet).
        keep = ("type", "signature", "slot", "block_time", "indexed_at", "dex", "launchpad", "pool", "mint", "base_mint", "quote_mint", "trader", "provider",
                "side", "kind", "base_amount", "quote_amount", "base_decimals", "quote_decimals", "base_usd", "quote_usd", "base_reserve", "quote_reserve",
                "fee_amount", "fee_mint", "price", "volume_usd", "progress_pct", "graduated")
        row = {k: msg[k] for k in keep if k in msg}; row["recv"] = round(now, 3)
        return row

    def _settle(self, now: float) -> None:
        """Quiet-window agreement. Comparing Blur's post-trade reserve with a poll taken seconds earlier fails on busy curves
        (measured 2026-09-28: one curve traded ~7 times/s before graduating; instant comparison agreed 17.7% of 186). So a pool is
        compared only after QUIET_S with no newer Blur swap AND once our poll is later than that swap: both sides then describe the
        same account state and should match exactly."""
        self._last_settle = now; s = self.stats
        for pool, (theirs, bt) in list(self._pending.items()):
            if now - bt < self.QUIET_S: continue
            got = self.reserve_fn(pool)
            ours, polled = got if isinstance(got, tuple) else (got, now)
            if ours is None: self._pending.pop(pool, None); continue
            if polled is None or polled < bt + 1.0:
                if now - bt > 120: self._pending.pop(pool, None); s["agree_unpolled"] = s.get("agree_unpolled", 0) + 1
                continue
            self._pending.pop(pool, None); s["agree_n"] += 1
            if abs(ours - theirs) / theirs <= 0.02: s["agree_ok"] += 1
            if ours == theirs: s["agree_exact"] = s.get("agree_exact", 0) + 1

    def health(self) -> dict:
        s = self.stats
        return dict(events=s["events"], by_type=s["by_type"], dex_seen=s["dex_seen"], filter_pools=s["filter_pools"],
                    lag_ms_avg=round(s["lag_ms_sum"] / s["lag_n"], 1) if s["lag_n"] else None,
                    reserve_agreement=round(s["agree_ok"] / s["agree_n"], 4) if s["agree_n"] else None, agree_n=s["agree_n"], agree_exact=s.get("agree_exact", 0), agree_unpolled=s.get("agree_unpolled", 0),
                    liquidity_add=s["liq_add"], liquidity_remove=s["liq_remove"], reconnects=s["reconnects"], last_event_ts=s["last_event_ts"], last_error=s.get("last_error"),
                    swaps_total=s["swaps_total"], swaps_tracked=s["swaps_tracked"])

    async def run(self):
        import websockets
        while True:
            try:
                async with websockets.connect(self.url, max_size=2 ** 23, ping_interval=20) as ws:
                    last_refresh = 0.0
                    async for raw in ws:
                        try: msg = json.loads(raw)
                        except Exception: continue
                        if time.time() - last_refresh > self.refresh_s:
                            self._tracked = {p for p in (self.pools_fn() or []) if p}; self.stats["filter_pools"] = len(self._tracked); last_refresh = time.time()
                            try: (DATA / "blur_health.json").write_text(json.dumps(dict(ts=time.time(), **self.health()), ensure_ascii=False), encoding="utf-8")
                            except Exception: pass
                        row = self.handle(msg)
                        if not row: continue
                        if row.get("type") == "swap":
                            self.stats["swaps_total"] += 1
                            if self.activity_fn and row.get("pool"): self.activity_fn(row["pool"])
                            if row.get("pool") not in self._tracked: continue
                            self.stats["swaps_tracked"] += 1
                        with (DATA / f"blur_{time.strftime('%Y%m%d', time.gmtime())}.jsonl").open("a", encoding="utf-8") as f:
                            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            except Exception as e:
                self.stats["reconnects"] += 1; wait = min(600, 5 * (2 ** min(self.stats["reconnects"], 7)))
                resp = getattr(e, "response", None); code = getattr(resp, "status_code", None)
                if code in (401, 403):   # 권한 없는 키(예: DataApi 누락) — 반복 접속은 무의미하니 15분 간격으로만 재시도
                    body = bytes(getattr(resp, "body", b"") or b"")[:200].decode("utf-8", "replace")
                    self.stats["last_error"] = f"{code} {body}"; wait = 900
                else:
                    self.stats["last_error"] = f"{type(e).__name__}: {str(e)[:120]}"
                print(f"blur error ({self.stats['last_error']}) — retry in {wait}s", flush=True); await asyncio.sleep(wait)
