"""dbc-pulse stream (v2 architecture) — cheap, complete lifecycle coverage + near-real-time pool state.

Why v2: the DBC program does ~25 tx/s. Fetching every transaction costs 25 RPC calls/s (2M/day) — not viable on
a small plan. Instead:
  1. logsSubscribe on the program (one WebSocket) classifies every tx by its "Instruction: X" logs.
  2. Only lifecycle transactions (initialize / migration / withdraw / claims — a few per minute) are fetched
     with getTransaction and fully decoded (event-CPI). Swaps are counted from logs (rate) and optionally sampled.
  3. Pool state is read directly from VirtualPool accounts with getMultipleAccounts every few seconds
     (100 pools per call): quote/base reserves, sqrt_price, fees, migration flags. Reserve deltas between polls
     give buy/sell pressure without per-swap fetches. PoolConfig accounts (cached) give migration thresholds.
Budget: ~1 WebSocket + ~1-2 HTTP calls/s regardless of chain volume.

Usage: python -m dbc_pulse.stream            (single instance; refuses to start if another stream heartbeat is fresh)
Output: data/lifecycle_YYYYMMDD.jsonl (decoded lifecycle events), data/state_YYYYMMDD.jsonl (pool snapshots),
        data/pools_live.json (current state of tracked pools), data/stream_heartbeat.json
"""
from __future__ import annotations
import asyncio, base64, json, os, time, glob
from pathlib import Path
import requests, websockets
from .idl_decoder import IdlDecoder
from .collector import Rpc, decode_tx, load_env, DBC_PROGRAM, DATA
from .damm import decode_migrations, DammDecoder, LpTracker
from .solami import provider_urls, solami_key, BlurTap

ROOT = Path(__file__).resolve().parent.parent
HB = DATA / "stream_heartbeat.json"; LIVE = DATA / "pools_live.json"; DAMM_LIVE = DATA / "damm_live.json"
DAMM_POLL_S = float(os.environ.get("DBC_DAMM_POLL_S", "10")); DAMM_MAX_AGE_S = float(os.environ.get("DBC_DAMM_MAX_AGE_H", "72")) * 3600
LIFECYCLE_MARKERS = ("Initialize", "Migration", "Migrate", "WithdrawLeftover", "ClaimTradingFee", "ClaimCreatorTradingFee", "CreatorWithdrawSurplus", "PartnerWithdrawSurplus", "WithdrawMigrationFee", "CreateConfig", "TransferPoolCreator")
POLL_S = float(os.environ.get("DBC_POLL_S", "3")); MAX_TRACK_AGE_S = 6 * 3600; SWAP_SAMPLE_EVERY = int(os.environ.get("DBC_SWAP_SAMPLE", "50"))


def _jsonl(name: str, rows: list[dict]):
    if not rows: return
    with (DATA / f"{name}_{time.strftime('%Y%m%d', time.gmtime())}.jsonl").open("a", encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r, ensure_ascii=False) + "\n")


class PoolUniverse:
    """Tracked pools: discovered from lifecycle events, prior pools.json, and state polling."""
    def __init__(self):
        self.pools: dict[str, dict] = {}   # pool -> live state
        self.configs: dict[str, dict] = {}  # config -> decoded PoolConfig (threshold etc.)
        self.seed()

    def seed(self):
        try:
            for k, p in json.loads((DATA / "pools.json").read_text(encoding="utf-8")).items():
                if not p.get("curve_complete_at"): self.pools.setdefault(k, {"first_seen": time.time(), "source": "pools.json"})
        except Exception: pass
        try:
            for k, p in json.loads(LIVE.read_text(encoding="utf-8")).items():
                if not p.get("is_migrated"): self.pools.setdefault(k, {"first_seen": time.time(), "source": "pools_live.json"})
        except Exception: pass

    def add(self, pool: str, source: str):
        self.pools.setdefault(pool, {"first_seen": time.time(), "source": source})

    def prune(self):
        now = time.time()
        for k in list(self.pools):
            p = self.pools[k]
            if p.get("is_migrated") or (now - p.get("last_activity", p.get("first_seen", now)) > MAX_TRACK_AGE_S): self.pools.pop(k, None)


class Stream:
    def __init__(self):
        load_env(); url, ws_url, self.provider = provider_urls()
        if not url: raise SystemExit("SOLANA_RPC_URL not set (.env) — or set SOLAMI_API_KEY + DBC_PROVIDER=solami")
        self.rpc = Rpc(url, rps=float(os.environ.get("DBC_RPS", "4"))); self.ws_url = ws_url
        self.dec = IdlDecoder(ROOT / "dbc_pulse" / "idl" / "dbc.json"); self.uni = PoolUniverse()
        self.damm_dec = DammDecoder(ROOT / "dbc_pulse" / "idl" / "damm_v2.json"); self.damm: dict[str, LpTracker] = {}; self._seed_damm()
        self.counts = {"logs": 0, "swaps": 0, "lifecycle": 0, "fetched": 0, "sampled": 0, "migrations": 0, "dropped": 0, "errors": 0}; self.swap_seen = 0; self.pending: list[tuple[str, str]] = []; self.retry: list[tuple[float, str, str]] = []

    def _seed_damm(self):
        cutoff = time.time() - DAMM_MAX_AGE_S
        for f in sorted(glob.glob(str(DATA / "migration_*.jsonl"))):
            for line in open(f, encoding="utf-8"):
                try: m = json.loads(line)
                except Exception: continue
                if m.get("version") == "damm_v2" and (m.get("block_time") or 0) >= cutoff: self.damm.setdefault(m["damm_pool"], LpTracker(m["damm_pool"], m))

    # ---------- lifecycle via logs ----------
    async def ws_loop(self):
        while True:
            try:
                async with websockets.connect(self.ws_url, max_size=2 ** 23, ping_interval=20) as ws:
                    await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": [{"mentions": [DBC_PROGRAM]}, {"commitment": "confirmed"}]}))
                    await ws.recv()
                    async for raw in ws:
                        msg = json.loads(raw); v = (msg.get("params") or {}).get("result", {}).get("value") or {}
                        if not v or v.get("err"): continue
                        logs = v.get("logs") or []; names = [l.split("Instruction: ", 1)[1] for l in logs if "Instruction: " in l]
                        self.counts["logs"] += 1; kind = "swap" if any(n.startswith("Swap") for n in names) else "other"
                        if any(any(n.startswith(m) for m in LIFECYCLE_MARKERS) for n in names): self.pending.append((v["signature"], "lifecycle"))
                        elif kind == "swap":
                            self.counts["swaps"] += 1; self.swap_seen += 1
                            if SWAP_SAMPLE_EVERY and self.swap_seen % SWAP_SAMPLE_EVERY == 0: self.pending.append((v["signature"], "sample"))
            except Exception as e:
                self.counts["errors"] += 1; print(f"ws error {type(e).__name__}: {e}", flush=True); await asyncio.sleep(3)

    async def fetch_loop(self):
        while True:
            now = time.time()
            while self.retry and self.retry[0][0] <= now: _, rs, rw = self.retry.pop(0); self.pending.append((rs, rw))
            if not self.pending: await asyncio.sleep(0.5); continue
            sig, why = self.pending.pop(0)[:2]; tries = 0
            if "|" in why: why, tries = why.split("|")[0], int(why.split("|")[1])
            try:
                tx = await asyncio.to_thread(self.rpc.call, "getTransaction", [sig, {"encoding": "json", "maxSupportedTransactionVersion": 1, "commitment": "confirmed"}])
                if not tx:   # not yet available at this commitment (or dropped): retry a few times, spaced out
                    if tries < 3: self.retry.append((time.time() + 4.0 * (tries + 1), sig, f"{why}|{tries + 1}"))
                    else: self.counts["dropped"] += 1
                    continue
                evs = decode_tx(self.dec, tx, sig); self.counts["fetched"] += 1
                for e in evs:
                    d = e.get("data") or {}
                    if "pool" in d: self.uni.add(d["pool"], e["name"]); self.uni.pools[d["pool"]]["last_activity"] = time.time()
                    if "virtual_pool" in d: self.uni.add(d["virtual_pool"], e["name"])
                if why == "lifecycle":
                    self.counts["lifecycle"] += len(evs); _jsonl("lifecycle", evs)
                    migs = decode_migrations(tx, sig)
                    if migs:
                        self.counts["migrations"] += len(migs); _jsonl("migration", migs)
                        for m in migs:
                            if m["virtual_pool"] in self.uni.pools: self.uni.pools[m["virtual_pool"]].update(damm_pool=m["damm_pool"], migrated_at=m["block_time"], migration_version=m["version"])
                            if m["version"] == "damm_v2": self.damm.setdefault(m["damm_pool"], LpTracker(m["damm_pool"], m))
                else: self.counts["sampled"] += len(evs); _jsonl("swap_sample", evs)
            except Exception as e: self.counts["errors"] += 1; print(f"fetch error {type(e).__name__}: {e}", flush=True)

    # ---------- state via accounts ----------
    def _decode_pool(self, b64: str) -> dict | None:
        raw = base64.b64decode(b64)
        if list(raw[:8]) != [213, 224, 5, 209, 98, 69, 119, 92]: return None
        st, _ = self.dec._read_defined("PoolState", raw, 8)
        m = st.get("metrics") or {}
        return dict(config=st["config"], creator=st["creator"], base_mint=st["base_mint"], quote_reserve=st["quote_reserve"], base_reserve=st["base_reserve"], sqrt_price=st["sqrt_price"],
                    is_migrated=st["is_migrated"], is_curve_complete=bool(st.get("finish_curve_timestamp")), finish_curve_timestamp=st.get("finish_curve_timestamp"), activation_point=st["activation_point"],
                    trading_quote_fee=m.get("total_trading_quote_fee"), trading_base_fee=m.get("total_trading_base_fee"), protocol_quote_fee=m.get("total_protocol_quote_fee"),
                    unclaimed_quote_fee=sum(int(st.get(k) or 0) for k in ("protocol_quote_fee", "partner_quote_fee", "creator_quote_fee")))

    def _decode_config(self, b64: str) -> dict | None:
        raw = base64.b64decode(b64); st, _ = self.dec._read_defined("PoolConfig", raw, 8)
        return dict(quote_mint=st["quote_mint"], migration_quote_threshold=st["migration_quote_threshold"], migration_base_threshold=st.get("migration_base_threshold"), token_decimal=st.get("token_decimal"), collect_fee_mode=st.get("collect_fee_mode"), migration_option=st.get("migration_option"), migration_fee_option=st.get("migration_fee_option"))

    async def poll_loop(self):
        while True:
            t0 = time.time(); keys = self._poll_keys(); snaps = []
            for i in range(0, len(keys), 100):
                chunk = keys[i:i + 100]
                try: res = await asyncio.to_thread(self.rpc.call, "getMultipleAccounts", [chunk, {"encoding": "base64"}])
                except Exception as e: self.counts["errors"] += 1; print(f"poll error {type(e).__name__}: {e}", flush=True); continue
                for k, acc in zip(chunk, (res or {}).get("value") or []):
                    if not acc: self.uni.pools.pop(k, None); continue
                    st = self._decode_pool(acc["data"][0])
                    if not st: continue
                    prev = self.uni.pools[k]; cfg = st["config"]
                    if cfg not in self.uni.configs: self.uni.configs[cfg] = None   # fetched below
                    cfgd = self.uni.configs.get(cfg) or {}; thr = cfgd.get("migration_quote_threshold")
                    if cfgd.get("quote_mint"): prev["quote_mint"] = cfgd["quote_mint"]
                    dq = st["quote_reserve"] - prev.get("quote_reserve", st["quote_reserve"])
                    prev.update(st); prev["progress_pct"] = round(100 * st["quote_reserve"] / thr, 3) if thr else None; prev["last_poll"] = t0
                    if dq: prev["last_activity"] = t0; prev["quote_delta_buy"] = prev.get("quote_delta_buy", 0) + max(dq, 0); prev["quote_delta_sell"] = prev.get("quote_delta_sell", 0) + max(-dq, 0)
                    snaps.append(dict(t=int(t0), pool=k, config=cfg, quote_reserve=st["quote_reserve"], base_reserve=st["base_reserve"], sqrt_price=str(st["sqrt_price"]), dq=dq, progress_pct=prev["progress_pct"], migrated=st["is_migrated"], fee_q=st["trading_quote_fee"]))
            # fetch unknown configs (cached forever)
            missing = [c for c, v in self.uni.configs.items() if v is None][:100]
            if missing:
                try:
                    res = await asyncio.to_thread(self.rpc.call, "getMultipleAccounts", [missing, {"encoding": "base64"}])
                    for c, acc in zip(missing, (res or {}).get("value") or []):
                        self.uni.configs[c] = self._decode_config(acc["data"][0]) if acc else {}
                except Exception as e: self.counts["errors"] += 1
            _jsonl("state", [s for s in snaps if s["dq"] or s["migrated"]]); self.uni.prune()
            try:
                LIVE.write_text(json.dumps(self.uni.pools, ensure_ascii=False), encoding="utf-8")
                HB.write_text(json.dumps(dict(ts=time.time(), pid=os.getpid(), provider=self.provider, pools=len(self.uni.pools), configs=len(self.uni.configs), damm=len(self.damm), blur=(self.blur.health() if getattr(self, "blur", None) else None), **self.counts)), encoding="utf-8")
            except Exception: pass
            await asyncio.sleep(max(0.5, POLL_S - (time.time() - t0)))

    # ---------- DAMM v2 LP realized yield ----------
    async def damm_loop(self):
        while True:
            t0 = time.time(); keys = list(self.damm)[:500]; snaps = []
            for i in range(0, len(keys), 100):
                chunk = keys[i:i + 100]
                try: res = await asyncio.to_thread(self.rpc.call, "getMultipleAccounts", [chunk, {"encoding": "base64", "commitment": "confirmed"}])
                except Exception as e: self.counts["errors"] += 1; print(f"damm poll error {type(e).__name__}: {e}", flush=True); continue
                for k, acc in zip(chunk, (res or {}).get("value") or []):
                    tr = self.damm.get(k)
                    if not tr: continue
                    if not acc: tr.meta["missing"] = tr.meta.get("missing", 0) + 1; continue
                    try: st = self.damm_dec.decode_pool(acc["data"][0])
                    except Exception as e: self.counts["errors"] += 1; continue
                    if not st: continue
                    snap = tr.update(st, t0)
                    if tr.n == 1 or (tr.prev and snap.get("price") != getattr(tr, "_last_written", None)): snaps.append(snap); tr._last_written = snap.get("price")
            for k in list(self.damm):
                tr = self.damm[k]
                if (tr.t0 and t0 - tr.t0 > DAMM_MAX_AGE_S) or tr.meta.get("missing", 0) > 20: self.damm.pop(k, None)
            _jsonl("damm", snaps)
            try: DAMM_LIVE.write_text(json.dumps({k: dict(since=tr.meta.get("block_time"), virtual_pool=tr.meta.get("virtual_pool"), base_mint=tr.meta.get("base_mint"), quote_mint=tr.meta.get("quote_mint"), price=tr.last_price, reserve_check=tr.reserve_check, **tr.summary()) for k, tr in self.damm.items()}, ensure_ascii=False), encoding="utf-8")
            except Exception: pass
            await asyncio.sleep(max(1.0, DAMM_POLL_S - (time.time() - t0)))

    async def status_loop(self):
        while True:
            await asyncio.sleep(60); c = self.counts
            print(f"{time.strftime('%H:%M:%S')} logs {c['logs']} swaps {c['swaps']} lifecycle-events {c['lifecycle']} fetched {c['fetched']} sampled {c['sampled']} errors {c['errors']} · pools {len(self.uni.pools)} configs {len(self.uni.configs)} · migrations {c['migrations']} damm-tracked {len(self.damm)}", flush=True)

    def _poll_keys(self, cap: int = 1000, hot: int = 700) -> list:
        """Pools to read this cycle. The old rule took the first `cap` pools in insertion order, so once the universe grew past
        1,000 the newest pools were never read (measured 2026-09-28: 427 of 1,395 unpolled, including the two busiest curves at
        ~5,000 Blur swaps / 30 min). Now: the `hot` most recently active pools every cycle + a rotating window over the rest."""
        allk = list(self.uni.pools)
        if len(allk) <= cap: return allk
        act = sorted(allk, key=lambda k: -(self.uni.pools[k].get("last_activity") or self.uni.pools[k].get("first_seen") or 0))
        rest = act[hot:]; n = cap - hot; i = getattr(self, "_rr", 0) % len(rest); self._rr = i + n
        return act[:hot] + (rest[i:i + n] + rest[:max(0, i + n - len(rest))])

    def _mark_active(self, pool):
        p = self.uni.pools.get(pool)
        if p is not None: p["last_activity"] = time.time()

    def _blur_pools(self):
        return list(self.uni.pools) + list(self.damm)

    def _our_quote_reserve(self, pool):
        p = self.uni.pools.get(pool)
        if p and p.get("quote_reserve"): return p["quote_reserve"] + (p.get("unclaimed_quote_fee") or 0), p.get("last_poll")   # vault balance (what Blur reports), poll time
        tr = self.damm.get(pool)
        if tr and tr.prev: return None   # DAMM reserves are per-unit in the tracker; agreement check uses DBC curves only
        return None

    async def run(self):
        tasks = [self.ws_loop(), self.fetch_loop(), self.poll_loop(), self.damm_loop(), self.status_loop()]
        key = solami_key()
        if key and os.environ.get("DBC_BLUR", "1") != "0":
            self.blur = BlurTap(key, self._blur_pools, self._our_quote_reserve); self.blur.activity_fn = self._mark_active; tasks.append(self.blur.run())
        await asyncio.gather(*tasks)


def _pid_alive(pid: int) -> bool:
    """Windows-safe liveness check (never os.kill: on Windows that terminates the process)."""
    try:
        import ctypes
        h = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))   # PROCESS_QUERY_LIMITED_INFORMATION
        if not h: return False
        code = ctypes.c_ulong(); ok = ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(code)); ctypes.windll.kernel32.CloseHandle(h)
        return bool(ok) and code.value == 259   # STILL_ACTIVE
    except Exception:
        try: os.kill(int(pid), 0); return True    # POSIX
        except Exception: return False


def another_alive(max_age: float = 90.0) -> bool:
    try:
        hb = json.loads(HB.read_text(encoding="utf-8"))
        if hb.get("pid") == os.getpid(): return False
        return time.time() - hb["ts"] < max_age and _pid_alive(hb.get("pid", -1))
    except Exception: return False


def main():
    if another_alive(): print("another stream is alive (heartbeat < 90s) — exiting", flush=True); return
    asyncio.run(Stream().run())


if __name__ == "__main__": main()
