"""dbc-pulse collector — pulls Meteora DBC program transactions from a Solana RPC and writes decoded events as JSONL.

Usage:
  python -m dbc_pulse.collector --backfill 500        # decode the last 500 signatures
  python -m dbc_pulse.collector --follow               # poll for new signatures forever
Env: SOLANA_RPC_URL (any JSON-RPC endpoint; Helius/Triton/QuickNode recommended for rate limits).
Output: data/events_YYYYMMDD.jsonl — one line per event: {slot, block_time, signature, ix_index, name, data}
"""
from __future__ import annotations
import argparse, json, os, sys, time
from pathlib import Path
import requests, base58
from .idl_decoder import IdlDecoder

ROOT = Path(__file__).resolve().parent.parent
DBC_PROGRAM = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"
DATA = ROOT / "data"; DATA.mkdir(exist_ok=True)
STATE = DATA / "collector_state.json"


def load_env():
    p = ROOT / ".env"
    if p.exists():
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip())


class Rpc:
    def __init__(self, url: str, rps: float = 8.0):
        self.url = url; self.min_gap = 1.0 / rps; self._last = 0.0; self.s = requests.Session()

    def call(self, method: str, params: list):
        wait = self.min_gap - (time.time() - self._last)
        if wait > 0: time.sleep(wait)
        for attempt in range(5):
            try:
                r = self.s.post(self.url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=40); self._last = time.time()
                if r.status_code == 429 or "json" not in (r.headers.get("content-type") or ""):
                    time.sleep(2.0 * (attempt + 1)); continue   # rate-limited or HTML error page: back off, never hammer
                j = r.json()
                if "error" in j:
                    if j["error"].get("code") == 429 or "rate" in str(j["error"]).lower(): time.sleep(1.5 * (attempt + 1)); continue
                    raise RuntimeError(j["error"])
                return j.get("result")
            except (requests.RequestException, ValueError) as e:
                time.sleep(1.0 * (attempt + 1))
        raise RuntimeError(f"rpc failed: {method}")


def account_keys(tx: dict) -> list[str]:
    msg = tx["transaction"]["message"]; keys = list(msg["accountKeys"]); la = tx["meta"].get("loadedAddresses") or {}
    return keys + list(la.get("writable", [])) + list(la.get("readonly", []))


def decode_tx(dec: IdlDecoder, tx: dict, sig: str) -> list[dict]:
    """Return decoded DBC events found in a transaction's inner instructions (event-CPI)."""
    out = []; keys = account_keys(tx); meta = tx.get("meta") or {}
    if meta.get("err"): return out
    for grp in meta.get("innerInstructions") or []:
        for i, ix in enumerate(grp.get("instructions", [])):
            pid = keys[ix["programIdIndex"]] if ix["programIdIndex"] < len(keys) else None
            if pid != DBC_PROGRAM: continue
            try: data = base58.b58decode(ix["data"])
            except Exception: continue
            ev = dec.decode_event_cpi(data)
            if ev: out.append(dict(slot=tx["slot"], block_time=tx.get("blockTime"), signature=sig, ix_index=f"{grp['index']}.{i}", name=ev["name"], data=ev.get("data"), disc=ev.get("disc")))
    return out


def write_events(events: list[dict]):
    if not events: return
    by_day = {}
    for e in events:
        day = time.strftime("%Y%m%d", time.gmtime(e["block_time"] or time.time())); by_day.setdefault(day, []).append(e)
    for day, evs in by_day.items():
        with (DATA / f"events_{day}.jsonl").open("a", encoding="utf-8") as f:
            for e in evs: f.write(json.dumps(e, ensure_ascii=False) + "\n")


def process_signatures(rpc: Rpc, dec: IdlDecoder, sigs: list[dict], stats: dict) -> list[dict]:
    events = []
    for s in sigs:
        stats["sigs"] += 1
        if s.get("err"): stats["errored"] += 1; continue
        tx = rpc.call("getTransaction", [s["signature"], {"encoding": "json", "maxSupportedTransactionVersion": 1}])
        if not tx: stats["missing"] += 1; continue
        evs = decode_tx(dec, tx, s["signature"]); stats["events"] += len(evs); stats["unknown"] += sum(1 for e in evs if e["name"] == "unknown"); events.extend(evs)
    return events


def backfill(rpc: Rpc, dec: IdlDecoder, n: int):
    stats = dict(sigs=0, errored=0, missing=0, events=0, unknown=0); before = None; got = 0; t0 = time.time(); newest = None
    while got < n:
        batch = rpc.call("getSignaturesForAddress", [DBC_PROGRAM, {"limit": min(1000, n - got), **({"before": before} if before else {})}]) or []
        if not batch: break
        if newest is None: newest = batch[0]["signature"]
        events = process_signatures(rpc, dec, batch, stats); write_events(events); got += len(batch); before = batch[-1]["signature"]
        print(f"  {got}/{n} sigs · events {stats['events']} · errored {stats['errored']} · unknown {stats['unknown']} · {time.time()-t0:.0f}s", flush=True)
    if newest: STATE.write_text(json.dumps({"last_signature": newest, "t": time.time()}), encoding="utf-8")
    return stats


HB = DATA / "follow_heartbeat.json"


def _another_follower_alive(max_age: float = 90.0) -> bool:
    try: return time.time() - json.loads(HB.read_text(encoding="utf-8"))["ts"] < max_age
    except Exception: return False


def follow(rpc: Rpc, dec: IdlDecoder, poll_s: float = 5.0):
    if _another_follower_alive():
        print("another follow collector is alive (heartbeat < 90s) — exiting to keep a single instance", flush=True); return
    st = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}; until = st.get("last_signature")
    print(f"follow mode · until={until} · poll {poll_s}s", flush=True)
    while True:
        try:
            batch = rpc.call("getSignaturesForAddress", [DBC_PROGRAM, {"limit": 1000, **({"until": until} if until else {})}]) or []
            if batch:
                stats = dict(sigs=0, errored=0, missing=0, events=0, unknown=0)
                events = process_signatures(rpc, dec, list(reversed(batch)), stats); write_events(events); until = batch[0]["signature"]
                STATE.write_text(json.dumps({"last_signature": until, "t": time.time()}), encoding="utf-8")
                print(f"{time.strftime('%H:%M:%S')} +{len(batch)} sigs · {stats['events']} events · errored {stats['errored']}", flush=True)
        except Exception as e:
            print(f"error {type(e).__name__}: {e}", flush=True); time.sleep(5)
        try: HB.write_text(json.dumps({"ts": time.time(), "pid": os.getpid(), "until": until}), encoding="utf-8")
        except Exception: pass
        time.sleep(poll_s)


def main():
    load_env(); ap = argparse.ArgumentParser(); ap.add_argument("--backfill", type=int, default=0); ap.add_argument("--follow", action="store_true"); ap.add_argument("--rps", type=float, default=4.0); a = ap.parse_args()
    url = os.environ.get("SOLANA_RPC_URL")
    if not url: sys.exit("SOLANA_RPC_URL not set (.env)")
    rpc = Rpc(url, a.rps); dec = IdlDecoder(ROOT / "dbc_pulse" / "idl" / "dbc.json")
    if a.backfill: print(json.dumps(backfill(rpc, dec, a.backfill)))
    if a.follow: follow(rpc, dec)


if __name__ == "__main__": main()
