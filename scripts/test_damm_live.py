"""Live smoke test for the DAMM v2 side: capture real MigrationDammV2 transactions from the log stream, map them to
DAMM v2 pools, then poll those pools and print the LP realized-yield bookkeeping (with the reserve identity self-check).

Usage: python scripts/test_damm_live.py [--max-wait 300] [--need 2] [--polls 3] [--gap 10]
Budget: one WebSocket + a handful of HTTP calls (rps 2). Safe to run next to the production stream.
"""
from __future__ import annotations
import argparse, asyncio, json, os, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import websockets
from dbc_pulse.collector import Rpc, load_env, DBC_PROGRAM
from dbc_pulse.damm import decode_migrations, DammDecoder, LpTracker, orient, unit_reserves

ROOT = Path(__file__).resolve().parent.parent


async def capture(ws_url: str, need: int, max_wait: float) -> list[str]:
    sigs = []; t0 = time.time()
    async with websockets.connect(ws_url, max_size=2 ** 23, ping_interval=20) as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": [{"mentions": [DBC_PROGRAM]}, {"commitment": "confirmed"}]}))
        await ws.recv()
        while len(sigs) < need and time.time() - t0 < max_wait:
            try: raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError: continue
            v = (json.loads(raw).get("params") or {}).get("result", {}).get("value") or {}
            if v.get("err"): continue
            if any("Instruction: MigrationDammV2" in l for l in v.get("logs") or []):
                sigs.append(v["signature"]); print(f"  +{time.time() - t0:5.0f}s migration tx {v['signature'][:20]}…", flush=True)
    return sigs


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--max-wait", type=float, default=300); ap.add_argument("--need", type=int, default=2); ap.add_argument("--polls", type=int, default=3); ap.add_argument("--gap", type=float, default=10); a = ap.parse_args()
    load_env(); url = os.environ["SOLANA_RPC_URL"]; rpc = Rpc(url, rps=2); dec = DammDecoder(ROOT / "dbc_pulse" / "idl" / "damm_v2.json")
    print(f"capturing up to {a.need} MigrationDammV2 tx (max {a.max_wait:.0f}s)…", flush=True)
    sigs = asyncio.run(capture(url.replace("https://", "wss://", 1), a.need, a.max_wait))
    if not sigs: print("no migration observed in window"); return 2
    migs = []
    for sig in sigs:
        tx = None
        for attempt in range(4):
            tx = rpc.call("getTransaction", [sig, {"encoding": "json", "maxSupportedTransactionVersion": 1, "commitment": "confirmed"}])
            if tx: break
            time.sleep(4)
        if not tx: print("tx not available yet:", sig[:20]); continue
        m = decode_migrations(tx, sig); migs += m
        print("decoded:", json.dumps(m, indent=None)[:600], flush=True)
    if not migs: print("FAIL: migration tx captured but decode_migrations returned nothing"); return 1
    trackers = {m["damm_pool"]: LpTracker(m["damm_pool"], m) for m in migs if m["version"] == "damm_v2"}
    for i in range(a.polls):
        res = rpc.call("getMultipleAccounts", [list(trackers), {"encoding": "base64"}]); t = time.time()
        for k, acc in zip(list(trackers), res["value"]):
            if not acc: print(k[:12], "account missing"); continue
            st = dec.decode_pool(acc["data"][0])
            if not st: print(k[:12], "not a DAMM v2 Pool account (discriminator mismatch)"); continue
            o = orient(st, trackers[k].meta["quote_mint"]); ua, ub = unit_reserves(o["sp"], o["sp_min"], o["sp_max"])
            snap = trackers[k].update(st, t)
            print(f"poll {i} {k[:12]}… quote_is_a={o['quote_is_a']} price(quote/base raw)={snap['price']:.6g} L={o['L']:.6g} res_base={o['res_base']} implied={ua * o['L']:.6g} "
                  f"res_quote={o['res_quote']} implied={ub * o['L']:.6g} reserve_check={snap['reserve_check']} lp_fee_b={st['total_lp_b_fee']} fee_quote_g={o['fee_quote_g']:.6g} status={st['pool_status']}", flush=True)
            if i: print("    summary:", json.dumps(trackers[k].summary()), flush=True)
        if i < a.polls - 1: time.sleep(a.gap)
    return 0


if __name__ == "__main__": sys.exit(main())
