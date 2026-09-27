"""dbc-pulse console viewer — human-readable snapshot of the live stream files (for demos and quick checks).

Usage: python -m dbc_pulse.view [pools|damm|all] [--top 12] [--watch SECONDS]
Reads data/pools_live.json, data/damm_live.json, data/stream_heartbeat.json. No RPC calls.
"""
from __future__ import annotations
import argparse, json, os, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; DATA = ROOT / "data"


def _load(name: str):
    try: return json.loads((DATA / name).read_text(encoding="utf-8"))
    except Exception: return {}


def _sol(x) -> str:
    return f"{x / 1e9:9.3f}" if isinstance(x, (int, float)) else "        -"


def show_heartbeat():
    hb = _load("stream_heartbeat.json")
    if not hb: print("stream: no heartbeat"); return
    age = time.time() - hb.get("ts", 0)
    print(f"stream pid {hb.get('pid')} · heartbeat {age:4.0f}s ago · logs {hb.get('logs')} swaps {hb.get('swaps')} lifecycle-events {hb.get('lifecycle')} migrations {hb.get('migrations', 0)} "
          f"· pools {hb.get('pools')} configs {hb.get('configs')} damm {hb.get('damm', 0)} · errors {hb.get('errors')} dropped {hb.get('dropped', 0)}")


def show_pools(top: int):
    pools = _load("pools_live.json")
    rows = [(k, p) for k, p in pools.items() if p.get("quote_reserve") is not None]
    rows.sort(key=lambda kv: -(kv[1].get("progress_pct") or 0))
    print(f"\n{'pool':14} {'progress':>8} {'quote res':>10} {'buy Δ':>9} {'sell Δ':>9} {'fee q':>9} {'migr':>4} {'age min':>7}  config")
    now = time.time()
    for k, p in rows[:top]:
        age = (now - p.get("first_seen", now)) / 60
        print(f"{k[:12]}…  {(p.get('progress_pct') or 0):7.2f}% {_sol(p.get('quote_reserve'))} {_sol(p.get('quote_delta_buy', 0))} {_sol(p.get('quote_delta_sell', 0))} {_sol(p.get('trading_quote_fee'))} {'yes' if p.get('is_migrated') else 'no':>4} {age:7.1f}  {(p.get('config') or '')[:10]}…")
    print(f"tracked pools {len(pools)} · with state {len(rows)} · migrated {sum(1 for _, p in rows if p.get('is_migrated'))}")


def show_damm(top: int):
    d = _load("damm_live.json")
    if not d: print("\ndamm: no pools tracked yet"); return
    rows = sorted(d.items(), key=lambda kv: -(kv[1].get("hours") or 0))
    print(f"\n{'DAMM v2 pool':14} {'hours':>6} {'price %':>8} {'fees %':>8} {'LVR %':>8} {'net %':>8} {'σ ann':>6} {'σ²/8 %':>7} {'chk':>5} {'polls':>5}")
    for k, p in rows[:top]:
        f = lambda key, w=8, d=3: (f"{p[key]:{w}.{d}f}" if isinstance(p.get(key), (int, float)) else f"{'-':>{w}}")
        print(f"{k[:12]}…  {f('hours', 6, 2)} {f('price_change_pct')} {f('fees_pct')} {f('lvr_pct')} {f('net_pct')} {f('sigma_ann', 6, 2)} {f('theory_lvr_pct', 7, 3)} {f('reserve_check', 5, 2)} {p.get('polls', 0):5d}")
    print(f"tracked DAMM v2 pools {len(d)} · fees/LVR/net are per unit of liquidity, in quote, since migration")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("what", nargs="?", default="all"); ap.add_argument("--top", type=int, default=12); ap.add_argument("--watch", type=float, default=0); a = ap.parse_args()
    while True:
        if a.watch: os.system("cls" if os.name == "nt" else "clear")
        print(time.strftime("%Y-%m-%d %H:%M:%S")); show_heartbeat()
        if a.what in ("pools", "all"): show_pools(a.top)
        if a.what in ("damm", "all"): show_damm(a.top)
        if not a.watch: break
        time.sleep(a.watch)


if __name__ == "__main__": main()
