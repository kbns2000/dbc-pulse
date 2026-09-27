"""dbc-pulse stats — summarize decoded events: per-pool swaps, volume, curve progress, graduations.

Usage: python -m dbc_pulse.stats [--hours 24]
"""
from __future__ import annotations
import argparse, glob, json, time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; DATA = ROOT / "data"


def load(hours: float):
    cutoff = time.time() - hours * 3600; rows = []
    for f in sorted(glob.glob(str(DATA / "events_*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            e = json.loads(line)
            if (e.get("block_time") or 0) >= cutoff: rows.append(e)
    return rows


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--hours", type=float, default=24); a = ap.parse_args(); rows = load(a.hours)
    by_name = defaultdict(int); pools = defaultdict(lambda: dict(swaps=0, buys=0, sells=0, quote_in=0, quote_out=0, fees=0, last_progress=None, complete=False, created=None, last_ts=None))
    for e in rows:
        by_name[e["name"]] += 1; d = e.get("data") or {}
        if e["name"] in ("EvtSwap2", "EvtSwap2WithTransferHook"):   # EvtSwap (legacy) is emitted alongside EvtSwap2 for the same fill — count once
            p = pools[d["pool"]]; p["swaps"] += 1; direction = d.get("trade_direction"); res = d.get("swap_result") or {}
            # trade_direction 0 = base->quote (sell), 1 = quote->base (buy) per SDK convention
            amt_in = res.get("included_fee_input_amount", 0); amt_out = res.get("output_amount", 0)
            if direction == 1: p["buys"] += 1; p["quote_in"] += amt_in
            else: p["sells"] += 1; p["quote_out"] += amt_out
            p["fees"] += res.get("trading_fee", 0)
            thr = d.get("migration_threshold") or 0
            if thr: p["last_progress"] = round(100 * d.get("quote_reserve_amount", 0) / thr, 2)
            p["last_ts"] = e["block_time"]
        elif e["name"] in ("EvtInitializePool", "EvtInitializePoolWithTransferHook"): pools[d["pool"]]["created"] = e["block_time"]
        elif e["name"] in ("EvtCurveComplete", "EvtCurveCompleteWithTransferHook"): pools[d["pool"]]["complete"] = True
    span = (max(r["block_time"] for r in rows) - min(r["block_time"] for r in rows)) if rows else 0
    print(f"events {len(rows)} · span {span/60:.1f} min · pools touched {len(pools)}")
    print("by event:", dict(sorted(by_name.items(), key=lambda kv: -kv[1])))
    created = sum(1 for p in pools.values() if p["created"]); complete = sum(1 for p in pools.values() if p["complete"])
    print(f"pools created in window {created} · curves completed {complete}")
    top = sorted(pools.items(), key=lambda kv: -(kv[1]["quote_in"] + kv[1]["quote_out"]))[:10]
    print("top pools by quote volume (lamports of quote token):")
    for pk, p in top:
        print(f"  {pk[:12]}… swaps {p['swaps']:5d} (buy {p['buys']}/sell {p['sells']}) · quote in {p['quote_in']/1e9:.3f} out {p['quote_out']/1e9:.3f} · fees {p['fees']/1e9:.4f} · progress {p['last_progress']}% · complete {p['complete']}")


if __name__ == "__main__": main()
