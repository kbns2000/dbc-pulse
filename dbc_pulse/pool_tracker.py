"""dbc-pulse pool tracker — folds decoded DBC events into per-pool and per-config state.

Reads data/events_*.jsonl (from collector), writes data/pools.json and data/configs.json.
Per pool: config, creator, base_mint, created_at, swaps, buys, sells, quote_in, quote_out, trading_fee_quote,
          quote_reserve, migration_threshold, progress_pct, unique_wallets (from tx signer when available),
          first_swap, last_swap, curve_complete_at, minutes_to_complete.
Per config: launches, completed, completion_rate, median_minutes_to_complete, median_swaps, total_quote_volume.

Usage: python -m dbc_pulse.pool_tracker [--hours 48]
Notes: EvtSwap (legacy) and EvtSwap2 are both emitted for one fill; only EvtSwap2 is counted.
       trade_direction: 0 = base->quote (sell), 1 = quote->base (buy). Amounts are raw token units (quote usually 9 dp for SOL).
"""
from __future__ import annotations
import argparse, glob, json, statistics as st, time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; DATA = ROOT / "data"


def load_events(hours: float):
    cutoff = time.time() - hours * 3600; rows = []
    for f in sorted(glob.glob(str(DATA / "events_*.jsonl"))):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                try: e = json.loads(line)
                except Exception: continue
                if (e.get("block_time") or 0) >= cutoff: rows.append(e)
    rows.sort(key=lambda e: (e.get("slot") or 0, e.get("ix_index") or ""))
    return rows


def new_pool():
    return dict(config=None, creator=None, base_mint=None, created_at=None, pool_type=None, swaps=0, buys=0, sells=0, quote_in=0, quote_out=0, trading_fee_quote=0,
                quote_reserve=None, migration_threshold=None, progress_pct=None, unique_wallets=set(), first_swap=None, last_swap=None, curve_complete_at=None,
                base_reserve_at_complete=None, quote_reserve_at_complete=None, minutes_to_complete=None, fee_claims=0, leftover_withdrawn=False)


def fold(rows: list[dict]):
    pools = defaultdict(new_pool)
    for e in rows:
        d = e.get("data") or {}; name = e["name"]; ts = e.get("block_time")
        if name in ("EvtInitializePool", "EvtInitializePoolWithTransferHook"):
            p = pools[d["pool"]]; p.update(config=d.get("config"), creator=d.get("creator"), base_mint=d.get("base_mint"), created_at=ts, pool_type=d.get("pool_type"))
        elif name in ("EvtSwap2", "EvtSwap2WithTransferHook"):
            p = pools[d["pool"]]; p["config"] = p["config"] or d.get("config"); res = d.get("swap_result") or {}
            direction = d.get("trade_direction"); amt_in = res.get("included_fee_input_amount", 0); amt_out = res.get("output_amount", 0)
            p["swaps"] += 1
            if direction == 1: p["buys"] += 1; p["quote_in"] += amt_in; p["trading_fee_quote"] += res.get("trading_fee", 0)   # fee taken on quote side for buys
            else: p["sells"] += 1; p["quote_out"] += amt_out; p["trading_fee_quote"] += res.get("trading_fee", 0)
            p["quote_reserve"] = d.get("quote_reserve_amount"); p["migration_threshold"] = d.get("migration_threshold")
            if p["migration_threshold"]: p["progress_pct"] = round(100.0 * (p["quote_reserve"] or 0) / p["migration_threshold"], 2)
            p["first_swap"] = p["first_swap"] or ts; p["last_swap"] = ts
            if e.get("signer"): p["unique_wallets"].add(e["signer"])
        elif name in ("EvtCurveComplete", "EvtCurveCompleteWithTransferHook"):
            p = pools[d["pool"]]; p["curve_complete_at"] = ts; p["base_reserve_at_complete"] = d.get("base_reserve"); p["quote_reserve_at_complete"] = d.get("quote_reserve"); p["progress_pct"] = 100.0
            if p["created_at"] and ts: p["minutes_to_complete"] = round((ts - p["created_at"]) / 60, 1)
        elif name in ("EvtClaimTradingFee", "EvtClaimCreatorTradingFee"): pools[d["pool"]]["fee_claims"] += 1
        elif name == "EvtWithdrawLeftover": pools[d["pool"]]["leftover_withdrawn"] = True
    for p in pools.values(): p["unique_wallets"] = len(p["unique_wallets"])
    return pools


def by_config(pools: dict):
    cfg = defaultdict(lambda: dict(launches=0, completed_of_launched=0, completed_any=0, instant=0, swaps=[], minutes=[], quote_volume=0, pools_seen=0))
    for pk, p in pools.items():
        c = cfg[p["config"] or "unknown"]; c["pools_seen"] += 1; c["quote_volume"] += p["quote_in"] + p["quote_out"]; c["swaps"].append(p["swaps"])
        if p["curve_complete_at"]: c["completed_any"] += 1
        if p["created_at"]:
            c["launches"] += 1
            if p["curve_complete_at"]:
                c["completed_of_launched"] += 1; c["minutes"].append(p["minutes_to_complete"] or 0)
                if (p["minutes_to_complete"] or 0) <= 1 and p["swaps"] <= 2: c["instant"] += 1   # launch + complete inside ~1 min with <=2 fills: bundled/self-filled launch
    out = {}
    for k, c in cfg.items():
        out[k] = dict(pools_seen=c["pools_seen"], launches=c["launches"], completed_of_launched=c["completed_of_launched"], completed_any=c["completed_any"],
                      completion_rate=(round(c["completed_of_launched"] / c["launches"], 3) if c["launches"] else None), instant_share=(round(c["instant"] / c["completed_of_launched"], 3) if c["completed_of_launched"] else None),
                      median_minutes_to_complete=(st.median(c["minutes"]) if c["minutes"] else None), median_swaps=(st.median(c["swaps"]) if c["swaps"] else None), quote_volume=c["quote_volume"])
    return out


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--hours", type=float, default=48); a = ap.parse_args()
    rows = load_events(a.hours); pools = fold(rows); cfgs = by_config(pools)
    (DATA / "pools.json").write_text(json.dumps(pools, ensure_ascii=False, indent=1), encoding="utf-8"); (DATA / "configs.json").write_text(json.dumps(cfgs, ensure_ascii=False, indent=1), encoding="utf-8")
    launched = [p for p in pools.values() if p["created_at"]]; completed = [p for p in pools.values() if p["curve_complete_at"]]
    print(f"events {len(rows)} · pools {len(pools)} · launched-in-window {len(launched)} · completed {len(completed)} · configs {len(cfgs)}")
    if completed: print(f"minutes to complete (launched & completed in window): median {st.median([p['minutes_to_complete'] for p in completed if p['minutes_to_complete'] is not None] or [0]):.1f}")
    top_cfg = sorted(cfgs.items(), key=lambda kv: -kv[1]["pools_seen"])[:8]
    print("top configs:")
    for k, c in top_cfg: print(f"  {k[:12]}… pools {c['pools_seen']:4d} launches {c['launches']:4d} completed {c['completed_of_launched']:3d} rate {c['completion_rate']} instant {c['instant_share']} · median swaps {c['median_swaps']} · median min-to-complete {c['median_minutes_to_complete']}")
    active = sorted([(k, p) for k, p in pools.items() if not p["curve_complete_at"] and p["progress_pct"]], key=lambda kv: -kv[1]["progress_pct"])[:8]
    print("closest to migration (not yet complete):")
    for k, p in active: print(f"  {k[:12]}… progress {p['progress_pct']:6.2f}% · swaps {p['swaps']} (b{p['buys']}/s{p['sells']}) · quote in {p['quote_in']/1e9:.3f} · last swap {time.strftime('%H:%M:%S', time.gmtime(p['last_swap'] or 0))}Z")


if __name__ == "__main__": main()
