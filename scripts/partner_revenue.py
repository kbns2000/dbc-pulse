"""Partner (launchpad) total revenue per graduated DBC pool: curve fees + migration fee + post-graduation LP fees.

Usage: python scripts/partner_revenue.py [--rps 4]
For every DAMM v2 migration in data/migration_*.jsonl it reads, on-chain and now:
  - the DBC VirtualPool (metrics.total_trading_quote_fee)          -> curve-phase partner fee = total x 80% x (1 - creator%)
  - the PoolConfig (fee_claimer = partner, LP and migration-fee %)  -> migration fee = threshold x migration% x (1 - creator migration%)
  - the DAMM v2 Pool (fee per liquidity, price) and both migration positions
      position earned = claimed + pending + (fee_per_liquidity_now - checkpoint) x position liquidity   (exact, per position)
      partner LP fee  = (sum of both positions) x partner LP share of the config                        (split by config %)
Only SOL-quoted pools are summed in SOL (base-token fees valued at the current pool price). Writes data/partner_revenue_<ts>.json.
"""
from __future__ import annotations
import argparse, base64, glob, json, statistics as st, sys, time
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dbc_pulse.collector import Rpc, load_env, DATA
from dbc_pulse.idl_decoder import IdlDecoder
from dbc_pulse.damm import DammDecoder, orient, Q64
from dbc_pulse.solami import working_urls

ROOT = Path(__file__).resolve().parent.parent; SOL = "So11111111111111111111111111111111111111112"; PROTOCOL_SHARE = 0.20
POS_DISC = bytes([170, 188, 143, 228, 122, 64, 247, 208])


def u256(b) -> int: return int.from_bytes(bytes(b), "little")


def fetch(rpc, keys):
    out = {}
    keys = [k for k in dict.fromkeys(keys) if k]
    for i in range(0, len(keys), 100):
        chunk = keys[i:i + 100]; res = rpc.call("getMultipleAccounts", [chunk, {"encoding": "base64"}])
        for k, acc in zip(chunk, (res or {}).get("value") or []):
            if acc: out[k] = base64.b64decode(acc["data"][0])
    return out


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--rps", type=float, default=4); a = ap.parse_args()
    load_env(); url, _, provider = working_urls(); rpc = Rpc(url, rps=a.rps)   # checked endpoint (falls back when the configured one does not answer)
    dbc = IdlDecoder(ROOT / "dbc_pulse" / "idl" / "dbc.json"); dd = DammDecoder(ROOT / "dbc_pulse" / "idl" / "damm_v2.json")
    migs = {}
    for f in sorted(glob.glob(str(DATA / "migration_*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            try: m = json.loads(line)
            except Exception: continue
            if m.get("version") == "damm_v2": migs.setdefault(m["damm_pool"], m)
    migs = list(migs.values())
    vp = fetch(rpc, [m["virtual_pool"] for m in migs]); cf_raw = fetch(rpc, [m["config"] for m in migs])
    pools = fetch(rpc, [m["damm_pool"] for m in migs]); pos = fetch(rpc, [p for m in migs for p in (m["first_position"], m["second_position"])])
    cfg = {}
    for k, raw in cf_raw.items():
        s, _ = dbc._read_defined("PoolConfig", raw, 8)
        lp = [s["partner_liquidity_percentage"], s["partner_permanent_locked_liquidity_percentage"], s["creator_liquidity_percentage"], s["creator_permanent_locked_liquidity_percentage"]]
        cfg[k] = dict(quote=s["quote_mint"], claimer=s["fee_claimer"], creator_pct=s["creator_trading_fee_percentage"] or 0, partner_lp_share=(lp[0] + lp[1]) / max(sum(lp), 1),
                      migration_fee_pct=s["migration_fee_percentage"] or 0, creator_migration_fee_pct=s["creator_migration_fee_percentage"] or 0, threshold=s["migration_quote_threshold"])
    rows = []; skipped = defaultdict(int)
    for m in migs:
        c = cfg.get(m["config"])
        if not c: skipped["no_config"] += 1; continue
        if c["quote"] != SOL: skipped["non_sol_quote"] += 1; continue
        if m["virtual_pool"] not in vp or m["damm_pool"] not in pools: skipped["account_missing"] += 1; continue
        s, _ = dbc._read_defined("PoolState", vp[m["virtual_pool"]], 8); trade_fee = (s.get("metrics") or {}).get("total_trading_quote_fee") or 0
        curve_partner = trade_fee * (1 - PROTOCOL_SHARE) * (1 - c["creator_pct"] / 100)
        mig_fee = c["threshold"] * c["migration_fee_pct"] / 100; mig_partner = mig_fee * (1 - c["creator_migration_fee_pct"] / 100)
        ps = dd.decode_pool(base64.b64encode(pools[m["damm_pool"]]).decode())
        if not ps: skipped["damm_decode"] += 1; continue
        o = orient(ps, SOL); price = o["sp"] ** 2; qa = o["quote_is_a"]
        g_q = ps["fee_a_per_liquidity"] if qa else ps["fee_b_per_liquidity"]; g_b = ps["fee_b_per_liquidity"] if qa else ps["fee_a_per_liquidity"]
        lp_q = lp_b = 0.0; pos_n = 0
        for pk in (m["first_position"], m["second_position"]):
            raw = pos.get(pk)
            if not raw or raw[:8] != POS_DISC: continue
            p, _ = dd.dec._read_defined("Position", raw, 8); L = (p["unlocked_liquidity"] + p["vested_liquidity"] + p["permanent_locked_liquidity"]) / Q64
            cq, cb = (u256(p["fee_a_per_token_checkpoint"]), u256(p["fee_b_per_token_checkpoint"])) if qa else (u256(p["fee_b_per_token_checkpoint"]), u256(p["fee_a_per_token_checkpoint"]))
            pend_q, pend_b = (p["fee_a_pending"], p["fee_b_pending"]) if qa else (p["fee_b_pending"], p["fee_a_pending"])
            clm_q, clm_b = (p["metrics"]["total_claimed_a_fee"], p["metrics"]["total_claimed_b_fee"]) if qa else (p["metrics"]["total_claimed_b_fee"], p["metrics"]["total_claimed_a_fee"])
            lp_q += clm_q + pend_q + max(g_q - cq, 0) / Q64 * L; lp_b += clm_b + pend_b + max(g_b - cb, 0) / Q64 * L; pos_n += 1
        lp_total = lp_q + lp_b * price; lp_partner = lp_total * c["partner_lp_share"]
        creator = trade_fee * (1 - PROTOCOL_SHARE) * c["creator_pct"] / 100 + mig_fee * c["creator_migration_fee_pct"] / 100 + lp_total * (1 - c["partner_lp_share"])
        rows.append(dict(pool=m["damm_pool"], config=m["config"], partner=c["claimer"], hours=round((time.time() - m["block_time"]) / 3600, 2), positions=pos_n,
                         curve_partner=curve_partner / 1e9, migration_partner=mig_partner / 1e9, lp_partner=lp_partner / 1e9, lp_total=lp_total / 1e9, trade_fee=trade_fee / 1e9,
                         partner_total=(curve_partner + mig_partner + lp_partner) / 1e9, partner_lp_share=c["partner_lp_share"],
                         creator_total=creator / 1e9, protocol_curve=trade_fee * PROTOCOL_SHARE / 1e9, creator_pct=c["creator_pct"]))
    if not rows: print("no rows", dict(skipped)); return
    span_h = (time.time() - min(m["block_time"] for m in migs if (m.get("block_time") or 0) > 1.7e9)) / 3600
    tot = {k: sum(r[k] for r in rows) for k in ("curve_partner", "migration_partner", "lp_partner", "lp_total", "partner_total", "creator_total", "protocol_curve", "trade_fee")}
    byp = defaultdict(lambda: dict(pools=0, total=0.0, curve=0.0, migration=0.0, lp=0.0))
    for r in rows:
        b = byp[r["partner"]]; b["pools"] += 1; b["total"] += r["partner_total"]; b["curve"] += r["curve_partner"]; b["migration"] += r["migration_partner"]; b["lp"] += r["lp_partner"]
    ranked = sorted(byp.items(), key=lambda kv: -kv[1]["total"])
    per_pool = sorted(r["partner_total"] for r in rows)
    print(f"provider {provider} · DAMM v2 migrations {len(migs)} · SOL-quoted scored {len(rows)} · skipped {dict(skipped)} · window {span_h:.1f} h")
    print(f"partner revenue (SOL): curve {tot['curve_partner']:.3f} + migration fee {tot['migration_partner']:.3f} + LP fees {tot['lp_partner']:.3f} [split by config %] = {tot['partner_total']:.3f}"
          f"  -> {tot['partner_total'] / span_h * 24:.2f} SOL/day · all migration-LP fees {tot['lp_total']:.3f}")
    print(f"per graduated pool: median {st.median(per_pool):.4f} · p90 {per_pool[int(0.9 * len(per_pool))]:.4f} · max {per_pool[-1]:.3f} SOL · partners {len(byp)}"
          f" · top3 share {sum(v['total'] for _, v in ranked[:3]) / max(tot['partner_total'], 1e-12):.1%}")
    print(f"creators {tot['creator_total']:.3f} SOL · protocol (curve 20%) {tot['protocol_curve']:.3f} SOL · curve trading fees {tot['trade_fee']:.3f} SOL"
          f" · pools with partner revenue 0: {sum(1 for r in rows if r['partner_total'] <= 0)} of {len(rows)} · creator_pct=100 configs' pools {sum(1 for r in rows if r['creator_pct'] >= 100)}")
    print("partner (fee_claimer)   pools   total   curve  migr.     LP")
    for k, v in ranked[:12]: print(f"  {k[:12]}…  {v['pools']:5d} {v['total']:7.3f} {v['curve']:7.3f} {v['migration']:6.3f} {v['lp']:6.3f}")
    out = dict(ts=time.time(), provider=provider, window_h=span_h, migrations=len(migs), scored=len(rows), skipped=dict(skipped), totals=tot,
               per_pool=dict(median=st.median(per_pool), p90=per_pool[int(0.9 * len(per_pool))], max=per_pool[-1]),
               partners={k: v for k, v in ranked}, rows=rows)
    (DATA / f"partner_revenue_{time.strftime('%Y%m%d_%H%M')}.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__": main()
