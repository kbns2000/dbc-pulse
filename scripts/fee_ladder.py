"""Fee ladder report: who earns the DBC trading fees, measured from PoolState metrics (unit-safe) over a window.

Usage: python scripts/fee_ladder.py [--hours 24] [--rps 2]
Reads data/state_*.jsonl (fee_q per pool snapshot) and pools_live.json (config per pool), fetches PoolConfig accounts once
(fee split: creator_trading_fee_percentage, partner LP shares, migration fee, fee_claimer) and prints per-config and
per-partner (fee_claimer) quote-fee growth in the window, with the implied partner share (protocol takes 20%).
Only SOL-quoted configs are summed in SOL; other quotes are listed raw. Output also saved to data/fee_ladder_<ts>.json.
"""
from __future__ import annotations
import argparse, base64, glob, json, os, sys, time
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dbc_pulse.collector import Rpc, load_env, DATA
from dbc_pulse.idl_decoder import IdlDecoder

ROOT = Path(__file__).resolve().parent.parent; SOL = "So11111111111111111111111111111111111111112"; PROTOCOL_SHARE = 0.20


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--hours", type=float, default=24); ap.add_argument("--rps", type=float, default=2); a = ap.parse_args()
    load_env(); rpc = Rpc(os.environ["SOLANA_RPC_URL"], rps=a.rps); dec = IdlDecoder(ROOT / "dbc_pulse" / "idl" / "dbc.json")
    now = time.time(); cutoff = now - a.hours * 3600; first: dict[str, tuple] = {}; last: dict[str, tuple] = {}
    for f in sorted(glob.glob(str(DATA / "state_*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            try: r = json.loads(line)
            except Exception: continue
            if r.get("fee_q") is None or r["t"] < cutoff: continue
            k = r["pool"]; first.setdefault(k, (r["t"], r["fee_q"], r.get("config"))); last[k] = (r["t"], r["fee_q"], r.get("config"))
    cfg_keys = sorted({v[2] for v in last.values() if v[2]}); cfg = {}
    for i in range(0, len(cfg_keys), 100):
        chunk = cfg_keys[i:i + 100]; res = rpc.call("getMultipleAccounts", [chunk, {"encoding": "base64"}])
        for k, acc in zip(chunk, (res or {}).get("value") or []):
            if not acc: continue
            s, _ = dec._read_defined("PoolConfig", base64.b64decode(acc["data"][0]), 8); bf = s["pool_fees"]["base_fee"]
            cfg[k] = dict(quote=s["quote_mint"], claimer=s["fee_claimer"], creator_pct=s["creator_trading_fee_percentage"], fee_bps=bf["cliff_fee_numerator"] / 1e5, base_fee_mode=bf["base_fee_mode"],
                          partner_lp=s["partner_liquidity_percentage"], partner_locked=s["partner_permanent_locked_liquidity_percentage"], creator_lp=s["creator_liquidity_percentage"], creator_locked=s["creator_permanent_locked_liquidity_percentage"],
                          migration_fee_pct=s["migration_fee_percentage"], creator_migration_fee_pct=s["creator_migration_fee_percentage"], threshold=s["migration_quote_threshold"], pool_creation_fee=s["pool_creation_fee"], collect_fee_mode=s["collect_fee_mode"])
    by_cfg = defaultdict(lambda: dict(pools=0, fee=0.0)); by_partner = defaultdict(lambda: dict(configs=set(), pools=0, fee=0.0, partner=0.0))
    for k, (t1, f1, c) in last.items():
        t0, f0, _ = first[k]; c = c or "?"; q = (cfg.get(c) or {}).get("quote")
        if q != SOL: continue
        d = max(f1 - f0, 0) / 1e9; by_cfg[c]["pools"] += 1; by_cfg[c]["fee"] += d
        cl = (cfg.get(c) or {}).get("claimer", "?"); cr = (cfg.get(c) or {}).get("creator_pct", 0) or 0
        p = by_partner[cl]; p["configs"].add(c); p["pools"] += 1; p["fee"] += d; p["partner"] += d * (1 - PROTOCOL_SHARE) * (1 - cr / 100)
    total = sum(v["fee"] for v in by_cfg.values()); H = a.hours
    print(f"window {H:.0f}h · SOL-quoted pools with snapshots {sum(v['pools'] for v in by_cfg.values())} · configs {len(by_cfg)} · partners {len(by_partner)} · trading fee {total:.2f} SOL → {total / H * 24:.1f} SOL/day (curve phase only)")
    rows = sorted(by_partner.items(), key=lambda kv: -kv[1]["fee"])
    print("partner wallet (fee_claimer)      configs pools   fee SOL  partner≈  /day")
    for k, v in rows[:15]: print(f"  {k[:12]}…  {len(v['configs']):5d} {v['pools']:5d} {v['fee']:9.3f} {v['partner']:9.3f} {v['partner'] / H * 24:7.2f}")
    if total: print(f"  top3 share {sum(v['fee'] for _, v in rows[:3]) / total:.1%} · partners ≥0.1 SOL {sum(1 for _, v in rows if v['fee'] >= 0.1)} · median partner fee {sorted(v['fee'] for _, v in rows)[len(rows) // 2]:.4f} SOL")
    out = dict(ts=now, hours=H, total_fee_sol=total, partners={k: dict(configs=sorted(v["configs"]), pools=v["pools"], fee=v["fee"], partner=v["partner"]) for k, v in rows}, configs={k: dict(v, **(cfg.get(k) or {})) for k, v in by_cfg.items()})
    (DATA / f"fee_ladder_{time.strftime('%Y%m%d_%H%M')}.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__": main()
