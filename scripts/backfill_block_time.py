"""Fill block_time = 0/None rows in data/migration_*.jsonl and data/lifecycle_*.jsonl from getBlockTime(slot).

Why: on 2026-09-28 Solami's getTransaction at 'confirmed' returned blockTime 0 right after confirmation for some transactions
(stream.py now fixes this at fetch time). Rows are rewritten in place; the original file is kept as <name>.bak_blocktime.
Usage: python scripts/backfill_block_time.py [--rps 4] [--kinds migration,lifecycle]
"""
from __future__ import annotations
import argparse, glob, json, shutil, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dbc_pulse.collector import Rpc, load_env, DATA
from dbc_pulse.solami import provider_urls


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--rps", type=float, default=4); ap.add_argument("--kinds", default="migration,lifecycle"); a = ap.parse_args()
    load_env(); rpc = Rpc(provider_urls()[0], rps=a.rps); cache = {}
    for kind in a.kinds.split(","):
        for f in sorted(glob.glob(str(DATA / f"{kind}_*.jsonl"))):
            lines = open(f, encoding="utf-8").read().splitlines(); rows = []; changed = 0
            for line in lines:
                try: r = json.loads(line)
                except Exception: rows.append(line); continue
                if not r.get("block_time") and r.get("slot"):
                    s = r["slot"]
                    if s not in cache:
                        try: cache[s] = rpc.call("getBlockTime", [s])
                        except Exception: cache[s] = None
                    if cache[s]: r["block_time"] = cache[s]; r["block_time_src"] = "getBlockTime_backfill"; changed += 1
                rows.append(json.dumps(r, ensure_ascii=False) if isinstance(r, dict) else r)
            if changed:
                shutil.copy2(f, f + ".bak_blocktime"); Path(f).write_text("\n".join(rows) + "\n", encoding="utf-8")
            print(f"{Path(f).name}: filled {changed}", flush=True)


if __name__ == "__main__": main()
