"""Publish a public, key-free JSON feed of dbc-pulse results to the `feed` branch (raw.githubusercontent.com).

Usage: python scripts/publish_feed.py [--feed-dir ../dbc_pulse_feed] [--no-push]
Writes into a separate clone checked out on branch `feed` (never touches main):
  feed/summary.json                 stream health, counts, Blur cross-check
  feed/configs.json                 per launch config: pools, completion, instant share, graduation take
  feed/graduation_take_flags.json   live curves whose config pays >=20% of the raise out at graduation
  feed/partner_revenue.json         latest per-graduation revenue split (totals + top partners), if measured
Only on-chain public data; no keys, no wallet data of ours. Commits and pushes only when something changed.
"""
from __future__ import annotations
import argparse, glob, json, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dbc_pulse import api

ROOT = Path(__file__).resolve().parent.parent; DATA = ROOT / "data"
FLAG_PCT = 20


def build() -> dict:
    hb = api.health(); s = hb.get("stream") or {}; b = s.get("blur") or {}
    cfgs = api.configs(); pools = api.pools(0.0, 100000)
    flags = [dict(pool=p["pool"], base_mint=p.get("base_mint"), config=p.get("config"), quote_mint=p.get("quote_mint"), progress_pct=p.get("progress_pct"),
                  graduation_take_pct=p.get("graduation_take_pct"), graduation_take_creator_pct=p.get("graduation_take_creator_pct"))
             for p in pools if (p.get("graduation_take_pct") or 0) >= FLAG_PCT and not p.get("is_migrated")]
    flags.sort(key=lambda r: -(r.get("progress_pct") or 0))
    keep = ("config", "pools", "curve_complete", "migrated", "launches", "completion_rate", "instant_share", "median_minutes_to_complete", "median_swaps",
            "graduation_take_pct", "graduation_take_creator_pct")
    configs = [{k: c.get(k) for k in keep} for c in cfgs]
    rev = None; files = sorted(glob.glob(str(DATA / "partner_revenue_*.json")))
    if files:
        r = json.loads(Path(files[-1]).read_text(encoding="utf-8"))
        top = sorted((r.get("partners") or {}).items(), key=lambda kv: -kv[1]["total"])[:20]
        rev = dict(measured_at=r.get("ts"), window_h=r.get("window_h"), graduations_scored=r.get("scored"), totals_sol=r.get("totals"), per_pool_sol=r.get("per_pool"),
                   top_partners=[dict(fee_claimer=k, **v) for k, v in top], method="scripts/partner_revenue.py (LP split by config %)")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    summary = dict(updated_utc=now, stream_alive=hb.get("stream_alive"), provider=s.get("provider"), curves_tracked=s.get("pools"), configs_seen=len(configs),
                   damm_pools_tracked=s.get("damm"), migrations_seen=s.get("migrations"), rpc_errors=s.get("errors"), graduation_take_flags=len(flags),
                   blur=dict(events=b.get("events"), lag_ms_avg=b.get("lag_ms_avg"), compared=b.get("agree_n"), identical=b.get("agree_exact"), within_2pct=b.get("reserve_agreement")) if b else None,
                   source="https://github.com/kbns2000/dbc-pulse")
    return {"summary.json": summary, "configs.json": dict(updated_utc=now, configs=configs), "graduation_take_flags.json": dict(updated_utc=now, threshold_pct=FLAG_PCT, curves=flags),
            **({"partner_revenue.json": rev} if rev else {})}


def git(feed: Path, *args, check=True):
    return subprocess.run(["git", "-C", str(feed), *args], capture_output=True, text=True, check=check)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--feed-dir", default=str(ROOT.parent / "dbc_pulse_feed")); ap.add_argument("--no-push", action="store_true"); a = ap.parse_args()
    feed = Path(a.feed_dir); out = feed / "feed"; out.mkdir(parents=True, exist_ok=True)
    files = build(); changed = False
    for name, obj in files.items():
        p = out / name; new = json.dumps(obj, ensure_ascii=False, indent=1)
        old = p.read_text(encoding="utf-8") if p.exists() else None
        # ignore the timestamp alone: only rewrite when content other than updated_utc changed, or once an hour
        def strip(t): return None if t is None else "\n".join(l for l in t.splitlines() if '"updated_utc"' not in l)
        if strip(old) != strip(new) or (old and time.time() - p.stat().st_mtime > 3300):
            p.write_text(new, encoding="utf-8"); changed = True
    if not changed: print("feed unchanged"); return
    git(feed, "add", "feed")
    if not git(feed, "diff", "--cached", "--quiet", check=False).returncode: print("nothing staged"); return
    git(feed, "commit", "-q", "-m", f"feed {time.strftime('%Y-%m-%d %H:%M', time.gmtime())} UTC")
    if a.no_push: print("committed (no push)"); return
    r = git(feed, "push", "-q", "origin", "feed", check=False)
    if r.returncode:
        git(feed, "pull", "-q", "--rebase", "origin", "feed", check=False); r = git(feed, "push", "-q", "origin", "feed", check=False)
    print("pushed" if not r.returncode else f"push failed: {r.stderr.strip()[:200]}")


if __name__ == "__main__": main()
