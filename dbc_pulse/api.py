"""dbc-pulse API — serves the live stream files over HTTP (REST + dashboard) and pushes snapshots over WebSocket.

It never touches the RPC: it only reads what `dbc_pulse.stream` writes under data/, so it can run on the same box
or anywhere the data directory is synced.

Usage: python -m dbc_pulse.api            (env DBC_API_HOST=127.0.0.1 DBC_API_PORT=8790 DBC_WS_PORT=8791 DBC_PUSH_S=5)
REST (JSON):
  GET /health                       stream heartbeat + api uptime
  GET /pools?min_progress=0&limit=100   tracked DBC pools, sorted by curve progress (from pools_live.json)
  GET /configs                      per-config live counts (pools tracked, curve complete, migrated) + tracker stats if present
  GET /damm?limit=100               DAMM v2 pools with LP realized yield (fees, LVR, net, σ, σ²/8 theory)
  GET /events?n=100                 last n decoded lifecycle events (today's lifecycle_*.jsonl)
  GET /migrations?n=50              last n DBC → DAMM v2 migrations
  GET /                             dashboard (static/index.html)
WS  ws://host:8791/                 every DBC_PUSH_S seconds: {"type":"snapshot","health":…,"pools":[top 50],"damm":[top 50]}
Single instance: binding the ports fails if another API is running.
"""
from __future__ import annotations
import asyncio, glob, json, os, threading, time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent.parent; DATA = ROOT / "data"; STATIC = Path(__file__).resolve().parent / "static"
HOST = os.environ.get("DBC_API_HOST", "127.0.0.1"); PORT = int(os.environ.get("DBC_API_PORT", "8790")); WS_PORT = int(os.environ.get("DBC_WS_PORT", "8791")); PUSH_S = float(os.environ.get("DBC_PUSH_S", "5"))
T_START = time.time()


def _read(name: str, default):
    try: return json.loads((DATA / name).read_text(encoding="utf-8"))
    except Exception: return default


def _tail_jsonl(prefix: str, n: int) -> list[dict]:
    files = sorted(glob.glob(str(DATA / f"{prefix}_*.jsonl")))[-2:]; rows: list[dict] = []
    for f in files:
        try:
            with open(f, encoding="utf-8") as fh: lines = fh.readlines()
        except Exception: continue
        for line in lines[-n:]:
            try: rows.append(json.loads(line))
            except Exception: pass
    return rows[-n:]


def health() -> dict:
    hb = _read("stream_heartbeat.json", {}); age = time.time() - hb.get("ts", 0) if hb else None
    return dict(ws_port=WS_PORT, stream_alive=bool(hb) and age < 120, heartbeat_age_s=round(age, 1) if age is not None else None, stream=hb, api_uptime_s=round(time.time() - T_START), ts=time.time())


def pools(min_progress: float = 0.0, limit: int = 100) -> list[dict]:
    live = _read("pools_live.json", {}); out = []
    for k, p in live.items():
        if p.get("quote_reserve") is None: continue
        prog = p.get("progress_pct") or 0.0
        if prog < min_progress: continue
        out.append(dict(pool=k, config=p.get("config"), base_mint=p.get("base_mint"), creator=p.get("creator"), progress_pct=prog, quote_mint=p.get("quote_mint"), quote_reserve=p.get("quote_reserve"), base_reserve=p.get("base_reserve"),
                        buy_quote=p.get("quote_delta_buy", 0), sell_quote=p.get("quote_delta_sell", 0), trading_quote_fee=p.get("trading_quote_fee"), is_curve_complete=p.get("is_curve_complete"), is_migrated=p.get("is_migrated"),
                        damm_pool=p.get("damm_pool"), first_seen=p.get("first_seen"), last_activity=p.get("last_activity"), source=p.get("source")))
    out.sort(key=lambda r: -r["progress_pct"]); return out[:limit]


def configs() -> list[dict]:
    live = _read("pools_live.json", {}); agg = defaultdict(lambda: dict(config=None, pools=0, curve_complete=0, migrated=0, quote_reserve=0.0, buy_quote=0.0, sell_quote=0.0))
    for k, p in live.items():
        c = p.get("config") or "unknown"; a = agg[c]; a["config"] = c; a["pools"] += 1
        if p.get("is_curve_complete"): a["curve_complete"] += 1
        if p.get("is_migrated"): a["migrated"] += 1
        a["quote_reserve"] += p.get("quote_reserve") or 0; a["buy_quote"] += p.get("quote_delta_buy", 0) or 0; a["sell_quote"] += p.get("quote_delta_sell", 0) or 0
    tracker = _read("configs.json", {})
    out = []
    for c, a in agg.items():
        t = tracker.get(c) or {}
        out.append(dict(**a, launches=t.get("launches"), completion_rate=t.get("completion_rate"), instant_share=t.get("instant_share"), median_minutes_to_complete=t.get("median_minutes_to_complete"), median_swaps=t.get("median_swaps")))
    out.sort(key=lambda r: -r["pools"]); return out


def damm(limit: int = 100) -> list[dict]:
    d = _read("damm_live.json", {}); out = [dict(pool=k, **v) for k, v in d.items()]
    out.sort(key=lambda r: -(r.get("hours") or 0)); return out[:limit]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass   # quiet

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Access-Control-Allow-Origin", "*"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path); q = {k: v[0] for k, v in parse_qs(u.query).items()}; path = u.path.rstrip("/") or "/"
        try:
            if path == "/":
                body = (STATIC / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body); return
            if path == "/health": return self._json(health())
            if path == "/pools": return self._json(pools(float(q.get("min_progress", 0)), int(q.get("limit", 100))))
            if path == "/configs": return self._json(configs())
            if path == "/damm": return self._json(damm(int(q.get("limit", 100))))
            if path == "/events": return self._json(_tail_jsonl("lifecycle", int(q.get("n", 100))))
            if path == "/migrations": return self._json(_tail_jsonl("migration", int(q.get("n", 50))))
            return self._json({"error": "not found", "routes": ["/health", "/pools", "/configs", "/damm", "/events", "/migrations", "/"]}, 404)
        except Exception as e:
            return self._json({"error": f"{type(e).__name__}: {e}"}, 500)


async def ws_server():
    import websockets
    clients: set = set()

    async def handler(ws):
        clients.add(ws)
        try:
            await ws.send(json.dumps({"type": "hello", "push_s": PUSH_S}))
            async for _ in ws: pass   # ignore client messages
        finally: clients.discard(ws)

    async def pusher():
        while True:
            if clients:
                msg = json.dumps({"type": "snapshot", "health": health(), "pools": pools(limit=50), "damm": damm(limit=50)}, ensure_ascii=False)
                await asyncio.gather(*(c.send(msg) for c in list(clients)), return_exceptions=True)
            await asyncio.sleep(PUSH_S)

    async with websockets.serve(handler, HOST, WS_PORT):
        await pusher()


def main():
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)   # raises if the port is taken → single instance
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print(f"dbc-pulse api http://{HOST}:{PORT}/  ws://{HOST}:{WS_PORT}/", flush=True)
    try: asyncio.run(ws_server())
    except OSError as e: print(f"ws port busy ({e}); HTTP only", flush=True); threading.Event().wait()


if __name__ == "__main__": main()
