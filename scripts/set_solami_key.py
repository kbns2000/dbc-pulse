"""Store your Solami API key in dbc_pulse/.env (hidden input, never printed, never committed).

Usage: python scripts/set_solami_key.py      (or double-click the Solami_키_입력.cmd launcher)
Get a key: https://solami.dev/signup  →  dashboard → API keys (a key with the DataApi permission enables Blur).
"""
from __future__ import annotations
import getpass
from pathlib import Path

ENV = Path(__file__).resolve().parent.parent / ".env"


def main():
    key = getpass.getpass("Solami API key (입력은 화면에 보이지 않습니다): ").strip()
    if not key:
        print("입력 없음 — 변경하지 않았습니다."); return
    lines = ENV.read_text(encoding="utf-8").splitlines() if ENV.exists() else []
    lines = [l for l in lines if not l.startswith("SOLAMI_API_KEY=") and not l.startswith("DBC_PROVIDER=")]
    lines += [f"SOLAMI_API_KEY={key}", "DBC_PROVIDER=solami"]
    ENV.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"저장 완료 ({ENV.name}, 키 길이 {len(key)}자). dbc-pulse 스트림을 재시작하면 Solami RPC·WebSocket·Blur 로 전환됩니다.")
    input("엔터를 누르면 창이 닫힙니다.")


if __name__ == "__main__":
    main()
