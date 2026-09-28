"""Solami API key entry window — paste button, masked field, save + live connection check. The key is never printed.

Writes SOLAMI_API_KEY and DBC_PROVIDER=solami to dbc_pulse/.env (git-ignored), then calls Solami RPC getSlot to confirm.
"""
from __future__ import annotations
import tkinter as tk
from pathlib import Path
import requests

ENV = Path(__file__).resolve().parent.parent / ".env"


def save_key(key: str) -> None:
    lines = ENV.read_text(encoding="utf-8").splitlines() if ENV.exists() else []
    lines = [l for l in lines if not l.startswith("SOLAMI_API_KEY=") and not l.startswith("DBC_PROVIDER=")]
    lines += [f"SOLAMI_API_KEY={key}", "DBC_PROVIDER=solami"]
    ENV.write_text("\n".join(lines) + "\n", encoding="utf-8")


def check(key: str) -> tuple[bool, str]:
    try:
        r = requests.post(f"https://rpc.solami.dev/sol?api_key={key}", json={"jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []}, timeout=15)
        if r.status_code == 200 and "result" in (r.json() or {}):
            return True, f"Solami 연결 확인 (현재 슬롯 {r.json()['result']:,})"
        if r.status_code in (401, 403):
            return False, "키가 거부됐습니다(401/403). 대시보드에서 키를 다시 복사해 주십시오."
        return False, f"응답 코드 {r.status_code} — 키는 저장했습니다. 잠시 뒤 다시 확인합니다."
    except Exception as e:
        return False, f"연결 오류 {type(e).__name__} — 키는 저장했습니다."


def main():
    root = tk.Tk(); root.title("Solami API 키 입력 — dbc-pulse"); root.geometry("560x230"); root.configure(bg="#fafaf8")
    tk.Label(root, text="Solami 대시보드에서 복사한 API 키를 붙여넣고 [저장]을 누르세요.", bg="#fafaf8", font=("Malgun Gothic", 11)).pack(pady=(16, 6))
    var = tk.StringVar(); ent = tk.Entry(root, textvariable=var, show="•", width=52, font=("Consolas", 11)); ent.pack(pady=4); ent.focus_set()
    msg = tk.StringVar(value=""); status = tk.Label(root, textvariable=msg, bg="#fafaf8", fg="#1a7f37", font=("Malgun Gothic", 10), wraplength=520); status.pack(pady=6)

    def paste():
        try: var.set(root.clipboard_get().strip())
        except Exception: msg.set("클립보드가 비어 있습니다. 키를 먼저 복사해 주십시오.")
        else: msg.set(f"붙여넣음 ({len(var.get())}자). [저장]을 누르세요.")

    def do_save():
        key = var.get().strip()
        if len(key) < 10: status.configure(fg="#c0392b"); msg.set("키가 비어 있거나 너무 짧습니다."); return
        save_key(key); ok, text = check(key)
        status.configure(fg="#1a7f37" if ok else "#c0392b"); msg.set(f"저장 완료({len(key)}자). {text}" + (" 이 창을 닫으셔도 됩니다." if ok else ""))

    row = tk.Frame(root, bg="#fafaf8"); row.pack(pady=6)
    tk.Button(row, text="붙여넣기", width=12, command=paste).pack(side="left", padx=6)
    tk.Button(row, text="저장", width=12, command=do_save).pack(side="left", padx=6)
    root.bind("<Return>", lambda e: do_save())
    root.mainloop()


if __name__ == "__main__":
    main()
