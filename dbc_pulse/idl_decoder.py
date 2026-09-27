"""Minimal Anchor-IDL borsh decoder for event payloads (no external deps besides base58).

Anchor programs that use `emit_cpi!` emit events as a self-CPI whose instruction data is:
    [8-byte anchor event tag] [8-byte event discriminator] [borsh-encoded event struct]
The event tag is sha256("anchor:event")[:8] = e445a52e51cb9a1d.
"""
from __future__ import annotations
import json, struct
from pathlib import Path
from typing import Any
import base58

ANCHOR_EVENT_TAG = bytes.fromhex("e445a52e51cb9a1d")


class IdlDecoder:
    def __init__(self, idl_path: str | Path):
        self.idl = json.loads(Path(idl_path).read_text(encoding="utf-8"))
        self.types = {t["name"]: t["type"] for t in self.idl.get("types", [])}
        self.events = {bytes(e["discriminator"]): e["name"] for e in self.idl.get("events", [])}
        self.program_id = self.idl.get("address")

    # ---- primitives ----
    def _read(self, ty: Any, buf: bytes, pos: int) -> tuple[Any, int]:
        if isinstance(ty, str):
            if ty == "bool": return bool(buf[pos]), pos + 1
            if ty == "u8": return buf[pos], pos + 1
            if ty == "i8": return struct.unpack_from("<b", buf, pos)[0], pos + 1
            if ty == "u16": return struct.unpack_from("<H", buf, pos)[0], pos + 2
            if ty == "i16": return struct.unpack_from("<h", buf, pos)[0], pos + 2
            if ty == "u32": return struct.unpack_from("<I", buf, pos)[0], pos + 4
            if ty == "i32": return struct.unpack_from("<i", buf, pos)[0], pos + 4
            if ty == "u64": return struct.unpack_from("<Q", buf, pos)[0], pos + 8
            if ty == "i64": return struct.unpack_from("<q", buf, pos)[0], pos + 8
            if ty == "u128": return int.from_bytes(buf[pos:pos + 16], "little"), pos + 16
            if ty == "i128": return int.from_bytes(buf[pos:pos + 16], "little", signed=True), pos + 16
            if ty == "pubkey": return base58.b58encode(buf[pos:pos + 32]).decode(), pos + 32
            if ty == "string":
                n = struct.unpack_from("<I", buf, pos)[0]; return buf[pos + 4:pos + 4 + n].decode("utf-8", "replace"), pos + 4 + n
            if ty == "bytes":
                n = struct.unpack_from("<I", buf, pos)[0]; return buf[pos + 4:pos + 4 + n].hex(), pos + 4 + n
            raise ValueError(f"unknown primitive {ty}")
        if "vec" in ty:
            n = struct.unpack_from("<I", buf, pos)[0]; pos += 4; out = []
            for _ in range(n):
                v, pos = self._read(ty["vec"], buf, pos); out.append(v)
            return out, pos
        if "option" in ty:
            flag = buf[pos]; pos += 1
            if not flag: return None, pos
            return self._read(ty["option"], buf, pos)
        if "array" in ty:
            inner, n = ty["array"]; out = []
            for _ in range(n):
                v, pos = self._read(inner, buf, pos); out.append(v)
            return out, pos
        if "defined" in ty:
            name = ty["defined"]["name"] if isinstance(ty["defined"], dict) else ty["defined"]
            return self._read_defined(name, buf, pos)
        raise ValueError(f"unsupported type {ty}")

    def _read_defined(self, name: str, buf: bytes, pos: int) -> tuple[Any, int]:
        t = self.types[name]
        if t["kind"] == "struct":
            out = {}
            for f in t.get("fields", []):
                v, pos = self._read(f["type"], buf, pos); out[f["name"]] = v
            return out, pos
        if t["kind"] == "enum":
            idx = buf[pos]; pos += 1; variant = t["variants"][idx]
            fields = variant.get("fields")
            if not fields: return variant["name"], pos
            vals = []
            for f in fields:
                v, pos = self._read(f["type"] if isinstance(f, dict) and "type" in f else f, buf, pos); vals.append(v)
            return {variant["name"]: vals}, pos
        raise ValueError(f"unsupported defined kind {t['kind']}")

    # ---- events ----
    def decode_event_cpi(self, data: bytes) -> dict | None:
        """Decode a self-CPI event instruction. Returns {'name':..., 'data':...} or None if not an event."""
        if len(data) < 16 or data[:8] != ANCHOR_EVENT_TAG: return None
        name = self.events.get(bytes(data[8:16]))
        if not name: return {"name": "unknown", "disc": data[8:16].hex(), "data": None}
        payload, _ = self._read_defined(name, data, 16)
        return {"name": name, "data": payload}
