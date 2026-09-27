"""IDL decoder round-trip: encode an event with borsh by hand, decode it through the IDL, compare."""
import json, struct, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import base58
from dbc_pulse.idl_decoder import IdlDecoder, ANCHOR_EVENT_TAG

ROOT = Path(__file__).resolve().parent.parent; IDL = ROOT / "dbc_pulse" / "idl" / "dbc.json"


def _enc(ty, v) -> bytes:
    if ty == "u8": return struct.pack("<B", v)
    if ty == "u16": return struct.pack("<H", v)
    if ty == "u32": return struct.pack("<I", v)
    if ty == "u64": return struct.pack("<Q", v)
    if ty == "i64": return struct.pack("<q", v)
    if ty == "u128": return int(v).to_bytes(16, "little")
    if ty == "bool": return b"\x01" if v else b"\x00"
    if ty == "pubkey": return base58.b58decode(v)
    if isinstance(ty, dict) and "array" in ty:
        inner, n = ty["array"]; return b"".join(_enc(inner, x) for x in v)
    raise ValueError(ty)


def _sample(ty):
    if ty in ("u8",): return 7
    if ty in ("u16",): return 300
    if ty in ("u32",): return 70000
    if ty in ("u64",): return 2 ** 40 + 5
    if ty in ("i64",): return -12345
    if ty in ("u128",): return 2 ** 100 + 9
    if ty == "bool": return True
    if ty == "pubkey": return "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"
    if isinstance(ty, dict) and "array" in ty:
        inner, n = ty["array"]; return [_sample(inner) for _ in range(n)]
    raise ValueError(ty)


def test_event_cpi_roundtrip_curve_complete():
    dec = IdlDecoder(IDL); idl = json.loads(IDL.read_text(encoding="utf-8"))
    ev = next(e for e in idl["events"] if e["name"] == "EvtCurveComplete"); ty = next(t for t in idl["types"] if t["name"] == "EvtCurveComplete")["type"]
    fields = ty["fields"]; vals = {f["name"]: _sample(f["type"]) for f in fields}
    payload = b"".join(_enc(f["type"], vals[f["name"]]) for f in fields)
    out = dec.decode_event_cpi(ANCHOR_EVENT_TAG + bytes(ev["discriminator"]) + payload)
    assert out["name"] == "EvtCurveComplete" and out["data"] == vals


def test_non_event_and_unknown_discriminator():
    dec = IdlDecoder(IDL)
    assert dec.decode_event_cpi(b"\x00" * 20) is None
    u = dec.decode_event_cpi(ANCHOR_EVENT_TAG + b"\xff" * 8 + b"\x00" * 8); assert u["name"] == "unknown"


def test_pool_state_layout_matches_expected_size():
    """VirtualPool accounts are 424 bytes (8 disc + 416 PoolState); decoding a zero buffer must consume exactly that."""
    dec = IdlDecoder(IDL); buf = bytes(8 + 416)
    st, pos = dec._read_defined("PoolState", buf, 8)
    assert pos == 424 and "quote_reserve" in st and "sqrt_price" in st and "is_migrated" in st
