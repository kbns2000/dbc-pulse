"""Unit tests for the LP realized-yield math (no network)."""
import math, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dbc_pulse.damm import unit_reserves, unit_value, orient, LpTracker, Q64, decode_migrations, MIG_V2_DISC
import base58

SPMIN, SPMAX = 1e-6, 1e6   # wide range


def _pool_state(price: float, liquidity: float, fee_a: float = 0.0, fee_b: float = 0.0, quote_is_a: bool = False, ta="A", tb="B"):
    """Fabricate a decoded DAMM v2 Pool dict consistent with the identities (reserves = L * unit reserves)."""
    sp = math.sqrt(price); a, b = unit_reserves(sp, SPMIN, SPMAX)
    return dict(token_a_mint=ta, token_b_mint=tb, liquidity=int(liquidity * Q64), sqrt_price=int(sp * Q64), sqrt_min_price=int(SPMIN * Q64), sqrt_max_price=int(SPMAX * Q64),
                token_a_amount=int(a * liquidity), token_b_amount=int(b * liquidity), pool_status=0, collect_fee_mode=0,
                fee_a_per_liquidity=int(fee_a * Q64), fee_b_per_liquidity=int(fee_b * Q64), total_lp_a_fee=0, total_lp_b_fee=0, protocol_a_fee=0, protocol_b_fee=0,
                permanent_lock_liquidity=0, total_position=2, activation_point=0)


def test_unit_reserves_identity_and_value():
    a, b = unit_reserves(1.0, SPMIN, SPMAX)
    assert math.isclose(a, 1.0 - 1.0 / SPMAX) and math.isclose(b, 1.0 - SPMIN)
    assert math.isclose(unit_value(1.0, SPMIN, SPMAX), a * 1.0 + b)
    # clamped outside the range
    a_lo, b_lo = unit_reserves(1e-9, SPMIN, SPMAX); assert math.isclose(b_lo, 0.0, abs_tol=1e-12)


def test_orient_mirrors_when_quote_is_token_a():
    st = _pool_state(4.0, 1000.0, fee_a=1.0, fee_b=2.0)
    o = orient(st, quote_mint="B"); assert not o["quote_is_a"] and math.isclose(o["sp"] ** 2, 4.0, rel_tol=1e-9) and math.isclose(o["fee_quote_g"], 2.0)
    m = orient(st, quote_mint="A"); assert m["quote_is_a"] and math.isclose(m["sp"] ** 2, 0.25, rel_tol=1e-9) and math.isclose(m["fee_quote_g"], 1.0)
    assert math.isclose(m["sp_min"], 1.0 / o["sp_max"]) and math.isclose(m["sp_max"], 1.0 / o["sp_min"])


def test_lvr_is_hold_minus_stay_and_nonnegative():
    tr = LpTracker("P", {"quote_mint": "B"})
    tr.update(_pool_state(1.0, 1000.0), 0.0)
    s = tr.update(_pool_state(1.21, 1000.0), 10.0)   # +21% price move, no fees
    sp0, sp1 = 1.0, math.sqrt(1.21); a0, b0 = unit_reserves(sp0, SPMIN, SPMAX)
    expected = a0 * 1.21 + b0 - unit_value(sp1, SPMIN, SPMAX)
    assert expected > 0 and math.isclose(tr.lvr_q, expected, rel_tol=1e-9)
    assert tr.fees_q == 0.0 and s["net_pct"] < 0 and s["reserve_check"] and math.isclose(s["reserve_check"], 1.0, abs_tol=1e-3)
    # a round trip back to the start price still leaves LVR > 0 (convexity), never negative
    tr.update(_pool_state(1.0, 1000.0), 20.0); assert tr.lvr_q > expected


def test_fees_accumulate_in_quote_and_base_valued_conservatively():
    tr = LpTracker("P", {"quote_mint": "B"})
    tr.update(_pool_state(2.0, 1000.0, fee_a=0.0, fee_b=0.0), 0.0)
    tr.update(_pool_state(2.0, 1000.0, fee_a=1.0, fee_b=3.0), 10.0)   # flat price: 1 base-fee valued at 2.0 + 3 quote = 5
    assert math.isclose(tr.fees_q, 5.0, rel_tol=1e-9) and tr.lvr_q == 0.0
    tr.update(_pool_state(200.0, 1000.0, fee_a=2.0, fee_b=3.0), 20.0)   # 100x jump: base fee valued at the lower price (2.0), and flagged
    assert math.isclose(tr.fees_q, 7.0, rel_tol=1e-9) and tr.jumps == 1 and tr.summary()["clean"] is False


def test_liquidity_drop_flag_and_sigma():
    tr = LpTracker("P", {"quote_mint": "B"})
    tr.update(_pool_state(1.0, 1000.0), 0.0); tr.update(_pool_state(1.0, 100.0), 10.0)
    assert tr.liq_drops == 1 and tr.summary()["clean"] is False and tr.summary()["sigma_ann"] is None


def test_decode_migrations_from_instruction_accounts():
    keys = [f"K{i}" for i in range(30)]; keys[0] = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"
    data = base58.b58encode(MIG_V2_DISC + b"\x00" * 4).decode()
    ix = {"programIdIndex": 0, "accounts": list(range(1, 26)), "data": data}
    tx = {"slot": 1, "blockTime": 2, "meta": {"err": None, "innerInstructions": []}, "transaction": {"message": {"accountKeys": keys, "instructions": [ix]}}}
    m = decode_migrations(tx, "sig")
    assert len(m) == 1 and m[0]["version"] == "damm_v2" and m[0]["virtual_pool"] == "K1" and m[0]["damm_pool"] == "K5" and m[0]["base_mint"] == "K14" and m[0]["quote_mint"] == "K15"
    assert decode_migrations(None, "sig") == [] and decode_migrations({**tx, "meta": {"err": {"x": 1}}}, "sig") == []
