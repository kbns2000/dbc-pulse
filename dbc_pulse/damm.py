"""DAMM v2 side of dbc-pulse: map DBC migrations to their DAMM v2 pools and measure LP realized yield (fees − LVR).

Migration mapping
  No DBC event carries the DAMM v2 pool address. The DBC instruction `migration_damm_v2`
  (discriminator 9ca9e66735e45040) lists it in its accounts:
    [0] virtual_pool  [2] config  [4] pool (DAMM v2)  [7] first_position  [10] second_position  [13] base_mint  [14] quote_mint
  `decode_migrations(tx)` scans top-level and inner instructions of a fetched transaction for that discriminator
  (and the legacy `migrate_meteora_damm`, disc 1b013016b43f76d9, [4] pool) and returns one record per migration.

LP realized yield (per unit of liquidity, quote as numéraire)
  DAMM v2 `Pool` account (disc f19a6d0411b16dbc): sqrt_price / sqrt_min_price / sqrt_max_price are Q64.64, `liquidity` is
  scaled by 2^64 (cp-amm RESOLUTION), reserves follow the concentrated-liquidity identities
    a(P) = L·(1/√P − 1/√Pmax)      b(P) = L·(√P − √Pmin)      V(P) = a(P)·P + b(P)   (value in token b)
  Over one polling interval [t0, t1]:
    LVR  = a(P0)·P1 + b(P0) − V(P1)      (hold the t0 composition vs. stay in the pool; ≥ 0 by convexity)
    fees = Δfee_a_per_liquidity·P1 + Δfee_b_per_liquidity   (both stored as u256 with 2^128 scale → per real L: /2^64)
  Summing short intervals approximates the continuous LVR of Milionis–Moallemi–Roughgarden–Zhang (2022).
  If the quote token is token a, roles are mirrored (P' = 1/P, bounds swapped) so that "b" is always the quote.
  Sanity check exposed in every snapshot: reserve_check = token_a_amount / (L·(1/√P − 1/√Pmax)) ≈ 1.0 means the
  account's reserve fields exclude accrued fees and the scaling above is right.
"""
from __future__ import annotations
import base64, json, math, time
from pathlib import Path
import base58
from .idl_decoder import IdlDecoder

ROOT = Path(__file__).resolve().parent.parent; DATA = ROOT / "data"
DBC_PROGRAM = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"; DAMM_V2_PROGRAM = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"
MIG_V2_DISC = bytes([156, 169, 230, 103, 53, 228, 80, 64]); MIG_V1_DISC = bytes([27, 1, 48, 22, 180, 63, 118, 217])
POOL_DISC = bytes([241, 154, 109, 4, 17, 177, 109, 188])
Q64 = float(2 ** 64); SEC_YEAR = 365.25 * 86400


def _keys(tx: dict) -> list[str]:
    msg = tx["transaction"]["message"]; la = (tx.get("meta") or {}).get("loadedAddresses") or {}
    return list(msg["accountKeys"]) + list(la.get("writable", [])) + list(la.get("readonly", []))


def decode_migrations(tx: dict, sig: str) -> list[dict]:
    """Return [{virtual_pool, damm_pool, version, config, base_mint, quote_mint, first_position, second_position, slot, block_time, signature}]."""
    out = []
    if not tx: return out
    meta = tx.get("meta") or {}
    if meta.get("err"): return out
    keys = _keys(tx); msg = tx["transaction"]["message"]
    ixs = [(ix, "top") for ix in msg.get("instructions", [])]
    for grp in meta.get("innerInstructions") or []: ixs += [(ix, f"inner{grp['index']}") for ix in grp.get("instructions", [])]
    for ix, where in ixs:
        pid = keys[ix["programIdIndex"]] if ix.get("programIdIndex", 1 << 30) < len(keys) else None
        if pid != DBC_PROGRAM: continue
        try: data = base58.b58decode(ix.get("data") or "")
        except Exception: continue
        acc = [keys[i] for i in ix.get("accounts", []) if i < len(keys)]
        if data[:8] == MIG_V2_DISC and len(acc) >= 15:
            out.append(dict(slot=tx["slot"], block_time=tx.get("blockTime"), signature=sig, where=where, version="damm_v2", virtual_pool=acc[0], config=acc[2], damm_pool=acc[4],
                            first_position=acc[7], second_position=acc[10], base_mint=acc[13], quote_mint=acc[14]))
        elif data[:8] == MIG_V1_DISC and len(acc) >= 9:
            out.append(dict(slot=tx["slot"], block_time=tx.get("blockTime"), signature=sig, where=where, version="damm_v1", virtual_pool=acc[0], config=acc[2], damm_pool=acc[4],
                            base_mint=acc[7], quote_mint=acc[8]))
    return out


class DammDecoder:
    def __init__(self, idl_path: str | Path):
        self.dec = IdlDecoder(idl_path)

    def decode_pool(self, b64: str) -> dict | None:
        raw = base64.b64decode(b64)
        if raw[:8] != POOL_DISC: return None
        st, _ = self.dec._read_defined("Pool", raw, 8); m = st.get("metrics") or {}
        return dict(token_a_mint=st["token_a_mint"], token_b_mint=st["token_b_mint"], liquidity=st["liquidity"], sqrt_price=st["sqrt_price"], sqrt_min_price=st["sqrt_min_price"],
                    sqrt_max_price=st["sqrt_max_price"], token_a_amount=st["token_a_amount"], token_b_amount=st["token_b_amount"], pool_status=st["pool_status"],
                    collect_fee_mode=st["collect_fee_mode"], fee_a_per_liquidity=int.from_bytes(bytes(st["fee_a_per_liquidity"]), "little"), fee_b_per_liquidity=int.from_bytes(bytes(st["fee_b_per_liquidity"]), "little"),
                    total_lp_a_fee=m.get("total_lp_a_fee"), total_lp_b_fee=m.get("total_lp_b_fee"), protocol_a_fee=st["protocol_a_fee"], protocol_b_fee=st["protocol_b_fee"],
                    permanent_lock_liquidity=st["permanent_lock_liquidity"], total_position=m.get("total_position"), activation_point=st["activation_point"])


def orient(st: dict, quote_mint: str | None) -> dict:
    """Express the pool with the quote token as 'b'. Returns floats: sp (√P), sp_min, sp_max, L (real), fee_base_g, fee_quote_g, res_base, res_quote, quote_is_a."""
    sp = st["sqrt_price"] / Q64; smin = st["sqrt_min_price"] / Q64; smax = st["sqrt_max_price"] / Q64; L = st["liquidity"] / Q64
    fa = st["fee_a_per_liquidity"] / Q64; fb = st["fee_b_per_liquidity"] / Q64   # per real unit of L, in raw token units
    quote_is_a = quote_mint is not None and st["token_a_mint"] == quote_mint
    if quote_is_a:   # mirror: base := b, quote := a, P' = 1/P
        sp, smin, smax = 1.0 / sp, 1.0 / smax, 1.0 / smin; fa, fb = fb, fa; ra, rb = st["token_b_amount"], st["token_a_amount"]
    else: ra, rb = st["token_a_amount"], st["token_b_amount"]
    return dict(sp=sp, sp_min=smin, sp_max=smax, L=L, fee_base_g=fa, fee_quote_g=fb, res_base=ra, res_quote=rb, quote_is_a=quote_is_a)


def unit_reserves(sp: float, sp_min: float, sp_max: float) -> tuple[float, float]:
    """Base and quote amounts per unit of liquidity at √P (clamped to the range)."""
    s = min(max(sp, sp_min), sp_max)
    return (1.0 / s - 1.0 / sp_max), (s - sp_min)


def unit_value(sp: float, sp_min: float, sp_max: float) -> float:
    a, b = unit_reserves(sp, sp_min, sp_max); return a * sp * sp + b


class LpTracker:
    """Accumulates fees and LVR per unit of liquidity for one DAMM v2 pool across polls."""
    def __init__(self, damm_pool: str, meta: dict):
        self.pool = damm_pool; self.meta = meta; self.prev: dict | None = None; self.t0 = None; self.v0 = None; self.p0 = None
        self.fees_q = 0.0; self.lvr_q = 0.0; self.sum_r2 = 0.0; self.n = 0; self.last_price = None; self.last_t = None; self.reserve_check = None
        self.jumps = 0; self.max_abs_log_move = 0.0; self.liq_drops = 0   # anomaly flags: >10x price move or >50% liquidity drop inside one poll interval

    def update(self, st: dict, t: float) -> dict:
        o = orient(st, self.meta.get("quote_mint")); P = o["sp"] ** 2
        if o["L"] > 0:
            ua, _ = unit_reserves(o["sp"], o["sp_min"], o["sp_max"]); implied = ua * o["L"]
            self.reserve_check = round(o["res_base"] / implied, 4) if implied > 0 else None
        snap = dict(t=int(t), pool=self.pool, price=P, L=o["L"], fee_quote_g=o["fee_quote_g"], fee_base_g=o["fee_base_g"], reserve_check=self.reserve_check, status=st["pool_status"])
        if self.prev is None:
            self.t0 = t; self.p0 = P; self.v0 = unit_value(o["sp"], o["sp_min"], o["sp_max"])
        else:
            pv = self.prev
            a0, b0 = unit_reserves(pv["sp"], pv["sp_min"], pv["sp_max"]); hold = a0 * P + b0; stay = unit_value(o["sp"], o["sp_min"], o["sp_max"])
            self.lvr_q += max(hold - stay, 0.0)
            P0 = pv["sp"] ** 2
            # base-denominated fees are valued at the lower of the interval's end-points: a fee earned in a token whose
            # price spiked 1000x inside one interval is not worth the spike price (thin pools, liquidity pulls).
            self.fees_q += max(o["fee_base_g"] - pv["fee_base_g"], 0.0) * min(P, P0) + max(o["fee_quote_g"] - pv["fee_quote_g"], 0.0)
            if pv["sp"] > 0 and o["sp"] > 0:
                r = 2.0 * math.log(o["sp"] / pv["sp"]); self.sum_r2 += r * r; self.max_abs_log_move = max(self.max_abs_log_move, abs(r))
                if abs(r) > math.log(10.0): self.jumps += 1
            if pv["L"] > 0 and o["L"] < 0.5 * pv["L"]: self.liq_drops += 1
            self.n += 1
        self.prev = o; self.last_price = P; self.last_t = t
        snap.update(self.summary()); return snap

    def summary(self) -> dict:
        if self.v0 is None or self.v0 <= 0: return {}
        hrs = ((self.last_t or self.t0) - self.t0) / 3600.0; yrs = hrs / (24 * 365.25)
        sigma_ann = math.sqrt(self.sum_r2 / yrs) if yrs > 0 and self.sum_r2 > 0 else None
        return dict(hours=round(hrs, 3), price_change_pct=round(100 * (self.last_price / self.p0 - 1), 3) if self.p0 else None, fees_pct=round(100 * self.fees_q / self.v0, 4),
                    lvr_pct=round(100 * self.lvr_q / self.v0, 4), net_pct=round(100 * (self.fees_q - self.lvr_q) / self.v0, 4), sigma_ann=round(sigma_ann, 3) if sigma_ann else None,
                    theory_lvr_pct=round(100 * (sigma_ann ** 2 / 8) * yrs, 4) if sigma_ann else None,   # CPMM full-range approximation σ²/8 per year × elapsed
                    fees_apr_pct=round(100 * self.fees_q / self.v0 / yrs, 2) if yrs > 0 else None, lvr_apr_pct=round(100 * self.lvr_q / self.v0 / yrs, 2) if yrs > 0 else None, polls=self.n,
                    jumps=self.jumps, liq_drops=self.liq_drops, max_move_x=round(math.exp(self.max_abs_log_move), 2), clean=(self.jumps == 0 and self.liq_drops == 0))
