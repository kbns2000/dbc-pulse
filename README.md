# dbc-pulse

Real-time data stream and analytics for **Meteora Dynamic Bonding Curve (DBC)** launches on Solana — built for launchpads, trading terminals and researchers.

**What it does**
- Streams DBC program events (`EvtInitializePool`, `EvtSwap2`, `EvtCurveComplete`, fee claims, migration) decoded from event-CPI, straight from the chain.
- Tracks every virtual pool: curve progress, quote reserve vs migration threshold, buy/sell pressure, unique wallets, time-to-graduation estimate.
- Follows graduated pools into **DAMM v2** and measures what liquidity providers actually earn: fees minus loss-versus-rebalancing (LVR), measured from on-chain state rather than displayed APR.
- Exposes everything as JSONL / WebSocket / REST so a launchpad or terminal can plug it in.

**Why**: launchpad builders configure curves and fees blind. dbc-pulse shows, per config, how launches actually behave and what the post-migration liquidity is worth.

Program: `dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN` (mainnet & devnet). Built during Colosseum Crypto World's Fair 2026 for the Meteora DBC side track.

## Architecture (v2)
The program does ~25 tx/s in busy minutes, so per-transaction fetching is not viable on a small RPC plan (2M calls/day).
dbc-pulse instead uses three cheap channels (`dbc_pulse/stream.py`):
1. **logsSubscribe** on the program — every transaction is classified from its `Instruction:` logs; swaps are counted, lifecycle transactions (initialize, migration, withdraw, claims) are the only ones fetched and fully decoded via event-CPI.
2. **Account polling** — tracked `VirtualPool` accounts are read with `getMultipleAccounts` (100 per call) every 3s and decoded from the IDL: reserves, sqrt price, fees, migration flags. Reserve deltas between polls give buy/sell pressure without per-swap fetches; `PoolConfig` (cached) gives the migration threshold.
3. **Sampling** — 1 in N swap transactions is fetched for wallet-level stats.
4. **DAMM v2 follow-through** — `migration_damm_v2` transactions are decoded from their account list (no DBC event carries the DAMM pool), and the resulting DAMM v2 `Pool` accounts are polled every 10s. Per unit of liquidity the tracker accumulates fees (from `fee_*_per_liquidity` growth) and LVR (hold-the-previous-composition vs. stay-in-pool, summed over polls), reports net realized yield, annualized σ and the σ²/8 theory line (`dbc_pulse/damm.py`).
Budget: one WebSocket plus roughly 1–2 HTTP calls/s regardless of chain volume. A heartbeat file makes the stream single-instance.

## Status
- [x] Event collector (RPC signatures → transactions → event-CPI decode) — `python -m dbc_pulse.collector --backfill N` (batch) / v2 live stream `python -m dbc_pulse.stream`
- [x] Pool state tracker (curve progress, thresholds, buy/sell pressure, per-config completion & instant-launch share) — `python -m dbc_pulse.pool_tracker`
- [x] DAMM v2 migration mapping (`migration_damm_v2` accounts → DAMM pool) + LP realized-yield tracker per unit liquidity: fees − LVR (interval rebalancing benchmark), σ and σ²/8 theory comparison, reserve-identity self-check — `dbc_pulse/damm.py`, live in the stream (`data/migration_*.jsonl`, `data/damm_*.jsonl`, `data/damm_live.json`)
- [ ] Stream API (WebSocket/REST) + dashboard
- [ ] Docs, tests, pitch

## Quick start
```bash
python -m venv .venv && .venv/Scripts/activate  # Windows
pip install -r requirements.txt
cp .env.example .env   # set SOLANA_RPC_URL
python -m dbc_pulse.collector --since 1h
```

## License
MIT
