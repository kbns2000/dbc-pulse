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
- [x] Stream API (REST + WebSocket push) + light dashboard — `python -m dbc_pulse.api` → http://127.0.0.1:8790/ (routes: /health /pools /configs /damm /events /migrations; ws://127.0.0.1:8791/)
- [x] Tests (`python -m pytest -q`: IDL round-trip, PoolState layout, LP math incl. LVR/fees/anomaly flags, migration decoding) + GitHub Actions CI
- [x] Videos — demo: https://youtu.be/cBaDFNhWGKk · pitch: https://youtu.be/E2WX_iJQx0I (Colosseum Crypto World's Fair, Meteora DBC track)

## Early measurements (mainnet, first day)
- Program throughput swings between ~1.4 and ~24 tx/s minute to minute; in 1.4 h: 240 pools touched, 83 launched, 70 curves completed across 123 configs. Some configs graduate 100% of launches in one fill within a minute (bundled), others take a median of 137–406 swaps.
- DAMM v2, first ~1.5 h after migration (clean pools with a price move, n=203): measured LVR / (σ²/8) = 0.92 median (p10 0.75, p90 1.04); fees cover 1.3% of LVR (median); 203/203 pools net negative for LPs; median price −12%. Pools with a >10× price move or a >50% liquidity pull inside one poll (25% of tracked) are flagged and excluded from aggregates.
- Fee ladder from `PoolConfig`: protocol 20% of trading fee → remainder split between partner (config owner) and creator (`creator_trading_fee_percentage`); partner also receives locked/unlocked LP at migration, migration fee (observed 0–99% of the raised quote) and surplus. Across 319 tracked young pools: 129.6 SOL lifetime trading fees, top 3 partner wallets 66.7%, median config 0.001 SOL.
- Who earns from a graduation (`scripts/partner_revenue.py`, 2,023 SOL-quoted DAMM v2 migrations in 16.2 h, read on-chain 2026-09-28): curve-phase trading fees 80.6 SOL (protocol 16.1 · partner 48.8 · creator 15.6); post-graduation LP fees earned by the two migration positions 190.7 SOL, of which the partner share by config % is 14.2 SOL and the creator share 176.5 SOL; migration fees 0.3 SOL to partners and 51.5 SOL to creators. Half of graduated pools (986/2,023) paid their partner nothing; the top three partners took 41%.
- Graduation take (`PoolConfig.migration_fee_percentage` × creator share): of 600 decoded live configs, 521 take 0%, 35 take 20% and 23 take 99% of the raised quote at graduation, all of it to the creator in the 99% group. One such curve raised 53.01 SOL, paid 50.36 SOL to its creator (also the partner wallet) at graduation and left a pool that now holds 0.114 SOL. The dashboard flags ⚑ curves whose config takes ≥20%; 108 of 1,171 live curves carried the flag.

## Running on Solami (RPC + WebSocket + Blur)
Demo on Solami (2:16): https://youtu.be/ntktsXHY6Cs

dbc-pulse can run entirely on [Solami](https://solami.dev) and adds Solami **Blur** (decoded market data) as a second, independent feed next to its own IDL decoder.

| env (dbc_pulse/.env) | effect |
|---|---|
| `SOLAMI_API_KEY` | your key — `python scripts/set_solami_key.py` stores it without echoing |
| `DBC_PROVIDER=solami` | RPC `https://rpc.solami.dev/sol` and WebSocket `wss://ws.solami.dev/ws/sol` for the whole stream |
| `DBC_BLUR=0` | disable the Blur tap (on by default when a key is set; needs the DataApi permission) |

The Blur tap subscribes to `swap`, `liquidity`, `pool_create`, `graduation` and `meme` events on the two Meteora venues, filtered **server-side** through the connect URL (`&type=…&dex=meteora_dbc,meteora_damm2`; a 3,334-pool text-frame filter was ignored in testing, so pools are not filtered that way). Swaps are written to `data/blur_*.jsonl` only for the pools dbc-pulse tracks (tracked set refreshed every 60 s as pools migrate); everything else is counted. Health goes to the heartbeat / `data/blur_health.json`:
- liquidity adds/removes per DAMM v2 pool with the provider wallet (Blur's `provider` field) and USD value (liquidity movement in/out),
- event lag (receive time − block time) and events per type (stream health),
- reserve agreement: Blur's quote reserve vs our polled VirtualPool state for the same pool (decoder cross-check). Blur reports the quote vault balance (`quote_reserve` + unclaimed protocol/partner/creator quote fees), so that is what we compare. A pool is compared only after 8 s with no newer Blur swap, once our read (at `confirmed`) is at or after the swap's slot, and only when both sides use the same quote mint (on a few curves Blur's 'quote' is the other token). Measured 2026-09-28 on mainnet over 153 comparisons: 128 identical (84%), 91% within 2%. Misses cluster in a few very active curves, including a recurring ~5.066 SOL value from Blur on three pools. Earlier, smaller samples (94/96, then 112/116 identical) were taken before these rules and over quieter pools.
- Blur data note (2026-09-28): part of the remaining misses are on Blur's side. In some DBC swaps Blur's `quote_reserve` is the post-trade WSOL balance of a different token account in the same transaction, not the pool's quote vault. Example: tx `b1fh1JRSZSXqqmE3RTEXdyXKJ4Ec5XRrw6edq3n5MFgAL76FVvzN2dLDtNr752G8CHzCjpi7vvrT7ooEp2bjX6V` on pool `8TqRRVfjF5BtfcSvQoEYgR7ikD6ZJyq1BHiM7DsiENf6` — Blur reported 5,066,247,482 lamports, which is the WSOL balance of an account owned by `ARu4n5mF…`; the pool's vault held 40,974,780,193. Because that account does not change, the same wrong value repeats across swaps.
- Polling covers every tracked curve: the most recently active 700 each cycle (Blur swaps mark activity) plus a rotating window over the rest. An earlier first-1,000 cap left the newest curves unread, including one that took 5,008 swaps from 207 wallets in 12 minutes before graduating.

Blur names the two venues `meteora_dbc` (bonding curves) and `meteora_damm2` (graduated pools). The key needs a role with the **DataApi** permission (dashboard → Members → New role → Data API, then attach it to the key under API Keys → Settings).

## API and dashboard
`python -m dbc_pulse.api` reads only the files the stream writes (no RPC) and serves:

| route | content |
|---|---|
| `/` | dashboard (curves closest to migration, DAMM v2 LP realized yield, live config table, recent migrations) |
| `/health` | stream heartbeat, counters, API uptime |
| `/pools?min_progress=&limit=` | tracked curves sorted by progress: reserves, buy/sell pressure, fees, config, migration flags, DAMM pool |
| `/configs` | per launch config: pools tracked, curves completed, migrated, plus event-tracker stats (launches, completion rate, instant share) when present |
| `/damm?limit=` | DAMM v2 pools: hours since migration, price change, fees %, LVR %, net %, σ (annualized ×), σ²/8 theory %, reserve check, anomaly flags (`jumps`, `liq_drops`, `clean`) |
| `/events?n=` · `/migrations?n=` | last decoded lifecycle events · last DBC→DAMM v2 migrations |
| `ws://…:8791/` | a `snapshot` message every 5s with health, top pools and DAMM rows |

LP numbers are per unit of liquidity with the quote token as numéraire. Base-denominated fees are valued at the lower of the interval's two prices, and a pool with a >10× price move or a >50% liquidity pull inside one poll is flagged `clean=false` (kept, but excluded from aggregates).

## Quick start
```bash
python -m venv .venv && .venv/Scripts/activate  # Windows
pip install -r requirements.txt
cp .env.example .env   # set SOLANA_RPC_URL
python -m dbc_pulse.stream          # live stream (single instance)
python -m dbc_pulse.api             # REST/WS + dashboard on :8790/:8791
python -m dbc_pulse.view --watch 5  # console viewer
python -m pytest -q                 # tests
```

## License
MIT
