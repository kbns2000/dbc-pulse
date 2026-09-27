# dbc-pulse

Real-time data stream and analytics for **Meteora Dynamic Bonding Curve (DBC)** launches on Solana — built for launchpads, trading terminals and researchers.

**What it does**
- Streams DBC program events (`EvtInitializePool`, `EvtSwap2`, `EvtCurveComplete`, fee claims, migration) decoded from event-CPI, straight from the chain.
- Tracks every virtual pool: curve progress, quote reserve vs migration threshold, buy/sell pressure, unique wallets, time-to-graduation estimate.
- Follows graduated pools into **DAMM v2** and measures what liquidity providers actually earn: fees minus loss-versus-rebalancing (LVR), measured from on-chain state rather than displayed APR.
- Exposes everything as JSONL / WebSocket / REST so a launchpad or terminal can plug it in.

**Why**: launchpad builders configure curves and fees blind. dbc-pulse shows, per config, how launches actually behave and what the post-migration liquidity is worth.

Program: `dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN` (mainnet & devnet). Built during Colosseum Crypto World's Fair 2026 for the Meteora DBC side track.

## Status
- [ ] Event collector (RPC signatures → transactions → event-CPI decode)
- [ ] Pool state tracker (curve progress, thresholds, pressure)
- [ ] DAMM v2 LP realized-yield tracker (fees − LVR)
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
