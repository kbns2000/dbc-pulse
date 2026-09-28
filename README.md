# dbc-pulse public feed

Key-free JSON published by [dbc-pulse](https://github.com/kbns2000/dbc-pulse) from Solana mainnet (Meteora Dynamic Bonding Curve + DAMM v2). Refreshed about hourly while the stream runs.

| file | contents |
|---|---|
| [`feed/summary.json`](feed/summary.json) | stream health, counts, Solami Blur cross-check |
| [`feed/configs.json`](feed/configs.json) | per launch config: pools, curve completions, migrations, completion rate, instant-graduation share, graduation take (% of the raise paid out at migration and the creator's share of it) |
| [`feed/graduation_take_flags.json`](feed/graduation_take_flags.json) | live curves whose config pays >= 20% of the raise out at graduation instead of seeding the pool |
| [`feed/partner_revenue.json`](feed/partner_revenue.json) | latest measured revenue split per graduation (curve fees, migration fee, LP fees; partner vs creator) |

Raw URL pattern: `https://raw.githubusercontent.com/kbns2000/dbc-pulse/feed/feed/<file>`

All values are read from public on-chain accounts; method and caveats are in the main README. MIT.
