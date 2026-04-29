# Dune Queries — LendingPool Analytics

Dune SQL queries (Trino/PrestoSQL dialect) for analyzing the LendingPool contract on Ethereum:

```
0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8
```

All queries are **window-free** — each re-run automatically picks up the full history from the first transfer touching the pool up to the latest block. No hardcoded dates.

## Queries

| File | What it produces |
|------|------------------|
| [`dune_daily_volume.sql`](./dune_daily_volume.sql) | Daily trading volume (USD), cumulative volume, and 7-day rolling average. |
| [`dune_weekly_volume_52w.sql`](./dune_weekly_volume_52w.sql) | Weekly volume (USD) — full pool history, ISO weeks (Mon-start). |
| [`dune_weekly_volume_kpis.sql`](./dune_weekly_volume_kpis.sql) | Weekly volume KPIs for investor decks: weekly volume, cumulative volume (hockey stick), and Week-over-Week % growth. |
| [`dune_usdc_yield_dashboard.sql`](./dune_usdc_yield_dashboard.sql) | USDC pool yield dashboard: inflow / outflow / TVL / 30-day rolling avg TVL / capital turnover / implied APR at 2/5/10 bps spread assumptions. |

## Volume methodology

All volume queries follow the same logic to avoid double-counting:

- Each row in `tokens.transfers` is an ERC-20 transfer **leg** touching the pool (`from = pool` OR `to = pool`).
- For each tx hash:
  - `IN_usd`  = sum of legs where the token arrives at the pool (`to = pool`)
  - `OUT_usd` = sum of legs where the token leaves the pool   (`from = pool`)
  - `volume_usd_for_tx = GREATEST(IN_usd, OUT_usd)` — this counts a swap once, not twice.
- Daily / weekly volume = sum of `volume_usd_for_tx` across all txs in the period.
- USD pricing comes from `tokens.transfers.amount_usd` (Dune's oracle). Values may differ slightly from Etherscan's USD column — that is expected.

## Yield methodology (USDC dashboard)

- **TVL** = cumulative net USDC flow into the pool (inflow − outflow), summed from genesis. Equals the pool's USDC balance if the contract has no hidden mints/burns.
- **USDC volume** counts every USDC leg touching the pool. For a market-maker that processes USDC↔X swaps this is exactly the USDC-side trade volume.
- **APR formula** (industry standard for MM pools):

  ```
  APR_percent = capital_turnover_annualized × spread_bps / 100
  capital_turnover_annualized = (trailing_30d_USDC_volume × 365 / 30) / TVL
  ```

- Three spread scenarios are pre-computed (configurable in the `spread_assumptions` CTE):
  - `2 bps`  — conservative (tight USDC/USDT-style execution)
  - `5 bps`  — realistic (typical MM capture)
  - `10 bps` — optimistic

## Running the queries

1. Open [dune.com](https://dune.com), New Query.
2. Paste the SQL.
3. Run. Charts can be built from the output columns described in each file's header.

## Performance notes

Each query has a `scan_floor` CTE (`DATE '2024-01-01'`) — a hard lower bound that limits how far back `tokens.transfers` is scanned. The contract was deployed later, so this only reduces scan cost, never trims actual data. If the pool is ever redeployed earlier, lower this date.
