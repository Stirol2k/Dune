# Dune Queries — LiquoriceSettlement Analytics

Dune SQL queries (Trino/PrestoSQL dialect) for analyzing the **LiquoriceSettlement** contract:

```
0x0448633eb8b0a42efed924c42069e0dcf08fb552
```

The contract is deployed via the [Deterministic Deployer](https://etherscan.io/address/0x4e59b44847b379578588920ca78fbf26c0b4956c) (CREATE2), so it lives at the **same address on every supported chain**.

| Chain | Explorer |
|-------|----------|
| Ethereum mainnet | https://etherscan.io/address/0x0448633eb8b0a42efed924c42069e0dcf08fb552 |
| Arbitrum One     | https://arbiscan.io/address/0x0448633eb8b0a42efed924c42069e0dcf08fb552 |

Deploy block: Ethereum `23326100` (2025-09-09 14:20:47 UTC).

All queries are **window-free** — each re-run automatically picks up the full history from the first transfer touching the contract on **any** tracked chain up to the latest block. No hardcoded dates.

## Queries

| File | What it produces |
|------|------------------|
| [`dune_daily_volume.sql`](./dune_daily_volume.sql) | Daily trading volume (USD) per chain, cumulative volume, and 7-day rolling average. |
| [`dune_weekly_volume_52w.sql`](./dune_weekly_volume_52w.sql) | Weekly volume (USD) per chain — full settlement history, ISO weeks (Mon-start). |
| [`dune_weekly_volume_kpis.sql`](./dune_weekly_volume_kpis.sql) | Weekly volume KPIs for investor decks: weekly volume, cumulative volume (hockey stick), and Week-over-Week % growth — per chain. |
| [`dune_usdc_yield_dashboard.sql`](./dune_usdc_yield_dashboard.sql) | USDC pool yield dashboard: inflow / outflow / TVL / 30-day rolling avg TVL / capital turnover / implied APR at 2/5/10 bps spread assumptions, per chain. |

## Multi-chain output shape

Every query returns one row per `(time_bucket, chain)` plus a synthetic
`chain = 'all'` row with cross-chain totals. This lets you build any of:

- **Stacked bar by chain** — filter `chain != 'all'`, group by `chain`.
- **Total volume only** — filter `chain = 'all'`.
- **Per-chain line / table** — filter to a single chain.

Adding a new EVM chain is a 2-line change in each file: append to the `chains`
CTE and to the `WHERE blockchain IN (...)` list. The yield query also needs
the chain's USDC variants added to `usdc_addrs`.

## Volume methodology

All volume queries follow the same logic to avoid double-counting:

- Each row in `tokens.transfers` is an ERC-20 transfer **leg** touching the contract (`from = pool` OR `to = pool`).
- Aggregation key is `(chain, tx_hash)` — `tx_hash` is **not** globally unique across chains, so `chain` is included in every `GROUP BY` and window `PARTITION BY`.
- For each `(chain, tx_hash)`:
  - `IN_usd`  = sum of legs where the token arrives at the pool (`to = pool`)
  - `OUT_usd` = sum of legs where the token leaves the pool   (`from = pool`)
  - `volume_usd_for_tx = GREATEST(IN_usd, OUT_usd)` — counts a swap once, not twice.
- Daily / weekly volume per chain = sum of `volume_usd_for_tx` across all txs in the period.
- USD pricing comes from `tokens.transfers.amount_usd` (Dune's oracle). Values may differ slightly from Etherscan's USD column — that is expected.

## Yield methodology (USDC dashboard)

- **TVL** = cumulative net USDC flow into the pool (inflow − outflow), summed
  from genesis, per chain. Equals the pool's USDC balance on that chain if the
  contract has no hidden mints/burns. The `chain = 'all'` row sums TVL across
  chains.
- **USDC variants tracked:**
  - Ethereum: USDC (Circle) `0xa0b86991...606eb48`
  - Arbitrum: USDC native (Circle) `0xaf88d065...268e5831` **and** USDC.e
    (bridged legacy) `0xff970a61...3ebddb5cc8`. Both denote the same $1-pegged
    exposure for the settlement contract and are summed together.
- **APR formula** (industry standard for MM pools):

  ```
  APR_percent = capital_turnover_annualized × spread_bps / 100
  capital_turnover_annualized = (trailing_30d_USDC_volume × 365 / 30) / TVL
  ```

- Three spread scenarios are pre-computed (configurable in `spread_assumptions`):
  - `2 bps`  — conservative (tight USDC/USDT-style execution)
  - `5 bps`  — realistic (typical MM capture)
  - `10 bps` — optimistic

## Running the queries

1. Open [dune.com](https://dune.com), New Query.
2. Paste the SQL.
3. Run. Charts can be built from the output columns described in each file's header.

## Performance notes

Each query has a `scan_floor` CTE (`DATE '2025-09-01'`) — a hard lower bound
that limits how far back `tokens.transfers` is scanned. The contract was
deployed 2025-09-09, so this only reduces scan cost, never trims actual data.
If the pool is ever redeployed earlier, lower this date.
