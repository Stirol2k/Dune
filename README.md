# Dune queries: Liquorice protocol analytics

Dune SQL queries (Trino / DuneSQL dialect) behind the public dashboard
[dune.com/liquorice_research/liquoricemetrics](https://dune.com/liquorice_research/liquoricemetrics):
settled volume, trades, lending pool TVL and capital turnover.

## Contracts

All contracts are deployed via the
[Deterministic Deployer](https://etherscan.io/address/0x4e59b44847b379578588920ca78fbf26c0b4956c)
(CREATE2), so each one lives at the **same address on every supported chain**.
Every query tracks all versions of a contract together, so the numbers are
continuous across the v1 to v1.5 migration.

| Contract | Version | Address | Live since (Ethereum) | Used for |
|---|---|---|---|---|
| LiquoriceSettlement | v1.5 | `0x43dcd6586e6209ee7235a21bdc4aa301e5bc44e8` | 2026-08-05, block 25689471 | volume, trades |
| LiquoriceSettlement | v1 | `0x0448633eb8b0a42efed924c42069e0dcf08fb552` | 2025-09-09, block 23326100 | volume, trades |
| LiquoriceSettlement | pre-v1 | `0xaca684a3f64e0eae4812b734e3f8f205d3eed167` | 2025-02-13, block 21839997 | trades only |
| LiquoriceSettlement | pre-v1 | `0x9c4cbaa092abe0e67b89558bf21bd9be76c8fcaf` | 2024-12-05, block 21337206 | trades only (Ethereum only) |
| LendingPool | v1.5 | `0x1cce0034331638fec50b291a0bf42073346bb7c0` | 2026-08-05, block 25689470 | TVL, turnover |
| LendingPool | v1 | `0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8` | 2025-02-21, block 21894230 | TVL, turnover |

Chains: Ethereum mainnet and Arbitrum One. Explorers:
[settlement v1.5](https://etherscan.io/address/0x43dcd6586e6209ee7235a21bdc4aa301e5bc44e8),
[settlement v1](https://etherscan.io/address/0x0448633eb8b0a42efed924c42069e0dcf08fb552),
[lending pool v1.5](https://etherscan.io/address/0x1cce0034331638fec50b291a0bf42073346bb7c0),
[lending pool v1](https://etherscan.io/address/0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8);
swap `etherscan.io` for `arbiscan.io` for the Arbitrum copies.

The volume queries start at the v1 settlement (September 2025). The two pre-v1
contracts only appear in the trade-level queries; they carried test flow before
the closed alpha and are kept there for completeness.

All queries are **window-free**: each re-run picks up the full history from
the first event or transfer touching the tracked contracts on any chain up to
the latest block. No hardcoded dates.

## Queries

| File | What it produces |
|------|------------------|
| [`dune_daily_volume.sql`](./dune_daily_volume.sql) | Daily settled volume (USD) and trade count per chain, cumulative volume, and 7-day rolling average. |
| [`dune_weekly_volume_52w.sql`](./dune_weekly_volume_52w.sql) | Weekly settled volume (USD) and trade count per chain, full history, ISO weeks (Mon-start). |
| [`dune_weekly_volume_kpis.sql`](./dune_weekly_volume_kpis.sql) | Weekly volume KPIs for investor decks: weekly volume, cumulative volume (hockey stick), and week-over-week growth, cross-chain total. |
| [`dune_lending_pool_tvl.sql`](./dune_lending_pool_tvl.sql) | Lending pool TVL (USD) per chain, pool version and asset, with rollups; daily inflow / outflow, cumulative inflow, pool-touching tx count. |
| [`dune_usdc_capital_turnover.sql`](./dune_usdc_capital_turnover.sql) | USDC capital turnover: settled USDC volume (from `TradeOrder` events) vs USDC held in the lending pools, trailing 7d / 30d, annualized turnover. |
| [`dune_trades_history.sql`](./dune_trades_history.sql) | One row per settled RFQ order, decoded from the `TradeOrder` event of every settlement version: trader, maker, tokens, amounts, USD. |
| [`dune_aggregated_amounts.sql`](./dune_aggregated_amounts.sql) | Lifetime traded amount per token (token units and USD), from the same events, per chain plus cross-chain rollup. |

## Multi-chain output shape

Every time-series query returns one row per `(time_bucket, chain)` plus a
synthetic `chain = 'all'` row with cross-chain totals. This lets you build any of:

- stacked bar by chain: filter `chain != 'all'`, group by `chain`
- total only: filter `chain = 'all'`
- per-chain line or table: filter to a single chain

The TVL query adds two more dimensions, `pool_version` and `symbol`, each with
its own `'all'` rollup (see the file header for the exact rollup sets).

Adding a new EVM chain: append it to the `chains` CTE, add a `UNION ALL`
branch over `<chain>.logs` in `raw_logs` (event-based files) or add the chain
to the `WHERE blockchain IN (...)` list (transfer-based files). The turnover
query also needs the chain's USDC variants added to `usdc_addrs`. Adding a new
contract version is a one-line change: append a row to `settlements` or
`pools` (plus a row in `topics` if the event layout changed).

## Volume methodology

Volume is counted from the `TradeOrder` event that `LiquoriceSettlement` emits
exactly once per settled order, on every settle path (`settle`, `settleSingle`
and their permit variants).

- Events are read from raw `ethereum.logs` / `arbitrum.logs`, filtered on the
  settlement addresses and the two `TradeOrder` topic0 hashes (see
  "Trade-level queries" below for the layouts).
- `trade_usd` = base amount x `prices.day` price of the base token on the
  trade day; if the base token has no quote that day, quote amount x quote
  price is used instead. Amounts are decimal-adjusted through `tokens.erc20`.
- Daily / weekly volume per chain = `SUM(trade_usd)`; `trades` = event count.
- Every time-series query returns per-chain rows plus a `chain = 'all'` total.

Why events and not token transfers. An earlier version of these queries
counted ERC-20 transfers touching the settlement contract and took
`GREATEST(IN, OUT)` per transaction. That breaks in two ways:

- Hook / interaction routes pass both legs of a swap through the settlement
  (the taker's token in, the maker's token back in from an external swap, then
  out to the taker), so `IN` and `OUT` both equal 2x the trade and the trade is
  counted twice. All Arbitrum flow since February 2026 settles this way; the
  transfer count there was exactly 2x the event count every month.
- Orders a maker fills straight from its own wallet never touch the settlement
  balance, so transfers miss them (most Arbitrum flow between September 2025
  and January 2026).

On Ethereum the two methods agree to within pricing noise on ordinary
pool-routed days, which is the check that the event decoding is right.

## TVL methodology

- TVL is measured at the LendingPool contracts, not at the settlement. The
  settlement is atomic, its own balance nets to about zero at the end of every
  transaction, so it holds no capital.
- `balance_tokens` = cumulative net flow per `(chain, pool_version, token)` in
  token units, i.e. the pool's on-chain balance of that token. This is **net
  TVL**: capital supplied minus capital currently borrowed by makers (borrowed
  amounts sit in maker wallets, not in the pool).
- `tvl_usd` = balance x `prices.day` price for that day, forward-filled on days
  without a quote. Tokens with no price history (dust airdropped to the pool
  address, the pool's own LLP / LLC / LD share tokens) are dropped.
- Cross-checked against `LendingPool.getAssetsWithState()` on-chain: for the
  v1.5 mainnet pool, `totalDeposits + totalLockedDeposits - totalBorrowAmount`
  matches the query's `balance_tokens` per asset.

## Capital turnover

```
capital_turnover_annualized = (trailing_30d_USDC_volume x 365 / 30) / avg_30d_pool_USDC_TVL
```

The numerator is the USDC side of every settled order (from `TradeOrder`
events); the denominator is the USDC balance of the lending pools. Turnover is
a capital-efficiency metric (how many times a dollar of LP capital settles
trades per year). It is not a yield figure and swings with weekly volume, so
quote it with an as-of date.

## Trade-level queries

`dune_trades_history.sql` and `dune_aggregated_amounts.sql` decode the
`TradeOrder` event from raw `ethereum.logs` / `arbitrum.logs`. Two layouts
exist:

- pre-v1 and v1: `TradeOrder(string indexed rfqId, address trader, address effectiveTrader, address baseToken, address quoteToken, uint256 baseTokenAmount, uint256 quoteTokenAmount, address recipient)`,
  topic0 `0x0fce007c38c6c8ed9e545b3a148095762738618f8c21b673222613e4d45734b6`
- v1.5 adds the maker `signer` after `effectiveTrader`,
  topic0 `0x26357f6024689f750d018696a0e2ff7bf56b6fb3ec684bc83a5b84e635d6846f`

Both files select on `topic0` and pick the word offsets by layout. Amounts are
decimal-adjusted through `tokens.erc20` and valued with `prices.day`, the same
way the volume queries do it, so per-order rows sum to the volume charts.

## Running the queries

1. Open [dune.com](https://dune.com), New Query.
2. Paste the SQL.
3. Run. Charts can be built from the output columns described in each file's header.

## Performance notes

Each query has a `scan_floor` CTE, a hard lower bound that limits how far back
the raw logs or `tokens.transfers` are scanned. Values: `2025-09-01` for the
volume queries (settlement v1 deployed 2025-09-09), `2025-02-01` for the TVL
and turnover queries (lending pool v1 deployed 2025-02-21), `2024-12-01` for
the trade-level queries (oldest settlement deployed 2024-12-05). This only
reduces scan cost, never trims actual data. If a contract is ever redeployed
earlier, lower the matching date.

Or with the Dune CLI: `dune query run-sql --sql "$(cat dune_daily_volume.sql)"`.
