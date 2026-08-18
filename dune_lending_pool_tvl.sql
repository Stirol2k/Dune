-- =============================================================================
-- Lending pool TVL (USD), full history, multi-chain, per asset.
--
-- Contracts (LendingPool, deployed via Deterministic Deployer / CREATE2, same
-- address on every supported chain). Both versions are tracked; the output
-- carries `pool_version` so the v1 -> v1.5 migration is visible, plus rollups:
--   v1    0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8   live 2025-02-21 (Ethereum block 21894230)
--   v1.5  0x1cce0034331638fec50b291a0bf42073346bb7c0   live 2026-08-05 (Ethereum block 25689470)
--
-- Chains covered:
--   * ethereum
--   * arbitrum
-- (To add another EVM chain: append it to `chains` and `WHERE blockchain IN ...`.
--  To add another pool: append a row to `pools`.)
--
-- Why the pool and not the settlement: LiquoriceSettlement settles atomically,
-- so its own token balance nets to about zero at the end of every tx. LP
-- capital lives in the LendingPool. TVL is therefore measured at the pool.
--
-- Method:
--   * legs    = every ERC-20 transfer leg touching a pool (from = pool OR to = pool)
--   * balance = cumulative net flow per (chain, pool_version, token) in token
--               units, i.e. the pool's on-chain balance of that token. This is
--               NET TVL: capital supplied minus capital currently borrowed by
--               makers (borrowed amounts sit in maker wallets, not in the pool).
--               Matches the "Live LP TVL (net)" definition used in company
--               materials.
--   * price   = prices.day for (chain, token, day), forward-filled on days with
--               no quote. Tokens with no price at all (e.g. the pool's own
--               LLP*/LLC*/LD* utility tokens, should they ever touch the pool)
--               are dropped by the price join.
--   * tvl_usd = balance * price, per day
--   * Also reported: daily inflow / outflow in USD (Dune oracle at transfer
--     time), cumulative inflow, cumulative pool-touching txs.
--
-- Window is computed automatically:
--   * start = day of the FIRST transfer touching any pool on ANY chain (UTC)
--   * end   = current_date (UTC)
--
-- Output: one row per (day_utc, chain, pool_version, symbol) plus rollup rows
-- where a grouped-away dimension reads 'all'. Rollup sets:
--   (chain, pool_version, symbol)   full detail
--   (chain, pool_version)           per chain and pool version, all assets
--   (chain, symbol)                 versions merged, per asset
--   (chain)                         per-chain total
--   (pool_version)                  per pool version across chains
--   (symbol)                        per-asset total across chains
--   ()                              grand total (chain = pool_version = symbol = 'all')
-- balance_tokens and price_usd are NULL on rows where symbol = 'all' (mixed units).
--
-- Suggested charts:
--   * Counter "LP TVL":            filter chain='all', pool_version='all', symbol='all', last row, tvl_usd
--   * Stacked area by asset:       filter chain='all', pool_version='all', symbol!='all'
--   * v1 vs v1.5 migration:        filter chain='all', symbol='all', pool_version!='all'
-- =============================================================================

WITH
  pools AS (
    SELECT from_hex('046ffb0dfde6a21b4fc609841f55c31b6297cfb8') AS addr, 'v1'   AS pool_version
    UNION ALL
    SELECT from_hex('1cce0034331638fec50b291a0bf42073346bb7c0'),          'v1.5'
  ),

  chains AS (
    SELECT 'ethereum' AS chain UNION ALL
    SELECT 'arbitrum'
  ),

  /* LendingPool v1 deployed 2025-02-21, keep floor a bit before that. */
  scan_floor AS (
    SELECT DATE '2025-02-01' AS floor_d
  ),

  /* Every ERC-20 leg touching a pool. A leg is IN for the pool it arrives at
     and OUT for the pool it leaves; a pool-to-pool hop is OUT for one and IN
     for the other, so per-pool balances stay right and totals net to zero. */
  legs AS (
    SELECT
      t.blockchain                 AS chain,
      p.pool_version,
      t.contract_address           AS token,
      t.symbol,
      t.tx_hash,
      CAST(t.block_time AS date)   AS day_utc,
      CASE WHEN t."to"   = p.addr THEN COALESCE(t.amount, 0)     ELSE 0 END AS in_amt,
      CASE WHEN t."from" = p.addr THEN COALESCE(t.amount, 0)     ELSE 0 END AS out_amt,
      CASE WHEN t."to"   = p.addr THEN COALESCE(t.amount_usd, 0) ELSE 0 END AS in_usd,
      CASE WHEN t."from" = p.addr THEN COALESCE(t.amount_usd, 0) ELSE 0 END AS out_usd
    FROM tokens.transfers t
    JOIN pools p
      ON (t."from" = p.addr OR t."to" = p.addr)
    CROSS JOIN scan_floor s
    WHERE t.blockchain IN ('ethereum', 'arbitrum')
      AND t.block_time >= CAST(s.floor_d AS timestamp)
  ),

  daily_raw AS (
    SELECT
      chain,
      pool_version,
      token,
      day_utc,
      SUM(in_amt)  AS in_amt,
      SUM(out_amt) AS out_amt,
      SUM(in_usd)  AS in_usd,
      SUM(out_usd) AS out_usd,
      COUNT(DISTINCT tx_hash) AS txs
    FROM legs
    GROUP BY chain, pool_version, token, day_utc
  ),

  /* One symbol per (chain, token); prefer the transfers table, fall back to prices.
     Tokens that never carried priced inflow (dust airdropped to the pool
     address) are dropped here. */
  tokens_seen AS (
    SELECT chain, pool_version, token, MAX(symbol) AS symbol
    FROM legs
    GROUP BY chain, pool_version, token
    HAVING SUM(in_usd) > 0
  ),

  /* Daily USD price per (chain, token). Restricted to tokens we actually saw. */
  px AS (
    SELECT
      p.blockchain               AS chain,
      p.contract_address         AS token,
      CAST(p."timestamp" AS date) AS day_utc,
      MAX(p.price)               AS price_usd,
      MAX(p.symbol)              AS px_symbol
    FROM prices.day p
    JOIN (SELECT DISTINCT chain, token FROM tokens_seen) ts
      ON ts.chain = p.blockchain
     AND ts.token = p.contract_address
    CROSS JOIN scan_floor s
    WHERE p.blockchain IN ('ethereum', 'arbitrum')
      AND p."timestamp" >= CAST(s.floor_d AS timestamp)
    GROUP BY p.blockchain, p.contract_address, CAST(p."timestamp" AS date)
  ),

  /* Only keep tokens that have at least one price quote. */
  priced_tokens AS (
    SELECT ts.chain, ts.pool_version, ts.token,
           COALESCE(ts.symbol, MAX(px.px_symbol)) AS symbol
    FROM tokens_seen ts
    JOIN px ON px.chain = ts.chain AND px.token = ts.token
    GROUP BY ts.chain, ts.pool_version, ts.token, ts.symbol
  ),

  /* Auto window: first day with pool activity on ANY chain -> today (UTC) */
  win AS (
    SELECT
      (SELECT MIN(day_utc) FROM daily_raw) AS start_d,
      current_date                          AS end_d
  ),

  /* Continuous (chain x pool x token x day) axis so cumulative sums and
     forward-filled prices are well-defined on quiet days. */
  axis AS (
    SELECT
      pt.chain,
      pt.pool_version,
      pt.token,
      pt.symbol,
      CAST(date_add('day', s, w.start_d) AS date) AS day_utc
    FROM win w
    CROSS JOIN priced_tokens pt
    CROSS JOIN UNNEST(
      sequence(0, CAST(date_diff('day', w.start_d, w.end_d) AS bigint))
    ) AS u(s)
  ),

  daily_full AS (
    SELECT
      a.chain,
      a.pool_version,
      a.token,
      a.symbol,
      a.day_utc,
      COALESCE(d.in_amt,  0) AS in_amt,
      COALESCE(d.out_amt, 0) AS out_amt,
      COALESCE(d.in_usd,  0) AS in_usd,
      COALESCE(d.out_usd, 0) AS out_usd,
      COALESCE(d.txs,     0) AS txs,
      px.price_usd           AS price_raw
    FROM axis a
    LEFT JOIN daily_raw d
      ON d.chain        = a.chain
     AND d.pool_version = a.pool_version
     AND d.token        = a.token
     AND d.day_utc      = a.day_utc
    LEFT JOIN px
      ON px.chain   = a.chain
     AND px.token   = a.token
     AND px.day_utc = a.day_utc
  ),

  /* Cumulative balance in token units + forward-filled price */
  balances AS (
    SELECT
      chain,
      pool_version,
      token,
      symbol,
      day_utc,
      in_amt,
      out_amt,
      in_usd,
      out_usd,
      txs,
      SUM(in_amt - out_amt) OVER (
        PARTITION BY chain, pool_version, token
        ORDER BY day_utc
      ) AS balance_tokens,
      LAST_VALUE(price_raw) IGNORE NULLS OVER (
        PARTITION BY chain, pool_version, token
        ORDER BY day_utc
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS price_usd
    FROM daily_full
  ),

  valued AS (
    SELECT
      chain,
      pool_version,
      token,
      symbol,
      day_utc,
      in_amt,
      out_amt,
      in_usd,
      out_usd,
      txs,
      /* Guard against float dust going a hair below zero after many legs */
      GREATEST(balance_tokens, 0)                        AS balance_tokens,
      price_usd,
      GREATEST(balance_tokens, 0) * COALESCE(price_usd, 0) AS tvl_usd
    FROM balances
  ),

  /* Rollups. Grouped-away dimensions read 'all'. */
  rolled AS (
    SELECT
      day_utc,
      COALESCE(chain,        'all') AS chain,
      COALESCE(pool_version, 'all') AS pool_version,
      COALESCE(symbol,       'all') AS symbol,
      CASE WHEN GROUPING(symbol) = 0 THEN SUM(balance_tokens) END AS balance_tokens,
      CASE WHEN GROUPING(symbol) = 0 THEN MAX(price_usd)      END AS price_usd,
      SUM(tvl_usd)  AS tvl_usd,
      SUM(in_usd)   AS inflow_usd,
      SUM(out_usd)  AS outflow_usd,
      SUM(txs)      AS txs
    FROM valued
    GROUP BY GROUPING SETS (
      (day_utc, chain, pool_version, symbol),
      (day_utc, chain, pool_version),
      (day_utc, chain, symbol),
      (day_utc, chain),
      (day_utc, pool_version),
      (day_utc, symbol),
      (day_utc)
    )
  )

SELECT
  day_utc,
  chain,
  pool_version,
  symbol,
  balance_tokens,
  price_usd,
  tvl_usd,
  inflow_usd,
  outflow_usd,
  inflow_usd - outflow_usd AS net_flow_usd,
  txs,
  SUM(inflow_usd) OVER (PARTITION BY chain, pool_version, symbol ORDER BY day_utc) AS cumulative_inflow_usd,
  SUM(txs)        OVER (PARTITION BY chain, pool_version, symbol ORDER BY day_utc) AS cumulative_txs,
  AVG(tvl_usd)    OVER (
    PARTITION BY chain, pool_version, symbol
    ORDER BY day_utc
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS avg_tvl_30d_usd
FROM rolled
ORDER BY day_utc, chain, pool_version, symbol
