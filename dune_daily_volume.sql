-- =============================================================================
-- Daily trading volume (USD) — full settlement history, multi-chain.
--
-- Contract (LiquoriceSettlement, deployed via Deterministic Deployer):
--   0x0448633eb8b0a42efed924c42069e0dcf08fb552
--   Same address on every supported chain (CREATE2 from Deterministic Deployer).
--
-- Chains covered:
--   • ethereum
--   • arbitrum
-- (To add another EVM chain: append it to `chains` and `WHERE blockchain IN ...`)
--
-- Window is computed automatically:
--   • start = day of the FIRST transfer touching the contract on ANY chain (UTC)
--   • end   = current_date (UTC)
--   No hardcoded dates — every re-run picks up new data up to the latest tx.
--
-- Logic (mirrors analyze_volume.py / dune_weekly_volume_52w.sql):
--   • Each row in tokens.transfers = an ERC-20 transfer leg touching the contract
--     (from = pool OR to = pool). `tokens.transfers` is the unified multi-chain
--     view; `t.blockchain` carries the chain name.
--   • IN_usd  = sum of legs where the token arrives at the pool (to = pool)
--   • OUT_usd = sum of legs where the token leaves the pool   (from = pool)
--   • Per (chain, tx_hash): vol_usd = GREATEST(IN, OUT)   (avoids double-counting
--     swaps; tx_hash is NOT globally unique across chains, so chain MUST be in
--     every grouping/partition key).
--   • Day (UTC): date of MIN(block_time) for that tx
--   • Final: sum of vol_usd across all txs in the (day, chain) bucket
--
-- Pricing: tokens.transfers.amount_usd (Dune oracle).
--          Values may differ slightly from Etherscan USD — this is expected.
--
-- Output columns (one row per (day_utc, chain), plus 'all' aggregate row):
--   • day_utc                          — X axis
--   • chain                            — 'ethereum' | 'arbitrum' | 'all'
--   • volume_usd                       — bars  (left axis)
--   • cumulative_volume_usd            — line  (right axis, hockey-stick)
--   • vol_7d_avg_usd                   — 7-day rolling avg, smoother trend line
--
-- Recommended Dune chart: stacked bar by chain (filter chain != 'all') OR
-- single line per chain (filter chain). 'all' rows are pre-aggregated totals.
-- =============================================================================

WITH
  pool AS (
    /* contract address as varbinary (no 0x prefix) — compared directly to from/to */
    SELECT from_hex('0448633eb8b0a42efed924c42069e0dcf08fb552') AS addr
  ),

  chains AS (
    SELECT 'ethereum' AS chain UNION ALL
    SELECT 'arbitrum'
  ),

  /* Safety lower bound for scanning tokens.transfers — purely cost optimization.
     LiquoriceSettlement was deployed 2025-09-09. Widen this if you ever
     redeploy earlier, but never narrower than the deploy date. */
  scan_floor AS (
    SELECT DATE '2025-09-01' AS floor_d
  ),

  legs AS (
    SELECT
      t.blockchain AS chain,
      t.block_time,
      t.tx_hash,
      t."from" AS f,
      t."to"   AS tto,
      COALESCE(t.amount_usd, 0) AS leg_usd
    FROM tokens.transfers t
    CROSS JOIN pool p
    CROSS JOIN scan_floor s
    WHERE t.blockchain IN ('ethereum', 'arbitrum')
      AND t.block_time >= CAST(s.floor_d AS timestamp)
      AND (t."from" = p.addr OR t."to" = p.addr)
  ),

  by_tx AS (
    SELECT
      l.chain,
      l.tx_hash,
      MIN(l.block_time) AS block_time_min,
      SUM(CASE WHEN l.tto = p.addr THEN l.leg_usd ELSE 0 END) AS in_usd,
      SUM(CASE WHEN l.f   = p.addr THEN l.leg_usd ELSE 0 END) AS out_usd
    FROM legs l
    CROSS JOIN pool p
    GROUP BY l.chain, l.tx_hash
  ),

  tx_vol AS (
    SELECT
      chain,
      tx_hash,
      block_time_min,
      GREATEST(in_usd, out_usd) AS vol_usd
    FROM by_tx
  ),

  daily AS (
    SELECT
      chain,
      CAST(block_time_min AS date) AS day_utc,
      SUM(vol_usd) AS volume_usd
    FROM tx_vol
    GROUP BY chain, CAST(block_time_min AS date)
  ),

  /* Auto window: first day with data on ANY chain → today (UTC) */
  win AS (
    SELECT
      (SELECT MIN(day_utc) FROM daily) AS start_d,
      current_date                     AS end_d
  ),

  /* Continuous (chain × day) axis so charts have no gaps and rolling avg is correct */
  axis AS (
    SELECT
      c.chain,
      CAST(date_add('day', s, w.start_d) AS date) AS day_utc
    FROM win w
    CROSS JOIN chains c
    CROSS JOIN UNNEST(
      sequence(0, CAST(date_diff('day', w.start_d, w.end_d) AS bigint))
    ) AS u(s)
  ),

  daily_full AS (
    SELECT
      a.chain,
      a.day_utc,
      COALESCE(d.volume_usd, 0) AS volume_usd
    FROM axis a
    LEFT JOIN daily d
      ON d.chain   = a.chain
     AND d.day_utc = a.day_utc
  ),

  /* Add a synthetic 'all' chain = sum across chains, so the chart can show
     a combined total alongside per-chain breakdown without re-aggregation. */
  with_all AS (
    SELECT chain, day_utc, volume_usd FROM daily_full
    UNION ALL
    SELECT 'all' AS chain, day_utc, SUM(volume_usd) AS volume_usd
    FROM daily_full
    GROUP BY day_utc
  )

SELECT
  day_utc,
  chain,
  volume_usd,

  /* Cumulative volume per chain — hockey-stick line */
  SUM(volume_usd) OVER (PARTITION BY chain ORDER BY day_utc) AS cumulative_volume_usd,

  /* 7-day rolling average per chain — smoother trend line */
  AVG(volume_usd) OVER (
    PARTITION BY chain
    ORDER BY day_utc
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS vol_7d_avg_usd
FROM with_all
ORDER BY day_utc, chain
