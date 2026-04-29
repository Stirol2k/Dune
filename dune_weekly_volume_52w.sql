-- =============================================================================
-- Weekly volume (USD) — full settlement history, multi-chain.
--
-- Contract (LiquoriceSettlement, deployed via Deterministic Deployer):
--   0x0448633eb8b0a42efed924c42069e0dcf08fb552
--   Same address on every supported chain (CREATE2 from Deterministic Deployer).
--
-- Chains covered:
--   • ethereum
--   • arbitrum
--
-- Window is computed automatically:
--   • start = Monday of the week of the FIRST transfer on ANY chain (UTC)
--   • end   = current_date (UTC)
--   Every re-run picks up new data up to the latest transaction.
--
-- Logic (mirrors analyze_volume.py):
--   • Each row in tokens.transfers = an ERC-20 transfer leg touching the contract.
--   • IN_usd  = sum of legs where the token arrives at the pool (to = pool)
--   • OUT_usd = sum of legs where the token leaves the pool   (from = pool)
--   • Per (chain, tx_hash): vol_usd = GREATEST(IN, OUT)   (avoid double-counting;
--     tx_hash is NOT globally unique — chain MUST be in every grouping key).
--   • Day (UTC): date of MIN(block_time) for that tx
--   • Week: Monday UTC — date_trunc('week', day)
--   • Final: sum of vol_usd across all days in the (week, chain) bucket
--
-- Pricing: tokens.transfers.amount_usd (Dune oracle).
--
-- Output: one row per (week_start_utc_monday, chain) plus 'all' aggregate.
-- =============================================================================

WITH
  pool AS (
    SELECT from_hex('0448633eb8b0a42efed924c42069e0dcf08fb552') AS addr
  ),

  chains AS (
    SELECT 'ethereum' AS chain UNION ALL
    SELECT 'arbitrum'
  ),

  /* LiquoriceSettlement deployed 2025-09-09 — keep floor a bit before that */
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

  weekly AS (
    SELECT
      chain,
      CAST(DATE_TRUNC('week', CAST(day_utc AS timestamp)) AS date) AS week_start_utc_monday,
      SUM(volume_usd) AS volume_usd
    FROM daily
    GROUP BY chain, CAST(DATE_TRUNC('week', CAST(day_utc AS timestamp)) AS date)
  ),

  /* Auto window: first week with data on ANY chain → current week */
  win AS (
    SELECT
      (SELECT MIN(week_start_utc_monday) FROM weekly) AS start_w,
      CAST(DATE_TRUNC('week', CAST(current_date AS timestamp)) AS date) AS end_w
  ),

  week_axis AS (
    SELECT
      c.chain,
      CAST(date_add('day', 7 * s, w.start_w) AS date) AS week_start_utc_monday
    FROM win w
    CROSS JOIN chains c
    CROSS JOIN UNNEST(
      sequence(0, CAST(date_diff('week', w.start_w, w.end_w) AS bigint))
    ) AS u(s)
  ),

  weekly_full AS (
    SELECT
      a.chain,
      a.week_start_utc_monday,
      COALESCE(w.volume_usd, 0) AS volume_usd
    FROM week_axis a
    LEFT JOIN weekly w
      ON w.chain                 = a.chain
     AND w.week_start_utc_monday = a.week_start_utc_monday
  ),

  /* Synthetic 'all' chain = sum across chains */
  with_all AS (
    SELECT chain, week_start_utc_monday, volume_usd FROM weekly_full
    UNION ALL
    SELECT 'all' AS chain, week_start_utc_monday, SUM(volume_usd) AS volume_usd
    FROM weekly_full
    GROUP BY week_start_utc_monday
  )

SELECT
  week_start_utc_monday,
  chain,
  volume_usd,
  SUM(volume_usd) OVER (PARTITION BY chain ORDER BY week_start_utc_monday) AS cumulative_volume_usd
FROM with_all
ORDER BY week_start_utc_monday, chain
