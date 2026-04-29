-- =============================================================================
-- Daily trading volume (USD) — full pool history, always up to date.
--
-- Contract (LendingPool, Etherscan tokentxns):
--   0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8
--
-- Window is computed automatically:
--   • start = day of the FIRST transfer touching the pool (UTC)
--   • end   = current_date (UTC)
--   No hardcoded dates — every re-run picks up new data up to the latest tx.
--
-- Logic (mirrors analyze_volume.py / dune_weekly_volume_52w.sql):
--   • Each row = an ERC-20 transfer leg touching the pool (from = pool OR to = pool)
--   • IN  = sum of USD legs where the token arrives at the pool (to = pool)
--   • OUT = sum of USD legs where the token leaves the pool   (from = pool)
--   • Per tx hash: volume_usd = GREATEST(IN, OUT)   (avoids double-counting swaps)
--   • Day (UTC): date of MIN(block_time) for that tx
--   • Final: sum of volume_usd across all txs in the day
--
-- Pricing: uses tokens.transfers.amount_usd (Dune's oracle).
--          Values may differ slightly from Etherscan USD — this is expected.
--
-- Output columns (ready for a bar chart):
--   • day_utc                 — X axis
--   • volume_usd              — bars  (left axis)
--   • cumulative_volume_usd   — line  (right axis, hockey-stick)
--   • vol_7d_avg_usd          — optional smoother line
-- =============================================================================

WITH
  pool AS (
    /* pool address as varbinary (no 0x prefix) — compared directly to from/to */
    SELECT from_hex('046ffb0dfde6a21b4fc609841f55c31b6297cfb8') AS addr
  ),

  /* Safety lower bound for scanning tokens.transfers — purely for query cost.
     The contract was deployed later than this, so it does not affect results. */
  scan_floor AS (
    SELECT DATE '2024-01-01' AS floor_d
  ),

  legs AS (
    SELECT
      t.block_time,
      t.tx_hash,
      t."from" AS f,
      t."to"   AS tto,
      COALESCE(t.amount_usd, 0) AS leg_usd
    FROM tokens.transfers t
    CROSS JOIN pool p
    CROSS JOIN scan_floor s
    WHERE t.blockchain = 'ethereum'
      AND t.block_time >= CAST(s.floor_d AS timestamp)
      AND (t."from" = p.addr OR t."to" = p.addr)
  ),

  by_tx AS (
    SELECT
      l.tx_hash,
      MIN(l.block_time) AS block_time_min,
      SUM(CASE WHEN l.tto = p.addr THEN l.leg_usd ELSE 0 END) AS in_usd,
      SUM(CASE WHEN l.f   = p.addr THEN l.leg_usd ELSE 0 END) AS out_usd
    FROM legs l
    CROSS JOIN pool p
    GROUP BY l.tx_hash
  ),

  tx_vol AS (
    SELECT
      tx_hash,
      block_time_min,
      GREATEST(in_usd, out_usd) AS vol_usd
    FROM by_tx
  ),

  daily AS (
    SELECT
      CAST(block_time_min AS date) AS day_utc,
      SUM(vol_usd) AS volume_usd
    FROM tx_vol
    GROUP BY 1
  ),

  /* Auto window: first day with data → today (UTC) */
  win AS (
    SELECT
      (SELECT MIN(day_utc) FROM daily) AS start_d,
      current_date                     AS end_d
  ),

  /* Continuous day axis so bars have no gaps and rolling avg is correct */
  day_axis AS (
    SELECT
      CAST(date_add('day', s, w.start_d) AS date) AS day_utc
    FROM win w
    CROSS JOIN UNNEST(
      sequence(0, CAST(date_diff('day', w.start_d, w.end_d) AS bigint))
    ) AS u(s)
  ),

  daily_full AS (
    SELECT
      a.day_utc,
      COALESCE(d.volume_usd, 0) AS volume_usd
    FROM day_axis a
    LEFT JOIN daily d
      ON d.day_utc = a.day_utc
  )

SELECT
  day_utc,
  volume_usd,

  /* Cumulative volume — "hockey stick" line */
  SUM(volume_usd) OVER (ORDER BY day_utc) AS cumulative_volume_usd,

  /* 7-day rolling average — smooths daily noise, nicer trend line on the chart */
  AVG(volume_usd) OVER (
    ORDER BY day_utc
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS vol_7d_avg_usd
FROM daily_full
ORDER BY day_utc
