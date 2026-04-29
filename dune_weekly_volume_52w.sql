-- =============================================================================
-- Weekly volume (USD) — full pool history, always up to date.
--
-- Contract (LendingPool, Etherscan tokentxns):
--   0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8
--
-- Window is computed automatically:
--   • start = Monday of the week of the FIRST transfer touching the pool
--   • end   = current_date (UTC)
--   Every re-run picks up new data up to the latest transaction.
--
-- Logic (mirrors analyze_volume.py):
--   • Each row = an ERC-20 transfer leg touching the pool (from = pool OR to = pool)
--   • IN  = sum of USD legs where the token arrives at the pool (to = pool)
--   • OUT = sum of USD legs where the token leaves the pool   (from = pool)
--   • Per tx hash: volume_usd = GREATEST(IN, OUT)
--   • Day (UTC): date of MIN(block_time) for that tx
--   • Week: Monday UTC — date_trunc('week', day)
--   • Final: sum of volume_usd across all days that fall into the week
--
-- Pricing: uses tokens.transfers.amount_usd (Dune's oracle).
--          Values may differ slightly from Etherscan USD — this is expected.
-- =============================================================================

WITH
  pool AS (
    /* pool address as varbinary (no 0x prefix) — compared directly to from/to */
    SELECT from_hex('046ffb0dfde6a21b4fc609841f55c31b6297cfb8') AS addr
  ),

  /* Safety lower bound for scanning tokens.transfers — purely to keep the
     query cheap. The contract was deployed later than this, so it does not
     affect the result; widen it if you ever change the contract. */
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

  weekly AS (
    SELECT
      CAST(DATE_TRUNC('week', CAST(day_utc AS timestamp)) AS date) AS week_start_utc_monday,
      SUM(volume_usd) AS volume_usd
    FROM daily
    GROUP BY 1
  ),

  /* Auto window: first week with data → current week */
  win AS (
    SELECT
      (SELECT MIN(week_start_utc_monday) FROM weekly) AS start_w,
      CAST(DATE_TRUNC('week', CAST(current_date AS timestamp)) AS date) AS end_w
  ),

  /* Continuous week axis so the bar chart has no gaps (zero-volume weeks included) */
  week_axis AS (
    SELECT
      CAST(date_add('day', 7 * s, w.start_w) AS date) AS week_start_utc_monday
    FROM win w
    CROSS JOIN UNNEST(
      sequence(0, CAST(date_diff('week', w.start_w, w.end_w) AS bigint))
    ) AS u(s)
  ),

  weekly_full AS (
    SELECT
      a.week_start_utc_monday,
      COALESCE(w.volume_usd, 0) AS volume_usd
    FROM week_axis a
    LEFT JOIN weekly w
      ON w.week_start_utc_monday = a.week_start_utc_monday
  )

SELECT
  week_start_utc_monday,
  volume_usd,
  SUM(volume_usd) OVER (ORDER BY week_start_utc_monday) AS cumulative_volume_usd
FROM weekly_full
ORDER BY week_start_utc_monday
