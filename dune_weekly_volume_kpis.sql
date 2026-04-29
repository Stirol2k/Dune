-- =============================================================================
-- Weekly volume KPIs — bar chart for investors with:
--   • volume_usd            — weekly USD volume   (bars)
--   • cumulative_volume_usd — cumulative volume   (hockey-stick line)
--   • wow_pct               — Week-over-Week growth, % (line)
--
-- Window is computed automatically:
--   • start = Monday of the week of the FIRST transfer touching the pool
--   • end   = current_date (UTC)
--   No hardcoded dates — every re-run picks up new data up to the latest tx.
--
-- Volume logic is identical to dune_weekly_volume_52w.sql
--   (LendingPool: 0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8)
-- =============================================================================

WITH
  pool AS (
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

  weekly AS (
    SELECT
      CAST(DATE_TRUNC('week', CAST(day_utc AS timestamp)) AS date) AS week_start_utc_monday,
      SUM(volume_usd) AS volume_usd
    FROM daily
    GROUP BY 1
  ),

  /* Auto window: first week with data → current week (UTC) */
  win AS (
    SELECT
      (SELECT MIN(week_start_utc_monday) FROM weekly) AS start_w,
      CAST(DATE_TRUNC('week', CAST(current_date AS timestamp)) AS date) AS end_w
  ),

  /* Continuous week axis so bars have no gaps and WoW LAG is correct */
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
  ),

  /* Tunables for WoW readability:
       • MIN_BASE_USD — minimum prior-week volume required to compute WoW.
         Below this, WoW is NULL (early bootstrap weeks won't blow up the chart).
       • WOW_CAP_PCT  — cap for the "clean" WoW so a single outlier doesn't
         dominate the Y axis. Set high if you don't want capping. */
  knobs AS (
    SELECT
      CAST(100000 AS double) AS min_base_usd,  /* $100k floor */
      CAST(300    AS double) AS wow_cap_pct    /* clip to ±300% */
  ),

  /* Step 1: window functions over the raw weekly series */
  enriched AS (
    SELECT
      f.week_start_utc_monday,
      f.volume_usd,

      /* Cumulative volume — "hockey stick" line */
      SUM(f.volume_usd) OVER (ORDER BY f.week_start_utc_monday) AS cumulative_volume_usd,

      /* Trailing 4-week average — smoother base for growth calc */
      AVG(f.volume_usd) OVER (
        ORDER BY f.week_start_utc_monday
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ) AS vol_4w_avg_usd,

      LAG(f.volume_usd, 1) OVER (ORDER BY f.week_start_utc_monday) AS prev_week_vol
    FROM weekly_full f
  ),

  /* Step 2: LAG over the smoothed series (Trino can't nest window funcs,
     so we do it in a separate CTE) */
  enriched2 AS (
    SELECT
      e.*,
      LAG(e.vol_4w_avg_usd, 1) OVER (ORDER BY e.week_start_utc_monday) AS prev_4w_avg
    FROM enriched e
  )

SELECT
  e.week_start_utc_monday,
  e.volume_usd,
  e.cumulative_volume_usd,

  /* Raw WoW — kept for reference; same as before, can spike to thousands of % */
  CASE
    WHEN e.prev_week_vol > 0
      THEN (e.volume_usd / e.prev_week_vol - 1) * 100.0
  END AS wow_pct_raw,

  /* WoW with min-base filter — NULL until prior week ≥ MIN_BASE_USD */
  CASE
    WHEN e.prev_week_vol >= k.min_base_usd
      THEN (e.volume_usd / e.prev_week_vol - 1) * 100.0
  END AS wow_pct,

  /* Same WoW but capped to ±WOW_CAP_PCT for the cleanest bar chart */
  CASE
    WHEN e.prev_week_vol >= k.min_base_usd
      THEN GREATEST(
             LEAST(
               (e.volume_usd / e.prev_week_vol - 1) * 100.0,
               k.wow_cap_pct
             ),
             -k.wow_cap_pct
           )
  END AS wow_pct_capped,

  /* Smoother alternative: growth of trailing 4-week average vs. prior week's
     trailing 4-week average. Much less reactive to single-week noise. */
  CASE
    WHEN e.prev_4w_avg >= k.min_base_usd
      THEN (e.vol_4w_avg_usd / e.prev_4w_avg - 1) * 100.0
  END AS growth_4w_avg_pct
FROM enriched2 e
CROSS JOIN knobs k
ORDER BY e.week_start_utc_monday
