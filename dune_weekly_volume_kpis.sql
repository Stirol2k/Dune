-- =============================================================================
-- Weekly volume KPIs — bar chart for investors with:
--   • volume_usd            — weekly USD volume   (bars)
--   • cumulative_volume_usd — cumulative volume   (hockey-stick line)
--   • wow_pct               — Week-over-Week growth, % (line)
--
-- Contract (LiquoriceSettlement, deployed via Deterministic Deployer):
--   0x0448633eb8b0a42efed924c42069e0dcf08fb552
--   Same address on every supported chain (CREATE2 from Deterministic Deployer).
--
-- Chains scanned (volume is summed across all of them):
--   • ethereum
--   • arbitrum
--
-- Output: one row per week — cross-chain totals only (no per-chain breakdown).
-- This is intentional: investor charts plot one point per week, so a chain
-- dimension in the output would produce duplicate points. For per-chain detail
-- see dune_daily_volume.sql / dune_weekly_volume_52w.sql which keep `chain`.
--
-- Window is computed automatically:
--   • start = Monday of the week of the FIRST transfer on ANY chain (UTC)
--   • end   = current_date (UTC)
--
-- Volume logic mirrors dune_weekly_volume_52w.sql:
--   • Each row in tokens.transfers = an ERC-20 transfer leg touching the contract.
--   • Aggregation key for de-dup is (chain, tx_hash) because tx_hash is NOT
--     globally unique across chains.
--   • Per (chain, tx_hash): vol_usd = GREATEST(IN, OUT)  (avoid double-counting).
--   • Daily / weekly volume = sum of vol_usd across all txs in the period,
--     summed over all chains.
-- =============================================================================

WITH
  pool AS (
    SELECT from_hex('0448633eb8b0a42efed924c42069e0dcf08fb552') AS addr
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

  /* De-dup at (chain, tx_hash) level — chain MUST be in the GROUP BY because
     the same tx_hash can collide across chains. */
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
      block_time_min,
      GREATEST(in_usd, out_usd) AS vol_usd
    FROM by_tx
  ),

  /* Collapse across chains here — output below is total-only */
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

  /* Step 1: window functions over the (single, total) weekly series */
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

  /* Step 2: LAG over the smoothed series (Trino can't nest window funcs) */
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
