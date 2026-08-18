-- =============================================================================
-- Weekly volume KPIs, bar chart for investors with:
--   * volume_usd            weekly USD volume   (bars)
--   * cumulative_volume_usd cumulative volume   (hockey-stick line)
--   * wow_pct               Week-over-Week growth, % (line)
-- Full settlement history, cross-chain total.
-- Basis: TradeOrder events emitted by LiquoriceSettlement, one event per
-- settled order.
--
-- Contracts (LiquoriceSettlement, deployed via Deterministic Deployer / CREATE2,
-- same address on every supported chain). All versions are tracked:
--   v1    0x0448633eb8b0a42efed924c42069e0dcf08fb552   live 2025-09-09 (Ethereum block 23326100)
--   v1.5  0x43dcd6586e6209ee7235a21bdc4aa301e5bc44e8   live 2026-08-05 (Ethereum block 25689471)
-- (pre-v1 test contracts from Dec 2024 / Feb 2025 are excluded here on
--  purpose: volume history starts with v1. They are in dune_trades_history.sql.)
--
-- Chains covered:
--   * ethereum
--   * arbitrum
-- (To add another EVM chain: add a UNION ALL branch in `raw_logs` and a row in
--  `chains`. To add another settlement version: append a row to `settlements`,
--  and to `topics` if the event layout changed.)
--
-- Why events and not token transfers: some routes (hooks / interactions) pass
-- both legs of a swap through the settlement contract, so counting transfers
-- that touch the settlement double-counts those trades (all Arbitrum flow
-- since Feb 2026 is like this). Other routes settle straight from a maker
-- wallet and never touch the settlement balance, so transfers miss them.
-- TradeOrder is emitted exactly once per settled order on every path, so it
-- is the exact record.
--
-- Method:
--   * decode TradeOrder from ethereum.logs / arbitrum.logs (two layouts, see
--     dune_trades_history.sql for the word offsets)
--   * trade_usd = base amount x prices.day price of the base token on the trade
--     day; quote side x its price as fallback when the base has no quote
--   * volume per (day, chain) = SUM(trade_usd)
--
-- Window is computed automatically:
--   * start = day of the FIRST TradeOrder on ANY chain (UTC)
--   * end   = current_date (UTC)
--
-- Output: one row per week, cross-chain totals only (no per-chain breakdown).
-- This is intentional: investor charts plot one point per week, so a chain
-- dimension in the output would produce duplicate points. For per-chain detail
-- see dune_daily_volume.sql / dune_weekly_volume_52w.sql which keep `chain`.
--   * week_start_utc_monday
--   * volume_usd, trades, cumulative_volume_usd
--   * wow_pct_raw, wow_pct, wow_pct_capped, growth_4w_avg_pct
-- =============================================================================

WITH
  settlements AS (
    SELECT from_hex('0448633eb8b0a42efed924c42069e0dcf08fb552') AS addr, 'v1'   AS version
    UNION ALL
    SELECT from_hex('43dcd6586e6209ee7235a21bdc4aa301e5bc44e8'),          'v1.5'
  ),

  topics AS (
    SELECT 0x0fce007c38c6c8ed9e545b3a148095762738618f8c21b673222613e4d45734b6 AS topic0, 'v1'   AS layout
    UNION ALL
    SELECT 0x26357f6024689f750d018696a0e2ff7bf56b6fb3ec684bc83a5b84e635d6846f,          'v1.5'
  ),

  chains AS (
    SELECT 'ethereum' AS chain UNION ALL
    SELECT 'arbitrum'
  ),

  /* Settlement v1 deployed 2025-09-09, keep floor a bit before that. */
  scan_floor AS (
    SELECT DATE '2025-09-01' AS floor_d
  ),

  raw_logs AS (
    SELECT 'ethereum' AS chain, l.block_time, l.tx_hash, l.index AS evt_index, l.topic0, l.data
    FROM ethereum.logs l
    CROSS JOIN scan_floor s
    WHERE l.block_time >= CAST(s.floor_d AS timestamp)
      AND l.contract_address IN (SELECT addr FROM settlements)
      AND l.topic0 IN (SELECT topic0 FROM topics)
    UNION ALL
    SELECT 'arbitrum' AS chain, l.block_time, l.tx_hash, l.index AS evt_index, l.topic0, l.data
    FROM arbitrum.logs l
    CROSS JOIN scan_floor s
    WHERE l.block_time >= CAST(s.floor_d AS timestamp)
      AND l.contract_address IN (SELECT addr FROM settlements)
      AND l.topic0 IN (SELECT topic0 FROM topics)
  ),

  decoded AS (
    SELECT
      r.chain,
      CAST(r.block_time AS date) AS day_utc,
      r.tx_hash,
      r.evt_index,
      CASE WHEN t.layout = 'v1.5'
           THEN varbinary_substring(r.data, 109, 20)
           ELSE varbinary_substring(r.data,  77, 20) END AS base_token,
      CASE WHEN t.layout = 'v1.5'
           THEN varbinary_substring(r.data, 141, 20)
           ELSE varbinary_substring(r.data, 109, 20) END AS quote_token,
      CASE WHEN t.layout = 'v1.5'
           THEN varbinary_to_uint256(varbinary_substring(r.data, 161, 32))
           ELSE varbinary_to_uint256(varbinary_substring(r.data, 129, 32)) END AS base_amount_raw,
      CASE WHEN t.layout = 'v1.5'
           THEN varbinary_to_uint256(varbinary_substring(r.data, 193, 32))
           ELSE varbinary_to_uint256(varbinary_substring(r.data, 161, 32)) END AS quote_amount_raw
    FROM raw_logs r
    JOIN topics t ON t.topic0 = r.topic0
  ),

  erc20 AS (
    SELECT blockchain AS chain, contract_address AS token, decimals
    FROM tokens.erc20
    WHERE blockchain IN ('ethereum', 'arbitrum')
  ),

  px AS (
    SELECT
      p.blockchain                AS chain,
      p.contract_address          AS token,
      CAST(p."timestamp" AS date) AS day_utc,
      MAX(p.price)                AS price_usd
    FROM prices.day p
    CROSS JOIN scan_floor s
    WHERE p.blockchain IN ('ethereum', 'arbitrum')
      AND p."timestamp" >= CAST(s.floor_d AS timestamp)
      AND p.contract_address IN (
            SELECT base_token  FROM decoded
            UNION
            SELECT quote_token FROM decoded)
    GROUP BY p.blockchain, p.contract_address, CAST(p."timestamp" AS date)
  ),

  trades AS (
    SELECT
      d.chain,
      d.day_utc,
      COALESCE(
        CAST(d.base_amount_raw  AS double) / power(10, COALESCE(eb.decimals, 18)) * pb.price_usd,
        CAST(d.quote_amount_raw AS double) / power(10, COALESCE(eq.decimals, 18)) * pq.price_usd,
        0
      ) AS trade_usd
    FROM decoded d
    LEFT JOIN erc20 eb ON eb.chain = d.chain AND eb.token = d.base_token
    LEFT JOIN erc20 eq ON eq.chain = d.chain AND eq.token = d.quote_token
    LEFT JOIN px pb ON pb.chain = d.chain AND pb.token = d.base_token  AND pb.day_utc = d.day_utc
    LEFT JOIN px pq ON pq.chain = d.chain AND pq.token = d.quote_token AND pq.day_utc = d.day_utc
  ),

  daily AS (
    SELECT chain, day_utc, SUM(trade_usd) AS volume_usd, COUNT(*) AS trades
    FROM trades
    GROUP BY chain, day_utc
  ),

  /* Collapse across chains here, output below is total-only */
  weekly AS (
    SELECT
      CAST(DATE_TRUNC('week', CAST(day_utc AS timestamp)) AS date) AS week_start_utc_monday,
      SUM(volume_usd) AS volume_usd,
      SUM(trades)     AS trades
    FROM daily
    GROUP BY 1
  ),

  /* Auto window: first week with data -> current week (UTC) */
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
      COALESCE(w.volume_usd, 0) AS volume_usd,
      COALESCE(w.trades,     0) AS trades
    FROM week_axis a
    LEFT JOIN weekly w
      ON w.week_start_utc_monday = a.week_start_utc_monday
  ),

  /* Tunables for WoW readability:
       * MIN_BASE_USD: minimum prior-week volume required to compute WoW.
         Below this, WoW is NULL (early bootstrap weeks won't blow up the chart).
       * WOW_CAP_PCT: cap for the "clean" WoW so a single outlier doesn't
         dominate the Y axis. Set high if you don't want capping. */
  knobs AS (
    SELECT
      CAST(100000 AS double) AS min_base_usd,  /* $100k floor */
      CAST(300    AS double) AS wow_cap_pct    /* clip to +/-300% */
  ),

  /* Step 1: window functions over the (single, total) weekly series */
  enriched AS (
    SELECT
      f.week_start_utc_monday,
      f.volume_usd,
      f.trades,

      /* Cumulative volume, "hockey stick" line */
      SUM(f.volume_usd) OVER (ORDER BY f.week_start_utc_monday) AS cumulative_volume_usd,

      /* Trailing 4-week average, smoother base for growth calc */
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
  e.trades,
  e.cumulative_volume_usd,

  /* Raw WoW, kept for reference; can spike to thousands of % */
  CASE
    WHEN e.prev_week_vol > 0
      THEN (e.volume_usd / e.prev_week_vol - 1) * 100.0
  END AS wow_pct_raw,

  /* WoW with min-base filter, NULL until prior week >= MIN_BASE_USD */
  CASE
    WHEN e.prev_week_vol >= k.min_base_usd
      THEN (e.volume_usd / e.prev_week_vol - 1) * 100.0
  END AS wow_pct,

  /* Same WoW but capped to +/-WOW_CAP_PCT for the cleanest bar chart */
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
