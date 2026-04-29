-- =============================================================================
-- USDC pool yield dashboard — LendingPool, always up to date.
--
-- Contract:  0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8
-- Asset:     USDC  (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 6 decimals)
--
-- What we compute (per UTC day, full history → today):
--   • usdc_inflow / usdc_outflow / usdc_volume — daily USDC throughput, $
--   • usdc_tvl                       — cumulative net flow = pool USDC balance, $
--   • avg_tvl_30d_usdc               — 30-day rolling avg TVL (smoother base)
--   • volume_30d_usdc                — trailing 30d USDC volume
--   • capital_turnover_annualized    — how many times TVL turns over per year
--                                      (computed from trailing-30d volume)
--   • implied_apr_<X>bps_pct         — annualized yield assuming X bps spread
--                                      X ∈ {2, 5, 10} → conservative / realistic / optimistic
--
-- Yield formula (industry standard for MM pools):
--   APR_fraction  = annualized_volume × spread_fraction / TVL
--   APR_percent   = capital_turnover_annualized × spread_bps / 100
--
-- Assumptions to be transparent about:
--   1. TVL = cumulative net USDC flow into the pool (inflow − outflow), summed
--      from genesis. Equals the pool's USDC balance if the contract has no
--      hidden mints/burns of accounting tokens.
--   2. "USDC volume" counts every USDC leg touching the pool (from = pool OR
--      to = pool). For a market-maker that processes USDC↔X swaps this is
--      exactly the USDC-side trade volume; LP deposits/withdrawals are
--      typically tiny vs. trading volume so they don't materially distort APR.
--   3. Spread is a free parameter — change the bps in CTE `spread_assumptions`
--      to match your real execution data from the bot.
--
-- Pricing: amount_usd column from Dune's tokens.transfers (USDC ≈ $1 always).
-- =============================================================================

WITH
  pool AS (
    SELECT from_hex('046ffb0dfde6a21b4fc609841f55c31b6297cfb8') AS addr
  ),

  usdc AS (
    SELECT from_hex('a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') AS addr
  ),

  /* Safety lower bound for tokens.transfers scan — purely cost optimization */
  scan_floor AS (
    SELECT DATE '2024-01-01' AS floor_d
  ),

  /* Spread assumptions in basis points (1 bp = 0.01%).
     Tune these to match what your MM bot actually captures on USDC↔X spreads. */
  spread_assumptions AS (
    SELECT
      CAST( 2 AS double) AS conservative_bps,  /* tight USDC/USDT */
      CAST( 5 AS double) AS realistic_bps,     /* typical MM capture */
      CAST(10 AS double) AS optimistic_bps     /* wider USDC↔volatile pairs */
  ),

  /* Every USDC transfer leg touching the pool */
  legs AS (
    SELECT
      t.block_time,
      t.tx_hash,
      t."from" AS f,
      t."to"   AS tto,
      COALESCE(t.amount_usd, 0) AS amt_usd
    FROM tokens.transfers t
    CROSS JOIN pool p
    CROSS JOIN usdc u
    CROSS JOIN scan_floor s
    WHERE t.blockchain = 'ethereum'
      AND t.block_time >= CAST(s.floor_d AS timestamp)
      AND t.contract_address = u.addr
      AND (t."from" = p.addr OR t."to" = p.addr)
  ),

  /* Aggregate to per-tx in/out (so a swap that moved USDC both directions
     in the same tx — rare — is collapsed cleanly) */
  by_tx AS (
    SELECT
      l.tx_hash,
      MIN(l.block_time) AS block_time_min,
      SUM(CASE WHEN l.tto = p.addr THEN l.amt_usd ELSE 0 END) AS in_usd,
      SUM(CASE WHEN l.f   = p.addr THEN l.amt_usd ELSE 0 END) AS out_usd
    FROM legs l
    CROSS JOIN pool p
    GROUP BY l.tx_hash
  ),

  daily_raw AS (
    SELECT
      CAST(block_time_min AS date) AS day_utc,
      SUM(in_usd)                          AS inflow_usdc,
      SUM(out_usd)                         AS outflow_usdc,
      SUM(in_usd) + SUM(out_usd)           AS volume_usdc,
      SUM(in_usd) - SUM(out_usd)           AS net_flow_usdc
    FROM by_tx
    GROUP BY 1
  ),

  /* Auto window: first day with USDC activity → today (UTC) */
  win AS (
    SELECT
      (SELECT MIN(day_utc) FROM daily_raw) AS start_d,
      current_date                          AS end_d
  ),

  /* Continuous date axis so rolling windows and bar charts have no gaps */
  day_axis AS (
    SELECT CAST(date_add('day', s, w.start_d) AS date) AS day_utc
    FROM win w
    CROSS JOIN UNNEST(
      sequence(0, CAST(date_diff('day', w.start_d, w.end_d) AS bigint))
    ) AS u(s)
  ),

  daily_full AS (
    SELECT
      a.day_utc,
      COALESCE(d.inflow_usdc,   0) AS inflow_usdc,
      COALESCE(d.outflow_usdc,  0) AS outflow_usdc,
      COALESCE(d.volume_usdc,   0) AS volume_usdc,
      COALESCE(d.net_flow_usdc, 0) AS net_flow_usdc
    FROM day_axis a
    LEFT JOIN daily_raw d
      ON d.day_utc = a.day_utc
  ),

  /* Step 1: cumulative TVL + rolling sums */
  rolling AS (
    SELECT
      day_utc,
      inflow_usdc,
      outflow_usdc,
      volume_usdc,
      net_flow_usdc,

      /* TVL = cumulative net USDC flow (≈ current pool USDC balance) */
      SUM(net_flow_usdc) OVER (ORDER BY day_utc) AS tvl_usdc,

      /* Trailing-window volume (annualization base) */
      SUM(volume_usdc) OVER (
        ORDER BY day_utc
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS volume_7d_usdc,
      SUM(volume_usdc) OVER (
        ORDER BY day_utc
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS volume_30d_usdc
    FROM daily_full
  ),

  /* Step 2: rolling avg of TVL (Trino can't nest window funcs, so 2nd CTE) */
  rolling2 AS (
    SELECT
      r.*,
      AVG(tvl_usdc) OVER (
        ORDER BY day_utc
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS avg_tvl_30d_usdc
    FROM rolling r
  )

SELECT
  r.day_utc,

  /* --- raw daily flows --- */
  r.inflow_usdc,
  r.outflow_usdc,
  r.volume_usdc,
  r.net_flow_usdc,

  /* --- balances --- */
  r.tvl_usdc,
  r.avg_tvl_30d_usdc,

  /* --- rolling volume --- */
  r.volume_7d_usdc,
  r.volume_30d_usdc,

  /* --- annualized volume (forward-looking estimate) --- */
  r.volume_30d_usdc / 30.0 * 365.0 AS volume_annualized_usdc,

  /* --- Capital turnover: how many times TVL recycles per year ---
     (a single number that already captures "capital efficiency") */
  CASE WHEN r.avg_tvl_30d_usdc > 0
    THEN (r.volume_30d_usdc / 30.0 * 365.0) / r.avg_tvl_30d_usdc
  END AS capital_turnover_annualized,

  /* --- Implied APR (%) at different spread assumptions ---
     APR% = capital_turnover × spread_bps / 100                              */
  CASE WHEN r.avg_tvl_30d_usdc > 0
    THEN (r.volume_30d_usdc / 30.0 * 365.0) / r.avg_tvl_30d_usdc
         * s.conservative_bps / 100.0
  END AS implied_apr_2bps_pct,

  CASE WHEN r.avg_tvl_30d_usdc > 0
    THEN (r.volume_30d_usdc / 30.0 * 365.0) / r.avg_tvl_30d_usdc
         * s.realistic_bps / 100.0
  END AS implied_apr_5bps_pct,

  CASE WHEN r.avg_tvl_30d_usdc > 0
    THEN (r.volume_30d_usdc / 30.0 * 365.0) / r.avg_tvl_30d_usdc
         * s.optimistic_bps / 100.0
  END AS implied_apr_10bps_pct
FROM rolling2 r
CROSS JOIN spread_assumptions s
ORDER BY r.day_utc
