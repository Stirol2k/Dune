-- =============================================================================
-- USDC pool yield dashboard — LiquoriceSettlement, multi-chain.
--
-- Contract:
--   0x0448633eb8b0a42efed924c42069e0dcf08fb552
--   (LiquoriceSettlement, deployed via Deterministic Deployer — same address
--    on every supported chain.)
--
-- Chains and USDC variants tracked:
--   ethereum:
--     • USDC                (Circle)   0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48  (6 decimals)
--   arbitrum:
--     • USDC native         (Circle)   0xaf88d065e77c8cc2239327c5edb3a432268e5831  (6 decimals)
--     • USDC.e bridged      (legacy)   0xff970a61a04b1ca14834a43f5de4533ebddb5cc8  (6 decimals)
--   Native USDC and USDC.e are tracked together — both denote the same
--   $1-pegged exposure for the settlement contract.
--
-- What we compute (per UTC day × chain, full history → today):
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
--      hidden mints/burns of accounting tokens. Computed per chain; the 'all'
--      row sums across chains.
--   2. "USDC volume" counts every USDC leg touching the pool (from = pool OR
--      to = pool). For a market-maker that processes USDC↔X swaps this is
--      exactly the USDC-side trade volume; LP deposits/withdrawals are
--      typically tiny vs. trading volume so they don't materially distort APR.
--   3. Spread is a free parameter — change the bps in CTE `spread_assumptions`
--      to match your real execution data from the bot.
--
-- Output: one row per (day_utc, chain), plus a synthetic 'all' chain.
-- =============================================================================

WITH
  pool AS (
    SELECT from_hex('0448633eb8b0a42efed924c42069e0dcf08fb552') AS addr
  ),

  /* All USDC token addresses we want to track, per chain. Add more chains
     by appending rows here AND adding the chain to `chains` / `WHERE blockchain IN`. */
  usdc_addrs AS (
    SELECT 'ethereum' AS chain, from_hex('a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') AS addr
    UNION ALL
    SELECT 'arbitrum',          from_hex('af88d065e77c8cc2239327c5edb3a432268e5831')          /* USDC native */
    UNION ALL
    SELECT 'arbitrum',          from_hex('ff970a61a04b1ca14834a43f5de4533ebddb5cc8')          /* USDC.e bridged */
  ),

  chains AS (
    SELECT 'ethereum' AS chain UNION ALL
    SELECT 'arbitrum'
  ),

  /* LiquoriceSettlement deployed 2025-09-09 — keep floor a bit before that */
  scan_floor AS (
    SELECT DATE '2025-09-01' AS floor_d
  ),

  /* Spread assumptions in basis points (1 bp = 0.01%).
     Tune these to match what your MM bot actually captures on USDC↔X spreads. */
  spread_assumptions AS (
    SELECT
      CAST( 2 AS double) AS conservative_bps,  /* tight USDC/USDT */
      CAST( 5 AS double) AS realistic_bps,     /* typical MM capture */
      CAST(10 AS double) AS optimistic_bps     /* wider USDC↔volatile pairs */
  ),

  /* Every USDC transfer leg touching the pool, on any tracked chain */
  legs AS (
    SELECT
      t.blockchain AS chain,
      t.block_time,
      t.tx_hash,
      t."from" AS f,
      t."to"   AS tto,
      COALESCE(t.amount_usd, 0) AS amt_usd
    FROM tokens.transfers t
    JOIN usdc_addrs ua
      ON ua.chain = t.blockchain
     AND ua.addr  = t.contract_address
    CROSS JOIN pool p
    CROSS JOIN scan_floor s
    WHERE t.blockchain IN ('ethereum', 'arbitrum')
      AND t.block_time >= CAST(s.floor_d AS timestamp)
      AND (t."from" = p.addr OR t."to" = p.addr)
  ),

  /* Aggregate to per-(chain, tx) in/out so a swap that moved USDC both
     directions in the same tx (rare) is collapsed cleanly. tx_hash is NOT
     globally unique across chains — chain MUST be in the GROUP BY. */
  by_tx AS (
    SELECT
      l.chain,
      l.tx_hash,
      MIN(l.block_time) AS block_time_min,
      SUM(CASE WHEN l.tto = p.addr THEN l.amt_usd ELSE 0 END) AS in_usd,
      SUM(CASE WHEN l.f   = p.addr THEN l.amt_usd ELSE 0 END) AS out_usd
    FROM legs l
    CROSS JOIN pool p
    GROUP BY l.chain, l.tx_hash
  ),

  daily_raw AS (
    SELECT
      chain,
      CAST(block_time_min AS date) AS day_utc,
      SUM(in_usd)                       AS inflow_usdc,
      SUM(out_usd)                      AS outflow_usdc,
      SUM(in_usd) + SUM(out_usd)        AS volume_usdc,
      SUM(in_usd) - SUM(out_usd)        AS net_flow_usdc
    FROM by_tx
    GROUP BY chain, CAST(block_time_min AS date)
  ),

  /* Auto window: first day with USDC activity on ANY chain → today (UTC) */
  win AS (
    SELECT
      (SELECT MIN(day_utc) FROM daily_raw) AS start_d,
      current_date                          AS end_d
  ),

  /* Continuous (chain × day) axis so rolling windows are well-defined */
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
      COALESCE(d.inflow_usdc,   0) AS inflow_usdc,
      COALESCE(d.outflow_usdc,  0) AS outflow_usdc,
      COALESCE(d.volume_usdc,   0) AS volume_usdc,
      COALESCE(d.net_flow_usdc, 0) AS net_flow_usdc
    FROM axis a
    LEFT JOIN daily_raw d
      ON d.chain   = a.chain
     AND d.day_utc = a.day_utc
  ),

  /* Synthetic 'all' chain = sum across chains, computed BEFORE rolling
     windows so cumulative TVL across chains is computed correctly */
  with_all AS (
    SELECT chain, day_utc, inflow_usdc, outflow_usdc, volume_usdc, net_flow_usdc
    FROM daily_full
    UNION ALL
    SELECT
      'all' AS chain,
      day_utc,
      SUM(inflow_usdc)   AS inflow_usdc,
      SUM(outflow_usdc)  AS outflow_usdc,
      SUM(volume_usdc)   AS volume_usdc,
      SUM(net_flow_usdc) AS net_flow_usdc
    FROM daily_full
    GROUP BY day_utc
  ),

  /* Step 1: cumulative TVL + rolling sums, partitioned by chain */
  rolling AS (
    SELECT
      chain,
      day_utc,
      inflow_usdc,
      outflow_usdc,
      volume_usdc,
      net_flow_usdc,

      /* TVL = cumulative net USDC flow (≈ current pool USDC balance) */
      SUM(net_flow_usdc) OVER (
        PARTITION BY chain
        ORDER BY day_utc
      ) AS tvl_usdc,

      /* Trailing-window volume (annualization base) */
      SUM(volume_usdc) OVER (
        PARTITION BY chain
        ORDER BY day_utc
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS volume_7d_usdc,
      SUM(volume_usdc) OVER (
        PARTITION BY chain
        ORDER BY day_utc
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS volume_30d_usdc
    FROM with_all
  ),

  /* Step 2: rolling avg of TVL (Trino can't nest window funcs, so 2nd CTE) */
  rolling2 AS (
    SELECT
      r.*,
      AVG(tvl_usdc) OVER (
        PARTITION BY chain
        ORDER BY day_utc
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS avg_tvl_30d_usdc
    FROM rolling r
  )

SELECT
  r.day_utc,
  r.chain,

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
ORDER BY r.day_utc, r.chain
