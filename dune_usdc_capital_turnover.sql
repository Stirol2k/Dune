-- =============================================================================
-- USDC capital turnover: settled USDC volume vs lending-pool USDC TVL, multi-chain.
--
-- Two contract families, both deployed via Deterministic Deployer / CREATE2
-- (same address on every supported chain):
--
--   Volume side, LiquoriceSettlement (all versions tracked):
--     v1    0x0448633eb8b0a42efed924c42069e0dcf08fb552   live 2025-09-09
--     v1.5  0x43dcd6586e6209ee7235a21bdc4aa301e5bc44e8   live 2026-08-05
--
--   Capital side, LendingPool (all versions summed):
--     v1    0x046ffb0dfde6a21b4fc609841f55c31b6297cfb8   live 2025-02-21
--     v1.5  0x1cce0034331638fec50b291a0bf42073346bb7c0   live 2026-08-05
--
-- Why two contracts: the settlement is atomic, its own USDC balance nets to
-- about zero at the end of every tx, so TVL cannot be read there. LP capital
-- sits in the LendingPool; trades are recorded by the settlement's TradeOrder
-- event. Turnover is the ratio of the two.
--
-- Chains and USDC variants tracked:
--   ethereum:
--     * USDC                (Circle)   0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48  (6 decimals)
--   arbitrum:
--     * USDC native         (Circle)   0xaf88d065e77c8cc2239327c5edb3a432268e5831  (6 decimals)
--     * USDC.e bridged      (legacy)   0xff970a61a04b1ca14834a43f5de4533ebddb5cc8  (6 decimals)
--   Native USDC and USDC.e are tracked together, both denote the same
--   $1-pegged exposure.
--
-- What we compute (per UTC day x chain, full history -> today):
--   * usdc_volume                   daily USDC volume settled through the protocol, $.
--                                   Basis: TradeOrder events (one per settled order); the
--                                   USDC side of each order (base if base is USDC, else
--                                   quote) x prices.day. Same basis as dune_daily_volume.sql.
--   * pool_usdc_inflow / outflow    daily USDC moved into / out of the lending pools, $
--   * pool_usdc_tvl                 cumulative net pool flow = pool USDC balance, $ (net TVL: supplied minus borrowed out)
--   * avg_tvl_30d_usdc              30-day rolling avg TVL (smoother base)
--   * volume_7d_usdc / volume_30d_usdc  trailing USDC volume
--   * capital_turnover_annualized   (trailing-30d volume x 365/30) / avg 30d TVL
--
-- Caveat: the numerator is all USDC settled through Liquorice, including
-- orders a maker filled from its own wallet without touching the pool. That
-- matches how cumulative "capital reuse" is quoted in company materials
-- (total settled volume over live LP TVL).
--
-- Turnover is a capital-efficiency metric (how many times a dollar of LP
-- capital settles trades per year), not a yield figure. It swings with weekly
-- volume, so quote it with an as-of date.
--
-- Output: one row per (day_utc, chain), plus a synthetic 'all' chain.
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

  pools AS (
    SELECT from_hex('046ffb0dfde6a21b4fc609841f55c31b6297cfb8') AS addr, 'v1'   AS pool_version
    UNION ALL
    SELECT from_hex('1cce0034331638fec50b291a0bf42073346bb7c0'),          'v1.5'
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

  /* LendingPool v1 deployed 2025-02-21, the oldest of the tracked contracts. */
  scan_floor AS (
    SELECT DATE '2025-02-01' AS floor_d
  ),

  /* ---------------- volume side: TradeOrder events ---------------- */
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

  usdc_px AS (
    SELECT
      p.blockchain                AS chain,
      p.contract_address          AS token,
      CAST(p."timestamp" AS date) AS day_utc,
      MAX(p.price)                AS price_usd
    FROM prices.day p
    JOIN usdc_addrs ua ON ua.chain = p.blockchain AND ua.addr = p.contract_address
    CROSS JOIN scan_floor s
    WHERE p.blockchain IN ('ethereum', 'arbitrum')
      AND p."timestamp" >= CAST(s.floor_d AS timestamp)
    GROUP BY p.blockchain, p.contract_address, CAST(p."timestamp" AS date)
  ),

  /* USDC side of every order: base leg if base is USDC, else quote leg if
     quote is USDC. All tracked USDC variants have 6 decimals. */
  usdc_trades AS (
    SELECT
      d.chain,
      d.day_utc,
      CASE
        WHEN ub.addr IS NOT NULL THEN CAST(d.base_amount_raw  AS double) / 1e6 * COALESCE(pb.price_usd, 1)
        WHEN uq.addr IS NOT NULL THEN CAST(d.quote_amount_raw AS double) / 1e6 * COALESCE(pq.price_usd, 1)
      END AS usdc_usd
    FROM decoded d
    LEFT JOIN usdc_addrs ub ON ub.chain = d.chain AND ub.addr = d.base_token
    LEFT JOIN usdc_addrs uq ON uq.chain = d.chain AND uq.addr = d.quote_token
    LEFT JOIN usdc_px pb ON pb.chain = d.chain AND pb.token = d.base_token  AND pb.day_utc = d.day_utc
    LEFT JOIN usdc_px pq ON pq.chain = d.chain AND pq.token = d.quote_token AND pq.day_utc = d.day_utc
    WHERE ub.addr IS NOT NULL OR uq.addr IS NOT NULL
  ),

  volume_daily AS (
    SELECT chain, day_utc, SUM(usdc_usd) AS usdc_volume
    FROM usdc_trades
    GROUP BY chain, day_utc
  ),

  /* ---------------- capital side: lending pool USDC flows ---------------- */
  pool_legs AS (
    SELECT
      t.blockchain AS chain,
      CAST(t.block_time AS date) AS day_utc,
      CASE WHEN t."to"   IN (SELECT addr FROM pools)
            AND t."from" NOT IN (SELECT addr FROM pools)
           THEN COALESCE(t.amount_usd, 0) ELSE 0 END AS pool_in_usd,
      CASE WHEN t."from" IN (SELECT addr FROM pools)
            AND t."to"   NOT IN (SELECT addr FROM pools)
           THEN COALESCE(t.amount_usd, 0) ELSE 0 END AS pool_out_usd
    FROM tokens.transfers t
    JOIN usdc_addrs ua
      ON ua.chain = t.blockchain
     AND ua.addr  = t.contract_address
    CROSS JOIN scan_floor s
    WHERE t.blockchain IN ('ethereum', 'arbitrum')
      AND t.block_time >= CAST(s.floor_d AS timestamp)
      AND (   t."from" IN (SELECT addr FROM pools)
           OR t."to"   IN (SELECT addr FROM pools))
  ),

  pool_daily AS (
    SELECT
      chain,
      day_utc,
      SUM(pool_in_usd)                    AS pool_usdc_inflow,
      SUM(pool_out_usd)                   AS pool_usdc_outflow,
      SUM(pool_in_usd) - SUM(pool_out_usd) AS pool_net_flow_usdc
    FROM pool_legs
    GROUP BY chain, day_utc
  ),

  daily_raw AS (
    SELECT
      COALESCE(v.chain, p.chain)     AS chain,
      COALESCE(v.day_utc, p.day_utc) AS day_utc,
      COALESCE(v.usdc_volume, 0)        AS usdc_volume,
      COALESCE(p.pool_usdc_inflow, 0)   AS pool_usdc_inflow,
      COALESCE(p.pool_usdc_outflow, 0)  AS pool_usdc_outflow,
      COALESCE(p.pool_net_flow_usdc, 0) AS pool_net_flow_usdc
    FROM volume_daily v
    FULL OUTER JOIN pool_daily p
      ON p.chain = v.chain AND p.day_utc = v.day_utc
  ),

  /* Auto window: first day with USDC activity on ANY chain -> today (UTC) */
  win AS (
    SELECT
      (SELECT MIN(day_utc) FROM daily_raw) AS start_d,
      current_date                          AS end_d
  ),

  /* Continuous (chain x day) axis so rolling windows are well-defined */
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
      COALESCE(d.usdc_volume, 0) AS usdc_volume,
      COALESCE(d.pool_usdc_inflow,   0) AS pool_usdc_inflow,
      COALESCE(d.pool_usdc_outflow,  0) AS pool_usdc_outflow,
      COALESCE(d.pool_net_flow_usdc, 0) AS pool_net_flow_usdc
    FROM axis a
    LEFT JOIN daily_raw d
      ON d.chain   = a.chain
     AND d.day_utc = a.day_utc
  ),

  /* Synthetic 'all' chain = sum across chains, computed BEFORE rolling
     windows so cumulative TVL across chains is computed correctly */
  with_all AS (
    SELECT chain, day_utc, usdc_volume, pool_usdc_inflow, pool_usdc_outflow, pool_net_flow_usdc
    FROM daily_full
    UNION ALL
    SELECT
      'all' AS chain,
      day_utc,
      SUM(usdc_volume),
      SUM(pool_usdc_inflow),
      SUM(pool_usdc_outflow),
      SUM(pool_net_flow_usdc)
    FROM daily_full
    GROUP BY day_utc
  ),

  /* Step 1: cumulative TVL + rolling sums, partitioned by chain */
  rolling AS (
    SELECT
      chain,
      day_utc,
      usdc_volume,
      pool_usdc_inflow,
      pool_usdc_outflow,
      pool_net_flow_usdc,

      /* Net TVL = cumulative net USDC flow into the pools = pool USDC balance */
      GREATEST(SUM(pool_net_flow_usdc) OVER (
        PARTITION BY chain
        ORDER BY day_utc
      ), 0) AS pool_usdc_tvl,

      /* Trailing-window USDC volume (annualization base) */
      SUM(usdc_volume) OVER (
        PARTITION BY chain
        ORDER BY day_utc
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS volume_7d_usdc,
      SUM(usdc_volume) OVER (
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
      AVG(pool_usdc_tvl) OVER (
        PARTITION BY chain
        ORDER BY day_utc
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS avg_tvl_30d_usdc
    FROM rolling r
  )

SELECT
  r.day_utc,
  r.chain,

  /* --- volume side --- */
  r.usdc_volume,
  r.volume_7d_usdc,
  r.volume_30d_usdc,
  r.volume_30d_usdc / 30.0 * 365.0 AS volume_annualized_usdc,

  /* --- lending pool side --- */
  r.pool_usdc_inflow,
  r.pool_usdc_outflow,
  r.pool_net_flow_usdc,
  r.pool_usdc_tvl,
  r.avg_tvl_30d_usdc,

  /* --- Capital turnover: how many times pool USDC recycles per year --- */
  CASE WHEN r.avg_tvl_30d_usdc > 0
    THEN (r.volume_30d_usdc / 30.0 * 365.0) / r.avg_tvl_30d_usdc
  END AS capital_turnover_annualized
FROM rolling2 r
ORDER BY r.day_utc, r.chain
