-- =============================================================================
-- Aggregated traded amounts per token, decoded from the TradeOrder event of
-- every LiquoriceSettlement version, multi-chain.
--
-- For each settled order the base amount is attributed to the base token and
-- the quote amount to the quote token, then summed per (chain, token). This is
-- the "how much of each asset went through Liquorice" table (token units and
-- USD), lifetime, all settlement versions.
--
-- Contracts and event layouts: see dune_trades_history.sql (same decoding).
--   pre-v1 (2024-12)  0x9c4cbaa092abe0e67b89558bf21bd9be76c8fcaf
--   pre-v1 (2025-02)  0xaca684a3f64e0eae4812b734e3f8f205d3eed167
--   v1                0x0448633eb8b0a42efed924c42069e0dcf08fb552
--   v1.5              0x43dcd6586e6209ee7235a21bdc4aa301e5bc44e8
--
-- Output: one row per (chain, token) plus a chain = 'all' rollup, ordered by
-- USD amount. Columns:
--   * chain          'ethereum' | 'arbitrum' | 'all'
--   * symbol, token
--   * amount         token units (base + quote legs)
--   * amount_usd     USD at prices.day on the trade day
--   * trades         number of TradeOrder events that touched the token
-- =============================================================================

WITH
  settlements AS (
    SELECT from_hex('9c4cbaa092abe0e67b89558bf21bd9be76c8fcaf') AS addr, 'pre-v1 (2024-12)' AS version
    UNION ALL
    SELECT from_hex('aca684a3f64e0eae4812b734e3f8f205d3eed167'),          'pre-v1 (2025-02)'
    UNION ALL
    SELECT from_hex('0448633eb8b0a42efed924c42069e0dcf08fb552'),          'v1'
    UNION ALL
    SELECT from_hex('43dcd6586e6209ee7235a21bdc4aa301e5bc44e8'),          'v1.5'
  ),

  topics AS (
    SELECT 0x0fce007c38c6c8ed9e545b3a148095762738618f8c21b673222613e4d45734b6 AS topic0, 'v1'   AS layout
    UNION ALL
    SELECT 0x26357f6024689f750d018696a0e2ff7bf56b6fb3ec684bc83a5b84e635d6846f,          'v1.5'
  ),

  scan_floor AS (
    SELECT DATE '2024-12-01' AS floor_d
  ),

  raw_logs AS (
    SELECT 'ethereum' AS chain, l.block_time, l.tx_hash, l.index AS evt_index,
           l.contract_address, l.topic0, l.data
    FROM ethereum.logs l
    CROSS JOIN scan_floor s
    WHERE l.block_time >= CAST(s.floor_d AS timestamp)
      AND l.contract_address IN (SELECT addr FROM settlements)
      AND l.topic0 IN (SELECT topic0 FROM topics)
    UNION ALL
    SELECT 'arbitrum' AS chain, l.block_time, l.tx_hash, l.index AS evt_index,
           l.contract_address, l.topic0, l.data
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

  /* Unpivot: one row per (trade, token side) */
  legs AS (
    SELECT chain, day_utc, tx_hash, evt_index, base_token  AS token, base_amount_raw  AS amount_raw FROM decoded
    UNION ALL
    SELECT chain, day_utc, tx_hash, evt_index, quote_token AS token, quote_amount_raw AS amount_raw FROM decoded
  ),

  erc20 AS (
    SELECT blockchain AS chain, contract_address AS token, symbol, decimals
    FROM tokens.erc20
    WHERE blockchain IN ('ethereum', 'arbitrum')
  ),

  px AS (
    SELECT
      p.blockchain                AS chain,
      p.contract_address          AS token,
      CAST(p."timestamp" AS date) AS day_utc,
      MAX(p.price)                AS price_usd,
      MAX(p.symbol)               AS symbol
    FROM prices.day p
    CROSS JOIN scan_floor s
    WHERE p.blockchain IN ('ethereum', 'arbitrum')
      AND p."timestamp" >= CAST(s.floor_d AS timestamp)
      AND p.contract_address IN (SELECT DISTINCT token FROM legs)
    GROUP BY p.blockchain, p.contract_address, CAST(p."timestamp" AS date)
  ),

  valued AS (
    SELECT
      l.chain,
      l.token,
      COALESCE(e.symbol, px.symbol)                                   AS symbol,
      CAST(l.amount_raw AS double) / power(10, COALESCE(e.decimals, 18)) AS amount,
      CAST(l.amount_raw AS double) / power(10, COALESCE(e.decimals, 18)) * px.price_usd AS amount_usd,
      l.tx_hash,
      l.evt_index
    FROM legs l
    LEFT JOIN erc20 e ON e.chain = l.chain AND e.token = l.token
    LEFT JOIN px      ON px.chain = l.chain AND px.token = l.token AND px.day_utc = l.day_utc
  ),

  per_chain AS (
    SELECT
      chain,
      symbol,
      token,
      SUM(amount)      AS amount,
      SUM(amount_usd)  AS amount_usd,
      COUNT(*)         AS trades
    FROM valued
    GROUP BY chain, symbol, token
  )

SELECT chain, symbol, token, amount, amount_usd, trades
FROM per_chain
UNION ALL
/* Cross-chain rollup by symbol (token address differs per chain, so it is blank here) */
SELECT 'all' AS chain, symbol, CAST(NULL AS varbinary) AS token, SUM(amount), SUM(amount_usd), SUM(trades)
FROM per_chain
GROUP BY symbol
ORDER BY chain, amount_usd DESC NULLS LAST
