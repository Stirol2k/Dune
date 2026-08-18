-- =============================================================================
-- Trades history: one row per settled RFQ order, decoded from the TradeOrder
-- event of every LiquoriceSettlement version, multi-chain.
--
-- Contracts (LiquoriceSettlement, deployed via Deterministic Deployer / CREATE2,
-- same address on every supported chain):
--   pre-v1 (2024-12)  0x9c4cbaa092abe0e67b89558bf21bd9be76c8fcaf   Ethereum only, 2024-12-05
--   pre-v1 (2025-02)  0xaca684a3f64e0eae4812b734e3f8f205d3eed167   2025-02-13
--   v1                0x0448633eb8b0a42efed924c42069e0dcf08fb552   2025-09-09
--   v1.5              0x43dcd6586e6209ee7235a21bdc4aa301e5bc44e8   2026-08-05
--
-- Event layouts (rfqId is an indexed string, so only its keccak hash lands in
-- topic1 and the plain-text id is not recoverable from the log):
--   pre-v1 / v1
--     TradeOrder(string indexed rfqId, address trader, address effectiveTrader,
--                address baseToken, address quoteToken,
--                uint256 baseTokenAmount, uint256 quoteTokenAmount, address recipient)
--     topic0 = 0x0fce007c38c6c8ed9e545b3a148095762738618f8c21b673222613e4d45734b6
--     data words: 0 trader | 1 effectiveTrader | 2 baseToken | 3 quoteToken
--                 4 baseTokenAmount | 5 quoteTokenAmount | 6 recipient
--   v1.5 (adds the maker `signer` between effectiveTrader and baseToken)
--     TradeOrder(string indexed rfqId, address trader, address effectiveTrader,
--                address signer, address baseToken, address quoteToken,
--                uint256 baseTokenAmount, uint256 quoteTokenAmount, address recipient)
--     topic0 = 0x26357f6024689f750d018696a0e2ff7bf56b6fb3ec684bc83a5b84e635d6846f
--     data words: 0 trader | 1 effectiveTrader | 2 signer | 3 baseToken | 4 quoteToken
--                 5 baseTokenAmount | 6 quoteTokenAmount | 7 recipient
--
-- Decoding: word i (0-based) sits at bytes [32*i+1, 32*i+32] of `data`
-- (varbinary_substring is 1-indexed). Addresses are the last 20 bytes of the
-- word, uint256 is the whole word.
--
-- Amounts are decimal-adjusted via tokens.erc20 and valued with prices.day
-- (base side first, quote side as fallback). USD here is trade-level and may
-- differ slightly from the transfer-based volume queries, which use Dune's
-- per-transfer oracle. The transfer-based queries remain the canonical volume
-- source; this table is for trade-level detail.
--
-- Output: newest first. `chain` = 'ethereum' | 'arbitrum'.
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

  /* Oldest settlement deployed 2024-12-05, keep floor a bit before that. */
  scan_floor AS (
    SELECT DATE '2024-12-01' AS floor_d
  ),

  raw_logs AS (
    SELECT 'ethereum' AS chain, l.block_time, l.block_number, l.tx_hash, l.index AS evt_index,
           l.contract_address, l.topic0, l.topic1, l.data
    FROM ethereum.logs l
    CROSS JOIN scan_floor s
    WHERE l.block_time >= CAST(s.floor_d AS timestamp)
      AND l.contract_address IN (SELECT addr FROM settlements)
      AND l.topic0 IN (SELECT topic0 FROM topics)
    UNION ALL
    SELECT 'arbitrum' AS chain, l.block_time, l.block_number, l.tx_hash, l.index AS evt_index,
           l.contract_address, l.topic0, l.topic1, l.data
    FROM arbitrum.logs l
    CROSS JOIN scan_floor s
    WHERE l.block_time >= CAST(s.floor_d AS timestamp)
      AND l.contract_address IN (SELECT addr FROM settlements)
      AND l.topic0 IN (SELECT topic0 FROM topics)
  ),

  decoded AS (
    SELECT
      r.chain,
      r.block_time,
      r.block_number,
      r.tx_hash,
      r.evt_index,
      s.version                                   AS settlement_version,
      r.contract_address                          AS settlement,
      r.topic1                                    AS rfq_id_hash,
      varbinary_substring(r.data,  13, 20)        AS trader,
      varbinary_substring(r.data,  45, 20)        AS effective_trader,
      CASE WHEN t.layout = 'v1.5'
           THEN varbinary_substring(r.data,  77, 20) END AS maker_signer,
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
           ELSE varbinary_to_uint256(varbinary_substring(r.data, 161, 32)) END AS quote_amount_raw,
      CASE WHEN t.layout = 'v1.5'
           THEN varbinary_substring(r.data, 237, 20)
           ELSE varbinary_substring(r.data, 205, 20) END AS recipient
    FROM raw_logs r
    JOIN settlements s ON s.addr   = r.contract_address
    JOIN topics      t ON t.topic0 = r.topic0
  ),

  /* Token metadata for decimal adjustment; symbols fall back to prices.day */
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
      AND p.contract_address IN (
            SELECT base_token  FROM decoded
            UNION
            SELECT quote_token FROM decoded)
    GROUP BY p.blockchain, p.contract_address, CAST(p."timestamp" AS date)
  ),

  enriched AS (
    SELECT
      d.*,
      COALESCE(eb.symbol, pb.symbol) AS base_symbol,
      COALESCE(eq.symbol, pq.symbol) AS quote_symbol,
      CAST(d.base_amount_raw  AS double) / power(10, COALESCE(eb.decimals, 18)) AS base_amount,
      CAST(d.quote_amount_raw AS double) / power(10, COALESCE(eq.decimals, 18)) AS quote_amount,
      pb.price_usd AS base_price_usd,
      pq.price_usd AS quote_price_usd
    FROM decoded d
    LEFT JOIN erc20 eb ON eb.chain = d.chain AND eb.token = d.base_token
    LEFT JOIN erc20 eq ON eq.chain = d.chain AND eq.token = d.quote_token
    LEFT JOIN px pb ON pb.chain = d.chain AND pb.token = d.base_token  AND pb.day_utc = CAST(d.block_time AS date)
    LEFT JOIN px pq ON pq.chain = d.chain AND pq.token = d.quote_token AND pq.day_utc = CAST(d.block_time AS date)
  )

SELECT
  block_time,
  chain,
  settlement_version,
  tx_hash,
  evt_index,
  trader,
  effective_trader,
  maker_signer,
  recipient,
  base_symbol,
  base_token,
  base_amount,
  quote_symbol,
  quote_token,
  quote_amount,
  COALESCE(base_amount * base_price_usd, quote_amount * quote_price_usd) AS trade_usd,
  rfq_id_hash,
  block_number
FROM enriched
ORDER BY block_time DESC, chain, evt_index DESC
