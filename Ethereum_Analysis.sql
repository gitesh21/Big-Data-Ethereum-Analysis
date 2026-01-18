/*
Project: Ethereum Blockchain Analysis
Description: SQL Logic equivalent to the PySpark RDD pipelines implemented in the main project.
Goal: Analyze transaction volume, identify top smart contracts, and track scam activity.
*/

-- 1. TIME ANALYSIS: Transaction Volume by Month
-- Corresponds to Part A of the report (Time Analysis)
SELECT 
    DATE_TRUNC('month', block_timestamp) AS month_year,
    COUNT(transaction_hash) AS total_transactions,
    AVG(value) AS avg_transaction_value
FROM transactions
GROUP BY 1
ORDER BY 1 DESC;

-- 2. TOP 10 SMART CONTRACTS BY VALUE
-- Corresponds to Part B (Top Ten Most Popular Services)
-- Logic: JOIN transactions with contracts to find where money is going
SELECT 
    c.address AS contract_address,
    SUM(t.value) AS total_ether_received
FROM contracts c
JOIN transactions t ON c.address = t.to_address
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- 3. MINER ACTIVITY ANALYSIS
-- Corresponds to Part C (Top Ten Most Active Miners)
-- Logic: Aggregate block sizes by miner address
SELECT 
    miner_address,
    SUM(size) AS total_block_size_mined
FROM blocks
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- 4. SCAM ANALYSIS & FRAUD DETECTION
-- Corresponds to Part D (Scam Analysis)
-- Logic: Filter transactions involved with known scam addresses
SELECT 
    s.category AS scam_type,
    s.id AS scam_id,
    SUM(t.value) AS total_profit_ether
FROM scams s
JOIN transactions t ON s.address = t.to_address
GROUP BY 1, 2
ORDER BY 3 DESC;
