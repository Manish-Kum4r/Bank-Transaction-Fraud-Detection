CREATE DATABASE bANK;
USE BANK;

CREATE TABLE IF NOT EXISTS bank_transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(10) NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    timestamp DATETIME NOT NULL,
    location VARCHAR(50),
    fraud_flag TINYINT DEFAULT 0
);

SELECT * FROM bank_transactions;

SELECT
    transaction_id,
    customer_id,
    amount,
    timestamp,
    location,
    fraud_flag,
    'HIGH_AMOUNT' AS anomaly_type
FROM bank_transactions
WHERE amount > 100000
ORDER BY amount DESC;

SELECT
    transaction_id,
    customer_id,
    amount,
    timestamp,
    HOUR(timestamp)  AS txn_hour,    
    location,
    fraud_flag,
    'ODD_HOUR' AS anomaly_type
FROM bank_transactions
WHERE HOUR(timestamp) BETWEEN 0 AND 3
ORDER BY timestamp ASC;

SELECT
    t1.transaction_id,
    t1.customer_id,
    t1.timestamp AS anchor_time,
    t1.amount,
    t1.location,
    t1.fraud_flag,
    COUNT(t2.transaction_id) AS txns_in_10min,
    'RAPID_TRANSACTION'  AS anomaly_type
FROM bank_transactions t1
JOIN bank_transactions t2
  ON  t1.customer_id = t2.customer_id
  AND t2.timestamp   BETWEEN t1.timestamp
AND t1.timestamp + INTERVAL 10 MINUTE
GROUP BY
    t1.transaction_id,
    t1.customer_id,
    t1.timestamp,
    t1.amount,
    t1.location,
    t1.fraud_flag
HAVING COUNT(t2.transaction_id) > 5
ORDER BY txns_in_10min DESC;

SELECT
    customer_id,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN amount > 100000 THEN 1 ELSE 0 END)  AS high_amount_count,
    SUM(CASE WHEN HOUR(timestamp) BETWEEN 0 AND 3
             THEN 1 ELSE 0 END) AS odd_hour_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    SUM(fraud_flag) AS confirmed_fraud_count
FROM bank_transactions
GROUP BY customer_id
HAVING
    SUM(CASE WHEN amount > 100000 THEN 1 ELSE 0 END) > 0
    OR SUM(CASE WHEN HOUR(timestamp) BETWEEN 0 AND 3 THEN 1 ELSE 0 END) > 2
    OR SUM(fraud_flag) > 0
ORDER BY confirmed_fraud_count DESC, total_amount DESC
LIMIT 20;
