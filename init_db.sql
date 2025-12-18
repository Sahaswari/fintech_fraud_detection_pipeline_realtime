-- Initialize database schema for fraud detection system

-- Table for valid (non-fraud) transactions
CREATE TABLE IF NOT EXISTS valid_transactions (
    transaction_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    location VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for fraud alerts
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    location VARCHAR(100) NOT NULL,
    fraud_type VARCHAR(50) NOT NULL, -- 'IMPOSSIBLE_TRAVEL' or 'HIGH_VALUE'
    fraud_reason TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for batch processing reconciliation (Ingress vs Validated)
CREATE TABLE IF NOT EXISTS daily_reconciliation (
    report_id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    ingress_count INTEGER NOT NULL,        -- Total transactions ingested via Kafka
    ingress_amount DECIMAL(15, 2) NOT NULL, -- Total amount of all ingested transactions
    valid_count INTEGER NOT NULL,           -- Validated (non-fraud) transaction count
    valid_amount DECIMAL(15, 2) NOT NULL,   -- Validated (non-fraud) transaction amount
    fraud_count INTEGER NOT NULL,           -- Flagged fraud transaction count
    fraud_amount DECIMAL(15, 2) NOT NULL,   -- Flagged fraud transaction amount
    discrepancy_amount DECIMAL(15, 2) DEFAULT 0, -- Ingress - (Valid + Fraud) difference
    reconciliation_status VARCHAR(20) DEFAULT 'BALANCED', -- BALANCED or DISCREPANCY
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_valid_trans_timestamp ON valid_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_valid_trans_user ON valid_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_timestamp ON fraud_alerts(timestamp);
CREATE INDEX IF NOT EXISTS idx_fraud_user ON fraud_alerts(user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_category ON fraud_alerts(merchant_category);

-- View for fraud analysis by merchant category
CREATE OR REPLACE VIEW fraud_by_category AS
SELECT 
    merchant_category,
    COUNT(*) as fraud_count,
    SUM(amount) as total_fraud_amount,
    AVG(amount) as avg_fraud_amount
FROM fraud_alerts
GROUP BY merchant_category
ORDER BY fraud_count DESC;

-- View for daily transaction summary
CREATE OR REPLACE VIEW daily_transaction_summary AS
SELECT 
    DATE(timestamp) as transaction_date,
    COUNT(*) as total_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM valid_transactions
GROUP BY DATE(timestamp)
ORDER BY transaction_date DESC;

COMMENT ON TABLE valid_transactions IS 'Stores validated non-fraudulent transactions';
COMMENT ON TABLE fraud_alerts IS 'Stores detected fraudulent transactions with fraud type';
COMMENT ON TABLE daily_reconciliation IS 'Daily batch processing reconciliation reports';