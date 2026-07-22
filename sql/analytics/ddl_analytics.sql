-- =================================================================
-- Banking Data Engineering Platform — Analytics Schema DDL
-- =================================================================
-- These are aggregate tables built on top of the warehouse schema.
-- They are designed specifically to power dashboards and reports.

-- 1. Daily Branch Performance
CREATE TABLE IF NOT EXISTS analytics.daily_branch_metrics (
    date_id INT,
    branch_id VARCHAR(50),
    total_transactions BIGINT,
    total_inflow DECIMAL(15, 2),
    total_outflow DECIMAL(15, 2),
    unique_customers BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id, branch_id)
);

-- 2. Customer Segmentation Summary
CREATE TABLE IF NOT EXISTS analytics.customer_segment_summary (
    month_id INT,  -- YYYYMM format
    customer_segment VARCHAR(50),
    total_customers BIGINT,
    avg_age INT,
    total_balance DECIMAL(20, 2),
    high_value_transactions BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (month_id, customer_segment)
);

-- 3. Fraud Analytics
CREATE TABLE IF NOT EXISTS analytics.fraud_summary (
    date_id INT,
    merchant_risk_category VARCHAR(50),
    channel VARCHAR(50),
    flagged_transactions BIGINT,
    flagged_amount DECIMAL(15, 2),
    confirmed_fraud BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id, merchant_risk_category, channel)
);
