-- =================================================================
-- Banking Data Engineering Platform — Warehouse Star Schema DDL
-- =================================================================
-- These tables represent the curated data layer, fully typed and modeled
-- as a star schema for analytical queries.

-- 1. Date Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- 2. Customer Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT,
    gender VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    occupation VARCHAR(100),
    income DECIMAL(15, 2),
    account_open_date DATE,
    customer_segment VARCHAR(50),
    -- Slowly Changing Dimension Type 2 fields
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'
);

-- 3. Branch Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_branch (
    branch_id VARCHAR(50) PRIMARY KEY,
    branch_name VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(100),
    branch_type VARCHAR(50)
);

-- 4. Merchant Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_merchant (
    merchant_id VARCHAR(50) PRIMARY KEY,
    merchant_name VARCHAR(255) NOT NULL,
    merchant_category VARCHAR(100),
    city VARCHAR(100),
    risk_category VARCHAR(50)
);

-- 5. Account Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_account (
    account_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES warehouse.dim_customer(customer_id),
    account_type VARCHAR(50) NOT NULL,
    branch_id VARCHAR(50) REFERENCES warehouse.dim_branch(branch_id),
    balance DECIMAL(15, 2),
    account_status VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'
);

-- 6. Transaction Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fact_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) REFERENCES warehouse.dim_account(account_id),
    customer_id VARCHAR(50) REFERENCES warehouse.dim_customer(customer_id),
    merchant_id VARCHAR(50) REFERENCES warehouse.dim_merchant(merchant_id),
    branch_id VARCHAR(50) REFERENCES warehouse.dim_branch(branch_id),
    date_id INT REFERENCES warehouse.dim_date(date_id),
    transaction_type VARCHAR(50) NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50),
    channel VARCHAR(50),
    location VARCHAR(100),
    -- Audit fields
    pipeline_run_id UUID,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for Fact Table
CREATE INDEX IF NOT EXISTS idx_fact_tx_account ON warehouse.fact_transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_fact_tx_customer ON warehouse.fact_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_tx_merchant ON warehouse.fact_transactions(merchant_id);
CREATE INDEX IF NOT EXISTS idx_fact_tx_date ON warehouse.fact_transactions(date_id);
