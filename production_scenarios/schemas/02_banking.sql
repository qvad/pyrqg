-- Banking System Schema
-- Handles accounts, transactions, fraud detection, and compliance

-- Customers
CREATE TABLE IF NOT EXISTS bank_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_number VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    ssn_hash VARCHAR(64) NOT NULL, -- Hashed for security
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    kyc_status VARCHAR(20) DEFAULT 'pending',
    risk_score INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Account types
CREATE TABLE IF NOT EXISTS account_types (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL,
    min_balance DECIMAL(12,2) DEFAULT 0,
    monthly_fee DECIMAL(6,2) DEFAULT 0,
    interest_rate DECIMAL(5,4) DEFAULT 0
);

-- Accounts
CREATE TABLE IF NOT EXISTS accounts (
    account_id SERIAL PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    customer_id INTEGER REFERENCES bank_customers(customer_id),
    account_type_id INTEGER REFERENCES account_types(type_id),
    balance DECIMAL(15,2) NOT NULL DEFAULT 0,
    available_balance DECIMAL(15,2) NOT NULL DEFAULT 0,
    status VARCHAR(20) DEFAULT 'active',
    opened_date DATE DEFAULT CURRENT_DATE,
    closed_date DATE,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_frozen BOOLEAN DEFAULT FALSE
);

-- Transactions
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_ref VARCHAR(50) UNIQUE NOT NULL,
    from_account_id INTEGER REFERENCES accounts(account_id),
    to_account_id INTEGER REFERENCES accounts(account_id),
    transaction_type VARCHAR(20) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'pending',
    description TEXT,
    initiated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    fee_amount DECIMAL(8,2) DEFAULT 0,
    metadata JSONB
);

-- Transaction holds
CREATE TABLE IF NOT EXISTS transaction_holds (
    hold_id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(account_id),
    transaction_id INTEGER REFERENCES transactions(transaction_id),
    amount DECIMAL(15,2) NOT NULL,
    reason VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    released_at TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Fraud alerts
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id INTEGER REFERENCES transactions(transaction_id),
    account_id INTEGER REFERENCES accounts(account_id),
    alert_type VARCHAR(50) NOT NULL,
    risk_score INTEGER NOT NULL,
    details JSONB,
    status VARCHAR(20) DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100)
);

-- Audit log
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    record_id INTEGER NOT NULL,
    action VARCHAR(20) NOT NULL,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_values JSONB,
    new_values JSONB
);

-- Indexes for performance
CREATE INDEX idx_accounts_customer ON accounts(customer_id);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_transactions_from ON transactions(from_account_id);
CREATE INDEX idx_transactions_to ON transactions(to_account_id);
CREATE INDEX idx_transactions_initiated ON transactions(initiated_at);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_fraud_alerts_status ON fraud_alerts(status);
CREATE INDEX idx_fraud_alerts_created ON fraud_alerts(created_at);

-- Constraints
ALTER TABLE accounts ADD CONSTRAINT chk_balance_positive CHECK (balance >= 0);
ALTER TABLE transactions ADD CONSTRAINT chk_amount_positive CHECK (amount > 0);

-- Triggers for audit logging
CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values)
    VALUES (
        TG_TABLE_NAME,
        COALESCE(NEW.account_id, OLD.account_id, NEW.transaction_id, OLD.transaction_id),
        TG_OP,
        CASE WHEN TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE NULL END,
        CASE WHEN TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN to_jsonb(NEW) ELSE NULL END
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply audit triggers
CREATE TRIGGER audit_accounts AFTER INSERT OR UPDATE OR DELETE ON accounts
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();
    
CREATE TRIGGER audit_transactions AFTER INSERT OR UPDATE OR DELETE ON transactions
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();