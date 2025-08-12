-- Financial Trading Schema
-- Placeholder for trading scenario

CREATE TABLE IF NOT EXISTS trading_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trading_status ON trading_main_table(status);
CREATE INDEX idx_trading_created ON trading_main_table(created_at);
