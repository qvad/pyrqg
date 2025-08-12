-- Manufacturing ERP Schema
-- Placeholder for manufacturing scenario

CREATE TABLE IF NOT EXISTS manufacturing_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_manufacturing_status ON manufacturing_main_table(status);
CREATE INDEX idx_manufacturing_created ON manufacturing_main_table(created_at);
