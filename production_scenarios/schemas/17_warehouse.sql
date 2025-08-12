-- Inventory Warehouse Schema
-- Placeholder for warehouse scenario

CREATE TABLE IF NOT EXISTS warehouse_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_warehouse_status ON warehouse_main_table(status);
CREATE INDEX idx_warehouse_created ON warehouse_main_table(created_at);
