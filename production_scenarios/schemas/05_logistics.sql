-- Logistics & Shipping Schema
-- Placeholder for logistics scenario

CREATE TABLE IF NOT EXISTS logistics_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_logistics_status ON logistics_main_table(status);
CREATE INDEX idx_logistics_created ON logistics_main_table(created_at);
