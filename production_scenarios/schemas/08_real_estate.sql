-- Real Estate Management Schema
-- Placeholder for real_estate scenario

CREATE TABLE IF NOT EXISTS real_estate_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_real_estate_status ON real_estate_main_table(status);
CREATE INDEX idx_real_estate_created ON real_estate_main_table(created_at);
