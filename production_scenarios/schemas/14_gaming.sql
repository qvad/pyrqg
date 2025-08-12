-- Gaming Platform Schema
-- Placeholder for gaming scenario

CREATE TABLE IF NOT EXISTS gaming_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_gaming_status ON gaming_main_table(status);
CREATE INDEX idx_gaming_created ON gaming_main_table(created_at);
