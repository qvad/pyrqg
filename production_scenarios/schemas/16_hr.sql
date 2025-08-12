-- HR Management Schema
-- Placeholder for hr scenario

CREATE TABLE IF NOT EXISTS hr_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_hr_status ON hr_main_table(status);
CREATE INDEX idx_hr_created ON hr_main_table(created_at);
