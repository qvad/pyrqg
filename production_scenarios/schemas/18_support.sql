-- Customer Support Schema
-- Placeholder for support scenario

CREATE TABLE IF NOT EXISTS support_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_support_status ON support_main_table(status);
CREATE INDEX idx_support_created ON support_main_table(created_at);
