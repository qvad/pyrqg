-- Subscription SaaS Schema
-- Placeholder for saas scenario

CREATE TABLE IF NOT EXISTS saas_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_saas_status ON saas_main_table(status);
CREATE INDEX idx_saas_created ON saas_main_table(created_at);
