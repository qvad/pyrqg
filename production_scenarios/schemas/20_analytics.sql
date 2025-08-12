-- Analytics Platform Schema
-- Placeholder for analytics scenario

CREATE TABLE IF NOT EXISTS analytics_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_analytics_status ON analytics_main_table(status);
CREATE INDEX idx_analytics_created ON analytics_main_table(created_at);
