-- Content Management Schema
-- Placeholder for cms scenario

CREATE TABLE IF NOT EXISTS cms_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cms_status ON cms_main_table(status);
CREATE INDEX idx_cms_created ON cms_main_table(created_at);
