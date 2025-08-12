-- Educational Platform Schema
-- Placeholder for education scenario

CREATE TABLE IF NOT EXISTS education_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_education_status ON education_main_table(status);
CREATE INDEX idx_education_created ON education_main_table(created_at);
