-- Social Media Network Schema
-- Placeholder for social_media scenario

CREATE TABLE IF NOT EXISTS social_media_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_social_media_status ON social_media_main_table(status);
CREATE INDEX idx_social_media_created ON social_media_main_table(created_at);
