-- Event Ticketing Schema
-- Placeholder for ticketing scenario

CREATE TABLE IF NOT EXISTS ticketing_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ticketing_status ON ticketing_main_table(status);
CREATE INDEX idx_ticketing_created ON ticketing_main_table(created_at);
