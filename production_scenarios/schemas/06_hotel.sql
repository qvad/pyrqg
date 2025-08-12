-- Hotel Reservation System Schema
-- Placeholder for hotel scenario

CREATE TABLE IF NOT EXISTS hotel_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_hotel_status ON hotel_main_table(status);
CREATE INDEX idx_hotel_created ON hotel_main_table(created_at);
