-- Food Delivery Service Schema
-- Placeholder for food_delivery scenario

CREATE TABLE IF NOT EXISTS food_delivery_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_food_delivery_status ON food_delivery_main_table(status);
CREATE INDEX idx_food_delivery_created ON food_delivery_main_table(created_at);
