-- IoT Sensor Network Schema
-- Placeholder for iot scenario

CREATE TABLE IF NOT EXISTS iot_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_iot_status ON iot_main_table(status);
CREATE INDEX idx_iot_created ON iot_main_table(created_at);
