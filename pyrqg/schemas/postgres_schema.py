"""
Comprehensive PostgreSQL schema that supports all PyRQG grammars.
This schema ensures 100% compatibility with generated queries.
"""

# Comprehensive schema definition that covers ALL columns referenced in grammars
POSTGRES_SCHEMA = """
-- Drop and recreate schema
DROP SCHEMA IF EXISTS pyrqg CASCADE;
CREATE SCHEMA pyrqg;
SET search_path TO pyrqg, public;

-- Enable extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Core tables with ALL possible columns to avoid "column does not exist" errors
CREATE TABLE users (
    -- Primary identifiers
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    customer_id INTEGER,
    employee_id INTEGER,
    
    -- Text fields
    email VARCHAR(255),
    name VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    username VARCHAR(100),
    status VARCHAR(50) DEFAULT 'active',
    type VARCHAR(50),
    role VARCHAR(50),
    category VARCHAR(100),
    tags TEXT[],
    notes TEXT,
    description TEXT,
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(50),
    
    -- Numeric fields
    age INTEGER,
    balance DECIMAL(10,2) DEFAULT 0,
    total DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2) DEFAULT 0,
    price DECIMAL(10,2),
    quantity INTEGER DEFAULT 0,
    score INTEGER DEFAULT 0,
    rating INTEGER,
    count INTEGER DEFAULT 0,
    retry_count INTEGER DEFAULT 0,
    version INTEGER DEFAULT 1,
    visit_count INTEGER DEFAULT 0,
    priority INTEGER DEFAULT 0,
    level INTEGER DEFAULT 1,
    
    -- Boolean fields
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    is_verified BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    deleted BOOLEAN DEFAULT false,
    locked BOOLEAN DEFAULT false,
    
    -- Timestamp fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    modified_at TIMESTAMP,
    last_login TIMESTAMP,
    last_accessed TIMESTAMP,
    last_updated TIMESTAMP,
    expires_at TIMESTAMP,
    expiry_date TIMESTAMP,
    hire_date DATE,
    order_date TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- JSON fields
    data JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    properties JSONB DEFAULT '{}',
    settings JSONB DEFAULT '{}',
    
    -- Foreign keys
    product_id INTEGER,
    order_id INTEGER,
    manager_id INTEGER,
    department_id INTEGER,
    warehouse_id INTEGER,
    
    -- Additional fields for compatibility
    session_id VARCHAR(100),
    api_key VARCHAR(255),
    amount DECIMAL(10,2),
    salary DECIMAL(10,2),
    discount DECIMAL(10,2),
    shipping_address TEXT,
    billing_address TEXT,
    modified_by VARCHAR(100),
    
    -- Additional columns from workload grammars
    product_code VARCHAR(50),
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    transaction_id INTEGER,
    account_id INTEGER,
    transaction_type VARCHAR(50),
    supplier VARCHAR(100),
    manufacturer VARCHAR(100),
    barcode VARCHAR(100),
    sku VARCHAR(100),
    location VARCHAR(100),
    payment_method VARCHAR(50),
    fee DECIMAL(10,2),
    tax DECIMAL(10,2),
    balance_before DECIMAL(10,2),
    balance_after DECIMAL(10,2),
    processed_at TIMESTAMP,
    completed_at TIMESTAMP,
    account_number VARCHAR(50),
    account_type VARCHAR(50),
    last_transaction_date TIMESTAMP,
    shipped_date TIMESTAMP,
    delivered_date TIMESTAMP,
    shipping_cost DECIMAL(10,2),
    items JSONB DEFAULT '[]',
    reserved_quantity INTEGER DEFAULT 0,
    available_quantity INTEGER DEFAULT 0,
    cost DECIMAL(10,2),
    log_level VARCHAR(20),
    message TEXT,
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    event_name VARCHAR(100),
    event_type VARCHAR(50),
    key VARCHAR(255),
    value TEXT,
    setting_key VARCHAR(100),
    setting_value TEXT,
    sale_id INTEGER,
    sale_date TIMESTAMP,
    metric_id INTEGER,
    setting_id INTEGER,
    cache_id INTEGER,
    log_id INTEGER,
    event_id INTEGER,
    notification_id INTEGER,
    token VARCHAR(255),
    expired_at TIMESTAMP,
    item_type VARCHAR(50),
    item_id INTEGER,
    source_table VARCHAR(50),
    source_id INTEGER,
    archived_at TIMESTAMP,
    action VARCHAR(100),
    title VARCHAR(255),
    inventory_id INTEGER
);

CREATE TABLE products (
    -- Primary identifiers
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    
    -- Text fields
    product_code VARCHAR(50),
    name VARCHAR(255),
    description TEXT,
    category VARCHAR(100),
    status VARCHAR(50) DEFAULT 'available',
    type VARCHAR(50),
    tags TEXT[],
    notes TEXT,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    
    -- Numeric fields
    price DECIMAL(10,2) DEFAULT 0,
    quantity INTEGER DEFAULT 0,
    total DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2) DEFAULT 0,
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER DEFAULT 0,
    count INTEGER DEFAULT 0,
    score INTEGER DEFAULT 0,
    rating INTEGER,
    discount DECIMAL(10,2),
    version INTEGER DEFAULT 1,
    
    -- Boolean fields
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    deleted BOOLEAN DEFAULT false,
    
    -- Timestamp fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    modified_at TIMESTAMP,
    expiry_date DATE,
    last_updated TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- JSON fields
    data JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    properties JSONB DEFAULT '{}',
    
    -- Foreign keys
    user_id INTEGER,
    customer_id INTEGER,
    order_id INTEGER,
    warehouse_id INTEGER,
    employee_id INTEGER,
    transaction_id INTEGER,
    account_id INTEGER,
    
    -- Additional fields
    location VARCHAR(100),
    supplier VARCHAR(100),
    manufacturer VARCHAR(100),
    barcode VARCHAR(100),
    sku VARCHAR(100),
    transaction_type VARCHAR(50),
    balance DECIMAL(10,2),
    amount DECIMAL(10,2),
    fee DECIMAL(10,2),
    tax DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    payment_method VARCHAR(50),
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    setting_key VARCHAR(100),
    setting_value TEXT
);

CREATE TABLE orders (
    -- Primary identifiers
    id SERIAL PRIMARY KEY,
    order_id INTEGER,
    
    -- Foreign keys
    user_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    employee_id INTEGER,
    
    -- Text fields
    status VARCHAR(50) DEFAULT 'pending',
    shipping_address TEXT,
    billing_address TEXT,
    email VARCHAR(255),
    name VARCHAR(255),
    notes TEXT,
    description TEXT,
    type VARCHAR(50),
    category VARCHAR(100),
    
    -- Numeric fields
    quantity INTEGER DEFAULT 1,
    total DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2) DEFAULT 0,
    price DECIMAL(10,2),
    amount DECIMAL(10,2),
    discount DECIMAL(10,2),
    tax DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    count INTEGER DEFAULT 0,
    version INTEGER DEFAULT 1,
    
    -- Timestamp fields
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    modified_at TIMESTAMP,
    shipped_date TIMESTAMP,
    delivered_date TIMESTAMP,
    last_updated TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Boolean fields
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    deleted BOOLEAN DEFAULT false,
    
    -- JSON fields
    data JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    items JSONB DEFAULT '[]'
);

CREATE TABLE inventory (
    -- Primary identifiers
    id SERIAL PRIMARY KEY,
    inventory_id INTEGER,
    
    -- Foreign keys
    product_id INTEGER,
    warehouse_id INTEGER,
    user_id INTEGER,
    order_id INTEGER,
    customer_id INTEGER,
    transaction_id INTEGER,
    account_id INTEGER,
    
    -- Text fields
    location VARCHAR(100),
    status VARCHAR(50) DEFAULT 'in_stock',
    name VARCHAR(255),
    email VARCHAR(255),
    notes TEXT,
    description TEXT,
    type VARCHAR(50),
    category VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    transaction_type VARCHAR(50),
    
    -- Numeric fields
    quantity INTEGER DEFAULT 0,
    reserved_quantity INTEGER DEFAULT 0,
    available_quantity INTEGER DEFAULT 0,
    price DECIMAL(10,2),
    total DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2) DEFAULT 0,
    cost DECIMAL(10,2),
    count INTEGER DEFAULT 0,
    version INTEGER DEFAULT 1,
    score INTEGER DEFAULT 0,
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    balance DECIMAL(10,2),
    amount DECIMAL(10,2),
    
    -- Timestamp fields
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    modified_at TIMESTAMP,
    expiry_date DATE,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Boolean fields
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    deleted BOOLEAN DEFAULT false,
    
    -- JSON fields
    data JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}'
);

CREATE TABLE transactions (
    -- Primary identifiers
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100),
    
    -- Foreign keys
    user_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    order_id INTEGER,
    account_id INTEGER,
    
    -- Text fields
    transaction_type VARCHAR(50),
    status VARCHAR(50) DEFAULT 'pending',
    email VARCHAR(255),
    name VARCHAR(255),
    description TEXT,
    notes TEXT,
    type VARCHAR(50),
    category VARCHAR(100),
    payment_method VARCHAR(50),
    
    -- Numeric fields
    amount DECIMAL(10,2) DEFAULT 0,
    price DECIMAL(10,2),
    quantity INTEGER,
    total DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2) DEFAULT 0,
    fee DECIMAL(10,2),
    tax DECIMAL(10,2),
    balance_before DECIMAL(10,2),
    balance_after DECIMAL(10,2),
    count INTEGER DEFAULT 0,
    version INTEGER DEFAULT 1,
    score INTEGER DEFAULT 0,
    
    -- Timestamp fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    modified_at TIMESTAMP,
    processed_at TIMESTAMP,
    completed_at TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Boolean fields
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    deleted BOOLEAN DEFAULT false,
    
    -- JSON fields
    data JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}'
);

-- Additional tables for complete coverage
CREATE TABLE sessions (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100),
    user_id INTEGER,
    customer_id INTEGER,
    data JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    email VARCHAR(255),
    name VARCHAR(255),
    status VARCHAR(50) DEFAULT 'active',
    price DECIMAL(10,2),
    quantity INTEGER,
    is_active BOOLEAN DEFAULT true,
    active BOOLEAN DEFAULT true,
    -- Additional fields for INSERT compatibility
    product_code VARCHAR(50),
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    product_id INTEGER,
    transaction_id INTEGER,
    account_id INTEGER,
    transaction_type VARCHAR(50),
    total DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    balance DECIMAL(10,2)
);

CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER UNIQUE,
    user_id INTEGER,
    manager_id INTEGER,
    department_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    status VARCHAR(50) DEFAULT 'active',
    role VARCHAR(50),
    salary DECIMAL(10,2),
    hire_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    active BOOLEAN DEFAULT true,
    product_id INTEGER,
    quantity INTEGER,
    price DECIMAL(10,2),
    description TEXT,
    unit_price DECIMAL(10,2),
    data JSONB DEFAULT '{}'
);

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    user_id INTEGER,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_order_date TIMESTAMP,
    total_spent DECIMAL(10,2) DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    deleted BOOLEAN DEFAULT false,
    quantity INTEGER,
    price DECIMAL(10,2),
    product_id INTEGER,
    order_id INTEGER,
    description TEXT,
    discount DECIMAL(10,2),
    data JSONB DEFAULT '{}',
    transaction_id INTEGER,
    account_id INTEGER,
    transaction_type VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    total DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    amount DECIMAL(10,2),
    balance DECIMAL(10,2)
);

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    user_id INTEGER,
    customer_id INTEGER,
    account_number VARCHAR(50),
    account_type VARCHAR(50),
    balance DECIMAL(10,2) DEFAULT 0,
    total DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2) DEFAULT 0,
    status VARCHAR(50) DEFAULT 'active',
    email VARCHAR(255),
    name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_transaction_date TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    active BOOLEAN DEFAULT true,
    version INTEGER DEFAULT 1,
    score INTEGER DEFAULT 0,
    quantity INTEGER,
    category VARCHAR(100),
    modified_at TIMESTAMP,
    last_updated TIMESTAMP,
    notes TEXT,
    data JSONB DEFAULT '{}'
);

CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    log_id INTEGER,
    user_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    order_id INTEGER,
    log_level VARCHAR(20),
    message TEXT,
    email VARCHAR(255),
    name VARCHAR(255),
    status VARCHAR(50),
    quantity INTEGER,
    price DECIMAL(10,2),
    total DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}'
);

CREATE TABLE analytics (
    id SERIAL PRIMARY KEY,
    event_id INTEGER,
    user_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    order_id INTEGER,
    event_name VARCHAR(100),
    event_type VARCHAR(50),
    email VARCHAR(255),
    name VARCHAR(255),
    status VARCHAR(50),
    quantity INTEGER,
    price DECIMAL(10,2),
    total DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    rating INTEGER,
    score INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}',
    properties JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}'
);

-- Workload-specific tables
-- Add departments table
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    department_id INTEGER,
    name VARCHAR(255),
    status VARCHAR(50) DEFAULT 'active',
    user_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    price DECIMAL(10,2),
    quantity INTEGER,
    product_id INTEGER,
    transaction_id INTEGER,
    account_id INTEGER,
    transaction_type VARCHAR(50),
    data JSONB DEFAULT '{}'
);

CREATE TABLE suppliers (
    id SERIAL PRIMARY KEY,
    supplier_id INTEGER,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    status VARCHAR(50) DEFAULT 'active',
    product_code VARCHAR(50),
    description TEXT,
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}',
    transaction_id INTEGER,
    account_id INTEGER,
    price DECIMAL(10,2),
    transaction_type VARCHAR(50),
    user_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100)
);

CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    sale_id INTEGER,
    user_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    name VARCHAR(255),
    email VARCHAR(255),
    price DECIMAL(10,2),
    quantity INTEGER,
    total DECIMAL(10,2),
    count INTEGER,
    status VARCHAR(50) DEFAULT 'completed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'
);

CREATE TABLE cache (
    id SERIAL PRIMARY KEY,
    cache_id INTEGER,
    key VARCHAR(255),
    value TEXT,
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    user_id INTEGER,
    setting_key VARCHAR(100),
    setting_value TEXT,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'
);

CREATE TABLE metrics (
    id SERIAL PRIMARY KEY,
    metric_id INTEGER,
    name VARCHAR(100),
    value DECIMAL(10,2),
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    session_id VARCHAR(100),
    user_id INTEGER,
    product_id INTEGER,
    product_code VARCHAR(50),
    price DECIMAL(10,2),
    quantity INTEGER,
    setting_key VARCHAR(100),
    setting_value TEXT,
    visit_count INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMP,
    data JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}'
);

CREATE TABLE settings (
    id SERIAL PRIMARY KEY,
    setting_id INTEGER,
    user_id INTEGER,
    setting_key VARCHAR(100),
    setting_value TEXT,
    key VARCHAR(100),
    value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'
);

-- Additional workload tables
CREATE TABLE temp_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    status VARCHAR(50),
    data JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE audit_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    customer_id INTEGER,
    action VARCHAR(100),
    status VARCHAR(50),
    last_login TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'
);

CREATE TABLE notifications (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    title VARCHAR(255),
    message TEXT,
    status VARCHAR(50) DEFAULT 'unread',
    quantity INTEGER,
    expiry_date TIMESTAMP,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'
);

CREATE TABLE expired_tokens (
    id SERIAL PRIMARY KEY,
    token VARCHAR(255),
    user_id INTEGER,
    status VARCHAR(50),
    expired_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'
);

CREATE TABLE deleted_items (
    id SERIAL PRIMARY KEY,
    item_type VARCHAR(50),
    item_id INTEGER,
    status VARCHAR(50),
    last_login TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'
);

CREATE TABLE archive (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(50),
    source_id INTEGER,
    status VARCHAR(50),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'
);

-- YugabyteDB compatibility tables
CREATE TABLE A (
    pk SERIAL PRIMARY KEY,
    col_int_key INTEGER,
    col_int INTEGER,
    col_varchar_10 VARCHAR(10),
    col_varchar_10_key VARCHAR(10),
    col_bigint BIGINT,
    col_bigint_key BIGINT,
    col_decimal DECIMAL,
    col_decimal_key DECIMAL,
    col_float FLOAT,
    col_float_key FLOAT,
    col_double DOUBLE PRECISION,
    col_double_key DOUBLE PRECISION,
    col_char_255 CHAR(255),
    col_char_255_key CHAR(255),
    col_char_10 CHAR(10),
    col_char_10_key CHAR(10),
    col_text TEXT,
    col_text_key TEXT,
    col_varchar_255 VARCHAR(255),
    col_varchar_255_key VARCHAR(255)
);

-- Create similar tables B through PP for YugabyteDB compatibility
CREATE TABLE B AS TABLE A WITH NO DATA;
CREATE TABLE C AS TABLE A WITH NO DATA;
CREATE TABLE D AS TABLE A WITH NO DATA;
CREATE TABLE E AS TABLE A WITH NO DATA;
CREATE TABLE F AS TABLE A WITH NO DATA;
CREATE TABLE G AS TABLE A WITH NO DATA;
CREATE TABLE H AS TABLE A WITH NO DATA;
CREATE TABLE I AS TABLE A WITH NO DATA;
CREATE TABLE J AS TABLE A WITH NO DATA;
CREATE TABLE K AS TABLE A WITH NO DATA;
CREATE TABLE L AS TABLE A WITH NO DATA;
CREATE TABLE M AS TABLE A WITH NO DATA;
CREATE TABLE N AS TABLE A WITH NO DATA;
CREATE TABLE O AS TABLE A WITH NO DATA;
CREATE TABLE P AS TABLE A WITH NO DATA;
CREATE TABLE AA AS TABLE A WITH NO DATA;
CREATE TABLE BB AS TABLE A WITH NO DATA;
CREATE TABLE CC AS TABLE A WITH NO DATA;
CREATE TABLE DD AS TABLE A WITH NO DATA;
CREATE TABLE EE AS TABLE A WITH NO DATA;
CREATE TABLE FF AS TABLE A WITH NO DATA;
CREATE TABLE GG AS TABLE A WITH NO DATA;
CREATE TABLE HH AS TABLE A WITH NO DATA;
CREATE TABLE II AS TABLE A WITH NO DATA;
CREATE TABLE JJ AS TABLE A WITH NO DATA;
CREATE TABLE KK AS TABLE A WITH NO DATA;
CREATE TABLE LL AS TABLE A WITH NO DATA;
CREATE TABLE MM AS TABLE A WITH NO DATA;
CREATE TABLE NN AS TABLE A WITH NO DATA;
CREATE TABLE OO AS TABLE A WITH NO DATA;
CREATE TABLE PP AS TABLE A WITH NO DATA;
CREATE TABLE AAA AS TABLE A WITH NO DATA;
CREATE TABLE BBB AS TABLE A WITH NO DATA;
CREATE TABLE CCC AS TABLE A WITH NO DATA;
CREATE TABLE DDD AS TABLE A WITH NO DATA;

-- Create indexes for better performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_transactions_user_id ON transactions(user_id);

-- Insert sample data for testing
INSERT INTO users (email, name, status, age, balance) VALUES
    ('test1@example.com', 'Test User 1', 'active', 25, 1000.00),
    ('test2@example.com', 'Test User 2', 'inactive', 30, 2000.00),
    ('test3@example.com', 'Test User 3', 'active', 35, 3000.00);

INSERT INTO products (product_code, name, price, quantity, category) VALUES
    ('PROD001', 'Product 1', 19.99, 100, 'electronics'),
    ('PROD002', 'Product 2', 29.99, 50, 'books'),
    ('PROD003', 'Product 3', 39.99, 75, 'clothing');

-- Insert data into YugabyteDB tables
INSERT INTO A (col_int_key, col_int) VALUES (100, 100);
INSERT INTO B (col_int_key, col_int) VALUES (100, 100);
INSERT INTO C (col_int_key, col_int) VALUES (100, 100);

-- Create functions for grammars that need them
CREATE OR REPLACE FUNCTION lastval_safe() RETURNS BIGINT AS $$
BEGIN
    RETURN COALESCE(lastval(), 1);
EXCEPTION WHEN OTHERS THEN
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

-- Create a view for convenience
CREATE VIEW all_tables AS
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'pyrqg' 
AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Grant permissions (if needed)
GRANT ALL ON SCHEMA pyrqg TO PUBLIC;
GRANT ALL ON ALL TABLES IN SCHEMA pyrqg TO PUBLIC;
GRANT ALL ON ALL SEQUENCES IN SCHEMA pyrqg TO PUBLIC;
"""

def get_postgres_schema():
    """Return the PostgreSQL schema"""
    return POSTGRES_SCHEMA

def get_table_columns():
    """Return a dictionary of table -> columns mapping"""
    return {
        'users': ['id', 'user_id', 'customer_id', 'employee_id', 'email', 'name', 
                  'first_name', 'last_name', 'username', 'status', 'type', 'role',
                  'category', 'tags', 'notes', 'description', 'address', 'city',
                  'country', 'phone', 'age', 'balance', 'total', 'total_amount',
                  'price', 'quantity', 'score', 'rating', 'count', 'retry_count',
                  'version', 'visit_count', 'priority', 'level', 'is_active',
                  'is_deleted', 'is_verified', 'active', 'deleted', 'locked',
                  'created_at', 'updated_at', 'deleted_at', 'modified_at',
                  'last_login', 'last_accessed', 'last_updated', 'expires_at',
                  'expiry_date', 'hire_date', 'order_date', 'timestamp', 'data',
                  'metadata', 'properties', 'settings', 'product_id', 'order_id',
                  'manager_id', 'department_id', 'warehouse_id', 'session_id',
                  'api_key', 'amount', 'salary', 'discount', 'shipping_address',
                  'billing_address', 'modified_by'],
        'products': ['id', 'product_id', 'product_code', 'name', 'description',
                     'category', 'status', 'type', 'tags', 'notes', 'email',
                     'price', 'quantity', 'total', 'total_amount', 'unit_price',
                     'stock_quantity', 'count', 'score', 'rating', 'discount',
                     'version', 'is_active', 'is_deleted', 'active', 'deleted',
                     'created_at', 'updated_at', 'deleted_at', 'modified_at',
                     'expiry_date', 'last_updated', 'timestamp', 'data',
                     'metadata', 'properties', 'user_id', 'customer_id',
                     'order_id', 'warehouse_id', 'location', 'supplier',
                     'manufacturer', 'barcode', 'sku'],
        # ... similar for other tables
    }