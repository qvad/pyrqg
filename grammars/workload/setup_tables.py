"""
Setup tables for workload testing.
Creates a set of tables in the pyrqg schema with proper columns and data types.
"""

from pyrqg.dsl.core import Grammar, choice, template, sequence

# Create grammar for table setup
grammar = Grammar("workload_setup")

# Define DDL for creating tables
grammar.define("setup", sequence(
    # Users table
    template("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email VARCHAR(255), name VARCHAR(255), status VARCHAR(50))"),
    
    # Products table
    template("CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name VARCHAR(255), price NUMERIC(10,2), status VARCHAR(50))"),
    
    # Orders table
    template("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, quantity INTEGER, status VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"),
    
    # Inventory table
    template("CREATE TABLE IF NOT EXISTS inventory (product_code VARCHAR(50) PRIMARY KEY, name VARCHAR(255), price NUMERIC(10,2), quantity INTEGER)"),
    
    # Sessions table
    template("CREATE TABLE IF NOT EXISTS sessions (id VARCHAR(100) PRIMARY KEY, user_id INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status VARCHAR(50))"),
    
    # Settings table
    template("CREATE TABLE IF NOT EXISTS settings (user_id INTEGER, setting_key VARCHAR(100), setting_value TEXT, PRIMARY KEY (user_id, setting_key))"),
    
    # Accounts table (for more complex queries)
    template("CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, email VARCHAR(255), name VARCHAR(255), status VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"),
    
    # Add some initial data
    template("INSERT INTO users (id, email, name, status) VALUES (1, 'admin@example.com', 'Admin User', 'active') ON CONFLICT (id) DO NOTHING"),
    template("INSERT INTO users (id, email, name, status) VALUES (2, 'user@example.com', 'Test User', 'active') ON CONFLICT (id) DO NOTHING"),
    
    template("INSERT INTO products (id, name, price, status) VALUES (1, 'Product A', 99.99, 'active') ON CONFLICT (id) DO NOTHING"),
    template("INSERT INTO products (id, name, price, status) VALUES (2, 'Product B', 149.99, 'active') ON CONFLICT (id) DO NOTHING"),
    
    template("INSERT INTO inventory (product_code, name, price, quantity) VALUES ('PROD001', 'Item 1', 25.00, 100) ON CONFLICT (product_code) DO NOTHING"),
    template("INSERT INTO inventory (product_code, name, price, quantity) VALUES ('PROD002', 'Item 2', 35.00, 50) ON CONFLICT (product_code) DO NOTHING")
))

# Export the grammar
__all__ = ['grammar']