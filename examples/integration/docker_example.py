#!/usr/bin/env python3
"""
docker_example.py - PyRQG Docker Integration Example

This example demonstrates how to use PyRQG in Docker environments:
- Dockerfile for PyRQG
- Docker Compose setup
- Database container integration
- Environment configuration
- Container orchestration

Also includes helper scripts for Docker deployment.
"""

import os
import sys
from pathlib import Path

# Create Dockerfile
DOCKERFILE_CONTENT = """
# PyRQG Dockerfile
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    postgresql-client \\
    build-essential \\
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy PyRQG package
COPY pyrqg/ ./pyrqg/
COPY setup.py .
COPY README.md .

# Install PyRQG
RUN pip install -e .

# Copy examples and scripts
COPY examples/ ./examples/
COPY grammars/ ./grammars/

# Create volume mount points
VOLUME ["/data", "/config"]

# Default environment variables
ENV PYRQG_DATABASE_URL=postgresql://postgres:password@db:5432/test_db
ENV PYRQG_LOG_LEVEL=INFO
ENV PYTHONUNBUFFERED=1

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD python -c "from pyrqg.api import RQG; rqg = RQG(); print('OK')" || exit 1

# Default command
CMD ["python", "-m", "pyrqg"]
"""

# Create docker-compose.yml
DOCKER_COMPOSE_CONTENT = """
version: '3.8'

services:
  # PostgreSQL Database
  db:
    image: postgres:14-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
      POSTGRES_DB: test_db
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  # PyRQG Query Generator
  pyrqg:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      db:
        condition: service_healthy
    environment:
      PYRQG_DATABASE_URL: postgresql://postgres:password@db:5432/test_db
      PYRQG_LOG_LEVEL: INFO
    volumes:
      - ./output:/data
      - ./config:/config
    command: >
      python -c "
      from pyrqg.api import RQG
      import time
      
      rqg = RQG()
      while True:
          query = rqg.generate_query('dml_basic')
          print(f'Generated: {query}')
          time.sleep(1)
      "

  # PyRQG API Server
  pyrqg-api:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      db:
        condition: service_healthy
    environment:
      PYRQG_DATABASE_URL: postgresql://postgres:password@db:5432/test_db
      FLASK_APP: examples/integration/api_server.py
    ports:
      - "5000:5000"
    command: ["python", "examples/integration/api_server.py", "--host", "0.0.0.0"]
    
  # PyRQG Performance Tester
  pyrqg-perf:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      db:
        condition: service_healthy
    environment:
      PYRQG_DATABASE_URL: postgresql://postgres:password@db:5432/test_db
    volumes:
      - ./reports:/data/reports
    command: >
      python -m pyrqg.runner production --config yugabyte --count 600 --threads 4

  # PyRQG Workload Generator
  pyrqg-workload:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      db:
        condition: service_healthy
    environment:
      PYRQG_DATABASE_URL: postgresql://postgres:password@db:5432/test_db
    command: >
      python -m pyrqg.runner grammar --grammar dml_yugabyte --count 1000

volumes:
  postgres_data:

networks:
  default:
    name: pyrqg-network
"""

# Create database initialization script
INIT_SQL_CONTENT = """
-- Initialize test database schema

-- Create schema
CREATE SCHEMA IF NOT EXISTS pyrqg;
SET search_path TO pyrqg, public;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    stock INTEGER DEFAULT 0 CHECK (stock >= 0),
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    status VARCHAR(20) DEFAULT 'pending',
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order items table
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);

-- Insert sample data
INSERT INTO users (username, email, full_name, status) VALUES
    ('john_doe', 'john@example.com', 'John Doe', 'active'),
    ('jane_smith', 'jane@example.com', 'Jane Smith', 'active'),
    ('bob_wilson', 'bob@example.com', 'Bob Wilson', 'active')
ON CONFLICT DO NOTHING;

INSERT INTO products (name, description, price, stock, category) VALUES
    ('Laptop', 'High-performance laptop', 999.99, 50, 'Electronics'),
    ('Mouse', 'Wireless mouse', 29.99, 200, 'Electronics'),
    ('Keyboard', 'Mechanical keyboard', 79.99, 100, 'Electronics'),
    ('Monitor', '27-inch 4K monitor', 399.99, 30, 'Electronics'),
    ('Desk Chair', 'Ergonomic office chair', 299.99, 25, 'Furniture')
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA pyrqg TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pyrqg TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA pyrqg TO postgres;
"""

# Create environment file template
ENV_FILE_CONTENT = """
# PyRQG Docker Environment Configuration

# Database Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=test_db
PYRQG_DATABASE_URL=postgresql://postgres:password@db:5432/test_db

# PyRQG Configuration
PYRQG_LOG_LEVEL=INFO
PYRQG_GRAMMARS_PATH=/app/grammars
PYRQG_OUTPUT_PATH=/data

# API Server Configuration
FLASK_ENV=production
API_HOST=0.0.0.0
API_PORT=5000

# Performance Testing
PERF_TEST_DURATION=300
PERF_TEST_THREADS=4
PERF_TEST_QPS=100

# Workload Simulation
WORKLOAD_SCENARIO=ecommerce
WORKLOAD_DURATION=3600
WORKLOAD_BASE_QPS=50
"""

# Create Makefile for easy management
MAKEFILE_CONTENT = """
# PyRQG Docker Makefile

.PHONY: help build up down logs shell test clean

help:
	@echo "PyRQG Docker Commands:"
	@echo "  make build    - Build Docker images"
	@echo "  make up       - Start all services"
	@echo "  make down     - Stop all services"
	@echo "  make logs     - View logs"
	@echo "  make shell    - Open shell in PyRQG container"
	@echo "  make test     - Run tests in container"
	@echo "  make clean    - Clean up volumes and images"

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "Services started. API available at http://localhost:5000"

down:
	docker-compose down

logs:
	docker-compose logs -f

shell:
	docker-compose exec pyrqg /bin/bash

test:
	docker-compose run --rm pyrqg pytest tests/ -v

clean:
	docker-compose down -v
	docker system prune -f

# Development commands
dev:
	docker-compose up db -d
	docker-compose run --rm -v $$(pwd):/app pyrqg /bin/bash

generate:
	docker-compose run --rm pyrqg python -m pyrqg generate \\
		--grammar dml_basic --count 100 --output /data/queries.sql

perf-test:
	docker-compose up pyrqg-perf

workload:
	docker-compose up pyrqg-workload

api:
	docker-compose up pyrqg-api
"""

# Python script to demonstrate Docker usage
DOCKER_USAGE_SCRIPT = """
#!/usr/bin/env python3
\"\"\"
docker_usage.py - Example of using PyRQG in Docker container
\"\"\"

import os
import sys
import psycopg2
import json
from datetime import datetime

def main():
    # Get database URL from environment
    db_url = os.environ.get('PYRQG_DATABASE_URL')
    if not db_url:
        print("Error: PYRQG_DATABASE_URL not set")
        sys.exit(1)
    
    print(f"Connecting to database: {db_url}")
    
    # Import PyRQG
    from pyrqg.api import RQG
    
    # Initialize
    rqg = RQG()
    
    # Generate queries
    print("\\nGenerating queries...")
    queries = []
    
    for i in range(10):
        query = rqg.generate_query('dml_basic', seed=i)
        queries.append(query)
        print(f"{i+1}. {query}")
    
    # Test database connection
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        print("\\nTesting database queries...")
        
        for query in queries[:5]:  # Test first 5 queries
            if query.upper().startswith('SELECT'):
                try:
                    cur.execute(query)
                    result = cur.fetchall()
                    print(f"✓ Query executed successfully: {len(result)} rows")
                except Exception as e:
                    print(f"✗ Query failed: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Database connection failed: {e}")
    
    # Save results
    output_path = os.environ.get('PYRQG_OUTPUT_PATH', '/data')
    output_file = os.path.join(output_path, 'docker_test_results.json')
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'queries_generated': len(queries),
        'database_url': db_url,
        'queries': queries
    }
    
    os.makedirs(output_path, exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\\nResults saved to: {output_file}")

if __name__ == "__main__":
    main()
"""

def create_docker_files():
    """Create Docker-related files."""
    
    files = {
        'Dockerfile': DOCKERFILE_CONTENT,
        'docker-compose.yml': DOCKER_COMPOSE_CONTENT,
        'init.sql': INIT_SQL_CONTENT,
        '.env.example': ENV_FILE_CONTENT,
        'Makefile': MAKEFILE_CONTENT,
        'docker_usage.py': DOCKER_USAGE_SCRIPT
    }
    
    # Create docker directory
    docker_dir = Path('docker_example_files')
    docker_dir.mkdir(exist_ok=True)
    
    for filename, content in files.items():
        filepath = docker_dir / filename
        with open(filepath, 'w') as f:
            f.write(content.strip() + '\n')
        print(f"Created: {filepath}")
    
    # Create directories
    (docker_dir / 'output').mkdir(exist_ok=True)
    (docker_dir / 'config').mkdir(exist_ok=True)
    (docker_dir / 'reports').mkdir(exist_ok=True)
    
    # Create README
    readme_content = """
# PyRQG Docker Example

This directory contains everything needed to run PyRQG in Docker.

## Quick Start

1. Copy `.env.example` to `.env` and adjust settings if needed
2. Build the images: `make build`
3. Start services: `make up`
4. View logs: `make logs`
5. Access API: http://localhost:5000

## Services

- **db**: PostgreSQL database
- **pyrqg**: Base PyRQG container
- **pyrqg-api**: REST API server
- **pyrqg-perf**: Performance testing
- **pyrqg-workload**: Workload simulation

## Commands

```bash
# Start all services
make up

# Run query generation
make generate

# Run performance test
make perf-test

# Open shell in container
make shell

# Run tests
make test

# Clean up
make clean
```

## Custom Configuration

Edit `docker-compose.yml` to:
- Change database credentials
- Adjust resource limits
- Add custom grammars
- Configure networking

## Production Deployment

For production:
1. Use secrets for passwords
2. Set resource limits
3. Configure logging
4. Use named volumes
5. Set up monitoring
"""
    
    with open(docker_dir / 'README.md', 'w') as f:
        f.write(readme_content.strip() + '\n')
    
    print(f"\nDocker files created in: {docker_dir}")
    print("To use: cd docker_example_files && make build && make up")


if __name__ == "__main__":
    print("Creating Docker integration example files...")
    create_docker_files()