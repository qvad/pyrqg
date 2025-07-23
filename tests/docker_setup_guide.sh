#!/bin/bash
# Docker-based PostgreSQL vs YugabyteDB Comparison Test Setup

echo "======================================================================"
echo "DOCKER SETUP GUIDE FOR POSTGRESQL VS YUGABYTEDB COMPARISON"
echo "======================================================================"
echo ""
echo "This guide shows how to run PyRQG comparison tests between"
echo "PostgreSQL and YugabyteDB using Docker containers."
echo ""

# Check if Docker is installed
if command -v docker &> /dev/null; then
    echo "✓ Docker is installed: $(docker --version)"
else
    echo "✗ Docker not found. Please install Docker first:"
    echo "  https://docs.docker.com/get-docker/"
    exit 1
fi

echo ""
echo "1. STARTING POSTGRESQL CONTAINER"
echo "--------------------------------"
echo "docker run -d \\"
echo "  --name postgres-test \\"
echo "  -e POSTGRES_PASSWORD=postgres \\"
echo "  -e POSTGRES_DB=testdb \\"
echo "  -p 5432:5432 \\"
echo "  postgres:15"

echo ""
echo "2. STARTING YUGABYTEDB CONTAINER"
echo "--------------------------------"
echo "docker run -d \\"
echo "  --name yugabyte-test \\"
echo "  -p 5433:5433 \\"
echo "  -p 7000:7000 \\"
echo "  -p 9000:9000 \\"
echo "  -p 15433:15433 \\"
echo "  yugabytedb/yugabyte:latest \\"
echo "  bin/yugabyted start --daemon=false"

echo ""
echo "3. WAIT FOR DATABASES TO BE READY"
echo "---------------------------------"
echo "# PostgreSQL (usually ready in 5-10 seconds)"
echo "docker exec postgres-test pg_isready"
echo ""
echo "# YugabyteDB (usually ready in 30-60 seconds)"
echo "docker exec yugabyte-test bin/ysqlsh -c 'SELECT 1'"

echo ""
echo "4. CREATE TEST SCHEMA"
echo "--------------------"
cat << 'EOF'
# PostgreSQL
docker exec -i postgres-test psql -U postgres testdb << SQL
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INTEGER,
    price DECIMAL(10,2),
    status VARCHAR(20),
    order_date DATE DEFAULT CURRENT_DATE
);

-- Insert test data
INSERT INTO users (name, email, age) VALUES
('Alice', 'alice@example.com', 28),
('Bob', 'bob@example.com', 34),
('Charlie', 'charlie@example.com', 42);

INSERT INTO orders (user_id, product_name, quantity, price, status) VALUES
(1, 'Laptop', 1, 999.99, 'completed'),
(2, 'Mouse', 2, 29.99, 'completed'),
(1, 'Keyboard', 1, 79.99, 'shipped');
SQL

# YugabyteDB (same schema)
docker exec -i yugabyte-test bin/ysqlsh -U yugabyte << SQL
-- Same CREATE TABLE and INSERT statements
SQL
EOF

echo ""
echo "5. RUN PYRQG COMPARISON TEST"
echo "---------------------------"
cat << 'EOF'
# Create comparison configuration
cat > comparison_config.yaml << YAML
comparison:
  postgres:
    dsn: "postgresql://postgres:postgres@localhost:5432/testdb"
  yugabyte:
    dsn: "postgresql://yugabyte@localhost:5433/yugabyte"
  
  explain_analyze:
    enabled: true
    options:
      ANALYZE: true
      BUFFERS: true
      VERBOSE: false
  
  validation:
    ignore_row_order: true  # When no ORDER BY
    float_precision: 6
    compare_performance: true
YAML

# Run comparison test
python3 -m pyrqg compare \
  --config comparison_config.yaml \
  --grammar grammars/yugabyte/transactions_postgres.py \
  --duration 300 \
  --report comparison_results.json
EOF

echo ""
echo "6. EXAMPLE COMPARISON OUTPUT"
echo "---------------------------"
cat << 'EOF'
Query: SELECT * FROM users WHERE age > 30 ORDER BY id
  PostgreSQL: 2 rows, 0.8ms
  YugabyteDB: 2 rows, 2.1ms
  ✓ Results MATCH

Query: SELECT u.name, COUNT(o.id) FROM users u 
       LEFT JOIN orders o ON u.id = o.user_id 
       GROUP BY u.id, u.name ORDER BY u.name
  PostgreSQL: 3 rows, 1.2ms
  YugabyteDB: 3 rows, 4.5ms
  ✓ Results MATCH

EXPLAIN ANALYZE Differences:
- PostgreSQL: HashAggregate with Hash Left Join
- YugabyteDB: GroupAggregate with Merge Left Join
  (Different algorithms but same results)

Summary after 300 seconds:
- Queries tested: 5,234
- Matching results: 5,227 (99.87%)
- Differences: 7 (row order without ORDER BY)
- Avg latency ratio: YugabyteDB 3.2x slower
EOF

echo ""
echo "7. PYTHON CODE EXAMPLE"
echo "---------------------"
cat << 'EOF'
from pyrqg.core.comparison_validator import ResultComparator

# Initialize comparator
comparator = ResultComparator(
    "postgresql://postgres:postgres@localhost:5432/testdb",
    "postgresql://yugabyte@localhost:5433/yugabyte",
    server1_name="PostgreSQL",
    server2_name="YugabyteDB",
    explain_analyze=True
)

# Test a query
query = "SELECT * FROM users ORDER BY id"
result = comparator.compare_query(query)

if result.matches:
    print("✓ Results match!")
else:
    print("✗ Results differ:")
    for diff in result.differences:
        print(f"  - {diff}")

# Show EXPLAIN output
if result.explain1:
    print("\nPostgreSQL EXPLAIN:")
    print(result.explain1)
if result.explain2:
    print("\nYugabyteDB EXPLAIN:")
    print(result.explain2)
EOF

echo ""
echo "8. CLEANUP"
echo "----------"
echo "docker stop postgres-test yugabyte-test"
echo "docker rm postgres-test yugabyte-test"

echo ""
echo "======================================================================"
echo "NOTES:"
echo "- YugabyteDB is PostgreSQL-compatible but uses distributed storage"
echo "- Expect 2-5x higher latency due to distributed coordination"
echo "- Results should match for all deterministic queries with ORDER BY"
echo "- Use PyRQG to continuously validate compatibility"
echo "======================================================================"