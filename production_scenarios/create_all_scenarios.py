#!/usr/bin/env python3
"""
Create all 20 production scenarios for PyRQG testing
"""
import os

scenarios = [
    ("01", "ecommerce", "E-Commerce Platform"),
    ("02", "banking", "Banking System"),
    ("03", "healthcare", "Healthcare Records"),
    ("04", "social_media", "Social Media Network"),
    ("05", "logistics", "Logistics & Shipping"),
    ("06", "hotel", "Hotel Reservation System"),
    ("07", "education", "Educational Platform"),
    ("08", "real_estate", "Real Estate Management"),
    ("09", "manufacturing", "Manufacturing ERP"),
    ("10", "food_delivery", "Food Delivery Service"),
    ("11", "saas", "Subscription SaaS"),
    ("12", "ticketing", "Event Ticketing"),
    ("13", "iot", "IoT Sensor Network"),
    ("14", "gaming", "Gaming Platform"),
    ("15", "cms", "Content Management"),
    ("16", "hr", "HR Management"),
    ("17", "warehouse", "Inventory Warehouse"),
    ("18", "support", "Customer Support"),
    ("19", "trading", "Financial Trading"),
    ("20", "analytics", "Analytics Platform")
]

# Template for creating workload generators
workload_template = '''"""
{name} Workload Generator
Simulates realistic {description} patterns
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe

g = Grammar("{scenario}_workload")

# Main workload distribution
g.rule("query",
    choice(
        ref("read_heavy"),      # 60% - Read operations
        ref("write_operations"), # 30% - Write operations  
        ref("analytics"),       # 10% - Analytics queries
        weights=[60, 30, 10]
    )
)

# Read operations
g.rule("read_heavy",
    choice(
        template("SELECT * FROM {scenario}_main_table WHERE id = {{id}}"),
        template("SELECT * FROM {scenario}_main_table WHERE status = '{{status}}' LIMIT 100"),
        template("SELECT COUNT(*) FROM {scenario}_main_table WHERE created_at > CURRENT_DATE - INTERVAL '{{days}} days'")
    )
)

# Write operations
g.rule("write_operations",
    choice(
        template("INSERT INTO {scenario}_main_table (name, status) VALUES ('{{name}}', '{{status}}')"),
        template("UPDATE {scenario}_main_table SET status = '{{new_status}}' WHERE id = {{id}}"),
        template("DELETE FROM {scenario}_main_table WHERE created_at < CURRENT_DATE - INTERVAL '90 days'")
    )
)

# Analytics
g.rule("analytics",
    template("""SELECT DATE_TRUNC('day', created_at) as day, COUNT(*) as count
FROM {scenario}_main_table
WHERE created_at > CURRENT_DATE - INTERVAL '{{days}} days'
GROUP BY day ORDER BY day DESC""")
)

# Parameters
g.rule("id", number(1, 100000))
g.rule("days", choice(1, 7, 30, 90))
g.rule("status", choice("active", "pending", "completed", "cancelled"))
g.rule("new_status", choice("active", "completed", "cancelled"))
g.rule("name", choice("test1", "test2", "test3"))

grammar = g
'''

# Create placeholder schemas and workloads for remaining scenarios
def create_scenario_files():
    for num, name, description in scenarios[3:]:  # Skip first 3 as they're already created
        # Create schema file
        schema_file = f"production_scenarios/schemas/{num}_{name}.sql"
        if not os.path.exists(schema_file):
            with open(schema_file, 'w') as f:
                f.write(f"""-- {description} Schema
-- Placeholder for {name} scenario

CREATE TABLE IF NOT EXISTS {name}_main_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_{name}_status ON {name}_main_table(status);
CREATE INDEX idx_{name}_created ON {name}_main_table(created_at);
""")
            print(f"Created schema: {schema_file}")
            
        # Create workload file
        workload_file = f"production_scenarios/workloads/{num}_{name}_workload.py"
        if not os.path.exists(workload_file):
            with open(workload_file, 'w') as f:
                f.write(workload_template.format(
                    name=description,
                    description=description.lower(),
                    scenario=name
                ))
            print(f"Created workload: {workload_file}")

if __name__ == "__main__":
    create_scenario_files()
    print("\nAll 20 production scenarios created!")
    print("\nTo start the 2-day test, run:")
    print("  ./start_2day_test.sh")