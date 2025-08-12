"""
HR Management Workload Generator
Simulates realistic hr management patterns
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe

g = Grammar("hr_workload")

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
        template("SELECT * FROM hr_main_table WHERE id = {id}"),
        template("SELECT * FROM hr_main_table WHERE status = '{status}' LIMIT 100"),
        template("SELECT COUNT(*) FROM hr_main_table WHERE created_at > CURRENT_DATE - INTERVAL '{days} days'")
    )
)

# Write operations
g.rule("write_operations",
    choice(
        template("INSERT INTO hr_main_table (name, status) VALUES ('{name}', '{status}')"),
        template("UPDATE hr_main_table SET status = '{new_status}' WHERE id = {id}"),
        template("DELETE FROM hr_main_table WHERE created_at < CURRENT_DATE - INTERVAL '90 days'")
    )
)

# Analytics
g.rule("analytics",
    template("""SELECT DATE_TRUNC('day', created_at) as day, COUNT(*) as count
FROM hr_main_table
WHERE created_at > CURRENT_DATE - INTERVAL '{days} days'
GROUP BY day ORDER BY day DESC""")
)

# Parameters
g.rule("id", number(1, 100000))
g.rule("days", choice(1, 7, 30, 90))
g.rule("status", choice("active", "pending", "completed", "cancelled"))
g.rule("new_status", choice("active", "completed", "cancelled"))
g.rule("name", choice("test1", "test2", "test3"))

grammar = g
