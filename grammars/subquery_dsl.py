#!/usr/bin/env python3
"""
Subquery Grammar using Ultra-Simple Python DSL

Shows how complex SQL patterns can be defined declaratively.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, table, field, number, ref

# Create grammar with one-liner configuration
g = (Grammar("subqueries")
     .define_tables(users=1000, orders=5000, products=100, reviews=10000)
     .define_fields('id', 'user_id', 'product_id', 'price', 'quantity', 'rating'))

# ============================================================================
# Subquery Patterns - Incredibly Simple!
# ============================================================================

# Scalar subquery
g.rule("scalar_subquery",
    template("SELECT * FROM {t1} WHERE {f1} = (SELECT MAX({f2}) FROM {t2})",
        t1=table(), f1=field(), f2=field(), t2=table())
)

# EXISTS subquery  
g.rule("exists_subquery",
    template("SELECT * FROM {t1} t1 WHERE EXISTS (SELECT 1 FROM {t2} t2 WHERE t2.{f} = t1.{f})",
        t1=table(), t2=table(), f=field())
)

# IN subquery
g.rule("in_subquery",
    template("SELECT * FROM {t1} WHERE {f1} IN (SELECT {f2} FROM {t2} WHERE {f3} > {n})",
        t1=table(), f1=field(), f2=field(), t2=table(), f3=field(), n=number(1, 100))
)

# ANY/ALL subqueries
g.rule("any_all_subquery",
    choice(
        template("SELECT * FROM {t} WHERE {f} > ANY (SELECT {f} FROM {t})",
            t=table(), f=field()),
        template("SELECT * FROM {t} WHERE {f} = ALL (SELECT {f} FROM {t} WHERE {f} IS NOT NULL)",
            t=table(), f=field()),
        template("SELECT * FROM {t} WHERE {f} < SOME (SELECT {f} FROM {t})",
            t=table(), f=field())
    )
)

# Correlated subquery
g.rule("correlated_subquery",
    template(
        "SELECT *, (SELECT COUNT(*) FROM {t2} t2 WHERE t2.{f1} = t1.{f1}) as count "
        "FROM {t1} t1 WHERE t1.{f2} > {n}",
        t1=table(), t2=table(), f1=field(), f2=field(), n=number()
    )
)

# Complex nested subquery
g.rule("nested_subquery",
    template(
        "SELECT * FROM {t1} WHERE {f1} IN ("
        "  SELECT {f2} FROM {t2} WHERE {f3} > ("
        "    SELECT AVG({f4}) FROM {t3}"
        "  )"
        ")",
        t1=table(), t2=table(), t3=table(),
        f1=field(), f2=field(), f3=field(), f4=field()
    )
)

# Main query combining different subquery types
g.rule("query",
    choice(
        ref("scalar_subquery"),
        ref("exists_subquery"),
        ref("in_subquery"),
        ref("any_all_subquery"),
        ref("correlated_subquery"),
        ref("nested_subquery")
    )
)

# ============================================================================
# Even More Concise: Define Everything Inline!
# ============================================================================

# One-liner JOIN with subquery
g.rule("join_subquery",
    "SELECT t1.*, sub.avg_price FROM orders t1 JOIN " +
    "(SELECT user_id, AVG(price) as avg_price FROM orders GROUP BY user_id) sub " +
    "ON t1.user_id = sub.user_id"
)

# ============================================================================
# Usage
# ============================================================================

if __name__ == "__main__":
    print("=== Subquery Patterns with Python DSL ===\n")
    
    # Generate different subquery types
    for rule in ["scalar_subquery", "exists_subquery", "in_subquery", 
                 "any_all_subquery", "correlated_subquery", "nested_subquery"]:
        print(f"{rule}:")
        print(f"  {g.generate(rule)}\n")
    
    print("=== Random Queries ===\n")
    for i in range(5):
        print(f"{i+1}. {g.generate('query', seed=i)}\n")
    
    print("=== How Simple Is This? ===")
    print("• Define tables in one line")
    print("• Define patterns with template()")
    print("• Use choice() for alternatives")
    print("• Reference other rules with ref()")
    print("• That's it! No complex syntax to learn")