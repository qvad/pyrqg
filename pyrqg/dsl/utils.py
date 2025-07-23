"""
Shared Python utilities for use within Grammar Lambdas.
Reduces logic duplication for common tasks like picking tables/columns.
"""
import uuid
from typing import Optional, List, Any

def random_id() -> str:
    """Generate a random 8-char identifier."""
    return str(uuid.uuid4()).replace('-', '')[:8]

def pick_table(ctx: Any) -> Optional[str]:
    """Select a random table name from context."""
    if not ctx.tables:
        return None
    return ctx.rng.choice(list(ctx.tables.keys()))

def get_columns(ctx: Any, table_name: str) -> List[str]:
    """Get column names for a table."""
    if not table_name or table_name not in ctx.tables:
        return []
    return ctx.tables[table_name].get_column_names()
