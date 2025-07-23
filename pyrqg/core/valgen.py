"""
Value Generation Strategy.
"""
import random
from typing import Dict, Callable
from pyrqg.core.types import is_numeric, is_string, is_datetime, is_boolean, is_json

class ValueGenerator:
    """Generates random SQL literal values for given types."""

    def __init__(self, rng: random.Random):
        self.rng = rng

    def _generate_varchar(self) -> str:
        values = ["'Test User'", "'Product X'", "'Active Status'", "'user@example.com'", "'Category A'"]
        return self.rng.choice(values)

    def _generate_text(self) -> str:
        return self.rng.choice(["'Sample text'", "'Notes'", "'Description'", "'Info'"])

    def generate(self, data_type: str) -> str:
        """Generate a value for the given type."""
        dtype = data_type.lower()
        
        if is_boolean(dtype):
            return self.rng.choice(['true', 'false'])
        
        if is_numeric(dtype):
            if 'int' in dtype or 'serial' in dtype:
                return str(self.rng.randint(1, 1000))
            if 'smallint' in dtype:
                return str(self.rng.randint(1, 100))
            if 'bigint' in dtype:
                return str(self.rng.randint(1, 100000))
            # Floating point / Decimal
            if 'real' in dtype or 'float' in dtype:
                return f"{self.rng.uniform(0, 1000):.2f}"
            if 'double' in dtype:
                return f"{self.rng.uniform(0, 1000):.4f}"
            # Default numeric/decimal
            return f"{self.rng.randint(1, 10000)}.{self.rng.randint(0, 99):02d}"

        if is_string(dtype):
            if 'char' in dtype and 'var' not in dtype and 'bpchar' not in dtype:
                 return "'A'" # Fixed char
            if 'text' in dtype:
                return self._generate_text()
            return self._generate_varchar()

        if is_datetime(dtype):
            if 'date' in dtype:
                return 'CURRENT_DATE'
            if 'time' in dtype and 'stamp' not in dtype:
                return 'CURRENT_TIME'
            return 'CURRENT_TIMESTAMP'

        if is_json(dtype):
            return "'{}'::jsonb" if 'jsonb' in dtype else "'{}'::json"

        if 'array' in dtype or dtype.endswith('[]'):
            return "ARRAY['item1', 'item2']"

        # Fallback: use NULL for unknown types (safer than string literal)
        return "NULL"
