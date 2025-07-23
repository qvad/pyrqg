"""
Data generation module for PyRQG.
Equivalent to data generation from .zz files in Perl RandGen.
"""

import random
import string
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
import json
from pathlib import Path


@dataclass
class FieldDefinition:
    """Definition of a table field."""
    name: str
    data_type: str
    nullable: bool = True
    indexed: bool = False
    primary_key: bool = False
    default: Optional[Any] = None
    charset: Optional[str] = None
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class TableDefinition:
    """Definition of a table."""
    name: str
    fields: List[FieldDefinition] = field(default_factory=list)
    rows: int = 0
    engine: str = "InnoDB"
    charset: str = "utf8"
    indexes: Dict[str, List[str]] = field(default_factory=dict)
    partition: Optional[str] = None


class DataGenerator:
    """Generate table schemas and data based on configuration."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None, seed: Optional[int] = None):
        self.seed = seed or random.randint(0, 2**32)
        self.rng = random.Random(self.seed)
        self.config = config or self._default_config()
        self.tables: List[TableDefinition] = []
    
    def _default_config(self) -> Dict[str, Any]:
        """Default YugabyteDB-style configuration."""
        return {
            "tables": {
                "names": ["A", "B", "C", "D", "AA", "BB", "CC", "DD"],
                "rows": [0, 1, 10, 100],
                "pk": ["integer auto_increment"]
            },
            "fields": {
                "types": ["int", "bigint", "varchar(255)", "char(10)", "decimal(10,2)"],
                "indexes": [None, "key"],
                "null": [None, "not null"],
                "charsets": ["utf8", "latin1"]
            },
            "data": {
                "numbers": ["digit", "tinyint", "null", "integer"],
                "strings": ["letter", "english", "null", "char(10)"],
                "temporals": ["date", "year", "null"]
            }
        }
    
    def load_config(self, config_file: str):
        """Load configuration from a JSON file (converted from .zz)."""
        path = Path(config_file)
        if path.suffix == ".json":
            with open(path, 'r') as f:
                self.config = json.load(f)
        elif path.suffix == ".zz":
            # Parse .zz file format
            self.config = self._parse_zz_file(config_file)
    
    def _parse_zz_file(self, zz_file: str) -> Dict[str, Any]:
        """Parse a .zz file and convert to config dict."""
        # Simplified parser - in real implementation would use proper Perl parser
        config = self._default_config()
        
        with open(zz_file, 'r') as f:
            content = f.read()
        
        # Extract table names
        import re
        names_match = re.search(r"names\s*=>\s*\[(.*?)\]", content, re.DOTALL)
        if names_match:
            names_str = names_match.group(1)
            names = re.findall(r"'([^']+)'", names_str)
            if names:
                config["tables"]["names"] = names
        
        # Extract row counts
        rows_match = re.search(r"rows\s*=>\s*\[(.*?)\]", content, re.DOTALL)
        if rows_match:
            rows_str = rows_match.group(1)
            rows = re.findall(r"\d+", rows_str)
            if rows:
                config["tables"]["rows"] = [int(r) for r in rows]
        
        return config
    
    def generate_schema(self, num_tables: Optional[int] = None) -> List[TableDefinition]:
        """Generate table schemas based on configuration."""
        table_names = self.config["tables"]["names"]
        
        if num_tables is None:
            num_tables = len(table_names)
        else:
            num_tables = min(num_tables, len(table_names))
        
        self.tables = []
        
        for i in range(num_tables):
            table_name = table_names[i]
            table_def = self._generate_table(table_name)
            self.tables.append(table_def)
        
        return self.tables
    
    def _generate_table(self, name: str) -> TableDefinition:
        """Generate a single table definition."""
        table = TableDefinition(name=name)
        
        # Set row count
        row_options = self.config["tables"]["rows"]
        table.rows = self.rng.choice(row_options)
        
        # Always add primary key
        pk_field = FieldDefinition(
            name="pk",
            data_type="int",
            nullable=False,
            primary_key=True,
            indexed=True
        )
        table.fields.append(pk_field)
        
        # Add other fields based on YugabyteDB patterns
        field_configs = [
            ("col_int", "int", True, False),
            ("col_int_key", "int", True, True),
            ("col_varchar", "varchar(255)", True, False),
            ("col_varchar_key", "varchar(255)", True, True),
        ]
        
        # Randomly select which fields to include
        num_fields = self.rng.randint(2, len(field_configs))
        selected_fields = self.rng.sample(field_configs, num_fields)
        
        for fname, ftype, nullable, indexed in selected_fields:
            field_def = FieldDefinition(
                name=fname,
                data_type=ftype,
                nullable=nullable,
                indexed=indexed
            )
            table.fields.append(field_def)
        
        # Add indexes
        for field_def in table.fields:
            if field_def.indexed and not field_def.primary_key:
                table.indexes[f"idx_{field_def.name}"] = [field_def.name]
        
        return table
    
    def generate_create_statements(self) -> List[str]:
        """Generate CREATE TABLE statements."""
        statements = []
        
        for table in self.tables:
            stmt = self._generate_create_table(table)
            statements.append(stmt)
        
        return statements
    
    def _generate_create_table(self, table: TableDefinition) -> str:
        """Generate a CREATE TABLE statement."""
        lines = [f"CREATE TABLE {table.name} ("]
        
        # Fields
        field_defs = []
        for field in table.fields:
            field_def = f"  {field.name} {field.data_type}"
            
            if not field.nullable:
                field_def += " NOT NULL"
            
            if field.primary_key:
                field_def += " PRIMARY KEY"
            elif field.data_type.startswith("int") and field.primary_key:
                field_def += " AUTO_INCREMENT"
            
            field_defs.append(field_def)
        
        # Indexes
        for idx_name, idx_fields in table.indexes.items():
            idx_def = f"  KEY {idx_name} ({', '.join(idx_fields)})"
            field_defs.append(idx_def)
        
        lines.append(",\n".join(field_defs))
        lines.append(f") ENGINE={table.engine} CHARSET={table.charset};")
        
        return "\n".join(lines)
    
    def generate_insert_statements(self, table_name: str, num_rows: Optional[int] = None) -> List[str]:
        """Generate INSERT statements for a table."""
        table = next((t for t in self.tables if t.name == table_name), None)
        if not table:
            raise ValueError(f"Table {table_name} not found")
        
        if num_rows is None:
            num_rows = table.rows
        
        statements = []
        for i in range(num_rows):
            values = self._generate_row_values(table)
            stmt = self._generate_insert(table, values)
            statements.append(stmt)
        
        return statements
    
    def _generate_row_values(self, table: TableDefinition) -> Dict[str, Any]:
        """Generate values for a row."""
        values = {}
        
        for field in table.fields:
            if field.primary_key:
                continue  # Skip auto-increment
            
            # Generate value based on type
            if field.nullable and self.rng.random() < 0.2:
                values[field.name] = None
            elif field.data_type.startswith("int") or field.data_type.startswith("bigint"):
                values[field.name] = self._generate_number(field.data_type)
            elif field.data_type.startswith("varchar") or field.data_type.startswith("char"):
                values[field.name] = self._generate_string(field.data_type)
            elif field.data_type.startswith("decimal"):
                values[field.name] = self._generate_decimal(field.data_type)
            else:
                values[field.name] = self._generate_default_value(field.data_type)
        
        return values
    
    def _generate_number(self, data_type: str) -> int:
        """Generate a number value."""
        if "tinyint" in data_type:
            return self.rng.randint(0, 255)
        elif "bigint" in data_type:
            return self.rng.randint(0, 2**31 - 1)
        else:  # int
            return self.rng.randint(0, 2**16 - 1)
    
    def _generate_string(self, data_type: str) -> str:
        """Generate a string value."""
        # Extract length from type
        import re
        length_match = re.search(r'\((\d+)\)', data_type)
        max_length = int(length_match.group(1)) if length_match else 10
        
        # Generate string based on config
        string_type = self.rng.choice(["letter", "english", "digit"])
        
        if string_type == "letter":
            length = self.rng.randint(1, min(max_length, 10))
            return ''.join(self.rng.choice(string.ascii_letters) for _ in range(length))
        elif string_type == "english":
            words = ["the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog"]
            result = []
            current_length = 0
            while current_length < max_length - 10:
                word = self.rng.choice(words)
                result.append(word)
                current_length += len(word) + 1
            return ' '.join(result)[:max_length]
        else:  # digit
            length = self.rng.randint(1, min(max_length, 10))
            return ''.join(self.rng.choice(string.digits) for _ in range(length))
    
    def _generate_decimal(self, data_type: str) -> float:
        """Generate a decimal value."""
        import re
        match = re.search(r'decimal\((\d+),(\d+)\)', data_type)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            max_val = 10 ** (precision - scale) - 1
            return round(self.rng.uniform(0, max_val), scale)
        return round(self.rng.uniform(0, 9999.99), 2)
    
    def _generate_default_value(self, data_type: str) -> Any:
        """Generate a default value for unknown types."""
        return "default"
    
    def _generate_insert(self, table: TableDefinition, values: Dict[str, Any]) -> str:
        """Generate an INSERT statement with proper escaping."""
        fields = []
        value_list = []
        
        for field_name, value in values.items():
            fields.append(field_name)
            if value is None:
                value_list.append("NULL")
            elif isinstance(value, str):
                # Properly escape single quotes in strings
                escaped_value = value.replace("'", "''")
                value_list.append(f"'{escaped_value}'")
            else:
                value_list.append(str(value))
        
        return f"INSERT INTO {table.name} ({', '.join(fields)}) VALUES ({', '.join(value_list)});"