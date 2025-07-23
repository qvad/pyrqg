"""
Dynamic data generation system for realistic and varied query content.

Provides unlimited data variety with configurable distributions, correlations,
and domain-specific generators for billion-scale unique query generation.
"""

import re
import math
import hashlib
import ipaddress
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
import string

from .entropy import EnhancedRandom


class Distribution(Enum):
    """Supported probability distributions"""
    UNIFORM = "uniform"
    NORMAL = "normal"
    EXPONENTIAL = "exponential"
    ZIPFIAN = "zipfian"
    POISSON = "poisson"
    BINOMIAL = "binomial"


class DataType(Enum):
    """Supported data types"""
    INTEGER = "integer"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    VARCHAR = "varchar"
    TEXT = "text"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    JSON = "json"
    UUID = "uuid"
    INET = "inet"
    ARRAY = "array"


@dataclass
class ColumnSchema:
    """Schema definition for a column"""
    name: str
    data_type: DataType
    nullable: bool = True
    unique: bool = False
    primary_key: bool = False
    foreign_key: Optional[Tuple[str, str]] = None  # (table, column)
    constraints: Dict[str, Any] = field(default_factory=dict)
    distribution: Distribution = Distribution.UNIFORM
    distribution_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableSchema:
    """Schema definition for a table"""
    name: str
    columns: List[ColumnSchema]
    row_count_min: int = 0
    row_count_max: int = 1000000
    indexes: List[List[str]] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)


@dataclass
class DataConfig:
    """Configuration for data generation"""
    # Distribution settings
    default_distribution: Distribution = Distribution.UNIFORM
    string_length_min: int = 1
    string_length_max: int = 50
    
    # Numeric ranges
    integer_min: int = -2147483648
    integer_max: int = 2147483647
    bigint_min: int = -9223372036854775808
    bigint_max: int = 9223372036854775807
    decimal_precision: int = 10
    decimal_scale: int = 2
    
    # Date ranges
    date_min: date = date(1970, 1, 1)
    date_max: date = date(2100, 12, 31)
    
    # String patterns
    varchar_pattern: Optional[str] = None  # Regex pattern
    text_vocabulary_size: int = 10000  # Number of unique words
    
    # Special generators
    enable_realistic_names: bool = True
    enable_realistic_addresses: bool = True
    enable_realistic_emails: bool = True
    enable_realistic_phones: bool = True
    
    # Performance
    cache_size: int = 10000  # Cache generated values
    
    # Correlations
    correlations: List[Dict[str, Any]] = field(default_factory=list)


class DynamicDataGenerator:
    """
    Generates unlimited varieties of realistic data for queries.
    
    Features:
    - Multiple probability distributions
    - Correlated data generation
    - Domain-specific generators (names, emails, etc.)
    - Dynamic schema generation
    - Value caching for performance
    """
    
    def __init__(self, config: DataConfig, rng: EnhancedRandom):
        self.config = config
        self.rng = rng
        
        # Caches
        self._word_cache: List[str] = []
        self._name_cache: List[str] = []
        self._domain_cache: List[str] = []
        self._value_cache: Dict[str, List[Any]] = {}
        
        # Initialize vocabularies
        self._initialize_vocabularies()
        
        # Schema generation state
        self._table_counter = 0
        self._column_counter = 0
    
    def _initialize_vocabularies(self):
        """Initialize word and name vocabularies"""
        # Generate synthetic words
        self._word_cache = self._generate_words(self.config.text_vocabulary_size)
        
        # Common first names
        self._first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
            "Michael", "Linda", "William", "Elizabeth", "David", "Barbara",
            "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
            "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa"
        ]
        
        # Common last names
        self._last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
            "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
            "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
            "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"
        ]
        
        # Email domains
        self._email_domains = [
            "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
            "aol.com", "icloud.com", "mail.com", "protonmail.com",
            "company.com", "example.com", "test.com", "email.com"
        ]
        
        # Street names
        self._street_names = [
            "Main", "Park", "Oak", "Pine", "Maple", "Cedar", "Elm", "View",
            "Washington", "Lake", "Hill", "First", "Second", "Third", "Fourth"
        ]
        
        # Cities
        self._cities = [
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
            "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte"
        ]
        
        # States
        self._states = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ"
        ]
    
    def _generate_words(self, count: int) -> List[str]:
        """Generate synthetic words for text data"""
        words = []
        
        # Common prefixes and suffixes
        prefixes = ["pre", "post", "anti", "de", "dis", "over", "under", "re", "un", ""]
        roots = ["act", "form", "struct", "ject", "port", "script", "vert", "dict", "mit", "tend"]
        suffixes = ["ion", "tion", "able", "ible", "ment", "ness", "ity", "er", "ist", ""]
        
        # Generate combinations
        for _ in range(count):
            prefix = self.rng.choice(prefixes)
            root = self.rng.choice(roots)
            suffix = self.rng.choice(suffixes)
            word = prefix + root + suffix
            
            # Add some variation
            if self.rng.random() < 0.1:
                word = word.upper()
            elif self.rng.random() < 0.2:
                word = word.capitalize()
                
            words.append(word)
            
        return words
    
    def generate_schema(self, complexity: str = "medium") -> TableSchema:
        """Generate a random table schema"""
        # Determine parameters based on complexity
        complexity_params = {
            "simple": {"columns": (2, 5), "types": ["integer", "varchar", "boolean"]},
            "medium": {"columns": (5, 15), "types": ["integer", "varchar", "text", "date", "decimal", "boolean"]},
            "complex": {"columns": (10, 30), "types": DataType._value2member_map_.keys()}
        }
        
        params = complexity_params.get(complexity, complexity_params["medium"])
        
        # Generate table name
        table_name = self._generate_table_name()
        
        # Generate columns
        num_columns = self.rng.randint(*params["columns"])
        columns = []
        
        # Always add an ID column
        columns.append(ColumnSchema(
            name="id",
            data_type=DataType.INTEGER,
            nullable=False,
            unique=True,
            primary_key=True
        ))
        
        # Generate other columns
        for i in range(num_columns - 1):
            column = self._generate_column(params["types"])
            columns.append(column)
            
        # Determine row count range
        row_min = self.rng.choice([0, 1, 10, 100, 1000])
        row_max = row_min * self.rng.choice([10, 100, 1000, 10000])
        
        return TableSchema(
            name=table_name,
            columns=columns,
            row_count_min=row_min,
            row_count_max=row_max
        )
    
    def _generate_table_name(self) -> str:
        """Generate unique table name"""
        prefixes = ["tbl", "tab", "t", "table", "entity", "record", "data"]
        infixes = ["user", "order", "product", "customer", "transaction", 
                  "log", "event", "metric", "config", "setting"]
        
        # Use counter to ensure uniqueness
        self._table_counter += 1
        
        if self.rng.random() < 0.7:
            # Common pattern: prefix_infix_number
            prefix = self.rng.choice(prefixes)
            infix = self.rng.choice(infixes)
            return f"{prefix}_{infix}_{self._table_counter}"
        else:
            # Just infix with number
            infix = self.rng.choice(infixes)
            return f"{infix}_{self._table_counter}"
    
    def _generate_column(self, allowed_types: List[str]) -> ColumnSchema:
        """Generate a random column"""
        self._column_counter += 1
        
        # Column name patterns
        prefixes = ["c", "col", "f", "field", "attr", "prop", ""]
        infixes = ["name", "value", "count", "total", "amount", "status",
                  "type", "code", "flag", "data", "info", "desc"]
        
        # Generate name
        if self.rng.random() < 0.6:
            prefix = self.rng.choice(prefixes)
            infix = self.rng.choice(infixes)
            name = f"{prefix}_{infix}_{self._column_counter}" if prefix else f"{infix}_{self._column_counter}"
        else:
            name = f"column_{self._column_counter}"
            
        # Choose type
        data_type = DataType(self.rng.choice(allowed_types))
        
        # Random attributes
        nullable = self.rng.random() < 0.7
        unique = self.rng.random() < 0.1
        
        # Distribution
        distribution = self.rng.choice(list(Distribution))
        
        return ColumnSchema(
            name=name,
            data_type=data_type,
            nullable=nullable,
            unique=unique,
            distribution=distribution
        )
    
    def generate_value(self, column: ColumnSchema, 
                      context: Optional[Dict[str, Any]] = None) -> Any:
        """Generate a value for a column based on its schema"""
        # Check for NULL
        if column.nullable and self.rng.random() < 0.1:
            return None
            
        # Generate based on data type
        generators = {
            DataType.INTEGER: self._generate_integer,
            DataType.BIGINT: self._generate_bigint,
            DataType.DECIMAL: self._generate_decimal,
            DataType.VARCHAR: self._generate_varchar,
            DataType.TEXT: self._generate_text,
            DataType.BOOLEAN: self._generate_boolean,
            DataType.DATE: self._generate_date,
            DataType.TIMESTAMP: self._generate_timestamp,
            DataType.JSON: self._generate_json,
            DataType.UUID: self._generate_uuid,
            DataType.INET: self._generate_inet,
            DataType.ARRAY: self._generate_array
        }
        
        generator = generators.get(column.data_type)
        if generator:
            return generator(column, context)
        else:
            return self._generate_varchar(column, context)
    
    def _apply_distribution(self, base_value: float, distribution: Distribution,
                           params: Dict[str, Any]) -> float:
        """Apply probability distribution to value generation"""
        if distribution == Distribution.UNIFORM:
            return base_value
            
        elif distribution == Distribution.NORMAL:
            mean = params.get("mean", 0.5)
            std = params.get("std", 0.15)
            return max(0, min(1, self.rng.gauss(mean, std)))
            
        elif distribution == Distribution.EXPONENTIAL:
            rate = params.get("rate", 1.0)
            return 1 - math.exp(-rate * base_value)
            
        elif distribution == Distribution.ZIPFIAN:
            # Approximate Zipfian distribution
            alpha = params.get("alpha", 1.0)
            return base_value ** (-alpha)
            
        else:
            return base_value
    
    def _generate_integer(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> int:
        """Generate integer value"""
        min_val = column.constraints.get("min", self.config.integer_min)
        max_val = column.constraints.get("max", self.config.integer_max)
        
        # Apply distribution
        base = self.rng.random()
        distributed = self._apply_distribution(base, column.distribution, 
                                             column.distribution_params)
        
        # Scale to range
        value = int(min_val + distributed * (max_val - min_val))
        
        # Handle special column names
        if "id" in column.name.lower():
            # IDs should be more sequential
            value = int(context.get("row_number", 1) * 1000 + self.rng.randint(0, 999))
        elif "age" in column.name.lower():
            value = self.rng.randint(0, 120)
        elif "year" in column.name.lower():
            value = self.rng.randint(1900, 2024)
            
        return value
    
    def _generate_bigint(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> int:
        """Generate bigint value"""
        min_val = column.constraints.get("min", self.config.bigint_min)
        max_val = column.constraints.get("max", self.config.bigint_max)
        
        # For bigint, we often want larger values
        if self.rng.random() < 0.3:
            # Sometimes generate very large values
            return self.rng.randint(max_val // 2, max_val)
        else:
            # Normal range
            return self.rng.randint(min_val, max_val // 1000)
    
    def _generate_decimal(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> float:
        """Generate decimal value"""
        precision = column.constraints.get("precision", self.config.decimal_precision)
        scale = column.constraints.get("scale", self.config.decimal_scale)
        
        # Generate within precision limits
        max_int_digits = precision - scale
        max_value = 10 ** max_int_digits - 1
        
        integer_part = self.rng.randint(0, max_value)
        decimal_part = self.rng.randint(0, 10 ** scale - 1)
        
        value = float(f"{integer_part}.{decimal_part:0{scale}d}")
        
        # Handle special column names
        if "price" in column.name.lower() or "amount" in column.name.lower():
            # Prices often follow certain patterns
            if self.rng.random() < 0.3:
                # Round prices (9.99, 19.99, etc.)
                value = float(f"{self.rng.randint(1, 999)}.99")
                
        return value
    
    def _generate_varchar(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> str:
        """Generate varchar value"""
        max_length = column.constraints.get("max_length", self.config.string_length_max)
        
        # Check for special column names
        col_lower = column.name.lower()
        
        if "email" in col_lower:
            return self._generate_email()
        elif "phone" in col_lower:
            return self._generate_phone()
        elif "name" in col_lower:
            if "first" in col_lower:
                return self.rng.choice(self._first_names)
            elif "last" in col_lower:
                return self.rng.choice(self._last_names)
            else:
                return self._generate_full_name()
        elif "address" in col_lower:
            return self._generate_address()
        elif "city" in col_lower:
            return self.rng.choice(self._cities)
        elif "state" in col_lower:
            return self.rng.choice(self._states)
        elif "country" in col_lower:
            return self.rng.choice(["USA", "Canada", "Mexico", "UK", "France", "Germany"])
        elif "code" in col_lower:
            # Generate code-like strings
            length = min(max_length, self.rng.randint(4, 12))
            chars = string.ascii_uppercase + string.digits
            return ''.join(self.rng.choice(chars) for _ in range(length))
        else:
            # Generate random text
            if self.config.varchar_pattern:
                # Use regex pattern if provided
                # This is simplified - real regex generation is complex
                length = self.rng.randint(self.config.string_length_min, 
                                        min(max_length, self.config.string_length_max))
                return ''.join(self.rng.choice(string.ascii_letters) for _ in range(length))
            else:
                # Use vocabulary
                num_words = self.rng.randint(1, 5)
                words = [self.rng.choice(self._word_cache) for _ in range(num_words)]
                text = ' '.join(words)
                return text[:max_length]
    
    def _generate_text(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> str:
        """Generate text value"""
        # Generate longer text
        num_sentences = self.rng.randint(1, 10)
        sentences = []
        
        for _ in range(num_sentences):
            num_words = self.rng.randint(5, 20)
            words = [self.rng.choice(self._word_cache) for _ in range(num_words)]
            sentence = ' '.join(words).capitalize() + '.'
            sentences.append(sentence)
            
        return ' '.join(sentences)
    
    def _generate_boolean(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> bool:
        """Generate boolean value"""
        # Check for bias in column name
        col_lower = column.name.lower()
        
        if "active" in col_lower or "enabled" in col_lower:
            # Bias towards true
            return self.rng.random() < 0.8
        elif "deleted" in col_lower or "disabled" in col_lower:
            # Bias towards false
            return self.rng.random() < 0.2
        else:
            # 50/50
            return self.rng.random() < 0.5
    
    def _generate_date(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> str:
        """Generate date value"""
        min_date = column.constraints.get("min", self.config.date_min)
        max_date = column.constraints.get("max", self.config.date_max)
        
        # Convert to timestamps
        min_ts = datetime.combine(min_date, datetime.min.time()).timestamp()
        max_ts = datetime.combine(max_date, datetime.min.time()).timestamp()
        
        # Generate random timestamp
        ts = self.rng.uniform(min_ts, max_ts)
        date_val = datetime.fromtimestamp(ts).date()
        
        # Handle special column names
        col_lower = column.name.lower()
        if "birth" in col_lower:
            # Birth dates - bias towards 20-80 years ago
            years_ago = self.rng.randint(20, 80)
            date_val = datetime.now().date() - timedelta(days=years_ago * 365)
        elif "created" in col_lower or "updated" in col_lower:
            # Recent dates
            days_ago = self.rng.randint(0, 365)
            date_val = datetime.now().date() - timedelta(days=days_ago)
            
        return date_val.isoformat()
    
    def _generate_timestamp(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> str:
        """Generate timestamp value"""
        # Similar to date but with time component
        date_str = self._generate_date(column, context)
        
        # Add time
        hour = self.rng.randint(0, 23)
        minute = self.rng.randint(0, 59)
        second = self.rng.randint(0, 59)
        microsecond = self.rng.randint(0, 999999)
        
        return f"{date_str} {hour:02d}:{minute:02d}:{second:02d}.{microsecond:06d}"
    
    def _generate_json(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> str:
        """Generate JSON value"""
        # Generate random JSON structure
        depth = self.rng.randint(1, 3)
        
        def generate_json_value(d):
            if d <= 0 or self.rng.random() < 0.3:
                # Leaf value
                value_type = self.rng.choice(["string", "number", "boolean", "null"])
                if value_type == "string":
                    return f'"{self.rng.choice(self._word_cache)}"'
                elif value_type == "number":
                    return str(self.rng.randint(-1000, 1000))
                elif value_type == "boolean":
                    return "true" if self.rng.random() < 0.5 else "false"
                else:
                    return "null"
            else:
                # Object or array
                if self.rng.random() < 0.5:
                    # Object
                    num_keys = self.rng.randint(1, 5)
                    pairs = []
                    for _ in range(num_keys):
                        key = self.rng.choice(["id", "name", "value", "type", "status", "count"])
                        val = generate_json_value(d - 1)
                        pairs.append(f'"{key}": {val}')
                    return "{" + ", ".join(pairs) + "}"
                else:
                    # Array
                    num_items = self.rng.randint(1, 5)
                    items = [generate_json_value(d - 1) for _ in range(num_items)]
                    return "[" + ", ".join(items) + "]"
                    
        return generate_json_value(depth)
    
    def _generate_uuid(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> str:
        """Generate UUID value"""
        # Generate UUID v4-like string
        parts = []
        for length in [8, 4, 4, 4, 12]:
            part = ''.join(self.rng.choice('0123456789abcdef') for _ in range(length))
            parts.append(part)
        return '-'.join(parts)
    
    def _generate_inet(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> str:
        """Generate INET (IP address) value"""
        if self.rng.random() < 0.7:
            # IPv4
            octets = [str(self.rng.randint(0, 255)) for _ in range(4)]
            return '.'.join(octets)
        else:
            # IPv6 (simplified)
            parts = [f"{self.rng.randint(0, 65535):04x}" for _ in range(8)]
            return ':'.join(parts)
    
    def _generate_array(self, column: ColumnSchema, context: Optional[Dict[str, Any]]) -> str:
        """Generate array value"""
        # Determine element type and count
        element_type = column.constraints.get("element_type", "integer")
        min_size = column.constraints.get("min_size", 0)
        max_size = column.constraints.get("max_size", 10)
        
        size = self.rng.randint(min_size, max_size)
        
        # Generate elements
        elements = []
        for _ in range(size):
            if element_type == "integer":
                elements.append(str(self.rng.randint(-1000, 1000)))
            elif element_type == "text":
                elements.append(f"'{self.rng.choice(self._word_cache)}'")
            else:
                elements.append(str(self.rng.random()))
                
        return "{" + ",".join(elements) + "}"
    
    def _generate_email(self) -> str:
        """Generate realistic email address"""
        first = self.rng.choice(self._first_names).lower()
        last = self.rng.choice(self._last_names).lower()
        domain = self.rng.choice(self._email_domains)
        
        # Various email patterns
        patterns = [
            f"{first}.{last}@{domain}",
            f"{first}{last}@{domain}",
            f"{first[0]}{last}@{domain}",
            f"{first}_{last}@{domain}",
            f"{first}{self.rng.randint(1, 999)}@{domain}"
        ]
        
        return self.rng.choice(patterns)
    
    def _generate_phone(self) -> str:
        """Generate realistic phone number"""
        # US phone number format
        area = self.rng.randint(200, 999)
        prefix = self.rng.randint(200, 999)
        line = self.rng.randint(0, 9999)
        
        formats = [
            f"({area}) {prefix}-{line:04d}",
            f"{area}-{prefix}-{line:04d}",
            f"+1-{area}-{prefix}-{line:04d}",
            f"{area}.{prefix}.{line:04d}"
        ]
        
        return self.rng.choice(formats)
    
    def _generate_full_name(self) -> str:
        """Generate full name"""
        first = self.rng.choice(self._first_names)
        last = self.rng.choice(self._last_names)
        
        if self.rng.random() < 0.1:
            # Sometimes include middle initial
            middle = self.rng.choice(string.ascii_uppercase)
            return f"{first} {middle}. {last}"
        else:
            return f"{first} {last}"
    
    def _generate_address(self) -> str:
        """Generate realistic address"""
        number = self.rng.randint(1, 9999)
        street = self.rng.choice(self._street_names)
        suffix = self.rng.choice(["St", "Ave", "Rd", "Blvd", "Ln", "Dr", "Way"])
        
        return f"{number} {street} {suffix}"
    
    def generate_correlated_values(self, correlation: Dict[str, Any]) -> Dict[str, Any]:
        """Generate correlated values based on rules"""
        values = {}
        
        correlation_type = correlation.get("type")
        fields = correlation.get("fields", [])
        
        if correlation_type == "sequential":
            # E.g., order_date < ship_date < delivery_date
            base_date = datetime.now() - timedelta(days=self.rng.randint(0, 365))
            
            for i, field in enumerate(fields):
                offset = timedelta(days=self.rng.randint(i * 2, (i + 1) * 5))
                values[field] = (base_date + offset).date().isoformat()
                
        elif correlation_type == "dependent":
            # E.g., state determines valid zip codes
            constraint = correlation.get("constraint")
            # This would require more complex logic
            # For now, just generate related values
            base_value = self.rng.randint(10000, 99999)
            for i, field in enumerate(fields):
                values[field] = base_value + i * 1000
                
        elif correlation_type == "proportional":
            # E.g., quantity affects total price
            base = self.rng.uniform(1, 100)
            multiplier = correlation.get("multiplier", 1.0)
            
            values[fields[0]] = base
            values[fields[1]] = base * multiplier
            
        return values
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get generation statistics"""
        return {
            "tables_generated": self._table_counter,
            "columns_generated": self._column_counter,
            "cache_sizes": {
                "words": len(self._word_cache),
                "values": sum(len(v) for v in self._value_cache.values())
            },
            "vocabulary_size": self.config.text_vocabulary_size
        }