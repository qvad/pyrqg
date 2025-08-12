#!/usr/bin/env python3
"""
01_custom_elements.py - Creating Custom DSL Elements

This example demonstrates how to create custom DSL elements:
- Understanding the Element base class
- Implementing the generate() method
- Using context in custom elements
- Creating reusable element libraries
- Advanced element patterns

Key concepts:
- Element inheritance
- Context usage
- Stateful elements
- Element composition
- Error handling
"""

import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import random

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Element, Context, Grammar, template, choice, ref


class SmartIdentifier(Element):
    """Generate context-aware identifiers based on table type."""
    
    def __init__(self, prefix_map: Optional[Dict[str, str]] = None):
        self.prefix_map = prefix_map or {
            'users': 'usr',
            'products': 'prd',
            'orders': 'ord',
            'customers': 'cst'
        }
    
    def generate(self, context: Context) -> str:
        # Try to determine current table from context
        current_table = getattr(context, 'current_table', None)
        
        if current_table and current_table in self.prefix_map:
            prefix = self.prefix_map[current_table]
        else:
            prefix = 'id'
        
        # Generate unique ID with prefix
        unique_part = context.rng.randint(100000, 999999)
        return f"'{prefix}_{unique_part}'"


class DateRangeElement(Element):
    """Generate date ranges with business logic."""
    
    def __init__(self, range_type: str = "recent"):
        self.range_type = range_type
        self.ranges = {
            'recent': (1, 30),      # Last 30 days
            'quarter': (1, 90),     # Last quarter
            'year': (1, 365),       # Last year
            'historical': (365, 1825)  # 1-5 years ago
        }
    
    def generate(self, context: Context) -> str:
        min_days, max_days = self.ranges.get(self.range_type, (1, 30))
        days_ago = context.rng.randint(min_days, max_days)
        
        return f"CURRENT_DATE - INTERVAL '{days_ago} days'"


class CorrelatedDataElement(Element):
    """Generate correlated data values (e.g., city/state/zip)."""
    
    def __init__(self):
        self.data = [
            {'city': 'New York', 'state': 'NY', 'zip': '10001', 'country': 'USA'},
            {'city': 'Los Angeles', 'state': 'CA', 'zip': '90001', 'country': 'USA'},
            {'city': 'Chicago', 'state': 'IL', 'zip': '60601', 'country': 'USA'},
            {'city': 'Houston', 'state': 'TX', 'zip': '77001', 'country': 'USA'},
            {'city': 'Phoenix', 'state': 'AZ', 'zip': '85001', 'country': 'USA'}
        ]
    
    def generate(self, context: Context) -> str:
        # Store selection in context for correlation
        if not hasattr(context, '_location_data'):
            context._location_data = context.rng.choice(self.data)
        
        return context._location_data
    
    def get_field(self, context: Context, field: str) -> str:
        """Get specific field from correlated data."""
        data = self.generate(context)
        return f"'{data.get(field, 'Unknown')}'"


class ProbabilisticElement(Element):
    """Element that changes behavior based on probabilities."""
    
    def __init__(self, options: List[tuple]):
        """
        Args:
            options: List of (element, probability) tuples
        """
        self.options = options
        self.elements = [opt[0] for opt in options]
        self.weights = [opt[1] for opt in options]
    
    def generate(self, context: Context) -> str:
        # Normalize weights
        total = sum(self.weights)
        normalized = [w/total for w in self.weights]
        
        # Select based on probability
        r = context.rng.random()
        cumsum = 0
        
        for element, prob in zip(self.elements, normalized):
            cumsum += prob
            if r <= cumsum:
                if isinstance(element, Element):
                    return element.generate(context)
                else:
                    return str(element)
        
        # Fallback
        return str(self.elements[-1])


class ConditionalElement(Element):
    """Element that generates based on conditions."""
    
    def __init__(self, condition, true_element, false_element):
        self.condition = condition
        self.true_element = true_element
        self.false_element = false_element
    
    def generate(self, context: Context) -> str:
        # Evaluate condition
        if callable(self.condition):
            result = self.condition(context)
        else:
            result = bool(self.condition)
        
        # Generate appropriate element
        element = self.true_element if result else self.false_element
        
        if isinstance(element, Element):
            return element.generate(context)
        else:
            return str(element)


class SequentialElement(Element):
    """Element that maintains sequence state."""
    
    def __init__(self, start: int = 1, step: int = 1):
        self.start = start
        self.step = step
        self.current = start
    
    def generate(self, context: Context) -> str:
        value = self.current
        self.current += self.step
        return str(value)
    
    def reset(self):
        """Reset sequence to start."""
        self.current = self.start


class ValidationElement(Element):
    """Element that validates generated values."""
    
    def __init__(self, base_element: Element, validator):
        self.base_element = base_element
        self.validator = validator
        self.max_retries = 10
    
    def generate(self, context: Context) -> str:
        for _ in range(self.max_retries):
            value = self.base_element.generate(context)
            
            if self.validator(value, context):
                return value
        
        # Failed validation after retries
        raise ValueError(f"Could not generate valid value after {self.max_retries} attempts")


class CachedElement(Element):
    """Element that caches generated values."""
    
    def __init__(self, base_element: Element, cache_size: int = 100):
        self.base_element = base_element
        self.cache_size = cache_size
        self.cache = []
    
    def generate(self, context: Context) -> str:
        # 70% chance to reuse from cache if available
        if self.cache and context.rng.random() < 0.7:
            return context.rng.choice(self.cache)
        
        # Generate new value
        value = self.base_element.generate(context)
        
        # Add to cache
        if len(self.cache) < self.cache_size:
            self.cache.append(value)
        else:
            # Replace random element
            idx = context.rng.randint(0, self.cache_size - 1)
            self.cache[idx] = value
        
        return value


def demonstrate_custom_elements():
    """Show custom elements in action."""
    
    print("Custom Element Examples")
    print("=" * 50)
    
    # 1. Smart Identifier
    print("\n1. Smart Identifier Element:")
    grammar = Grammar("smart_id")
    grammar.rule("id", SmartIdentifier())
    
    # Set different table contexts
    for table in ['users', 'products', 'orders', 'unknown']:
        grammar.context.current_table = table
        print(f"  Table '{table}': {grammar.generate('id', seed=42)}")
    
    # 2. Date Range Element
    print("\n2. Date Range Element:")
    grammar = Grammar("dates")
    grammar.rule("recent", DateRangeElement("recent"))
    grammar.rule("historical", DateRangeElement("historical"))
    
    print(f"  Recent: {grammar.generate('recent')}")
    print(f"  Historical: {grammar.generate('historical')}")
    
    # 3. Correlated Data
    print("\n3. Correlated Data Element:")
    grammar = Grammar("location")
    location = CorrelatedDataElement()
    
    grammar.rule("full_address", template(
        "{city}, {state} {zip}",
        city=lambda ctx: location.get_field(ctx, 'city'),
        state=lambda ctx: location.get_field(ctx, 'state'),
        zip=lambda ctx: location.get_field(ctx, 'zip')
    ))
    
    print("  Correlated addresses:")
    for i in range(3):
        # Reset context to get new location
        grammar.context._location_data = None
        print(f"    {grammar.generate('full_address', seed=i)}")
    
    # 4. Probabilistic Element
    print("\n4. Probabilistic Element:")
    grammar = Grammar("probabilistic")
    
    prob_element = ProbabilisticElement([
        ("'common_value'", 0.7),    # 70% chance
        ("'rare_value'", 0.2),      # 20% chance
        ("'ultra_rare'", 0.1)       # 10% chance
    ])
    
    grammar.rule("weighted", prob_element)
    
    # Generate many to show distribution
    counts = {}
    for i in range(100):
        value = grammar.generate("weighted", seed=i)
        counts[value] = counts.get(value, 0) + 1
    
    print("  Distribution over 100 generations:")
    for value, count in sorted(counts.items()):
        print(f"    {value}: {count}%")
    
    # 5. Conditional Element
    print("\n5. Conditional Element:")
    grammar = Grammar("conditional")
    
    # Condition based on context
    def is_large_table(ctx):
        current_table = getattr(ctx, 'current_table', '')
        large_tables = ['orders', 'transactions', 'logs']
        return current_table in large_tables
    
    conditional = ConditionalElement(
        condition=is_large_table,
        true_element=template("SELECT * FROM {table} LIMIT 1000"),
        false_element=template("SELECT * FROM {table}")
    )
    
    grammar.rule("smart_select", conditional)
    grammar.rule("table", lambda ctx: ctx.current_table)
    
    for table in ['users', 'orders', 'products']:
        grammar.context.current_table = table
        print(f"  {table}: {grammar.generate('smart_select')}")
    
    # 6. Sequential Element
    print("\n6. Sequential Element:")
    grammar = Grammar("sequential")
    seq = SequentialElement(start=1000, step=1)
    grammar.rule("order_id", seq)
    
    print("  Sequential IDs:")
    for i in range(5):
        print(f"    Order #{grammar.generate('order_id')}")
    
    # 7. Validation Element
    print("\n7. Validation Element:")
    grammar = Grammar("validation")
    
    # Validator function - only even numbers
    def is_even(value, ctx):
        try:
            return int(value) % 2 == 0
        except:
            return False
    
    base = choice("1", "2", "3", "4", "5", "6")
    validated = ValidationElement(base, is_even)
    
    grammar.rule("even_only", validated)
    
    print("  Even numbers only:")
    for i in range(5):
        print(f"    {grammar.generate('even_only', seed=i*10)}")
    
    # 8. Cached Element
    print("\n8. Cached Element:")
    grammar = Grammar("cached")
    
    # Base element that's expensive to generate
    expensive = choice(
        "'value_1'", "'value_2'", "'value_3'", 
        "'value_4'", "'value_5'"
    )
    
    cached = CachedElement(expensive, cache_size=3)
    grammar.rule("cached_value", cached)
    
    print("  Cached values (notice repetition):")
    for i in range(10):
        print(f"    {grammar.generate('cached_value', seed=i)}")


def create_element_library():
    """Create a reusable library of custom elements."""
    
    print("\n\nCreating Reusable Element Library")
    print("=" * 50)
    
    class ElementLibrary:
        """Collection of reusable custom elements."""
        
        @staticmethod
        def email(domain: str = None):
            """Generate email addresses."""
            class EmailElement(Element):
                def generate(self, context: Context) -> str:
                    first_names = ['john', 'jane', 'bob', 'alice', 'charlie']
                    last_names = ['smith', 'jones', 'brown', 'davis', 'wilson']
                    
                    first = context.rng.choice(first_names)
                    last = context.rng.choice(last_names)
                    
                    if domain:
                        return f"'{first}.{last}@{domain}'"
                    else:
                        domains = ['gmail.com', 'yahoo.com', 'outlook.com']
                        d = context.rng.choice(domains)
                        return f"'{first}.{last}@{d}'"
            
            return EmailElement()
        
        @staticmethod
        def phone(country_code: str = '+1'):
            """Generate phone numbers."""
            class PhoneElement(Element):
                def generate(self, context: Context) -> str:
                    area = context.rng.randint(200, 999)
                    exchange = context.rng.randint(200, 999)
                    number = context.rng.randint(1000, 9999)
                    return f"'{country_code} ({area}) {exchange}-{number}'"
            
            return PhoneElement()
        
        @staticmethod
        def json_object(fields: Dict[str, Element]):
            """Generate JSON objects."""
            class JSONElement(Element):
                def generate(self, context: Context) -> str:
                    obj = {}
                    for key, element in fields.items():
                        if isinstance(element, Element):
                            obj[key] = element.generate(context).strip("'")
                        else:
                            obj[key] = str(element).strip("'")
                    
                    import json
                    return f"'{json.dumps(obj)}'"
            
            return JSONElement()
    
    # Use the library
    grammar = Grammar("library")
    lib = ElementLibrary()
    
    # Email addresses
    grammar.rule("email", lib.email())
    grammar.rule("company_email", lib.email("company.com"))
    
    # Phone numbers
    grammar.rule("phone", lib.phone())
    grammar.rule("uk_phone", lib.phone("+44"))
    
    # JSON objects
    grammar.rule("user_json", lib.json_object({
        'name': choice("'John'", "'Jane'"),
        'age': choice("25", "30", "35"),
        'active': choice("true", "false")
    }))
    
    print("\nLibrary Elements:")
    print(f"  Email: {grammar.generate('email')}")
    print(f"  Company Email: {grammar.generate('company_email')}")
    print(f"  Phone: {grammar.generate('phone')}")
    print(f"  UK Phone: {grammar.generate('uk_phone')}")
    print(f"  User JSON: {grammar.generate('user_json')}")


def main():
    """Run all custom element examples."""
    
    demonstrate_custom_elements()
    create_element_library()
    
    print("\n" + "=" * 50)
    print("Custom Element Summary:")
    print("- Inherit from Element base class")
    print("- Implement generate(context) method")
    print("- Access context for state and randomness")
    print("- Create reusable, composable components")
    print("- Build domain-specific element libraries")


if __name__ == "__main__":
    main()