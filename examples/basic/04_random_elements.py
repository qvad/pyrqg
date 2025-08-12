#!/usr/bin/env python3
"""
04_random_elements.py - Random Value Generation

This example demonstrates PyRQG's random value generation capabilities:
- Number ranges and distributions
- String generation patterns
- Date and time values
- Weighted choices
- Custom random elements

Key concepts:
- Random number generation
- Choice with weights
- Maybe (optional) elements
- Repeat for lists
- Custom randomization
"""

import sys
from pathlib import Path
import string

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import (
    Grammar, choice, number, digit, maybe, repeat, 
    template, ref, Lambda, Element, Context
)


def number_generation():
    """Demonstrate various number generation patterns."""
    
    print("Number Generation Examples")
    print("=" * 50)
    
    grammar = Grammar("numbers")
    
    # Basic number ranges
    grammar.rule("small_int", number(1, 10))
    grammar.rule("medium_int", number(100, 1000))
    grammar.rule("large_int", number(10000, 1000000))
    grammar.rule("negative", number(-100, -1))
    grammar.rule("around_zero", number(-10, 10))
    
    # Single digits
    grammar.rule("single_digit", digit())
    
    # Composed numbers
    grammar.rule("phone", template(
        "{area}-{exchange}-{number}",
        area=repeat(digit(), 3, 3, ""),
        exchange=repeat(digit(), 3, 3, ""),
        number=repeat(digit(), 4, 4, "")
    ))
    
    # Decimal numbers using composition
    grammar.rule("decimal", template(
        "{whole}.{fraction}",
        whole=number(0, 999),
        fraction=template("{d1}{d2}", d1=digit(), d2=digit())
    ))
    
    # Percentages
    grammar.rule("percentage", template(
        "{value}%",
        value=choice(0, 25, 50, 75, 100, weights=[10, 20, 40, 20, 10])
    ))
    
    # Currency
    grammar.rule("price", Lambda(lambda ctx: 
        "${:.2f}".format(ctx.rng.uniform(0.99, 999.99))
    ))
    
    # Show examples
    examples = [
        ("Small integers (1-10)", "small_int"),
        ("Medium integers (100-1000)", "medium_int"),
        ("Large integers (10k-1M)", "large_int"),
        ("Negative numbers", "negative"),
        ("Around zero (-10 to 10)", "around_zero"),
        ("Single digit", "single_digit"),
        ("Phone number", "phone"),
        ("Decimal number", "decimal"),
        ("Percentage", "percentage"),
        ("Price", "price")
    ]
    
    for desc, rule in examples:
        print(f"\n{desc}:")
        for i in range(3):
            print(f"  {grammar.generate(rule, seed=i)}")


def string_generation():
    """Show string generation patterns."""
    
    print("\n\nString Generation Examples")
    print("=" * 50)
    
    grammar = Grammar("strings")
    
    # Predefined choices
    grammar.rule("first_name", choice(
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank",
        weights=[20, 20, 15, 15, 15, 15]  # Alice and Bob more common
    ))
    
    grammar.rule("last_name", choice(
        "Smith", "Johnson", "Williams", "Brown", "Jones"
    ))
    
    grammar.rule("domain", choice(
        "gmail.com", "yahoo.com", "outlook.com", "example.com",
        weights=[40, 20, 20, 20]  # Gmail most common
    ))
    
    # Composed strings
    grammar.rule("full_name", template(
        "{first} {last}",
        first=ref("first_name"),
        last=ref("last_name")
    ))
    
    grammar.rule("email", template(
        "{first}.{last}@{domain}",
        first=Lambda(lambda ctx: ctx.rng.choice(["alice", "bob", "charlie"]).lower()),
        last=Lambda(lambda ctx: ctx.rng.choice(["smith", "jones", "brown"]).lower()),
        domain=ref("domain")
    ))
    
    # Random string generation
    grammar.rule("random_string", Lambda(lambda ctx:
        ''.join(ctx.rng.choices(string.ascii_lowercase, k=8))
    ))
    
    grammar.rule("random_hex", Lambda(lambda ctx:
        ''.join(ctx.rng.choices(string.hexdigits.lower(), k=16))
    ))
    
    grammar.rule("product_code", template(
        "{prefix}-{numbers}-{suffix}",
        prefix=choice("PRD", "ITM", "SKU"),
        numbers=repeat(digit(), 4, 4, ""),
        suffix=choice("A", "B", "C", "X", "Y", "Z")
    ))
    
    # Realistic text patterns
    grammar.rule("lorem_word", choice(
        "lorem", "ipsum", "dolor", "sit", "amet",
        "consectetur", "adipiscing", "elit", "sed", "do"
    ))
    
    grammar.rule("lorem_sentence", template(
        "{words}.",
        words=repeat(ref("lorem_word"), min=5, max=10, separator=" ")
    ))
    
    # Status values with realistic distribution
    grammar.rule("order_status", choice(
        "pending",     # 30%
        "processing",  # 25%
        "shipped",     # 20%
        "delivered",   # 20%
        "cancelled",   # 5%
        weights=[30, 25, 20, 20, 5]
    ))
    
    # Show examples
    examples = [
        ("First name", "first_name"),
        ("Full name", "full_name"),
        ("Email address", "email"),
        ("Random string", "random_string"),
        ("Hex string", "random_hex"),
        ("Product code", "product_code"),
        ("Lorem sentence", "lorem_sentence"),
        ("Order status", "order_status")
    ]
    
    for desc, rule in examples:
        print(f"\n{desc}:")
        for i in range(3):
            print(f"  {grammar.generate(rule, seed=i*10)}")


def date_time_generation():
    """Generate date and time values."""
    
    print("\n\nDate/Time Generation Examples")
    print("=" * 50)
    
    grammar = Grammar("datetime")
    
    # Date components
    grammar.rule("year", choice(2020, 2021, 2022, 2023, 2024))
    grammar.rule("month", number(1, 12))
    grammar.rule("day", number(1, 28))  # Safe for all months
    
    # Time components
    grammar.rule("hour", number(0, 23))
    grammar.rule("minute", number(0, 59))
    grammar.rule("second", number(0, 59))
    
    # Formatted dates
    grammar.rule("iso_date", template(
        "{year}-{month:02d}-{day:02d}",
        year=ref("year"),
        month=ref("month"),
        day=ref("day")
    ))
    
    # Use Lambda for proper formatting
    grammar.rule("formatted_date", Lambda(lambda ctx:
        f"{ctx.rng.choice([2020, 2021, 2022, 2023, 2024])}-"
        f"{ctx.rng.randint(1, 12):02d}-"
        f"{ctx.rng.randint(1, 28):02d}"
    ))
    
    grammar.rule("timestamp", Lambda(lambda ctx:
        f"{ctx.rng.choice([2023, 2024])}-"
        f"{ctx.rng.randint(1, 12):02d}-"
        f"{ctx.rng.randint(1, 28):02d} "
        f"{ctx.rng.randint(0, 23):02d}:"
        f"{ctx.rng.randint(0, 59):02d}:"
        f"{ctx.rng.randint(0, 59):02d}"
    ))
    
    # Relative dates
    grammar.rule("relative_date", choice(
        "CURRENT_DATE",
        "CURRENT_DATE - INTERVAL '1 day'",
        "CURRENT_DATE - INTERVAL '7 days'",
        "CURRENT_DATE - INTERVAL '30 days'",
        "CURRENT_DATE - INTERVAL '1 year'",
        "CURRENT_DATE + INTERVAL '1 day'"
    ))
    
    # Business hours
    grammar.rule("business_hour", Lambda(lambda ctx:
        f"{ctx.rng.randint(9, 17):02d}:{ctx.rng.choice(['00', '15', '30', '45'])}:00"
    ))
    
    # Date ranges
    grammar.rule("date_range", Lambda(lambda ctx:
        f"BETWEEN '{2024 - ctx.rng.randint(1, 5)}-01-01' AND '2024-12-31'"
    ))
    
    examples = [
        ("ISO date", "iso_date"),
        ("Formatted date", "formatted_date"),
        ("Timestamp", "timestamp"),
        ("Relative date", "relative_date"),
        ("Business hour", "business_hour"),
        ("Date range", "date_range")
    ]
    
    for desc, rule in examples:
        print(f"\n{desc}:")
        for i in range(3):
            print(f"  {grammar.generate(rule, seed=i*5)}")


def weighted_distributions():
    """Demonstrate weighted random choices."""
    
    print("\n\nWeighted Distribution Examples")
    print("=" * 50)
    
    grammar = Grammar("weighted")
    
    # Customer segments (Pareto principle - 80/20 rule)
    grammar.rule("customer_segment", choice(
        "regular",    # 80% of customers
        "premium",    # 15% of customers
        "vip",        # 5% of customers
        weights=[80, 15, 5]
    ))
    
    # Traffic sources (realistic web traffic)
    grammar.rule("traffic_source", choice(
        "organic",     # 35%
        "direct",      # 25%
        "social",      # 20%
        "referral",    # 15%
        "paid",        # 5%
        weights=[35, 25, 20, 15, 5]
    ))
    
    # Response times (long-tail distribution)
    grammar.rule("response_time_ms", choice(
        number(10, 50),      # 70% - fast
        number(50, 200),     # 20% - normal
        number(200, 1000),   # 8%  - slow
        number(1000, 5000),  # 2%  - very slow
        weights=[70, 20, 8, 2]
    ))
    
    # Error rates (mostly success)
    grammar.rule("http_status", choice(
        200,  # 90% - OK
        201,  # 3%  - Created
        400,  # 3%  - Bad Request
        404,  # 2%  - Not Found
        500,  # 2%  - Server Error
        weights=[90, 3, 3, 2, 2]
    ))
    
    # Product ratings (J-shaped distribution)
    grammar.rule("rating", choice(
        1,    # 5%  - Very bad
        2,    # 10% - Bad
        3,    # 20% - Average
        4,    # 35% - Good
        5,    # 30% - Excellent
        weights=[5, 10, 20, 35, 30]
    ))
    
    # Generate statistics
    print("Generating 1000 samples to show distributions:\n")
    
    distributions = [
        ("Customer Segments", "customer_segment"),
        ("Traffic Sources", "traffic_source"),
        ("HTTP Status Codes", "http_status"),
        ("Product Ratings", "rating")
    ]
    
    for desc, rule in distributions:
        counts = {}
        for i in range(1000):
            value = grammar.generate(rule, seed=i)
            counts[value] = counts.get(value, 0) + 1
        
        print(f"{desc}:")
        for value, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
            percentage = count / 10  # Out of 1000, so /10 gives percentage
            bar = "█" * int(percentage / 2)  # Scale bar to fit
            print(f"  {str(value):12} {count:4d} ({percentage:5.1f}%) {bar}")
        print()


def custom_random_elements():
    """Create custom random elements."""
    
    print("\n\nCustom Random Elements")
    print("=" * 50)
    
    # Custom element for realistic names
    class RealisticName(Element):
        def __init__(self):
            self.first_names = {
                'M': ['James', 'John', 'Robert', 'Michael', 'William'],
                'F': ['Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth']
            }
            self.last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']
        
        def generate(self, context: Context) -> str:
            gender = context.rng.choice(['M', 'F'])
            first = context.rng.choice(self.first_names[gender])
            last = context.rng.choice(self.last_names)
            return f"{first} {last}"
    
    # Custom element for correlated values
    class CorrelatedLocation(Element):
        def __init__(self):
            self.locations = {
                'US': {
                    'cities': ['New York', 'Los Angeles', 'Chicago'],
                    'states': ['NY', 'CA', 'IL'],
                    'zip_prefix': ['10', '90', '60']
                },
                'UK': {
                    'cities': ['London', 'Manchester', 'Birmingham'],
                    'states': ['England', 'England', 'England'],
                    'zip_prefix': ['SW', 'M', 'B']
                }
            }
        
        def generate(self, context: Context) -> str:
            country = context.rng.choice(['US', 'UK'])
            idx = context.rng.randint(0, 2)
            city = self.locations[country]['cities'][idx]
            state = self.locations[country]['states'][idx]
            zip_code = self.locations[country]['zip_prefix'][idx] + \
                       ''.join(str(context.rng.randint(0, 9)) for _ in range(3))
            
            return f"{city}, {state} {zip_code}"
    
    # Custom element for business names
    class BusinessName(Element):
        def __init__(self):
            self.prefixes = ['Global', 'Premier', 'Advanced', 'Digital', 'Smart']
            self.cores = ['Tech', 'Solutions', 'Systems', 'Dynamics', 'Innovations']
            self.suffixes = ['Inc', 'LLC', 'Corp', 'Group', 'Partners']
        
        def generate(self, context: Context) -> str:
            parts = [
                context.rng.choice(self.prefixes),
                context.rng.choice(self.cores),
                context.rng.choice(self.suffixes)
            ]
            return ' '.join(parts)
    
    # Use custom elements in grammar
    grammar = Grammar("custom")
    grammar.rule("person_name", RealisticName())
    grammar.rule("address", CorrelatedLocation())
    grammar.rule("company", BusinessName())
    
    # UUID-like generator
    grammar.rule("uuid", Lambda(lambda ctx:
        '-'.join([
            ''.join(ctx.rng.choices(string.hexdigits.lower(), k=8)),
            ''.join(ctx.rng.choices(string.hexdigits.lower(), k=4)),
            ''.join(ctx.rng.choices(string.hexdigits.lower(), k=4)),
            ''.join(ctx.rng.choices(string.hexdigits.lower(), k=4)),
            ''.join(ctx.rng.choices(string.hexdigits.lower(), k=12))
        ])
    ))
    
    # Show examples
    examples = [
        ("Realistic name", "person_name"),
        ("Correlated address", "address"),
        ("Business name", "company"),
        ("UUID", "uuid")
    ]
    
    for desc, rule in examples:
        print(f"\n{desc}:")
        for i in range(5):
            print(f"  {grammar.generate(rule, seed=i*3)}")


def optional_elements():
    """Demonstrate optional (maybe) elements."""
    
    print("\n\nOptional Elements (Maybe)")
    print("=" * 50)
    
    grammar = Grammar("optional")
    
    # SQL query with optional clauses
    grammar.rule("flexible_query", template(
        "SELECT {distinct} {columns} FROM {table} {where} {order} {limit}",
        distinct=maybe("DISTINCT", 0.2),  # 20% chance
        columns=choice("*", "id, name", "COUNT(*)"),
        table="users",
        where=maybe("WHERE active = true", 0.7),  # 70% chance
        order=maybe("ORDER BY created_at DESC", 0.5),  # 50% chance
        limit=maybe(template("LIMIT {n}", n=choice(10, 50, 100)), 0.3)  # 30% chance
    ))
    
    print("Queries with optional clauses:")
    for i in range(10):
        query = grammar.generate("flexible_query", seed=i*2)
        # Clean up extra spaces
        query = " ".join(query.split())
        print(f"{i+1:2d}. {query}")
    
    # Optional with different probabilities
    grammar.rule("user_data", template(
        "({id}, {name}, {email}, {phone}, {address})",
        id=number(1, 1000),
        name="'John Doe'",
        email="'john@example.com'",
        phone=maybe("'555-1234'", 0.8),  # 80% have phone
        address=maybe("'123 Main St'", 0.3)  # 30% have address
    ))
    
    print("\n\nUser data with optional fields:")
    for i in range(5):
        print(f"  {grammar.generate('user_data', seed=i+50)}")


def main():
    """Run all random generation examples."""
    
    number_generation()
    string_generation()
    date_time_generation()
    weighted_distributions()
    custom_random_elements()
    optional_elements()
    
    print("\n" + "=" * 50)
    print("Random Generation Summary:")
    print("- number() for integer ranges")
    print("- choice() with weights for realistic distributions")
    print("- Lambda for complex random logic")
    print("- maybe() for optional elements")
    print("- Custom Element classes for specialized needs")
    print("- Weighted choices model real-world distributions")


if __name__ == "__main__":
    main()