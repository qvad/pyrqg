"""
Simple Python DSL Framework for RandGen

This provides a declarative way to define SQL grammars with minimal syntax.
Now includes built-in Schema Awareness (replacing separate SchemaAwareContext).
"""

import logging
import random
import re
from typing import List, Dict, Any, Union, Callable, Optional, Tuple
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

from pyrqg.core.schema import Table
from pyrqg.core.introspection import SchemaProvider
from pyrqg.core.valgen import ValueGenerator

logger = logging.getLogger(__name__)


# ============================================================================
# Core DSL Components
# ============================================================================

class Element(ABC):
    """Base class for all DSL elements"""
    @abstractmethod
    def generate(self, context: 'Context') -> str:
        pass


@dataclass
class Context:
    """
    Execution context for grammar generation.

    Unified context that handles:
    1. Random Number Generation (seeded).
    2. State tracking (per-generation variables).
    3. Schema Metadata (tables, columns, types).
    4. Database Introspection (optional, via DSN).
    5. Value Generation (for data population).
    """
    tables: Dict[str, Table] = field(default_factory=dict)
    fields: List[str] = field(default_factory=list)
    state: Dict[str, Any] = field(default_factory=dict)
    seed: Optional[int] = None
    dsn: Optional[str] = None
    _rng: Optional[random.Random] = None
    _val_gen: Optional[ValueGenerator] = None

    def __post_init__(self):
        if self.seed is not None:
            self._rng = random.Random(self.seed)
        else:
            self._rng = random.Random()

        self._val_gen = ValueGenerator(self._rng)

        if self.dsn:
            provider = SchemaProvider(self.dsn)
            introspection_tables = provider.introspect()
            self.tables.update(introspection_tables)

    @property
    def rng(self):
        return self._rng

    def get_table(self, min_rows: Optional[int] = None, max_rows: Optional[int] = None) -> str:
        """Get a table name matching row count constraints."""
        eligible_tables = []
        for table in self.tables.values():
            if min_rows is not None and table.row_count < min_rows:
                continue
            if max_rows is not None and table.row_count > max_rows:
                continue
            eligible_tables.append(table.name)

        if not eligible_tables:
            if self.tables:
                return self.rng.choice(list(self.tables.keys()))
            return "table1"

        return self.rng.choice(eligible_tables)

    def get_field(self, data_type: Optional[str] = None, table: Optional[str] = None) -> str:
        """Get a field name matching type and table constraints."""
        if table and table in self.tables:
            table_meta = self.tables[table]
            candidates = list(table_meta.columns.values())
            if data_type:
                candidates = [c for c in candidates if data_type.lower() in c.data_type.lower()]
            if candidates:
                return self.rng.choice([c.name for c in candidates])

        if not table and self.tables:
            t = self.rng.choice(list(self.tables.values()))
            candidates = list(t.columns.values())
            if data_type:
                candidates = [c for c in candidates if data_type.lower() in c.data_type.lower()]
            if candidates:
                return self.rng.choice([c.name for c in candidates])

        return self.rng.choice(self.fields) if self.fields else "col1"

    def get_column_value(self, table: str, column: str) -> str:
        """Get appropriate value for a column based on its type."""
        if table not in self.tables or column not in self.tables[table].columns:
            return "'unknown'"
        col_meta = self.tables[table].columns[column]

        if col_meta.is_primary_key and col_meta.has_default:
            return 'DEFAULT'

        return self._val_gen.generate(col_meta.data_type)


# ============================================================================
# Basic Elements
# ============================================================================

class Literal(Element):
    """A literal string."""
    def __init__(self, text: str):
        self.text = text

    def generate(self, context: Context) -> str:
        return self.text


class Choice(Element):
    """Random choice from options."""
    def __init__(self, *options: Union[str, Element], weights: Optional[List[int]] = None):
        self.options = list(options)
        self.weights = weights
        self.grammar = None

    def generate(self, context: Context) -> str:
        if self.weights:
            chosen = context.rng.choices(self.options, weights=self.weights)[0]
        else:
            chosen = context.rng.choice(self.options)

        if isinstance(chosen, str):
            if self.grammar and chosen in self.grammar.rules:
                return self.grammar.rules[chosen].generate(context)
            return chosen
        return chosen.generate(context)


class TableElement(Element):
    """Random table name from context."""
    def __init__(self, min_rows: Optional[int] = None, max_rows: Optional[int] = None):
        self.min_rows = min_rows
        self.max_rows = max_rows

    def generate(self, context: Context) -> str:
        return context.get_table(min_rows=self.min_rows, max_rows=self.max_rows)


class FieldElement(Element):
    """Random field name from context."""
    def __init__(self, data_type: Optional[str] = None, table: Optional[str] = None):
        self.data_type = data_type
        self.table = table

    def generate(self, context: Context) -> str:
        return context.get_field(data_type=self.data_type, table=self.table)


class Number(Element):
    """Random number in range."""
    def __init__(self, min_val: int = 0, max_val: int = 100):
        self.min_val = min_val
        self.max_val = max_val

    def generate(self, context: Context) -> str:
        return str(context.rng.randint(self.min_val, self.max_val))


class Digit(Element):
    """Single digit 0-9."""
    def generate(self, context: Context) -> str:
        return str(context.rng.randint(0, 9))


class Maybe(Element):
    """Optional element with probability."""
    def __init__(self, element: Union[str, Element], probability: float = 0.5):
        self.element = Literal(element) if isinstance(element, str) else element
        self.probability = probability

    def generate(self, context: Context) -> str:
        if context.rng.random() < self.probability:
            return self.element.generate(context)
        return ""


class Repeat(Element):
    """Repeat element n times."""
    def __init__(self, element: Union[str, Element], min_val: int = 1, max_val: int = 1, separator: str = " "):
        self.element = Literal(element) if isinstance(element, str) else element
        self.min_val = min_val
        self.max_val = max_val
        self.separator = separator

    def generate(self, context: Context) -> str:
        count = context.rng.randint(self.min_val, self.max_val)
        parts = [self.element.generate(context) for _ in range(count)]
        return self.separator.join(parts)


# ============================================================================
# Template System
# ============================================================================

class Template(Element):
    """
    Template with placeholders.

    Supports placeholders in the format:
    - {name}: Replaced by value of rule 'name'.
    - {name:rule}: Replaced by value of 'rule', stored as 'name' (if needed).
    - {{name}}: Escaped braces (becomes {name}).
    """
    def __init__(self, template: str, grammar=None, **kwargs):
        self.template = template
        self.grammar = grammar
        self.elements = {}

        if grammar:
            parsed_elements = parse_template(template, grammar)
            self.elements.update(parsed_elements)

        for key, value in kwargs.items():
            if isinstance(value, str):
                self.elements[key] = Literal(value)
            elif isinstance(value, Element):
                self.elements[key] = value
            elif callable(value):
                self.elements[key] = Lambda(value)
            else:
                self.elements[key] = Literal(str(value))

    def generate(self, context: Context) -> str:
        values = {}
        for key, element in self.elements.items():
            values[key] = element.generate(context)

        result = self.template
        pattern = r'(?<!\{)\{([^{}:]+)(?::([^{}]+))?\}(?!\})'

        def replacer(match):
            placeholder = match.group(1)
            if placeholder in values:
                return values[placeholder]
            return match.group(0)

        result = re.sub(pattern, replacer, result)

        unresolved = re.findall(pattern, result)
        if unresolved:
            grammar_name = self.grammar.name if self.grammar else "UNKNOWN"
            raise ValueError(
                f"Unresolved placeholders in template for grammar '{grammar_name}': {unresolved}\n"
                f"Template: {self.template}"
            )

        result = result.replace("{{", "{").replace("}}", "}")
        return result


class Lambda(Element):
    """Lambda function element."""
    def __init__(self, func: Callable[[Context], str]):
        self.func = func

    def generate(self, context: Context) -> str:
        return str(self.func(context))


# ============================================================================
# Rule System
# ============================================================================

class Rule:
    """A named grammar rule."""
    def __init__(self, name: str, definition: Union[str, Element, Callable]):
        self.name = name
        if isinstance(definition, str):
            self.definition = Literal(definition)
        elif isinstance(definition, Element):
            self.definition = definition
        elif callable(definition):
            self.definition = Lambda(definition)
        else:
            raise ValueError(f"Invalid rule definition type: {type(definition)}")

    def generate(self, context: Context) -> str:
        return self.definition.generate(context)


class RuleRef(Element):
    """Reference to another rule."""
    def __init__(self, rule_name: str):
        self.rule_name = rule_name
        self.grammar = None

    def generate(self, context: Context) -> str:
        if self.grammar and self.rule_name in self.grammar.rules:
            return self.grammar.rules[self.rule_name].generate(context)
        return f"{{{self.rule_name}}}"


# ============================================================================
# Grammar Container
# ============================================================================

class Grammar:
    """Container for grammar rules."""
    def __init__(self, name: str = "grammar"):
        self.name = name
        self.rules = {}
        self.context = Context()

    def define_tables(self, **tables):
        """Define tables with their row counts."""
        self.context.tables = tables
        return self

    def define_fields(self, *fields):
        """Define available fields."""
        self.context.fields = list(fields)
        return self

    def rule(self, name: str, definition: Union[str, Element, Callable]):
        """Add a rule to the grammar."""
        rule = Rule(name, definition)
        self.rules[name] = rule
        self._set_grammar_refs(rule.definition)
        return rule

    def _set_grammar_refs(self, element):
        """Recursively set grammar reference for all RuleRef instances."""
        if isinstance(element, RuleRef):
            element.grammar = self
        elif isinstance(element, Choice):
            element.grammar = self
            for opt in element.options:
                if not isinstance(opt, str):
                    self._set_grammar_refs(opt)
        elif isinstance(element, (Maybe, Repeat)):
            if hasattr(element, 'element'):
                self._set_grammar_refs(element.element)
        elif isinstance(element, Template):
            element.grammar = self
            if not element.elements:
                parsed_elements = parse_template(element.template, self)
                element.elements.update(parsed_elements)
            for elem in element.elements.values():
                self._set_grammar_refs(elem)

    def generate(self, rule_name: str = "query", seed: Optional[int] = None, context: Any = None) -> str:
        """Generate output from a rule."""
        self._finalize_templates()
        ctx = context if context is not None else self.context

        if seed is not None:
            ctx.seed = seed
            # Always use _rng for Context objects (rng is a read-only property)
            if hasattr(ctx, '_rng'):
                ctx._rng = random.Random(seed)

        if hasattr(ctx, 'state'):
            ctx.state.clear()

        if rule_name not in self.rules:
            raise ValueError(f"Rule '{rule_name}' not found")

        return self.rules[rule_name].generate(ctx)

    def _finalize_templates(self):
        """Finalize all templates to ensure placeholders are resolved."""
        if hasattr(self, '_finalized'):
            return

        for rule in self.rules.values():
            self._finalize_element(rule.definition)

        self._finalized = True

    def _finalize_element(self, element):
        """Recursively finalize an element."""
        if isinstance(element, Template) and element.grammar == self:
            parsed_elements = parse_template(element.template, self)
            for key, elem in parsed_elements.items():
                if key not in element.elements:
                    element.elements[key] = elem
                    self._set_grammar_refs(elem)
        elif isinstance(element, Choice):
            for opt in element.options:
                if not isinstance(opt, str):
                    self._finalize_element(opt)
        elif isinstance(element, (Maybe, Repeat)):
            if hasattr(element, 'element'):
                self._finalize_element(element.element)


# ============================================================================
# Convenience Functions
# ============================================================================

def choice(*options, weights=None):
    """Create a choice element."""
    return Choice(*options, weights=weights)

def maybe(element, probability=0.5):
    """Create an optional element."""
    return Maybe(element, probability)

def repeat(element, min_val=1, max_val=1, sep=" "):
    """Create a repeat element."""
    return Repeat(element, min_val, max_val, sep)

def template(tmpl, **kwargs):
    """Create a template."""
    return Template(tmpl, **kwargs)

def table(min_rows=None, max_rows=None):
    """Create a table reference."""
    return TableElement(min_rows, max_rows)

def field(data_type=None, table=None):
    """Create a field reference."""
    return FieldElement(data_type, table)

def number(min_val=0, max_val=100):
    """Create a number."""
    return Number(min_val, max_val)

def digit():
    """Create a digit."""
    return Digit()

def ref(rule_name):
    """Create a rule reference."""
    return RuleRef(rule_name)


def parse_template(template_str, grammar):
    """Parse template string and extract placeholders as RuleRef elements."""
    pattern = r'(?<!\{)\{([^{}:]+)(?::([^{}]+))?\}(?!\})'
    placeholders = re.findall(pattern, template_str)

    elements = {}
    for placeholder, rule_name in placeholders:
        if placeholder in elements:
            continue
        if rule_name:
            elements[placeholder] = RuleRef(rule_name)
        elif grammar and placeholder in grammar.rules:
            elements[placeholder] = RuleRef(placeholder)

    return elements
