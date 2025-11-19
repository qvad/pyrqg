"""
Simple Python DSL Framework for RandGen

This provides a declarative way to define SQL grammars with minimal syntax.
"""

import random
from typing import List, Dict, Any, Union, Callable, Optional, Tuple
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import re


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
    """Execution context for grammar generation"""
    tables: Dict[str, int] = field(default_factory=dict)  # table_name: row_count
    fields: List[str] = field(default_factory=list)
    state: Dict[str, Any] = field(default_factory=dict)
    seed: Optional[int] = None
    _rng: Optional[random.Random] = None

    def __post_init__(self):
        if self.seed is not None:
            self._rng = random.Random(self.seed)
        else:
            self._rng = random.Random()

    @property
    def rng(self):
        return self._rng


# ============================================================================
# Basic Elements
# ============================================================================

class Literal(Element):
    """A literal string"""
    def __init__(self, text: str):
        self.text = text
    
    def generate(self, context: Context) -> str:
        return self.text


class Choice(Element):
    """Random choice from options"""
    def __init__(self, *options: Union[str, Element], weights: Optional[List[int]] = None):
        # Store raw options - will convert to proper elements later
        self.options = list(options)
        self.weights = weights
        self.grammar = None
    
    def generate(self, context: Context) -> str:
        if self.weights:
            chosen = context.rng.choices(self.options, weights=self.weights)[0]
        else:
            chosen = context.rng.choice(self.options)
        
        if isinstance(chosen, str):
            # Check if it's a rule reference
            if self.grammar and chosen in self.grammar.rules:
                return self.grammar.rules[chosen].generate(context)
            return chosen
        return chosen.generate(context)


class Table(Element):
    """Random table name from context"""
    def __init__(self, min_rows: Optional[int] = None, max_rows: Optional[int] = None):
        self.min_rows = min_rows
        self.max_rows = max_rows
    
    def generate(self, context: Context) -> str:
        eligible_tables = []
        for table, rows in context.tables.items():
            if self.min_rows is not None and rows < self.min_rows:
                continue
            if self.max_rows is not None and rows > self.max_rows:
                continue
            eligible_tables.append(table)
        
        if not eligible_tables:
            eligible_tables = list(context.tables.keys())
        
        return context.rng.choice(eligible_tables) if eligible_tables else "table1"


class Field(Element):
    """Random field name from context"""
    def __init__(self, type: Optional[str] = None):
        self.type = type
    
    def generate(self, context: Context) -> str:
        if self.type:
            # Filter fields by type
            matching = [f for f in context.fields if self.type in f]
            if matching:
                return context.rng.choice(matching)
        return context.rng.choice(context.fields) if context.fields else "col1"


class Number(Element):
    """Random number in range"""
    def __init__(self, min: int = 0, max: int = 100):
        self.min = min
        self.max = max
    
    def generate(self, context: Context) -> str:
        return str(context.rng.randint(self.min, self.max))


class Digit(Element):
    """Single digit 0-9"""
    def generate(self, context: Context) -> str:
        return str(context.rng.randint(0, 9))


class Maybe(Element):
    """Optional element with probability"""
    def __init__(self, element: Union[str, Element], probability: float = 0.5):
        self.element = Literal(element) if isinstance(element, str) else element
        self.probability = probability
    
    def generate(self, context: Context) -> str:
        if context.rng.random() < self.probability:
            return self.element.generate(context)
        return ""


class Repeat(Element):
    """Repeat element n times"""
    def __init__(self, element: Union[str, Element], min: int = 1, max: int = 1, separator: str = " "):
        self.element = Literal(element) if isinstance(element, str) else element
        self.min = min
        self.max = max
        self.separator = separator
    
    def generate(self, context: Context) -> str:
        count = context.rng.randint(self.min, self.max)
        parts = [self.element.generate(context) for _ in range(count)]
        return self.separator.join(parts)


# ============================================================================
# Template System
# ============================================================================

class Template(Element):
    """Template with placeholders"""
    def __init__(self, template: str, grammar=None, **kwargs):
        self.template = template
        self.grammar = grammar
        self.elements = {}
        
        # Parse template to find placeholders
        if grammar:
            parsed_elements = parse_template(template, grammar)
            self.elements.update(parsed_elements)
        
        # Convert kwargs to elements (overrides parsed elements)
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
        # Generate all values
        values = {}
        for key, element in self.elements.items():
            values[key] = element.generate(context)

        # Template substitution - handle both {key} and {key:rule} formats
        result = self.template
        import re

        # Replace all placeholders
        def replacer(match):
            placeholder = match.group(1)
            # Use the generated value if available
            if placeholder in values:
                return values[placeholder]
            # Otherwise return the original placeholder
            return match.group(0)

        result = re.sub(r'\{([^:}]+)(?::([^}]+))?\}', replacer, result)

        # Check for any placeholders that were not resolved.
        unresolved = re.findall(r'\{([^}]+)\}', result)
        if unresolved:
            grammar_name = self.grammar.name if self.grammar else "UNKNOWN"
            raise ValueError(
                f"Unresolved placeholders in template for grammar '{grammar_name}': {unresolved}\n"
                f"Template: {self.template}"
            )

        return result


class Lambda(Element):
    """Lambda function element"""
    def __init__(self, func: Callable[[Context], str]):
        self.func = func
    
    def generate(self, context: Context) -> str:
        return str(self.func(context))


# ============================================================================
# Rule System
# ============================================================================

class Rule:
    """A named grammar rule"""
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
    """Reference to another rule"""
    def __init__(self, rule_name: str):
        self.rule_name = rule_name
        self.grammar = None  # Set by Grammar
    
    def generate(self, context: Context) -> str:
        if self.grammar and self.rule_name in self.grammar.rules:
            return self.grammar.rules[self.rule_name].generate(context)
        return f"{{{self.rule_name}}}"


# ============================================================================
# Grammar Container
# ============================================================================

class Grammar:
    """Container for grammar rules"""
    def __init__(self, name: str = "grammar"):
        self.name = name
        self.rules = {}
        self.context = Context()
    
    def define_tables(self, **tables):
        """Define tables with their row counts"""
        self.context.tables = tables
        return self
    
    def define_fields(self, *fields):
        """Define available fields"""
        self.context.fields = list(fields)
        return self
    
    def rule(self, name: str, definition: Union[str, Element, Callable]):
        """Add a rule to the grammar"""
        rule = Rule(name, definition)
        self.rules[name] = rule
        
        # Set grammar reference for RuleRef instances
        self._set_grammar_refs(rule.definition)
        
        return rule
    
    def _set_grammar_refs(self, element):
        """Recursively set grammar reference for all RuleRef instances"""
        if isinstance(element, RuleRef):
            element.grammar = self
        elif isinstance(element, Choice):
            # Set grammar on Choice itself
            element.grammar = self
            # Process all options
            for i, opt in enumerate(element.options):
                if isinstance(opt, str):
                    # String options might be rule references
                    pass
                else:
                    # Recursively process non-string options
                    self._set_grammar_refs(opt)
        elif isinstance(element, (Maybe, Repeat)):
            if hasattr(element, 'element'):
                self._set_grammar_refs(element.element)
        elif isinstance(element, Template):
            # Set grammar on template itself so it can parse placeholders
            element.grammar = self
            # Re-parse template with grammar context
            if not element.elements:
                parsed_elements = parse_template(element.template, self)
                element.elements.update(parsed_elements)
            # Set grammar refs on all template elements
            for elem in element.elements.values():
                self._set_grammar_refs(elem)
    
    def generate(self, rule_name: str = "query", seed: Optional[int] = None) -> str:
        """Generate output from a rule"""
        # Ensure all templates are properly initialized
        self._finalize_templates()

        if seed is not None:
            self.context.seed = seed
            self.context._rng = random.Random(seed)

        # Reset per-generation state so grammars start fresh
        self.context.state.clear()

        if rule_name not in self.rules:
            raise ValueError(f"Rule '{rule_name}' not found")

        return self.rules[rule_name].generate(self.context)
    
    def _finalize_templates(self):
        """Finalize all templates to ensure placeholders are resolved"""
        if hasattr(self, '_finalized'):
            return
        
        # Re-process all rules to ensure templates have all placeholders resolved
        for rule in self.rules.values():
            self._finalize_element(rule.definition)
        
        self._finalized = True
    
    def _finalize_element(self, element):
        """Recursively finalize an element"""
        if isinstance(element, Template) and element.grammar == self:
            # Re-parse template now that all rules are defined
            parsed_elements = parse_template(element.template, self)
            # Update elements (don't overwrite existing ones)
            for key, elem in parsed_elements.items():
                if key not in element.elements:
                    element.elements[key] = elem
                    self._set_grammar_refs(elem)
        elif isinstance(element, Choice):
            # Finalize all options in a Choice
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
    """Create a choice element"""
    return Choice(*options, weights=weights)

def maybe(element, probability=0.5):
    """Create an optional element"""
    return Maybe(element, probability)

def repeat(element, min=1, max=1, sep=" "):
    """Create a repeat element"""
    return Repeat(element, min, max, sep)

def template(tmpl, **kwargs):
    """Create a template"""
    return Template(tmpl, **kwargs)

def table(min_rows=None, max_rows=None):
    """Create a table reference"""
    return Table(min_rows, max_rows)

def field(type=None):
    """Create a field reference"""
    return Field(type)

def number(min=0, max=100):
    """Create a number"""
    return Number(min, max)

def digit():
    """Create a digit"""
    return Digit()

def ref(rule_name):
    """Create a rule reference"""
    return RuleRef(rule_name)


def parse_template(template_str, grammar):
    """Parse template string and extract placeholders"""
    import re
    # Match {placeholder} or {placeholder:rule_name}
    pattern = r'\{([^:}]+)(?::([^}]+))?\}'
    placeholders = re.findall(pattern, template_str)
    
    elements = {}
    for placeholder, rule_name in placeholders:
        # Skip if already defined (avoid duplicates)
        if placeholder in elements:
            continue
            
        if rule_name:
            # Explicit rule reference like {name:rule}
            elements[placeholder] = RuleRef(rule_name)
        elif grammar and placeholder in grammar.rules:
            # Implicit rule reference - placeholder matches a rule name
            elements[placeholder] = RuleRef(placeholder)
        # else: will be provided via kwargs or left as placeholder
    
    return elements
