"""
PyRQG - Python Random Query Generator

A powerful framework for generating random SQL queries for database testing.
"""

__version__ = "1.0.0"

from .dsl.core import Grammar, choice, template, ref, number, maybe, repeat
from .core.engine import Engine, EngineConfig
from .core.executor import create_executor
from .core.result import Result

__all__ = [
    'Grammar', 'choice', 'template', 'ref', 'number', 'maybe', 'repeat',
    'Engine', 'EngineConfig', 'create_executor', 'Result'
]
