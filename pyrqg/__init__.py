"""
PyRQG - Python Random Query Generator

A powerful framework for generating random SQL queries for database testing.
"""

__version__ = "1.0.0"

from .dsl.core import Grammar, choice, template, ref, number, maybe, repeat
from .api import RQG, create_rqg

__all__ = [
    'Grammar', 'choice', 'template', 'ref', 'number', 'maybe', 'repeat',
    'RQG', 'create_rqg'
]
