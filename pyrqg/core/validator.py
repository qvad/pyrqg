"""Validator registry used by the lightweight CLI engine."""
from __future__ import annotations

import re
from typing import Callable, Dict, List


ValidatorFn = Callable[[str], bool]


def _not_empty(query: str) -> bool:
    return bool(query and query.strip())


def _has_sql_keyword(query: str) -> bool:
    if not query:
        return False
    keywords = [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
    ]
    upper_query = query.upper()
    return any(keyword in upper_query for keyword in keywords)


def _balanced_parentheses(query: str) -> bool:
    count = 0
    for char in query:
        if char == "(":
            count += 1
        elif char == ")":
            count -= 1
        if count < 0:
            return False
    return count == 0


def _balanced_quotes(query: str) -> bool:
    # naive but effective for lightweight checks
    single = len(re.findall(r"(?<!\\)'", query))
    double = len(re.findall(r'(?<!\\)"', query))
    return single % 2 == 0 and double % 2 == 0


class ValidatorRegistry:
    """Provide reusable validator callables by name."""

    _registry: Dict[str, ValidatorFn] = {
        "error_message": _not_empty,
        "result_set": _has_sql_keyword,
        "performance": _not_empty,
        "transaction": _balanced_parentheses,
        "replication": _balanced_quotes,
        "zero_sum": _has_sql_keyword,
    }

    @classmethod
    def list(cls) -> List[str]:
        return sorted(cls._registry.keys())

    @classmethod
    def create(cls, name: str) -> ValidatorFn:
        try:
            return cls._registry[name]
        except KeyError as exc:
            raise ValueError(f"Unknown validator: {name}") from exc
