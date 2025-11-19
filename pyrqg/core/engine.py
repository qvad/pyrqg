"""Lightweight execution engine used by the legacy CLI.

The goal is to offer a pragmatic bridge between the CLI flags and the
modern runner APIs without depending on the historical (and now missing)
implementation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import importlib.util
from pathlib import Path
from typing import Callable, List, Optional

from pyrqg.dsl.core import Grammar
from .validator import ValidatorRegistry
from .reporter import ReporterRegistry, Reporter


@dataclass
class EngineConfig:
    """Configuration passed to the Engine."""

    grammar_file: str
    duration: int = 300
    queries: Optional[int] = None
    threads: int = 1
    seed: Optional[int] = None
    dsn: Optional[str] = None
    database: str = "postgres"
    validators: List[str] = field(default_factory=lambda: ["error_message"])
    reporters: List[str] = field(default_factory=lambda: ["console"])
    debug: bool = False


@dataclass
class EngineStats:
    """Summary returned after an engine run."""

    queries_total: int = 0
    queries_failed: int = 0

    def record_success(self) -> None:
        self.queries_total += 1

    def record_failure(self) -> None:
        self.queries_total += 1
        self.queries_failed += 1


class Engine:
    """Simple grammar execution engine.

    The engine loads the provided grammar file, generates a fixed number of
    queries, runs validators, and streams events to the selected reporters.
    The implementation is intentionally lightweight so the CLI stays
    functional without the historical engine implementation.
    """

    def __init__(self, config: EngineConfig):
        if not config.grammar_file:
            raise ValueError("grammar_file is required")
        self.config = config
        self._grammar: Optional[Grammar] = None
        self._validators: List[Callable[[str], bool]] = [
            ValidatorRegistry.create(name) for name in (config.validators or [])
        ]
        self._reporters: List[Reporter] = [
            ReporterRegistry.create(name) for name in (config.reporters or [])
        ]
        # Always have at least one reporter so summary is visible
        if not self._reporters:
            self._reporters = [ReporterRegistry.create("console")]

    def _load_grammar(self) -> Grammar:
        if self._grammar is not None:
            return self._grammar
        path = Path(self.config.grammar_file)
        if not path.exists():
            raise FileNotFoundError(f"Grammar file not found: {path}")
        spec = importlib.util.spec_from_file_location("pyrqg_cli_grammar", path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load grammar file: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[arg-type]
        grammar = getattr(module, "g", None)
        if grammar is None:
            raise AttributeError(f"Grammar file {path} must expose a variable named 'g'")
        if not isinstance(grammar, Grammar):
            raise TypeError(
                f"Grammar 'g' in {path} must be an instance of pyrqg.dsl.core.Grammar"
            )
        self._grammar = grammar
        return grammar

    def _emit(self, event: str, payload) -> None:
        for reporter in self._reporters:
            try:
                reporter.emit(event, payload)
            except Exception:
                # Keep reporters best-effort; do not crash the CLI
                pass

    def _run_validators(self, query: str) -> bool:
        result = True
        for validator in self._validators:
            try:
                if not validator(query):
                    result = False
            except Exception:
                result = False
        return result

    def run(self) -> EngineStats:
        grammar = self._load_grammar()
        stats = EngineStats()
        target = self.config.queries or max(1, int(self.config.duration))
        base_seed = self.config.seed

        for i in range(target):
            seed = base_seed + i if base_seed is not None else None
            try:
                query = grammar.generate("query", seed=seed)
            except Exception as exc:
                stats.record_failure()
                self._emit("error", {"type": "generation", "message": str(exc)})
                continue

            if self._run_validators(query):
                stats.record_success()
            else:
                stats.record_failure()
                self._emit(
                    "error",
                    {"type": "validation", "message": "Validator rejected query", "query": query},
                )

        self._emit("summary", stats)
        return stats
