"""Reporter registry for the lightweight CLI engine."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


@dataclass
class Reporter:
    name: str

    def emit(self, event: str, payload) -> None:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass
class ConsoleReporter(Reporter):
    verbose: bool = False

    def emit(self, event: str, payload) -> None:
        if event == "error":
            msg = payload.get("message", "") if isinstance(payload, dict) else str(payload)
            print(f"[PyRQG][{self.name}] ERROR: {msg}")
            if isinstance(payload, dict) and payload.get("query"):
                print(payload["query"])
        elif event == "summary":
            print(
                f"[PyRQG][{self.name}] queries_total={payload.queries_total} "
                f"queries_failed={payload.queries_failed}"
            )


@dataclass
class ErrorReporter(ConsoleReporter):
    def emit(self, event: str, payload) -> None:
        if event == "error":
            super().emit(event, payload)


@dataclass
class JsonReporter(Reporter):
    events: List[Dict[str, object]] = field(default_factory=list)

    def emit(self, event: str, payload) -> None:
        if event == "summary":
            data = {
                "queries_total": payload.queries_total,
                "queries_failed": payload.queries_failed,
            }
        else:
            data = payload if isinstance(payload, dict) else {"message": str(payload)}
        self.events.append({"event": event, "data": data})
        if event == "summary":
            print(json.dumps(self.events, indent=2))


@dataclass
class FileReporter(Reporter):
    path: Path
    events: List[Dict[str, object]] = field(default_factory=list)

    def emit(self, event: str, payload) -> None:
        if event == "summary":
            data = {
                "queries_total": payload.queries_total,
                "queries_failed": payload.queries_failed,
            }
        else:
            data = payload if isinstance(payload, dict) else {"message": str(payload)}
        self.events.append({"event": event, "data": data})
        if event == "summary":
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(self.events, indent=2), encoding="utf-8")
            print(f"[PyRQG][file] wrote report to {self.path}")


class ReporterRegistry:
    """Return reporter instances by name."""

    _default_file = Path(os.environ.get("PYRQG_REPORT_PATH", "pyrqg_report.json"))

    _factory = {
        "console": lambda: ConsoleReporter(name="console"),
        "errors": lambda: ErrorReporter(name="errors"),
        "json": lambda: JsonReporter(name="json"),
        "file": lambda: FileReporter(name="file", path=ReporterRegistry._default_file),
    }

    @classmethod
    def list(cls) -> List[str]:
        return sorted(cls._factory.keys())

    @classmethod
    def create(cls, name: str) -> Reporter:
        if name not in cls._factory:
            raise ValueError(f"Unknown reporter: {name}")
        return cls._factory[name]()
