"""
Query watchdog to report long-running statements.

API:
- QueryWatchdog(threshold_s=300, interval_s=5, reporter: Optional[Callable[[str,int],None]]=None)
- start(), stop()
- register(future, sql: str)
- unregister(future)
- snapshot() -> List[dict]

The reporter callback receives (formatted_sql, elapsed_seconds).
"""
from __future__ import annotations

import re
import threading
import time
from typing import Callable, Dict, List, Optional


def format_sql_multiline(sql: str) -> str:
    # Lightweight formatting: insert newlines before common clauses
    s = sql.strip()
    # Normalize whitespace
    s = re.sub(r"\s+", " ", s)
    # Insert newlines before keywords
    keywords = [
        "WITH", "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT",
        "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE FROM", "JOIN", "LEFT JOIN",
        "RIGHT JOIN", "FULL JOIN", "INNER JOIN", "ON", "RETURNING", "MERGE INTO", "USING",
        "WHEN MATCHED", "WHEN NOT MATCHED", "CREATE", "ALTER", "DROP", "CALL",
    ]
    # Sort keywords by length descending to avoid partial matches clobbering longer ones
    keywords.sort(key=len, reverse=True)
    for kw in keywords:
        s = re.sub(rf"\s({re.escape(kw)})\s", r"\n\1 ", s, flags=re.IGNORECASE)
    # Ensure semicolons end lines
    s = s.replace(";", ";\n")
    return s.strip()


class QueryWatchdog:
    def __init__(
        self,
        threshold_s: float = 300.0,
        interval_s: float = 5.0,
        reporter: Optional[Callable[[str, int], None]] = None,
    ) -> None:
        self.threshold_s = float(threshold_s)
        self.interval_s = max(0.5, float(interval_s))
        self.reporter = reporter or self._default_reporter
        self._outstanding: Dict[object, Dict[str, object]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="pyrqg-watchdog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            try:
                self._thread.join(timeout=1.0)
            except Exception:
                pass

    def register(self, future: object, sql: str) -> None:
        with self._lock:
            self._outstanding[future] = {
                "start": time.time(),
                "sql": sql,
                "reported": False,
            }

    def unregister(self, future: object) -> None:
        with self._lock:
            self._outstanding.pop(future, None)

    def snapshot(self) -> List[Dict[str, object]]:
        with self._lock:
            now = time.time()
            return [
                {
                    "elapsed": now - info["start"],
                    "sql": info["sql"],
                    "reported": info["reported"],
                }
                for info in self._outstanding.values()
            ]

    def _run(self) -> None:
        while not self._stop.is_set():
            now = time.time()
            with self._lock:
                for fut, info in list(self._outstanding.items()):
                    # Future protocol: must provide .done()
                    try:
                        done = getattr(fut, "done")()
                    except Exception:
                        done = True
                    if done:
                        continue
                    elapsed = now - float(info["start"])  # type: ignore[arg-type]
                    if (not info.get("reported")) and elapsed >= self.threshold_s:
                        formatted = format_sql_multiline(str(info["sql"]))
                        try:
                            self.reporter(formatted, int(elapsed))
                        except Exception:
                            # ignore reporter errors
                            pass
                        info["reported"] = True
            self._stop.wait(self.interval_s)

    @staticmethod
    def _default_reporter(formatted_sql: str, elapsed_s: int) -> None:
        print(
            f"[PyRQG][Watchdog] Query running > {elapsed_s}s:\n{formatted_sql}\n",
            flush=True,
        )

