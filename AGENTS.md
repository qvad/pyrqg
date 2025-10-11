# Repository Guidelines

## Project Structure & Module Organization
- Source: `pyrqg/` (CLI `runner.py`, DSL in `dsl/`, production in `production/`, filters in `filters/`).
- Tests: `tests/` with pytest config (`tests/pytest.ini`) and docs (`tests/README.md`).
- Grammars: `grammars/` (not a Python package by default; loaded from repo when running from source).
- Docs and examples: `docs/`, `examples/`.
- Configs and scenarios: `configs/`, `production_scenarios/`.

## Build, Test, and Development Commands
- Create venv and install deps: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
- Editable install: `pip install -e .` (exposes `pyrqg` console script).
- Build wheel/sdist: `python -m build` (artifacts in `dist/`).
- Run locally:
  - List grammars: `python -m pyrqg.runner list`
  - Generate: `python -m pyrqg.runner grammar --grammar dml_yugabyte --count 100`
  - End-to-end exec: `python -m pyrqg.runner exec --dsn postgresql://...`
- Tests: `pytest` or `pytest -m "not slow and not integration"`.
- Coverage (optional): `pytest --cov=pyrqg --cov-report=html`.

## Coding Style & Naming Conventions
- Python 3.8+; follow PEP 8 with 4-space indentation and type hints (module uses `typing` widely).
- Names: modules and functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Keep public APIs documented with short docstrings; prefer small, composable functions.
- File layout: new modules under `pyrqg/<area>/` (e.g., `pyrqg/filters/`), tests in `tests/test_<module>.py`.

## Testing Guidelines
- Framework: `pytest` with markers `unit`, `integration`, `slow`, `yugabyte` (see `tests/pytest.ini`).
- Conventions: test files `test_*.py`; functions `test_*`; classes start with `Test`.
- Coverage goals (guidance): core DSL ≥90%, production ≥85% (see `tests/README.md`).
- Integration tests may require `psycopg2-binary` and a running Postgres/YugabyteDSN; skip or mark appropriately.

## Commit & Pull Request Guidelines
- Commits: prefer Conventional Commits style, e.g., `feat: add schema-aware insert generator`, `fix(filters): handle NULL comparisons`.
- PRs: include a clear summary, linked issues, runnable examples (command lines), and tests for new behavior. Update docs in `docs/` when user-facing.
- CI-friendly: ensure `pytest` passes locally and avoid introducing mandatory external services for unit tests.

## Security & Configuration Tips
- Do not commit secrets or real DSNs. Use local env vars or examples in `configs/`.
- For local DBs, prefer Docker commands in `README.md`. Clean up with `docker rm -f <name>`.

