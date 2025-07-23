# PyRQG Code Structure & Architecture

This document provides a deep dive into the internal architecture of PyRQG, detailing the responsibilities of each module and class. It is intended for developers who want to contribute to the core engine or understand the system's design.

## 1. High-Level Architecture

PyRQG follows a layered architecture:

1.  **Interface Layer (`pyrqg.runner`)**: Handles CLI arguments, output formatting, and user interaction.
2.  **API Layer (`pyrqg.api`)**: The primary facade for the library. It orchestrates the loading of grammars, management of schema metadata, and initialization of generators.
3.  **Execution Layer (`pyrqg.core.execution`)**: A robust, multi-threaded engine for executing generated SQL against a target database.
4.  **DSL Layer (`pyrqg.dsl`)**: The core domain logic. It defines the grammar rules, the evaluation engine (`Context`, `Element`), and the primitives for text generation.

## 2. Directory & Module Breakdown

### 2.1. `pyrqg/core/` - System Internals & Models

This directory contains the foundational logic and data structures.

*   **`schema.py`**:
    *   `Table`, `Column`, `TableConstraint`, `Index`: Unified data classes modeling the database structure.
    *   Used by both introspection (reading DB) and generation (creating DDL).
*   **`introspection.py`**:
    *   `SchemaProvider`: Handles connecting to the database (via `psycopg2`) and populating `Table` objects.
*   **`valgen.py`**:
    *   `ValueGenerator`: Generates random SQL literal values (e.g., `'active'`, `123`, `CURRENT_TIMESTAMP`) based on column types.
*   **`types.py`**:
    *   Centralized sets of PostgreSQL type names (`NUMERIC_TYPES`, `STRING_TYPES`, etc.) and helper functions (`is_numeric`, `is_string`) to avoid logic duplication.
*   **`execution.py`**:
    *   `WorkloadExecutor`: The main driver for running fuzz tests.
        *   **Concurrency**: Uses `ThreadPoolExecutor` to issue queries in parallel.
        *   **DDL Barriers**: Implements a synchronization mechanism. When a DDL statement is detected, it pauses all worker threads to execute the DDL safely on the main thread.
        *   **Backpressure**: Manages the queue size to prevent memory exhaustion.

### 2.2. `pyrqg/dsl/` - The Language Engine

This directory contains the building blocks for defining query grammars.

*   **`core.py`**:
    *   `Grammar`: The container for a set of rules. It acts as the entry point for generation (`generate()`).
    *   `Rule`: A named mapping to an `Element`.
    *   `Context`: The execution state. It now delegates duties via composition:
        *   `rng`: A seeded `random.Random` instance.
        *   `state`: A mutable `dict` for sharing data between rules.
        *   `tables`: A `dict` of `Table` objects.
        *   **Decoupled**: Uses `SchemaProvider` for introspection and `ValueGenerator` for data generation, keeping the Context clean.
    *   **Elements** (`Template`, `Choice`, `Lambda`, `Repeat`, `Maybe`).
*   **`utils.py`**:
    *   Shared Python helper functions for use in Grammars (e.g., `pick_table`, `random_id`).
*   **`primitives.py`**:
    *   Shared DSL elements (e.g., `common_table_names`).

### 2.3. `pyrqg/` - API & Entry Points

*   **`api.py`**:
    *   `RQG`: The main facade.
    *   Responsible for loading grammars via `GrammarLoader`.
    *   Provides convenience methods like `generate_ddl` (delegating to `DDLGenerator`) and `run_mixed_workload`.
    *   **Note**: The legacy `QueryGenerator` class has been removed in favor of the `basic_crud` grammar.
*   **`ddl_generator.py`**:
    *   `DDLGenerator`: Logic for generating complex `CREATE TABLE` statements. It uses the unified `Table` model from `core.schema`.
*   **`runner.py`**:
    *   The CLI entry point. Uses `WorkloadExecutor` for running tests.

## 3. Key Implementation Details

### 3.1. Context & Determinism
The `Context` object is passed to every `Element.generate()` call. Crucially, it initializes its own random number generator based on the provided `--seed`. This ensures that **every run is deterministic**.

### 3.2. Introspection
When `pyrqg` starts with a DSN, `Context` uses `SchemaProvider` to connect to the database and populate `Context.tables`. This allows grammars to reference *actual* tables and columns.

### 3.3. DDL Handling
The executor differentiates between DDL and DML. DDL statements pause the thread pool to prevent catalog locking issues, ensuring safe schema modifications during fuzzing.

## 4. Extending the System

*   **New Grammars**: Add a Python file to `grammars/` exporting a `grammar` object. Use `pyrqg.dsl.utils` for common tasks.
*   **New Types**: Update `pyrqg/core/types.py` if adding support for new database data types.