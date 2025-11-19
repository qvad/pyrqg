"""Online Schema Catalog

Provides a live, database-backed implementation of the SchemaCatalog API. It
loads schema information from a PostgreSQL-compatible database using
psycopg2, then initializes a regular offline SchemaCatalog with the same
TableInfo/ColumnInfo structures so the rest of the code can rely on a
consistent interface.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import os

try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover - optional dependency until used
    psycopg2 = None  # type: ignore

from .schema_support import SchemaCatalog, TableInfo, ColumnInfo


class OnlineSchemaCatalog(SchemaCatalog):
    """Live database-backed catalog with the same public API as SchemaCatalog.

    Parameters:
    - dsn: Optional DSN string; if not provided, resolves via env var
      PYRQG_DSN, then falls back to a sensible default.
    - schema: Target schema name to introspect; defaults to env PYRQG_SCHEMA
      or 'pyrqg'.
    """

    def __init__(self, dsn: Optional[str] = None, schema: Optional[str] = None):
        if psycopg2 is None:
            raise ImportError("psycopg2 not installed. Install with: pip install psycopg2-binary")
        dsn_effective = dsn or os.environ.get("PYRQG_DSN") or "postgresql://postgres:postgres@localhost:5432/postgres"
        schema_effective = schema or os.environ.get("PYRQG_SCHEMA", "pyrqg")

        tables = self._load_schema(dsn_effective, schema_effective)
        super().__init__(tables)

    @staticmethod
    def _load_schema(dsn: str, schema: str) -> Dict[str, TableInfo]:
        table_map: Dict[str, TableInfo] = {}
        with psycopg2.connect(dsn) as conn:  # type: ignore
            with conn.cursor() as cur:
                # Column details including types, nullability, defaults, and lengths
                cur.execute(
                    """
                    SELECT c.table_name,
                           c.column_name,
                           c.data_type,
                           c.is_nullable,
                           c.column_default,
                           c.character_maximum_length
                    FROM information_schema.columns c
                    WHERE c.table_schema = %s
                    ORDER BY c.table_name, c.ordinal_position
                    """,
                    (schema,),
                )
                # Build columns for each table
                col_rows = cur.fetchall()
                tmp_cols: Dict[str, Dict[str, ColumnInfo]] = {}
                for table_name, col_name, data_type, is_nullable, default, char_len in col_rows:
                    cols = tmp_cols.setdefault(table_name, {})
                    cols[col_name] = ColumnInfo(
                        name=col_name,
                        data_type=str(data_type),
                        nullable=(str(is_nullable).upper() == "YES"),
                        has_default=(default is not None),
                        unique=False,  # will be updated from constraints
                        length=int(char_len) if char_len is not None else None,
                    )

                # Primary keys and unique constraints
                cur.execute(
                    """
                    SELECT tc.table_name,
                           tc.constraint_type,
                           kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                         ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = %s
                      AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                    ORDER BY tc.table_name
                    """,
                    (schema,),
                )
                pk_map: Dict[str, List[str]] = {}
                unique_map: Dict[str, List[str]] = {}
                for tname, ctype, col in cur.fetchall():
                    if ctype == "PRIMARY KEY":
                        pk_map.setdefault(tname, []).append(col)
                    elif ctype == "UNIQUE":
                        unique_map.setdefault(tname, []).append(col)

                # Finalize TableInfo
                for tname, cols in tmp_cols.items():
                    # mark single-column unique columns as conflict targets
                    uniques = []
                    if tname in unique_map:
                        # retain only columns that are unique individually
                        # information_schema groups may contain multiple columns; we keep as-is
                        # but mark columns as unique in ColumnInfo if they appear alone multiple times
                        uniques = list(dict.fromkeys(unique_map[tname]))
                        for ucol in uniques:
                            if ucol in cols:
                                cols[ucol].unique = True

                    pk_cols = pk_map.get(tname, [])
                    pk_name = pk_cols[0] if pk_cols else None
                    table_map[tname] = TableInfo(
                        name=tname,
                        columns=cols,
                        primary_key=pk_name,
                        unique_columns=uniques,
                        primary_key_columns=pk_cols,
                    )

        return table_map
