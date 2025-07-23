"""
Database Introspection Logic.
"""
import logging
from typing import List, Tuple, Dict
from pyrqg.core.schema import Table, Column

try:
    import psycopg2
except ImportError:
    psycopg2 = None

logger = logging.getLogger(__name__)

class SchemaProvider:
    """Handles database introspection to populate schema metadata."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def introspect(self) -> Dict[str, Table]:
        """Connect to DB and return a dict of Table objects."""
        if not psycopg2:
            logger.warning("psycopg2 not installed, cannot introspect %s", self.dsn)
            return {}

        tables = {}
        conn = None
        try:
            conn = psycopg2.connect(self.dsn)
            cur = conn.cursor()
            # Set search path to ensure we see relevant tables
            cur.execute("SET search_path TO pyrqg, public")

            tables_info = self._fetch_tables_info(cur)

            for table_name, row_count, table_schema in tables_info:
                columns_data = self._fetch_columns_info(cur, table_schema, table_name)
                tables[table_name] = self._build_table_metadata(
                    table_name, row_count, columns_data
                )
        except psycopg2.OperationalError as e:
            logger.warning("Database connection failed: %s", e)
        except psycopg2.DatabaseError as e:
            logger.warning("Database query failed: %s", e)
        except Exception as e:
            logger.warning("Schema introspection failed: %s", e)
        finally:
            if conn:
                conn.close()
        
        return tables

    def _fetch_tables_info(self, cur) -> List[Tuple[str, int, str]]:
        """Fetch table names and row counts from database."""
        cur.execute("""
            SELECT t.table_name, c.reltuples::bigint, t.table_schema
            FROM information_schema.tables t
            JOIN pg_class c ON c.relname = t.table_name
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE t.table_schema IN ('pyrqg', 'public')
            AND t.table_type = 'BASE TABLE'
            AND t.table_name NOT LIKE '%pkey%'
        """)
        return cur.fetchall()

    def _fetch_columns_info(self, cur, table_schema: str, table_name: str) -> List[Tuple]:
        """Fetch column metadata for a specific table."""
        cur.execute("""
            SELECT
                c.column_name, c.data_type, c.is_nullable, c.column_default IS NOT NULL,
                (pk.column_name IS NOT NULL) as is_pk, (uc.column_name IS NOT NULL) as is_uniq
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.column_name = pk.column_name
            LEFT JOIN (
                SELECT kcu.column_name FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'UNIQUE'
            ) uc ON c.column_name = uc.column_name
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (table_schema, table_name, table_schema, table_name, table_schema, table_name))
        return cur.fetchall()

    def _build_table_metadata(self, table_name: str, row_count: int,
                              columns_data: List[Tuple]) -> Table:
        """Construct Table from raw column data."""
        cols = {}
        pk = None
        unique_cols = []

        for row in columns_data:
            c_name, c_type, c_null, c_def, c_pk, c_uniq = row
            col = Column(
                name=c_name,
                data_type=c_type,
                is_nullable=(c_null == 'YES'),
                has_default=c_def,
                is_primary_key=c_pk,
                is_unique=c_uniq
            )
            cols[c_name] = col
            if c_pk:
                pk = c_name
            if c_uniq:
                unique_cols.append(c_name)

        return Table(
            name=table_name,
            columns=cols,
            primary_key=pk,
            unique_columns=unique_cols,
            row_count=row_count if row_count and row_count >= 0 else 0
        )
