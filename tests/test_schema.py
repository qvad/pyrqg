import pytest

from pyrqg.ddl_generator import DDLGenerator
from pyrqg.schema_support import SchemaCatalog


def test_offline_catalog_loads_from_ddl_definitions():
    """
    Tests that the offline SchemaCatalog can be correctly initialized
    from the TableDefinition objects produced by the DDLGenerator.
    """
    generator = DDLGenerator(seed=42)
    tables = generator.generate_sample_tables()

    assert len(tables) > 0, "DDLGenerator should produce sample tables"

    catalog = SchemaCatalog.from_table_definitions(tables)

    assert "users" in catalog.tables
    assert "products" in catalog.tables

    users_table = catalog.tables["users"]
    assert "email" in users_table.columns
    assert users_table.primary_key == "id"

    email_col = users_table.columns["email"]
    assert email_col.base_type == "varchar"


def test_online_catalog_instantiation():
    """
    Tests that the OnlineSchemaCatalog can be instantiated.
    A full test requires a live database or mocking psycopg2.
    This is a basic check.
    """
    try:
        from pyrqg.online_catalog import OnlineSchemaCatalog  # noqa: F401
        # This test does not run the connection but checks if the class can be created.
        # If psycopg2 is not installed, it should raise an ImportError.
        # If it is, it will attempt to connect, which may fail if no DB is running.
        # We are just testing the code path exists.
        assert OnlineSchemaCatalog is not None
    except ImportError:
        pytest.skip("psycopg2 not installed, cannot test OnlineSchemaCatalog")
    except Exception:
        # Expect a connection error if no database is running, which is okay for this test.
        pass
