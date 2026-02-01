"""Tests for the database introspection module."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyrqg.core.introspection import SchemaProvider
from pyrqg.core.schema import Table, Column


class TestSchemaProvider:
    """Test the SchemaProvider class."""

    def test_init_stores_dsn(self):
        provider = SchemaProvider("postgresql://localhost/test")
        assert provider.dsn == "postgresql://localhost/test"

    @patch('pyrqg.core.introspection.psycopg2', None)
    def test_introspect_returns_empty_when_no_psycopg2(self):
        provider = SchemaProvider("postgresql://localhost/test")
        result = provider.introspect()
        assert result == {}

    @patch('pyrqg.core.introspection.psycopg2')
    def test_introspect_success(self, mock_psycopg2):
        import psycopg2 as real_psycopg2
        mock_psycopg2.OperationalError = real_psycopg2.OperationalError
        mock_psycopg2.DatabaseError = real_psycopg2.DatabaseError

        # Setup mock connection and cursor
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn

        # Mock table info
        mock_cursor.fetchall.side_effect = [
            # First call: _fetch_tables_info
            [('users', 100, 'public')],
            # Second call: _fetch_columns_info
            [
                ('id', 'integer', 'NO', True, True, False),
                ('name', 'varchar', 'YES', False, False, False),
                ('email', 'text', 'YES', False, False, True),
            ]
        ]

        provider = SchemaProvider("postgresql://localhost/test")
        result = provider.introspect()

        assert 'users' in result
        assert isinstance(result['users'], Table)
        assert result['users'].name == 'users'
        assert result['users'].row_count == 100
        assert 'id' in result['users'].columns
        assert result['users'].columns['id'].is_primary_key is True
        assert result['users'].columns['email'].is_unique is True

    @patch('pyrqg.core.introspection.psycopg2')
    def test_introspect_handles_operational_error(self, mock_psycopg2):
        import psycopg2
        mock_psycopg2.OperationalError = psycopg2.OperationalError
        mock_psycopg2.connect.side_effect = psycopg2.OperationalError("Connection failed")

        provider = SchemaProvider("postgresql://localhost/test")
        result = provider.introspect()

        assert result == {}

    @patch('pyrqg.core.introspection.psycopg2')
    def test_introspect_handles_database_error(self, mock_psycopg2):
        import psycopg2
        mock_psycopg2.DatabaseError = psycopg2.DatabaseError
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value = mock_conn
        mock_cursor.execute.side_effect = psycopg2.DatabaseError("Query failed")

        provider = SchemaProvider("postgresql://localhost/test")
        result = provider.introspect()

        assert result == {}

    @patch('pyrqg.core.introspection.psycopg2')
    def test_introspect_handles_generic_exception(self, mock_psycopg2):
        import psycopg2 as real_psycopg2
        mock_psycopg2.OperationalError = real_psycopg2.OperationalError
        mock_psycopg2.DatabaseError = real_psycopg2.DatabaseError

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        mock_cursor.execute.side_effect = Exception("Unexpected error")

        provider = SchemaProvider("postgresql://localhost/test")
        result = provider.introspect()

        assert result == {}

    @patch('pyrqg.core.introspection.psycopg2')
    def test_introspect_closes_connection(self, mock_psycopg2):
        import psycopg2 as real_psycopg2
        mock_psycopg2.OperationalError = real_psycopg2.OperationalError
        mock_psycopg2.DatabaseError = real_psycopg2.DatabaseError

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        mock_cursor.fetchall.return_value = []

        provider = SchemaProvider("postgresql://localhost/test")
        provider.introspect()

        mock_conn.close.assert_called_once()

    def test_fetch_tables_info(self):
        # Test the SQL query structure
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ('users', 100, 'public'),
            ('orders', 50, 'public'),
        ]

        provider = SchemaProvider("postgresql://localhost/test")
        result = provider._fetch_tables_info(mock_cursor)

        assert len(result) == 2
        assert result[0] == ('users', 100, 'public')
        mock_cursor.execute.assert_called_once()

    def test_fetch_columns_info(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ('id', 'integer', 'NO', True, True, False),
            ('name', 'varchar', 'YES', False, False, False),
        ]

        provider = SchemaProvider("postgresql://localhost/test")
        result = provider._fetch_columns_info(mock_cursor, 'public', 'users')

        assert len(result) == 2
        mock_cursor.execute.assert_called_once()
        # Verify parameters were passed
        call_args = mock_cursor.execute.call_args
        assert call_args[0][1] == ('public', 'users', 'public', 'users', 'public', 'users')

    def test_build_table_metadata(self):
        provider = SchemaProvider("postgresql://localhost/test")
        columns_data = [
            ('id', 'integer', 'NO', True, True, False),
            ('name', 'varchar', 'YES', False, False, False),
            ('email', 'text', 'YES', False, False, True),
        ]

        result = provider._build_table_metadata('users', 100, columns_data)

        assert isinstance(result, Table)
        assert result.name == 'users'
        assert result.row_count == 100
        assert result.primary_key == 'id'
        assert 'email' in result.unique_columns
        assert len(result.columns) == 3

    def test_build_table_metadata_no_pk(self):
        provider = SchemaProvider("postgresql://localhost/test")
        columns_data = [
            ('col1', 'text', 'YES', False, False, False),
            ('col2', 'integer', 'YES', False, False, False),
        ]

        result = provider._build_table_metadata('test_table', 10, columns_data)

        assert result.primary_key is None
        assert result.unique_columns == []

    def test_build_table_metadata_negative_row_count(self):
        provider = SchemaProvider("postgresql://localhost/test")
        columns_data = [
            ('id', 'integer', 'NO', True, True, False),
        ]

        result = provider._build_table_metadata('users', -1, columns_data)
        assert result.row_count == 0

    def test_build_table_metadata_none_row_count(self):
        provider = SchemaProvider("postgresql://localhost/test")
        columns_data = [
            ('id', 'integer', 'NO', True, True, False),
        ]

        result = provider._build_table_metadata('users', None, columns_data)
        assert result.row_count == 0

    @patch('pyrqg.core.introspection.psycopg2')
    def test_introspect_multiple_tables(self, mock_psycopg2):
        import psycopg2 as real_psycopg2
        mock_psycopg2.OperationalError = real_psycopg2.OperationalError
        mock_psycopg2.DatabaseError = real_psycopg2.DatabaseError

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn

        mock_cursor.fetchall.side_effect = [
            # Tables
            [('users', 100, 'public'), ('orders', 50, 'public')],
            # Users columns
            [('id', 'integer', 'NO', True, True, False)],
            # Orders columns
            [('order_id', 'bigint', 'NO', True, True, False)],
        ]

        provider = SchemaProvider("postgresql://localhost/test")
        result = provider.introspect()

        assert len(result) == 2
        assert 'users' in result
        assert 'orders' in result
