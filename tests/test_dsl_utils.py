"""Tests for DSL utility functions."""

import pytest
import random
from dataclasses import dataclass, field
from typing import Dict, Any
from pyrqg.core.schema import Table, Column
from pyrqg.dsl.utils import (
    random_id, pick_table, pick_table_and_store, pick_column, get_columns,
    random_int, random_bigint, random_boolean, random_text, random_numeric,
    random_float, random_date, random_timestamp, random_inet, random_bit,
    random_money, random_bytea, random_range, generate_constant,
    get_depth, inc_depth, dec_depth, VALUE_GENERATORS
)


@dataclass
class MockContext:
    """Mock context for testing."""
    rng: random.Random
    tables: Dict[str, Table] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)


@pytest.fixture
def ctx():
    """Create a mock context with tables."""
    rng = random.Random(42)
    tables = {
        'users': Table(
            name='users',
            columns={
                'id': Column(name='id', data_type='integer', is_nullable=False, is_primary_key=True),
                'name': Column(name='name', data_type='varchar', is_nullable=True),
                'email': Column(name='email', data_type='text', is_nullable=True),
                'age': Column(name='age', data_type='integer', is_nullable=True),
                'active': Column(name='active', data_type='boolean', is_nullable=True),
            },
            primary_key='id',
            unique_columns=['email'],
            row_count=100
        ),
        'orders': Table(
            name='orders',
            columns={
                'order_id': Column(name='order_id', data_type='bigint', is_nullable=False, is_primary_key=True),
                'user_id': Column(name='user_id', data_type='integer', is_nullable=True),
                'total': Column(name='total', data_type='numeric', is_nullable=True),
            },
            primary_key='order_id',
            unique_columns=[],
            row_count=50
        ),
    }
    return MockContext(rng=rng, tables=tables, state={})


@pytest.fixture
def empty_ctx():
    """Create a mock context without tables."""
    return MockContext(rng=random.Random(42), tables={}, state={})


class TestRandomId:
    """Test random_id function."""

    def test_returns_8_chars(self):
        result = random_id()
        assert len(result) == 8

    def test_no_hyphens(self):
        result = random_id()
        assert '-' not in result

    def test_unique_ids(self):
        ids = [random_id() for _ in range(100)]
        assert len(set(ids)) == 100  # All unique


class TestPickTable:
    """Test pick_table function."""

    def test_picks_from_available_tables(self, ctx):
        result = pick_table(ctx)
        assert result in ['users', 'orders']

    def test_returns_none_when_no_tables(self, empty_ctx):
        result = pick_table(empty_ctx)
        assert result is None

    def test_deterministic_with_seed(self, ctx):
        ctx2 = MockContext(rng=random.Random(42), tables=ctx.tables, state={})
        assert pick_table(ctx) == pick_table(ctx2)


class TestPickTableAndStore:
    """Test pick_table_and_store function."""

    def test_picks_and_stores_table(self, ctx):
        result = pick_table_and_store(ctx)
        assert result in ['users', 'orders']
        assert ctx.state['table'] == result
        assert result in ctx.state['available_tables']

    def test_returns_fallback_when_no_tables(self, empty_ctx):
        result = pick_table_and_store(empty_ctx, fallback='default')
        assert result == 'default'
        assert 'table' not in empty_ctx.state

    def test_appends_to_available_tables(self, ctx):
        pick_table_and_store(ctx)
        pick_table_and_store(ctx)
        assert len(ctx.state['available_tables']) == 2


class TestPickColumn:
    """Test pick_column function."""

    def test_picks_column_from_stored_table(self, ctx):
        ctx.state['table'] = 'users'
        result = pick_column(ctx)
        assert result in ['id', 'name', 'email', 'age', 'active']

    def test_filters_by_primary_key_true(self, ctx):
        ctx.state['table'] = 'users'
        result = pick_column(ctx, is_pk=True)
        assert result == 'id'

    def test_filters_by_primary_key_false(self, ctx):
        ctx.state['table'] = 'users'
        result = pick_column(ctx, is_pk=False)
        assert result in ['name', 'email', 'age', 'active']

    def test_filters_by_data_type(self, ctx):
        ctx.state['table'] = 'users'
        result = pick_column(ctx, data_type='INT')
        assert result in ['id', 'age']

    def test_filters_by_text_type(self, ctx):
        ctx.state['table'] = 'users'
        result = pick_column(ctx, data_type='TEXT')
        assert result in ['name', 'email']

    def test_filters_by_boolean_type(self, ctx):
        ctx.state['table'] = 'users'
        result = pick_column(ctx, data_type='BOOLEAN')
        assert result == 'active'

    def test_returns_fallback_when_no_table(self, ctx):
        result = pick_column(ctx, fallback='default_col')
        assert result == 'default_col'

    def test_returns_fallback_for_unknown_table(self, ctx):
        ctx.state['table'] = 'nonexistent'
        result = pick_column(ctx, fallback='fallback')
        assert result == 'fallback'


class TestGetColumns:
    """Test get_columns function."""

    def test_returns_column_names(self, ctx):
        result = get_columns(ctx, 'users')
        assert set(result) == {'id', 'name', 'email', 'age', 'active'}

    def test_returns_empty_for_unknown_table(self, ctx):
        result = get_columns(ctx, 'nonexistent')
        assert result == []

    def test_returns_empty_for_none(self, ctx):
        result = get_columns(ctx, None)
        assert result == []


class TestRandomInt:
    """Test random_int function."""

    def test_returns_int_string(self, ctx):
        result = random_int(ctx)
        int(result)  # Should not raise

    def test_respects_min_max(self, ctx):
        result = random_int(ctx, min_val=10, max_val=20)
        assert 10 <= int(result) <= 20


class TestRandomBigint:
    """Test random_bigint function."""

    def test_returns_large_int_string(self, ctx):
        result = random_bigint(ctx)
        val = int(result)
        # Should be within scaled bigint range
        assert abs(val) <= 9223372036854775807 // 1000000


class TestRandomBoolean:
    """Test random_boolean function."""

    def test_returns_true_or_false(self, ctx):
        result = random_boolean(ctx)
        assert result in ['TRUE', 'FALSE']


class TestRandomText:
    """Test random_text function."""

    def test_returns_quoted_string(self, ctx):
        result = random_text(ctx)
        assert result.startswith("'")
        assert result.endswith("'")

    def test_respects_max_length(self, ctx):
        result = random_text(ctx, max_length=5)
        # Remove quotes and check length
        inner = result[1:-1]
        assert len(inner) <= 5

    def test_escapes_quotes(self):
        # Create a context that will generate a string with quotes
        rng = random.Random()
        ctx = MockContext(rng=rng, tables={}, state={})
        # Run many times to check escaping works
        for _ in range(100):
            result = random_text(ctx)
            # Check that any single quotes are properly escaped
            inner = result[1:-1]
            assert "''" in inner or "'" not in inner


class TestRandomNumeric:
    """Test random_numeric function."""

    def test_returns_decimal_string(self, ctx):
        result = random_numeric(ctx)
        float(result)  # Should not raise
        assert '.' in result


class TestRandomFloat:
    """Test random_float function."""

    def test_returns_float_string(self, ctx):
        result = random_float(ctx)
        float(result)  # Should not raise


class TestRandomDate:
    """Test random_date function."""

    def test_returns_date_string(self, ctx):
        result = random_date(ctx)
        assert result.startswith("'")
        assert result.endswith("'")
        # Should be YYYY-MM-DD format
        inner = result[1:-1]
        parts = inner.split('-')
        assert len(parts) == 3
        assert 1970 <= int(parts[0]) <= 2030


class TestRandomTimestamp:
    """Test random_timestamp function."""

    def test_returns_timestamp_string(self, ctx):
        result = random_timestamp(ctx)
        assert result.startswith("'")
        assert result.endswith("'")
        inner = result[1:-1]
        assert ' ' in inner  # Date and time separated by space


class TestRandomInet:
    """Test random_inet function."""

    def test_returns_inet_string(self, ctx):
        result = random_inet(ctx)
        assert result.endswith("::inet")
        assert result.startswith("'")


class TestRandomBit:
    """Test random_bit function."""

    def test_returns_bit_string(self, ctx):
        result = random_bit(ctx)
        assert result.startswith("B'")
        assert result.endswith("'")
        inner = result[2:-1]
        assert all(c in '01' for c in inner)


class TestRandomMoney:
    """Test random_money function."""

    def test_returns_money_string(self, ctx):
        result = random_money(ctx)
        assert result.endswith("::money")
        assert result.startswith("'")


class TestRandomBytea:
    """Test random_bytea function."""

    def test_returns_bytea_string(self, ctx):
        result = random_bytea(ctx)
        assert result.endswith("::bytea")
        assert "'\\x" in result


class TestRandomRange:
    """Test random_range function."""

    def test_returns_range_string(self, ctx):
        result = random_range(ctx)
        assert result.endswith("::int4range")
        # Should have proper range format
        assert result.startswith("'[") or result.startswith("'(")


class TestGenerateConstant:
    """Test generate_constant function."""

    def test_generates_null_sometimes(self, ctx):
        nulls = sum(1 for _ in range(1000) if generate_constant(ctx, null_probability=0.5) == 'NULL')
        assert 400 < nulls < 600  # Should be around 50%

    def test_respects_data_type(self, ctx):
        result = generate_constant(ctx, data_type='INT', null_probability=0)
        int(result)  # Should not raise

    def test_random_type_when_none(self, ctx):
        result = generate_constant(ctx, data_type=None, null_probability=0)
        # Should return some valid constant
        assert result is not None

    def test_uses_value_generators_mapping(self, ctx):
        for dtype in ['INT', 'TEXT', 'BOOLEAN', 'DATE', 'INET']:
            result = generate_constant(ctx, data_type=dtype, null_probability=0)
            assert result != 'NULL'


class TestValueGeneratorsMapping:
    """Test VALUE_GENERATORS dictionary."""

    def test_contains_common_types(self):
        assert 'INT' in VALUE_GENERATORS
        assert 'INTEGER' in VALUE_GENERATORS
        assert 'TEXT' in VALUE_GENERATORS
        assert 'BOOLEAN' in VALUE_GENERATORS
        assert 'DATE' in VALUE_GENERATORS
        assert 'TIMESTAMP' in VALUE_GENERATORS

    def test_generators_are_callable(self, ctx):
        for dtype, generator in VALUE_GENERATORS.items():
            result = generator(ctx)
            assert result is not None


class TestDepthControl:
    """Test expression depth control functions."""

    def test_get_depth_default(self, ctx):
        assert get_depth(ctx) == 0

    def test_get_depth_after_set(self, ctx):
        ctx.state['depth'] = 5
        assert get_depth(ctx) == 5

    def test_inc_depth(self, ctx):
        assert inc_depth(ctx) == 1
        assert inc_depth(ctx) == 2
        assert ctx.state['depth'] == 2

    def test_dec_depth(self, ctx):
        ctx.state['depth'] = 3
        dec_depth(ctx)
        assert ctx.state['depth'] == 2

    def test_dec_depth_does_not_go_negative(self, ctx):
        ctx.state['depth'] = 0
        dec_depth(ctx)
        assert ctx.state['depth'] == 0

    def test_depth_cycle(self, ctx):
        inc_depth(ctx)
        inc_depth(ctx)
        inc_depth(ctx)
        assert get_depth(ctx) == 3
        dec_depth(ctx)
        dec_depth(ctx)
        assert get_depth(ctx) == 1
