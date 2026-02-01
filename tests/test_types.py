"""Tests for the type classification module."""

import pytest
from pyrqg.core.types import (
    NUMERIC_TYPES, STRING_TYPES, DATETIME_TYPES, BOOLEAN_TYPES,
    JSON_TYPES, NET_TYPES, GEO_TYPES, TYPE_CATEGORIES,
    is_numeric, is_string, is_datetime, is_boolean, is_json,
    is_net, is_geo, matches_type_category
)


class TestTypeConstants:
    """Test the type set constants."""

    def test_numeric_types_contains_common_types(self):
        assert 'integer' in NUMERIC_TYPES
        assert 'bigint' in NUMERIC_TYPES
        assert 'decimal' in NUMERIC_TYPES
        assert 'real' in NUMERIC_TYPES

    def test_string_types_contains_common_types(self):
        assert 'text' in STRING_TYPES
        assert 'varchar' in STRING_TYPES
        assert 'char' in STRING_TYPES

    def test_datetime_types_contains_common_types(self):
        assert 'timestamp' in DATETIME_TYPES
        assert 'date' in DATETIME_TYPES
        assert 'time' in DATETIME_TYPES
        assert 'interval' in DATETIME_TYPES

    def test_boolean_types(self):
        assert 'boolean' in BOOLEAN_TYPES
        assert 'bool' in BOOLEAN_TYPES

    def test_json_types(self):
        assert 'json' in JSON_TYPES
        assert 'jsonb' in JSON_TYPES

    def test_net_types(self):
        assert 'inet' in NET_TYPES
        assert 'cidr' in NET_TYPES
        assert 'macaddr' in NET_TYPES

    def test_geo_types(self):
        assert 'point' in GEO_TYPES
        assert 'polygon' in GEO_TYPES
        assert 'circle' in GEO_TYPES


class TestIsNumeric:
    """Test is_numeric function."""

    def test_integer_types(self):
        assert is_numeric('integer')
        assert is_numeric('int')
        assert is_numeric('smallint')
        assert is_numeric('bigint')

    def test_serial_types(self):
        assert is_numeric('serial')
        assert is_numeric('bigserial')

    def test_decimal_types(self):
        assert is_numeric('decimal')
        assert is_numeric('numeric')
        assert is_numeric('NUMERIC(10,2)')
        assert is_numeric('decimal(5)')

    def test_float_types(self):
        assert is_numeric('real')
        assert is_numeric('double precision')
        assert is_numeric('float')
        assert is_numeric('money')

    def test_non_numeric_types(self):
        assert not is_numeric('text')
        assert not is_numeric('varchar')
        assert not is_numeric('boolean')
        assert not is_numeric('date')

    def test_case_insensitive(self):
        assert is_numeric('INTEGER')
        assert is_numeric('BIGINT')
        assert is_numeric('Decimal')


class TestIsString:
    """Test is_string function."""

    def test_string_types(self):
        assert is_string('text')
        assert is_string('varchar')
        assert is_string('character varying')
        assert is_string('char')
        assert is_string('character')

    def test_parameterized_types(self):
        assert is_string('varchar(50)')
        assert is_string('char(10)')
        assert is_string('character varying(255)')

    def test_non_string_types(self):
        assert not is_string('integer')
        assert not is_string('boolean')
        assert not is_string('jsonb')


class TestIsDatetime:
    """Test is_datetime function."""

    def test_timestamp_types(self):
        assert is_datetime('timestamp')
        assert is_datetime('timestamp without time zone')
        assert is_datetime('timestamptz')
        assert is_datetime('timestamp with time zone')

    def test_date_and_time_types(self):
        assert is_datetime('date')
        assert is_datetime('time')
        assert is_datetime('time without time zone')
        assert is_datetime('timetz')
        assert is_datetime('interval')

    def test_non_datetime_types(self):
        assert not is_datetime('text')
        assert not is_datetime('integer')


class TestIsBoolean:
    """Test is_boolean function."""

    def test_boolean_types(self):
        assert is_boolean('boolean')
        assert is_boolean('bool')
        assert is_boolean('BOOLEAN')

    def test_non_boolean_types(self):
        assert not is_boolean('text')
        assert not is_boolean('integer')


class TestIsJson:
    """Test is_json function."""

    def test_json_types(self):
        assert is_json('json')
        assert is_json('jsonb')
        assert is_json('JSON')
        assert is_json('JSONB')

    def test_non_json_types(self):
        assert not is_json('text')
        assert not is_json('integer')


class TestIsNet:
    """Test is_net function."""

    def test_net_types(self):
        assert is_net('inet')
        assert is_net('cidr')
        assert is_net('macaddr')
        assert is_net('macaddr8')

    def test_non_net_types(self):
        assert not is_net('text')
        assert not is_net('integer')


class TestIsGeo:
    """Test is_geo function."""

    def test_geo_types(self):
        assert is_geo('point')
        assert is_geo('line')
        assert is_geo('polygon')
        assert is_geo('circle')
        assert is_geo('box')
        assert is_geo('path')
        assert is_geo('lseg')

    def test_non_geo_types(self):
        assert not is_geo('text')
        assert not is_geo('integer')


class TestMatchesTypeCategory:
    """Test matches_type_category function."""

    def test_direct_match(self):
        assert matches_type_category('integer', 'integer')
        assert matches_type_category('text', 'text')

    def test_category_match_numeric(self):
        assert matches_type_category('integer', 'numeric')
        assert matches_type_category('bigint', 'numeric')
        assert matches_type_category('decimal', 'numeric')

    def test_category_match_int(self):
        assert matches_type_category('integer', 'int')
        assert matches_type_category('bigint', 'int')
        assert matches_type_category('smallint', 'int')

    def test_category_match_text(self):
        assert matches_type_category('varchar', 'text')
        assert matches_type_category('character varying', 'text')

    def test_parameterized_types(self):
        assert matches_type_category('varchar(50)', 'text')
        assert matches_type_category('numeric(10,2)', 'numeric')

    def test_type_in_same_category(self):
        # If target is in a category, col should match if in same category
        assert matches_type_category('bigint', 'integer')
        assert matches_type_category('smallint', 'integer')

    def test_helper_function_fallback(self):
        # Uses is_numeric, is_string, etc. as fallback
        assert matches_type_category('real', 'float')
        assert matches_type_category('double precision', 'decimal')

    def test_boolean_category(self):
        assert matches_type_category('boolean', 'bool')
        assert matches_type_category('bool', 'boolean')

    def test_temporal_category(self):
        assert matches_type_category('timestamp', 'temporal')
        assert matches_type_category('date', 'temporal')

    def test_json_category(self):
        assert matches_type_category('json', 'jsonb')
        assert matches_type_category('jsonb', 'json')

    def test_no_match(self):
        assert not matches_type_category('integer', 'text')
        assert not matches_type_category('text', 'integer')
        assert not matches_type_category('boolean', 'numeric')

    def test_unknown_types(self):
        assert not matches_type_category('custom_type', 'another_type')
