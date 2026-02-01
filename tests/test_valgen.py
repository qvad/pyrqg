"""Tests for the value generation module."""

import pytest
import random
from pyrqg.core.valgen import ValueGenerator


class TestValueGenerator:
    """Test the ValueGenerator class."""

    @pytest.fixture
    def generator(self):
        """Create a ValueGenerator with a seeded RNG."""
        return ValueGenerator(random.Random(42))

    def test_generate_boolean(self, generator):
        result = generator.generate('boolean')
        assert result in ('true', 'false')

    def test_generate_bool(self, generator):
        result = generator.generate('bool')
        assert result in ('true', 'false')

    def test_generate_integer(self, generator):
        result = generator.generate('integer')
        assert result.isdigit() or (result.startswith('-') and result[1:].isdigit())
        assert 1 <= int(result) <= 1000

    def test_generate_int(self, generator):
        result = generator.generate('int')
        assert 1 <= int(result) <= 1000

    def test_generate_smallint(self, generator):
        result = generator.generate('smallint')
        # smallint falls through to the general integer branch (1-1000)
        assert 1 <= int(result) <= 1000

    def test_generate_bigint(self, generator):
        result = generator.generate('bigint')
        assert 1 <= int(result) <= 100000

    def test_generate_serial(self, generator):
        result = generator.generate('serial')
        assert 1 <= int(result) <= 1000

    def test_generate_real(self, generator):
        result = generator.generate('real')
        assert '.' in result
        float(result)  # Should not raise

    def test_generate_float(self, generator):
        result = generator.generate('float')
        assert '.' in result
        float(result)

    def test_generate_double(self, generator):
        result = generator.generate('double precision')
        assert '.' in result
        float(result)

    def test_generate_decimal(self, generator):
        result = generator.generate('decimal')
        assert '.' in result
        float(result)

    def test_generate_numeric(self, generator):
        result = generator.generate('numeric')
        assert '.' in result
        float(result)

    def test_generate_numeric_parameterized(self, generator):
        result = generator.generate('numeric(10,2)')
        assert '.' in result

    def test_generate_text(self, generator):
        result = generator.generate('text')
        assert result.startswith("'")
        assert result.endswith("'")

    def test_generate_varchar(self, generator):
        result = generator.generate('varchar')
        assert result.startswith("'")
        assert result.endswith("'")

    def test_generate_varchar_parameterized(self, generator):
        result = generator.generate('varchar(50)')
        assert result.startswith("'")
        assert result.endswith("'")

    def test_generate_char(self, generator):
        result = generator.generate('char')
        assert result == "'A'"

    def test_generate_character(self, generator):
        result = generator.generate('character')
        assert result == "'A'"

    def test_generate_date(self, generator):
        result = generator.generate('date')
        assert result == 'CURRENT_DATE'

    def test_generate_timestamp(self, generator):
        result = generator.generate('timestamp')
        assert result == 'CURRENT_TIMESTAMP'

    def test_generate_timestamptz(self, generator):
        result = generator.generate('timestamptz')
        assert result == 'CURRENT_TIMESTAMP'

    def test_generate_time(self, generator):
        result = generator.generate('time')
        assert result == 'CURRENT_TIME'

    def test_generate_timetz(self, generator):
        result = generator.generate('timetz')
        assert result == 'CURRENT_TIME'

    def test_generate_json(self, generator):
        result = generator.generate('json')
        assert result == "'{}'::json"

    def test_generate_jsonb(self, generator):
        result = generator.generate('jsonb')
        assert result == "'{}'::jsonb"

    def test_generate_array(self, generator):
        result = generator.generate('integer[]')
        assert result == "ARRAY['item1', 'item2']"

    def test_generate_array_keyword(self, generator):
        result = generator.generate('text array')
        assert result == "ARRAY['item1', 'item2']"

    def test_generate_unknown_type(self, generator):
        result = generator.generate('custom_type')
        assert result == 'NULL'

    def test_case_insensitivity(self, generator):
        result1 = generator.generate('INTEGER')
        result2 = generator.generate('integer')
        # Both should produce valid integers
        assert 1 <= int(result1) <= 1000
        assert 1 <= int(result2) <= 1000

    def test_deterministic_with_seed(self):
        """Verify that the same seed produces the same output."""
        gen1 = ValueGenerator(random.Random(123))
        gen2 = ValueGenerator(random.Random(123))

        for dtype in ['integer', 'varchar', 'boolean']:
            assert gen1.generate(dtype) == gen2.generate(dtype)

    def test_private_generate_varchar(self, generator):
        result = generator._generate_varchar()
        assert result.startswith("'")
        assert result.endswith("'")

    def test_private_generate_text(self, generator):
        result = generator._generate_text()
        assert result.startswith("'")
        assert result.endswith("'")
