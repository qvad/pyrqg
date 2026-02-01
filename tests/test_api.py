"""Tests for the PyRQG API."""

import pytest
from pyrqg.api import RQG, create_rqg
from pyrqg.core.schema import Table, Column


class TestCreateRqg:
    """Test the create_rqg factory function."""

    def test_creates_rqg_instance(self):
        rqg = create_rqg()
        assert isinstance(rqg, RQG)

    def test_rqg_has_grammars(self):
        rqg = create_rqg()
        assert len(rqg.grammars) > 0


class TestRQGTables:
    """Test table management in RQG."""

    @pytest.fixture
    def rqg(self):
        return create_rqg()

    @pytest.fixture
    def sample_table(self):
        return Table(
            name='users',
            columns={
                'id': Column(name='id', data_type='integer', is_primary_key=True),
                'name': Column(name='name', data_type='varchar'),
            },
            primary_key='id'
        )

    def test_add_table(self, rqg, sample_table):
        rqg.add_table(sample_table)
        assert 'users' in rqg.tables
        assert rqg.tables['users'] == sample_table

    def test_add_tables(self, rqg):
        tables = [
            Table(name='t1', columns={'id': Column(name='id', data_type='int')}, primary_key='id'),
            Table(name='t2', columns={'id': Column(name='id', data_type='int')}, primary_key='id'),
        ]
        rqg.add_tables(tables)
        assert 't1' in rqg.tables
        assert 't2' in rqg.tables


class TestRQGGrammars:
    """Test grammar management in RQG."""

    @pytest.fixture
    def rqg(self):
        return create_rqg()

    def test_list_grammars(self, rqg):
        grammars = rqg.list_grammars()
        assert isinstance(grammars, dict)
        assert len(grammars) > 0
        # Check some expected grammars
        assert 'basic_crud' in grammars or 'real_workload' in grammars

    def test_add_custom_grammar(self, rqg):
        class MockGrammar:
            def generate(self, rule, **kwargs):
                return "SELECT 1"

        rqg.add_grammar('custom', MockGrammar())
        assert 'custom' in rqg.grammars

    def test_generate_from_unknown_grammar_raises(self, rqg):
        with pytest.raises(ValueError) as exc:
            list(rqg.generate_from_grammar('nonexistent_grammar'))
        assert "not found" in str(exc.value)
        assert "Available:" in str(exc.value)


class TestRQGGenerate:
    """Test query generation."""

    @pytest.fixture
    def rqg(self):
        return create_rqg()

    def test_generate_from_grammar(self, rqg):
        queries = list(rqg.generate_from_grammar('basic_crud', count=5))
        assert len(queries) == 5
        for q in queries:
            assert isinstance(q, str)
            assert len(q) > 0

    def test_generate_with_seed(self, rqg):
        q1 = list(rqg.generate_from_grammar('basic_crud', count=3, seed=42))
        q2 = list(rqg.generate_from_grammar('basic_crud', count=3, seed=42))
        assert q1 == q2

    def test_generate_wrapper(self, rqg):
        queries = rqg.generate(grammar='basic_crud', count=3)
        assert isinstance(queries, list)
        assert len(queries) == 3

    def test_generate_default_grammar(self, rqg):
        # Should use first available grammar
        queries = rqg.generate(count=1)
        assert len(queries) == 1

    def test_generate_with_rule(self, rqg):
        queries = rqg.generate(grammar='basic_crud', rule='select', count=3)
        assert len(queries) == 3
        for q in queries:
            assert 'SELECT' in q.upper()


class TestRQGDDL:
    """Test DDL generation."""

    @pytest.fixture
    def rqg(self):
        return create_rqg()

    def test_generate_ddl_without_tables(self, rqg):
        ddl = rqg.generate_ddl()
        assert isinstance(ddl, list)
        assert len(ddl) > 0
        assert any('CREATE TABLE' in stmt for stmt in ddl)

    def test_generate_ddl_with_added_table(self, rqg):
        table = Table(
            name='test_table',
            columns={
                'id': Column(name='id', data_type='integer', is_primary_key=True),
                'name': Column(name='name', data_type='varchar'),
            },
            primary_key='id'
        )
        rqg.add_table(table)
        ddl = rqg.generate_ddl()
        assert any('test_table' in stmt for stmt in ddl)

    def test_generate_ddl_with_specific_tables(self, rqg):
        ddl = rqg.generate_ddl(tables=['users'])
        # May or may not include 'users' depending on sample tables
        assert isinstance(ddl, list)

    def test_generate_complex_ddl(self, rqg):
        ddl = rqg.generate_complex_ddl(num_tables=3)
        assert isinstance(ddl, list)
        assert len(ddl) > 0
        create_count = sum(1 for stmt in ddl if 'CREATE TABLE' in stmt)
        assert create_count >= 3

    def test_generate_random_table_ddl(self, rqg):
        ddl = rqg.generate_random_table_ddl('my_random_table')
        assert isinstance(ddl, list)
        assert len(ddl) >= 1
        assert 'my_random_table' in ddl[0]
        assert 'CREATE TABLE' in ddl[0]

    def test_generate_random_schema(self, rqg):
        schema = rqg.generate_random_schema(num_tables=2)
        assert isinstance(schema, list)
        assert len(schema) > 0


class TestRQGRandomConstraintsAndFunctions:
    """Test constraint and function generation."""

    @pytest.fixture
    def rqg(self):
        return create_rqg()

    def test_generate_random_constraints_and_functions(self, rqg):
        # This depends on 'ddl' grammar being available
        if 'ddl' not in rqg.grammars:
            pytest.skip("ddl grammar not available")

        stmts = rqg.generate_random_constraints_and_functions(
            constraints=3, functions=2, seed=42
        )
        assert isinstance(stmts, list)
        assert len(stmts) == 5

    def test_generate_random_constraints_without_ddl_grammar(self, rqg):
        # Remove ddl grammar to test fallback
        if 'ddl' in rqg.grammars:
            del rqg.grammars['ddl']

        stmts = rqg.generate_random_constraints_and_functions(constraints=3, functions=2)
        assert stmts == []


class TestRQGDataInserts:
    """Test data insert generation."""

    @pytest.fixture
    def rqg(self):
        rqg = create_rqg()
        table = Table(
            name='test_users',
            columns={
                'id': Column(name='id', data_type='integer', is_primary_key=True),
                'name': Column(name='name', data_type='varchar'),
            },
            primary_key='id'
        )
        rqg.add_table(table)
        return rqg

    def test_generate_random_data_inserts(self, rqg):
        inserts = rqg.generate_random_data_inserts(rows_per_table=3, seed=42)
        assert isinstance(inserts, list)
        assert len(inserts) == 3
        for stmt in inserts:
            assert 'INSERT' in stmt.upper()

    def test_generate_random_data_inserts_without_tables(self):
        rqg = create_rqg()
        inserts = rqg.generate_random_data_inserts(rows_per_table=3)
        assert inserts == []

    def test_generate_random_data_inserts_without_grammar(self, rqg):
        # Remove basic_crud grammar
        if 'basic_crud' in rqg.grammars:
            del rqg.grammars['basic_crud']

        inserts = rqg.generate_random_data_inserts(rows_per_table=3)
        assert inserts == []


class TestRQGMixedWorkload:
    """Test mixed workload generation."""

    @pytest.fixture
    def rqg(self):
        rqg = create_rqg()
        table = Table(
            name='workload_table',
            columns={
                'id': Column(name='id', data_type='integer', is_primary_key=True),
                'value': Column(name='value', data_type='varchar'),
            },
            primary_key='id'
        )
        rqg.add_table(table)
        return rqg

    def test_run_mixed_workload(self, rqg):
        workload = rqg.run_mixed_workload(count=10, seed=42)
        assert isinstance(workload, list)
        assert len(workload) == 10

    def test_run_mixed_workload_selects_only(self, rqg):
        workload = rqg.run_mixed_workload(
            count=10, seed=42,
            include_selects=True,
            include_inserts=False,
            include_updates=False,
            include_deletes=False
        )
        assert len(workload) == 10
        for stmt in workload:
            assert 'SELECT' in stmt.upper()

    def test_run_mixed_workload_without_grammar(self, rqg):
        if 'basic_crud' in rqg.grammars:
            del rqg.grammars['basic_crud']

        workload = rqg.run_mixed_workload(count=10)
        assert workload == []


class TestRQGGrammarLoading:
    """Test grammar loading functionality."""

    @pytest.fixture
    def rqg(self):
        return create_rqg()

    def test_load_grammar_file_invalid(self, rqg, tmp_path):
        # Create an invalid grammar file
        invalid_file = tmp_path / "invalid_grammar.py"
        invalid_file.write_text("# No grammar object defined\nx = 1")

        with pytest.raises(ValueError) as exc:
            rqg.load_grammar_file('invalid', str(invalid_file))
        assert "No grammar 'g' found" in str(exc.value)

    def test_grammars_property(self, rqg):
        grammars = rqg.grammars
        assert isinstance(grammars, dict)
        # Same as _loader.grammars
        assert grammars is rqg._loader.grammars
