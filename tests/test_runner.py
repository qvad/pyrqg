"""Tests for the PyRQG CLI runner."""

import pytest
import sys
from io import StringIO
from unittest.mock import patch, Mock, MagicMock
from pyrqg.runner import (
    build_parser, main, action_list, action_runners,
    action_grammar, action_ddl, _get_grammar_target_api,
    _get_runner_config, _init_context
)
from pyrqg.api import create_rqg
from pyrqg.core.runners import RunnerConfig


class TestCliParser:
    """Test CLI argument parsing."""

    def test_cli_parses_list_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(['list'])
        assert args.mode == 'list'

    def test_cli_parses_runners_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(['runners'])
        assert args.mode == 'runners'

    def test_cli_parses_grammar_subcommand(self):
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'my_grammar',
            '--count', '50',
            '--seed', '123',
        ])
        assert args.mode == 'grammar'
        assert args.grammar == 'my_grammar'
        assert args.count == 50
        assert args.seed == 123

    def test_cli_grammar_requires_argument(self, capsys):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(['grammar'])
        capsys.readouterr()

    def test_cli_parses_all_subcommand(self):
        parser = build_parser()
        args = parser.parse_args([
            'all',
            '--count', '200',
            '--init-schema',
        ])
        assert args.mode == 'all'
        assert args.count == 200
        assert args.init_schema is True

    def test_cli_parses_ddl_subcommand(self):
        parser = build_parser()
        args = parser.parse_args([
            'ddl',
            '--num-tables', '10',
            '--table', 'my_table',
        ])
        assert args.mode == 'ddl'
        assert args.num_tables == 10
        assert args.table == 'my_table'

    def test_cli_parses_common_arguments(self):
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'test',
            '--dsn', 'postgresql://localhost/db',
            '--continue-on-error',
            '--verbose',
            '--seed', '42',
            '--threads', '5',
        ])
        assert args.dsn == 'postgresql://localhost/db'
        assert args.continue_on_error is True
        assert args.verbose is True
        assert args.seed == 42
        assert args.threads == 5

    def test_cli_parses_ycql_arguments(self):
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'yugabyte_ycql',
            '--ycql-host', 'ycql.example.com',
            '--ycql-port', '9043',
            '--ycql-keyspace', 'my_keyspace',
        ])
        assert args.ycql_host == 'ycql.example.com'
        assert args.ycql_port == 9043
        assert args.ycql_keyspace == 'my_keyspace'

    def test_cli_parses_runner_argument(self):
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'basic_crud',
            '--runner', 'postgresql',
        ])
        assert args.runner == 'postgresql'

    def test_cli_parses_output_argument(self):
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'test',
            '--output', '/tmp/queries.sql',
        ])
        assert args.output == '/tmp/queries.sql'


class TestActionList:
    """Test the list grammars action."""

    def test_action_list_returns_zero(self, capsys):
        rqg = create_rqg()
        parser = build_parser()
        args = parser.parse_args(['list'])
        result = action_list(rqg, args)
        assert result == 0

        captured = capsys.readouterr()
        assert "Available grammars" in captured.out

    def test_action_list_shows_grammars(self, capsys):
        rqg = create_rqg()
        parser = build_parser()
        args = parser.parse_args(['list'])
        action_list(rqg, args)

        captured = capsys.readouterr()
        # Should list some built-in grammars
        assert "basic_crud" in captured.out or "real_workload" in captured.out


class TestActionRunners:
    """Test the list runners action."""

    def test_action_runners_returns_zero(self, capsys):
        rqg = create_rqg()
        parser = build_parser()
        args = parser.parse_args(['runners'])
        result = action_runners(rqg, args)
        assert result == 0

        captured = capsys.readouterr()
        assert "Available database runners" in captured.out

    def test_action_runners_shows_runners(self, capsys):
        rqg = create_rqg()
        parser = build_parser()
        args = parser.parse_args(['runners'])
        action_runners(rqg, args)

        captured = capsys.readouterr()
        assert "postgresql" in captured.out
        assert "ysql" in captured.out


class TestGetGrammarTargetApi:
    """Test grammar target API detection."""

    def test_detects_ysql_grammar(self):
        rqg = create_rqg()
        api = _get_grammar_target_api(rqg, 'basic_crud')
        assert api in ('ysql', 'postgres', 'sql')

    def test_detects_ycql_grammar(self):
        rqg = create_rqg()
        api = _get_grammar_target_api(rqg, 'yugabyte_ycql')
        assert api == 'ycql'

    def test_unknown_grammar_defaults_to_ysql(self):
        rqg = create_rqg()
        api = _get_grammar_target_api(rqg, 'nonexistent_grammar')
        assert api == 'ysql'


class TestGetRunnerConfig:
    """Test runner config building from args."""

    def test_builds_config_from_args(self):
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'test',
            '--dsn', 'postgresql://localhost/db',
            '--threads', '20',
            '--continue-on-error',
        ])

        config = _get_runner_config(args)
        assert config.dsn == 'postgresql://localhost/db'
        assert config.threads == 20
        assert config.continue_on_error is True

    def test_uses_ycql_args(self):
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'test',
            '--ycql-host', 'ycql-host',
            '--ycql-port', '9043',
            '--ycql-keyspace', 'test_ks',
        ])

        config = _get_runner_config(args)
        assert config.host == 'ycql-host'
        assert config.port == 9043
        assert config.keyspace == 'test_ks'

    def test_uses_env_fallbacks(self, monkeypatch):
        monkeypatch.setenv('PYRQG_DSN', 'postgresql://env-host/db')
        monkeypatch.setenv('YCQL_HOST', 'env-ycql-host')

        parser = build_parser()
        args = parser.parse_args(['grammar', '--grammar', 'test'])

        config = _get_runner_config(args)
        assert config.dsn == 'postgresql://env-host/db'


class TestInitContext:
    """Test context initialization."""

    def test_returns_none_without_dsn(self):
        ctx = _init_context(None, 42)
        assert ctx is None

    def test_handles_invalid_dsn(self):
        # Invalid DSN should not crash - context is created but tables may be empty
        ctx = _init_context('invalid://not-a-real-dsn', 42)
        # Context is created with empty tables (introspection fails gracefully)
        assert ctx is None or len(ctx.tables) == 0


class TestActionGrammar:
    """Test the grammar execution action."""

    def test_dry_run_outputs_queries(self, capsys):
        rqg = create_rqg()
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'basic_crud',
            '--count', '3',
            '--seed', '42',
        ])

        result = action_grammar(rqg, args)
        assert result == 0

        captured = capsys.readouterr()
        # Should output SQL statements
        assert "SELECT" in captured.out or "INSERT" in captured.out or "UPDATE" in captured.out

    def test_output_to_file(self, tmp_path, capsys):
        rqg = create_rqg()
        output_file = tmp_path / "queries.sql"
        parser = build_parser()
        args = parser.parse_args([
            'grammar',
            '--grammar', 'basic_crud',
            '--count', '5',
            '--output', str(output_file),
        ])

        result = action_grammar(rqg, args)
        assert result == 0

        assert output_file.exists()
        content = output_file.read_text()
        assert len(content) > 0

        captured = capsys.readouterr()
        assert "Saved" in captured.out


class TestActionDdl:
    """Test the DDL generation action."""

    def test_ddl_outputs_statements(self, capsys):
        rqg = create_rqg()
        parser = build_parser()
        args = parser.parse_args([
            'ddl',
            '--num-tables', '2',
        ])

        result = action_ddl(rqg, args)
        assert result == 0

        captured = capsys.readouterr()
        assert "CREATE TABLE" in captured.out

    def test_ddl_single_table(self, capsys):
        rqg = create_rqg()
        parser = build_parser()
        args = parser.parse_args([
            'ddl',
            '--table', 'my_custom_table',
        ])

        result = action_ddl(rqg, args)
        assert result == 0

        captured = capsys.readouterr()
        assert "my_custom_table" in captured.out

    def test_ddl_output_to_file(self, tmp_path, capsys):
        rqg = create_rqg()
        output_file = tmp_path / "schema.sql"
        parser = build_parser()
        args = parser.parse_args([
            'ddl',
            '--num-tables', '3',
            '--output', str(output_file),
        ])

        result = action_ddl(rqg, args)
        assert result == 0

        assert output_file.exists()
        content = output_file.read_text()
        assert "CREATE TABLE" in content


class TestMain:
    """Test the main entry point."""

    def test_main_list(self, capsys):
        result = main(['list'])
        assert result == 0

    def test_main_runners(self, capsys):
        result = main(['runners'])
        assert result == 0

    def test_main_grammar_dry_run(self, capsys):
        result = main([
            'grammar',
            '--grammar', 'basic_crud',
            '--count', '2',
        ])
        assert result == 0

    def test_main_ddl(self, capsys):
        result = main([
            'ddl',
            '--num-tables', '1',
        ])
        assert result == 0

    def test_main_missing_grammar_arg(self, capsys):
        # Should fail because --grammar is required
        with pytest.raises(SystemExit):
            main(['grammar'])


class TestIntegration:
    """Integration tests requiring a database."""

    @pytest.fixture
    def ysql_dsn(self):
        import os
        dsn = os.environ.get("YUGABYTE_DSN")
        if not dsn:
            pytest.skip("YUGABYTE_DSN not set")
        return dsn

    def test_grammar_execution(self, ysql_dsn, capsys):
        result = main([
            'grammar',
            '--grammar', 'basic_crud',
            '--count', '5',
            '--dsn', ysql_dsn,
            '--execute',
            '--continue-on-error',
        ])
        # May have errors due to missing schema, but should not crash
        assert result in (0, 2)

    def test_ddl_execution(self, ysql_dsn, capsys):
        result = main([
            'ddl',
            '--num-tables', '1',
            '--dsn', ysql_dsn,
        ])
        # Should execute or report errors, not crash
        assert result in (0, 2)
