"""Tests for the pluggable runner architecture."""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from pyrqg.core.runners import (
    Runner, RunnerConfig, ExecutionStats,
    RunnerRegistry, PostgreSQLRunner, YSQLRunner, YCQL_AVAILABLE
)
from pyrqg.core.runners.base import query_shape
from pyrqg.core.runners.registry import register_runner


class TestQueryShape:
    """Test the query shape normalization function."""

    def test_replaces_string_literals(self):
        result = query_shape("SELECT * FROM foo WHERE name = 'John'")
        assert "'?'" in result
        assert "'John'" not in result

    def test_replaces_numeric_literals(self):
        result = query_shape("SELECT * FROM foo WHERE id = 123")
        assert "?" in result
        assert "123" not in result

    def test_replaces_decimal_literals(self):
        result = query_shape("SELECT * FROM foo WHERE price = 99.99")
        assert "?" in result
        assert "99.99" not in result

    def test_normalizes_whitespace(self):
        result = query_shape("SELECT   *   FROM    foo")
        assert "  " not in result

    def test_handles_escaped_quotes(self):
        result = query_shape("SELECT * FROM foo WHERE name = 'O''Brien'")
        assert "'?'" in result


class TestRunnerConfig:
    def test_default_config(self):
        config = RunnerConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "postgres"
        assert config.threads == 10
        assert config.statement_timeout == 1000
        assert config.continue_on_error is True
        assert config.progress_interval == 10000

    def test_custom_config(self):
        config = RunnerConfig(host="dbhost", port=5433, database="mydb")
        assert config.host == "dbhost"
        assert config.port == 5433
        assert config.database == "mydb"

    def test_get_dsn_from_parts(self):
        config = RunnerConfig(host="localhost", port=5432, database="testdb", username="user", password="pass")
        assert config.get_dsn() == "postgresql://user:pass@localhost:5432/testdb"

    def test_get_dsn_username_only(self):
        config = RunnerConfig(host="localhost", port=5432, database="testdb", username="user")
        assert config.get_dsn() == "postgresql://user@localhost:5432/testdb"

    def test_get_dsn_no_auth(self):
        config = RunnerConfig(host="localhost", port=5432, database="testdb")
        assert config.get_dsn() == "postgresql://localhost:5432/testdb"

    def test_get_dsn_override(self):
        config = RunnerConfig(dsn="postgresql://custom@localhost/db", host="other", port=1234)
        assert config.get_dsn() == "postgresql://custom@localhost/db"

    def test_ycql_specific_config(self):
        config = RunnerConfig(keyspace="my_keyspace", replication_factor=3)
        assert config.keyspace == "my_keyspace"
        assert config.replication_factor == 3


class TestExecutionStats:
    def test_default_stats(self):
        stats = ExecutionStats()
        assert stats.total == 0
        assert stats.success == 0
        assert stats.failed == 0
        assert stats.syntax_errors == 0
        assert stats.timeouts == 0
        assert stats.connection_errors == 0

    def test_elapsed(self):
        stats = ExecutionStats()
        time.sleep(0.01)  # Small delay
        elapsed = stats.elapsed()
        assert elapsed > 0
        assert elapsed < 1  # Should be less than a second

    def test_qps_zero_elapsed(self):
        stats = ExecutionStats()
        stats.total = 100
        # QPS depends on elapsed time
        qps = stats.qps()
        assert isinstance(qps, float)
        assert qps >= 0

    def test_summary_basic(self):
        stats = ExecutionStats()
        stats.total = 10
        stats.success = 8
        stats.failed = 2
        summary = stats.summary()
        assert "Total: 10" in summary
        assert "Success: 8" in summary
        assert "Failed: 2" in summary
        assert "Unique Query Shapes: 0" in summary

    def test_summary_with_errors(self):
        stats = ExecutionStats()
        stats.total = 100
        stats.failed = 20
        stats.errors["SyntaxError"] = 10
        stats.errors["TypeError"] = 5
        stats.errors["ValueError"] = 5
        summary = stats.summary()
        assert "Top Errors:" in summary
        assert "SyntaxError" in summary

    def test_summary_with_shapes(self):
        stats = ExecutionStats()
        stats.shapes.add("SELECT * FROM ?")
        stats.shapes.add("INSERT INTO ? VALUES (?)")
        summary = stats.summary()
        assert "Unique Query Shapes: 2" in summary


class TestRunnerRegistry:
    def test_list_runners(self):
        runners = RunnerRegistry.list_runners()
        assert "postgresql" in runners
        assert "ysql" in runners
        if YCQL_AVAILABLE:
            assert "ycql" in runners

    def test_is_registered(self):
        assert RunnerRegistry.is_registered("postgresql")
        assert RunnerRegistry.is_registered("ysql")
        assert not RunnerRegistry.is_registered("nonexistent")

    def test_available_runners(self):
        runners = RunnerRegistry.available_runners()
        assert "postgresql" in runners
        assert "ysql" in runners

    def test_get_unknown_runner_raises(self):
        with pytest.raises(ValueError) as exc:
            RunnerRegistry.get("unknown_db")
        assert "not found" in str(exc.value)
        assert "Available:" in str(exc.value)

    def test_get_runner_class(self):
        cls = RunnerRegistry.get_runner_class("postgresql")
        assert cls is PostgreSQLRunner

    def test_get_runner_class_not_found(self):
        cls = RunnerRegistry.get_runner_class("nonexistent")
        assert cls is None

    def test_get_for_api_ysql(self):
        runner = RunnerRegistry.get_for_api("ysql", dsn="postgresql://localhost/test")
        assert isinstance(runner, YSQLRunner)

    def test_get_for_api_postgres(self):
        runner = RunnerRegistry.get_for_api("postgres", dsn="postgresql://localhost/test")
        assert isinstance(runner, PostgreSQLRunner)

    def test_get_for_api_postgresql(self):
        runner = RunnerRegistry.get_for_api("postgresql", dsn="postgresql://localhost/test")
        assert isinstance(runner, PostgreSQLRunner)

    def test_get_for_api_sql_default(self):
        runner = RunnerRegistry.get_for_api("sql", dsn="postgresql://localhost/test")
        assert isinstance(runner, PostgreSQLRunner)

    def test_get_for_api_unknown_defaults_to_postgresql(self):
        runner = RunnerRegistry.get_for_api("unknown_api", dsn="postgresql://localhost/test")
        assert isinstance(runner, PostgreSQLRunner)

    def test_register_and_unregister(self):
        """Test registering and unregistering a custom runner."""
        class TestRunner(Runner):
            name = "test_runner"
            description = "Test runner"
            target_api = "test"

            def connect(self):
                pass

            def close(self):
                pass

            def execute_one(self, query):
                return ".", None

        # Register
        RunnerRegistry.register(TestRunner)
        assert RunnerRegistry.is_registered("test_runner")

        # Unregister
        result = RunnerRegistry.unregister("test_runner")
        assert result is True
        assert not RunnerRegistry.is_registered("test_runner")

        # Unregister non-existent
        result = RunnerRegistry.unregister("nonexistent")
        assert result is False

    def test_register_with_custom_name(self):
        """Test registering a runner with a custom name."""
        class AnotherRunner(Runner):
            name = "original_name"
            description = "Another runner"
            target_api = "test"

            def connect(self):
                pass

            def close(self):
                pass

            def execute_one(self, query):
                return ".", None

        RunnerRegistry.register(AnotherRunner, name="custom_name")
        assert RunnerRegistry.is_registered("custom_name")
        RunnerRegistry.unregister("custom_name")


class TestRegisterRunnerDecorator:
    def test_decorator(self):
        @register_runner("decorated_runner")
        class DecoratedRunner(Runner):
            name = "decorated"
            description = "Decorated runner"
            target_api = "test"

            def connect(self):
                pass

            def close(self):
                pass

            def execute_one(self, query):
                return ".", None

        assert RunnerRegistry.is_registered("decorated_runner")
        RunnerRegistry.unregister("decorated_runner")


class TestPostgreSQLRunner:
    def test_runner_attributes(self):
        assert PostgreSQLRunner.name == "postgresql"
        assert PostgreSQLRunner.target_api == "postgres"
        assert "PostgreSQL" in PostgreSQLRunner.description

    def test_is_ddl(self):
        runner = PostgreSQLRunner(dsn="postgresql://localhost/test")
        assert runner.is_ddl("CREATE TABLE foo (id int)")
        assert runner.is_ddl("ALTER TABLE foo ADD COLUMN bar text")
        assert runner.is_ddl("DROP TABLE foo")
        assert runner.is_ddl("TRUNCATE table1")
        assert runner.is_ddl("  CREATE TABLE foo (id int)")  # With whitespace
        assert not runner.is_ddl("SELECT * FROM foo")
        assert not runner.is_ddl("INSERT INTO foo VALUES (1)")
        assert not runner.is_ddl("UPDATE foo SET bar = 1")
        assert not runner.is_ddl("DELETE FROM foo")

    def test_config_from_kwargs(self):
        runner = PostgreSQLRunner(host="dbhost", port=5555, database="mydb")
        assert runner.config.host == "dbhost"
        assert runner.config.port == 5555
        assert runner.config.database == "mydb"

    def test_config_from_config_object(self):
        config = RunnerConfig(host="confighost", port=1234)
        runner = PostgreSQLRunner(config=config)
        assert runner.config.host == "confighost"
        assert runner.config.port == 1234


class TestYSQLRunner:
    def test_runner_attributes(self):
        assert YSQLRunner.name == "ysql"
        assert YSQLRunner.target_api == "ysql"
        assert "YSQL" in YSQLRunner.description

    def test_default_port(self):
        runner = YSQLRunner()
        assert runner.config.port == 5433
        assert runner.config.database == "yugabyte"
        assert runner.config.username == "yugabyte"
        assert runner.config.password == "yugabyte"

    def test_custom_dsn_overrides_defaults(self):
        runner = YSQLRunner(dsn="postgresql://custom@host:5432/db")
        # When DSN is provided, it overrides defaults
        assert runner.config.dsn == "postgresql://custom@host:5432/db"

    def test_is_ddl_extended(self):
        runner = YSQLRunner()
        assert runner.is_ddl("REINDEX TABLE foo")
        assert runner.is_ddl("REFRESH MATERIALIZED VIEW bar")
        # Standard DDL still works
        assert runner.is_ddl("CREATE TABLE foo (id int)")
        assert runner.is_ddl("DROP TABLE foo")


@pytest.mark.skipif(not YCQL_AVAILABLE, reason="cassandra-driver not installed")
class TestYCQLRunner:
    def test_runner_attributes(self):
        from pyrqg.core.runners import YCQLRunner
        assert YCQLRunner.name == "ycql"
        assert YCQLRunner.target_api == "ycql"
        assert "YCQL" in YCQLRunner.description

    def test_default_port(self):
        from pyrqg.core.runners import YCQLRunner
        runner = YCQLRunner()
        assert runner.config.port == 9042
        assert runner.config.keyspace == "test_keyspace"

    def test_is_ddl(self):
        from pyrqg.core.runners import YCQLRunner
        runner = YCQLRunner()
        assert runner.is_ddl("CREATE TABLE foo (id int PRIMARY KEY)")
        assert runner.is_ddl("DROP TABLE foo")
        assert runner.is_ddl("USE my_keyspace")
        assert runner.is_ddl("ALTER TABLE foo ADD bar text")
        assert runner.is_ddl("TRUNCATE foo")
        assert not runner.is_ddl("SELECT * FROM foo")
        assert not runner.is_ddl("INSERT INTO foo (id) VALUES (1)")

    def test_custom_config(self):
        from pyrqg.core.runners import YCQLRunner
        config = RunnerConfig(host="ycql-host", port=9043, keyspace="custom_ks")
        runner = YCQLRunner(config=config)
        assert runner.config.host == "ycql-host"
        assert runner.config.port == 9043
        assert runner.config.keyspace == "custom_ks"


class TestRunnerContextManager:
    """Test the context manager functionality of runners."""

    def test_postgresql_context_manager(self):
        """Test that context manager calls connect and close."""
        runner = PostgreSQLRunner(dsn="postgresql://localhost/test")

        # Mock the connect and close methods
        runner.connect = Mock()
        runner.close = Mock()

        with runner:
            runner.connect.assert_called_once()

        runner.close.assert_called_once()

    def test_ysql_context_manager(self):
        runner = YSQLRunner()
        runner.connect = Mock()
        runner.close = Mock()

        with runner:
            pass

        runner.connect.assert_called_once()
        runner.close.assert_called_once()


class TestRunnerIntegration:
    """Integration tests that require a running database."""

    @pytest.fixture
    def ysql_dsn(self):
        """Get YSQL DSN from environment or skip."""
        import os
        dsn = os.environ.get("YUGABYTE_DSN")
        if not dsn:
            pytest.skip("YUGABYTE_DSN not set")
        return dsn

    def test_ysql_execute_queries(self, ysql_dsn):
        """Test executing queries against YugabyteDB."""
        runner = YSQLRunner(dsn=ysql_dsn)
        queries = iter([
            "SELECT 1",
            "SELECT 2",
            "SELECT 'test'",
        ])
        stats = runner.execute_queries(queries)
        assert stats.total == 3
        assert stats.success == 3
        assert stats.failed == 0

    @pytest.mark.skipif(not YCQL_AVAILABLE, reason="cassandra-driver not installed")
    def test_ycql_execute_queries(self):
        """Test YCQL execution if available."""
        import os
        host = os.environ.get("YCQL_HOST")
        if not host:
            pytest.skip("YCQL_HOST not set")

        from pyrqg.core.runners import YCQLRunner
        runner = YCQLRunner(host=host)
        # Just test connection and a simple query
        runner.connect()
        symbol, error = runner.execute_one("SELECT release_version FROM system.local")
        runner.close()
        # Either success or error is acceptable (depends on keyspace setup)
        assert symbol in (".", "e", "S")
