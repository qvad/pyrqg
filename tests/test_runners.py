"""Tests for the pluggable runner architecture."""

import pytest
from pyrqg.core.runners import (
    Runner, RunnerConfig, ExecutionStats,
    RunnerRegistry, PostgreSQLRunner, YSQLRunner, YCQL_AVAILABLE
)


class TestRunnerConfig:
    def test_default_config(self):
        config = RunnerConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "postgres"
        assert config.threads == 10

    def test_custom_config(self):
        config = RunnerConfig(host="dbhost", port=5433, database="mydb")
        assert config.host == "dbhost"
        assert config.port == 5433
        assert config.database == "mydb"

    def test_get_dsn_from_parts(self):
        config = RunnerConfig(host="localhost", port=5432, database="testdb", username="user", password="pass")
        assert config.get_dsn() == "postgresql://user:pass@localhost:5432/testdb"

    def test_get_dsn_override(self):
        config = RunnerConfig(dsn="postgresql://custom@localhost/db", host="other", port=1234)
        assert config.get_dsn() == "postgresql://custom@localhost/db"


class TestExecutionStats:
    def test_default_stats(self):
        stats = ExecutionStats()
        assert stats.total == 0
        assert stats.success == 0
        assert stats.failed == 0
        assert stats.syntax_errors == 0

    def test_qps(self):
        stats = ExecutionStats()
        stats.total = 100
        # QPS depends on elapsed time, just check it returns a float
        assert isinstance(stats.qps(), float)

    def test_summary(self):
        stats = ExecutionStats()
        stats.total = 10
        stats.success = 8
        stats.failed = 2
        summary = stats.summary()
        assert "Total: 10" in summary
        assert "Success: 8" in summary
        assert "Failed: 2" in summary


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

    def test_get_runner_class(self):
        cls = RunnerRegistry.get_runner_class("postgresql")
        assert cls is PostgreSQLRunner

    def test_get_for_api_ysql(self):
        # Just test that it doesn't raise (can't actually connect)
        runner = RunnerRegistry.get_for_api("ysql", dsn="postgresql://localhost/test")
        assert isinstance(runner, YSQLRunner)

    def test_get_for_api_postgres(self):
        runner = RunnerRegistry.get_for_api("postgres", dsn="postgresql://localhost/test")
        assert isinstance(runner, PostgreSQLRunner)


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
        assert not runner.is_ddl("SELECT * FROM foo")
        assert not runner.is_ddl("INSERT INTO foo VALUES (1)")


class TestYSQLRunner:
    def test_runner_attributes(self):
        assert YSQLRunner.name == "ysql"
        assert YSQLRunner.target_api == "ysql"
        assert "YSQL" in YSQLRunner.description

    def test_default_port(self):
        runner = YSQLRunner()
        assert runner.config.port == 5433

    def test_is_ddl_extended(self):
        runner = YSQLRunner()
        assert runner.is_ddl("REINDEX TABLE foo")
        assert runner.is_ddl("REFRESH MATERIALIZED VIEW bar")


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

    def test_is_ddl(self):
        from pyrqg.core.runners import YCQLRunner
        runner = YCQLRunner()
        assert runner.is_ddl("CREATE TABLE foo (id int PRIMARY KEY)")
        assert runner.is_ddl("DROP TABLE foo")
        assert runner.is_ddl("USE my_keyspace")
        assert not runner.is_ddl("SELECT * FROM foo")
