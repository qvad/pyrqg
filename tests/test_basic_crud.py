"""Unit tests for basic_crud grammar."""
import pytest
from pyrqg.api import create_rqg
from pyrqg.core.schema import Table, Column
from pyrqg.dsl.core import Context

@pytest.fixture
def rqg():
    return create_rqg()

@pytest.fixture
def sample_context():
    users = Table(
        name="users",
        columns={
            "id": Column("id", "INTEGER", is_primary_key=True),
            "age": Column("age", "INTEGER"),
            "name": Column("name", "VARCHAR(100)")
        },
        primary_key="id"
    )
    ctx = Context()
    ctx.tables = {"users": users}
    return ctx

def test_basic_crud_select(rqg, sample_context):
    """Test SELECT query generation from basic_crud grammar."""
    queries = list(rqg.generate_from_grammar("basic_crud", rule="select", count=5, context=sample_context))
    assert len(queries) == 5
    for q in queries:
        assert "SELECT" in q
        assert "FROM users" in q

def test_basic_crud_insert(rqg, sample_context):
    """Test INSERT query generation from basic_crud grammar."""
    queries = list(rqg.generate_from_grammar("basic_crud", rule="insert", count=5, context=sample_context))
    assert len(queries) == 5
    for q in queries:
        assert "INSERT INTO users" in q
        assert "VALUES" in q

def test_basic_crud_update(rqg, sample_context):
    """Test UPDATE query generation from basic_crud grammar."""
    queries = list(rqg.generate_from_grammar("basic_crud", rule="update", count=5, context=sample_context))
    assert len(queries) == 5
    for q in queries:
        assert "UPDATE users" in q
        assert "SET" in q

def test_basic_crud_delete(rqg, sample_context):
    """Test DELETE query generation from basic_crud grammar."""
    queries = list(rqg.generate_from_grammar("basic_crud", rule="delete", count=5, context=sample_context))
    assert len(queries) == 5
    for q in queries:
        assert "DELETE FROM users" in q
