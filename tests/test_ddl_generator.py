"""Unit tests for DDLGenerator class."""
import pytest
from pyrqg.ddl_generator import (
    DDLGenerator,
)
from pyrqg.core.schema import Table, Column, TableConstraint, Index


@pytest.fixture
def generator():
    """Create a DDLGenerator with fixed seed."""
    return DDLGenerator(seed=42)


class TestColumnDefinition:
    """Tests for column definition generation."""

    def test_simple_column(self, generator):
        """Test simple column definition."""
        col = Column("name", "VARCHAR(100)")
        result = generator.generate_column_definition(col)
        assert result == "name VARCHAR(100)"

    def test_not_null_column(self, generator):
        """Test NOT NULL column."""
        col = Column("name", "VARCHAR(100)", is_nullable=False)
        result = generator.generate_column_definition(col)
        assert "NOT NULL" in result

    def test_column_with_default(self, generator):
        """Test column with DEFAULT."""
        col = Column("status", "VARCHAR(20)", default="'active'")
        result = generator.generate_column_definition(col)
        assert "DEFAULT 'active'" in result

    def test_unique_column(self, generator):
        """Test UNIQUE column."""
        col = Column("email", "VARCHAR(100)", is_unique=True)
        result = generator.generate_column_definition(col)
        assert "UNIQUE" in result

    def test_column_with_check(self, generator):
        """Test column with CHECK constraint."""
        col = Column("age", "INTEGER", check="age >= 0")
        result = generator.generate_column_definition(col)
        assert "CHECK (age >= 0)" in result

    def test_column_with_references(self, generator):
        """Test column with REFERENCES."""
        col = Column(
            "user_id", "INTEGER",
            references="users(id)",
            on_delete="CASCADE"
        )
        result = generator.generate_column_definition(col)
        assert "REFERENCES users(id)" in result
        assert "ON DELETE CASCADE" in result


class TestTableConstraint:
    """Tests for table constraint generation."""

    def test_primary_key(self, generator):
        """Test PRIMARY KEY constraint."""
        constraint = TableConstraint(None, "PRIMARY KEY", ["id"])
        result = generator.generate_constraint_definition(constraint)
        assert "PRIMARY KEY (id)" in result

    def test_composite_primary_key(self, generator):
        """Test composite PRIMARY KEY."""
        constraint = TableConstraint(None, "PRIMARY KEY", ["order_date", "order_id"])
        result = generator.generate_constraint_definition(constraint)
        assert "PRIMARY KEY (order_date, order_id)" in result

    def test_named_unique_constraint(self, generator):
        """Test named UNIQUE constraint."""
        constraint = TableConstraint("uk_users_email", "UNIQUE", ["email"])
        result = generator.generate_constraint_definition(constraint)
        assert "CONSTRAINT uk_users_email" in result
        assert "UNIQUE (email)" in result

    def test_check_constraint(self, generator):
        """Test CHECK constraint."""
        constraint = TableConstraint(
            "chk_positive", "CHECK", [],
            check_expression="amount > 0"
        )
        result = generator.generate_constraint_definition(constraint)
        assert "CHECK (amount > 0)" in result

    def test_foreign_key_constraint(self, generator):
        """Test FOREIGN KEY constraint."""
        constraint = TableConstraint(
            "fk_orders_user", "FOREIGN KEY", ["user_id"],
            references_table="users",
            references_columns=["id"],
            on_delete="CASCADE"
        )
        result = generator.generate_constraint_definition(constraint)
        assert "FOREIGN KEY (user_id)" in result
        assert "REFERENCES users" in result
        assert "(id)" in result
        assert "ON DELETE CASCADE" in result


class TestCreateTable:
    """Tests for CREATE TABLE generation."""

    def test_simple_table(self, generator):
        """Test simple table creation."""
        table = Table(
            name="users",
            columns={
                "id": Column("id", "INTEGER", is_nullable=False),
                "name": Column("name", "VARCHAR(100)"),
            },
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id"])
            ]
        )
        result = generator.generate_create_table(table)
        assert "CREATE TABLE IF NOT EXISTS users" in result
        assert "id INTEGER NOT NULL" in result
        assert "name VARCHAR(100)" in result
        assert "PRIMARY KEY (id)" in result

    def test_table_with_comment(self, generator):
        """Test table with comment."""
        table = Table(
            name="users",
            columns={"id": Column("id", "INTEGER")},
            comment="User accounts table"
        )
        result = generator.generate_create_table(table)
        assert "COMMENT ON TABLE users IS 'User accounts table'" in result


class TestCreateIndex:
    """Tests for CREATE INDEX generation."""

    def test_simple_index(self, generator):
        """Test simple index creation."""
        index = Index("idx_users_name", ["name"])
        result = generator.generate_create_index("users", index)
        assert "CREATE INDEX IF NOT EXISTS idx_users_name ON users" in result
        assert "(name)" in result

    def test_unique_index(self, generator):
        """Test unique index."""
        index = Index("uk_users_email", ["email"], unique=True)
        result = generator.generate_create_index("users", index)
        assert "CREATE UNIQUE INDEX" in result


class TestGenerateDefault:
    """Tests for default value generation."""

    def test_integer_default(self, generator):
        """Test INTEGER default."""
        result = generator._generate_default("INTEGER")
        assert result is not None
        assert result.isdigit() or result.lstrip('-').isdigit()

    def test_varchar_default(self, generator):
        """Test VARCHAR default."""
        result = generator._generate_default("VARCHAR(100)")
        assert result == "'default'"

    def test_boolean_default(self, generator):
        """Test BOOLEAN default."""
        result = generator._generate_default("BOOLEAN")
        assert result in ("true", "false")


class TestRandomTableGeneration:
    """Tests for random table generation."""

    def test_generate_random_table(self, generator):
        """Test random table generation."""
        table = generator.generate_random_table("test_table")
        assert table.name == "test_table"
        assert len(table.columns) >= 5
        assert len(table.constraints) >= 1

    def test_random_table_has_primary_key(self, generator):
        """Test random table always has primary key."""
        table = generator.generate_random_table("test_table")
        pk_constraints = [c for c in table.constraints if c.constraint_type == "PRIMARY KEY"]
        assert len(pk_constraints) == 1
        assert pk_constraints[0].columns == ["id"]


class TestSampleTables:
    """Tests for sample table generation."""

    def test_generate_sample_tables(self, generator):
        """Test sample tables generation."""
        tables = generator.generate_sample_tables()
        assert len(tables) >= 5

    def test_sample_tables_have_names(self, generator):
        """Test sample tables have expected names."""
        tables = generator.generate_sample_tables()
        table_names = {t.name for t in tables}
        expected = {"users", "products", "orders", "categories"}
        assert expected.issubset(table_names)


class TestSchemaGeneration:
    """Tests for schema generation."""

    def test_generate_schema(self, generator):
        """Test schema generation."""
        ddl = generator.generate_schema(num_tables=3)
        assert len(ddl) >= 3
        create_tables = [s for s in ddl if "CREATE TABLE" in s]
        assert len(create_tables) >= 3


class TestDeterminism:
    """Tests for deterministic generation."""

    def test_same_seed_same_table(self):
        """Test same seed produces same table."""
        gen1 = DDLGenerator(seed=42)
        gen2 = DDLGenerator(seed=42)

        table1 = gen1.generate_random_table("test")
        table2 = gen2.generate_random_table("test")

        assert len(table1.columns) == len(table2.columns)
        for c1, c2 in zip(table1.columns.values(), table2.columns.values()):
            assert c1.name == c2.name
            assert c1.data_type == c2.data_type