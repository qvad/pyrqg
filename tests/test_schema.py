"""Tests for the schema module."""

import pytest
from pyrqg.core.schema import Column, TableConstraint, Index, Table


class TestColumn:
    """Test the Column dataclass."""

    def test_basic_column(self):
        col = Column(name='id', data_type='integer')
        assert col.name == 'id'
        assert col.data_type == 'integer'
        assert col.is_nullable is True  # default
        assert col.is_primary_key is False
        assert col.is_unique is False

    def test_primary_key_column(self):
        col = Column(name='id', data_type='integer', is_primary_key=True, is_nullable=False)
        assert col.is_primary_key is True
        assert col.is_nullable is False

    def test_column_with_foreign_key(self):
        col = Column(
            name='user_id',
            data_type='integer',
            foreign_key='users.id',
            on_delete='CASCADE'
        )
        assert col.foreign_key == 'users.id'
        assert col.on_delete == 'CASCADE'

    def test_column_with_default(self):
        col = Column(
            name='status',
            data_type='varchar',
            has_default=True,
            default="'active'"
        )
        assert col.has_default is True
        assert col.default == "'active'"

    def test_column_with_check(self):
        col = Column(name='age', data_type='integer', check='age >= 0')
        assert col.check == 'age >= 0'

    def test_column_with_comment(self):
        col = Column(name='email', data_type='text', comment='User email address')
        assert col.comment == 'User email address'


class TestTableConstraint:
    """Test the TableConstraint dataclass."""

    def test_primary_key_constraint(self):
        constraint = TableConstraint(
            name='pk_users',
            constraint_type='PRIMARY KEY',
            columns=['id']
        )
        assert constraint.name == 'pk_users'
        assert constraint.constraint_type == 'PRIMARY KEY'
        assert constraint.columns == ['id']

    def test_composite_primary_key(self):
        constraint = TableConstraint(
            name='pk_order_items',
            constraint_type='PRIMARY KEY',
            columns=['order_id', 'product_id']
        )
        assert len(constraint.columns) == 2

    def test_unique_constraint(self):
        constraint = TableConstraint(
            name='uq_email',
            constraint_type='UNIQUE',
            columns=['email'],
            nulls_not_distinct=True
        )
        assert constraint.nulls_not_distinct is True

    def test_foreign_key_constraint(self):
        constraint = TableConstraint(
            name='fk_orders_user',
            constraint_type='FOREIGN KEY',
            columns=['user_id'],
            references_table='users',
            references_columns=['id'],
            on_delete='CASCADE',
            on_update='NO ACTION'
        )
        assert constraint.references_table == 'users'
        assert constraint.on_delete == 'CASCADE'

    def test_check_constraint(self):
        constraint = TableConstraint(
            name='chk_age',
            constraint_type='CHECK',
            columns=['age'],
            check_expression='age >= 0 AND age <= 150'
        )
        assert constraint.check_expression == 'age >= 0 AND age <= 150'

    def test_deferrable_constraint(self):
        constraint = TableConstraint(
            name='fk_deferred',
            constraint_type='FOREIGN KEY',
            columns=['ref_id'],
            deferrable=True,
            initially_deferred=True
        )
        assert constraint.deferrable is True
        assert constraint.initially_deferred is True


class TestIndex:
    """Test the Index dataclass."""

    def test_basic_index(self):
        idx = Index(name='idx_users_email', columns=['email'])
        assert idx.name == 'idx_users_email'
        assert idx.columns == ['email']
        assert idx.unique is False
        assert idx.method == 'btree'

    def test_unique_index(self):
        idx = Index(name='idx_users_email', columns=['email'], unique=True)
        assert idx.unique is True

    def test_composite_index(self):
        idx = Index(name='idx_orders', columns=['user_id', 'created_at'])
        assert len(idx.columns) == 2

    def test_partial_index(self):
        idx = Index(
            name='idx_active_users',
            columns=['email'],
            where_clause="status = 'active'"
        )
        assert idx.where_clause == "status = 'active'"

    def test_covering_index(self):
        idx = Index(
            name='idx_covering',
            columns=['user_id'],
            include_columns=['name', 'email']
        )
        assert idx.include_columns == ['name', 'email']

    def test_gin_index(self):
        idx = Index(name='idx_search', columns=['search_vector'], method='gin')
        assert idx.method == 'gin'


class TestTable:
    """Test the Table dataclass."""

    @pytest.fixture
    def sample_columns(self):
        return {
            'id': Column(name='id', data_type='integer', is_primary_key=True),
            'name': Column(name='name', data_type='varchar'),
            'email': Column(name='email', data_type='text', is_unique=True),
            'age': Column(name='age', data_type='integer'),
            'balance': Column(name='balance', data_type='numeric'),
        }

    def test_basic_table(self, sample_columns):
        table = Table(name='users', columns=sample_columns, primary_key='id')
        assert table.name == 'users'
        assert table.primary_key == 'id'
        assert len(table.columns) == 5

    def test_get_column_names(self, sample_columns):
        table = Table(name='users', columns=sample_columns)
        names = table.get_column_names()
        assert set(names) == {'id', 'name', 'email', 'age', 'balance'}

    def test_get_numeric_columns(self, sample_columns):
        table = Table(name='users', columns=sample_columns)
        numeric = table.get_numeric_columns()
        assert set(numeric) == {'id', 'age', 'balance'}

    def test_get_string_columns(self, sample_columns):
        table = Table(name='users', columns=sample_columns)
        strings = table.get_string_columns()
        assert set(strings) == {'name', 'email'}

    def test_columns_list_property(self, sample_columns):
        table = Table(name='users', columns=sample_columns)
        cols_list = table.columns_list
        assert len(cols_list) == 5
        assert all(isinstance(c, Column) for c in cols_list)

    def test_table_with_constraints(self, sample_columns):
        constraint = TableConstraint(
            name='chk_age',
            constraint_type='CHECK',
            columns=['age'],
            check_expression='age >= 0'
        )
        table = Table(
            name='users',
            columns=sample_columns,
            constraints=[constraint]
        )
        assert len(table.constraints) == 1

    def test_table_with_indexes(self, sample_columns):
        idx = Index(name='idx_email', columns=['email'], unique=True)
        table = Table(name='users', columns=sample_columns, indexes=[idx])
        assert len(table.indexes) == 1

    def test_table_with_foreign_keys(self, sample_columns):
        table = Table(
            name='orders',
            columns=sample_columns,
            foreign_keys={'user_id': 'users.id'}
        )
        assert table.foreign_keys['user_id'] == 'users.id'

    def test_table_with_tablespace(self, sample_columns):
        table = Table(name='users', columns=sample_columns, tablespace='fast_storage')
        assert table.tablespace == 'fast_storage'

    def test_table_with_comment(self, sample_columns):
        table = Table(name='users', columns=sample_columns, comment='Main user table')
        assert table.comment == 'Main user table'

    def test_partitioned_table(self, sample_columns):
        table = Table(
            name='logs',
            columns=sample_columns,
            partitioned_by='RANGE (created_at)'
        )
        assert table.partitioned_by == 'RANGE (created_at)'

    def test_inherited_table(self, sample_columns):
        table = Table(name='child_table', columns=sample_columns, inherits='parent_table')
        assert table.inherits == 'parent_table'


class TestTableFromList:
    """Test Table.from_list factory method."""

    def test_basic_from_list(self):
        columns_list = [
            {'name': 'id', 'data_type': 'integer'},
            {'name': 'name', 'data_type': 'varchar'},
        ]
        table = Table.from_list('users', columns_list, primary_key='id')
        assert table.name == 'users'
        assert table.primary_key == 'id'
        assert 'id' in table.columns
        assert table.columns['id'].is_primary_key is True

    def test_from_list_with_type_key(self):
        # Test legacy 'type' key instead of 'data_type'
        columns_list = [
            {'name': 'id', 'type': 'integer'},
        ]
        table = Table.from_list('users', columns_list)
        assert table.columns['id'].data_type == 'integer'

    def test_from_list_defaults_to_text(self):
        columns_list = [
            {'name': 'unknown'},
        ]
        table = Table.from_list('users', columns_list)
        assert table.columns['unknown'].data_type == 'text'

    def test_from_list_with_unique_columns(self):
        columns_list = [
            {'name': 'id', 'data_type': 'integer'},
            {'name': 'email', 'data_type': 'text'},
        ]
        table = Table.from_list('users', columns_list, unique_columns=['email'])
        assert table.columns['email'].is_unique is True
        assert 'email' in table.unique_columns

    def test_from_list_with_unique_keys_legacy(self):
        # Test legacy 'unique_keys' parameter
        columns_list = [
            {'name': 'id', 'data_type': 'integer'},
            {'name': 'email', 'data_type': 'text'},
        ]
        table = Table.from_list('users', columns_list, unique_keys=['email'])
        assert table.columns['email'].is_unique is True

    def test_from_list_with_foreign_keys(self):
        columns_list = [
            {'name': 'user_id', 'data_type': 'integer'},
        ]
        table = Table.from_list('orders', columns_list, foreign_keys={'user_id': 'users.id'})
        assert table.foreign_keys['user_id'] == 'users.id'

    def test_from_list_with_is_primary_key_in_dict(self):
        columns_list = [
            {'name': 'id', 'data_type': 'integer', 'is_primary_key': True},
        ]
        table = Table.from_list('users', columns_list)
        assert table.columns['id'].is_primary_key is True

    def test_from_list_with_is_unique_in_dict(self):
        columns_list = [
            {'name': 'email', 'data_type': 'text', 'is_unique': True},
        ]
        table = Table.from_list('users', columns_list)
        assert table.columns['email'].is_unique is True

    def test_from_list_with_is_nullable(self):
        columns_list = [
            {'name': 'id', 'data_type': 'integer', 'is_nullable': False},
        ]
        table = Table.from_list('users', columns_list)
        assert table.columns['id'].is_nullable is False
