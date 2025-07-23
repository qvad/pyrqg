"""
Enhanced DDL Generator for PyRQG
Supports complex constraints, composite keys, and realistic schemas
"""

from typing import List, Optional, Dict, Tuple
import random
from pyrqg.core.schema import Table, Column, TableConstraint, Index

class DDLGenerator:
    """Generate complex DDL statements"""
    
    def __init__(
        self,
        dialect: str = "postgresql",
        seed: Optional[int] = None,
        profile: str = "core",
        fk_ratio: float = 0.3,
        index_ratio: float = 0.7,
        composite_index_ratio: float = 0.3,
        partial_index_ratio: float = 0.2,
    ):
        self.dialect = dialect
        self.generated_tables = []
        # Use a local RNG for reproducibility and isolation from global random
        self.rng = random.Random(seed)
        # Knobs
        self.profile = profile
        self.fk_ratio = max(0.0, min(1.0, fk_ratio))
        self.index_ratio = max(0.0, min(1.0, index_ratio))
        self.composite_index_ratio = max(0.0, min(1.0, composite_index_ratio))
        self.partial_index_ratio = max(0.0, min(1.0, partial_index_ratio))
        # Weighted data type coverage (PostgreSQL-oriented)
        self._init_type_weights()

    def _init_type_weights(self):
        base_weights = [
            (lambda: "INTEGER", 18),
            (lambda: "BIGINT", 16),
            (lambda: "SMALLINT", 3),
            (lambda: f"VARCHAR({self.rng.choice([50,100,200,255])})", 18),
            (lambda: "TEXT", 8),
            (lambda: f"NUMERIC({self.rng.randint(8,18)},{self.rng.choice([0,2,4])})", 7),
            (lambda: f"DECIMAL({self.rng.randint(8,18)},{self.rng.choice([0,2,4])})", 5),
            (lambda: "REAL", 3),
            (lambda: "DOUBLE PRECISION", 5),
            (lambda: "BOOLEAN", 10),
            (lambda: "DATE", 6),
            (lambda: "TIMESTAMP", 7),
            (lambda: "TIMESTAMPTZ", 6),
            (lambda: "TIME", 2),
            (lambda: "TIMETZ", 2),
            (lambda: "UUID", 6),
            (lambda: "JSONB", 7),
            (lambda: "JSON", 2),
            (lambda: "BYTEA", 3),
            (lambda: "INET", 2),
            (lambda: "CIDR", 1),
            (lambda: "MACADDR", 1),
            (lambda: f"CHAR({self.rng.choice([1,2,10])})", 2),
            (lambda: "MONEY", 1),
            (lambda: "INTERVAL", 2),
            # Range types
            (lambda: "INT4RANGE", 1),
            (lambda: "INT8RANGE", 1),
            (lambda: "NUMRANGE", 1),
            (lambda: "DATERANGE", 1),
            (lambda: "TSRANGE", 1),
            (lambda: "TSTZRANGE", 1),
        ]
        self._base_type_weights = self._apply_profile_weights(base_weights, self.profile)

    def _apply_profile_weights(self, base: List[Tuple], profile: str) -> List[Tuple]:
        items: List[Tuple] = [(f, w) for (f, w) in base]
        def bump(pred, factor):
            for i, (f, w) in enumerate(items):
                t = f()
                if pred(t):
                    items[i] = (lambda f=f: f(), max(1, int(w * factor)))
        p = (profile or "core").lower()
        if p == "json_heavy":
            bump(lambda t: t in ("JSONB", "JSON") or t == "TEXT", 2.5)
        elif p == "time_series":
            bump(lambda t: t in ("TIMESTAMPTZ", "TIMESTAMP", "DATE", "INTERVAL"), 2.5)
            bump(lambda t: "NUMERIC" in t or "DECIMAL" in t, 1.5)
        elif p == "network_heavy":
            bump(lambda t: t in ("INET", "CIDR", "MACADDR"), 3.0)
        elif p == "wide_range":
            avg = max(1, int(sum(w for _, w in items) / len(items)))
            items = [(f, max(1, int((w + avg) / 2))) for (f, w) in items]
        return items

    def _weighted_choice(self, items):
        total = sum(w for _, w in items)
        pick = self.rng.uniform(0, total)
        upto = 0
        for f, w in items:
            if upto + w >= pick:
                return f()
            upto += w
        return items[-1][0]()

    def _random_data_type(self) -> str:
        base = self._weighted_choice(self._base_type_weights)
        if self.rng.random() < 0.08 and not base.endswith("[]") and base not in ("JSON", "JSONB"):
            return f"{base}[]"
        return base
        
    def generate_column_definition(self, col: Column) -> str:
        """Generate column definition SQL"""
        parts = [col.name, col.data_type]
        
        if not col.is_nullable:
            parts.append("NOT NULL")
            
        if col.default is not None:
            parts.append(f"DEFAULT {col.default}")
            
        if col.is_unique:
            parts.append("UNIQUE")
            
        if col.check:
            parts.append(f"CHECK ({col.check})")
            
        if col.references:
            ref_parts = [f"REFERENCES {col.references}"]
            if col.on_delete:
                ref_parts.append(f"ON DELETE {col.on_delete}")
            if col.on_update:
                ref_parts.append(f"ON UPDATE {col.on_update}")
            parts.append(" ".join(ref_parts))
        
        return " ".join(parts)
    
    def generate_constraint_definition(self, constraint: TableConstraint) -> str:
        """Generate constraint definition SQL"""
        parts = []
        
        if constraint.name:
            parts.append(f"CONSTRAINT {constraint.name}")
        
        if constraint.constraint_type == "PRIMARY KEY":
            parts.append(f"PRIMARY KEY ({', '.join(constraint.columns)})")
            
        elif constraint.constraint_type == "UNIQUE":
            if getattr(constraint, 'nulls_not_distinct', False):
                parts.append(f"UNIQUE NULLS NOT DISTINCT ({', '.join(constraint.columns)})")
            else:
                parts.append(f"UNIQUE ({', '.join(constraint.columns)})")
            
        elif constraint.constraint_type == "CHECK":
            parts.append(f"CHECK ({constraint.check_expression})")
            
        elif constraint.constraint_type == "FOREIGN KEY":
            fk_parts = [f"FOREIGN KEY ({', '.join(constraint.columns)})"]
            fk_parts.append(f"REFERENCES {constraint.references_table}")
            if constraint.references_columns:
                fk_parts.append(f"({', '.join(constraint.references_columns)})")
            if constraint.on_delete:
                fk_parts.append(f"ON DELETE {constraint.on_delete}")
            if constraint.on_update:
                fk_parts.append(f"ON UPDATE {constraint.on_update}")
            if constraint.deferrable:
                fk_parts.append("DEFERRABLE")
                if constraint.initially_deferred:
                    fk_parts.append("INITIALLY DEFERRED")
            parts.extend(fk_parts)
        
        return " ".join(parts)
    
    def generate_create_table(self, table: Table) -> str:
        """Generate CREATE TABLE statement"""
        lines = [f"CREATE TABLE IF NOT EXISTS {table.name} ("]
        
        col_lines = []
        for col in table.columns.values():
            col_lines.append(f"    {self.generate_column_definition(col)}")
        
        for constraint in table.constraints:
            col_lines.append(f"    {self.generate_constraint_definition(constraint)}")
        
        lines.append(",\n".join(col_lines))
        lines.append(")")
        
        options = []
        if table.tablespace:
            options.append(f"TABLESPACE {table.tablespace}")
        if table.partitioned_by:
            options.append(f"PARTITION BY {table.partitioned_by}")
        if table.inherits:
            options.append(f"INHERITS ({table.inherits})")
        
        if options:
            lines.append(" ".join(options))
        
        sql = "\n".join(lines)
        
        if table.comment:
            sql += f";\nCOMMENT ON TABLE {table.name} IS '{table.comment}'"
        
        return sql
    
    def generate_create_index(self, table_name: str, index: Index) -> str:
        """Generate CREATE INDEX statement"""
        unique = "UNIQUE " if index.unique else ""
        sql = f"CREATE {unique}INDEX IF NOT EXISTS {index.name} ON {table_name}"
        
        if index.method != "btree":
            sql += f" USING {index.method}"
        
        sql += f" ({', '.join(index.columns)})"
        
        if index.include_columns:
            sql += f" INCLUDE ({', '.join(index.include_columns)})"
        
        if index.where_clause:
            sql += f" WHERE {index.where_clause}"
        
        return sql
    
    def _create_table(self, name: str, columns: List[Column], **kwargs) -> Table:
        """Helper to create Table object from list of columns"""
        cols_dict = {c.name: c for c in columns}
        return Table(name=name, columns=cols_dict, **kwargs)

    def generate_sample_tables(self) -> List[Table]:
        """Generate sample complex table definitions"""
        tables = []
        
        users_table = self._create_table(
            name="users",
            columns=[
                Column("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", is_nullable=False),
                Column("username", "VARCHAR(50)", is_nullable=False, is_unique=True),
                Column("email", "VARCHAR(100)", is_nullable=False),
                Column("first_name", "VARCHAR(50)", is_nullable=False),
                Column("last_name", "VARCHAR(50)", is_nullable=False),
                Column("age", "INTEGER", check="age >= 18 AND age <= 120"),
                Column("phone", "VARCHAR(20)"),
                Column("status", "VARCHAR(20)", is_nullable=False, default="'active'",
                       check="status IN ('active', 'inactive', 'suspended', 'deleted')"),
                Column("created_at", "TIMESTAMP", is_nullable=False, default="CURRENT_TIMESTAMP"),
                Column("updated_at", "TIMESTAMP", is_nullable=False, default="CURRENT_TIMESTAMP")
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id"]),
                TableConstraint("uk_users_email_status", "UNIQUE", ["email", "status"]),
                TableConstraint("chk_users_names", "CHECK", [], check_expression="first_name != last_name")
            ],
            indexes=[
                Index("idx_users_email", ["email"]),
                Index("idx_users_status_created", ["status", "created_at"]),
                Index("idx_users_fullname", ["last_name", "first_name"])
            ]
        )
        tables.append(users_table)
        
        categories_table = self._create_table(
            name="categories",
            columns=[
                Column("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", is_nullable=False),
                Column("name", "VARCHAR(200)", is_nullable=False, is_unique=True),
                Column("parent_id", "INTEGER")
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id"]),
                TableConstraint("fk_categories_parent", "FOREIGN KEY", ["parent_id"],
                               references_table="categories", references_columns=["id"],
                               on_delete="SET NULL")
            ],
            indexes=[Index("idx_categories_parent", ["parent_id"])]
        )
        tables.append(categories_table)
        
        addresses_table = self._create_table(
            name="addresses",
            columns=[
                Column("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", is_nullable=False),
                Column("user_id", "INTEGER"),
                Column("line1", "VARCHAR(200)", is_nullable=False),
                Column("line2", "VARCHAR(200)"),
                Column("city", "VARCHAR(100)", is_nullable=False),
                Column("state", "VARCHAR(100)"),
                Column("postal_code", "VARCHAR(20)"),
                Column("country", "VARCHAR(100)", is_nullable=False)
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id"]),
                TableConstraint("fk_addresses_user", "FOREIGN KEY", ["user_id"],
                               references_table="users", references_columns=["id"],
                               on_delete="SET NULL")
            ],
            indexes=[Index("idx_addresses_user", ["user_id"])]
        )
        tables.append(addresses_table)
        
        products_table = self._create_table(
            name="products",
            columns=[
                Column("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", is_nullable=False),
                Column("sku", "VARCHAR(50)", is_nullable=False),
                Column("name", "VARCHAR(200)", is_nullable=False),
                Column("description", "TEXT"),
                Column("category_id", "INTEGER", is_nullable=False),
                Column("price", "DECIMAL(10,2)", is_nullable=False, check="price > 0"),
                Column("cost", "DECIMAL(10,2)", check="cost >= 0"),
                Column("quantity", "INTEGER", is_nullable=False, default="0", check="quantity >= 0"),
                Column("min_quantity", "INTEGER", default="0"),
                Column("max_quantity", "INTEGER"),
                Column("is_active", "BOOLEAN", is_nullable=False, default="true"),
                Column("created_by", "INTEGER", is_nullable=False),
                Column("created_at", "TIMESTAMP", is_nullable=False, default="CURRENT_TIMESTAMP")
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id"]),
                TableConstraint("uk_products_sku", "UNIQUE", ["sku"]),
                TableConstraint("uk_products_name_category", "UNIQUE", ["name", "category_id"]),
                TableConstraint("chk_products_quantities", "CHECK", [],
                               check_expression="min_quantity <= max_quantity"),
                TableConstraint("chk_products_profit", "CHECK", [],
                               check_expression="price > cost OR cost IS NULL"),
                TableConstraint("fk_products_category", "FOREIGN KEY", ["category_id"],
                               references_table="categories", references_columns=["id"],
                               on_delete="RESTRICT"),
                TableConstraint("fk_products_creator", "FOREIGN KEY", ["created_by"],
                               references_table="users", references_columns=["id"],
                               on_delete="RESTRICT")
            ],
            indexes=[
                Index("idx_products_category", ["category_id"]),
                Index("idx_products_active_category", ["category_id", "is_active"],
                               where_clause="is_active = true"),
                Index("idx_products_price", ["price"], include_columns=["name", "sku"])
            ]
        )
        tables.append(products_table)
        
        orders_table = self._create_table(
            name="orders",
            columns=[
                Column("order_date", "DATE", is_nullable=False),
                Column("order_number", "INTEGER", is_nullable=False),
                Column("customer_id", "INTEGER", is_nullable=False),
                Column("status", "VARCHAR(20)", is_nullable=False, default="'pending'"),
                Column("total_amount", "DECIMAL(12,2)", is_nullable=False, check="total_amount >= 0"),
                Column("tax_amount", "DECIMAL(10,2)", default="0"),
                Column("discount_amount", "DECIMAL(10,2)", default="0"),
                Column("shipping_address_id", "INTEGER"),
                Column("billing_address_id", "INTEGER"),
                Column("notes", "TEXT"),
                Column("created_at", "TIMESTAMP", is_nullable=False, default="CURRENT_TIMESTAMP"),
                Column("updated_at", "TIMESTAMP", is_nullable=False, default="CURRENT_TIMESTAMP")
            ],
            constraints=[
                TableConstraint("pk_orders", "PRIMARY KEY", ["order_date", "order_number"]),
                TableConstraint("fk_orders_customer", "FOREIGN KEY", ["customer_id"],
                               references_table="users", references_columns=["id"],
                               on_delete="RESTRICT"),
                TableConstraint("fk_orders_shipping", "FOREIGN KEY", ["shipping_address_id"],
                               references_table="addresses", references_columns=["id"],
                               on_delete="SET NULL"),
                TableConstraint("fk_orders_billing", "FOREIGN KEY", ["billing_address_id"],
                               references_table="addresses", references_columns=["id"],
                               on_delete="SET NULL"),
                TableConstraint("chk_orders_amounts", "CHECK", [],
                               check_expression="total_amount >= (tax_amount + discount_amount)")
            ],
            indexes=[
                Index("idx_orders_customer_date", ["customer_id", "order_date"]),
                Index("idx_orders_status", ["status"], where_clause="status != 'completed'"),
                Index("uk_orders_date_customer", ["order_date", "customer_id", "order_number"], 
                               unique=True)
            ]
        )
        tables.append(orders_table)
        
        order_items_table = self._create_table(
            name="order_items",
            columns=[
                Column("order_date", "DATE", is_nullable=False),
                Column("order_number", "INTEGER", is_nullable=False),
                Column("line_number", "INTEGER", is_nullable=False),
                Column("product_id", "INTEGER", is_nullable=False),
                Column("quantity", "INTEGER", is_nullable=False, check="quantity > 0"),
                Column("unit_price", "DECIMAL(10,2)", is_nullable=False, check="unit_price >= 0"),
                Column("discount_percent", "DECIMAL(5,2)", default="0",
                               check="discount_percent >= 0 AND discount_percent <= 100"),
                Column("tax_rate", "DECIMAL(5,2)", default="0",
                               check="tax_rate >= 0 AND tax_rate <= 100")
            ],
            constraints=[
                TableConstraint("pk_order_items", "PRIMARY KEY", 
                               ["order_date", "order_number", "line_number"]),
                TableConstraint("fk_order_items_order", "FOREIGN KEY", 
                               ["order_date", "order_number"],
                               references_table="orders", 
                               references_columns=["order_date", "order_number"],
                               on_delete="CASCADE"),
                TableConstraint("fk_order_items_product", "FOREIGN KEY", ["product_id"],
                               references_table="products", references_columns=["id"],
                               on_delete="RESTRICT")
            ],
            indexes=[
                Index("idx_order_items_product", ["product_id"]),
                Index("idx_order_items_order", ["order_date", "order_number"])
            ]
        )
        tables.append(order_items_table)
        
        audit_log_table = self._create_table(
            name="audit_log",
            columns=[
                Column("id", "BIGINT GENERATED BY DEFAULT AS IDENTITY", is_nullable=False),
                Column("table_name", "VARCHAR(50)", is_nullable=False),
                Column("record_id", "VARCHAR(100)", is_nullable=False),
                Column("action", "VARCHAR(10)", is_nullable=False,
                               check="action IN ('INSERT', 'UPDATE', 'DELETE')"),
                Column("user_id", "INTEGER"),
                Column("changed_data", "JSONB"),
                Column("ip_address", "INET"),
                Column("user_agent", "TEXT"),
                Column("created_at", "TIMESTAMP", is_nullable=False, default="CURRENT_TIMESTAMP")
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id", "created_at"]),
                TableConstraint("fk_audit_user", "FOREIGN KEY", ["user_id"],
                               references_table="users", references_columns=["id"],
                               on_delete="SET NULL", deferrable=True)
            ],
            indexes=[
                Index("idx_audit_table_record", ["table_name", "record_id"]),
                Index("idx_audit_user_date", ["user_id", "created_at"]),
                Index("idx_audit_recent_deletes", ["table_name", "created_at"],
                               where_clause="action = 'DELETE' AND created_at >= DATE '2000-01-01'"),
                Index("idx_audit_data_gin", ["changed_data"], method="gin")
            ],
            partitioned_by="RANGE (created_at)"
        )
        tables.append(audit_log_table)
        
        return tables
    
    def generate_random_table(self, table_name: str,
                            num_columns: Optional[int] = None,
                            num_constraints: Optional[int] = None) -> Table:
        """Generate a random table with complex constraints.

        Args:
            table_name: Name for the generated table.
            num_columns: Number of columns to generate (default: random 5-15).
            num_constraints: Number of constraints to generate (default: random 2-6).

        Returns:
            Table object with columns, constraints, and indexes.
        """
        if num_columns is None:
            num_columns = self.rng.randint(5, 15)
        if num_constraints is None:
            num_constraints = self.rng.randint(2, 6)
        
        columns = []
        
        # Always have an ID column
        columns.append(Column("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", is_nullable=False))
        
        for i in range(num_columns - 1):
            col_name = f"col_{self.rng.choice(['data', 'value', 'info', 'attr'])}_{i}"
            col_type = self._random_data_type()
            
            use_default = self.rng.random() < 0.3
            if use_default and (col_type.endswith("[]") or "RANGE" in col_type):
                use_default = False

            col = Column(
                name=col_name,
                data_type=col_type,
                is_nullable=self.rng.choice([True, True, False]),
                is_unique=self.rng.random() < 0.1 and not col_type.endswith("[]") and col_type not in ("JSONB", "JSON", "BYTEA"),
                default=self._generate_default(col_type) if use_default else None,
                check=None 
            )
            columns.append(col)
        
        if self.rng.random() < 0.3:
            columns.append(Column("parent_id", "INTEGER", is_nullable=True))
        
        constraints = []
        non_pk_types = {"JSONB", "JSON", "INTERVAL", "BYTEA", "TEXT"}

        constraints.append(TableConstraint(None, "PRIMARY KEY", ["id"]))
        remaining = max(num_constraints - 1, 0)

        uniqueable_cols = [
            c.name for c in columns
            if c.name != "id"
            and not c.data_type.endswith("[]")
            and c.data_type not in non_pk_types
            and "RANGE" not in c.data_type
        ]
        if remaining > 0 and uniqueable_cols:
            for i in range(self.rng.randint(0, min(2, remaining))):
                k = min(self.rng.randint(1, 3), len(uniqueable_cols))
                unique_cols = self.rng.sample(uniqueable_cols, k)
                constraints.append(
                    TableConstraint(f"uk_{table_name}_{i}", "UNIQUE", unique_cols, nulls_not_distinct=(self.rng.random() < 0.5))
                )
                remaining -= 1
                if remaining == 0:
                    break
        
        if remaining > 0 and any(c.name == "parent_id" for c in columns) and self.rng.random() < 0.9:
            constraints.append(
                TableConstraint(f"fk_{table_name}_parent", "FOREIGN KEY", ["parent_id"],
                                references_table=table_name, references_columns=["id"],
                                on_delete=self.rng.choice(["SET NULL", "RESTRICT", "CASCADE"]),
                                deferrable=(self.rng.random() < 0.2), initially_deferred=(self.rng.random() < 0.5))
            )
            remaining -= 1
        
        indexes = []
        indexable_cols = [
            c for c in columns
            if not c.data_type.endswith("[]")
            and c.data_type not in ("JSONB", "JSON", "BYTEA")
            and "RANGE" not in c.data_type
        ]
        indexable_names = [c.name for c in indexable_cols]

        if indexable_names:
            max_idx = max(0, int(1 + round(4 * self.index_ratio)))
            for i in range(self.rng.randint(0, max_idx)):
                kcols = 1 if self.rng.random() > self.composite_index_ratio else self.rng.randint(2, min(3, len(indexable_names)))
                idx_cols = self.rng.sample(indexable_names, min(kcols, len(indexable_names)))
                where_clause = None
                if self.rng.random() < self.partial_index_ratio:
                    where_clause = self._generate_where_clause(indexable_cols)
                index = Index(
                    name=f"idx_{table_name}_{i}",
                    columns=idx_cols,
                    unique=self.rng.random() < 0.1,
                    where_clause=where_clause
                )
                indexes.append(index)
        
        return self._create_table(name=table_name, columns=columns, constraints=constraints, indexes=indexes)

    def _generate_default(self, data_type: str) -> Optional[str]:
        """Generate appropriate default value for data type"""
        if data_type.endswith("[]"):
            base = data_type[:-2]
            if "INT" in base or base in ("INTEGER", "BIGINT", "SMALLINT"):
                return "ARRAY[1,2,3]::INTEGER[]"
            elif base.startswith("VARCHAR") or base == "TEXT" or base.startswith("CHAR"):
                return "ARRAY['a','b']::TEXT[]"
            elif "NUMERIC" in base or "DECIMAL" in base:
                return "ARRAY[1.0,2.0]::NUMERIC[]"
            elif "BOOL" in base:
                return "ARRAY[true,false]::BOOLEAN[]"
            elif "TIMESTAMP" in base:
                return "ARRAY[NOW()]::TIMESTAMP[]"
            elif base == "DATE":
                return "ARRAY[CURRENT_DATE]::DATE[]"
            elif base == "UUID":
                return "ARRAY[gen_random_uuid()]::UUID[]"
            else:
                return None

        if data_type == "INT4RANGE":
            return "'[0,100)'::int4range"
        elif data_type == "INT8RANGE":
            return "'[0,100)'::int8range"
        elif data_type == "NUMRANGE":
            return "'[0.0,100.0)'::numrange"
        elif data_type == "DATERANGE":
            return "'[2020-01-01,2020-12-31)'::daterange"
        elif data_type == "TSRANGE":
            return "tsrange(NOW(), NOW() + interval '1 day')"
        elif data_type == "TSTZRANGE":
            return "tstzrange(NOW(), NOW() + interval '1 day')"

        if "INT" in data_type:
            return str(self.rng.randint(0, 100))
        elif "VARCHAR" in data_type or data_type == "TEXT" or data_type.startswith("CHAR"):
            return "'default'"
        elif "BOOL" in data_type:
            return self.rng.choice(["true", "false"])
        elif "TIMESTAMP" in data_type:
            return "CURRENT_TIMESTAMP"
        elif "DATE" in data_type:
            return "CURRENT_DATE"
        elif "DECIMAL" in data_type or "NUMERIC" in data_type:
            return "0.00"
        elif data_type == "UUID":
            return "gen_random_uuid()"
        elif data_type in ("JSONB", "JSON"):
            return "'{}'::jsonb" if data_type == "JSONB" else "'{}'::json"
        elif data_type == "BYTEA":
            return None
        elif data_type == "INET":
            return "'127.0.0.1'::inet"
        elif data_type == "CIDR":
            return "'10.0.0.0/8'::cidr"
        elif data_type == "MACADDR":
            return "'08:00:2b:01:02:03'"
        elif data_type == "MONEY":
            return "0"
        return None

    def generate_schema(self, num_tables: int = 5) -> List[str]:
        """Generate complete schema with multiple related tables"""
        ddl_statements = []
        
        base_tables = self.generate_sample_tables()
        if num_tables <= len(base_tables):
            tables = list(base_tables[:num_tables])
        else:
            tables = list(base_tables)
            for i in range(num_tables - len(base_tables)):
                table = self.generate_random_table(f"table_{i}")
                tables.append(table)
        
        for table in tables:
            ddl_statements.append(self.generate_create_table(table))
            if table.partitioned_by:
                ddl_statements.append(
                    f"CREATE TABLE IF NOT EXISTS {table.name}_default PARTITION OF {table.name} DEFAULT"
                )
            for index in table.indexes:
                ddl_statements.append(self.generate_create_index(table.name, index))
        
        ddl_statements.extend(self._generate_cross_table_fks(tables))
        return ddl_statements

    def _generate_cross_table_fks(self, tables: List[Table]) -> List[str]:
        if self.fk_ratio <= 0.0 or len(tables) < 2:
            return []
        
        tmeta: Dict[str, Dict[str, str]] = {}
        for t in tables:
            has_single_id_pk = False
            for con in t.constraints:
                if con.constraint_type == "PRIMARY KEY" and con.columns == ["id"]:
                    has_single_id_pk = True
                    break
            if has_single_id_pk:
                pk_cols = [c for c in t.columns.values() if c.name == 'id']
                if pk_cols:
                    tmeta[t.name] = {"pk": pk_cols[0].data_type}
        if not tmeta:
            return []
        
        out: List[str] = []
        for t in tables:
            if self.rng.random() > self.fk_ratio:
                continue
            n = 1 if self.rng.random() < 0.7 else 2
            for _ in range(n):
                ref_table = self.rng.choice([n for n in tmeta.keys() if n != t.name]) if len(tmeta) > 1 else None
                if not ref_table:
                    continue
                ref_type = tmeta[ref_table]["pk"]
                candidates = [c.name for c in t.columns.values() if c.data_type.split('(')[0] == ref_type.split('(')[0] and c.name != 'id']
                if candidates and self.rng.random() < 0.7:
                    col = self.rng.choice(candidates)
                else:
                    base = f"{ref_table}_id"
                    new_name = base
                    suffix = 2
                    existing = set(t.columns.keys())
                    while new_name in existing:
                        new_name = f"{base}_{suffix}"
                        suffix += 1
                    t.columns[new_name] = Column(new_name, ref_type, is_nullable=True)
                    col = new_name
                    out.append(f"ALTER TABLE {t.name} ADD COLUMN {col} {ref_type}")
                
                cname = f"fk_{t.name}_{ref_table}_{self.rng.randint(1, 1_000_000)}"
                action = self.rng.choice(["RESTRICT", "SET NULL", "CASCADE"])
                out.append(
                    f"ALTER TABLE {t.name} ADD CONSTRAINT {cname} FOREIGN KEY ({col}) REFERENCES {ref_table}(id) ON DELETE {action}"
                )
        return out

    def generate_alter_table_statements(self, table: Table, max_alters: int = 3) -> List[str]:
        """Generate safe ALTER TABLE statements for an existing table."""
        stmts: List[str] = []

        existing_cols = list(table.columns.keys())
        pk_cols = set()
        for con in table.constraints:
            if con.constraint_type == "PRIMARY KEY":
                for c in con.columns:
                    pk_cols.add(c)

        def _add_column() -> None:
            base = f"new_col_{self.rng.randint(1, 1_000_000)}"
            new_name = base
            suffix = 2
            while new_name in existing_cols:
                new_name = f"{base}_{suffix}"
                suffix += 1
            dtype = self._random_data_type()
            default = self._generate_default(dtype) if self.rng.random() < 0.3 else None
            parts = [f"ALTER TABLE {table.name} ADD COLUMN {new_name} {dtype}"]
            if default is not None:
                parts.append(f"DEFAULT {default}")
            stmt = " ".join(parts)
            stmts.append(stmt)
            table.columns[new_name] = Column(name=new_name, data_type=dtype, is_nullable=True, default=default)
            existing_cols.append(new_name)

        def _alter_default() -> None:
            candidates = [c for c in table.columns.values() if c.name not in pk_cols]
            if not candidates:
                return
            col = self.rng.choice(candidates)
            if col.default is None and self.rng.random() < 0.7:
                new_def = self._generate_default(col.data_type)
                if new_def is None:
                    return
                stmts.append(f"ALTER TABLE {table.name} ALTER COLUMN {col.name} SET DEFAULT {new_def}")
                col.default = new_def
            else:
                stmts.append(f"ALTER TABLE {table.name} ALTER COLUMN {col.name} DROP DEFAULT")
                col.default = None

        def _add_check() -> None:
            num_cols = [c.name for c in table.columns.values() if ("INT" in c.data_type or "DECIMAL" in c.data_type or "NUMERIC" in c.data_type)]
            if not num_cols:
                return
            col = self.rng.choice(num_cols)
            cname = f"chk_{table.name}_{self.rng.randint(1, 1_000_000)}"
            stmts.append(f"ALTER TABLE {table.name} ADD CONSTRAINT {cname} CHECK ({col} >= 0)")

        def _add_unique() -> None:
            if len(existing_cols) == 0:
                return
            k = 1 if self.rng.random() < 0.7 else min(2, len(existing_cols))
            cols = self.rng.sample(existing_cols, k)
            cname = f"uk_{table.name}_{self.rng.randint(1, 1_000_000)}"
            cols_list = ", ".join(cols)
            stmts.append(f"ALTER TABLE {table.name} ADD CONSTRAINT {cname} UNIQUE ({cols_list})")

        actions = [_add_column, _alter_default, _add_check, _add_unique]
        for _ in range(max(0, max_alters)):
            self.rng.choice(actions)()

        return stmts

    def _generate_where_clause(self, columns: List[Column]) -> Optional[str]:
        """Generate WHERE clause for partial index"""
        bool_cols = [c.name for c in columns if "BOOL" in c.data_type and not c.data_type.endswith("[]")]
        if bool_cols:
            return f"{self.rng.choice(bool_cols)} = true"

        int_cols = [c.name for c in columns if "INT" in c.data_type and not c.data_type.endswith("[]")]
        if int_cols:
            return f"{self.rng.choice(int_cols)} > 0"

        varchar_cols = [c.name for c in columns if ("VARCHAR" in c.data_type or c.data_type.startswith("CHAR")) and not c.data_type.endswith("[]")]
        if varchar_cols:
            return f"{self.rng.choice(varchar_cols)} IS NOT NULL"

        return None