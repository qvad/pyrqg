"""
Enhanced DDL Generator for PyRQG
Supports complex constraints, composite keys, and realistic schemas
"""

from typing import List, Optional, Tuple, Dict
from dataclasses import dataclass, field
import random

@dataclass
class ColumnDefinition:
    """Enhanced column definition with constraints"""
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[str] = None
    check: Optional[str] = None
    unique: bool = False
    references: Optional[str] = None  # Foreign key reference
    on_delete: Optional[str] = None   # CASCADE, SET NULL, etc.
    on_update: Optional[str] = None
    comment: Optional[str] = None

@dataclass 
class TableConstraint:
    """Table-level constraint definition"""
    name: Optional[str]
    constraint_type: str  # PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY
    columns: List[str]
    check_expression: Optional[str] = None
    references_table: Optional[str] = None
    references_columns: Optional[List[str]] = None
    on_delete: Optional[str] = None
    on_update: Optional[str] = None
    deferrable: bool = False
    initially_deferred: bool = False
    nulls_not_distinct: bool = False  # PG15 feature for UNIQUE

@dataclass
class IndexDefinition:
    """Index definition"""
    name: str
    columns: List[str]
    unique: bool = False
    where_clause: Optional[str] = None
    include_columns: Optional[List[str]] = None
    method: str = "btree"  # btree, hash, gist, gin

@dataclass
class TableDefinition:
    """Enhanced table definition with all constraints"""
    name: str
    columns: List[ColumnDefinition]
    constraints: List[TableConstraint] = field(default_factory=list)
    indexes: List[IndexDefinition] = field(default_factory=list)
    tablespace: Optional[str] = None
    comment: Optional[str] = None
    partitioned_by: Optional[str] = None
    inherits: Optional[str] = None
    
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
        # Heavier weights for common OLTP types; include wide coverage for others.
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
        self._base_type_weights = self._apply_profile_weights(base_weights, profile)

    def _apply_profile_weights(self, base: List[Tuple], profile: str) -> List[Tuple]:
        # Copy weights to a mutable list
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
            # Flatten weights somewhat to increase diversity
            avg = max(1, int(sum(w for _, w in items) / len(items)))
            items = [(f, max(1, int((w + avg) / 2))) for (f, w) in items]
        else:
            # core: keep as-is (OLTP-heavy)
            pass
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
        # Occasionally wrap as array
        if self.rng.random() < 0.08 and not base.endswith("[]") and base not in ("JSON", "JSONB"):
            return f"{base}[]"
        return base
        
    def generate_column_definition(self, col: ColumnDefinition) -> str:
        """Generate column definition SQL"""
        parts = [col.name, col.data_type]
        
        if not col.nullable:
            parts.append("NOT NULL")
            
        if col.default is not None:
            parts.append(f"DEFAULT {col.default}")
            
        if col.unique:
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
    
    def generate_create_table(self, table: TableDefinition) -> str:
        """Generate CREATE TABLE statement"""
        lines = [f"CREATE TABLE IF NOT EXISTS {table.name} ("]
        
        # Add columns
        col_lines = []
        for col in table.columns:
            col_lines.append(f"    {self.generate_column_definition(col)}")
        
        # Add constraints
        for constraint in table.constraints:
            col_lines.append(f"    {self.generate_constraint_definition(constraint)}")
        
        lines.append(",\n".join(col_lines))
        lines.append(")")
        
        # Add table options
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
        
        # Add comment if specified
        if table.comment:
            sql += f";\nCOMMENT ON TABLE {table.name} IS '{table.comment}'"
        
        return sql
    
    def generate_create_index(self, table_name: str, index: IndexDefinition) -> str:
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
    
    def generate_sample_tables(self) -> List[TableDefinition]:
        """Generate sample complex table definitions"""
        tables = []
        
        # Users table with composite unique constraint
        users_table = TableDefinition(
            name="users",
            columns=[
                ColumnDefinition("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", nullable=False),
                ColumnDefinition("username", "VARCHAR(50)", nullable=False, unique=True),
                ColumnDefinition("email", "VARCHAR(100)", nullable=False),
                ColumnDefinition("first_name", "VARCHAR(50)", nullable=False),
                ColumnDefinition("last_name", "VARCHAR(50)", nullable=False),
                ColumnDefinition("age", "INTEGER", check="age >= 18 AND age <= 120"),
                ColumnDefinition("phone", "VARCHAR(20)"),
                ColumnDefinition("status", "VARCHAR(20)", nullable=False, default="'active'",
                               check="status IN ('active', 'inactive', 'suspended', 'deleted')"),
                ColumnDefinition("created_at", "TIMESTAMP", nullable=False, default="CURRENT_TIMESTAMP"),
                ColumnDefinition("updated_at", "TIMESTAMP", nullable=False, default="CURRENT_TIMESTAMP")
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id"]),
                TableConstraint("uk_users_email_status", "UNIQUE", ["email", "status"]),
                TableConstraint("chk_users_names", "CHECK", [], 
                               check_expression="first_name != last_name")
            ],
            indexes=[
                IndexDefinition("idx_users_email", ["email"]),
                IndexDefinition("idx_users_status_created", ["status", "created_at"]),
                IndexDefinition("idx_users_fullname", ["last_name", "first_name"])
            ]
        )
        tables.append(users_table)
        
        # Categories table referenced by products
        categories_table = TableDefinition(
            name="categories",
            columns=[
                ColumnDefinition("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", nullable=False),
                ColumnDefinition("name", "VARCHAR(200)", nullable=False, unique=True),
                ColumnDefinition("parent_id", "INTEGER")
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id"]),
                TableConstraint("fk_categories_parent", "FOREIGN KEY", ["parent_id"],
                               references_table="categories", references_columns=["id"],
                               on_delete="SET NULL")
            ],
            indexes=[
                IndexDefinition("idx_categories_parent", ["parent_id"]) 
            ]
        )
        tables.append(categories_table)
        
        # Addresses table referenced by orders
        addresses_table = TableDefinition(
            name="addresses",
            columns=[
                ColumnDefinition("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", nullable=False),
                ColumnDefinition("user_id", "INTEGER"),
                ColumnDefinition("line1", "VARCHAR(200)", nullable=False),
                ColumnDefinition("line2", "VARCHAR(200)"),
                ColumnDefinition("city", "VARCHAR(100)", nullable=False),
                ColumnDefinition("state", "VARCHAR(100)"),
                ColumnDefinition("postal_code", "VARCHAR(20)"),
                ColumnDefinition("country", "VARCHAR(100)", nullable=False)
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id"]),
                TableConstraint("fk_addresses_user", "FOREIGN KEY", ["user_id"],
                               references_table="users", references_columns=["id"],
                               on_delete="SET NULL")
            ],
            indexes=[
                IndexDefinition("idx_addresses_user", ["user_id"]) 
            ]
        )
        tables.append(addresses_table)
        
        # Products table with multiple constraints
        products_table = TableDefinition(
            name="products",
            columns=[
                ColumnDefinition("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", nullable=False),
                ColumnDefinition("sku", "VARCHAR(50)", nullable=False),
                ColumnDefinition("name", "VARCHAR(200)", nullable=False),
                ColumnDefinition("description", "TEXT"),
                ColumnDefinition("category_id", "INTEGER", nullable=False),
                ColumnDefinition("price", "DECIMAL(10,2)", nullable=False, check="price > 0"),
                ColumnDefinition("cost", "DECIMAL(10,2)", check="cost >= 0"),
                ColumnDefinition("quantity", "INTEGER", nullable=False, default="0", check="quantity >= 0"),
                ColumnDefinition("min_quantity", "INTEGER", default="0"),
                ColumnDefinition("max_quantity", "INTEGER"),
                ColumnDefinition("is_active", "BOOLEAN", nullable=False, default="true"),
                ColumnDefinition("created_by", "INTEGER", nullable=False),
                ColumnDefinition("created_at", "TIMESTAMP", nullable=False, default="CURRENT_TIMESTAMP")
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
                IndexDefinition("idx_products_category", ["category_id"]),
                IndexDefinition("idx_products_active_category", ["category_id", "is_active"],
                               where_clause="is_active = true"),
                IndexDefinition("idx_products_price", ["price"], include_columns=["name", "sku"])
            ]
        )
        tables.append(products_table)
        
        # Orders table with composite primary key
        orders_table = TableDefinition(
            name="orders",
            columns=[
                ColumnDefinition("order_date", "DATE", nullable=False),
                ColumnDefinition("order_number", "INTEGER", nullable=False),
                ColumnDefinition("customer_id", "INTEGER", nullable=False),
                ColumnDefinition("status", "VARCHAR(20)", nullable=False, default="'pending'"),
                ColumnDefinition("total_amount", "DECIMAL(12,2)", nullable=False, check="total_amount >= 0"),
                ColumnDefinition("tax_amount", "DECIMAL(10,2)", default="0"),
                ColumnDefinition("discount_amount", "DECIMAL(10,2)", default="0"),
                ColumnDefinition("shipping_address_id", "INTEGER"),
                ColumnDefinition("billing_address_id", "INTEGER"),
                ColumnDefinition("notes", "TEXT"),
                ColumnDefinition("created_at", "TIMESTAMP", nullable=False, default="CURRENT_TIMESTAMP"),
                ColumnDefinition("updated_at", "TIMESTAMP", nullable=False, default="CURRENT_TIMESTAMP")
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
                IndexDefinition("idx_orders_customer_date", ["customer_id", "order_date"]),
                IndexDefinition("idx_orders_status", ["status"], where_clause="status != 'completed'"),
                IndexDefinition("uk_orders_date_customer", ["order_date", "customer_id", "order_number"], 
                               unique=True)
            ]
        )
        tables.append(orders_table)
        
        # Order items with composite foreign key
        order_items_table = TableDefinition(
            name="order_items",
            columns=[
                ColumnDefinition("order_date", "DATE", nullable=False),
                ColumnDefinition("order_number", "INTEGER", nullable=False),
                ColumnDefinition("line_number", "INTEGER", nullable=False),
                ColumnDefinition("product_id", "INTEGER", nullable=False),
                ColumnDefinition("quantity", "INTEGER", nullable=False, check="quantity > 0"),
                ColumnDefinition("unit_price", "DECIMAL(10,2)", nullable=False, check="unit_price >= 0"),
                ColumnDefinition("discount_percent", "DECIMAL(5,2)", default="0",
                               check="discount_percent >= 0 AND discount_percent <= 100"),
                ColumnDefinition("tax_rate", "DECIMAL(5,2)", default="0",
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
                IndexDefinition("idx_order_items_product", ["product_id"]),
                IndexDefinition("idx_order_items_order", ["order_date", "order_number"])
            ]
        )
        tables.append(order_items_table)
        
        # Audit log table with partial indexes
        audit_log_table = TableDefinition(
            name="audit_log",
            columns=[
                ColumnDefinition("id", "BIGINT GENERATED BY DEFAULT AS IDENTITY", nullable=False),
                ColumnDefinition("table_name", "VARCHAR(50)", nullable=False),
                ColumnDefinition("record_id", "VARCHAR(100)", nullable=False),
                ColumnDefinition("action", "VARCHAR(10)", nullable=False,
                               check="action IN ('INSERT', 'UPDATE', 'DELETE')"),
                ColumnDefinition("user_id", "INTEGER"),
                ColumnDefinition("changed_data", "JSONB"),
                ColumnDefinition("ip_address", "INET"),
                ColumnDefinition("user_agent", "TEXT"),
                ColumnDefinition("created_at", "TIMESTAMP", nullable=False, default="CURRENT_TIMESTAMP")
            ],
            constraints=[
                TableConstraint(None, "PRIMARY KEY", ["id", "created_at"]),
                TableConstraint("fk_audit_user", "FOREIGN KEY", ["user_id"],
                               references_table="users", references_columns=["id"],
                               on_delete="SET NULL", deferrable=True)
            ],
            indexes=[
                IndexDefinition("idx_audit_table_record", ["table_name", "record_id"]),
                IndexDefinition("idx_audit_user_date", ["user_id", "created_at"]),
                IndexDefinition("idx_audit_recent_deletes", ["table_name", "created_at"],
                               where_clause="action = 'DELETE' AND created_at >= DATE '2000-01-01'"),
                IndexDefinition("idx_audit_data_gin", ["changed_data"], method="gin")
            ],
            partitioned_by="RANGE (created_at)"
        )
        tables.append(audit_log_table)
        
        return tables
    
    def generate_random_table(self, table_name: str, 
                            num_columns: int = None,
                            num_constraints: int = None) -> TableDefinition:
        """Generate a random table with complex constraints"""
        if num_columns is None:
            num_columns = self.rng.randint(5, 15)
        if num_constraints is None:
            num_constraints = self.rng.randint(2, 6)
        
        # Generate columns
        columns = []
        column_names = []
        
        # Always have an ID column (PG15 identity)
        columns.append(ColumnDefinition("id", "INTEGER GENERATED BY DEFAULT AS IDENTITY", nullable=False))
        column_names.append("id")
        
        # Generate other columns
        for i in range(num_columns - 1):
            col_name = f"col_{self.rng.choice(['data', 'value', 'info', 'attr'])}_{i}"
            col_type = self._random_data_type()
            
            col = ColumnDefinition(
                name=col_name,
                data_type=col_type,
                nullable=self.rng.choice([True, True, False]),
                unique=self.rng.random() < 0.1,
                default=self._generate_default(col_type) if self.rng.random() < 0.3 else None,
                check=self._generate_check(col_name, col_type) if self.rng.random() < 0.2 else None
            )
            columns.append(col)
            column_names.append(col_name)
        
        # Optionally add a self-referencing FK column
        if self.rng.random() < 0.3:
            columns.append(ColumnDefinition("parent_id", "INTEGER", nullable=True))
            column_names.append("parent_id")
        
        # Generate constraints
        constraints = []
        
        # Primary key
        if self.rng.random() < 0.8:
            # Single column PK
            constraints.append(TableConstraint(None, "PRIMARY KEY", ["id"]))
        else:
            # Composite PK
            pk_cols = self.rng.sample(column_names[:5], self.rng.randint(2, 3))
            constraints.append(TableConstraint(f"pk_{table_name}", "PRIMARY KEY", pk_cols))
        
        # Respect the requested number of constraints if provided
        remaining = max(num_constraints - 1, 0)  # account for the PK already added

        # Unique constraints
        if remaining > 0:
            for i in range(self.rng.randint(0, min(2, remaining))):
                unique_cols = self.rng.sample(column_names[1:], self.rng.randint(1, 3))
                constraints.append(
                    TableConstraint(f"uk_{table_name}_{i}", "UNIQUE", unique_cols, nulls_not_distinct=(self.rng.random() < 0.5))
                )
                remaining -= 1
                if remaining == 0:
                    break
        
        # Check constraints
        if remaining > 0:
            for i in range(self.rng.randint(0, min(2, remaining))):
                check_expr = self._generate_table_check(columns)
                if check_expr:
                    constraints.append(
                        TableConstraint(f"chk_{table_name}_{i}", "CHECK", [], check_expression=check_expr)
                    )
                    remaining -= 1
                    if remaining == 0:
                        break
        
        # Optional self-referencing FK constraint (counts towards constraints)
        if remaining > 0 and any(c.name == "parent_id" for c in columns) and self.rng.random() < 0.9:
            constraints.append(
                TableConstraint(f"fk_{table_name}_parent", "FOREIGN KEY", ["parent_id"],
                                references_table=table_name, references_columns=["id"],
                                on_delete=self.rng.choice(["SET NULL", "RESTRICT", "CASCADE"]),
                                deferrable=(self.rng.random() < 0.2), initially_deferred=(self.rng.random() < 0.5))
            )
            remaining -= 1
        
        # Generate indexes (density and composite controlled by knobs)
        indexes = []
        max_idx = max(0, int(1 + round(4 * self.index_ratio)))
        for i in range(self.rng.randint(0, max_idx)):
            kcols = 1 if self.rng.random() > self.composite_index_ratio else self.rng.randint(2, min(3, len(column_names)))
            idx_cols = self.rng.sample(column_names, kcols)
            index = IndexDefinition(
                name=f"idx_{table_name}_{i}",
                columns=idx_cols,
                unique=self.rng.random() < 0.1,
                where_clause=self._generate_where_clause(columns) if (self.rng.random() < self.partial_index_ratio) else None
            )
            indexes.append(index)
        
        return TableDefinition(
            name=table_name,
            columns=columns,
            constraints=constraints,
            indexes=indexes
        )
    
    def _generate_default(self, data_type: str) -> Optional[str]:
        """Generate appropriate default value for data type"""
        if "INT" in data_type and not data_type.endswith("[]"):
            return str(self.rng.randint(0, 100))
        elif "VARCHAR" in data_type or "TEXT" in data_type or data_type.startswith("CHAR"):
            return "'default'"
        elif "BOOL" in data_type:
            return self.rng.choice(["true", "false"])
        elif data_type in ("TIMESTAMPTZ", "TSRANGE", "TSTZRANGE") or "TIMESTAMP" in data_type:
            return "CURRENT_TIMESTAMP"
        elif "DATE" in data_type and "RANGE" not in data_type:
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
        elif data_type.endswith("[]"):
            base = data_type[:-2]
            if base in ("INTEGER", "BIGINT", "SMALLINT"):
                return "ARRAY[1,2,3]"
            elif base.startswith("VARCHAR") or base == "TEXT":
                return "ARRAY['a','b']"
            elif base in ("NUMERIC", "DECIMAL"):
                return "ARRAY[1.0,2.0]"
            else:
                return None
        return None
    
    def _generate_check(self, col_name: str, data_type: str) -> Optional[str]:
        """Generate check constraint for column"""
        if "INT" in data_type and not data_type.endswith("[]"):
            return f"{col_name} >= 0"
        elif "VARCHAR" in data_type or data_type.startswith("CHAR"):
            return f"LENGTH({col_name}) > 0"
        elif "DECIMAL" in data_type or "NUMERIC" in data_type:
            return f"{col_name} >= 0"
        elif data_type.endswith("[]"):
            return f"array_length({col_name}, 1) >= 0"
        return None
    
    def _generate_table_check(self, columns: List[ColumnDefinition]) -> Optional[str]:
        """Generate table-level check constraint"""
        numeric_cols = [c.name for c in columns if "INT" in c.data_type or "DECIMAL" in c.data_type]
        if len(numeric_cols) >= 2:
            return f"{numeric_cols[0]} <= {numeric_cols[1]}"
        return None
    
    def _generate_where_clause(self, columns: List[ColumnDefinition]) -> Optional[str]:
        """Generate WHERE clause for partial index"""
        bool_cols = [c.name for c in columns if "BOOL" in c.data_type]
        if bool_cols:
            return f"{bool_cols[0]} = true"
        
        varchar_cols = [c.name for c in columns if ("VARCHAR" in c.data_type or c.data_type.startswith("CHAR"))]
        if varchar_cols:
            return f"{varchar_cols[0]} IS NOT NULL"
        
        json_cols = [c.name for c in columns if c.data_type in ("JSONB", "JSON")]
        if json_cols:
            return f"{json_cols[0]} IS NOT NULL"

        return None
    
    def generate_schema(self, num_tables: int = 5) -> List[str]:
        """Generate complete schema with multiple related tables"""
        ddl_statements = []
        
        # Generate mix of predefined and random tables (keep dependency order)
        base_tables = self.generate_sample_tables()
        if num_tables <= len(base_tables):
            tables = list(base_tables[:num_tables])
        else:
            tables = list(base_tables)
            # Add random tables
            for i in range(num_tables - len(base_tables)):
                table = self.generate_random_table(f"table_{i}")
                tables.append(table)
        
        # Generate CREATE statements
        for table in tables:
            ddl_statements.append(self.generate_create_table(table))
            if table.partitioned_by:
                ddl_statements.append(
                    f"CREATE TABLE IF NOT EXISTS {table.name}_default PARTITION OF {table.name} DEFAULT"
                )
            
            # Add indexes
            for index in table.indexes:
                ddl_statements.append(self.generate_create_index(table.name, index))
        # Add cross-table foreign keys per fk_ratio
        ddl_statements.extend(self._generate_cross_table_fks(tables))
        return ddl_statements

    def _generate_cross_table_fks(self, tables: List[TableDefinition]) -> List[str]:
        if self.fk_ratio <= 0.0 or len(tables) < 2:
            return []
        # Build map of candidate referenced tables (prefer PK id)
        tmeta: Dict[str, Dict[str, str]] = {}
        for t in tables:
            pk_cols = [c for c in t.columns if c.name == 'id']
            if pk_cols:
                tmeta[t.name] = {"pk": pk_cols[0].data_type}
        if not tmeta:
            return []
        out: List[str] = []
        for t in tables:
            # probability based on fk_ratio to add 0..2 FKs to other tables
            if self.rng.random() > self.fk_ratio:
                continue
            n = 1 if self.rng.random() < 0.7 else 2
            for _ in range(n):
                ref_table = self.rng.choice([n for n in tmeta.keys() if n != t.name]) if len(tmeta) > 1 else None
                if not ref_table:
                    continue
                # Find/create a referring column compatible with referenced pk type
                ref_type = tmeta[ref_table]["pk"]
                # Pick or add a column of that type
                candidates = [c.name for c in t.columns if c.data_type.split('(')[0] == ref_type.split('(')[0] and c.name != 'id']
                if candidates and self.rng.random() < 0.7:
                    col = self.rng.choice(candidates)
                else:
                    # Add a new nullable column
                    base = f"{ref_table}_id"
                    new_name = base
                    suffix = 2
                    existing = {c.name for c in t.columns}
                    while new_name in existing:
                        new_name = f"{base}_{suffix}"
                        suffix += 1
                    t.columns.append(ColumnDefinition(new_name, ref_type, nullable=True))
                    col = new_name
                    # Also emit ALTER ADD COLUMN since CREATE TABLE already emitted
                    out.append(f"ALTER TABLE {t.name} ADD COLUMN {col} {ref_type}")
                cname = f"fk_{t.name}_{ref_table}_{self.rng.randint(1, 1_000_000)}"
                action = self.rng.choice(["RESTRICT", "SET NULL", "CASCADE"])
                out.append(
                    f"ALTER TABLE {t.name} ADD CONSTRAINT {cname} FOREIGN KEY ({col}) REFERENCES {ref_table}(id) ON DELETE {action}"
                )
        return out

    def generate_alter_table_statements(self, table: TableDefinition, max_alters: int = 3) -> List[str]:
        """Generate safe ALTER TABLE statements for an existing table.
        Focus on non-destructive, always-compilable operations:
         - ADD COLUMN (nullable, with optional DEFAULT)
         - ALTER COLUMN SET/DROP DEFAULT
         - ADD CHECK constraint
         - ADD UNIQUE constraint (on empty tables this compiles safely)
        Mutates the provided table definition when adding columns so callers can keep metadata in sync.
        """
        stmts: List[str] = []

        # Helper: pick existing non-PK, non-identity column names
        existing_cols = [c.name for c in table.columns]
        pk_cols = set()
        for con in table.constraints:
            if con.constraint_type == "PRIMARY KEY":
                for c in con.columns:
                    pk_cols.add(c)

        # 1. ADD COLUMN operations
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
            # Keep columns nullable for safety
            if default is not None:
                parts.append(f"DEFAULT {default}")
            stmt = " ".join(parts)
            stmts.append(stmt)
            # Mutate table definition so callers can use new column in metadata
            table.columns.append(ColumnDefinition(name=new_name, data_type=dtype, nullable=True, default=default))
            existing_cols.append(new_name)

        # 2. ALTER COLUMN SET/DROP DEFAULT on existing non-PK columns
        def _alter_default() -> None:
            candidates = [c for c in table.columns if c.name not in pk_cols]
            if not candidates:
                return
            col = self.rng.choice(candidates)
            if col.default is None and self.rng.random() < 0.7:
                # SET DEFAULT
                new_def = self._generate_default(col.data_type)
                if new_def is None:
                    return
                stmts.append(f"ALTER TABLE {table.name} ALTER COLUMN {col.name} SET DEFAULT {new_def}")
                col.default = new_def
            else:
                # DROP DEFAULT
                stmts.append(f"ALTER TABLE {table.name} ALTER COLUMN {col.name} DROP DEFAULT")
                col.default = None

        # 3. ADD CHECK constraint on a numeric column
        def _add_check() -> None:
            num_cols = [c.name for c in table.columns if ("INT" in c.data_type or "DECIMAL" in c.data_type or "NUMERIC" in c.data_type)]
            if not num_cols:
                return
            col = self.rng.choice(num_cols)
            cname = f"chk_{table.name}_{self.rng.randint(1, 1_000_000)}"
            stmts.append(f"ALTER TABLE {table.name} ADD CONSTRAINT {cname} CHECK ({col} >= 0)")

        # 4. ADD UNIQUE constraint on 1-2 columns (safe on empty tables)
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
