"""
Enhanced DDL Generator for PyRQG
Supports complex constraints, composite keys, and realistic schemas
"""

from typing import List, Dict, Optional, Any, Union
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
    
    def __init__(self, dialect: str = "postgresql"):
        self.dialect = dialect
        self.generated_tables = []
        
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
        lines = [f"CREATE TABLE {table.name} ("]
        
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
        sql = f"CREATE {unique}INDEX {index.name} ON {table_name}"
        
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
                ColumnDefinition("id", "SERIAL", nullable=False),
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
        
        # Products table with multiple constraints
        products_table = TableDefinition(
            name="products",
            columns=[
                ColumnDefinition("id", "SERIAL", nullable=False),
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
                ColumnDefinition("id", "BIGSERIAL", nullable=False),
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
                TableConstraint(None, "PRIMARY KEY", ["id"]),
                TableConstraint("fk_audit_user", "FOREIGN KEY", ["user_id"],
                               references_table="users", references_columns=["id"],
                               on_delete="SET NULL", deferrable=True)
            ],
            indexes=[
                IndexDefinition("idx_audit_table_record", ["table_name", "record_id"]),
                IndexDefinition("idx_audit_user_date", ["user_id", "created_at"]),
                IndexDefinition("idx_audit_recent_deletes", ["table_name", "created_at"],
                               where_clause="action = 'DELETE' AND created_at > CURRENT_DATE - INTERVAL '30 days'"),
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
            num_columns = random.randint(5, 15)
        if num_constraints is None:
            num_constraints = random.randint(2, 6)
        
        # Column types
        data_types = [
            "INTEGER", "BIGINT", "SMALLINT",
            "VARCHAR(50)", "VARCHAR(100)", "VARCHAR(200)", "TEXT",
            "DECIMAL(10,2)", "NUMERIC(12,4)", "REAL", "DOUBLE PRECISION",
            "BOOLEAN", "DATE", "TIMESTAMP", "TIME",
            "UUID", "JSONB", "ARRAY[INTEGER]", "INET", "MACADDR"
        ]
        
        # Generate columns
        columns = []
        column_names = []
        
        # Always have an ID column
        columns.append(ColumnDefinition("id", "SERIAL", nullable=False))
        column_names.append("id")
        
        # Generate other columns
        for i in range(num_columns - 1):
            col_name = f"col_{random.choice(['data', 'value', 'info', 'attr'])}_{i}"
            col_type = random.choice(data_types)
            
            col = ColumnDefinition(
                name=col_name,
                data_type=col_type,
                nullable=random.choice([True, True, False]),
                unique=random.random() < 0.1,
                default=self._generate_default(col_type) if random.random() < 0.3 else None,
                check=self._generate_check(col_name, col_type) if random.random() < 0.2 else None
            )
            columns.append(col)
            column_names.append(col_name)
        
        # Generate constraints
        constraints = []
        
        # Primary key
        if random.random() < 0.8:
            # Single column PK
            constraints.append(TableConstraint(None, "PRIMARY KEY", ["id"]))
        else:
            # Composite PK
            pk_cols = random.sample(column_names[:5], random.randint(2, 3))
            constraints.append(TableConstraint(f"pk_{table_name}", "PRIMARY KEY", pk_cols))
        
        # Unique constraints
        for i in range(random.randint(0, 2)):
            unique_cols = random.sample(column_names[1:], random.randint(1, 3))
            constraints.append(
                TableConstraint(f"uk_{table_name}_{i}", "UNIQUE", unique_cols)
            )
        
        # Check constraints
        for i in range(random.randint(0, 2)):
            check_expr = self._generate_table_check(columns)
            if check_expr:
                constraints.append(
                    TableConstraint(f"chk_{table_name}_{i}", "CHECK", [], check_expression=check_expr)
                )
        
        # Generate indexes
        indexes = []
        for i in range(random.randint(1, 4)):
            idx_cols = random.sample(column_names, random.randint(1, 3))
            index = IndexDefinition(
                name=f"idx_{table_name}_{i}",
                columns=idx_cols,
                unique=random.random() < 0.1,
                where_clause=self._generate_where_clause(columns) if random.random() < 0.3 else None
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
        if "INT" in data_type:
            return str(random.randint(0, 100))
        elif "VARCHAR" in data_type or "TEXT" in data_type:
            return "'default'"
        elif "BOOL" in data_type:
            return random.choice(["true", "false"])
        elif "TIMESTAMP" in data_type:
            return "CURRENT_TIMESTAMP"
        elif "DATE" in data_type:
            return "CURRENT_DATE"
        elif "DECIMAL" in data_type or "NUMERIC" in data_type:
            return "0.00"
        return None
    
    def _generate_check(self, col_name: str, data_type: str) -> Optional[str]:
        """Generate check constraint for column"""
        if "INT" in data_type:
            return f"{col_name} >= 0"
        elif "VARCHAR" in data_type:
            return f"LENGTH({col_name}) > 0"
        elif "DECIMAL" in data_type:
            return f"{col_name} >= 0"
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
        
        varchar_cols = [c.name for c in columns if "VARCHAR" in c.data_type]
        if varchar_cols:
            return f"{varchar_cols[0]} IS NOT NULL"
        
        return None
    
    def generate_schema(self, num_tables: int = 5) -> List[str]:
        """Generate complete schema with multiple related tables"""
        ddl_statements = []
        
        # Generate mix of predefined and random tables
        tables = self.generate_sample_tables()[:3]  # Use some predefined
        
        # Add random tables
        for i in range(num_tables - 3):
            table = self.generate_random_table(f"table_{i}")
            tables.append(table)
        
        # Generate CREATE statements
        for table in tables:
            ddl_statements.append(self.generate_create_table(table))
            
            # Add indexes
            for index in table.indexes:
                ddl_statements.append(self.generate_create_index(table.name, index))
        
        return ddl_statements

# Example usage
if __name__ == "__main__":
    generator = DDLGenerator()
    
    # Generate sample complex tables
    print("=== Complex DDL Examples ===\n")
    
    for table in generator.generate_sample_tables()[:2]:
        print(generator.generate_create_table(table))
        print()
        
        for index in table.indexes:
            print(generator.generate_create_index(table.name, index) + ";")
        print("\n" + "="*60 + "\n")