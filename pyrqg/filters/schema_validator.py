"""
Schema validator to check queries against PostgreSQL schema
"""

from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
import re


@dataclass 
class ColumnInfo:
    """Information about a database column"""
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_unique: bool = False
    default_value: Optional[str] = None


@dataclass
class TableInfo:
    """Information about a database table"""
    name: str
    columns: Dict[str, ColumnInfo]
    primary_key: Optional[str] = None
    unique_keys: List[str] = None
    foreign_keys: Dict[str, str] = None


class SchemaValidator:
    """Validates queries against database schema"""
    
    def __init__(self):
        self.tables = self._load_schema()
        
        # Common column aliases that map to real columns
        self.column_aliases = {
            'id': ['user_id', 'product_id', 'order_id', 'customer_id'],
            'name': ['first_name', 'last_name', 'username'],
            'amount': ['total_amount', 'balance', 'price'],
            'date': ['created_at', 'updated_at', 'order_date'],
            'status': ['is_active', 'active'],
        }
        
        # Data type mappings
        self.type_groups = {
            'numeric': ['INTEGER', 'BIGINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE PRECISION', 'SERIAL'],
            'text': ['VARCHAR', 'TEXT', 'CHAR'],
            'timestamp': ['TIMESTAMP', 'DATE', 'TIME'],
            'boolean': ['BOOLEAN'],
            'json': ['JSONB', 'JSON']
        }
    
    def _load_schema(self) -> Dict[str, TableInfo]:
        """Load the PostgreSQL schema"""
        # Import schema from postgres_schema.py
        from pyrqg.schemas.postgres_schema import get_table_columns
        
        # Build comprehensive schema
        tables = {}
        
        # Define main tables with all columns
        table_definitions = {
            'users': self._get_user_columns(),
            'products': self._get_product_columns(),
            'orders': self._get_order_columns(),
            'inventory': self._get_inventory_columns(),
            'transactions': self._get_transaction_columns(),
            'sessions': self._get_session_columns(),
            'employees': self._get_employee_columns(),
            'customers': self._get_customer_columns(),
            'accounts': self._get_account_columns(),
            'logs': self._get_log_columns(),
            'analytics': self._get_analytics_columns(),
            'departments': self._get_department_columns(),
            'suppliers': self._get_supplier_columns(),
            'sales': self._get_sales_columns(),
            'cache': self._get_cache_columns(),
            'metrics': self._get_metrics_columns(),
            'settings': self._get_settings_columns(),
            'notifications': self._get_notification_columns(),
            'temp_data': self._get_temp_data_columns(),
            'audit_logs': self._get_audit_log_columns(),
            'expired_tokens': self._get_expired_token_columns(),
            'deleted_items': self._get_deleted_item_columns(),
            'archive': self._get_archive_columns(),
        }
        
        # Add YugabyteDB compatibility tables
        yugabyte_tables = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 
                          'N', 'O', 'P', 'AA', 'BB', 'CC', 'DD', 'EE', 'FF', 'GG', 'HH', 
                          'II', 'JJ', 'KK', 'LL', 'MM', 'NN', 'OO', 'PP', 'AAA', 'BBB', 
                          'CCC', 'DDD']
        
        for table in yugabyte_tables:
            table_definitions[table] = self._get_yugabyte_columns()
        
        # Create TableInfo objects
        for table_name, columns in table_definitions.items():
            table_info = TableInfo(
                name=table_name,
                columns={col.name: col for col in columns}
            )
            
            # Set primary keys
            for col in columns:
                if col.is_primary_key:
                    table_info.primary_key = col.name
                    break
            
            tables[table_name] = table_info
        
        return tables
    
    def validate_query(self, query: str, query_info) -> List[str]:
        """Validate a query against the schema"""
        errors = []
        
        # Validate tables
        for table in query_info.tables:
            if table not in self.tables:
                errors.append(f"Table '{table}' does not exist")
        
        # Validate columns
        for table, columns in query_info.columns.items():
            if table not in self.tables:
                continue
                
            table_info = self.tables[table]
            for column in columns:
                if column == '*':
                    continue
                    
                # Check exact match
                if column not in table_info.columns:
                    # Try to find similar column
                    similar = self._find_similar_column(column, table_info)
                    if similar:
                        errors.append(f"Column '{column}' not found in table '{table}'. Did you mean '{similar}'?")
                    else:
                        errors.append(f"Column '{column}' does not exist in table '{table}'")
        
        return errors
    
    def fix_column_references(self, query: str, query_info) -> str:
        """Fix column references in a query"""
        fixed_query = query
        
        for table, columns in query_info.columns.items():
            if table not in self.tables:
                continue
                
            table_info = self.tables[table]
            for column in columns:
                if column == '*' or column in table_info.columns:
                    continue
                
                # Find replacement column
                replacement = self._find_replacement_column(column, table_info)
                if replacement:
                    # Replace column reference
                    # Use word boundaries to avoid partial replacements
                    pattern = r'\b' + re.escape(column) + r'\b'
                    fixed_query = re.sub(pattern, replacement, fixed_query)
        
        return fixed_query
    
    def _find_similar_column(self, column: str, table_info: TableInfo) -> Optional[str]:
        """Find a similar column name in the table"""
        column_lower = column.lower()
        
        # Exact match (case insensitive)
        for col_name in table_info.columns:
            if col_name.lower() == column_lower:
                return col_name
        
        # Check aliases
        for alias, real_columns in self.column_aliases.items():
            if column_lower == alias:
                for real_col in real_columns:
                    if real_col in table_info.columns:
                        return real_col
        
        # Partial match
        for col_name in table_info.columns:
            if column_lower in col_name.lower() or col_name.lower() in column_lower:
                return col_name
        
        return None
    
    def _find_replacement_column(self, column: str, table_info: TableInfo) -> Optional[str]:
        """Find a replacement column for fixing queries"""
        # First try to find similar column
        similar = self._find_similar_column(column, table_info)
        if similar:
            return similar
        
        # If not found, use a default column based on context
        column_lower = column.lower()
        
        # Common replacements
        if 'id' in column_lower:
            return 'id'
        elif 'name' in column_lower:
            return 'name'
        elif 'email' in column_lower:
            return 'email'
        elif 'status' in column_lower:
            return 'status'
        elif 'amount' in column_lower or 'price' in column_lower:
            return 'price' if 'price' in table_info.columns else 'amount'
        elif 'date' in column_lower or 'time' in column_lower:
            return 'created_at'
        elif 'quantity' in column_lower or 'qty' in column_lower:
            return 'quantity'
        else:
            # Return first text column as fallback
            for col_name, col_info in table_info.columns.items():
                if col_info.data_type in ['VARCHAR', 'TEXT']:
                    return col_name
        
        return None
    
    def _get_user_columns(self) -> List[ColumnInfo]:
        """Get column definitions for users table"""
        return [
            ColumnInfo('id', 'SERIAL', False, True),
            ColumnInfo('user_id', 'INTEGER'),
            ColumnInfo('customer_id', 'INTEGER'),
            ColumnInfo('employee_id', 'INTEGER'),
            ColumnInfo('email', 'VARCHAR(255)'),
            ColumnInfo('name', 'VARCHAR(255)'),
            ColumnInfo('first_name', 'VARCHAR(100)'),
            ColumnInfo('last_name', 'VARCHAR(100)'),
            ColumnInfo('username', 'VARCHAR(100)'),
            ColumnInfo('status', 'VARCHAR(50)', default_value='active'),
            ColumnInfo('type', 'VARCHAR(50)'),
            ColumnInfo('role', 'VARCHAR(50)'),
            ColumnInfo('category', 'VARCHAR(100)'),
            ColumnInfo('tags', 'TEXT[]'),
            ColumnInfo('notes', 'TEXT'),
            ColumnInfo('description', 'TEXT'),
            ColumnInfo('address', 'TEXT'),
            ColumnInfo('city', 'VARCHAR(100)'),
            ColumnInfo('country', 'VARCHAR(100)'),
            ColumnInfo('phone', 'VARCHAR(50)'),
            ColumnInfo('age', 'INTEGER'),
            ColumnInfo('balance', 'DECIMAL(10,2)', default_value='0'),
            ColumnInfo('total', 'DECIMAL(10,2)', default_value='0'),
            ColumnInfo('total_amount', 'DECIMAL(10,2)', default_value='0'),
            ColumnInfo('price', 'DECIMAL(10,2)'),
            ColumnInfo('quantity', 'INTEGER', default_value='0'),
            ColumnInfo('score', 'INTEGER', default_value='0'),
            ColumnInfo('rating', 'INTEGER'),
            ColumnInfo('count', 'INTEGER', default_value='0'),
            ColumnInfo('retry_count', 'INTEGER', default_value='0'),
            ColumnInfo('version', 'INTEGER', default_value='1'),
            ColumnInfo('visit_count', 'INTEGER', default_value='0'),
            ColumnInfo('priority', 'INTEGER', default_value='0'),
            ColumnInfo('level', 'INTEGER', default_value='1'),
            ColumnInfo('is_active', 'BOOLEAN', default_value='true'),
            ColumnInfo('is_deleted', 'BOOLEAN', default_value='false'),
            ColumnInfo('is_verified', 'BOOLEAN', default_value='false'),
            ColumnInfo('active', 'BOOLEAN', default_value='true'),
            ColumnInfo('deleted', 'BOOLEAN', default_value='false'),
            ColumnInfo('locked', 'BOOLEAN', default_value='false'),
            ColumnInfo('created_at', 'TIMESTAMP', default_value='CURRENT_TIMESTAMP'),
            ColumnInfo('updated_at', 'TIMESTAMP', default_value='CURRENT_TIMESTAMP'),
            ColumnInfo('deleted_at', 'TIMESTAMP'),
            ColumnInfo('modified_at', 'TIMESTAMP'),
            ColumnInfo('last_login', 'TIMESTAMP'),
            ColumnInfo('last_accessed', 'TIMESTAMP'),
            ColumnInfo('last_updated', 'TIMESTAMP'),
            ColumnInfo('expires_at', 'TIMESTAMP'),
            ColumnInfo('expiry_date', 'TIMESTAMP'),
            ColumnInfo('hire_date', 'DATE'),
            ColumnInfo('order_date', 'TIMESTAMP'),
            ColumnInfo('timestamp', 'TIMESTAMP', default_value='CURRENT_TIMESTAMP'),
            ColumnInfo('data', 'JSONB', default_value='{}'),
            ColumnInfo('metadata', 'JSONB', default_value='{}'),
            ColumnInfo('properties', 'JSONB', default_value='{}'),
            ColumnInfo('settings', 'JSONB', default_value='{}'),
            ColumnInfo('product_id', 'INTEGER'),
            ColumnInfo('order_id', 'INTEGER'),
            ColumnInfo('manager_id', 'INTEGER'),
            ColumnInfo('department_id', 'INTEGER'),
            ColumnInfo('warehouse_id', 'INTEGER'),
            ColumnInfo('session_id', 'VARCHAR(100)'),
            ColumnInfo('api_key', 'VARCHAR(255)'),
            ColumnInfo('amount', 'DECIMAL(10,2)'),
            ColumnInfo('salary', 'DECIMAL(10,2)'),
            ColumnInfo('discount', 'DECIMAL(10,2)'),
            ColumnInfo('shipping_address', 'TEXT'),
            ColumnInfo('billing_address', 'TEXT'),
            ColumnInfo('modified_by', 'VARCHAR(100)'),
            ColumnInfo('product_code', 'VARCHAR(50)'),
            ColumnInfo('unit_price', 'DECIMAL(10,2)'),
            ColumnInfo('stock_quantity', 'INTEGER'),
            ColumnInfo('transaction_id', 'INTEGER'),
            ColumnInfo('account_id', 'INTEGER'),
            ColumnInfo('transaction_type', 'VARCHAR(50)'),
            ColumnInfo('supplier', 'VARCHAR(100)'),
            ColumnInfo('manufacturer', 'VARCHAR(100)'),
            ColumnInfo('barcode', 'VARCHAR(100)'),
            ColumnInfo('sku', 'VARCHAR(100)'),
            ColumnInfo('location', 'VARCHAR(100)'),
            ColumnInfo('payment_method', 'VARCHAR(50)'),
            ColumnInfo('fee', 'DECIMAL(10,2)'),
            ColumnInfo('tax', 'DECIMAL(10,2)'),
            ColumnInfo('balance_before', 'DECIMAL(10,2)'),
            ColumnInfo('balance_after', 'DECIMAL(10,2)'),
            ColumnInfo('processed_at', 'TIMESTAMP'),
            ColumnInfo('completed_at', 'TIMESTAMP'),
            ColumnInfo('account_number', 'VARCHAR(50)'),
            ColumnInfo('account_type', 'VARCHAR(50)'),
            ColumnInfo('last_transaction_date', 'TIMESTAMP'),
            ColumnInfo('shipped_date', 'TIMESTAMP'),
            ColumnInfo('delivered_date', 'TIMESTAMP'),
            ColumnInfo('shipping_cost', 'DECIMAL(10,2)'),
            ColumnInfo('items', 'JSONB', default_value='[]'),
            ColumnInfo('reserved_quantity', 'INTEGER', default_value='0'),
            ColumnInfo('available_quantity', 'INTEGER', default_value='0'),
            ColumnInfo('cost', 'DECIMAL(10,2)'),
            ColumnInfo('log_level', 'VARCHAR(20)'),
            ColumnInfo('message', 'TEXT'),
            ColumnInfo('metric_name', 'VARCHAR(100)'),
            ColumnInfo('metric_value', 'DECIMAL(10,2)'),
            ColumnInfo('event_name', 'VARCHAR(100)'),
            ColumnInfo('event_type', 'VARCHAR(50)'),
            ColumnInfo('key', 'VARCHAR(255)'),
            ColumnInfo('value', 'TEXT'),
            ColumnInfo('setting_key', 'VARCHAR(100)'),
            ColumnInfo('setting_value', 'TEXT'),
            ColumnInfo('sale_id', 'INTEGER'),
            ColumnInfo('sale_date', 'TIMESTAMP'),
            ColumnInfo('metric_id', 'INTEGER'),
            ColumnInfo('setting_id', 'INTEGER'),
            ColumnInfo('cache_id', 'INTEGER'),
            ColumnInfo('log_id', 'INTEGER'),
            ColumnInfo('event_id', 'INTEGER'),
            ColumnInfo('notification_id', 'INTEGER'),
            ColumnInfo('token', 'VARCHAR(255)'),
            ColumnInfo('expired_at', 'TIMESTAMP'),
            ColumnInfo('item_type', 'VARCHAR(50)'),
            ColumnInfo('item_id', 'INTEGER'),
            ColumnInfo('source_table', 'VARCHAR(50)'),
            ColumnInfo('source_id', 'INTEGER'),
            ColumnInfo('archived_at', 'TIMESTAMP'),
            ColumnInfo('action', 'VARCHAR(100)'),
            ColumnInfo('title', 'VARCHAR(255)'),
            ColumnInfo('inventory_id', 'INTEGER'),
        ]
    
    def _get_product_columns(self) -> List[ColumnInfo]:
        """Get column definitions for products table"""
        # Similar comprehensive list for products
        return self._get_user_columns()  # Products has same columns in our schema
    
    def _get_order_columns(self) -> List[ColumnInfo]:
        """Get column definitions for orders table"""
        return self._get_user_columns()  # Orders has same columns in our schema
    
    def _get_inventory_columns(self) -> List[ColumnInfo]:
        """Get column definitions for inventory table"""
        return self._get_user_columns()  # Inventory has same columns in our schema
    
    def _get_transaction_columns(self) -> List[ColumnInfo]:
        """Get column definitions for transactions table"""
        return self._get_user_columns()  # Transactions has same columns in our schema
    
    def _get_session_columns(self) -> List[ColumnInfo]:
        """Get column definitions for sessions table"""
        return self._get_user_columns()  # Sessions has same columns in our schema
    
    def _get_employee_columns(self) -> List[ColumnInfo]:
        """Get column definitions for employees table"""
        return self._get_user_columns()  # Employees has same columns in our schema
    
    def _get_customer_columns(self) -> List[ColumnInfo]:
        """Get column definitions for customers table"""
        return self._get_user_columns()  # Customers has same columns in our schema
    
    def _get_account_columns(self) -> List[ColumnInfo]:
        """Get column definitions for accounts table"""
        return self._get_user_columns()  # Accounts has same columns in our schema
    
    def _get_log_columns(self) -> List[ColumnInfo]:
        """Get column definitions for logs table"""
        return self._get_user_columns()  # Logs has same columns in our schema
    
    def _get_analytics_columns(self) -> List[ColumnInfo]:
        """Get column definitions for analytics table"""
        return self._get_user_columns()  # Analytics has same columns in our schema
    
    def _get_department_columns(self) -> List[ColumnInfo]:
        """Get column definitions for departments table"""
        return self._get_user_columns()  # Departments has same columns in our schema
    
    def _get_supplier_columns(self) -> List[ColumnInfo]:
        """Get column definitions for suppliers table"""
        return self._get_user_columns()  # Suppliers has same columns in our schema
    
    def _get_sales_columns(self) -> List[ColumnInfo]:
        """Get column definitions for sales table"""
        return self._get_user_columns()  # Sales has same columns in our schema
    
    def _get_cache_columns(self) -> List[ColumnInfo]:
        """Get column definitions for cache table"""
        return self._get_user_columns()  # Cache has same columns in our schema
    
    def _get_metrics_columns(self) -> List[ColumnInfo]:
        """Get column definitions for metrics table"""
        return self._get_user_columns()  # Metrics has same columns in our schema
    
    def _get_settings_columns(self) -> List[ColumnInfo]:
        """Get column definitions for settings table"""
        return self._get_user_columns()  # Settings has same columns in our schema
    
    def _get_notification_columns(self) -> List[ColumnInfo]:
        """Get column definitions for notifications table"""
        return self._get_user_columns()  # Notifications has same columns in our schema
    
    def _get_temp_data_columns(self) -> List[ColumnInfo]:
        """Get column definitions for temp_data table"""
        return [
            ColumnInfo('id', 'SERIAL', False, True),
            ColumnInfo('name', 'VARCHAR(255)'),
            ColumnInfo('status', 'VARCHAR(50)'),
            ColumnInfo('data', 'JSONB', default_value='{}'),
            ColumnInfo('created_at', 'TIMESTAMP', default_value='CURRENT_TIMESTAMP'),
        ]
    
    def _get_audit_log_columns(self) -> List[ColumnInfo]:
        """Get column definitions for audit_logs table"""
        return [
            ColumnInfo('id', 'SERIAL', False, True),
            ColumnInfo('user_id', 'INTEGER'),
            ColumnInfo('customer_id', 'INTEGER'),
            ColumnInfo('action', 'VARCHAR(100)'),
            ColumnInfo('status', 'VARCHAR(50)'),
            ColumnInfo('last_login', 'TIMESTAMP'),
            ColumnInfo('created_at', 'TIMESTAMP', default_value='CURRENT_TIMESTAMP'),
            ColumnInfo('data', 'JSONB', default_value='{}'),
        ]
    
    def _get_expired_token_columns(self) -> List[ColumnInfo]:
        """Get column definitions for expired_tokens table"""
        return [
            ColumnInfo('id', 'SERIAL', False, True),
            ColumnInfo('token', 'VARCHAR(255)'),
            ColumnInfo('user_id', 'INTEGER'),
            ColumnInfo('status', 'VARCHAR(50)'),
            ColumnInfo('expired_at', 'TIMESTAMP'),
            ColumnInfo('created_at', 'TIMESTAMP', default_value='CURRENT_TIMESTAMP'),
            ColumnInfo('data', 'JSONB', default_value='{}'),
        ]
    
    def _get_deleted_item_columns(self) -> List[ColumnInfo]:
        """Get column definitions for deleted_items table"""
        return [
            ColumnInfo('id', 'SERIAL', False, True),
            ColumnInfo('item_type', 'VARCHAR(50)'),
            ColumnInfo('item_id', 'INTEGER'),
            ColumnInfo('status', 'VARCHAR(50)'),
            ColumnInfo('last_login', 'TIMESTAMP'),
            ColumnInfo('deleted_at', 'TIMESTAMP', default_value='CURRENT_TIMESTAMP'),
            ColumnInfo('data', 'JSONB', default_value='{}'),
        ]
    
    def _get_archive_columns(self) -> List[ColumnInfo]:
        """Get column definitions for archive table"""
        return [
            ColumnInfo('id', 'SERIAL', False, True),
            ColumnInfo('source_table', 'VARCHAR(50)'),
            ColumnInfo('source_id', 'INTEGER'),
            ColumnInfo('status', 'VARCHAR(50)'),
            ColumnInfo('archived_at', 'TIMESTAMP', default_value='CURRENT_TIMESTAMP'),
            ColumnInfo('data', 'JSONB', default_value='{}'),
        ]
    
    def _get_yugabyte_columns(self) -> List[ColumnInfo]:
        """Get column definitions for YugabyteDB compatibility tables"""
        return [
            ColumnInfo('pk', 'SERIAL', False, True),
            ColumnInfo('col_int_key', 'INTEGER'),
            ColumnInfo('col_int', 'INTEGER'),
            ColumnInfo('col_varchar_10', 'VARCHAR(10)'),
            ColumnInfo('col_varchar_10_key', 'VARCHAR(10)'),
            ColumnInfo('col_bigint', 'BIGINT'),
            ColumnInfo('col_bigint_key', 'BIGINT'),
            ColumnInfo('col_decimal', 'DECIMAL'),
            ColumnInfo('col_decimal_key', 'DECIMAL'),
            ColumnInfo('col_float', 'FLOAT'),
            ColumnInfo('col_float_key', 'FLOAT'),
            ColumnInfo('col_double', 'DOUBLE PRECISION'),
            ColumnInfo('col_double_key', 'DOUBLE PRECISION'),
            ColumnInfo('col_char_255', 'CHAR(255)'),
            ColumnInfo('col_char_255_key', 'CHAR(255)'),
            ColumnInfo('col_char_10', 'CHAR(10)'),
            ColumnInfo('col_char_10_key', 'CHAR(10)'),
            ColumnInfo('col_text', 'TEXT'),
            ColumnInfo('col_text_key', 'TEXT'),
            ColumnInfo('col_varchar_255', 'VARCHAR(255)'),
            ColumnInfo('col_varchar_255_key', 'VARCHAR(255)'),
        ]