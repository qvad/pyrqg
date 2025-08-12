"""
Query analyzer for parsing and understanding SQL queries
"""

import re
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field


@dataclass
class QueryInfo:
    """Information extracted from a query"""
    query_type: str  # SELECT, INSERT, UPDATE, DELETE, etc.
    tables: List[str] = field(default_factory=list)
    columns: Dict[str, List[str]] = field(default_factory=dict)  # table -> columns
    functions: List[str] = field(default_factory=list)
    subqueries: List[str] = field(default_factory=list)
    has_returning: bool = False
    has_on_conflict: bool = False
    has_distinct: bool = False
    has_order_by: bool = False
    has_limit: bool = False
    has_cte: bool = False
    errors: List[str] = field(default_factory=list)


class QueryAnalyzer:
    """Analyzes SQL queries to extract structure and identify issues"""
    
    def __init__(self):
        # Regex patterns for query analysis
        self.patterns = {
            'query_type': re.compile(r'^\s*(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|DROP|ALTER|BEGIN|COMMIT|ROLLBACK)', re.IGNORECASE),
            'tables': re.compile(r'(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM)\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE),
            'insert_columns': re.compile(r'INSERT\s+INTO\s+\w+\s*\(([^)]+)\)', re.IGNORECASE),
            'select_columns': re.compile(r'SELECT\s+(.*?)\s+FROM', re.IGNORECASE | re.DOTALL),
            'functions': re.compile(r'(\w+)\s*\(', re.IGNORECASE),
            'returning': re.compile(r'\bRETURNING\b', re.IGNORECASE),
            'on_conflict': re.compile(r'\bON\s+CONFLICT\b', re.IGNORECASE),
            'distinct': re.compile(r'\bDISTINCT\b', re.IGNORECASE),
            'order_by': re.compile(r'\bORDER\s+BY\b', re.IGNORECASE),
            'limit': re.compile(r'\bLIMIT\b', re.IGNORECASE),
            'cte': re.compile(r'^\s*WITH\b', re.IGNORECASE),
            'delete_order_limit': re.compile(r'DELETE\s+.*?\s+(ORDER\s+BY|LIMIT)', re.IGNORECASE | re.DOTALL),
        }
        
        # Common SQL functions
        self.sql_functions = {
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'ROUND', 'ABS', 'CEIL', 'FLOOR',
            'UPPER', 'LOWER', 'LENGTH', 'TRIM', 'SUBSTRING', 'CONCAT',
            'NOW', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'DATE_PART', 'EXTRACT',
            'COALESCE', 'NULLIF', 'CAST', 'CONVERT',
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD',
            'RANDOM', 'SQRT', 'POWER', 'LOG', 'EXP'
        }
    
    def analyze(self, query: str) -> QueryInfo:
        """Analyze a query and extract information"""
        info = QueryInfo(query_type='UNKNOWN')
        
        # Clean query
        query = self._clean_query(query)
        
        # Extract query type
        type_match = self.patterns['query_type'].search(query)
        if type_match:
            info.query_type = type_match.group(1).upper()
        
        # Extract tables
        info.tables = self._extract_tables(query)
        
        # Extract columns based on query type
        if info.query_type == 'INSERT':
            info.columns = self._extract_insert_columns(query, info.tables)
        elif info.query_type == 'SELECT':
            info.columns = self._extract_select_columns(query, info.tables)
        elif info.query_type in ['UPDATE', 'DELETE']:
            info.columns = self._extract_dml_columns(query, info.tables)
        
        # Extract functions
        info.functions = self._extract_functions(query)
        
        # Check for specific features
        info.has_returning = bool(self.patterns['returning'].search(query))
        info.has_on_conflict = bool(self.patterns['on_conflict'].search(query))
        info.has_distinct = bool(self.patterns['distinct'].search(query))
        info.has_order_by = bool(self.patterns['order_by'].search(query))
        info.has_limit = bool(self.patterns['limit'].search(query))
        info.has_cte = bool(self.patterns['cte'].search(query))
        
        # Check for known issues
        self._check_for_errors(query, info)
        
        return info
    
    def _clean_query(self, query: str) -> str:
        """Clean query for analysis"""
        # Remove comments
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
        
        # Normalize whitespace
        query = ' '.join(query.split())
        
        return query
    
    def _extract_tables(self, query: str) -> List[str]:
        """Extract table names from query"""
        tables = []
        
        # Find all table references
        for match in self.patterns['tables'].finditer(query):
            table = match.group(1)
            if table.upper() not in ['DUAL', 'VALUES'] and table not in tables:
                tables.append(table)
        
        return tables
    
    def _extract_insert_columns(self, query: str, tables: List[str]) -> Dict[str, List[str]]:
        """Extract columns from INSERT statement"""
        columns = {}
        
        match = self.patterns['insert_columns'].search(query)
        if match and tables:
            col_str = match.group(1)
            cols = [col.strip() for col in col_str.split(',')]
            columns[tables[0]] = cols
        
        # Also extract from VALUES if explicit columns not specified
        if not columns and tables:
            # Look for VALUES pattern
            values_match = re.search(r'VALUES\s*\((.*?)\)', query, re.IGNORECASE | re.DOTALL)
            if values_match:
                # Count values to infer columns
                values = values_match.group(1).split(',')
                columns[tables[0]] = [f'col_{i}' for i in range(len(values))]
        
        return columns
    
    def _extract_select_columns(self, query: str, tables: List[str]) -> Dict[str, List[str]]:
        """Extract columns from SELECT statement"""
        columns = {}
        
        match = self.patterns['select_columns'].search(query)
        if match:
            select_list = match.group(1)
            
            # Handle SELECT *
            if '*' in select_list:
                for table in tables:
                    columns[table] = ['*']
            else:
                # Parse column list
                col_list = []
                for col in select_list.split(','):
                    col = col.strip()
                    # Remove aliases
                    col = re.sub(r'\s+AS\s+\w+', '', col, flags=re.IGNORECASE)
                    # Extract base column name
                    if '.' in col:
                        table, column = col.split('.', 1)
                        if table not in columns:
                            columns[table] = []
                        columns[table].append(column)
                    else:
                        col_list.append(col)
                
                # Assign unqualified columns to first table
                if col_list and tables:
                    if tables[0] not in columns:
                        columns[tables[0]] = []
                    columns[tables[0]].extend(col_list)
        
        return columns
    
    def _extract_dml_columns(self, query: str, tables: List[str]) -> Dict[str, List[str]]:
        """Extract columns from UPDATE/DELETE statements"""
        columns = {}
        
        if not tables:
            return columns
        
        # For UPDATE, look for SET clause
        if 'UPDATE' in query.upper():
            set_match = re.search(r'SET\s+(.*?)(?:WHERE|RETURNING|$)', query, re.IGNORECASE | re.DOTALL)
            if set_match:
                set_clause = set_match.group(1)
                cols = []
                for assignment in set_clause.split(','):
                    if '=' in assignment:
                        col = assignment.split('=')[0].strip()
                        cols.append(col)
                if cols:
                    columns[tables[0]] = cols
        
        # Look for WHERE clause columns
        where_match = re.search(r'WHERE\s+(.*?)(?:ORDER|LIMIT|RETURNING|$)', query, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            # Extract column references
            col_refs = re.findall(r'(\w+)\s*(?:=|>|<|>=|<=|<>|!=|LIKE|IN|BETWEEN)', where_clause, re.IGNORECASE)
            for col in col_refs:
                if col.upper() not in ['AND', 'OR', 'NOT', 'NULL']:
                    if tables[0] not in columns:
                        columns[tables[0]] = []
                    if col not in columns[tables[0]]:
                        columns[tables[0]].append(col)
        
        return columns
    
    def _extract_functions(self, query: str) -> List[str]:
        """Extract function calls from query"""
        functions = []
        
        for match in self.patterns['functions'].finditer(query):
            func = match.group(1).upper()
            if func not in self.sql_functions and func not in functions:
                # Skip common keywords
                if func not in ['INSERT', 'SELECT', 'UPDATE', 'DELETE', 'VALUES', 'WHERE', 'FROM']:
                    functions.append(func)
        
        return functions
    
    def _check_for_errors(self, query: str, info: QueryInfo):
        """Check for common PostgreSQL compatibility issues"""
        
        # DELETE with ORDER BY or LIMIT
        if info.query_type == 'DELETE' and self.patterns['delete_order_limit'].search(query):
            info.errors.append("DELETE doesn't support ORDER BY/LIMIT in PostgreSQL")
        
        # DISTINCT placement
        if info.has_distinct:
            # Check if DISTINCT is not at the beginning of SELECT list
            distinct_match = re.search(r'SELECT\s+(?!DISTINCT).*?\bDISTINCT\b', query, re.IGNORECASE)
            if distinct_match:
                info.errors.append("DISTINCT must be at the beginning of SELECT list")
        
        # ON CONFLICT without proper syntax
        if info.has_on_conflict:
            # Check for valid ON CONFLICT syntax
            conflict_match = re.search(r'ON\s+CONFLICT\s*\([^)]*\)\s*(DO\s+NOTHING|DO\s+UPDATE)', query, re.IGNORECASE)
            if not conflict_match:
                info.errors.append("Invalid ON CONFLICT syntax")
        
        # Check for undefined functions
        for func in info.functions:
            if func.upper() not in self.sql_functions:
                info.errors.append(f"Possibly undefined function: {func}")