"""
Unified Schema Metadata Definitions for PyRQG.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pyrqg.core.types import is_numeric, is_string

__all__ = [
    "Column",
    "TableConstraint",
    "Index",
    "Table",
]

@dataclass
class Column:
    """Unified column definition/metadata"""
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_unique: bool = False
    has_default: bool = False
    default: Optional[str] = None
    check: Optional[str] = None
    foreign_key: Optional[str] = None  # table.column reference
    references: Optional[str] = None   # table.column reference (DDL style)
    on_delete: Optional[str] = None
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
class Index:
    """Index definition"""
    name: str
    columns: List[str]
    unique: bool = False
    where_clause: Optional[str] = None
    include_columns: Optional[List[str]] = None
    method: str = "btree"  # btree, hash, gist, gin

@dataclass
class Table:
    """Unified table definition/metadata"""
    name: str
    columns: Dict[str, Column]
    primary_key: Optional[str] = None
    unique_columns: List[str] = field(default_factory=list)
    foreign_keys: Dict[str, str] = field(default_factory=dict)  # column -> table.column
    row_count: int = 0
    
    # DDL generation fields
    constraints: List[TableConstraint] = field(default_factory=list)
    indexes: List[Index] = field(default_factory=list)
    tablespace: Optional[str] = None
    comment: Optional[str] = None
    partitioned_by: Optional[str] = None
    inherits: Optional[str] = None

    def get_column_names(self) -> List[str]:
        return list(self.columns.keys())

    def get_numeric_columns(self) -> List[str]:
        return [c.name for c in self.columns.values() if is_numeric(c.data_type)]
    
    def get_string_columns(self) -> List[str]:
        return [c.name for c in self.columns.values() if is_string(c.data_type)]
    
    @property
    def columns_list(self) -> List[Column]:
        return list(self.columns.values())

    @classmethod
    def from_list(cls, name: str, columns_list: List[Dict[str, Any]], **kwargs):
        """Factory to create from simple list-of-dicts format (Legacy support)."""
        cols = {}
        pk = kwargs.get('primary_key')
        unique = kwargs.get('unique_columns', kwargs.get('unique_keys', []))
        fks = kwargs.get('foreign_keys', {})
        
        for c in columns_list:
            cname = c['name']
            ctype = c.get('data_type') or c.get('type') or 'text'
            is_pk = (cname == pk) or c.get('is_primary_key', False)
            is_uniq = (cname in unique) or c.get('is_unique', False)
            
            cols[cname] = Column(
                name=cname,
                data_type=ctype,
                is_nullable=c.get('is_nullable', True),
                is_primary_key=is_pk,
                is_unique=is_uniq
            )
        
        return cls(name=name, columns=cols, primary_key=pk, unique_columns=unique, foreign_keys=fks)
