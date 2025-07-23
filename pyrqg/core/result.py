"""
Result class for query execution.
Equivalent to GenTest::Result in Perl RandGen.
"""

from dataclasses import dataclass, field
from typing import Any, Optional, List, Dict
from .constants import Status


@dataclass
class Result:
    """Represents the result of a query execution."""
    
    query: str = ""
    status: Status = Status.OK
    err: int = 0
    errstr: str = ""
    sqlstate: str = ""
    column_names: List[str] = field(default_factory=list)
    column_types: List[str] = field(default_factory=list)
    column_collations: List[str] = field(default_factory=list)
    data: List[List[Any]] = field(default_factory=list)
    affected_rows: int = 0
    insert_id: Optional[int] = None
    start_time: float = 0.0
    end_time: float = 0.0
    warnings: List[str] = field(default_factory=list)
    info: str = ""
    
    @property
    def duration(self) -> float:
        """Calculate query execution duration."""
        return self.end_time - self.start_time
    
    @property
    def row_count(self) -> int:
        """Get number of rows returned."""
        return len(self.data)
    
    @property
    def column_count(self) -> int:
        """Get number of columns."""
        return len(self.column_names)
    
    def is_error(self) -> bool:
        """Check if result represents an error."""
        return self.status != Status.OK
    
    def is_success(self) -> bool:
        """Check if result represents successful execution."""
        return self.status == Status.OK
    
    def get_column_index(self, column_name: str) -> Optional[int]:
        """Get index of column by name."""
        try:
            return self.column_names.index(column_name)
        except ValueError:
            return None
    
    def get_column_values(self, column_name: str) -> List[Any]:
        """Get all values for a specific column."""
        idx = self.get_column_index(column_name)
        if idx is None:
            return []
        return [row[idx] for row in self.data if idx < len(row)]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            'query': self.query,
            'status': self.status.name,
            'err': self.err,
            'errstr': self.errstr,
            'sqlstate': self.sqlstate,
            'column_names': self.column_names,
            'column_types': self.column_types,
            'row_count': self.row_count,
            'affected_rows': self.affected_rows,
            'duration': self.duration,
            'warnings': self.warnings
        }
    
    def __str__(self) -> str:
        """String representation of result."""
        if self.is_error():
            return f"Error: {self.errstr} (Status: {self.status.name})"
        return f"Success: {self.row_count} rows, {self.affected_rows} affected"