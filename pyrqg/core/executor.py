"""
Query Executor - Handles database connections and query execution
"""

import time
import logging
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urlparse
import traceback

from .result import Result, Status


logger = logging.getLogger(__name__)


class Executor:
    """Base executor class"""
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection = None
        self.database_type = self._parse_database_type(dsn)
        self.explain_cache = {}  # Cache for EXPLAIN results
    
    def _parse_database_type(self, dsn: str) -> str:
        """Parse database type from DSN"""
        parsed = urlparse(dsn)
        return parsed.scheme.lower()
    
    def connect(self) -> None:
        """Connect to database"""
        raise NotImplementedError
    
    def execute(self, query: str, explain_analyze: bool = False, 
                explain_options: Dict[str, Any] = None) -> Result:
        """Execute query and return result"""
        raise NotImplementedError
    
    def explain_analyze(self, query: str, options: Dict[str, Any] = None) -> Optional[str]:
        """Get EXPLAIN ANALYZE output for a query"""
        # Default implementation
        explain_parts = ["EXPLAIN"]
        
        # Add options
        opts = []
        if options is None:
            options = {"ANALYZE": True}
        
        for opt, val in options.items():
            if val is True:
                opts.append(opt.upper())
            elif val is not False:
                opts.append(f"{opt.upper()} {val}")
        
        if opts:
            explain_parts.append(f"({', '.join(opts)})")
            
        explain_query = f"{' '.join(explain_parts)} {query}"
        
        try:
            result = self.execute(explain_query)
            if result.status == Status.OK and result.data:
                # Format output
                lines = []
                for row in result.data:
                    if isinstance(row, tuple):
                        lines.append(str(row[0]))
                    else:
                        lines.append(str(row))
                return '\n'.join(lines)
        except Exception as e:
            logger.warning(f"EXPLAIN ANALYZE failed: {e}")
            
        return None
    
    def close(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None


class PostgreSQLExecutor(Executor):
    """PostgreSQL executor using psycopg2"""
    
    def connect(self) -> None:
        """Connect to PostgreSQL"""
        try:
            import psycopg2
            self.connection = psycopg2.connect(self.dsn)
            self.connection.autocommit = True
            logger.info("Connected to PostgreSQL")
        except ImportError:
            logger.error("psycopg2 not installed. Install with: pip install psycopg2-binary")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def execute(self, query: str) -> Result:
        """Execute query on PostgreSQL"""
        start_time = time.time()
        cursor = None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Get affected rows
            rows = cursor.rowcount if cursor.rowcount >= 0 else 0
            
            # Try to fetch results for SELECT queries
            data = []
            if cursor.description:
                try:
                    data = cursor.fetchall()
                    rows = len(data)
                except:
                    pass
            
            result = Result(
                query=query,
                status=Status.OK,
                data=data if cursor.description else [],
                affected_rows=rows
            )
            result.start_time = start_time
            result.end_time = time.time()
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_str = str(e)
            
            # Determine status based on error
            status = Status.UNKNOWN_ERROR
            
            if "syntax error" in error_str.lower():
                status = Status.SYNTAX_ERROR
            elif "permission denied" in error_str.lower():
                status = Status.SEMANTIC_ERROR
            elif "violates" in error_str.lower():
                status = Status.SEMANTIC_ERROR
            elif "connection" in error_str.lower():
                status = Status.SERVER_CRASHED
            
            result = Result(
                query=query,
                status=status,
                errstr=error_str,
                affected_rows=0
            )
            result.start_time = start_time
            result.end_time = time.time()
            return result
        
        finally:
            if cursor:
                cursor.close()


class MySQLExecutor(Executor):
    """MySQL executor using pymysql"""
    
    def connect(self) -> None:
        """Connect to MySQL"""
        try:
            import pymysql
            # Parse DSN
            parsed = urlparse(self.dsn)
            self.connection = pymysql.connect(
                host=parsed.hostname,
                port=parsed.port or 3306,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip('/'),
                autocommit=True
            )
            logger.info("Connected to MySQL")
        except ImportError:
            logger.error("pymysql not installed. Install with: pip install pymysql")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise
    
    def execute(self, query: str) -> Result:
        """Execute query on MySQL"""
        start_time = time.time()
        cursor = None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Get affected rows
            rows = cursor.rowcount if cursor.rowcount >= 0 else 0
            
            # Try to fetch results for SELECT queries
            data = []
            if cursor.description:
                try:
                    data = cursor.fetchall()
                    rows = len(data)
                except:
                    pass
            
            result = Result(
                query=query,
                status=Status.OK,
                data=data if cursor.description else [],
                affected_rows=rows
            )
            result.start_time = start_time
            result.end_time = time.time()
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_str = str(e)
            
            # Determine status based on error
            status = Status.UNKNOWN_ERROR
            
            if "syntax" in error_str.lower():
                status = Status.SYNTAX_ERROR
            elif "access denied" in error_str.lower():
                status = Status.SEMANTIC_ERROR
            elif "constraint" in error_str.lower():
                status = Status.SEMANTIC_ERROR
            
            result = Result(
                query=query,
                status=status,
                errstr=error_str,
                affected_rows=0
            )
            result.start_time = start_time
            result.end_time = time.time()
            return result
        
        finally:
            if cursor:
                cursor.close()


class SQLiteExecutor(Executor):
    """SQLite executor using built-in sqlite3"""
    
    def connect(self) -> None:
        """Connect to SQLite"""
        try:
            import sqlite3
            # Extract database path from DSN
            if self.dsn.startswith('sqlite://'):
                db_path = self.dsn.replace('sqlite://', '')
            else:
                db_path = ':memory:'
            
            self.connection = sqlite3.connect(db_path)
            self.connection.isolation_level = None  # Autocommit mode
            logger.info(f"Connected to SQLite: {db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise
    
    def execute(self, query: str) -> Result:
        """Execute query on SQLite"""
        start_time = time.time()
        cursor = None
        
        try:
            cursor = self.connection.cursor()
            
            # SQLite doesn't support some PostgreSQL/MySQL features
            # Do basic query modifications
            modified_query = self._modify_query_for_sqlite(query)
            
            cursor.execute(modified_query)
            
            # Get affected rows
            rows = cursor.rowcount if cursor.rowcount >= 0 else 0
            
            # Try to fetch results for SELECT queries
            data = []
            if cursor.description:
                try:
                    data = cursor.fetchall()
                    rows = len(data)
                except:
                    pass
            
            result = Result(
                query=query,
                status=Status.OK,
                data=data if cursor.description else [],
                affected_rows=rows
            )
            result.start_time = start_time
            result.end_time = time.time()
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_str = str(e)
            
            # Determine status based on error
            status = Status.UNKNOWN_ERROR
            
            if "syntax error" in error_str.lower():
                status = Status.SYNTAX_ERROR
            elif "no such table" in error_str.lower():
                status = Status.SEMANTIC_ERROR
            elif "constraint" in error_str.lower():
                status = Status.SEMANTIC_ERROR
            
            result = Result(
                query=query,
                status=status,
                errstr=error_str,
                affected_rows=0
            )
            result.start_time = start_time
            result.end_time = time.time()
            return result
        
        finally:
            if cursor:
                cursor.close()
    
    def _modify_query_for_sqlite(self, query: str) -> str:
        """Modify query to work with SQLite"""
        # Replace PostgreSQL specific syntax
        query = query.replace('::INTEGER', '')
        query = query.replace('DEFAULT', 'NULL')  # For auto-increment
        
        # SQLite doesn't support SAVEPOINT in the same way
        if 'SAVEPOINT' in query:
            return query.replace('SAVEPOINT', '-- SAVEPOINT')
        if 'ROLLBACK TO' in query:
            return query.replace('ROLLBACK TO', '-- ROLLBACK TO')
        
        return query


class DryRunExecutor(Executor):
    """Executor for dry run mode - no actual execution"""
    
    def connect(self) -> None:
        """No connection needed for dry run"""
        logger.info("Dry run mode - no database connection")
    
    def execute(self, query: str) -> Result:
        """Simulate query execution"""
        # Basic syntax validation
        query_upper = query.strip().upper()
        
        if not query_upper:
            return Result(
                query=query,
                status=Status.SYNTAX_ERROR,
                errstr="Empty query",
                affected_rows=0
            )
        
        # Check for basic SQL keywords
        valid_starts = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 
                       'ALTER', 'START', 'COMMIT', 'ROLLBACK', 'SAVEPOINT']
        
        if not any(query_upper.startswith(keyword) for keyword in valid_starts):
            return Result(
                query=query,
                status=Status.SYNTAX_ERROR,
                errstr="Query does not start with valid SQL keyword",
                affected_rows=0
            )
        
        # Simulate successful execution
        result = Result(
            query=query,
            status=Status.OK,
            affected_rows=0
        )
        result.start_time = time.time()
        result.end_time = result.start_time + 0.001
        return result


# Factory function
def create_executor(dsn: Optional[str] = None) -> Executor:
    """Create appropriate executor based on DSN"""
    if not dsn:
        return DryRunExecutor("")
    
    parsed = urlparse(dsn)
    db_type = parsed.scheme.lower()
    
    executor = None
    if db_type in ['postgresql', 'postgres']:
        executor = PostgreSQLExecutor(dsn)
    elif db_type in ['mysql']:
        executor = MySQLExecutor(dsn)
    elif db_type in ['sqlite', 'sqlite3']:
        executor = SQLiteExecutor(dsn)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    # Connect to the database
    executor.connect()
    return executor