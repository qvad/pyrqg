"""
Constants and status codes for PyRQG.
Equivalent to GenTest::Constants in Perl RandGen.
"""

from enum import IntEnum, auto


class Status(IntEnum):
    """Status codes for test execution results."""
    
    # Success codes
    OK = 0
    
    # Test result codes (1-99)
    EOF = 1
    SYNTAX_ERROR = 2
    SEMANTIC_ERROR = 3
    TRANSACTION_ERROR = 4
    
    # Environmental failures (100-199)
    ENVIRONMENT_FAILURE = 100
    PERL_FAILURE = 101
    UNKNOWN_ERROR = 102
    CUSTOM_OUTCOME = 103
    SKIP = 104
    
    # Database errors (200-299)
    SERVER_SHUTDOWN = 201
    SERVER_CRASHED = 202
    SERVER_KILLED = 203
    SERVER_DEADLOCKED = 204
    REPLICATION_FAILURE = 205
    DATABASE_CORRUPTION = 206
    SERVER_DISAPPEARED = 207
    
    # Critical errors (300+)
    CRITICAL_FAILURE = 301
    
    # Utility status codes
    WONT_HANDLE = 401
    ANY_ERROR = 402


class DatabaseType(IntEnum):
    """Supported database types."""
    MYSQL = 1
    POSTGRES = 2
    JAVADB = 3
    DRIZZLE = 4
    DUMMY = 5
    YUGABYTE = 6  # Added for YugabyteDB support


# Executor flags
EXECUTOR_FLAG_SILENT = 1
EXECUTOR_FLAG_CONNECT = 2
EXECUTOR_FLAG_EXECUTED = 4
EXECUTOR_FLAG_PERFORMANCE = 8

# Mixer flags
MIXER_FLAG_LOOP_UNTIL_DONE = 1

# Default values
DEFAULT_THREADS = 10
DEFAULT_QUERIES = 1000
DEFAULT_DURATION = 3600
DEFAULT_DSN = "dbi:mysql:host=127.0.0.1:port=9306:user=root:database=test"

# Grammar constants
FIELD_SEPARATOR = "|"
RULE_SEPARATOR = ";"
GRAMMAR_MAX_OCCURRENCES = 3500
GRAMMAR_MAX_LENGTH = 10000

# Data generation limits
DATA_MAX_INTEGER = 2147483647
DATA_MAX_SMALLINT = 65535
DATA_MAX_TINYINT = 255
DATA_MAX_BIGINT = 9223372036854775807


def status_to_text(status):
    """Convert status code to human-readable text."""
    return Status(status).name.replace('_', ' ').title()


def is_error_status(status):
    """Check if status indicates an error."""
    return status != Status.OK and status != Status.EOF and status != Status.SKIP


def is_critical_status(status):
    """Check if status indicates a critical error."""
    return status >= Status.CRITICAL_FAILURE