"""
Python-based configuration system for PyRQG.

No more config files! Everything is code-based, type-safe, and IDE-friendly.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

from .entropy import EntropyConfig
from .threading import ThreadingConfig
from .data_generator import DataConfig, Distribution
from .uniqueness import UniquenessConfig, UniquenessMode
from .config import ProductionConfig, MonitoringConfig, DatabaseConfig


def billion_scale_config() -> ProductionConfig:
    """
    Configuration for generating billions of unique queries.
    
    Optimized for maximum performance and scale.
    """
    return ProductionConfig(
        name="billion_scale_production",
        description="Generate 1 billion unique PostgreSQL/YugabyteDB queries",
        target_queries=1_000_000_000,
        output_dir="./output/billion_scale",
        log_level="INFO",
        
        # Use all available grammars with weights
        grammars=[
            "dml_unique",
            "functions_ddl", 
            "dml_with_functions",
            "workload_select",
            "workload_insert",
            "workload_update"
        ],
        grammar_weights={
            "dml_unique": 0.3,
            "functions_ddl": 0.1,
            "dml_with_functions": 0.2,
            "workload_select": 0.2,
            "workload_insert": 0.1,
            "workload_update": 0.1
        },
        
        # Maximum entropy for true randomness
        entropy=EntropyConfig(
            primary_source="urandom",
            state_bits=256,
            reseed_interval=10_000_000,
            thread_local=True
        ),
        
        # Use all CPU cores
        threading=ThreadingConfig(
            num_threads=None,  # Auto-detect CPU count
            queue_size=100_000,
            batch_size=10_000,
            backpressure_threshold=0.8,
            monitor_interval=1.0,
            enable_affinity=True
        ),
        
        # Rich data generation
        data=DataConfig(
            default_distribution=Distribution.UNIFORM,
            string_length_min=5,
            string_length_max=50,
            text_vocabulary_size=100_000,
            enable_realistic_names=True,
            enable_realistic_addresses=True,
            enable_realistic_emails=True,
            enable_realistic_phones=True,
            cache_size=100_000,
            correlations=[
                {
                    "type": "sequential",
                    "fields": ["created_at", "updated_at"],
                    "constraint": "updated_at >= created_at"
                },
                {
                    "type": "proportional", 
                    "fields": ["quantity", "total_price"],
                    "multiplier": 9.99
                }
            ]
        ),
        
        # Efficient uniqueness tracking
        uniqueness=UniquenessConfig(
            mode=UniquenessMode.PROBABILISTIC,
            false_positive_rate=0.00001,  # 0.001%
            expected_elements=1_000_000_000,
            bloom_filter_size_mb=4096,  # 4GB
            rotation_interval=100_000_000,
            hash_functions=7,
            normalize_whitespace=True,
            normalize_case=True,
            normalize_literals=False
        ),
        
        # Comprehensive monitoring
        monitoring=MonitoringConfig(
            enabled=True,
            interval=100_000,
            metrics=["qps", "memory", "uniqueness_rate", "thread_utilization"],
            export_format="json",
            export_path="./output/billion_scale/metrics.jsonl",
            alert_on_duplicate_rate=0.01,
            alert_on_error_rate=0.001,
            alert_on_qps_drop=0.5
        ),
        
        # Database settings
        database=DatabaseConfig(
            primary_dialect="postgresql",
            primary_version="15.0",
            secondary_dialect="yugabyte", 
            secondary_version="2.14",
            features=[
                "json_table", "multirange", "on_conflict", 
                "returning", "lateral", "cte_recursive", 
                "window_functions"
            ],
            validate_syntax=True,
            syntax_check_sample_rate=0.001
        ),
        
        # Performance settings
        enable_caching=True,
        cache_size_mb=2048,
        checkpoint_interval=1_000_000
    )


def test_config() -> ProductionConfig:
    """
    Test configuration for development and debugging.
    
    Uses deterministic settings for reproducibility.
    """
    return ProductionConfig(
        name="test_run",
        description="Test configuration with deterministic settings",
        target_queries=10_000,
        output_dir="./output/test",
        log_level="DEBUG",
        
        grammars=["dml_unique", "functions_ddl"],
        
        # Deterministic for testing
        entropy=EntropyConfig(
            primary_source="deterministic",
            seed=42,
            state_bits=128,
            reseed_interval=1000
        ),
        
        # Limited threads for debugging
        threading=ThreadingConfig(
            num_threads=4,
            queue_size=1000,
            batch_size=100,
            backpressure_threshold=0.8
        ),
        
        # Smaller data generation
        data=DataConfig(
            text_vocabulary_size=1000,
            enable_realistic_names=True,
            cache_size=1000
        ),
        
        # Relaxed uniqueness for speed
        uniqueness=UniquenessConfig(
            mode=UniquenessMode.PROBABILISTIC,
            false_positive_rate=0.01,
            expected_elements=10_000,
            bloom_filter_size_mb=1,
            rotation_interval=5000
        ),
        
        # Simple monitoring
        monitoring=MonitoringConfig(
            enabled=True,
            interval=1000,
            metrics=["qps", "memory", "uniqueness_rate"]
        ),
        
        # Skip validation for speed
        database=DatabaseConfig(
            primary_dialect="postgresql",
            primary_version="15.0",
            validate_syntax=False
        ),
        
        checkpoint_interval=5000
    )


def performance_test_config() -> ProductionConfig:
    """
    Configuration for performance testing and benchmarking.
    
    Optimized for measuring maximum QPS.
    """
    config = billion_scale_config()
    config.name = "performance_test"
    config.description = "Benchmark maximum query generation speed"
    config.target_queries = 100_000_000  # 100M queries
    
    # Disable features that slow down generation
    config.uniqueness.mode = UniquenessMode.NONE  # No uniqueness checking
    config.database.validate_syntax = False  # No syntax validation
    config.monitoring.interval = 1_000_000  # Less frequent monitoring
    config.checkpoint_interval = 10_000_000  # Less frequent checkpoints
    
    return config


def minimal_config() -> ProductionConfig:
    """
    Minimal configuration for simple use cases.
    
    Just generate queries, no bells and whistles.
    """
    return ProductionConfig(
        name="minimal",
        description="Minimal configuration",
        target_queries=1000,
        grammars=["dml_unique"],
        
        # Minimal settings
        threading=ThreadingConfig(num_threads=1),
        uniqueness=UniquenessConfig(mode=UniquenessMode.NONE),
        monitoring=MonitoringConfig(enabled=False),
        database=DatabaseConfig(validate_syntax=False),
        
        enable_caching=False,
        checkpoint_interval=0  # No checkpoints
    )


def yugabyte_config() -> ProductionConfig:
    """
    Configuration optimized for YugabyteDB testing.
    
    Uses YugabyteDB-specific grammars and features.
    """
    config = billion_scale_config()
    config.name = "yugabyte_test"
    config.description = "YugabyteDB-specific query generation"
    
    # Use YugabyteDB grammars
    config.grammars = [
        "dml_yugabyte",
        "yugabyte_transactions_dsl",
        "yugabyte/optimizer_subquery_portable",
        "yugabyte/outer_join_portable"
    ]
    config.grammar_weights = {
        "dml_yugabyte": 0.4,
        "yugabyte_transactions_dsl": 0.3,
        "yugabyte/optimizer_subquery_portable": 0.2,
        "yugabyte/outer_join_portable": 0.1
    }
    
    # YugabyteDB as primary
    config.database.primary_dialect = "yugabyte"
    config.database.primary_version = "2.14"
    config.database.features.extend(["yb_hash_code", "colocated_tables"])
    
    return config


def custom_config(
    name: str,
    queries: int,
    grammars: List[str],
    threads: Optional[int] = None,
    uniqueness: bool = True,
    **kwargs
) -> ProductionConfig:
    """
    Create a custom configuration with specified parameters.
    
    Args:
        name: Configuration name
        queries: Number of queries to generate
        grammars: List of grammar names to use
        threads: Number of threads (None for auto)
        uniqueness: Enable uniqueness checking
        **kwargs: Additional config overrides
        
    Returns:
        ProductionConfig instance
    """
    # Start with test config as base
    config = test_config()
    
    # Apply custom settings
    config.name = name
    config.target_queries = queries
    config.grammars = grammars
    
    if threads is not None:
        config.threading.num_threads = threads
        
    if not uniqueness:
        config.uniqueness.mode = UniquenessMode.NONE
        
    # Apply any additional overrides
    for key, value in kwargs.items():
        if hasattr(config, key):
            setattr(config, key, value)
            
    return config


# Configuration registry for easy access
CONFIGS = {
    "billion": billion_scale_config,
    "test": test_config,
    "performance": performance_test_config,
    "minimal": minimal_config,
    "yugabyte": yugabyte_config,
}


def get_config(name: str) -> ProductionConfig:
    """
    Get a configuration by name.
    
    Args:
        name: Configuration name (billion, test, performance, minimal, yugabyte)
        
    Returns:
        ProductionConfig instance
        
    Raises:
        ValueError: If configuration name not found
    """
    if name not in CONFIGS:
        available = ", ".join(CONFIGS.keys())
        raise ValueError(f"Unknown config '{name}'. Available: {available}")
        
    return CONFIGS[name]()