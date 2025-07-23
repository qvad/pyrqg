"""
Production configuration system for PyRQG.

Provides comprehensive configuration for all production components with
validation, defaults, and easy serialization.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict

from .entropy import EntropyConfig
from .threading import ThreadingConfig
from .data_generator import DataConfig, Distribution
from .uniqueness import UniquenessConfig, UniquenessMode


@dataclass
class MonitoringConfig:
    """Configuration for performance monitoring"""
    enabled: bool = True
    interval: int = 1000  # Log stats every N queries
    metrics: List[str] = field(default_factory=lambda: [
        "qps", "memory", "uniqueness_rate", "thread_utilization"
    ])
    export_format: str = "json"  # json, csv, prometheus
    export_path: Optional[str] = None
    
    # Alerts
    alert_on_duplicate_rate: float = 0.01  # Alert if >1% duplicates
    alert_on_error_rate: float = 0.001  # Alert if >0.1% errors
    alert_on_qps_drop: float = 0.5  # Alert if QPS drops by 50%


@dataclass
class DatabaseConfig:
    """Configuration for database-specific features"""
    primary_dialect: str = "postgresql"
    primary_version: str = "15.0"
    secondary_dialect: Optional[str] = "yugabyte"
    secondary_version: Optional[str] = "2.14"
    
    # Feature flags
    features: List[str] = field(default_factory=lambda: [
        "json_table", "multirange", "on_conflict", "returning"
    ])
    
    # Syntax validation
    validate_syntax: bool = True
    syntax_check_sample_rate: float = 0.01  # Check 1% of queries
    
    # Connection settings (for validation)
    connection_string: Optional[str] = None
    connection_pool_size: int = 10


@dataclass
class ProductionConfig:
    """Master configuration for production PyRQG"""
    # Component configs
    entropy: EntropyConfig = field(default_factory=EntropyConfig)
    threading: ThreadingConfig = field(default_factory=ThreadingConfig)
    data: DataConfig = field(default_factory=DataConfig)
    uniqueness: UniquenessConfig = field(default_factory=UniquenessConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    
    # Global settings
    name: str = "production_run"
    description: str = ""
    output_dir: str = "./output"
    log_level: str = "INFO"
    
    # Query generation settings
    target_queries: int = 1_000_000
    grammars: List[str] = field(default_factory=lambda: ["dml_unique"])
    grammar_weights: Dict[str, float] = field(default_factory=dict)
    
    # Performance settings
    enable_caching: bool = True
    cache_size_mb: int = 512
    checkpoint_interval: int = 100_000  # Save progress every N queries
    
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProductionConfig':
        """Create configuration from dictionary"""
        config = cls()
        
        # Update top-level fields
        for key in ['name', 'description', 'output_dir', 'log_level', 
                   'target_queries', 'grammars', 'grammar_weights',
                   'enable_caching', 'cache_size_mb', 'checkpoint_interval']:
            if key in data:
                setattr(config, key, data[key])
        
        # Update component configs
        if 'entropy' in data:
            config.entropy = EntropyConfig(**data['entropy'])
        if 'threading' in data:
            config.threading = ThreadingConfig(**data['threading'])
        if 'data' in data:
            config.data = DataConfig(**data['data'])
        if 'uniqueness' in data:
            # Handle enum conversion
            uniqueness_data = data['uniqueness'].copy()
            if 'mode' in uniqueness_data:
                uniqueness_data['mode'] = UniquenessMode(uniqueness_data['mode'])
            config.uniqueness = UniquenessConfig(**uniqueness_data)
        if 'monitoring' in data:
            config.monitoring = MonitoringConfig(**data['monitoring'])
        if 'database' in data:
            config.database = DatabaseConfig(**data['database'])
            
        return config
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        data = asdict(self)
        
        # Convert enums to strings
        if 'uniqueness' in data and 'mode' in data['uniqueness']:
            data['uniqueness']['mode'] = data['uniqueness']['mode'].value
            
        return data
    
    
    def validate(self) -> List[str]:
        """Validate configuration and return any errors"""
        errors = []
        
        # Validate target queries
        if self.target_queries <= 0:
            errors.append("target_queries must be positive")
            
        # Validate grammars
        if not self.grammars:
            errors.append("At least one grammar must be specified")
            
        # Validate grammar weights
        if self.grammar_weights:
            total_weight = sum(self.grammar_weights.values())
            if abs(total_weight - 1.0) > 0.01:
                errors.append(f"Grammar weights must sum to 1.0, got {total_weight}")
                
        # Validate entropy config
        if self.entropy.state_bits < 128:
            errors.append("Entropy state_bits should be at least 128 for billion-scale generation")
            
        # Validate threading
        if self.threading.num_threads and self.threading.num_threads > 64:
            errors.append("num_threads > 64 may cause performance degradation")
            
        # Validate output directory
        output_path = Path(self.output_dir)
        if output_path.exists() and not output_path.is_dir():
            errors.append(f"Output path exists but is not a directory: {self.output_dir}")
            
        return errors
    
    def create_directories(self):
        """Create necessary directories"""
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        if self.monitoring.export_path:
            Path(self.monitoring.export_path).parent.mkdir(parents=True, exist_ok=True)
            
        if self.uniqueness.use_disk_backing:
            Path(self.uniqueness.disk_cache_dir).mkdir(parents=True, exist_ok=True)


