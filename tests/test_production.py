"""
Test suite for pyrqg.production modules
Tests entropy management, query generation, and production features
"""

import pytest
import threading
import time
import os
from unittest.mock import patch, MagicMock, mock_open
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.production.entropy import EntropyManager, EntropyConfig
from pyrqg.production.generator import ProductionQueryGenerator
from pyrqg.production.monitor import QueryMonitor, MonitorConfig, PerformanceStats
from pyrqg.production.configs import (
    ProductionConfig, billion_scale_config, high_throughput_config,
    data_validation_config, stress_test_config
)


class TestEntropyConfig:
    """Test EntropyConfig class"""
    
    def test_default_config(self):
        """Test default entropy configuration"""
        config = EntropyConfig()
        
        assert config.primary_source == "urandom"
        assert config.fallback_source == "random"
        assert config.state_bits == 256
        assert config.reseed_interval == 1000000
        assert config.thread_local is True
        
    def test_custom_config(self):
        """Test custom entropy configuration"""
        config = EntropyConfig(
            primary_source="hardware",
            state_bits=512,
            reseed_interval=500000
        )
        
        assert config.primary_source == "hardware"
        assert config.state_bits == 512
        assert config.reseed_interval == 500000
        
    def test_config_validation(self):
        """Test configuration validation"""
        # Invalid state bits
        with pytest.raises(ValueError):
            EntropyConfig(state_bits=100)
            
        # Invalid source
        with pytest.raises(ValueError):
            EntropyConfig(primary_source="invalid_source")


class TestEntropyManager:
    """Test EntropyManager class"""
    
    def test_singleton_pattern(self):
        """Test that EntropyManager is a singleton"""
        manager1 = EntropyManager()
        manager2 = EntropyManager()
        
        assert manager1 is manager2
        
    def test_initialization(self):
        """Test entropy manager initialization"""
        # Reset singleton for testing
        EntropyManager._instance = None
        
        config = EntropyConfig(state_bits=256)
        manager = EntropyManager(config)
        
        assert manager.config == config
        assert manager._entropy_state is not None
        assert len(manager._entropy_state) == 32  # 256 bits / 8
        
    def test_get_random(self):
        """Test getting random number generator"""
        manager = EntropyManager()
        
        rng1 = manager.get_random()
        rng2 = manager.get_random()
        
        # Should return random.Random instances
        assert hasattr(rng1, 'randint')
        assert hasattr(rng2, 'randint')
        
        # Different instances for thread safety
        assert rng1 is not rng2
        
    def test_thread_local_generators(self):
        """Test thread-local random generators"""
        manager = EntropyManager(EntropyConfig(thread_local=True))
        
        results = {}
        
        def get_generator_id():
            rng = manager.get_random()
            results[threading.current_thread().ident] = id(rng)
            
        threads = []
        for _ in range(5):
            t = threading.Thread(target=get_generator_id)
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()
            
        # Each thread should have different generator
        generator_ids = list(results.values())
        assert len(set(generator_ids)) == len(generator_ids)
        
    def test_deterministic_generation(self):
        """Test deterministic generation with seed"""
        manager = EntropyManager()
        
        rng1 = manager.get_random(seed=42)
        rng2 = manager.get_random(seed=42)
        
        # Same seed should produce same sequence
        vals1 = [rng1.randint(0, 1000000) for _ in range(10)]
        vals2 = [rng2.randint(0, 1000000) for _ in range(10)]
        
        assert vals1 == vals2
        
    def test_reseed_functionality(self):
        """Test reseeding functionality"""
        EntropyManager._instance = None
        manager = EntropyManager(EntropyConfig(reseed_interval=10))
        
        initial_state = manager._entropy_state
        
        # Generate enough randoms to trigger reseed
        for _ in range(15):
            manager.get_random()
            
        # State should have changed
        assert manager._entropy_state != initial_state
        
    def test_cleanup_terminated_threads(self):
        """Test cleanup of terminated threads"""
        manager = EntropyManager()
        
        # Create thread that terminates
        def short_task():
            manager.get_random()
            
        t = threading.Thread(target=short_task)
        t.start()
        t.join()
        
        thread_id = t.ident
        
        # Thread generator should be in dict
        assert thread_id in manager._thread_generators
        
        # Trigger cleanup
        manager._cleanup_terminated_threads()
        
        # Dead thread should be removed
        assert thread_id not in manager._thread_generators
        
    def test_entropy_sources(self):
        """Test different entropy sources"""
        # Test urandom source
        with patch('os.urandom', return_value=b'test_entropy'):
            manager = EntropyManager(EntropyConfig(primary_source="urandom"))
            state = manager._generate_entropy_state()
            assert state == b'test_entropy'
            
        # Test fallback to random
        with patch('os.urandom', side_effect=OSError("No entropy")):
            manager = EntropyManager(EntropyConfig(primary_source="urandom"))
            state = manager._generate_entropy_state()
            assert len(state) == manager.config.state_bits // 8


class TestProductionQueryGenerator:
    """Test ProductionQueryGenerator class"""
    
    def test_initialization(self):
        """Test generator initialization"""
        config = billion_scale_config()
        generator = ProductionQueryGenerator(config)
        
        assert generator.config == config
        assert generator._entropy_manager is not None
        assert generator._generated_count == 0
        
    def test_generate_single_query(self):
        """Test single query generation"""
        config = ProductionConfig(
            name="test",
            grammars=["simple_dml"],
            workload_distribution={"simple_dml": 1.0}
        )
        
        # Mock grammar
        mock_grammar = MagicMock()
        mock_grammar.generate.return_value = "SELECT * FROM test"
        
        with patch('pyrqg.production.generator.registered_grammars', {'simple_dml': mock_grammar}):
            generator = ProductionQueryGenerator(config)
            query = generator.generate_query()
            
            assert query == "SELECT * FROM test"
            assert generator._generated_count == 1
            
    def test_generate_batch(self):
        """Test batch query generation"""
        config = ProductionConfig(
            name="test",
            grammars=["dml1", "dml2"],
            workload_distribution={"dml1": 0.5, "dml2": 0.5}
        )
        
        mock_grammars = {
            'dml1': MagicMock(generate=lambda rule="query", seed=None: "SELECT 1"),
            'dml2': MagicMock(generate=lambda rule="query", seed=None: "INSERT INTO test")
        }
        
        with patch('pyrqg.production.generator.registered_grammars', mock_grammars):
            generator = ProductionQueryGenerator(config)
            queries = generator.generate_batch(10)
            
            assert len(queries) == 10
            assert generator._generated_count == 10
            
            # Check distribution (roughly)
            select_count = sum(1 for q in queries if "SELECT" in q)
            insert_count = sum(1 for q in queries if "INSERT" in q)
            
            # Should be roughly 50/50
            assert 3 <= select_count <= 7
            assert 3 <= insert_count <= 7
            
    def test_workload_distribution(self):
        """Test workload distribution accuracy"""
        config = ProductionConfig(
            name="test",
            grammars=["heavy", "medium", "light"],
            workload_distribution={
                "heavy": 0.7,
                "medium": 0.2,
                "light": 0.1
            }
        )
        
        mock_grammars = {
            'heavy': MagicMock(generate=lambda **k: "HEAVY"),
            'medium': MagicMock(generate=lambda **k: "MEDIUM"),
            'light': MagicMock(generate=lambda **k: "LIGHT")
        }
        
        with patch('pyrqg.production.generator.registered_grammars', mock_grammars):
            generator = ProductionQueryGenerator(config)
            queries = generator.generate_batch(1000)
            
            heavy_count = queries.count("HEAVY")
            medium_count = queries.count("MEDIUM")
            light_count = queries.count("LIGHT")
            
            # Check distribution (with some tolerance)
            assert 650 <= heavy_count <= 750
            assert 150 <= medium_count <= 250
            assert 50 <= light_count <= 150
            
    def test_stream_generation(self):
        """Test streaming query generation"""
        config = ProductionConfig(
            name="test",
            grammars=["test_grammar"]
        )
        
        mock_grammar = MagicMock(generate=lambda **k: "QUERY")
        
        with patch('pyrqg.production.generator.registered_grammars', {'test_grammar': mock_grammar}):
            generator = ProductionQueryGenerator(config)
            
            count = 0
            for query in generator.stream_queries(max_queries=5):
                assert query == "QUERY"
                count += 1
                
            assert count == 5
            assert generator._generated_count == 5
            
    def test_parallel_generation(self):
        """Test parallel query generation"""
        config = ProductionConfig(
            name="test",
            grammars=["test_grammar"],
            parallel_generators=4
        )
        
        mock_grammar = MagicMock(generate=lambda **k: "QUERY")
        
        with patch('pyrqg.production.generator.registered_grammars', {'test_grammar': mock_grammar}):
            generator = ProductionQueryGenerator(config)
            
            results = []
            
            def generate_worker():
                queries = generator.generate_batch(25)
                results.extend(queries)
                
            threads = []
            for _ in range(4):
                t = threading.Thread(target=generate_worker)
                threads.append(t)
                t.start()
                
            for t in threads:
                t.join()
                
            assert len(results) == 100
            assert all(q == "QUERY" for q in results)


class TestQueryMonitor:
    """Test QueryMonitor class"""
    
    def test_initialization(self):
        """Test monitor initialization"""
        config = MonitorConfig()
        monitor = QueryMonitor(config)
        
        assert monitor.config == config
        assert monitor._start_time is not None
        assert monitor._query_count == 0
        
    def test_record_query(self):
        """Test recording query metrics"""
        monitor = QueryMonitor()
        
        monitor.record_query("SELECT * FROM test", duration_ms=10.5)
        
        assert monitor._query_count == 1
        assert monitor._total_duration == 10.5
        
        # Check stats update
        stats = monitor._type_stats["SELECT"]
        assert stats.count == 1
        assert stats.total_duration == 10.5
        
    def test_query_type_detection(self):
        """Test detection of query types"""
        monitor = QueryMonitor()
        
        queries = [
            ("SELECT * FROM users", "SELECT"),
            ("INSERT INTO users VALUES (1)", "INSERT"),
            ("UPDATE users SET name='test'", "UPDATE"),
            ("DELETE FROM users", "DELETE"),
            ("WITH cte AS (...) SELECT", "WITH"),
            ("CREATE TABLE test", "CREATE"),
            ("DROP TABLE test", "DROP"),
            ("BEGIN", "BEGIN"),
            ("COMMIT", "COMMIT")
        ]
        
        for query, expected_type in queries:
            monitor.record_query(query, duration_ms=1.0)
            assert expected_type in monitor._type_stats
            
    def test_get_stats(self):
        """Test getting monitor statistics"""
        monitor = QueryMonitor()
        
        # Record various queries
        monitor.record_query("SELECT 1", duration_ms=5.0)
        monitor.record_query("SELECT 2", duration_ms=15.0)
        monitor.record_query("INSERT INTO test", duration_ms=10.0)
        
        stats = monitor.get_stats()
        
        assert stats['total_queries'] == 3
        assert stats['total_duration_ms'] == 30.0
        assert stats['average_duration_ms'] == 10.0
        assert stats['queries_per_second'] > 0
        
        assert 'by_type' in stats
        assert stats['by_type']['SELECT']['count'] == 2
        assert stats['by_type']['SELECT']['average_duration'] == 10.0
        
    def test_resource_tracking(self):
        """Test resource usage tracking"""
        monitor = QueryMonitor(MonitorConfig(track_resources=True))
        
        # Mock resource usage
        with patch('psutil.Process') as mock_process:
            mock_proc = MagicMock()
            mock_proc.memory_info.return_value.rss = 100 * 1024 * 1024  # 100MB
            mock_proc.cpu_percent.return_value = 25.0
            mock_process.return_value = mock_proc
            
            monitor.record_query("SELECT 1", duration_ms=1.0)
            stats = monitor.get_stats()
            
            assert 'resource_usage' in stats
            assert stats['resource_usage']['memory_mb'] == 100.0
            assert stats['resource_usage']['cpu_percent'] == 25.0
            
    def test_export_metrics(self):
        """Test metrics export"""
        monitor = QueryMonitor()
        
        monitor.record_query("SELECT 1", duration_ms=5.0)
        monitor.record_query("INSERT INTO test", duration_ms=10.0)
        
        metrics = monitor.export_metrics()
        
        assert 'timestamp' in metrics
        assert metrics['total_queries'] == 2
        assert 'by_type' in metrics
        
    def test_reset_stats(self):
        """Test resetting statistics"""
        monitor = QueryMonitor()
        
        monitor.record_query("SELECT 1", duration_ms=5.0)
        assert monitor._query_count == 1
        
        monitor.reset()
        
        assert monitor._query_count == 0
        assert monitor._total_duration == 0.0
        assert len(monitor._type_stats) == 0


class TestProductionConfigs:
    """Test production configuration presets"""
    
    def test_billion_scale_config(self):
        """Test billion-scale configuration"""
        config = billion_scale_config()
        
        assert config.name == "billion_scale_production"
        assert config.target_queries == 1_000_000_000
        assert config.batch_size == 10000
        assert config.parallel_generators == 16
        assert config.entropy.state_bits == 256
        
    def test_high_throughput_config(self):
        """Test high-throughput configuration"""
        config = high_throughput_config()
        
        assert config.name == "high_throughput"
        assert config.batch_size == 50000
        assert config.parallel_generators == 32
        assert config.monitoring.enabled is False  # Disabled for performance
        
    def test_data_validation_config(self):
        """Test data validation configuration"""
        config = data_validation_config()
        
        assert config.name == "data_validation"
        assert config.batch_size == 100  # Smaller for validation
        assert config.uniqueness_tracking.enabled is True
        assert config.monitoring.track_resources is True
        
    def test_stress_test_config(self):
        """Test stress test configuration"""
        config = stress_test_config()
        
        assert config.name == "stress_test"
        assert "complex" in config.workload_distribution
        assert config.workload_distribution["complex"] >= 0.7
        
    def test_config_validation(self):
        """Test configuration validation"""
        # Invalid workload distribution (doesn't sum to 1.0)
        with pytest.raises(ValueError):
            ProductionConfig(
                name="invalid",
                grammars=["g1", "g2"],
                workload_distribution={"g1": 0.5, "g2": 0.3}
            )
            
        # Missing grammars
        with pytest.raises(ValueError):
            ProductionConfig(
                name="invalid",
                grammars=[],
                workload_distribution={}
            )


class TestIntegrationScenarios:
    """Integration tests for production scenarios"""
    
    def test_full_production_pipeline(self):
        """Test complete production pipeline"""
        config = ProductionConfig(
            name="integration_test",
            grammars=["test_grammar"],
            target_queries=100,
            batch_size=10,
            monitoring=MonitorConfig(enabled=True)
        )
        
        mock_grammar = MagicMock()
        query_num = 0
        
        def generate_query(**kwargs):
            nonlocal query_num
            query_num += 1
            return f"QUERY_{query_num}"
            
        mock_grammar.generate = generate_query
        
        with patch('pyrqg.production.generator.registered_grammars', {'test_grammar': mock_grammar}):
            generator = ProductionQueryGenerator(config)
            monitor = QueryMonitor()
            
            queries_generated = []
            
            for query in generator.stream_queries(max_queries=100):
                start_time = time.time()
                queries_generated.append(query)
                duration_ms = (time.time() - start_time) * 1000
                monitor.record_query(query, duration_ms)
                
            assert len(queries_generated) == 100
            assert queries_generated[0] == "QUERY_1"
            assert queries_generated[99] == "QUERY_100"
            
            stats = monitor.get_stats()
            assert stats['total_queries'] == 100
            
    def test_concurrent_generation_monitoring(self):
        """Test concurrent generation with monitoring"""
        config = ProductionConfig(
            name="concurrent_test",
            grammars=["test_grammar"],
            parallel_generators=4
        )
        
        mock_grammar = MagicMock(generate=lambda **k: "CONCURRENT_QUERY")
        
        with patch('pyrqg.production.generator.registered_grammars', {'test_grammar': mock_grammar}):
            generator = ProductionQueryGenerator(config)
            monitor = QueryMonitor()
            
            def worker():
                for _ in range(25):
                    query = generator.generate_query()
                    monitor.record_query(query, duration_ms=1.0)
                    
            threads = []
            for _ in range(4):
                t = threading.Thread(target=worker)
                threads.append(t)
                t.start()
                
            for t in threads:
                t.join()
                
            stats = monitor.get_stats()
            assert stats['total_queries'] == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])