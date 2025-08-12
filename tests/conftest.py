"""
PyTest configuration and fixtures for PyRQG tests
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def sample_grammar():
    """Create a simple test grammar"""
    from pyrqg.dsl.core import Grammar
    
    g = Grammar("test_grammar")
    g.define_tables(users=100, products=50, orders=200)
    g.define_fields("id", "name", "email", "price", "quantity")
    
    # Add some basic rules
    g.rule("table", lambda ctx: ctx.rng.choice(list(ctx.tables.keys())))
    g.rule("field", lambda ctx: ctx.rng.choice(ctx.fields))
    g.rule("value", lambda ctx: str(ctx.rng.randint(1, 100)))
    
    return g


@pytest.fixture
def mock_random():
    """Create a mock random number generator with fixed sequence"""
    import random
    
    class MockRandom(random.Random):
        def __init__(self):
            super().__init__(42)  # Fixed seed
            
    return MockRandom()


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset singleton instances between tests"""
    # Reset EntropyManager singleton
    from pyrqg.production.entropy import EntropyManager
    EntropyManager._instance = None
    
    yield
    
    # Cleanup after test
    EntropyManager._instance = None


@pytest.fixture
def temp_grammar_file(tmp_path):
    """Create a temporary grammar file for testing"""
    def _create_grammar(name: str, content: str):
        file_path = tmp_path / f"{name}.py"
        file_path.write_text(content)
        return file_path
        
    return _create_grammar


# Configure pytest options
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "security: marks tests that check security vulnerabilities"
    )


# Test collection hooks
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names"""
    for item in items:
        # Mark integration tests
        if "integration" in item.nodeid.lower():
            item.add_marker(pytest.mark.integration)
            
        # Mark security tests
        if "security" in item.nodeid.lower() or "injection" in item.nodeid.lower():
            item.add_marker(pytest.mark.security)
            
        # Mark slow tests
        if "stress" in item.nodeid.lower() or "concurrent" in item.nodeid.lower():
            item.add_marker(pytest.mark.slow)