# PyRQG Test Suite

Comprehensive test suite for the Python Random Query Generator (PyRQG).

## Test Structure

```
tests/
├── test_dsl_core.py      # Core DSL functionality tests
├── test_api.py           # API endpoint and security tests  
├── test_production.py    # Production features tests
├── test_grammars.py      # Grammar validation tests
├── test_integration.py   # End-to-end integration tests
├── conftest.py          # PyTest configuration and fixtures
└── __init__.py
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run specific test file
```bash
pytest tests/test_dsl_core.py
```

### Run with coverage
```bash
pytest --cov=pyrqg --cov-report=html
```

### Run only fast tests (skip slow/integration)
```bash
pytest -m "not slow and not integration"
```

### Run security tests
```bash
pytest -m security
```

## Test Categories

### Core Tests (`test_dsl_core.py`)
- Grammar creation and configuration
- Element types (Literal, Choice, Template, etc.)
- Context and random generation
- Rule references and composition
- Edge cases and error handling

### API Tests (`test_api.py`)
- Flask endpoint functionality
- Security vulnerability checks
- Input validation
- Error handling
- Concurrent request handling

### Production Tests (`test_production.py`)
- Entropy management
- Query generation at scale
- Performance monitoring
- Configuration validation
- Thread safety

### Grammar Tests (`test_grammars.py`)
- Grammar loading validation
- SQL syntax validity
- Feature coverage analysis
- Pattern detection
- Code quality checks

### Integration Tests (`test_integration.py`)
- End-to-end scenarios
- Real-world workload patterns
- Multi-component interaction
- Stress testing

## Key Test Scenarios

### Security Testing
- SQL injection prevention
- Path traversal protection
- Input sanitization
- Error message safety

### Performance Testing
- Concurrent generation
- Large batch processing
- Memory usage
- Thread safety

### Quality Assurance
- Deterministic generation
- Grammar variety
- SQL validity
- API reliability

## Fixtures

### `sample_grammar`
Creates a basic test grammar with tables and fields.

### `mock_random`
Provides deterministic random generation for testing.

### `reset_singletons`
Ensures clean state between tests.

### `temp_grammar_file`
Creates temporary grammar files for testing.

## Coverage Goals

- Core DSL: 90%+ coverage
- API endpoints: 95%+ coverage
- Production features: 85%+ coverage
- Grammar files: 100% loadable

## Adding New Tests

1. Create test file following naming convention: `test_*.py`
2. Use appropriate markers for categorization
3. Include both positive and negative test cases
4. Test edge cases and error conditions
5. Add integration tests for new features

## CI/CD Integration

Tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run tests
  run: |
    pytest --cov=pyrqg --cov-fail-under=80
```

## Debugging Tests

### Run specific test
```bash
pytest tests/test_dsl_core.py::TestGrammar::test_grammar_creation -v
```

### Enable debugging
```bash
pytest --pdb  # Drop to debugger on failure
```

### Show print statements
```bash
pytest -s  # No capture
```