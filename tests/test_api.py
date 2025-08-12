"""
Test suite for pyrqg.api module
Tests Flask API endpoints, security, and error handling
"""

import pytest
import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.api import app, registered_grammars, _load_grammars


@pytest.fixture
def client():
    """Create Flask test client"""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_grammars():
    """Mock registered grammars"""
    with patch('pyrqg.api.registered_grammars', {
        'test_grammar': MagicMock(generate=lambda rule, seed=None: "SELECT * FROM test"),
        'simple_dml': MagicMock(generate=lambda rule, seed=None: "INSERT INTO users VALUES (1)")
    }):
        yield


class TestAPIEndpoints:
    """Test API endpoints"""
    
    def test_index_endpoint(self, client):
        """Test root endpoint"""
        response = client.get('/')
        assert response.status_code == 200
        data = json.loads(response.data)
        
        assert 'message' in data
        assert 'endpoints' in data
        assert '/api/grammars' in data['endpoints']
        
    def test_list_grammars(self, client, mock_grammars):
        """Test grammar listing endpoint"""
        response = client.get('/api/grammars')
        assert response.status_code == 200
        data = json.loads(response.data)
        
        assert 'grammars' in data
        assert 'test_grammar' in data['grammars']
        assert 'simple_dml' in data['grammars']
        
    def test_grammar_info(self, client, mock_grammars):
        """Test grammar info endpoint"""
        response = client.get('/api/grammars/test_grammar')
        assert response.status_code == 200
        data = json.loads(response.data)
        
        assert 'name' in data
        assert data['name'] == 'test_grammar'
        
    def test_grammar_info_not_found(self, client):
        """Test grammar info for non-existent grammar"""
        response = client.get('/api/grammars/non_existent')
        assert response.status_code == 404
        data = json.loads(response.data)
        
        assert 'error' in data
        
    def test_generate_endpoint_get(self, client, mock_grammars):
        """Test query generation via GET"""
        response = client.get('/api/generate/test_grammar')
        assert response.status_code == 200
        data = json.loads(response.data)
        
        assert 'query' in data
        assert data['query'] == "SELECT * FROM test"
        
    def test_generate_endpoint_post(self, client, mock_grammars):
        """Test query generation via POST"""
        response = client.post('/api/generate/test_grammar', 
                             json={'seed': 42, 'count': 2})
        assert response.status_code == 200
        data = json.loads(response.data)
        
        assert 'queries' in data
        assert len(data['queries']) == 2
        
    def test_generate_invalid_grammar(self, client):
        """Test generation with invalid grammar"""
        response = client.get('/api/generate/invalid_grammar')
        assert response.status_code == 404
        
    def test_generate_with_rule(self, client, mock_grammars):
        """Test generation with specific rule"""
        response = client.get('/api/generate/test_grammar?rule=specific_rule')
        assert response.status_code == 200
        
    def test_batch_generate(self, client, mock_grammars):
        """Test batch generation endpoint"""
        request_data = {
            'grammars': [
                {'name': 'test_grammar', 'count': 2},
                {'name': 'simple_dml', 'count': 1}
            ]
        }
        response = client.post('/api/batch', json=request_data)
        assert response.status_code == 200
        data = json.loads(response.data)
        
        assert 'results' in data
        assert 'test_grammar' in data['results']
        assert 'simple_dml' in data['results']
        
    def test_batch_missing_data(self, client):
        """Test batch endpoint with missing data"""
        response = client.post('/api/batch', json={})
        assert response.status_code == 400
        
    def test_health_endpoint(self, client):
        """Test health check endpoint"""
        response = client.get('/api/health')
        assert response.status_code == 200
        data = json.loads(response.data)
        
        assert data['status'] == 'healthy'
        assert 'grammars_loaded' in data


class TestSecurity:
    """Test security vulnerabilities"""
    
    def test_sql_injection_prevention(self, client):
        """Test SQL injection is prevented"""
        # These should be safely handled, not executed
        dangerous_inputs = [
            "'; DROP TABLE users; --",
            "1' OR '1'='1",
            "../../../etc/passwd",
            "<script>alert('xss')</script>"
        ]
        
        for dangerous in dangerous_inputs:
            response = client.get(f'/api/grammars/{dangerous}')
            # Should return 404, not execute injection
            assert response.status_code == 404
            
    def test_path_traversal_prevention(self, client):
        """Test path traversal is prevented"""
        response = client.get('/api/grammars/../../../etc/passwd')
        assert response.status_code == 404
        
    def test_large_request_handling(self, client):
        """Test handling of large requests"""
        # Create large request
        large_data = {
            'grammars': [{'name': 'test', 'count': 1000000}]
        }
        response = client.post('/api/batch', json=large_data)
        # Should handle gracefully, not crash
        assert response.status_code in [200, 400, 413]
        
    def test_invalid_json(self, client):
        """Test handling of invalid JSON"""
        response = client.post('/api/generate/test_grammar',
                             data='{"invalid": json}',
                             content_type='application/json')
        assert response.status_code == 400


class TestErrorHandling:
    """Test error handling"""
    
    def test_grammar_generation_error(self, client):
        """Test handling of grammar generation errors"""
        with patch('pyrqg.api.registered_grammars', {
            'broken': MagicMock(generate=MagicMock(side_effect=Exception("Generation failed")))
        }):
            response = client.get('/api/generate/broken')
            assert response.status_code == 500
            data = json.loads(response.data)
            assert 'error' in data
            # Should not expose internal error details
            assert "Generation failed" not in data['error']
            
    def test_missing_rule_error(self, client):
        """Test handling of missing rule errors"""
        with patch('pyrqg.api.registered_grammars', {
            'test': MagicMock(generate=MagicMock(side_effect=KeyError("rule not found")))
        }):
            response = client.get('/api/generate/test?rule=missing')
            assert response.status_code == 500
            
    def test_type_error_handling(self, client, mock_grammars):
        """Test handling of type errors in parameters"""
        response = client.post('/api/generate/test_grammar',
                             json={'seed': 'not_a_number'})
        # Should handle gracefully
        assert response.status_code in [200, 400]


class TestGrammarLoading:
    """Test grammar loading functionality"""
    
    def test_load_grammars(self):
        """Test _load_grammars function"""
        # Mock the import system
        with patch('importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.g = MagicMock(name="test_grammar")
            mock_import.return_value = mock_module
            
            with patch('pathlib.Path.glob') as mock_glob:
                mock_glob.return_value = [
                    Path('/fake/path/test_grammar.py'),
                    Path('/fake/path/another_grammar.py')
                ]
                
                grammars = _load_grammars()
                
                # Should attempt to load both files
                assert mock_import.call_count >= 1
                
    def test_load_grammars_with_error(self):
        """Test grammar loading with import errors"""
        with patch('importlib.import_module', side_effect=ImportError("Failed to import")):
            with patch('pathlib.Path.glob') as mock_glob:
                mock_glob.return_value = [Path('/fake/path/broken_grammar.py')]
                
                # Should handle errors gracefully
                grammars = _load_grammars()
                # Should continue despite errors
                

class TestInputValidation:
    """Test input validation"""
    
    def test_validate_count_parameter(self, client, mock_grammars):
        """Test count parameter validation"""
        # Negative count
        response = client.post('/api/generate/test_grammar',
                             json={'count': -1})
        assert response.status_code == 400
        
        # Zero count
        response = client.post('/api/generate/test_grammar',
                             json={'count': 0})
        assert response.status_code == 400
        
        # Too large count
        response = client.post('/api/generate/test_grammar',
                             json={'count': 10001})
        assert response.status_code == 400
        
    def test_validate_seed_parameter(self, client, mock_grammars):
        """Test seed parameter validation"""
        # Valid seeds
        for seed in [0, 1, 42, 999999]:
            response = client.post('/api/generate/test_grammar',
                                 json={'seed': seed})
            assert response.status_code == 200
            
    def test_validate_grammar_name(self, client):
        """Test grammar name validation"""
        # Invalid characters in name
        invalid_names = [
            "../../etc/passwd",
            "grammar; DROP TABLE users",
            "grammar\x00null",
            "grammar<script>"
        ]
        
        for name in invalid_names:
            response = client.get(f'/api/generate/{name}')
            assert response.status_code == 404


class TestConcurrency:
    """Test concurrent request handling"""
    
    def test_concurrent_generation(self, client, mock_grammars):
        """Test handling of concurrent generation requests"""
        import threading
        
        results = []
        
        def make_request():
            response = client.get('/api/generate/test_grammar')
            results.append(response.status_code)
            
        threads = []
        for _ in range(10):
            t = threading.Thread(target=make_request)
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()
            
        # All requests should succeed
        assert all(status == 200 for status in results)


class TestAPIDocumentation:
    """Test API documentation and help"""
    
    def test_api_has_documentation(self, client):
        """Test that API endpoints have documentation"""
        response = client.get('/')
        data = json.loads(response.data)
        
        assert 'endpoints' in data
        endpoints = data['endpoints']
        
        # Check that all major endpoints are documented
        expected_endpoints = [
            '/api/grammars',
            '/api/grammars/<name>',
            '/api/generate/<grammar_name>',
            '/api/batch',
            '/api/health'
        ]
        
        for endpoint in expected_endpoints:
            assert endpoint in endpoints
            
    def test_options_request(self, client):
        """Test OPTIONS request for CORS"""
        response = client.options('/api/grammars')
        # Should handle OPTIONS for CORS preflight
        assert response.status_code in [200, 204, 405]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])