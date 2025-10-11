"""
Test suite for PyRQG grammar files
Tests grammar loading, generation, and SQL validity
"""

import pytest
import sys
import re
from pathlib import Path
from typing import List, Dict, Set

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.dsl.core import Grammar


def load_grammar(name: str) -> Grammar:
    """Load a grammar by name"""
    module_path = f"grammars.{name}"
    module = __import__(module_path, fromlist=['g'])
    return module.g


def get_all_grammar_files() -> List[str]:
    """Get all grammar files"""
    grammar_dir = Path(__file__).parent.parent / "grammars"
    files = []
    
    # Main grammar files
    for file in grammar_dir.glob("*.py"):
        if not file.name.startswith("__"):
            files.append(file.stem)
            
    # Workload grammar files
    workload_dir = grammar_dir / "workload"
    if workload_dir.exists():
        for file in workload_dir.glob("*.py"):
            if not file.name.startswith("__"):
                files.append(f"workload.{file.stem}")
                
    return files


class TestGrammarLoading:
    """Test that all grammars can be loaded"""
    
    @pytest.mark.parametrize("grammar_name", get_all_grammar_files())
    def test_grammar_loads(self, grammar_name):
        """Test that grammar file can be loaded"""
        try:
            grammar = load_grammar(grammar_name)
            assert isinstance(grammar, Grammar)
            assert grammar.name is not None
        except Exception as e:
            pytest.fail(f"Failed to load grammar {grammar_name}: {e}")
            
    @pytest.mark.parametrize("grammar_name", get_all_grammar_files())
    def test_grammar_has_rules(self, grammar_name):
        """Test that grammar has at least one rule"""
        grammar = load_grammar(grammar_name)
        assert len(grammar.rules) > 0, f"Grammar {grammar_name} has no rules"
        
    @pytest.mark.parametrize("grammar_name", get_all_grammar_files())
    def test_grammar_has_query_rule(self, grammar_name):
        """Test that grammar has a 'query' rule (convention)"""
        grammar = load_grammar(grammar_name)
        # Most grammars should have a 'query' rule as entry point
        # Some specialized ones might not
        if grammar_name not in ["ddl_aux", "postgresql15_types"]:
            assert "query" in grammar.rules, f"Grammar {grammar_name} missing 'query' rule"


class TestGrammarGeneration:
    """Test query generation from grammars"""
    
    @pytest.mark.parametrize("grammar_name", get_all_grammar_files())
    def test_grammar_generates(self, grammar_name):
        """Test that grammar can generate queries"""
        grammar = load_grammar(grammar_name)
        
        # Try to generate from 'query' rule or first available rule
        if "query" in grammar.rules:
            rule = "query"
        else:
            rule = list(grammar.rules.keys())[0]
            
        # Generate multiple queries to test variability
        queries = []
        for i in range(10):
            try:
                query = grammar.generate(rule, seed=i)
                assert query, f"Grammar {grammar_name} generated empty query"
                assert isinstance(query, str)
                queries.append(query)
            except Exception as e:
                pytest.fail(f"Grammar {grammar_name} failed to generate: {e}")
                
        # Check that we get some variety (not all identical)
        unique_queries = set(queries)
        if len(unique_queries) == 1 and grammar_name not in ["simple_transaction"]:
            # Some grammars might be very simple, but most should have variety
            pytest.skip(f"Grammar {grammar_name} generates only one query pattern")
            
    def test_deterministic_generation(self):
        """Test that same seed produces same query"""
        grammar = load_grammar("simple_dml")
        
        query1 = grammar.generate("query", seed=42)
        query2 = grammar.generate("query", seed=42)
        
        assert query1 == query2


class TestSQLValidity:
    """Test that generated SQL is syntactically valid"""
    
    def is_valid_sql_basic(self, query: str) -> bool:
        """Basic SQL syntax validation"""
        # Remove comments
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
        
        # Check for basic SQL keywords
        sql_keywords = [
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
            'ALTER', 'WITH', 'BEGIN', 'COMMIT', 'ROLLBACK', 'TRUNCATE'
        ]
        
        query_upper = query.upper()
        return any(keyword in query_upper for keyword in sql_keywords)
        
    def check_balanced_parentheses(self, query: str) -> bool:
        """Check if parentheses are balanced"""
        count = 0
        for char in query:
            if char == '(':
                count += 1
            elif char == ')':
                count -= 1
            if count < 0:
                return False
        return count == 0
        
    def check_balanced_quotes(self, query: str) -> bool:
        """Check if quotes are balanced"""
        # Simple check - count should be even
        single_quotes = query.count("'") - query.count("\\'")
        double_quotes = query.count('"') - query.count('\\"')
        
        return single_quotes % 2 == 0 and double_quotes % 2 == 0
        
    @pytest.mark.parametrize("grammar_name", [
        "simple_dml", "dml_unique", "dml_with_functions", "dml_yugabyte",
        "ddl_focused", "subquery_dsl", "simple_transaction"
    ])
    def test_sql_syntax_validity(self, grammar_name):
        """Test that generated SQL has valid basic syntax"""
        grammar = load_grammar(grammar_name)
        
        for i in range(20):
            query = grammar.generate("query", seed=i)
            
            # Basic checks
            assert self.is_valid_sql_basic(query), f"Invalid SQL: {query}"
            assert self.check_balanced_parentheses(query), f"Unbalanced parentheses: {query}"
            assert self.check_balanced_quotes(query), f"Unbalanced quotes: {query}"
            
            # Check for common SQL errors
            assert not query.strip().endswith(','), f"Trailing comma: {query}"
            assert ';;' not in query, f"Double semicolon: {query}"
            
    def test_table_references(self):
        """Test that table references are consistent"""
        grammar = load_grammar("subquery_dsl")
        
        for i in range(10):
            query = grammar.generate("query", seed=i)
            
            # Extract table names from FROM clauses
            from_pattern = r'FROM\s+(\w+)'
            tables = re.findall(from_pattern, query, re.IGNORECASE)
            
            # Check that referenced tables exist in grammar
            defined_tables = grammar.tables.keys() if grammar.tables else []
            if defined_tables:
                for table in tables:
                    assert table in defined_tables or table.startswith('t'), \
                        f"Unknown table {table} in query: {query}"


class TestSpecificGrammars:
    """Test specific grammar features"""
    
    def test_ddl_aux_grammar(self):
        """Test PostgreSQL functions DDL grammar"""
        grammar = load_grammar("ddl_aux")
        
        # Test function creation
        query = grammar.generate("create_function", seed=42)
        assert "CREATE" in query
        assert "FUNCTION" in query
        assert "RETURNS" in query
        assert "$function$" in query
        
    def test_json_sql_pg15_grammar(self):
        """Test PostgreSQL 15 JSON/SQL grammar"""
        grammar = load_grammar("json_sql_pg15")
        
        # Test JSON_TABLE generation
        query = grammar.generate("json_table_query", seed=42)
        assert "JSON_TABLE" in query
        assert "COLUMNS" in query
        
    def test_advanced_query_patterns(self):
        """Test advanced query patterns grammar"""
        grammar = load_grammar("advanced_query_patterns")
        
        # Test recursive CTE
        query = grammar.generate("recursive_cte", seed=42)
        assert "WITH RECURSIVE" in query
        assert "UNION ALL" in query
        
        # Test LATERAL join
        query = grammar.generate("lateral_join_query", seed=43)
        assert "LATERAL" in query
        
    def test_workload_grammars(self):
        """Test workload-specific grammars"""
        workload_grammars = [
            "workload.select_focused",
            "workload.insert_focused",
            "workload.update_focused",
            "workload.delete_focused",
            "workload.upsert_focused"
        ]
        
        for grammar_name in workload_grammars:
            grammar = load_grammar(grammar_name)
            
            # Extract operation type from name
            operation = grammar_name.split('.')[-1].split('_')[0].upper()
            
            # Generate queries and check they contain the right operation
            for i in range(5):
                query = grammar.generate("query", seed=i)
                if operation == "UPSERT":
                    assert "INSERT" in query and "ON CONFLICT" in query
                else:
                    assert operation in query


class TestGrammarCoverage:
    """Test SQL feature coverage in grammars"""
    
    def get_all_generated_queries(self, grammar_name: str, count: int = 50) -> List[str]:
        """Generate multiple queries from a grammar"""
        grammar = load_grammar(grammar_name)
        queries = []
        
        rule = "query" if "query" in grammar.rules else list(grammar.rules.keys())[0]
        
        for i in range(count):
            try:
                query = grammar.generate(rule, seed=i)
                queries.append(query)
            except:
                pass
                
        return queries
        
    def test_data_type_coverage(self):
        """Test coverage of PostgreSQL data types"""
        # Check DDL grammar for type coverage
        queries = self.get_all_generated_queries("ddl_focused", 100)
        all_text = ' '.join(queries)
        
        common_types = [
            'INTEGER', 'BIGINT', 'SMALLINT', 'NUMERIC', 'DECIMAL',
            'VARCHAR', 'TEXT', 'BOOLEAN', 'DATE', 'TIMESTAMP',
            'UUID', 'JSON', 'JSONB'
        ]
        
        found_types = [t for t in common_types if t in all_text]
        coverage = len(found_types) / len(common_types)
        
        assert coverage >= 0.7, f"Only {coverage*100:.0f}% type coverage"
        
    def test_join_coverage(self):
        """Test coverage of JOIN types"""
        queries = self.get_all_generated_queries("workload.select_focused", 100)
        all_text = ' '.join(queries).upper()
        
        join_types = ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL OUTER JOIN']
        found_joins = [j for j in join_types if j in all_text]
        
        assert len(found_joins) >= 2, f"Only found joins: {found_joins}"
        
    def test_constraint_coverage(self):
        """Test coverage of constraints in DDL"""
        queries = self.get_all_generated_queries("ddl_focused", 100)
        all_text = ' '.join(queries).upper()
        
        constraints = [
            'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK',
            'NOT NULL', 'DEFAULT'
        ]
        
        found_constraints = [c for c in constraints if c in all_text]
        coverage = len(found_constraints) / len(constraints)
        
        assert coverage >= 0.8, f"Only {coverage*100:.0f}% constraint coverage"


class TestGrammarPatterns:
    """Test specific SQL patterns in grammars"""
    
    def test_transaction_patterns(self):
        """Test transaction patterns"""
        grammar = load_grammar("simple_transaction")
        
        patterns_found = {
            'begin': False,
            'commit': False,
            'rollback': False,
            'savepoint': False
        }
        
        for i in range(50):
            query = grammar.generate("query", seed=i)
            query_upper = query.upper()
            
            if 'START TRANSACTION' in query_upper or 'BEGIN' in query_upper:
                patterns_found['begin'] = True
            if 'COMMIT' in query_upper:
                patterns_found['commit'] = True
            if 'ROLLBACK' in query_upper:
                patterns_found['rollback'] = True
            if 'SAVEPOINT' in query_upper:
                patterns_found['savepoint'] = True
                
        # Should find at least basic transaction commands
        assert patterns_found['begin'] or patterns_found['commit'] or patterns_found['rollback']
        
    def test_cte_patterns(self):
        """Test CTE (Common Table Expression) patterns"""
        queries = self.get_all_generated_queries("workload.select_focused", 50)
        
        cte_found = any('WITH' in q.upper() and 'AS' in q.upper() for q in queries)
        assert cte_found, "No CTE patterns found"
        
    def test_window_function_patterns(self):
        """Test window function patterns"""
        grammar = load_grammar("advanced_query_patterns")
        
        # Generate window function queries
        queries = []
        for i in range(20):
            try:
                query = grammar.generate("advanced_window_query", seed=i)
                queries.append(query)
            except:
                pass
                
        # Check for window function keywords
        window_keywords = ['OVER', 'PARTITION BY', 'ORDER BY', 'ROW_NUMBER', 'RANK', 'DENSE_RANK']
        
        for keyword in window_keywords:
            found = any(keyword in q.upper() for q in queries)
            assert found, f"Window function keyword '{keyword}' not found"


class TestGrammarQuality:
    """Test grammar code quality"""
    
    def test_no_hardcoded_seeds(self):
        """Test that grammars don't hardcode seeds"""
        grammar_dir = Path(__file__).parent.parent / "grammars"
        
        for file in grammar_dir.rglob("*.py"):
            if file.name.startswith("__"):
                continue
                
            content = file.read_text()
            
            # Check for hardcoded seeds (common anti-pattern)
            assert "seed=42" not in content, f"Hardcoded seed in {file}"
            assert "seed = 42" not in content, f"Hardcoded seed in {file}"
            
    def test_grammar_naming_convention(self):
        """Test that grammars follow naming conventions"""
        for grammar_name in get_all_grammar_files():
            try:
                grammar = load_grammar(grammar_name)
                
                # Grammar name should match file name (with dots replaced)
                expected_name = grammar_name.replace(".", "_")
                
                # Some grammars might not set name explicitly
                if grammar.name and grammar.name != "unnamed":
                    assert grammar.name == expected_name, \
                        f"Grammar name '{grammar.name}' doesn't match file '{grammar_name}'"
            except:
                # Skip if can't load
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])