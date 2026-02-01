"""Tests for the grammar loader module."""

import pytest
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from pyrqg.core.grammar_loader import GrammarLoader


class TestGrammarLoader:
    """Test the GrammarLoader class."""

    @pytest.fixture
    def loader(self):
        return GrammarLoader()

    def test_init(self, loader):
        assert loader.grammars == {}

    def test_load_builtin_success(self, loader):
        result = loader.load_builtin('basic_crud', 'grammars.basic_crud')
        assert result is True
        assert 'basic_crud' in loader.grammars

    def test_load_builtin_no_g_attribute(self, loader):
        # Load a module that exists but doesn't have 'g'
        result = loader.load_builtin('os_module', 'os', grammar_attr='g')
        assert result is False

    def test_load_builtin_import_error(self, loader):
        result = loader.load_builtin('nonexistent', 'grammars.nonexistent_module')
        assert result is False

    def test_load_from_file_success(self, loader, tmp_path):
        # Create a valid grammar file
        grammar_file = tmp_path / "test_grammar.py"
        grammar_file.write_text("""
class MockGrammar:
    def generate(self, rule, **kwargs):
        return "SELECT 1"

g = MockGrammar()
""")
        result = loader.load_from_file('test', str(grammar_file))
        assert result is True
        assert 'test' in loader.grammars

    def test_load_from_file_not_found(self, loader):
        result = loader.load_from_file('missing', '/nonexistent/path/grammar.py')
        assert result is False

    def test_load_from_file_no_g_attribute(self, loader, tmp_path):
        grammar_file = tmp_path / "no_g.py"
        grammar_file.write_text("x = 1")
        result = loader.load_from_file('no_g', str(grammar_file))
        assert result is False

    def test_load_from_file_syntax_error(self, loader, tmp_path):
        grammar_file = tmp_path / "syntax_error.py"
        grammar_file.write_text("def broken(")
        result = loader.load_from_file('broken', str(grammar_file))
        assert result is False

    def test_load_by_name_already_loaded(self, loader):
        loader.grammars['existing'] = MagicMock()
        result = loader.load_by_name('existing')
        assert result is True

    def test_load_by_name_as_module(self, loader):
        result = loader.load_by_name('basic_crud')
        assert result is True
        assert 'basic_crud' in loader.grammars

    def test_load_by_name_not_found(self, loader):
        result = loader.load_by_name('totally_nonexistent_grammar_xyz')
        assert result is False

    def test_load_from_env_empty(self, loader):
        with patch.dict(os.environ, {'PYRQG_GRAMMARS': ''}, clear=True):
            result = loader.load_from_env()
        assert result == 0

    def test_load_from_env_not_set(self, loader):
        with patch.dict(os.environ, {}, clear=True):
            if 'PYRQG_GRAMMARS' in os.environ:
                del os.environ['PYRQG_GRAMMARS']
            result = loader.load_from_env()
        assert result == 0

    def test_get_grammar(self, loader):
        mock_grammar = MagicMock()
        loader.grammars['test'] = mock_grammar
        result = loader.get('test')
        assert result == mock_grammar

    def test_get_grammar_not_found(self, loader):
        result = loader.get('nonexistent')
        assert result is None

    def test_list_names(self, loader):
        loader.grammars['a'] = MagicMock()
        loader.grammars['b'] = MagicMock()
        names = loader.list_names()
        assert set(names) == {'a', 'b'}

    def test_unique_name_no_conflict(self, loader):
        name = loader._unique_name('test')
        assert name == 'test'

    def test_unique_name_with_conflict(self, loader):
        loader.grammars['test'] = MagicMock()
        name = loader._unique_name('test')
        assert name == 'test_2'

    def test_unique_name_multiple_conflicts(self, loader):
        loader.grammars['test'] = MagicMock()
        loader.grammars['test_2'] = MagicMock()
        loader.grammars['test_3'] = MagicMock()
        name = loader._unique_name('test')
        assert name == 'test_4'

    def test_load_by_name_with_custom_dir(self, loader, tmp_path):
        # Create a grammar file in a custom directory
        grammar_file = tmp_path / "custom_grammar.py"
        grammar_file.write_text("""
class Grammar:
    def generate(self, rule, **kwargs):
        return "SELECT 1"
g = Grammar()
""")
        result = loader.load_by_name('custom_grammar', grammars_dir=tmp_path)
        assert result is True
        assert 'custom_grammar' in loader.grammars

    def test_load_by_name_with_dotted_path(self, loader, tmp_path):
        # Create a nested grammar file
        subdir = tmp_path / "sub"
        subdir.mkdir()
        grammar_file = subdir / "nested.py"
        grammar_file.write_text("""
class Grammar:
    def generate(self, rule, **kwargs):
        return "SELECT 1"
g = Grammar()
""")
        result = loader.load_by_name('sub.nested', grammars_dir=tmp_path)
        assert result is True


class TestGrammarLoaderEnv:
    """Test grammar loading from environment variable."""

    @pytest.fixture
    def loader(self):
        return GrammarLoader()

    def test_load_from_env_valid_module(self, loader):
        # Use a real module that has 'g'
        with patch.dict(os.environ, {'PYRQG_GRAMMARS': 'grammars.basic_crud'}):
            result = loader.load_from_env()
        assert result == 1
        assert 'basic_crud' in loader.grammars

    def test_load_from_env_multiple_modules(self, loader):
        with patch.dict(os.environ, {'PYRQG_GRAMMARS': 'grammars.basic_crud,grammars.real_workload'}):
            result = loader.load_from_env()
        assert result == 2

    def test_load_from_env_invalid_module(self, loader):
        with patch.dict(os.environ, {'PYRQG_GRAMMARS': 'nonexistent.module'}):
            result = loader.load_from_env()
        assert result == 0

    def test_load_from_env_module_without_g(self, loader):
        with patch.dict(os.environ, {'PYRQG_GRAMMARS': 'os'}):
            result = loader.load_from_env()
        assert result == 0

    def test_load_from_env_custom_var(self, loader):
        with patch.dict(os.environ, {'CUSTOM_GRAMMARS': 'grammars.basic_crud'}):
            result = loader.load_from_env(env_var='CUSTOM_GRAMMARS')
        assert result == 1
