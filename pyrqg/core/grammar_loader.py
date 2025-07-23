"""
Grammar Loading Utilities for PyRQG.

Handles loading grammars from:
- Built-in package grammars
- Plugin modules via PYRQG_GRAMMARS environment variable
- File paths
"""
import os
import logging
import importlib.util
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class GrammarLoader:
    """Loads and manages grammars from various sources."""

    def __init__(self):
        self.grammars: Dict[str, Any] = {}

    def load_builtin(self, name: str, module_path: str, grammar_attr: str = 'g') -> bool:
        """
        Load a built-in grammar module.

        Args:
            name: Name to register the grammar under.
            module_path: Dotted module path (e.g., 'grammars.ddl_focused').
            grammar_attr: Attribute name containing the grammar object.

        Returns:
            True if loaded successfully, False otherwise.
        """
        try:
            module = __import__(module_path, fromlist=[grammar_attr])
            if hasattr(module, grammar_attr):
                self.grammars[name] = getattr(module, grammar_attr)
                return True
            else:
                logger.warning("Module '%s' does not expose '%s'", module_path, grammar_attr)
                return False
        except ImportError as e:
            logger.warning("Could not import builtin grammar '%s': %s", module_path, e)
            return False

    def load_from_env(self, env_var: str = "PYRQG_GRAMMARS") -> int:
        """
        Load grammars from environment variable.

        The environment variable should contain comma-separated module paths.

        Args:
            env_var: Name of the environment variable.

        Returns:
            Number of grammars successfully loaded.
        """
        value = os.environ.get(env_var)
        if not value:
            return 0

        loaded = 0
        for module_path in [p.strip() for p in value.split(',') if p.strip()]:
            try:
                module = __import__(module_path, fromlist=['g'])
                if hasattr(module, 'g'):
                    name = self._unique_name(module_path.split('.')[-1])
                    self.grammars[name] = getattr(module, 'g')
                    loaded += 1
                else:
                    logger.warning("Grammar module '%s' does not expose 'g'", module_path)
            except Exception as e:
                logger.warning("Failed to load plugin grammar '%s': %s", module_path, e)

        return loaded

    def load_from_file(self, name: str, file_path: str, grammar_attr: str = 'g') -> bool:
        """
        Load a grammar from a Python file.

        Args:
            name: Name to register the grammar under.
            file_path: Path to the Python file.
            grammar_attr: Attribute name containing the grammar object.

        Returns:
            True if loaded successfully, False otherwise.
        """
        path = Path(file_path)
        if not path.exists():
            logger.error("Grammar file not found: %s", file_path)
            return False

        try:
            spec = importlib.util.spec_from_file_location(f"grammar_{name}", str(path))
            if spec is None or spec.loader is None:
                logger.error("Could not load spec for: %s", file_path)
                return False

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            if hasattr(module, grammar_attr):
                self.grammars[name] = getattr(module, grammar_attr)
                return True
            else:
                logger.error("File '%s' does not expose '%s'", file_path, grammar_attr)
                return False
        except Exception as e:
            logger.error("Failed to load grammar from '%s': %s", file_path, e)
            return False

    def load_by_name(self, grammar_name: str, grammars_dir: Optional[Path] = None) -> bool:
        """
        Attempt to load a grammar by name, searching common locations.

        Args:
            grammar_name: Name of the grammar to load.
            grammars_dir: Optional directory to search for grammar files.

        Returns:
            True if loaded successfully, False otherwise.
        """
        if grammar_name in self.grammars:
            return True

        # Try as module path
        module_path = f"grammars.{grammar_name.replace('/', '.')}".rstrip('.')
        try:
            module = __import__(module_path, fromlist=['g'])
            if hasattr(module, 'g'):
                self.grammars[grammar_name] = getattr(module, 'g')
                return True
        except ImportError:
            pass

        # Try as file path
        if grammars_dir is None:
            grammars_dir = Path(__file__).parent.parent.parent / "grammars"

        rel_path = Path(*grammar_name.replace('.', '/').split('/'))
        file_path = grammars_dir / (str(rel_path) + ".py")

        if not file_path.exists():
            rel_path_alt = Path(*grammar_name.split('.'))
            file_path_alt = grammars_dir / (str(rel_path_alt) + ".py")
            if file_path_alt.exists():
                file_path = file_path_alt

        if file_path.exists():
            return self.load_from_file(grammar_name, str(file_path))

        return False

    def get(self, name: str) -> Optional[Any]:
        """Get a grammar by name."""
        return self.grammars.get(name)

    def list_names(self) -> list:
        """List all loaded grammar names."""
        return list(self.grammars.keys())

    def _unique_name(self, base_name: str) -> str:
        """Generate a unique name for a grammar."""
        name = base_name
        i = 2
        while name in self.grammars:
            name = f"{base_name}_{i}"
            i += 1
        return name
