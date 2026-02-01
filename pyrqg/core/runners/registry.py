"""
Runner Registry

Manages registration and discovery of database runners.
Supports both built-in and user-defined runners.
"""

from __future__ import annotations

import logging
from typing import Dict, Type, Optional, List

from pyrqg.core.runners.base import Runner, RunnerConfig

logger = logging.getLogger(__name__)


class RunnerRegistry:
    """Registry for database runners.

    Example:
        # Register a custom runner
        RunnerRegistry.register(MyCustomRunner)

        # Get a runner by name
        runner = RunnerRegistry.get("postgresql", dsn="...")

        # List available runners
        for name, desc in RunnerRegistry.list_runners().items():
            print(f"{name}: {desc}")
    """

    _runners: Dict[str, Type[Runner]] = {}

    @classmethod
    def register(cls, runner_class: Type[Runner], name: Optional[str] = None) -> None:
        """Register a runner class.

        Args:
            runner_class: The runner class to register
            name: Optional name override (defaults to runner_class.name)
        """
        runner_name = name or runner_class.name
        cls._runners[runner_name] = runner_class
        logger.debug("Registered runner: %s", runner_name)

    @classmethod
    def unregister(cls, name: str) -> bool:
        """Unregister a runner by name.

        Returns:
            True if runner was removed, False if not found
        """
        if name in cls._runners:
            del cls._runners[name]
            return True
        return False

    @classmethod
    def get(cls, name: str, config: Optional[RunnerConfig] = None, **kwargs) -> Runner:
        """Get a runner instance by name.

        Args:
            name: Runner name (e.g., 'postgresql', 'ysql', 'ycql')
            config: Optional RunnerConfig
            **kwargs: Additional arguments passed to runner constructor

        Returns:
            Configured runner instance

        Raises:
            ValueError: If runner not found
        """
        if name not in cls._runners:
            available = ", ".join(sorted(cls._runners.keys()))
            raise ValueError(f"Runner '{name}' not found. Available: {available}")

        runner_class = cls._runners[name]
        return runner_class(config=config, **kwargs)

    @classmethod
    def get_for_api(cls, target_api: str, config: Optional[RunnerConfig] = None, **kwargs) -> Runner:
        """Get a runner suitable for a target API.

        Args:
            target_api: The target API ('ysql', 'ycql', 'postgres', etc.)
            config: Optional RunnerConfig
            **kwargs: Additional arguments passed to runner constructor

        Returns:
            Configured runner instance
        """
        # Map API to runner name
        api_mapping = {
            'ysql': 'ysql',
            'ycql': 'ycql',
            'postgres': 'postgresql',
            'postgresql': 'postgresql',
            'sql': 'postgresql',
        }

        runner_name = api_mapping.get(target_api.lower(), 'postgresql')
        return cls.get(runner_name, config=config, **kwargs)

    @classmethod
    def list_runners(cls) -> Dict[str, str]:
        """List all registered runners with descriptions.

        Returns:
            Dict mapping runner names to descriptions
        """
        return {
            name: runner_class.description
            for name, runner_class in sorted(cls._runners.items())
        }

    @classmethod
    def get_runner_class(cls, name: str) -> Optional[Type[Runner]]:
        """Get a runner class by name without instantiating.

        Returns:
            Runner class or None if not found
        """
        return cls._runners.get(name)

    @classmethod
    def is_registered(cls, name: str) -> bool:
        """Check if a runner is registered."""
        return name in cls._runners

    @classmethod
    def available_runners(cls) -> List[str]:
        """Get list of available runner names."""
        return list(cls._runners.keys())


def register_runner(name: Optional[str] = None):
    """Decorator to register a runner class.

    Example:
        @register_runner("my_database")
        class MyDatabaseRunner(Runner):
            ...
    """
    def decorator(runner_class: Type[Runner]) -> Type[Runner]:
        RunnerRegistry.register(runner_class, name)
        return runner_class
    return decorator
