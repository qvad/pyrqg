"""Test configuration to ensure the local package is importable.

Adds the repository root to sys.path so `import pyrqg` works when running tests
without installing the package.
"""

import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
