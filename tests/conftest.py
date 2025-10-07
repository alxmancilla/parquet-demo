"""Pytest configuration to ensure the project's `src/` directory is on sys.path

This makes it possible to run tests without installing the package into the
virtual environment (editable install). It's a small, explicit convenience for
local development and CI jobs that run tests from the repository root.
"""
from pathlib import Path
import sys

_root = Path(__file__).resolve().parents[1]
_src = str((_root / 'src').resolve())
if _src not in sys.path:
    # insert at front so local package takes precedence
    sys.path.insert(0, _src)
