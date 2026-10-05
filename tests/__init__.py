"""
Mark the repository test suite as an importable package.

The initializer deliberately performs no setup; pytest owns suite configuration
through the root conftest and registered fixture plugins.

Example:
    Exercise   init   through a regression that loads the root fixture configuration::

        python -m pytest -q tests/scripts/test_docstring_ownership.py
"""
