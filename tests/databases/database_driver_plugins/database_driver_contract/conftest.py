"""Driver contract suite conftest.

Intentionally empty.

Shared fixtures are loaded from the *top-level* ``tests/conftest.py`` via
``tests.databases.database_driver_plugins.database_driver_contract.fixture_plugin``.

Modern pytest versions deprecate defining ``pytest_plugins`` in nested conftest
files, so this file must not register plugins.
"""
