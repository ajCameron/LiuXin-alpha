"""
Mark the driver contract fixture boundary.

The top-level tests/conftest.py registers fixture_plugin once for the driver and
database contract suites; this module registers no additional plugin.

Example:
    Run the shared fixtures through a contract test::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py
"""
