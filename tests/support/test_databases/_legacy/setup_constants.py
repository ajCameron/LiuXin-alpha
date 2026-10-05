"""
Support retained legacy database-fixture setup constants behavior.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise setup constants through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""

test_asset_version = "1_2_1"
