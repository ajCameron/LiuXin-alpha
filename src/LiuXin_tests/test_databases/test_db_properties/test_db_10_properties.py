"""
Provide test db 10 properties utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test db 10 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
"""
from tests.support.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


class TestDB10Properties(CommonDBProperties):

    """
    Provide the testdb10properties contract for validated ebook processing.

    Example:
        Exercise TestDB10Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
    """
    theo_title_count = 98
    theo_series_count = 42

    theo_main_tables = {
        "files",
        "publishers",
        "genres",
        "custom_columns",
        "folder_stores",
        "covers",
        "tags",
        "series",
        "notes",
        "identifiers",
        "devices",
        "folders",
        "languages",
        "last_read_positions",
        "books",
        "comments",
        "synopses",
        "titles",
        "feeds",
        "creators",
        "subjects",
    }

    series_41_val = "Dark Tide Rising"
