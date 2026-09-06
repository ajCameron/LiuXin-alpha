"""Writer implementation imports preserve package exports and field dispatch."""

import importlib
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.caches import write
from LiuXin_alpha.databases.db_types import MANY_MANY, MANY_ONE, ONE_MANY

SOURCE_ROOT = Path(__file__).resolve().parents[3] / "src"
WRITER_OWNERS = {
    "BaseWriter": "base_writer",
    "AuthorSortWriter": "author_sort_writer",
    "CustomSeriesIndexWriter": "custom_columns_writers",
    "IdentifiersWrite": "identifiers_writer",
    "LanguagesWriter": "languages_writer",
    "TitleWriter": "title_writer",
    "UUIDWriter": "uuid_writer",
    "CoversWrite": "covers_writer",
    "DummyWriter": "utils",
    "OneToOneWriter": "generic_writers.one_to_one_writer",
    "ManyToOneWriter": "generic_writers.many_to_one_writer",
    "ManyToManyWriter": "generic_writers.many_to_many_writer",
    "OneToManyWriter": "generic_writers.one_to_many_writer",
}


@pytest.mark.parametrize("first", tuple(WRITER_OWNERS.values()))
def test_writer_imports_are_safe_from_each_entry_point(first: str) -> None:
    source = f"""
import importlib
import sys
sys.path.insert(0, {str(SOURCE_ROOT)!r})
importlib.import_module('LiuXin_alpha.caches.write.' + {first!r})
package = importlib.import_module('LiuXin_alpha.caches.write')
for name, owner in {WRITER_OWNERS!r}.items():
    module = importlib.import_module('LiuXin_alpha.caches.write.' + owner)
    assert getattr(package, name) is getattr(module, name), name
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", source],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    ("name", "table_type", "expected"),
    [
        ("title", None, "TitleWriter"),
        ("author_sort", None, "AuthorSortWriter"),
        ("identifiers", None, "IdentifiersWrite"),
        ("languages", None, "LanguagesWriter"),
        ("uuid", None, "UUIDWriter"),
        ("cover", None, "CoversWrite"),
        ("#series_index", None, "CustomSeriesIndexWriter"),
        ("id", None, "DummyWriter"),
        ("publisher", None, "ManyToManyWriter"),
        ("ordinary", None, "OneToOneWriter"),
        ("ordinary", MANY_ONE, "ManyToOneWriter"),
        ("ordinary", MANY_MANY, "ManyToManyWriter"),
        ("ordinary", ONE_MANY, "OneToManyWriter"),
    ],
)
def test_field_dispatch_constructs_original_writer_classes(
    name: str, table_type: object, expected: str
) -> None:
    field = SimpleNamespace(
        name=name,
        metadata={
            "datatype": "text",
            "is_multiple": False,
            "table": "books",
            "column": name,
        },
        table=SimpleNamespace(
            name="books", table_type=table_type, typed=False, priority=False
        ),
        is_many=False,
        is_many_many=False,
    )
    result = write.get_writer(field)
    owner = importlib.import_module(
        "LiuXin_alpha.caches.write." + WRITER_OWNERS[expected]
    )
    assert type(result) is getattr(owner, expected)
    assert result.field is field
