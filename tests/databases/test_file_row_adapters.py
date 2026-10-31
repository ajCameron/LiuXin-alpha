"""Exercise mapped file-row updates after retiring metadata SQL wrappers."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.specific_many_to_many_tables import (
    CalibreFormatsTable,
)


def file_cache(file_id):
    return SimpleNamespace(
        book_file_map={7: {"EPUB_1": file_id}},
        fname_map={7: {"EPUB_1": "original.epub"}},
        size_map={7: {"EPUB_1": 12}},
        book_col_map={7: ("EPUB_1",)},
        col_book_map={"EPUB": {7}},
        check_fmt_is_priority_fmt=CalibreFormatsTable.check_fmt_is_priority_fmt,
    )


def test_mapped_file_name_and_size_use_current_columns_without_touching_bytes(
    db, tmp_path
):
    physical = tmp_path / "original.epub"
    physical.write_bytes(b"source bytes")
    file_id = db.macros.insert_row(
        "files", {"file_name": physical.name, "file_size_bytes": 12}
    )
    cache = file_cache(file_id)
    CalibreFormatsTable.set_fname(cache, 7, "EPUB_1", "renamed.epub", db)
    assert db.macros.get_row("files", file_id)["file_name"] == "renamed.epub"
    assert cache.fname_map[7]["EPUB_1"] == "renamed.epub"
    CalibreFormatsTable.update_fmt(cache, 7, "EPUB", "second.epub", 99, db)
    CalibreFormatsTable.update_fmt(cache, 7, "EPUB_1", None, 100, db)
    row = db.macros.get_row("files", file_id)
    assert (row["file_name"], row["file_size_bytes"]) == ("second.epub", 100)
    assert cache.fname_map[7]["EPUB_1"] == "second.epub"
    assert cache.size_map[7]["EPUB_1"] == 100
    assert cache.col_book_map == {"EPUB": {7}}
    assert cache.book_col_map == {7: ("EPUB_1",)}
    assert physical.read_bytes() == b"source bytes"
    assert not (tmp_path / "renamed.epub").exists()
    assert not (tmp_path / "second.epub").exists()


@pytest.mark.parametrize("method", ["set_fname", "update_fmt"])
def test_database_failure_leaves_file_cache_unchanged(db, monkeypatch, method):
    file_id = db.macros.insert_row("files", {"file_name": "original.epub"})
    cache = file_cache(file_id)
    before = deepcopy(vars(cache))

    def fail(*args, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(db.macros, "update_row", fail)
    arguments = (7, "EPUB_1", "changed.epub")
    if method == "update_fmt":
        arguments += (99,)
    with pytest.raises(RuntimeError, match="write failed"):
        getattr(CalibreFormatsTable, method)(cache, *arguments, db)
    assert vars(cache) == before
    assert db.macros.get_row("files", file_id)["file_name"] == "original.epub"


def test_mapped_file_updates_do_not_commit_an_enclosing_transaction(db):
    file_id = db.macros.insert_row(
        "files", {"file_name": "original.epub", "file_size_bytes": 12}
    )
    cache = file_cache(file_id)
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            CalibreFormatsTable.set_fname(cache, 7, "EPUB_1", "pending.epub", db)
            CalibreFormatsTable.update_fmt(
                cache, 7, "EPUB", "pending-again.epub", 99, db
            )
            raise RuntimeError("rollback")
    row = db.macros.get_row("files", file_id)
    assert (row["file_name"], row["file_size_bytes"]) == ("original.epub", 12)
    # These legacy cache objects still require caller reconciliation after rollback.
    assert cache.fname_map[7]["EPUB_1"] == "pending-again.epub"
