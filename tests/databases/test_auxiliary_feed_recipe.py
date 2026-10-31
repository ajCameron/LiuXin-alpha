"""Execute the documented feed-row recipe through portable transactions."""

from pathlib import Path

import pytest


@pytest.fixture
def replace_feeds():
    guide = (
        Path(__file__).resolve().parents[2]
        / "dev-docs/databases/metadata-sql-wemi-audit-2026-10-29.md"
    ).read_text()
    code = guide.split("```python\n", 1)[1].split("```", 1)[0]
    namespace = {}
    exec(compile(code, "<documented-feed-recipe>", "exec"), namespace)
    return namespace["replace_feed_rows"]


def test_feed_recipe_is_atomic_for_late_failure_and_enclosing_rollback(
    db, monkeypatch, replace_feeds
):
    db.macros.insert_row("feeds", {"feed_title": "Keep", "feed_script": "keep recipe"})
    before = db.macros.get_rows("feeds")
    original_insert = db.macros.insert_row

    def fail_second(table, values, **kwargs):
        if table == "feeds" and values["feed_title"] == "Fail":
            raise RuntimeError("insertion failed")
        return original_insert(table, values, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(db.macros, "insert_row", fail_second)
        with pytest.raises(RuntimeError, match="insertion failed"):
            replace_feeds(db, [("First", "recipe"), ("Fail", "recipe")])
    assert db.macros.get_rows("feeds") == before
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            replace_feeds(db, [("New", "new recipe")])
            assert db.macros.get_rows("feeds")[0]["feed_title"] == "New"
            raise RuntimeError("rollback")
    assert db.macros.get_rows("feeds") == before
    replace_feeds(db, [("New", "new recipe")])
    assert [
        (r["feed_title"], r["feed_script"]) for r in db.macros.get_rows("feeds")
    ] == [("New", "new recipe")]
    replace_feeds(db, [])
    assert not db.macros.get_rows("feeds")


def test_feed_recipe_materializes_input_before_deleting(db, replace_feeds):
    db.macros.insert_row("feeds", {"feed_title": "Keep"})
    before = db.macros.get_rows("feeds")

    def broken_input():
        yield "First", "recipe"
        raise RuntimeError("input failed")

    with pytest.raises(RuntimeError, match="input failed"):
        replace_feeds(db, broken_input())
    assert db.macros.get_rows("feeds") == before
