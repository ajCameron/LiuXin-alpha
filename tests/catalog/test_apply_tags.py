"""Exercise Tag dispatch, reuse and aggregate rollback through public Apply."""

from __future__ import annotations

from importlib import import_module

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


def _work(catalog, db):
    work_id = catalog.works.create({"title": "Tag application"})
    return Row(db, dict(catalog.works.require(work_id)))


def test_tag_text_rows_and_iterables_reuse_complete_values(db):
    catalog = Catalog(db)
    work = _work(catalog, db)
    catalog.apply.tag("Science fiction", work)
    first = catalog.ensure.tag("Science fiction")
    catalog.apply.tag(
        (value for value in (" SCIENCE FICTION ", "Adventure", first)), work
    )
    spec = db.driver_wrapper.get_link_spec("works", "tags")
    rows = db.macros.get_link_rows(spec, work.row_id)
    names = {catalog.tags.require(row.secondary_id)["tag"] for row in rows}
    assert names == {"Science fiction", "Adventure"}
    assert len(rows) == 2


@pytest.mark.parametrize("bad_value", [object(), b"bytes", {"tag": "mapping"}])
def test_later_invalid_tag_rolls_back_created_values_and_links(db, bad_value):
    catalog = Catalog(db)
    work = _work(catalog, db)
    before = catalog.tags.list()
    with pytest.raises(InputIntegrityError):
        catalog.apply.tag(["Rollback candidate", bad_value], work)
    assert catalog.tags.list() == before
    spec = db.driver_wrapper.get_link_spec("works", "tags")
    assert not db.macros.get_link_rows(spec, work.row_id)


def test_later_tag_link_failure_propagates_and_rolls_back(db, monkeypatch):
    catalog = Catalog(db)
    work = _work(catalog, db)
    before = catalog.tags.list()
    module = import_module(
        "LiuXin_alpha.catalog.metadata_tools.apply.label_apply_mixin"
    )
    original = module.link_rows
    calls = 0

    def fail_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise DatabaseIntegrityError("injected relationship failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "link_rows", fail_second)
    with pytest.raises(DatabaseIntegrityError, match="injected"):
        catalog.apply.tag(["First rollback tag", "Second rollback tag"], work)
    assert calls == 2
    assert catalog.tags.list() == before
    spec = db.driver_wrapper.get_link_spec("works", "tags")
    assert not db.macros.get_link_rows(spec, work.row_id)


@pytest.mark.parametrize("value", ["Uncreated tag", []])
def test_invalid_resource_is_rejected_before_resolving_tags(db, value):
    catalog = Catalog(db)
    before = catalog.tags.list()
    with pytest.raises(InputIntegrityError, match="Resource"):
        catalog.apply.tag(value, object())
    assert catalog.tags.list() == before
