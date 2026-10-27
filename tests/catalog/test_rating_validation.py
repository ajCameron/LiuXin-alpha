"""Exercise the shared rating value/scale boundary through supported write routes."""

from __future__ import annotations

from collections import OrderedDict
from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.api.common import MetadataCandidate
from LiuXin_alpha.catalog.row_mutations import create_row, persist_row
from LiuXin_alpha.core.authorization import CoreTransportPolicy
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.proxies.remote import RemoteCoreClient, RemoteProxyError
from LiuXin_alpha.core.transport.http import CoreHttpDaemon
from tests.catalog.test_write_boundary_parity import write_runtime as write_runtime


def test_rating_creation_routes_reject_invalid_pairs_without_writes(
    write_runtime, monkeypatch
):
    """Repository, matching, Row-shaped and Core writes share value rules."""
    runtime, catalog, db = write_runtime
    invalid = (
        {"rating": -1},
        {"rating": True},
        {"rating": float("nan")},
        {"rating": float("inf")},
        {"rating": "not a number"},
        {"rating": 6, "rating_out_of": 5},
        {"rating_out_of": 0},
        {"rating_out_of": -1},
        {"rating_out_of": 2.5},
        {"rating_out_of": True},
    )
    before = db.macros.get_rows("ratings")
    for data in invalid:
        for operation in (
            lambda data=data: catalog.ratings.create(data),
            lambda data=data: catalog.ratings.match_or_create(MetadataCandidate(data)),
            lambda data=data: create_row(catalog, data, table="ratings"),
            lambda data=data: runtime.command(
                "catalog.entity.create", {"repository": "ratings", "data": data}
            ),
        ):
            with pytest.raises((ValueError, CoreHandlerError)):
                operation()
            assert db.macros.get_rows("ratings") == before

    def fail_readback(*args):
        raise RuntimeError("rating readback failed")

    with monkeypatch.context() as patch:
        patch.setattr(catalog.ratings, "require", fail_readback)
        with pytest.raises(RuntimeError, match="rating readback failed"):
            catalog.ratings.match_or_create(MetadataCandidate({"value": 4.25}))
    assert db.macros.get_rows("ratings") == before


def test_rating_partial_updates_preserve_omitted_fields_and_tolerant_reads(
    write_runtime,
):
    """Validate the effective pair; unrelated updates do not repair historical data."""
    runtime, catalog, db = write_runtime
    owner = catalog.ratings.create({"value": "4.25", "out_of": 5, "source": "reader"})
    catalog.ratings.update(owner, {"value": 4.5})
    row = catalog.ratings.require(owner)
    assert (row["rating"], row["rating_out_of"], row["rating_source"]) == (
        4.5,
        5,
        "reader",
    )
    with pytest.raises(ValueError):
        catalog.ratings.update(owner, {"out_of": 4})
    assert catalog.ratings.require(owner) == row
    with pytest.raises(CoreHandlerError):
        runtime.command(
            "catalog.entity.update",
            {"repository": "ratings", "entity_id": owner, "data": {"out_of": 4}},
        )
    draft = db.get_row_from_id("ratings", owner)
    draft["rating_out_of"] = 4
    with pytest.raises(ValueError):
        persist_row(catalog, draft)
    assert catalog.ratings.require(owner) == row
    catalog.write_one("ratings", "rating_out_of", owner, 10)
    assert catalog.ratings.require(owner)["rating"] == 4.5
    catalog.ratings.update(owner, {"value": None})
    assert catalog.ratings.require(owner)["rating"] is None
    assert catalog.ratings.require(owner)["rating_out_of"] == 10
    catalog.ratings.update(owner, {"out_of": None, "value": 20})
    assert catalog.ratings.require(owner)["rating_out_of"] is None
    # Raw historical data remains readable; reads and unrelated edits do not repair it.
    db.macros.update_row("ratings", owner, {"rating": -2, "rating_out_of": 5})
    assert catalog.ratings.require(owner)["rating"] == -2
    catalog.ratings.update(owner, {"source": "historical"})
    assert catalog.ratings.require(owner)["rating"] == -2
    with pytest.raises(ValueError):
        catalog.ratings.update(owner, {"out_of": 10})
    catalog.ratings.update(owner, {"value": 3})
    assert catalog.ratings.require(owner)["rating"] == 3


def test_rating_schema_batch_preflight_and_late_failure_are_atomic(
    write_runtime, monkeypatch
):
    """A bad second value or second backend write leaves both original rows intact."""
    _runtime, catalog, db = write_runtime
    first = catalog.ratings.create({"value": 2, "out_of": 5})
    second = catalog.ratings.create({"value": 3, "out_of": 5})
    before = db.macros.get_rows("ratings")
    original = db.macros.update_row
    calls = []

    def update(table, row_id, values, **kwargs):
        calls.append(row_id)
        if len(calls) == 2:
            raise RuntimeError("second rating write failed")
        return original(table, row_id, values, **kwargs)

    monkeypatch.setattr(db.macros, "update_row", update)
    with pytest.raises(ValueError):
        catalog.write("ratings", "rating", {first: 4, second: 6})
    assert calls == []
    assert db.macros.get_rows("ratings") == before
    with pytest.raises(RuntimeError, match="second rating write"):
        catalog.write("ratings", "rating", {first: 4, second: 5})
    assert calls == [first, second]
    assert db.macros.get_rows("ratings") == before


def test_rating_value_links_cannot_reuse_invalid_historical_values(write_runtime):
    """Supplying an invalid value is refused even if a raw historical row matches."""
    _runtime, catalog, db = write_runtime
    work = catalog.works.create({"title": "Rating reference"})
    invalid = db.macros.insert_row("ratings", {"rating": -1})
    before = db.macros.get_rows("rating_work_links")
    with pytest.raises(ValueError):
        catalog.write_one("works", "rating", work, -1)
    assert db.macros.get_rows("rating_work_links") == before
    assert catalog.ratings.require(invalid)["rating"] == -1
    invalid_pair = db.macros.insert_row(
        "ratings", {"rating": 20, "rating_out_of": 5, "rating_source": "invalid scale"}
    )
    with pytest.raises(ValueError):
        catalog.ratings.match_or_create(MetadataCandidate({"value": 20}))
    with pytest.raises(ValueError):
        catalog.ratings.match_or_create(MetadataCandidate({"source": "invalid scale"}))
    with pytest.raises(ValueError):
        catalog.write_one("works", "rating", work, 20)
    with pytest.raises(ValueError):
        catalog.apply.rating(
            db.get_row_from_id("ratings", invalid_pair),
            "user",
            db.get_row_from_id("works", work),
        )
    assert db.macros.get_rows("rating_work_links") == before
    catalog.write_one("works", "rating", work, 3.5)
    assert len(db.macros.get_rows("rating_work_links")) == len(before) + 1


def test_rating_legacy_ensure_and_apply_preserve_fractions_and_roll_back(
    write_runtime, monkeypatch
):
    """Compatibility inputs no longer infer Rating IDs or truncate values."""
    _runtime, catalog, db = write_runtime
    work_id = catalog.works.create({"title": "Compatibility rating"})
    work = db.get_row_from_id("works", work_id)
    ensured = catalog.ensure.rating(3.9)
    assert ensured["rating"] == 3.9
    assert catalog.ensure.rating(3.9).row_id == ensured.row_id
    for value in (-1, True, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            catalog.ensure.rating(value)
        with pytest.raises(ValueError):
            catalog.apply.rating(value, "user", work)
    catalog.apply.rating(4.25, "user", work)
    assert any(row["rating"] == 4.25 for row in db.macros.get_rows("ratings"))
    before = {
        table: db.macros.get_rows(table) for table in ("ratings", "rating_work_links")
    }

    def fail_link(*args, **kwargs):
        raise RuntimeError("rating attachment failed")

    monkeypatch.setattr(
        "LiuXin_alpha.catalog.metadata_tools.apply.link_rows", fail_link
    )
    with pytest.raises(RuntimeError, match="rating attachment failed"):
        catalog.apply.rating(2.75, "user", work)
    assert {table: db.macros.get_rows(table) for table in before} == before


def test_rating_http_writes_use_the_same_semantic_owner(write_runtime):
    """An authorized transport cannot bypass rating pair validation."""
    runtime, catalog, db = write_runtime
    token = "rating-test-token"
    daemon = CoreHttpDaemon(runtime, policy=CoreTransportPolicy.read_write(token))
    daemon.start()
    try:
        client = RemoteCoreClient(endpoint=daemon.base_url, bearer_token=token)
        before = db.macros.get_rows("ratings")
        with pytest.raises(RemoteProxyError):
            client.command(
                "catalog.entity.create",
                {"repository": "ratings", "data": {"value": 9, "out_of": 5}},
            )
        assert db.macros.get_rows("ratings") == before
        result = client.command(
            "catalog.entity.create",
            {"repository": "ratings", "data": {"value": 4.25, "out_of": 5}},
        )
        assert catalog.ratings.require(result["entity_id"])["rating"] == 4.25
    finally:
        daemon.stop()


def test_rating_container_import_preserves_fractions_and_rejects_late_values(
    write_runtime,
    monkeypatch,
    request,
):
    """The retained metadata importer rolls back the WEMI aggregate on a bad rating."""
    if request.node.callspec.params["write_runtime"] == "PostgreSQL":
        pytest.skip(
            "Retained Library importer requires SQLite's titles/books projections; "
            "the PostgreSQL schema does not provide these compatibility views."
        )
    from LiuXin_alpha.library.library_metadata import Metadata as LibraryMetadata
    from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
        CalibreLikeLiuXinBookMetaData,
    )

    _runtime, catalog, db = write_runtime
    importer = LibraryMetadata(db, SimpleNamespace(fsm=object()))
    tables = (
        "works",
        "expressions",
        "manifestations",
        "items",
        "ratings",
        "rating_work_links",
    )
    before = {table: db.macros.get_rows(table) for table in tables}
    metadata = CalibreLikeLiuXinBookMetaData(title="Rejected rating import", authors=())
    # Keep this fixture about ratings; shared primary-language ordering has its own contract.
    metadata.direct_add("language", None)
    metadata.direct_add("ratings", OrderedDict((("user", -1),)))
    with pytest.raises(ValueError):
        importer.to_title(metadata)
    assert {table: db.macros.get_rows(table) for table in tables} == before
    metadata = CalibreLikeLiuXinBookMetaData(
        title="Fractional rating import", authors=()
    )
    metadata.direct_add("language", None)
    metadata.direct_add("ratings", OrderedDict((("user", 4.25),)))
    work, _item = importer.to_title(metadata)
    assert (
        catalog.works.require(int(work["title_id"]))["work_title"]
        == "Fractional rating import"
    )
    assert any(row["rating"] == 4.25 for row in db.macros.get_rows("ratings"))
    before = {table: db.macros.get_rows(table) for table in tables}
    replacement = CalibreLikeLiuXinBookMetaData(
        title="Rejected replacement", authors=()
    )
    replacement.direct_add("language", None)
    replacement.direct_add("ratings", OrderedDict((("user", -1),)))
    with pytest.raises(ValueError):
        importer.to_title(replacement, force_book_id=int(work["title_id"]))
    assert {table: db.macros.get_rows(table) for table in tables} == before
    insert = db.macros.insert_row

    def fail_rating_insert(table, values, **kwargs):
        if table == "ratings":
            raise RuntimeError("late rating persistence failure")
        return insert(table, values, **kwargs)

    monkeypatch.setattr(db.macros, "insert_row", fail_rating_insert)
    metadata = CalibreLikeLiuXinBookMetaData(
        title="Failed rating persistence", authors=()
    )
    metadata.direct_add("language", None)
    metadata.direct_add("ratings", OrderedDict((("user", 2.75),)))
    with pytest.raises(RuntimeError, match="late rating persistence"):
        importer.to_title(metadata)
    assert {table: db.macros.get_rows(table) for table in tables} == before


def test_value_reference_validation_keeps_immutable_languages_linkable(
    write_runtime, request
):
    """Checking reused values must not mistake linking a seeded constant for editing it."""
    _runtime, catalog, db = write_runtime
    work = catalog.works.create({"title": "Reference constants"})
    # PostgreSQL does not yet ship SQLite's language seed dataset.
    if request.node.callspec.params["write_runtime"] == "PostgreSQL":
        db.macros.insert_row(
            "languages",
            {
                "language": "English",
                "language_code": "eng",
                "language_iso639_2_b": "eng",
                "language_bcp47_primary": "en",
            },
        )
    language = catalog.languages.exact("English").entity_id
    assert language is not None
    before = db.macros.get_rows("languages")
    catalog.write_one("works", "language", work, "English")
    assert db.macros.get_rows("languages") == before
    assert any(
        row["language_work_link_language_id"] == language
        for row in db.macros.get_rows("language_work_links")
    )


def test_rating_bootstrap_keeps_allocation_and_repair_atomic(
    write_runtime, monkeypatch
):
    """Seed IDs do not collide with new scores; a late repair failure changes no rows."""
    _runtime, catalog, db = write_runtime
    created = catalog.ratings.create({"value": 4.25})
    assert created > 11
    db.macros.update_row("ratings", 1, {"rating": 20})
    db.macros.update_row("ratings", 2, {"rating": 20})
    before = db.macros.get_rows("ratings")
    update = db.macros.update_row

    def fail_second_repair(table, row_id, values, **kwargs):
        if table == "ratings" and row_id == 2:
            raise RuntimeError("second rating repair failed")
        return update(table, row_id, values, **kwargs)

    monkeypatch.setattr(db.macros, "update_row", fail_second_repair)
    with pytest.raises(RuntimeError, match="second rating repair"):
        db.check_rating_table()
    assert db.macros.get_rows("ratings") == before
