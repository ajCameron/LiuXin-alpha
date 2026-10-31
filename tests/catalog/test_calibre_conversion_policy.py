"""Exercise explicit Calibre identities, WEMI scope and atomic policy writes."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.api import CalibreCompatibilityAPI
from LiuXin_alpha.catalog.api.common import CatalogMutationError, CatalogNotFoundError
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.catalog.relationships import CatalogRelationships
from LiuXin_alpha.metadata.calibre_conversion import (
    DEFAULT_CALIBRE_CONVERSION_POLICY as POLICY,
)
from LiuXin_alpha.metadata.calibre_conversion import (
    CalibreBookRef,
)


@pytest.fixture
def catalog(db):
    catalog = Catalog(db)
    for work_id, title in ((3, "The Novel"), (4, "Introduction"), (7, "Other Work")):
        db.macros.insert_row("works", {"work_id": work_id, "work_title": title})
    for expression_id, label in (
        (13, "Novel text"),
        (14, "Intro text"),
        (15, "Translation"),
        (21, "Orphan"),
    ):
        db.macros.insert_row(
            "expressions", {"expression_id": expression_id, "expression_label": label}
        )
    for manifestation_id in (105, 106, 107):
        db.macros.insert_row(
            "manifestations",
            {
                "manifestation_id": manifestation_id,
                "manifestation_edition_statement": f"Edition {manifestation_id}",
            },
        )
    links = CatalogRelationships(db)
    for work, expression, priority in ((3, 13, 0), (3, 15, 10), (4, 14, 0)):
        links.link("works", work, "expressions", expression, priority=priority)
    for expression, manifestation, priority in (
        (13, 105, 0),
        (13, 106, 10),
        (14, 105, 0),
        (15, 107, 0),
    ):
        links.link(
            "expressions",
            expression,
            "manifestations",
            manifestation,
            priority=priority,
        )
    for item_id in (1005, 1006):
        db.macros.insert_row(
            "items", {"item_id": item_id, "item_manifestation_id": 105}
        )
    return catalog


def test_policy_identity_and_units_are_explicit():
    assert CalibreBookRef.from_sql_ray("3:13:105") == CalibreBookRef(13, 3, 105)
    for invalid in (
        "13",
        "3:13",
        "03:13:105",
        "3:-13:105",
        "3:13:105:1005",
        "３:13:105",
    ):
        with pytest.raises(ValueError):
            CalibreBookRef.from_sql_ray(invalid)
    for invalid in (None, True, -1, "13"):
        with pytest.raises(ValueError):
            CalibreBookRef(invalid)
    assert POLICY.prepare("last_modified", 1000) == 1000
    assert POLICY.prepare("last_modified", "1970-01-01T00:00:01Z") == 1000
    assert (
        POLICY.prepare("last_modified", datetime(1970, 1, 1, 1, tzinfo=UTC)) == 3600000
    )
    assert POLICY.prepare("last_modified", datetime(1970, 1, 1, 0, 0, 1)) == 1000
    assert POLICY.prepare("rating", "8.5") == 8.5
    assert POLICY.prepare("rating", 0) is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("rating", True),
        ("rating", -1),
        ("rating", 10.1),
        ("rating", float("nan")),
        ("last_modified", True),
        ("last_modified", None),
        ("last_modified", 1.5),
        ("last_modified", "not a date"),
        ("last_modified", 2**63),
        ("title", 123),
        ("language", False),
        ("language", -1),
        ("has_cover", True),
        ("series_index", 2),
        ("isbn", "9780306406157"),
    ],
)
def test_unsupported_or_invalid_values_are_rejected(field, value):
    with pytest.raises((TypeError, ValueError)):
        POLICY.prepare(field, value)


def test_projection_groups_editions_at_expression_without_collapsing_introduction(
    catalog,
):
    compat = catalog.calibre
    assert isinstance(compat, CalibreCompatibilityAPI)
    novel = compat.project(CalibreBookRef(13))
    intro = compat.project("4:14:105")
    assert (novel["expression_id"], novel["work_id"], novel["manifestation_id"]) == (
        13,
        3,
        105,
    )
    assert novel["manifestation_ids"] == (105, 106)
    assert intro["manifestation_ids"] == (105,)
    assert novel["title"] == "The Novel"
    assert intro["title"] == "Introduction"
    assert compat.project("3:13:106")["manifestation_id"] == 106
    assert compat.project(CalibreBookRef(21))["manifestation_id"] is None
    # Existing SQL identity is still one complete W:E:M ray per row.
    ids = {row["book_id"] for row in catalog.db.macros.get_rows("books")}
    assert {"3:13:105", "3:13:106", "4:14:105", "3:15:107"} <= ids


@pytest.mark.parametrize("key", [13, True, "13", "3:13:107", "7:13:105", "3:999:105"])
def test_invalid_or_unlinked_identity_cannot_write(catalog, key):
    before = catalog.expressions.require(13)
    with pytest.raises(
        (TypeError, ValueError, CatalogMutationError, CatalogNotFoundError)
    ):
        catalog.calibre.write_field("title", {key: "Wrong"})
    assert catalog.expressions.require(13) == before


def test_title_language_and_timestamp_target_expression_only(catalog):
    compat = catalog.calibre
    language = catalog.db.macros.get_rows("languages")[0]["language_id"]
    catalog.works.set_original_language(3, language)
    before_translation = catalog.expressions.require(15)
    before_work = catalog.works.require(3)
    before_edition = catalog.manifestations.require(105)
    compat.write_field("title", {"3:13:105": "A revised title"})
    compat.write_field("language", {CalibreBookRef(13): language})
    compat.write_field("last_modified", {"3:13:106": "2001-02-03T04:05:06Z"})
    projection = compat.project(CalibreBookRef(13))
    assert projection["title"] == "A revised title"
    assert projection["language"] == language
    assert projection["last_modified"] == 981173106000
    assert catalog.works.require(3) == before_work
    assert catalog.expressions.require(15) == before_translation
    assert catalog.manifestations.require(105) == before_edition
    compat.write_field("language", {CalibreBookRef(13): None})
    assert catalog.works.require(3)["work_original_language_id"] == language
    assert catalog.expressions.require(13)["expression_language_id"] is None
    with pytest.raises(CatalogNotFoundError):
        compat.write_field("language", {CalibreBookRef(13): 9999999})


def test_display_selection_is_not_authority_for_ambiguous_work_writes(catalog):
    CatalogRelationships(catalog.db).link("works", 7, "expressions", 13)
    compat = catalog.calibre
    assert compat.project(CalibreBookRef(13))["work_ids"] == (3, 7)
    with pytest.raises(CatalogMutationError, match="select work_id"):
        compat.write_field("author_sort", {CalibreBookRef(13): "Ambiguous"})
    assert compat.resolve_field_target("author_sort", "7:13:105") == ("work", 7)
    compat.write_field("author_sort", {"7:13:105": "Selected author"})
    assert catalog.works.require(7)["work_creator_sort"] == "Selected author"
    assert catalog.works.require(3)["work_creator_sort"] is None
    compat.write_field("title", {CalibreBookRef(13): "Expression override"})
    assert compat.project("3:13:105")["title"] == "Expression override"


def test_aliases_coalesce_and_conflicting_targets_reject_the_entire_batch(
    catalog, monkeypatch
):
    compat = catalog.calibre
    before = catalog.expressions.require(13)
    with pytest.raises(CatalogMutationError, match="conflicting"):
        compat.write_field("title", {"3:13:105": "First", "3:13:106": "Second"})
    assert catalog.expressions.require(13) == before
    with pytest.raises(CatalogMutationError, match="conflicting"):
        compat.write_field("author_sort", {"3:13:105": "A", "3:15:107": "B"})
    original = catalog.expressions.update
    writes = []

    def track(owner, values):
        writes.append(owner)
        return original(owner, values)

    monkeypatch.setattr(catalog.expressions, "update", track)
    result = compat.write_field("title", {"3:13:105": "Same", "3:13:106": "Same"})
    assert writes == [13]
    assert result == {"3:13:105": 13, "3:13:106": 13}


def test_whole_batch_preflight_and_late_failure_rollback(catalog, monkeypatch):
    compat = catalog.calibre
    before = catalog.expressions.require(13)
    with pytest.raises(TypeError):
        compat.write_field("title", {CalibreBookRef(13): "First", 15: "Unmapped"})
    assert catalog.expressions.require(13) == before
    original = catalog.expressions.update

    def fail_later(owner, values):
        if owner == 15:
            raise RuntimeError("late failure")
        return original(owner, values)

    with monkeypatch.context() as patch:
        patch.setattr(catalog.expressions, "update", fail_later)
        with pytest.raises(RuntimeError, match="late failure"):
            compat.write_field(
                "title", {CalibreBookRef(13): "First", CalibreBookRef(15): "Second"}
            )
    assert catalog.expressions.require(13) == before
    with pytest.raises(RuntimeError, match="outer rollback"):
        with catalog.db.macros.transaction():
            compat.write_field("title", {CalibreBookRef(13): "Pending"})
            raise RuntimeError("outer rollback")
    assert catalog.expressions.require(13) == before


def test_comments_belong_to_expression_and_replacement_rolls_back(catalog, monkeypatch):
    work_note = catalog.comments.add_for_wemi(
        level="work", entity_id=3, data={"text": "Work note"}
    )
    old = catalog.comments.add_for_wemi(
        level="expression", entity_id=13, data={"text": "Old"}
    )
    original = catalog.comments.create

    def fail(data):
        if data.get("text") == "Fail":
            raise RuntimeError("comment failure")
        return original(data)

    with monkeypatch.context() as patch:
        patch.setattr(catalog.comments, "create", fail)
        with pytest.raises(RuntimeError, match="comment failure"):
            catalog.calibre.write_field(
                "comments", {CalibreBookRef(13): "New", CalibreBookRef(15): "Fail"}
            )
    assert [
        r["comment_id"]
        for r in catalog.comments.list_for_wemi(level="expression", entity_id=13)
    ] == [old]
    result = catalog.calibre.write_field(
        "comments", {"3:13:105": "Revised", "3:13:106": "Revised"}
    )
    assert result["3:13:105"] == result["3:13:106"]
    assert catalog.calibre.project(CalibreBookRef(13))["comments"] == "Revised"
    catalog.calibre.write_field("comments", {CalibreBookRef(13): None})
    assert catalog.comments.require(old)["comment"] == "Old"
    assert [
        r["comment_id"]
        for r in catalog.comments.list_for_wemi(level="work", entity_id=3)
    ] == [work_note]


def test_ratings_have_explicit_work_scope_preserve_other_sources_and_rollback(
    catalog, monkeypatch
):
    other = catalog.ratings.create({"value": 4, "out_of": 5, "source": "reviewer"})
    CatalogRelationships(catalog.db).link(
        "works", 3, "ratings", other, link_type="review"
    )
    compat = catalog.calibre
    first = compat.write_field("rating", {"3:13:105": 8.5})["3:13:105"]
    assert compat.project("3:15:107")["rating"] == 8.5
    assert compat.write_field("rating", {"3:13:106": 8.5})["3:13:106"] == first
    spec = catalog.db.driver_wrapper.get_link_spec("works", "ratings")
    before = catalog.db.macros.get_link_rows(spec, 3)

    def fail(data):
        raise RuntimeError("rating failure")

    with monkeypatch.context() as patch:
        patch.setattr(catalog.ratings, "create", fail)
        with pytest.raises(RuntimeError, match="rating failure"):
            compat.write_field("rating", {CalibreBookRef(13): 9})
    assert catalog.db.macros.get_link_rows(spec, 3) == before
    compat.write_field("rating", {CalibreBookRef(13): 0})
    assert compat.project(CalibreBookRef(13))["rating"] == 0
    remaining = catalog.db.macros.get_link_rows(spec, 3)
    assert [(r.secondary_id, r.link_type) for r in remaining] == [(other, "review")]


def test_migrated_cache_hooks_use_policy_and_do_not_write_legacy_views(catalog):
    from LiuXin_alpha.caches.write.author_sort_writer import AuthorSortWriter
    from LiuXin_alpha.caches.write.generic_writers.many_to_many_writer import (
        ManyToManyWriter,
    )
    from LiuXin_alpha.caches.write.generic_writers.many_to_one_writer import (
        ManyToOneWriter,
    )
    from LiuXin_alpha.caches.write.generic_writers.one_to_one_writer import (
        OneToOneWriter,
    )
    from LiuXin_alpha.caches.write.title_writer import TitleWriter

    db = catalog.db
    key = "3:13:105"
    assert AuthorSortWriter.set_author_sort({key: "Surname, Name"}, db, None) == {key}
    OneToOneWriter.last_modified_one_one_db_updater(db, {key: 1000}, None, None)
    ids, owners = OneToOneWriter.comments_one_one_in_other_updater(
        db, None, {key: "A comment"}
    )
    assert ids[owners[key]] == "A comment"
    ManyToManyWriter.language_many_many_db_clean_links(db, None, [key])
    ManyToOneWriter.do_rating_many_one_db_update(db, None, None, False, {key: 8.5}, ())
    assert catalog.calibre.project(key)["rating"] == 8.5
    normalized = {}
    ManyToOneWriter.get_rating_id(
        "8.5", None, None, None, None, {}, False, {}, normalized
    )
    assert normalized == {"8.5": 8.5}
    writer = object.__new__(TitleWriter)
    writer.name = "title"
    # Any attempt to call the old companion sort writer would fail this object.
    field = SimpleNamespace(
        table=SimpleNamespace(book_col_map={}), title_sort_field=object()
    )
    assert writer.set_title({key: "A translated title"}, db, field) == {key}
    assert catalog.works.require(3)["work_title"] == "The Novel"
    assert catalog.works.require(3)["work_sort_title"] is None
    assert catalog.calibre.project(key)["title"] == "A translated title"
    assert field.table.book_col_map[key] == "A translated title"
