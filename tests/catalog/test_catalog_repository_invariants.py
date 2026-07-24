"""Mutation and identity invariants for the schema-backed Catalog repositories."""

from __future__ import annotations

from types import SimpleNamespace
import unicodedata
import uuid

import pytest

from LiuXin_alpha.caches.write.identifiers_writer import IdentifiersWrite
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api import (
    CatalogAmbiguousMatchError,
    CatalogMutationError,
    CatalogNotFoundError,
    IdentifierCandidate,
    MetadataCandidate,
)


def _token(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _identifier_rows(catalog: Catalog) -> tuple[dict[str, object], ...]:
    return tuple(dict(row) for row in catalog.identifiers._all_rows())


def test_identifier_replacement_rejects_normalised_scheme_collisions_atomically(
    db,
) -> None:
    """One authoritative mapping cannot contain two values for one scheme."""

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("identifier-collision")})
    catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=work_id,
        identifiers={"uuid": str(uuid.uuid4())},
    )
    before = _identifier_rows(catalog)

    with pytest.raises(CatalogMutationError, match="duplicate normalized scheme"):
        catalog.identifiers.replace_for_wemi(
            level="work",
            entity_id=work_id,
            identifiers={
                "DOI": f"10.1234/{uuid.uuid4()}",
                "doi": f"10.5678/{uuid.uuid4()}",
            },
        )

    assert _identifier_rows(catalog) == before

    with pytest.raises(ValueError, match="invalid ISBN"):
        catalog.identifiers.replace_for_wemi(
            level="work",
            entity_id=work_id,
            identifiers={
                "doi": f"10.9012/{uuid.uuid4()}",
                "isbn13": "978-0-306-40615-8",
            },
        )

    assert _identifier_rows(catalog) == before


def test_identifier_match_or_create_reuses_normalised_logical_identity(db) -> None:
    """Equivalent DOI spellings reuse one row without replacing provenance."""

    catalog = Catalog(db)
    suffix = str(uuid.uuid4())
    identifier_id = catalog.identifiers.match_or_create(
        IdentifierCandidate(
            "DOI",
            f"https://doi.org/10.1357/{suffix}",
            source="first-observation",
        )
    )
    repeated_id = catalog.identifiers.match_or_create(
        IdentifierCandidate(
            "doi",
            f"doi:10.1357/{suffix.upper()}",
            source="later-observation",
        )
    )

    assert repeated_id == identifier_id
    assert catalog.identifiers.require(identifier_id)[
        "entity_identifier_provenance"
    ] == "first-observation"
    assert catalog.identifiers.find(
        identifier_type="doi",
        value=f"10.1357/{suffix.upper()}",
    ) == catalog.identifiers.require(identifier_id)
    assert catalog.identifiers.find(
        identifier_type="doi",
        value=f"10.1357/{uuid.uuid4()}",
    ) is None


def test_identifier_replacement_rolls_back_a_mid_transaction_failure(
    db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Creation, assignment, and stale deletion form one database transaction."""

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("identifier-rollback")})
    catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=work_id,
        identifiers={"uuid": str(uuid.uuid4())},
    )
    before = _identifier_rows(catalog)
    original_link = catalog.identifiers.link_to_wemi
    call_count = 0

    def fail_second_assignment(
        *,
        identifier_id: int,
        level: str,
        entity_id: int,
        priority: int | None = None,
    ) -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("injected identifier assignment failure")
        return original_link(
            identifier_id=identifier_id,
            level=level,  # type: ignore[arg-type]
            entity_id=entity_id,
            priority=priority,
        )

    monkeypatch.setattr(
        catalog.identifiers,
        "link_to_wemi",
        fail_second_assignment,
    )

    with pytest.raises(RuntimeError, match="injected identifier assignment"):
        catalog.identifiers.replace_for_wemi(
            level="work",
            entity_id=work_id,
            identifiers={
                "doi": f"10.3456/{uuid.uuid4()}",
                "url": f"https://rollback.example/{uuid.uuid4()}",
            },
        )

    assert call_count == 2
    assert _identifier_rows(catalog) == before


def test_identifier_copy_and_replacement_preserve_other_owners(db) -> None:
    """Owned identifier copies retain provenance and remain owner-isolated."""

    catalog = Catalog(db)
    first_work = catalog.works.create({"title": _token("identifier-owner-one")})
    second_work = catalog.works.create({"title": _token("identifier-owner-two")})
    shared_value = str(uuid.uuid4())
    original_id = catalog.identifiers.match_or_create(
        IdentifierCandidate("uuid", shared_value, source="owner-isolation-test")
    )
    first_id = catalog.identifiers.link_to_wemi(
        identifier_id=original_id,
        level="work",
        entity_id=first_work,
        priority=1,
    )
    second_id = catalog.identifiers.link_to_wemi(
        identifier_id=original_id,
        level="work",
        entity_id=second_work,
        priority=0,
    )

    assert first_id == original_id
    assert second_id != first_id
    first_row = catalog.identifiers.require(first_id)
    second_row = catalog.identifiers.require(second_id)
    assert second_row["entity_identifier_scheme"] == first_row[
        "entity_identifier_scheme"
    ]
    assert second_row["entity_identifier_value"] == first_row[
        "entity_identifier_value"
    ]
    assert second_row["entity_identifier_provenance"] == (
        first_row["entity_identifier_provenance"]
    ) == "owner-isolation-test"
    assert first_row["entity_identifier_is_primary"] == 0
    assert second_row["entity_identifier_is_primary"] == 1

    replacement = f"10.7890/{uuid.uuid4()}"
    assigned = catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=first_work,
        identifiers={"DOI": replacement},
    )

    assert tuple(assigned) == ("doi",)
    assert {
        (
            row["entity_identifier_scheme"],
            row["entity_identifier_value"],
        )
        for row in catalog.identifiers.list_for_wemi(
            level="work",
            entity_id=first_work,
        )
    } == {("doi", replacement)}
    assert catalog.identifiers.require(second_id)["entity_identifier_value"] == (
        shared_value
    )

    catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=first_work,
        identifiers={},
    )

    assert not catalog.identifiers.list_for_wemi(
        level="work",
        entity_id=first_work,
    )
    assert catalog.identifiers.list_for_wemi(
        level="work",
        entity_id=second_work,
    ) == (catalog.identifiers.require(second_id),)


def test_identifier_cache_writer_changes_cache_only_after_storage_succeeds(
    db,
) -> None:
    """A rejected storage replacement must not leave cache maps ahead of SQL."""

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("identifier-cache")})
    old_value = f"10.2468/{uuid.uuid4()}"
    catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=work_id,
        identifiers={"doi": old_value},
    )
    table = SimpleNamespace(
        book_col_map={work_id: {"doi": old_value}},
        col_book_map={"doi": {work_id}},
    )
    field = SimpleNamespace(table=table)

    with pytest.raises(ValueError, match="invalid ISBN"):
        IdentifiersWrite.identifiers(
            {work_id: {"isbn13": "978-0-306-40615-8"}},
            db,
            field,
        )

    assert table.book_col_map == {work_id: {"doi": old_value}}
    assert table.col_book_map == {"doi": {work_id}}
    stored = catalog.identifiers.list_for_wemi(
        level="work",
        entity_id=work_id,
    )
    assert tuple(
        (row["entity_identifier_scheme"], row["entity_identifier_value"])
        for row in stored
    ) == (("doi", old_value),)

    new_value = str(uuid.uuid4())
    assert IdentifiersWrite.identifiers(
        {work_id: {"uuid": new_value}},
        db,
        field,
    ) == {work_id}

    assert table.book_col_map == {work_id: {"uuid": new_value}}
    assert table.col_book_map == {
        "doi": set(),
        "uuid": {work_id},
    }
    assert tuple(
        (row["entity_identifier_scheme"], row["entity_identifier_value"])
        for row in catalog.identifiers.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
    ) == (("uuid", new_value),)


def test_work_match_or_create_reuses_one_unicode_equivalent_identity(db) -> None:
    """Creation is idempotent across NFKC, case, and whitespace variants."""

    catalog = Catalog(db)
    suffix = _token("work-idempotency")
    stored = f"  Ｃａｆｅ\u0301 東京 {suffix}  "
    candidate = MetadataCandidate(
        {"title": unicodedata.normalize("NFKD", stored).swapcase()}
    )
    before = db.driver_wrapper.get_record_count("works")

    work_id = catalog.works.match_or_create(candidate)
    repeated_id = catalog.works.match_or_create(
        MetadataCandidate({"title": stored})
    )

    assert repeated_id == work_id
    assert db.driver_wrapper.get_record_count("works") == before + 1
    assert catalog.works.require(work_id)["work_title"] == candidate.data["title"]


def test_work_lookup_uses_canonical_titles_with_stable_unicode_limits(db) -> None:
    """Canonical and preferred title matches retain stable Work-ID ordering."""

    catalog = Catalog(db)
    suffix = _token("canonical-order")
    canonical = f"  Ｃａｆｅ\u0301 東京 {suffix}  "
    equivalent = unicodedata.normalize("NFKD", canonical).swapcase()
    first_id = catalog.works.create(
        {
            "title": _token("unrelated-preferred"),
            "canonical_title": canonical,
        }
    )
    second_id = catalog.works.create({"title": equivalent})
    third_id = catalog.works.create(
        {
            "title": _token("another-unrelated-preferred"),
            "canonical_title": equivalent,
        }
    )

    assert tuple(
        row["work_id"]
        for row in catalog.works.find_by_title(canonical, limit=2)
    ) == (first_id, second_id)
    assert tuple(
        row["work_id"]
        for row in catalog.works.find_by_title(equivalent)
    ) == (first_id, second_id, third_id)


def test_work_match_or_create_rejects_unicode_equivalent_ambiguity(db) -> None:
    """Equivalent duplicate Works require intervention and do not create a third."""

    catalog = Catalog(db)
    suffix = _token("work-ambiguity")
    title = f"Ｃａｆｅ\u0301 — 東京 {suffix}"
    equivalent = unicodedata.normalize("NFKD", title).swapcase()
    first_id = catalog.works.create({"title": title})
    second_id = catalog.works.create({"canonical_title": equivalent})
    before = db.driver_wrapper.get_record_count("works")

    with pytest.raises(CatalogAmbiguousMatchError) as raised:
        catalog.works.match_or_create(
            MetadataCandidate({"title": equivalent})
        )

    assert raised.value.result.alternatives == (first_id, second_id)
    assert db.driver_wrapper.get_record_count("works") == before


def test_repository_alias_conflicts_are_rejected_without_writes(db) -> None:
    """Two caller keys cannot silently choose different values for one column."""

    catalog = Catalog(db)
    before = db.driver_wrapper.get_record_count("works")

    with pytest.raises(CatalogMutationError, match="conflicting values"):
        catalog.works.create(
            {
                "title": _token("public-title"),
                "work_title": _token("storage-title"),
            }
        )

    assert db.driver_wrapper.get_record_count("works") == before


def test_repeated_ordered_link_retains_existing_priority(db) -> None:
    """Omitting priority on a repeated link must not reorder existing credit."""

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("ordered-link")})
    agent_id = catalog.agents.create_person({"name": _token("ordered-agent")})
    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=work_id,
        role="aut",
        priority=7,
    )

    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=work_id,
        role="aut",
    )

    linked = catalog.agents.list_for_wemi(
        level="work",
        entity_id=work_id,
    )
    assert len(linked) == 1
    assert linked[0]["_catalog_link"]["priority"] == 7


def test_dangling_catalog_link_raises_a_catalog_not_found_error(
    db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Corrupt relationships are reported instead of silently disappearing."""

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("dangling-link")})
    agent_id = catalog.agents.create_person({"name": _token("dangling-agent")})
    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=work_id,
        role="aut",
    )
    macros = catalog.agents._macros
    original_get_row = macros.get_row

    def hide_linked_agent(
        table: str,
        row_id: int,
        *,
        id_column: str | None = None,
    ):
        if table == "agents" and row_id == agent_id:
            return None
        return original_get_row(table, row_id, id_column=id_column)

    monkeypatch.setattr(macros, "get_row", hide_linked_agent)

    with pytest.raises(CatalogNotFoundError, match="linked from works"):
        catalog.agents.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
