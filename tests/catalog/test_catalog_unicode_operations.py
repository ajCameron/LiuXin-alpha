"""Unicode and hostile-text contracts for live Catalog operations."""

from __future__ import annotations

from collections.abc import Sequence
import unicodedata
import uuid

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api import (
    CatalogMutationError,
    IdentifierCandidate,
    MetadataCandidate,
)
from LiuXin_alpha.catalog.matching.policy import normalise_match_text
from tests.support.file_format_unicode import (
    MULTISCRIPT_TEXT,
    deterministic_unicode_fuzz,
)


def _token(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _equivalent_text(value: str) -> str:
    """Return a differently encoded/cased but comparison-equivalent value."""

    return unicodedata.normalize("NFKD", value).swapcase()


def test_work_repository_round_trips_shared_hostile_text_corpora(
    db,
    torture_strings: Sequence[str],
    sql_injection_payloads: Sequence[str],
) -> None:
    """Catalog text remains data across Unicode, controls, and SQL-shaped input."""

    catalog = Catalog(db)
    values = (*torture_strings, *sql_injection_payloads)
    created: dict[int, str] = {}

    for index, value in enumerate(values):
        title = f"{_token(f'catalog-torture-{index}')}::{value}"
        work_id = catalog.works.create({"title": title})
        created[work_id] = title

        assert catalog.works.require(work_id)["work_title"] == title
        assert catalog.works.find_by_title(title) == (
            catalog.works.require(work_id),
        )

    assert {
        row["work_id"]: row["work_title"]
        for row in catalog.works.list(limit=100_000)
        if row["work_id"] in created
    } == created
    assert db.driver_wrapper.get_record_count("works") >= len(values)


def test_unicode_equivalent_work_agent_and_exact_values_share_identity(db) -> None:
    """NFKC, case, and whitespace variants resolve without changing stored text."""

    catalog = Catalog(db)
    suffix = _token("identity")
    stored = f"  Ｃａｆｅ\u0301 Straße 東京 {suffix}  "
    equivalent = _equivalent_text(stored)

    work_id = catalog.works.create({"title": stored})
    assert catalog.works.require(work_id)["work_title"] == stored
    assert catalog.works.find_by_title(equivalent)[0]["work_id"] == work_id
    assert catalog.works.match(
        MetadataCandidate({"title": equivalent})
    ).entity_id == work_id

    canonical_name = f"Éditions 東京 {suffix}"
    first_alias = f"Café Press {suffix}"
    duplicate_alias = _equivalent_text(first_alias)
    cjk_alias = f"出版社 {suffix}"
    agent_id = catalog.agents.create_organisation(
        {
            "name": canonical_name,
            "aliases": (
                first_alias,
                duplicate_alias,
                f"  {cjk_alias}  ",
                cjk_alias,
            ),
        }
    )
    agent = catalog.agents.require(agent_id)
    assert agent["agent_canonical_name"] == canonical_name
    assert agent["agent_aliases"] == (
        f"{first_alias}(#BREAK#){cjk_alias}"
    )
    assert catalog.agents.resolve(
        name=_equivalent_text(canonical_name)
    )["agent_id"] == agent_id
    assert catalog.agents.match(
        MetadataCandidate({"name": _equivalent_text(first_alias)})
    ).entity_id == agent_id

    canonical_equivalent = unicodedata.normalize("NFKD", stored)
    exact_entities = (
        ("tags", {"name": stored}, "tag", equivalent),
        ("labels", {"text": stored}, "label_text", equivalent),
        ("genres", {"name": stored}, "genre", equivalent),
        ("subjects", {"name": stored}, "subject", equivalent),
        ("series", {"name": stored}, "series", equivalent),
        ("synopses", {"text": stored}, "synopsis", canonical_equivalent),
        ("notes", {"text": stored}, "note", canonical_equivalent),
    )
    for repository_name, payload, storage_column, query in exact_entities:
        repository = getattr(catalog, repository_name)
        entity_id = repository.create(payload)
        assert repository.require(entity_id)[storage_column] == stored
        assert repository.exact(query).entity_id == entity_id

    label = catalog.labels.require(
        catalog.labels.exact(equivalent).entity_id
    )
    series = catalog.series.require(
        catalog.series.exact(equivalent).entity_id
    )
    assert label["label_text_norm"] == normalise_match_text(stored)
    assert series["series_name_norm"] == normalise_match_text(stored)


def test_wemi_stack_attachment_and_bundle_preserve_multiscript_metadata(db) -> None:
    """One atomic WEMI operation preserves multilingual graph metadata."""

    catalog = Catalog(db)
    suffix = _token("wemi")
    work_title = f"{MULTISCRIPT_TEXT}\nWork {suffix}"
    expression_title = f"表現形 Καλημέρα {suffix}"
    manifestation_title = f"طبعة מיוחדת 👩🏽‍💻 {suffix}"
    item_code = f"所蔵品-नमस्ते-{suffix}"
    origin = f"來源/مصدر/{suffix}"

    created = catalog.mutations.writer.create_wemi_stack(
        work={
            "title": work_title,
            "canonical_title": f"正規化作品 {suffix}",
        },
        expression={
            "label": expression_title,
            "title_override": expression_title,
        },
        manifestation={
            "subtitle": manifestation_title,
            "format_detail": "EPUB",
        },
        items=(
            {
                "inventory_code": item_code,
                "source": f"移行-source-{suffix}",
                "location": f"書架/رف/{suffix}",
            },
        ),
        origin=origin,
    )
    item_id = created.item_ids[0]

    agent_name = f"Zoë 李 مرحبا {suffix}"
    note_text = f"{MULTISCRIPT_TEXT}\nNote {suffix}"
    sort_title = f"Sort Ångström 東京 {suffix}"
    identifier_source = f"輸入-происхождение-{suffix}"
    doi_value = f"10.1234/{uuid.uuid4()}"
    catalog.mutations.writer.attach_metadata(
        level="work",
        entity_id=created.work_id,
        data={
            "fields": {"sort_title": sort_title},
            "agents": (
                {
                    "data": {
                        "name": agent_name,
                        "aliases": (f"Z. 李 {suffix}",),
                    },
                    "role": "aut",
                    "priority": 0,
                },
            ),
            "identifiers": (
                {
                    "scheme": "doi",
                    "value": doi_value,
                    "source": identifier_source,
                    "priority": 0,
                },
            ),
            "notes": (note_text,),
        },
    )

    bundle = catalog.retrieval.bundles.for_item(item_id)
    assert bundle.work["work_title"] == work_title
    assert bundle.work["work_sort_title"] == sort_title
    assert bundle.expression["expression_title_override"] == expression_title
    assert bundle.manifestation["manifestation_subtitle"] == manifestation_title
    assert bundle.item["item_inventory_code"] == item_code
    assert [row["agent_canonical_name"] for row in bundle.agents] == [
        agent_name
    ]
    assert [row["note"] for row in bundle.notes] == [note_text]
    assert [
        (
            row["entity_identifier_scheme"],
            row["entity_identifier_value"],
            row["entity_identifier_provenance"],
        )
        for row in bundle.identifiers
    ] == [("doi", doi_value, identifier_source)]
    assert catalog.agents.match(
        MetadataCandidate({"name": _equivalent_text(agent_name)})
    ).entity_id == bundle.agents[0]["agent_id"]
    assert catalog.retrieval.projections.display_title(
        level="item",
        entity_id=item_id,
    ) == manifestation_title

    work_link = catalog.expressions.list_works(created.expression_id)[0]
    assert origin in work_link["_catalog_link"]["extra"].values()


def test_logical_title_operations_preserve_unicode_at_each_wemi_level(db) -> None:
    """Embedded title columns behave as one Unicode-safe logical repository."""

    catalog = Catalog(db)
    suffix = _token("titles")
    work_id = catalog.titles.create(
        {"title": f"作品タイトル 👩🏽‍💻 {suffix}"}
    )
    expression_id = catalog.expressions.create(
        {"label": f"expression {suffix}"}
    )
    manifestation_id = catalog.manifestations.create(
        {"format_detail": f"פורמט {suffix}"}
    )
    item_id = catalog.items.create(
        {
            "manifestation_id": manifestation_id,
            "inventory_code": f"item {suffix}",
        }
    )

    updated_work_title = f"Обновлённое произведение {suffix}"
    expression_title = f"ترجمة العنوان {suffix}"
    expression_subtitle = f"उपशीर्षक {suffix}"
    manifestation_title = f"版の副題 {suffix}"
    catalog.titles.update(work_id, {"title": updated_work_title})
    catalog.titles.add_for_wemi(
        level="expression",
        entity_id=expression_id,
        data={
            "title": expression_title,
            "expression_subtitle": expression_subtitle,
        },
    )
    catalog.titles.add_for_wemi(
        level="manifestation",
        entity_id=manifestation_id,
        data={"title": manifestation_title},
    )

    assert catalog.titles.get(work_id)["title"] == updated_work_title
    assert catalog.titles.preferred_for_wemi(
        level="expression",
        entity_id=expression_id,
    )["title"] == expression_title
    assert catalog.titles.list_for_wemi(
        level="expression",
        entity_id=expression_id,
    )[0]["title_values"] == {
        "expression_title_override": expression_title,
        "expression_subtitle": expression_subtitle,
    }
    assert catalog.titles.preferred_for_wemi(
        level="manifestation",
        entity_id=manifestation_id,
    )["title"] == manifestation_title
    with pytest.raises(CatalogMutationError, match="Items do not own"):
        catalog.titles.add_for_wemi(
            level="item",
            entity_id=item_id,
            data={"title": "禁止"},
        )

    catalog.titles.delete(work_id)
    assert catalog.works.require(work_id)["work_title"] is None
    assert catalog.titles.preferred_for_wemi(
        level="work",
        entity_id=work_id,
    ) is None


def test_owned_text_relations_round_trip_unicode_across_declared_wemi_routes(
    db,
) -> None:
    """Owned text retains Unicode and unsupported graph routes stay atomic."""

    catalog = Catalog(db)
    suffix = _token("relations")
    created = catalog.mutations.writer.create_wemi_stack(
        work={"title": f"Relation work {suffix}"},
        expression={"label": f"Relation expression {suffix}"},
        manifestation={"format_detail": f"Relation format {suffix}"},
        items=({"inventory_code": f"Relation item {suffix}"},),
    )
    owners = (
        ("work", created.work_id),
        ("expression", created.expression_id),
        ("manifestation", created.manifestation_id),
        ("item", created.item_ids[0]),
    )

    for level, entity_id in owners:
        comment = f"Коментар 👩🏽‍💻 {level} {suffix}"
        comment_id = catalog.comments.add_for_wemi(
            level=level,
            entity_id=entity_id,
            data={"text": comment},
        )
        assert catalog.comments.list_for_wemi(
            level=level,
            entity_id=entity_id,
        )[0]["comment_id"] == comment_id
        assert catalog.comments.require(comment_id)["comment"] == comment

    for level, entity_id in owners[:2]:
        note = f"注記 ملاحظة {level} {suffix}"
        note_id = catalog.notes.add_for_wemi(
            level=level,
            entity_id=entity_id,
            data={"text": note},
        )
        assert catalog.notes.list_for_wemi(
            level=level,
            entity_id=entity_id,
        )[0]["note_id"] == note_id
        assert catalog.notes.require(note_id)["note"] == note

    synopsis = f"Σύνοψη कहानी work {suffix}"
    synopsis_id = catalog.synopses.add_for_wemi(
        level="work",
        entity_id=created.work_id,
        data={"text": synopsis},
    )
    assert catalog.synopses.list_for_wemi(
        level="work",
        entity_id=created.work_id,
    )[0]["synopsis_id"] == synopsis_id
    assert catalog.synopses.require(synopsis_id)["synopsis"] == synopsis

    note_count = db.driver_wrapper.get_record_count("notes")
    synopsis_count = db.driver_wrapper.get_record_count("synopses")
    with pytest.raises(CatalogMutationError, match="no catalog link"):
        catalog.notes.add_for_wemi(
            level="manifestation",
            entity_id=created.manifestation_id,
            data={"text": "unsupported note"},
        )
    with pytest.raises(CatalogMutationError, match="no catalog link"):
        catalog.synopses.add_for_wemi(
            level="expression",
            entity_id=created.expression_id,
            data={"text": "unsupported synopsis"},
        )
    assert db.driver_wrapper.get_record_count("notes") == note_count
    assert db.driver_wrapper.get_record_count("synopses") == synopsis_count


def test_normalized_catalog_writers_preserve_unicode_values(db) -> None:
    """Schema-selected column and shared-link writers preserve caller text."""

    catalog = Catalog(db)
    suffix = _token("writers")
    work_id = catalog.works.create({"title": f"before {suffix}"})
    title = f"{MULTISCRIPT_TEXT}\n{deterministic_unicode_fuzz(seed=7, length=96)}"

    assert catalog.write_one(
        "works",
        "work_title",
        work_id,
        title,
    ) == {work_id: title}
    assert catalog.works.require(work_id)["work_title"] == title

    tag_values = (
        f"标签-タグ-وسم-{suffix}",
        f"SQL-shaped '); DROP TABLE tags; -- {suffix}",
    )
    result = catalog.write(
        "works",
        "tag",
        {work_id: tag_values},
    )
    linked_tag_ids = tuple(row.secondary_id for row in result[work_id])
    assert tuple(
        catalog.tags.require(tag_id)["tag"] for tag_id in linked_tag_ids
    ) == tag_values
    assert db.driver_wrapper.get_record_count("works") >= 1
    assert db.driver_wrapper.get_record_count("tags") >= 2


def test_existing_and_requested_work_ids_replace_unicode_wemi_paths(db) -> None:
    """Stack writes update requested Works and replace only the selected path."""

    catalog = Catalog(db)
    suffix = _token("replace-stack")
    original = catalog.mutations.writer.create_wemi_stack(
        work={"title": f"原始作品 {suffix}"},
        expression={"label": f"旧表現 {suffix}"},
        manifestation={"format_detail": f"旧版 {suffix}"},
        origin=f"旧由来 {suffix}",
    )
    replacement = catalog.mutations.writer.create_wemi_stack(
        work={
            "title": f"置換作品 {suffix}",
            "canonical_title": f"Canonical 作品 {suffix}",
        },
        expression={"label": f"新表現 {suffix}"},
        manifestation={"format_detail": f"新版 {suffix}"},
        origin=f"新由来 {suffix}",
        work_id=original.work_id,
    )

    assert replacement.work_id == original.work_id
    assert catalog.works.require(original.work_id)["work_title"] == (
        f"置換作品 {suffix}"
    )
    assert [
        row["expression_id"]
        for row in catalog.expressions.list_for_work(original.work_id)
    ] == [replacement.expression_id]
    assert catalog.expressions.list_works(original.expression_id) == ()
    assert catalog.expressions.require(original.expression_id)[
        "expression_label"
    ] == f"旧表現 {suffix}"
    replacement_link = catalog.expressions.list_works(
        replacement.expression_id
    )[0]["_catalog_link"]
    assert f"新由来 {suffix}" in replacement_link["extra"].values()

    requested_id = max(
        row["work_id"] for row in catalog.works.list(limit=100_000)
    ) + 10_000
    requested = catalog.mutations.writer.create_wemi_stack(
        work={"title": f"明示 ID 作品 {suffix}"},
        expression={"label": f"明示 ID 表現 {suffix}"},
        manifestation={"format_detail": f"明示 ID 版 {suffix}"},
        work_id=requested_id,
    )
    assert requested.work_id == requested_id
    assert catalog.works.require(requested_id)["work_title"] == (
        f"明示 ID 作品 {suffix}"
    )


def test_unicode_attachment_supports_existing_rows_and_rejects_bad_groups_atomically(
    db,
) -> None:
    """Structured attachment accepts existing IDs and preflights every group."""

    catalog = Catalog(db)
    suffix = _token("attachment")
    work_id = catalog.works.create({"title": f"Attachment work {suffix}"})
    agent_id = catalog.agents.create(
        {
            "name": f"Existing लेखक {suffix}",
            "aliases": (f"既存別名 {suffix}",),
        }
    )
    identifier_id = catalog.identifiers.match_or_create(
        IdentifierCandidate(
            "uuid",
            str(uuid.uuid4()),
            source=f"台帳 {suffix}",
        )
    )
    title = f"付加された標題 {suffix}"
    sort_title = f"Sorted Ångström {suffix}"
    note = f"既存 ID を使う注記 {suffix}"

    catalog.mutations.writer.attach_metadata(
        level="work",
        entity_id=work_id,
        data={
            "canonical_title": f"正規形 {suffix}",
            "title": title,
            "titles": ({"work_sort_title": sort_title},),
            "agents": (
                {
                    "agent_id": agent_id,
                    "role": "trl",
                    "priority": 2,
                },
            ),
            "identifiers": (
                {
                    "identifier_id": identifier_id,
                    "priority": 0,
                },
            ),
            "notes": ({"text": note},),
        },
    )

    work = catalog.works.require(work_id)
    assert (
        work["work_title"],
        work["work_canonical_title"],
        work["work_sort_title"],
    ) == (title, f"正規形 {suffix}", sort_title)
    assert catalog.agents.list_for_wemi(
        level="work",
        entity_id=work_id,
    )[0]["agent_id"] == agent_id
    assert catalog.identifiers.list_for_wemi(
        level="work",
        entity_id=work_id,
    )[0]["entity_identifier_id"] == identifier_id
    assert catalog.notes.list_for_wemi(
        level="work",
        entity_id=work_id,
    )[0]["note"] == note

    before = {
        table: db.driver_wrapper.get_record_count(table)
        for table in ("works", "agents", "entity_identifiers", "notes")
    }
    bad_payloads = (
        {"fields": ()},
        {"titles": 42},
        {"titles": (42,)},
        {"agents": ("not-a-mapping",)},
        {"agents": ({"name": "missing role"},)},
        {"identifiers": ("not-a-mapping",)},
        {"identifiers": ({"scheme": "doi"},)},
        {"notes": (42,)},
    )
    for payload in bad_payloads:
        with pytest.raises(CatalogMutationError):
            catalog.mutations.writer.attach_metadata(
                level="work",
                entity_id=work_id,
                data=payload,
            )

    item_id = catalog.items.create(
        {"inventory_code": f"attachment-item-{suffix}"}
    )
    with pytest.raises(CatalogMutationError, match="Items do not own"):
        catalog.mutations.writer.attach_metadata(
            level="item",
            entity_id=item_id,
            data={"title": "Item title is unsupported"},
        )
    assert {
        table: db.driver_wrapper.get_record_count(table)
        for table in before
    } == before


def test_unicode_merge_preserves_graph_metadata_and_primary_identifier_policy(
    db,
) -> None:
    """Merge fills missing values, transfers relations, and demotes conflicts."""

    catalog = Catalog(db)
    suffix = _token("merge-unicode")
    source_id = catalog.works.create(
        {
            "title": f"源作品 {suffix}",
            "discovery_note": f"発見 σημείωση {suffix}",
        }
    )
    target_id = catalog.works.create(
        {"canonical_title": f"Целевая каноническая {suffix}"}
    )
    expression_id = catalog.expressions.match_or_create(
        source_id,
        MetadataCandidate({"label": f"転送表現 {suffix}"}),
    )
    agent_id = catalog.agents.create(
        {"name": f"نویسنده स्रोत {suffix}"}
    )
    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=source_id,
        role="aut",
    )
    note_id = catalog.notes.add_for_wemi(
        level="work",
        entity_id=source_id,
        data={"text": f"転送注記 {suffix}"},
    )
    source_identifier = catalog.identifiers.match_or_create(
        IdentifierCandidate("doi", f"10.4321/source-{uuid.uuid4()}")
    )
    target_identifier = catalog.identifiers.match_or_create(
        IdentifierCandidate("doi", f"10.4321/target-{uuid.uuid4()}")
    )
    source_identifier = catalog.identifiers.link_to_wemi(
        identifier_id=source_identifier,
        level="work",
        entity_id=source_id,
        priority=0,
    )
    target_identifier = catalog.identifiers.link_to_wemi(
        identifier_id=target_identifier,
        level="work",
        entity_id=target_id,
        priority=0,
    )

    catalog.mutations.writer.merge_entities(
        level="work",
        source_id=source_id,
        target_id=target_id,
    )

    assert catalog.works.get(source_id) is None
    target = catalog.works.require(target_id)
    assert target["work_title"] == f"源作品 {suffix}"
    assert target["work_canonical_title"] == f"Целевая каноническая {suffix}"
    assert target["work_discovery_note"] == f"発見 σημείωση {suffix}"
    assert catalog.expressions.list_for_work(target_id)[0][
        "expression_id"
    ] == expression_id
    assert catalog.agents.list_for_wemi(
        level="work",
        entity_id=target_id,
    )[0]["agent_id"] == agent_id
    assert catalog.notes.list_for_wemi(
        level="work",
        entity_id=target_id,
    )[0]["note_id"] == note_id
    identifiers = {
        row["entity_identifier_id"]: row["entity_identifier_is_primary"]
        for row in catalog.identifiers.list_for_wemi(
            level="work",
            entity_id=target_id,
        )
    }
    assert identifiers == {
        source_identifier: 0,
        target_identifier: 1,
    }

    manifestation_id = catalog.manifestations.create(
        {"format_detail": f"Merge format {suffix}"}
    )
    source_item = catalog.items.create(
        {
            "manifestation_id": manifestation_id,
            "location": f"書架 {suffix}",
        }
    )
    target_item = catalog.items.create(
        {
            "manifestation_id": manifestation_id,
            "inventory_code": f"INV-{suffix}",
        }
    )
    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="item",
        entity_id=source_item,
        role="own",
    )
    catalog.mutations.writer.merge_entities(
        level="item",
        source_id=source_item,
        target_id=target_item,
    )
    assert catalog.items.get(source_item) is None
    assert catalog.items.require(target_item)["item_location"] == (
        f"書架 {suffix}"
    )
    assert catalog.agents.list_for_wemi(
        level="item",
        entity_id=target_item,
    )[0]["agent_id"] == agent_id


def test_unicode_agent_identifier_evidence_is_explicitly_resolved(db) -> None:
    """Unicode aliases remain subordinate to decisive identifier ownership."""

    catalog = Catalog(db)
    suffix = _token("agent-evidence")
    shared_alias = f"Éditeur 東京 {suffix}"
    first_id = catalog.agents.create_organisation(
        {
            "name": f"Première société {suffix}",
            "aliases": (shared_alias,),
        }
    )
    second_id = catalog.agents.create_organisation(
        {
            "name": f"Deuxième société {suffix}",
            "aliases": (_equivalent_text(shared_alias),),
        }
    )

    ambiguous_name = catalog.agents.match(
        MetadataCandidate({"name": shared_alias})
    )
    assert ambiguous_name.decision == "ambiguous"
    assert ambiguous_name.alternatives == (first_id, second_id)

    shared_identifier = catalog.identifiers.match_or_create(
        IdentifierCandidate(
            "url",
            f"https://共有.example/{uuid.uuid4()}",
        )
    )
    catalog.identifiers.link_to_agent(
        identifier_id=shared_identifier,
        agent_id=first_id,
    )
    catalog.identifiers.link_to_agent(
        identifier_id=shared_identifier,
        agent_id=second_id,
    )
    shared_value = catalog.identifiers.require(shared_identifier)[
        "entity_identifier_value"
    ]
    ambiguous_identifier = catalog.agents.match(
        MetadataCandidate(
            {},
            hints={"identifiers": {"url": shared_value}},
        )
    )
    assert ambiguous_identifier.decision == "ambiguous"
    assert ambiguous_identifier.alternatives == (first_id, second_id)

    first_unique = f"https://première.example/{uuid.uuid4()}"
    second_unique = f"https://第二.example/{uuid.uuid4()}"
    for agent_id, value in (
        (first_id, first_unique),
        (second_id, second_unique),
    ):
        identifier_id = catalog.identifiers.match_or_create(
            IdentifierCandidate("url", value)
        )
        catalog.identifiers.link_to_agent(
            identifier_id=identifier_id,
            agent_id=agent_id,
        )

    conflict = catalog.agents.match(
        MetadataCandidate(
            {},
            hints={
                "identifiers": (
                    ("url", first_unique),
                    ("url", second_unique),
                )
            },
        )
    )
    assert conflict.decision == "conflict"
    assert conflict.alternatives == (first_id, second_id)

    resolved = catalog.agents.match(
        MetadataCandidate(
            {
                "name": _equivalent_text(shared_alias),
                "type": "organisation",
            },
            hints={"identifiers": {"url": first_unique}},
        )
    )
    assert resolved.entity_id == first_id
    assert resolved.matched_on == ("identifiers",)

    wrong_type = catalog.agents.match(
        MetadataCandidate(
            {"type": "person"},
            hints={"identifiers": {"url": first_unique}},
        )
    )
    assert wrong_type.decision == "conflict"
    assert wrong_type.alternatives == (first_id,)


def test_unicode_person_and_organisation_aggregates_round_trip_and_reuse(db) -> None:
    """Agent aggregates preserve subtype, identifier, language, and text data."""

    catalog = Catalog(db)
    suffix = _token("agent-aggregate")
    language = catalog.languages.exact("English")
    assert language.entity_id is not None
    language_id = language.entity_id

    person_name = f"Ürsula 李 مرحبا {suffix}"
    person_note = f"Biographical 注記 {suffix}"
    person_url = f"https://著者.example/{uuid.uuid4()}"
    person_id = catalog.agents.create_person(
        {
            "name": person_name,
            "type": "author",
            "aliases": (
                f"U. 李 {suffix}",
                _equivalent_text(f"U. 李 {suffix}"),
            ),
        },
        details={
            "human_agent_given_name": f"Ürsula {suffix}",
            "human_agent_family_name": f"李 {suffix}",
            "human_agent_biography": f"Βιογραφία سيرة {suffix}",
        },
        identifiers=(
            {
                "scheme": "url",
                "value": person_url,
                "source": f"人名典拠 {suffix}",
                "is_primary": True,
            },
        ),
        language_ids=(language_id,),
        notes=(person_note,),
    )

    person = catalog.agents.require(person_id)
    assert person["agent_type"] == "person"
    assert person["agent_aliases"] == f"U. 李 {suffix}"
    human = db.macros.get_rows(
        "human_agents",
        where={"human_agent_agent_id": person_id},
    )[0]
    assert human["human_agent_given_name"] == f"Ürsula {suffix}"
    assert human["human_agent_family_name"] == f"李 {suffix}"
    assert human["human_agent_biography"] == f"Βιογραφία سيرة {suffix}"
    assert catalog.identifiers.list_for_agent(person_id)[0][
        "entity_identifier_provenance"
    ] == f"人名典拠 {suffix}"
    assert catalog.notes.require(
        db.macros.get_link_rows(
            catalog.agents._link_spec("agents", "notes"),
            person_id,
        )[0].secondary_id
    )["note"] == person_note
    language_links = db.macros.get_link_rows(
        catalog.agents._link_spec("agents", "languages"),
        person_id,
    )
    assert [(row.secondary_id, row.link_type) for row in language_links] == [
        (language_id, "native")
    ]
    assert catalog.agents.match_or_create_person(
        MetadataCandidate({"name": _equivalent_text(person_name)})
    ) == person_id

    parent_name = f"親会社 Société {suffix}"
    parent_id = catalog.agents.create_organisation(
        {"name": parent_name, "type": "publisher"},
        details={"org_agent_legal_name": f"親会社株式会社 {suffix}"},
    )
    child_name = f"子会社 Éditions {suffix}"
    child_note = f"Organisation note ملاحظة {suffix}"
    child_synopsis = f"Organisation synopsis 概要 {suffix}"
    child_id = catalog.agents.create_organisation(
        {
            "name": child_name,
            "type": "organization",
            "aliases": (f"Imprint 東京 {suffix}",),
        },
        details={
            "org_agent_legal_name": f"子会社合同会社 {suffix}",
            "org_agent_website": f"https://出版社.example/{uuid.uuid4()}",
        },
        parent_id=parent_id,
        relation_type="imprint_of",
        relation_note=f"取得 provenance {suffix}",
        language_ids=(language_id,),
        notes=(child_note,),
        synopses=(child_synopsis,),
    )

    relation = db.macros.get_rows(
        "org_agent_relations",
        where={"org_agent_relation_child_agent_id": child_id},
    )[0]
    assert relation["org_agent_relation_parent_agent_id"] == parent_id
    assert relation["org_agent_relation_note"] == f"取得 provenance {suffix}"
    assert catalog.agents.match_or_create_organisation(
        MetadataCandidate({"name": _equivalent_text(child_name)})
    ) == child_id
    child_note_id = db.macros.get_link_rows(
        catalog.agents._link_spec("agents", "notes"),
        child_id,
    )[0].secondary_id
    child_synopsis_id = db.macros.get_link_rows(
        catalog.agents._link_spec("agents", "synopses"),
        child_id,
    )[0].secondary_id
    assert catalog.notes.require(child_note_id)["note"] == child_note
    assert catalog.synopses.require(child_synopsis_id)["synopsis"] == (
        child_synopsis
    )

    with pytest.raises(CatalogMutationError, match="non-person"):
        catalog.agents.match_or_create_person(
            MetadataCandidate({"name": child_name})
        )
    with pytest.raises(CatalogMutationError, match="non-organisation"):
        catalog.agents.match_or_create_organisation(
            MetadataCandidate({"name": person_name})
        )


def test_agent_role_replacement_and_aggregate_failures_are_atomic(db) -> None:
    """Role-scoped credit replacement retains other roles and failures roll back."""

    catalog = Catalog(db)
    suffix = _token("agent-roles")
    work_id = catalog.works.create({"title": f"Credit work 作品 {suffix}"})
    first = catalog.agents.create_person(
        {"name": f"著者 один {suffix}"}
    )
    second = catalog.agents.create_person(
        {"name": f"著者 два {suffix}"}
    )
    publisher = catalog.agents.create_organisation(
        {"name": f"出版社 ثلاثة {suffix}"}
    )
    catalog.agents.link_to_wemi(
        agent_id=first,
        level="work",
        entity_id=work_id,
        role="aut",
    )
    catalog.agents.link_to_wemi(
        agent_id=publisher,
        level="work",
        entity_id=work_id,
        role="pbl",
    )
    catalog.agents.replace_for_wemi(
        level="work",
        entity_id=work_id,
        role="aut",
        agent_ids=(second, first),
    )

    credits = catalog.agents.list_for_wemi(
        level="work",
        entity_id=work_id,
    )
    by_role = {
        role: {
            row["agent_id"]
            for row in credits
            if row["_catalog_link"]["type"] == role
        }
        for role in ("aut", "pbl")
    }
    assert by_role == {
        "aut": {first, second},
        "pbl": {publisher},
    }

    with pytest.raises(ValueError, match="unknown WEMI"):
        catalog.agents.replace_for_wemi(
            level="invalid",  # type: ignore[arg-type]
            entity_id=work_id,
            role="aut",
            agent_ids=(first,),
        )
    with pytest.raises(ValueError, match="non-empty"):
        catalog.agents.replace_for_wemi(
            level="work",
            entity_id=work_id,
            role=" ",
            agent_ids=(first,),
        )
    with pytest.raises(TypeError, match="sequence"):
        catalog.agents.replace_for_wemi(
            level="work",
            entity_id=work_id,
            role="aut",
            agent_ids="not-ids",  # type: ignore[arg-type]
        )

    before = {
        table: db.driver_wrapper.get_record_count(table)
        for table in ("agents", "human_agents", "entity_identifiers")
    }
    with pytest.raises(TypeError, match="priority"):
        catalog.agents.create_person(
            {"name": f"Rollback person {suffix}"},
            identifiers=(
                {
                    "scheme": "url",
                    "value": f"https://rollback.example/{uuid.uuid4()}",
                    "priority": True,
                },
            ),
        )
    assert {
        table: db.driver_wrapper.get_record_count(table)
        for table in before
    } == before

    raw_organisation = catalog.agents.create(
        {
            "name": f"Sidecar-less organisation {suffix}",
            "type": "organisation",
        }
    )
    with pytest.raises(CatalogMutationError, match="no org_agents sidecar"):
        catalog.agents.create_organisation(
            {"name": f"Rejected child {suffix}"},
            parent_id=raw_organisation,
        )
    with pytest.raises(CatalogMutationError, match="not an organisation"):
        catalog.agents.create_organisation(
            {"name": f"Rejected person child {suffix}"},
            parent_id=first,
        )
