"""Behavior tests for the schema-backed semantic catalog facade."""

from __future__ import annotations

import uuid

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api import IdentifierCandidate, MetadataCandidate
from LiuXin_alpha.catalog.api.catalog import CatalogAPI
from LiuXin_alpha.catalog.api.matching_api import (
    AgentMatcherAPI,
    CatalogMatchingAPI,
    IdentifierMatcherAPI,
    WorkMatcherAPI,
)
from LiuXin_alpha.catalog.api.mutations_api import CatalogMutationsAPI
from LiuXin_alpha.catalog.api.repositories import (
    AgentRepositoryAPI,
    CatalogRepositoriesAPI,
    ExpressionRepositoryAPI,
    IdentifierRepositoryAPI,
    ItemRepositoryAPI,
    ManifestationRepositoryAPI,
    NoteRepositoryAPI,
    TitleRepositoryAPI,
    WorkRepositoryAPI,
)
from LiuXin_alpha.catalog.api.retrieval import CatalogRetrievalAPI


def _token(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def test_catalog_repositories_round_trip_real_wemi_schema(db) -> None:
    catalog = Catalog(db)
    title = _token("catalog-work")

    work_id = catalog.works.create({"title": title})
    assert catalog.works.get(work_id)["work_id"] == work_id
    assert catalog.works.require(work_id)["work_title"] == title
    assert work_id in {row["work_id"] for row in catalog.works.list()}
    assert catalog.works.find_by_title(title.swapcase())[0]["work_id"] == work_id
    catalog.works.update(work_id, {"canonical_title": f"Canonical {title}"})

    expression_id = catalog.expressions.match_or_create(
        work_id,
        MetadataCandidate({"label": _token("expression")}),
    )
    manifestation_id = catalog.manifestations.match_or_create(
        expression_id,
        MetadataCandidate({"subtitle": _token("manifestation")}),
    )
    item_id = catalog.items.match_or_create(
        manifestation_id,
        MetadataCandidate({"inventory_code": _token("item")}),
    )

    assert catalog.expressions.list_for_work(work_id)[0][
        "expression_id"
    ] == expression_id
    assert catalog.expressions.list_works(expression_id)[0]["work_id"] == work_id
    assert catalog.manifestations.list_for_expression(expression_id)[0][
        "manifestation_id"
    ] == manifestation_id
    assert catalog.manifestations.list_expressions(manifestation_id)[0][
        "expression_id"
    ] == expression_id
    assert catalog.items.list_for_manifestation(manifestation_id)[0][
        "item_id"
    ] == item_id
    assert catalog.items.manifestation_for_item(item_id)[
        "manifestation_id"
    ] == manifestation_id

    assert catalog.works.match(MetadataCandidate({"title": title})).entity_id == work_id
    assert catalog.matching.works.exact(title).entity_id == work_id
    assert catalog.expressions.match(
        work_id,
        MetadataCandidate(
            {
                "label": catalog.expressions.require(expression_id)[
                    "expression_label"
                ]
            }
        ),
    ).entity_id == expression_id
    assert catalog.manifestations.match(
        expression_id,
        MetadataCandidate(
            {
                "subtitle": catalog.manifestations.require(
                    manifestation_id
                )["manifestation_subtitle"]
            }
        ),
    ).entity_id == manifestation_id
    assert catalog.items.match(
        manifestation_id,
        MetadataCandidate(
            {
                "inventory_code": catalog.items.require(item_id)[
                    "item_inventory_code"
                ]
            }
        ),
    ).entity_id == item_id
    assert catalog.matching.works.best(
        MetadataCandidate({"title": title})
    ).entity_id == work_id
    assert catalog.matching.works.candidates(
        MetadataCandidate({"title": title})
    )[0].entity_id == work_id

    disposable_id = catalog.works.create({"title": _token("disposable")})
    catalog.works.delete(disposable_id)
    assert catalog.works.get(disposable_id) is None


def test_catalog_attached_metadata_bundle_and_projections(db) -> None:
    catalog = Catalog(db)
    title = _token("bundle-work")
    work_id = catalog.works.create({"title": title})
    expression_id = catalog.expressions.match_or_create(
        work_id,
        MetadataCandidate({"label": _token("bundle-expression")}),
    )
    manifestation_id = catalog.manifestations.match_or_create(
        expression_id,
        MetadataCandidate({"subtitle": "Special edition"}),
    )
    item_id = catalog.items.match_or_create(
        manifestation_id,
        MetadataCandidate({"location": "shelf-1"}),
    )

    agent_name = _token("agent")
    agent_id = catalog.agents.match_or_create(
        MetadataCandidate({"name": agent_name})
    )
    assert catalog.agents.resolve(name=agent_name.swapcase())["agent_id"] == agent_id
    assert catalog.agents.match(
        MetadataCandidate({"name": agent_name})
    ).entity_id == agent_id
    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=work_id,
        role="aut",
        priority=1,
    )
    identifier_id = catalog.identifiers.match_or_create(
        IdentifierCandidate("uuid", str(uuid.uuid4()), source="test")
    )
    identifier = catalog.identifiers.require(identifier_id)
    assert catalog.identifiers.find(
        identifier_type="UUID",
        value=identifier["entity_identifier_value"].swapcase(),
    )["entity_identifier_id"] == identifier_id
    assert catalog.identifiers.match(
        IdentifierCandidate(
            "uuid",
            identifier["entity_identifier_value"],
        )
    ).entity_id == identifier_id
    assigned_identifier_id = catalog.identifiers.link_to_wemi(
        identifier_id=identifier_id,
        level="work",
        entity_id=work_id,
        priority=0,
    )
    note_id = catalog.notes.add_for_wemi(
        level="work",
        entity_id=work_id,
        data={"text": "A catalog note"},
    )
    catalog.notes.update(note_id, {"text": "An updated catalog note"})

    bundle = catalog.retrieval.bundles.for_item(item_id)
    assert bundle.work["work_id"] == work_id
    assert bundle.expression["expression_id"] == expression_id
    assert bundle.manifestation["manifestation_id"] == manifestation_id
    assert bundle.item["item_id"] == item_id
    assert {row["agent_id"] for row in bundle.agents} == {agent_id}
    assert {row["entity_identifier_id"] for row in bundle.identifiers} == {
        assigned_identifier_id
    }
    assert {row["note_id"] for row in bundle.notes} == {note_id}
    assert catalog.titles.get(work_id)["title"] == title
    assert catalog.titles.preferred_for_wemi(
        level="work",
        entity_id=work_id,
    )["title"] == title
    assert catalog.notes.get(note_id)["note"] == "An updated catalog note"
    assert catalog.matching.agents.exact(agent_name).entity_id == agent_id
    assert catalog.matching.agents.best(
        MetadataCandidate({"name": agent_name})
    ).entity_id == agent_id
    assert catalog.matching.agents.candidates(
        MetadataCandidate({"name": agent_name})
    )[0].entity_id == agent_id
    assert catalog.matching.identifiers.exact(
        identifier["entity_identifier_value"],
        "uuid",
    ).entity_id == identifier_id
    assert catalog.matching.identifiers.best(
        IdentifierCandidate("uuid", identifier["entity_identifier_value"])
    ).entity_id == identifier_id
    assert catalog.matching.identifiers.candidates(
        IdentifierCandidate("uuid", identifier["entity_identifier_value"])
    )[0].entity_id == identifier_id
    assert catalog.retrieval.bundles.for_work(work_id).work["work_id"] == work_id
    assert catalog.retrieval.bundles.for_expression(expression_id).expression[
        "expression_id"
    ] == expression_id
    assert catalog.retrieval.bundles.for_manifestation(
        manifestation_id
    ).manifestation["manifestation_id"] == manifestation_id
    assert catalog.retrieval.projections.display_title(
        level="item",
        entity_id=item_id,
    ) == "Special edition"
    assert catalog.retrieval.projections.item_summary(item_id)["work_id"] == work_id


def test_coordinated_attachment_and_work_merge_preserve_relationships(db) -> None:
    catalog = Catalog(db)
    source_id = catalog.works.create({"title": _token("merge-source")})
    target_id = catalog.works.create({"canonical_title": _token("merge-target")})
    expression_id = catalog.expressions.match_or_create(
        source_id,
        MetadataCandidate({"label": _token("merge-expression")}),
    )

    assert catalog.mutations.policy.can_create(
        level="work",
        data={"title": "valid"},
    )
    assert catalog.mutations.policy.can_update(
        level="work",
        entity_id=source_id,
        data={"title": "valid"},
    )
    assert not catalog.mutations.policy.can_merge(
        level="work",
        source_id=source_id,
        target_id=source_id,
    )

    catalog.mutations.writer.attach_metadata(
        level="work",
        entity_id=source_id,
        data={
            "notes": ["attached note"],
            "identifiers": [
                {"scheme": "uuid", "value": str(uuid.uuid4()), "priority": 1}
            ],
        },
    )
    catalog.mutations.writer.merge_entities(
        level="work",
        source_id=source_id,
        target_id=target_id,
    )

    assert catalog.works.get(source_id) is None
    assert catalog.expressions.list_for_work(target_id)[0][
        "expression_id"
    ] == expression_id
    assert catalog.notes.list_for_wemi(level="work", entity_id=target_id)
    assert catalog.identifiers.list_for_wemi(level="work", entity_id=target_id)
    assert catalog.titles.preferred_for_wemi(level="work", entity_id=target_id)[
        "title"
    ]

    expression_source = catalog.expressions.match_or_create(
        target_id,
        MetadataCandidate({"label": _token("expression-merge-source")}),
    )
    expression_target = catalog.expressions.match_or_create(
        target_id,
        MetadataCandidate({"label": _token("expression-merge-target")}),
    )
    manifestation_source = catalog.manifestations.match_or_create(
        expression_source,
        MetadataCandidate({"subtitle": _token("manifestation-to-transfer")}),
    )
    catalog.mutations.writer.merge_entities(
        level="expression",
        source_id=expression_source,
        target_id=expression_target,
    )
    assert catalog.expressions.get(expression_source) is None
    assert {
        row["manifestation_id"]
        for row in catalog.manifestations.list_for_expression(expression_target)
    } >= {manifestation_source}

    manifestation_target = catalog.manifestations.match_or_create(
        expression_target,
        MetadataCandidate({"subtitle": _token("manifestation-merge-target")}),
    )
    item_source = catalog.items.match_or_create(
        manifestation_source,
        MetadataCandidate({"inventory_code": _token("item-to-transfer")}),
    )
    catalog.mutations.writer.merge_entities(
        level="manifestation",
        source_id=manifestation_source,
        target_id=manifestation_target,
    )
    assert catalog.manifestations.get(manifestation_source) is None
    assert catalog.items.require(item_source)[
        "item_manifestation_id"
    ] == manifestation_target


def test_coordinated_attachment_rolls_back_every_repository_write(db) -> None:
    catalog = Catalog(db)
    original_title = _token("atomic-attachment")
    rejected_agent = _token("rejected-agent")
    work_id = catalog.works.create({"title": original_title})

    with pytest.raises(Exception, match="allowed|does not exist"):
        catalog.mutations.writer.attach_metadata(
            level="work",
            entity_id=work_id,
            data={
                "fields": {"title": "must roll back"},
                "agents": [
                    {"name": rejected_agent, "role": "not-a-marc-role"}
                ],
            },
        )

    assert catalog.works.require(work_id)["work_title"] == original_title
    assert catalog.agents.resolve(name=rejected_agent) is None


def test_catalog_implementation_satisfies_public_protocols(db) -> None:
    catalog = Catalog(db)

    assert isinstance(catalog, CatalogAPI)
    assert isinstance(catalog.repositories, CatalogRepositoriesAPI)
    assert isinstance(catalog.matching, CatalogMatchingAPI)
    assert isinstance(catalog.retrieval, CatalogRetrievalAPI)
    assert isinstance(catalog.mutations, CatalogMutationsAPI)
    for repository, protocol in (
        (catalog.works, WorkRepositoryAPI),
        (catalog.expressions, ExpressionRepositoryAPI),
        (catalog.manifestations, ManifestationRepositoryAPI),
        (catalog.items, ItemRepositoryAPI),
        (catalog.agents, AgentRepositoryAPI),
        (catalog.identifiers, IdentifierRepositoryAPI),
        (catalog.titles, TitleRepositoryAPI),
        (catalog.notes, NoteRepositoryAPI),
    ):
        assert isinstance(repository, protocol)
    assert isinstance(catalog.matching.works, WorkMatcherAPI)
    assert isinstance(catalog.matching.agents, AgentMatcherAPI)
    assert isinstance(catalog.matching.identifiers, IdentifierMatcherAPI)


def test_shared_value_writer_build_is_pure_and_failed_write_is_atomic(db) -> None:
    db.driver_wrapper.executescript(
        """
        CREATE TABLE catalog_atomic_sources (
            catalog_atomic_source_id INTEGER PRIMARY KEY
        );
        CREATE TABLE catalog_atomic_values (
            catalog_atomic_value_id INTEGER PRIMARY KEY,
            catalog_atomic_value_name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE catalog_atomic_source_catalog_atomic_value_links (
            catalog_atomic_source_catalog_atomic_value_link_catalog_atomic_source_id
                INTEGER NOT NULL UNIQUE,
            catalog_atomic_source_catalog_atomic_value_link_catalog_atomic_value_id
                INTEGER NOT NULL,
            FOREIGN KEY(
                catalog_atomic_source_catalog_atomic_value_link_catalog_atomic_source_id
            ) REFERENCES catalog_atomic_sources(catalog_atomic_source_id),
            FOREIGN KEY(
                catalog_atomic_source_catalog_atomic_value_link_catalog_atomic_value_id
            ) REFERENCES catalog_atomic_values(catalog_atomic_value_id)
        );
        INSERT INTO catalog_atomic_sources VALUES (1);
        """
    )
    catalog = Catalog(db)
    writer = catalog.create_writer(
        "catalog_atomic_sources",
        "catalog_atomic_value_name",
        force_refresh=True,
    )

    update = writer.build_update({1: "created only during apply"})
    assert db.driver_wrapper.get_record_count("catalog_atomic_values") == 0
    assert update

    with pytest.raises(ValueError, match="at most one destination"):
        writer.write({1: ("first", "second")})

    assert db.driver_wrapper.get_record_count("catalog_atomic_values") == 0

    with pytest.raises(Exception, match="FOREIGN KEY|foreign key|constraint"):
        writer.write({999: "rolled back after destination resolution"})

    assert db.driver_wrapper.get_record_count("catalog_atomic_values") == 0
