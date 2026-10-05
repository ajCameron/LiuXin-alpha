"""
Exercise Catalog convenience composition, relationships, metadata replacement, and annotations.

Use real schema-backed rows and writer operations for a branched WEMI graph,
primary/origin metadata, group clear/replace behavior, rollback observation, and
Item/user/kind filtering. Private helpers generate isolated labels, create stacks,
and interpret primary flags; runtime protocol assertions cover structural presence.
"""

from __future__ import annotations

import uuid

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api import CatalogMutationError
from LiuXin_alpha.catalog.api.mutations_api import MetadataWriterAPI
from LiuXin_alpha.catalog.api.repositories import (
    AgentRepositoryAPI,
    AnnotationRepositoryAPI,
    CatalogRepositoriesAPI,
    CommentRepositoryAPI,
    IdentifierRepositoryAPI,
    NoteRepositoryAPI,
    SynopsisRepositoryAPI,
    TitleRepositoryAPI,
)
from LiuXin_alpha.catalog.api.retrieval import (
    CatalogRetrievalAPI,
    HierarchyRetrieverAPI,
    WemiGraphRetrieverAPI,
)


def _token(prefix: str) -> str:
    """
    Build a readable fixture label with a fresh UUID4 hexadecimal suffix.

    Format the prefix directly and append one hyphen plus 32 hexadecimal UUID characters. Each call
    is independent; the helper does not query the database or enforce absolute uniqueness.

    Example:
        >>> value = _token("label")
        >>> value.startswith("label-"), len(value[6:]), uuid.UUID(value[6:]).version
        (True, 32, 4)


    :param prefix: Readable label prepended to a new UUID4 hex string.
    :return: Prefix and UUID4 hex joined by a hyphen.
    """
    return f"{prefix}-{uuid.uuid4().hex}"


def _stack(catalog: Catalog):
    """
    Create a one-Item WEMI stack for the convenience integration tests.

    Call the coordinated writer with fresh Work title, Expression label, Manifestation subtitle, and
    Item inventory code. Mark the operation with catalog-convenience-test origin and return the
    writer's creation receipt unchanged. This helper performs real persistence, not a stand-in
    construction.

    Example:
        >>> created = _stack(catalog)  # doctest: +SKIP


    :param catalog: Catalog whose mutation writer creates the linked WEMI test entities.
    :return: Writer creation receipt carrying the Work, Expression, Manifestation, and Item IDs.
    """
    return catalog.mutations.writer.create_wemi_stack(
        work={"title": _token("Convenience work")},
        expression={"label": _token("Convenience expression")},
        manifestation={"subtitle": _token("Convenience manifestation")},
        items=({"inventory_code": _token("Convenience item")},),
        origin="catalog-convenience-test",
    )


def _link_primary(row) -> bool:
    """
    Report whether any primary-marker extra field has a truthy value.

    Inspect row["_catalog_link"]["extra"] keys ending in _primary and apply bool to their values.
    Return False when there are no such keys or all are false. This is a test assertion helper, not
    a schema/type validator or a comparison of the link priority.

    Example:
        >>> _link_primary({"_catalog_link": {"extra": {"credit_primary": 1}}})
        True
        >>> _link_primary({"_catalog_link": {"extra": {}}})
        False


    :param row: Traversed row mapping containing _catalog_link and an extra mapping with string keys.
    :return: True if any matching extra-field value is truthy, otherwise False.
    """
    link = row["_catalog_link"]
    return any(
        bool(value)
        for key, value in link["extra"].items()
        if key.endswith("_primary")
    )


def test_convenience_contracts_and_repository_lookup_are_public(db) -> None:
    """
    Check selected convenience services satisfy protocols and resolve public aliases.

    Verify runtime protocol presence on the repository group, mutation writer, retrieval
    group/hierarchy/graph, and eight specialized repositories. Require Work, Item-Identifier, and
    synopsis lookups to return the existing shortcut instances, and reject books with KeyError.
    Structural isinstance checks do not establish signature compatibility or exercise every declared
    operation.

    Example:
        >>> test_convenience_contracts_and_repository_lookup_are_public(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed test database receiving real Catalog, relationship, and mutation operations.
    :return: None after the stated regression assertions pass.
    """

    catalog = Catalog(db)

    assert isinstance(catalog.repositories, CatalogRepositoriesAPI)
    assert isinstance(catalog.mutations.writer, MetadataWriterAPI)
    assert isinstance(catalog.retrieval, CatalogRetrievalAPI)
    assert isinstance(catalog.retrieval.hierarchy, HierarchyRetrieverAPI)
    assert isinstance(catalog.retrieval.graph, WemiGraphRetrieverAPI)
    assert isinstance(catalog.agents, AgentRepositoryAPI)
    assert isinstance(catalog.identifiers, IdentifierRepositoryAPI)
    assert isinstance(catalog.titles, TitleRepositoryAPI)
    assert isinstance(catalog.notes, NoteRepositoryAPI)
    assert isinstance(catalog.comments, CommentRepositoryAPI)
    assert isinstance(catalog.synopses, SynopsisRepositoryAPI)
    assert isinstance(catalog.annotations, AnnotationRepositoryAPI)

    assert catalog.repositories.for_name("work") is catalog.works
    assert catalog.repositories.for_name("Item-Identifier") is (
        catalog.item_identifiers
    )
    assert catalog.repositories.for_name("synopsis") is catalog.synopses
    with pytest.raises(KeyError, match="unknown Catalog repository"):
        catalog.repositories.for_name("books")


def test_wemi_link_unlink_hierarchy_and_graph_conveniences(db) -> None:
    """
    Preserve WEMI link metadata while traversing, bounding, and unlinking a branched stack.

    Create a second Expression/Manifestation/Item branch and make its Work link primary at priority
    seven with origin metadata. Verify the original branch loses primary status, hierarchy
    parents/children contain the expected IDs, and a full graph contains both branches and six links
    without truncation.

    Limit Expressions to one and require descendant truncation markers; zero limits yield no
    descendants or links. Unlink the second Item, observe its unassigned parent and a false
    repeated-unlink result, then reject a nonadjacent Work-to-Item link. No assertion here requires
    deleting the detached Item.

    Example:
        >>> test_wemi_link_unlink_hierarchy_and_graph_conveniences(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed test database receiving real Catalog, relationship, and mutation operations.
    :return: None after the stated regression assertions pass.
    """

    catalog = Catalog(db)
    created = _stack(catalog)
    expression_id = catalog.expressions.create(
        {"label": _token("Second expression")}
    )
    manifestation_id = catalog.manifestations.create(
        {"subtitle": _token("Second manifestation")}
    )
    item_id = catalog.items.create(
        {"inventory_code": _token("Second item")}
    )

    receipt = catalog.mutations.writer.link_wemi(
        parent_level="work",
        parent_id=created.work_id,
        child_level="expression",
        child_id=expression_id,
        primary=True,
        priority=7,
        origin="catalog-convenience-link",
    )
    assert receipt["link"]["priority"] == 7
    assert any(
        value == "catalog-convenience-link"
        for key, value in receipt["link"]["extra"].items()
        if key.endswith("_origin")
    )
    catalog.mutations.writer.link_wemi(
        parent_level="expression",
        parent_id=expression_id,
        child_level="manifestation",
        child_id=manifestation_id,
    )
    catalog.mutations.writer.link_wemi(
        parent_level="manifestation",
        parent_id=manifestation_id,
        child_level="item",
        child_id=item_id,
    )

    expressions = catalog.expressions.list_for_work(created.work_id)
    primary_by_id = {
        row["expression_id"]: _link_primary(row) for row in expressions
    }
    assert primary_by_id == {
        created.expression_id: False,
        expression_id: True,
    }

    children = catalog.retrieval.hierarchy.children(
        level="work",
        entity_id=created.work_id,
    )
    assert children.related_level == "expression"
    assert {row["expression_id"] for row in children.entities} == {
        created.expression_id,
        expression_id,
    }
    parents = catalog.retrieval.hierarchy.parents(
        level="item",
        entity_id=item_id,
    )
    assert parents.related_level == "manifestation"
    assert [row["manifestation_id"] for row in parents.entities] == [
        manifestation_id
    ]

    graph = catalog.retrieval.graph.for_work(created.work_id)
    assert graph.work["work_id"] == created.work_id
    assert {row["expression_id"] for row in graph.expressions} == {
        created.expression_id,
        expression_id,
    }
    assert {row["manifestation_id"] for row in graph.manifestations} == {
        created.manifestation_id,
        manifestation_id,
    }
    assert {row["item_id"] for row in graph.items} == {
        created.item_ids[0],
        item_id,
    }
    assert len(graph.links) == 6
    assert graph.truncated_levels == ()

    bounded = catalog.retrieval.graph.for_work(
        created.work_id,
        max_expressions=1,
        max_manifestations=10,
        max_items=10,
    )
    assert len(bounded.expressions) == 1
    assert bounded.truncated_levels == (
        "expression",
        "manifestation",
        "item",
    )
    empty = catalog.retrieval.graph.for_work(
        created.work_id,
        max_expressions=0,
        max_manifestations=0,
        max_items=0,
    )
    assert empty.expressions == empty.manifestations == empty.items == ()
    assert empty.links == ()

    assert catalog.mutations.writer.unlink_wemi(
        parent_level="manifestation",
        parent_id=manifestation_id,
        child_level="item",
        child_id=item_id,
    )
    assert catalog.items.manifestation_for_item(item_id) is None
    assert not catalog.mutations.writer.unlink_wemi(
        parent_level="manifestation",
        parent_id=manifestation_id,
        child_level="item",
        child_id=item_id,
    )
    with pytest.raises(CatalogMutationError, match="adjacent"):
        catalog.mutations.writer.link_wemi(
            parent_level="work",
            parent_id=created.work_id,
            child_level="item",
            child_id=item_id,
        )


def test_replace_metadata_has_complete_group_and_rollback_semantics(db) -> None:
    """
    Exercise populated metadata replacement, explicit clearing, and a later rollback.

    Replace Work fields/title, one Agent credit, two identifier schemes, Note, Comment, and Synopsis
    groups, then verify their stored projections. Clear all these groups explicitly; title=None also
    clears the canonical title despite omitting a separate fields payload in that call.

    Restore a canonical title through fields, attempt another field update followed by an invalid
    Note member, and verify the earlier title survives the TypeError. That final assertion observes
    field rollback; it is not a full-table snapshot or proof that every omitted group is independent
    of title conveniences.

    Example:
        >>> test_replace_metadata_has_complete_group_and_rollback_semantics(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed test database receiving real Catalog, relationship, and mutation operations.
    :return: None after the stated regression assertions pass.
    """

    catalog = Catalog(db)
    created = _stack(catalog)
    work_id = created.work_id
    person_id = catalog.agents.create_person(
        {"name": _token("Convenience author")}
    )
    note = _token("Convenience note")
    comment = _token("Convenience comment")
    synopsis = _token("Convenience synopsis")
    replacement_title = _token("Convenience replacement title")

    catalog.mutations.writer.replace_metadata(
        level="work",
        entity_id=work_id,
        data={
            "fields": {"canonical_title": "Persistent canonical title"},
            "title": replacement_title,
            "agents": (
                {"agent_id": person_id, "role": "aut", "priority": 0},
            ),
            "identifiers": {
                "ISBN-13": "978-0-14-143947-1",
                "doi": "10.1000/catalog-convenience",
            },
            "notes": [note],
            "comments": comment,
            "synopses": [{"text": synopsis}],
        },
    )

    work = catalog.works.require(work_id)
    assert work["work_title"] == replacement_title
    assert work["work_canonical_title"] == "Persistent canonical title"
    assert [
        row["agent_id"]
        for row in catalog.agents.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
    ] == [person_id]
    assert catalog.identifiers.primary_values_for_wemi(
        level="work",
        entity_id=work_id,
    ) == {
        "isbn13": "978-0-14-143947-1",
        "doi": "10.1000/catalog-convenience",
    }
    assert [
        row["note"]
        for row in catalog.notes.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
    ] == [note]
    assert [
        row["comment"]
        for row in catalog.comments.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
    ] == [comment]
    assert [
        row["synopsis"]
        for row in catalog.synopses.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
    ] == [synopsis]

    catalog.mutations.writer.replace_metadata(
        level="work",
        entity_id=work_id,
        data={
            "title": None,
            "agents": [],
            "identifiers": {},
            "notes": [],
            "comments": None,
            "synopses": [],
        },
    )
    work = catalog.works.require(work_id)
    assert work["work_title"] is None
    assert work["work_canonical_title"] is None
    assert not catalog.agents.list_for_wemi(level="work", entity_id=work_id)
    assert not catalog.identifiers.list_for_wemi(
        level="work",
        entity_id=work_id,
    )
    assert not catalog.notes.list_for_wemi(level="work", entity_id=work_id)
    assert not catalog.comments.list_for_wemi(level="work", entity_id=work_id)
    assert not catalog.synopses.list_for_wemi(
        level="work",
        entity_id=work_id,
    )

    catalog.mutations.writer.replace_metadata(
        level="work",
        entity_id=work_id,
        data={"fields": {"canonical_title": "Persistent canonical title"}},
    )
    with pytest.raises(TypeError, match="notes must contain"):
        catalog.mutations.writer.replace_metadata(
            level="work",
            entity_id=work_id,
            data={
                "fields": {"canonical_title": "Must roll back"},
                "notes": [object()],
            },
        )
    assert catalog.works.require(work_id)["work_canonical_title"] == (
        "Persistent canonical title"
    )


def test_annotation_listing_is_item_scoped_and_filterable(db) -> None:
    """
    Select one Item Annotation by user and kind, and reject a blank kind filter.

    Create two differently classified annotations on one Item and a same-user/kind annotation on
    another Item. Check unfiltered results are ID ordered, then require combined user/kind filtering
    to return only the expected first-Item ID. A whitespace-only kind must raise ValueError. This
    test uses existing Items and does not exercise the missing-Item or invalid-user-ID error paths.

    Example:
        >>> test_annotation_listing_is_item_scoped_and_filterable(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed test database receiving real Catalog, relationship, and mutation operations.
    :return: None after the stated regression assertions pass.
    """

    catalog = Catalog(db)
    created = _stack(catalog)
    item_id = created.item_ids[0]
    other_item_id = catalog.items.create({"type": "digital"})
    matching_id = catalog.annotations.create(
        {
            "item_id": item_id,
            "user_id": 11,
            "kind": "highlight",
            "anchor_type": "offset",
            "anchor_start": "10",
            "source": "test",
        }
    )
    catalog.annotations.create(
        {
            "item_id": item_id,
            "user_id": 12,
            "kind": "bookmark",
            "anchor_type": "offset",
            "anchor_start": "20",
            "source": "test",
        }
    )
    catalog.annotations.create(
        {
            "item_id": other_item_id,
            "user_id": 11,
            "kind": "highlight",
            "anchor_type": "offset",
            "anchor_start": "10",
            "source": "test",
        }
    )

    assert [
        row["annotation_id"]
        for row in catalog.annotations.list_for_item(item_id)
    ] == sorted(
        row["annotation_id"]
        for row in catalog.annotations.list_for_item(item_id)
    )
    assert [
        row["annotation_id"]
        for row in catalog.annotations.list_for_item(
            item_id,
            user_id=11,
            kind="highlight",
        )
    ] == [matching_id]
    with pytest.raises(ValueError, match="non-empty"):
        catalog.annotations.list_for_item(item_id, kind=" ")
