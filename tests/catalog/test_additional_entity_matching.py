"""Behavior tests for exact-default matching on additional catalog entities."""

from __future__ import annotations

import uuid

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api import (
    CatalogAmbiguousMatchError,
    CatalogMatchConflictError,
    CatalogMutationError,
    IdentifierCandidate,
    MetadataCandidate,
)
from LiuXin_alpha.catalog.api.matching_api import (
    ExactEntityMatcherAPI,
    ItemIdentifierMatcherAPI,
)
from LiuXin_alpha.catalog.api.repositories import (
    ExactEntityRepositoryAPI,
    ItemIdentifierRepositoryAPI,
)


def _token(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


@pytest.mark.parametrize(
    ("repository_name", "payload", "candidate", "scalar"),
    (
        ("tags", {"name": "Exact Tag"}, {"name": "exact tag"}, "EXACT TAG"),
        (
            "labels",
            {"text": "Exact Label"},
            {"text": "exact label"},
            "EXACT LABEL",
        ),
        (
            "genres",
            {"name": "Exact Genre"},
            {"name": "exact genre"},
            "EXACT GENRE",
        ),
        (
            "subjects",
            {"name": "Exact Subject"},
            {"name": "exact subject"},
            "EXACT SUBJECT",
        ),
        (
            "series",
            {"name": "Exact Series"},
            {"name": "exact series"},
            "EXACT SERIES",
        ),
        (
            "ratings",
            {"value": 4.25, "out_of": 5, "source": "local"},
            {"value": 4.25, "out_of": 5, "source": "LOCAL"},
            4.25,
        ),
        (
            "comments",
            {"text": "Exact comment text."},
            {"text": "Exact comment text."},
            "Exact comment text.",
        ),
        (
            "synopses",
            {"text": "Exact synopsis text."},
            {"text": "Exact synopsis text."},
            "Exact synopsis text.",
        ),
        (
            "notes",
            {"text": "Exact note text."},
            {"text": "Exact note text."},
            "Exact note text.",
        ),
    ),
)
def test_additional_entities_match_exactly_by_default(
    db,
    repository_name: str,
    payload: dict[str, object],
    candidate: dict[str, object],
    scalar: object,
) -> None:
    catalog = Catalog(db)
    repository = getattr(catalog, repository_name)
    entity_id = repository.create(payload)

    repository_result = repository.match(MetadataCandidate(candidate))
    grouped_result = getattr(catalog.matching, repository_name).best(
        MetadataCandidate(candidate)
    )
    scalar_result = repository.exact(scalar)

    assert repository_result.entity_id == entity_id
    assert grouped_result.entity_id == entity_id
    assert scalar_result.entity_id == entity_id
    assert repository_result.evidence
    assert all(item.kind == "exact" for item in repository_result.evidence)
    assert all(item.decisive for item in repository_result.evidence)


def test_policy_matching_is_off_by_default_and_explicitly_opt_in(db) -> None:
    catalog = Catalog(db)
    name = _token("Speculative Fiction")
    tag_id = catalog.tags.create({"name": name})
    approximate_name = name.replace("Fiction", "Fictio")
    candidate = MetadataCandidate({"name": approximate_name})

    default_result = catalog.tags.match(candidate)
    grouped_default = catalog.matching.tags.best(candidate)
    policy_result = catalog.tags.match(candidate, use_policy=True)

    assert default_result.decision == "no_match"
    assert grouped_default.decision == "no_match"
    assert policy_result.entity_id == tag_id
    assert policy_result.evidence[0].kind == "approximate"


def test_exact_match_or_create_does_not_enable_policy_implicitly(db) -> None:
    catalog = Catalog(db)
    name = _token("Reusable Tag")
    first_id = catalog.tags.match_or_create(MetadataCandidate({"name": name}))
    exact_id = catalog.tags.match_or_create(
        MetadataCandidate({"name": name.swapcase()})
    )
    approximate_id = catalog.tags.match_or_create(
        MetadataCandidate({"name": name[:-1]})
    )

    assert exact_id == first_id
    assert approximate_id != first_id


def test_match_or_create_can_reuse_policy_match_when_explicitly_enabled(db) -> None:
    catalog = Catalog(db)
    name = _token("Policy Reuse Tag")
    tag_id = catalog.tags.create({"name": name})

    reused_id = catalog.tags.match_or_create(
        MetadataCandidate({"name": name[:-1]}),
        use_policy=True,
    )

    assert reused_id == tag_id


def test_exact_matching_does_not_depend_on_derived_storage_columns(db) -> None:
    catalog = Catalog(db)
    label_text = _token("Legacy Label")
    label_id = db.macros.insert_row(
        "labels",
        {"label_text": label_text},
        id_column="label_id",
    )

    result = catalog.labels.exact(label_text.swapcase())

    assert result.entity_id == label_id


def test_duplicate_exact_values_are_ambiguous_and_block_reuse(db) -> None:
    catalog = Catalog(db)
    name = _token("Duplicate Tag")
    first_id = catalog.tags.create({"name": name})
    second_id = catalog.tags.create({"name": name.swapcase()})
    candidate = MetadataCandidate({"name": name})

    result = catalog.tags.match(candidate)

    assert result.decision == "ambiguous"
    assert result.alternatives == (first_id, second_id)
    with pytest.raises(CatalogAmbiguousMatchError):
        catalog.tags.match_or_create(candidate)


def test_conflicting_exact_fields_report_conflict(db) -> None:
    catalog = Catalog(db)
    first_name = _token("First Tag")
    second_name = _token("Second Tag")
    first_id = catalog.tags.create({"name": first_name, "phash": _token("hash-a")})
    second_hash = _token("hash-b")
    second_id = catalog.tags.create({"name": second_name, "phash": second_hash})
    candidate = MetadataCandidate({"name": first_name, "phash": second_hash})

    result = catalog.tags.match(candidate)

    assert result.decision == "conflict"
    assert result.alternatives == (first_id, second_id)
    with pytest.raises(CatalogMatchConflictError):
        catalog.tags.match_or_create(candidate)


def test_hierarchical_exact_matches_can_be_scoped_by_parent(db) -> None:
    catalog = Catalog(db)
    first_parent = catalog.genres.create({"name": _token("Parent A")})
    second_parent = catalog.genres.create({"name": _token("Parent B")})
    child_name = _token("Shared Child")
    first_child = catalog.genres.create(
        {"name": child_name, "parent_id": first_parent}
    )
    second_child = catalog.genres.create(
        {"name": child_name, "parent_id": second_parent}
    )

    unscoped = catalog.genres.exact(child_name)
    scoped = catalog.genres.exact(child_name, parent_id=first_parent)

    assert unscoped.decision == "ambiguous"
    assert unscoped.alternatives == (first_child, second_child)
    assert scoped.entity_id == first_child


def test_languages_match_seeded_names_and_code_variants_but_are_read_only(db) -> None:
    catalog = Catalog(db)

    by_name = catalog.languages.exact("English")
    by_iso_one = catalog.languages.exact("en")
    by_code_candidate = catalog.languages.match(MetadataCandidate({"code": "en"}))

    assert by_name.is_match
    assert by_iso_one.entity_id == by_name.entity_id
    assert by_code_candidate.entity_id == by_name.entity_id
    with pytest.raises(CatalogMutationError, match="read-only"):
        catalog.languages.create({"code": "zzz"})
    with pytest.raises(CatalogMutationError, match="read-only"):
        catalog.languages.match_or_create(MetadataCandidate({"code": "zzz"}))


def test_owned_comments_and_annotations_reject_global_reuse(db) -> None:
    catalog = Catalog(db)
    comment_text = _token("Owned comment")
    comment_id = catalog.comments.create({"text": comment_text})
    item_id = catalog.items.create({"type": "digital"})
    annotation_id = catalog.annotations.create(
        {
            "item_id": item_id,
            "kind": "bookmark",
            "anchor_type": "percentage",
            "anchor_start": "0.5",
            "source": "internal",
        }
    )

    assert catalog.comments.exact(comment_text).entity_id == comment_id
    assert catalog.annotations.match(
        MetadataCandidate(
            {
                "item_id": item_id,
                "kind": "bookmark",
                "anchor_type": "percentage",
                "anchor_start": "0.5",
                "source": "internal",
            }
        )
    ).entity_id == annotation_id
    assert catalog.annotations.match(
        MetadataCandidate(
            {
                "kind": "bookmark",
                "anchor_type": "percentage",
                "anchor_start": "0.5",
            }
        )
    ).decision == "no_match"
    assert catalog.annotations.match(
        MetadataCandidate(
            {
                "item_id": item_id,
                "anchor_start": "0.5",
            }
        )
    ).decision == "no_match"
    with pytest.raises(CatalogMutationError, match="contextual creation"):
        catalog.comments.match_or_create(MetadataCandidate({"text": comment_text}))
    with pytest.raises(CatalogMutationError, match="contextual creation"):
        catalog.annotations.match_or_create(
            MetadataCandidate(
                {
                    "item_id": item_id,
                    "kind": "bookmark",
                    "anchor_type": "percentage",
                    "anchor_start": "0.5",
                }
            )
        )


def test_observed_item_identifiers_match_exactly_with_optional_item_scope(db) -> None:
    catalog = Catalog(db)
    first_item = catalog.items.create({"type": "digital"})
    second_item = catalog.items.create({"type": "physical"})
    value = str(uuid.uuid4())
    candidate = IdentifierCandidate("UUID", value.upper(), source="scan")
    first_identifier = catalog.item_identifiers.match_or_create(
        first_item,
        candidate,
    )
    repeated_identifier = catalog.item_identifiers.match_or_create(
        first_item,
        IdentifierCandidate("uuid", value),
    )
    second_identifier = catalog.item_identifiers.match_or_create(
        second_item,
        IdentifierCandidate("uuid", value),
    )

    assert repeated_identifier == first_identifier
    assert second_identifier != first_identifier
    assert catalog.item_identifiers.match(
        IdentifierCandidate("uuid", value),
        item_id=first_item,
    ).entity_id == first_identifier
    assert catalog.item_identifiers.exact(
        value,
        "uuid",
        item_id=first_item,
    ).entity_id == first_identifier
    assert catalog.matching.item_identifiers.exact(
        value,
        "uuid",
        item_id=second_item,
    ).entity_id == second_identifier
    assert tuple(
        row["item_identifier_id"]
        for row in catalog.item_identifiers.list_for_item(first_item)
    ) == (first_identifier,)


def test_grouped_factory_and_public_protocols_cover_every_additional_entity(db) -> None:
    catalog = Catalog(db)
    entity_names = (
        "tags",
        "labels",
        "genres",
        "subjects",
        "series",
        "languages",
        "ratings",
        "comments",
        "synopses",
        "notes",
        "annotations",
    )

    for entity_name in entity_names:
        repository = getattr(catalog, entity_name)
        matcher = getattr(catalog.matching, entity_name)
        assert isinstance(repository, ExactEntityRepositoryAPI)
        assert isinstance(matcher, ExactEntityMatcherAPI)
        assert catalog.matching.for_entity(entity_name) is matcher
    assert catalog.matching.for_entity("tag") is catalog.matching.tags
    assert catalog.matching.for_entity("synopsis") is catalog.matching.synopses
    assert isinstance(catalog.item_identifiers, ItemIdentifierRepositoryAPI)
    assert isinstance(
        catalog.matching.item_identifiers,
        ItemIdentifierMatcherAPI,
    )
    with pytest.raises(KeyError, match="no exact-default matcher"):
        catalog.matching.for_entity("publisher")
