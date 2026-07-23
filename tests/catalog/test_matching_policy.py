"""Behavior tests for the catalog identity matching policy."""

from __future__ import annotations

import uuid

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api import (
    CatalogAmbiguousMatchError,
    CatalogMatchConflictError,
    IdentifierCandidate,
    MatchEvidence,
    MatchResult,
    MetadataCandidate,
)
from LiuXin_alpha.catalog.matching.policy import MatchingPolicy, normalise_identifier


def test_match_results_expose_explicit_validated_decisions() -> None:
    legacy_match = MatchResult(7, 0.9, "legacy compatible")
    legacy_non_match = MatchResult(None, 0.0, "legacy compatible")

    assert legacy_match.decision == "match"
    assert legacy_match.is_match
    assert legacy_non_match.decision == "no_match"
    assert not legacy_non_match.is_match
    assert not legacy_non_match.requires_resolution
    assert MatchResult(
        None,
        1.0,
        "duplicate exact values",
        decision="ambiguous",
        alternatives=(2, 3),
    ).requires_resolution

    with pytest.raises(ValueError, match="requires an entity_id"):
        MatchResult(None, 1.0, "invalid", decision="match")
    with pytest.raises(ValueError, match="only a match decision"):
        MatchResult(7, 0.0, "invalid", decision="no_match")
    with pytest.raises(ValueError, match="between zero and one"):
        MatchEvidence("title", "exact", 1.1, 1.0, "invalid")


def test_identifier_normalization_is_scheme_specific() -> None:
    isbn = normalise_identifier(
        IdentifierCandidate("ISBN-13", "978-0-306-40615-7")
    )
    doi = normalise_identifier(
        IdentifierCandidate("DOI", "https://doi.org/10.1000/ABC")
    )
    generic = normalise_identifier(
        IdentifierCandidate("publisher_code", " AbC/Def ")
    )

    assert (isbn.identifier_type, isbn.normalised_value) == (
        "isbn13",
        "9780306406157",
    )
    assert doi.normalised_value == "10.1000/abc"
    assert generic.normalised_value == "AbC/Def"
    with pytest.raises(ValueError, match="invalid ISBN"):
        normalise_identifier(IdentifierCandidate("isbn13", "978-0-306-40615-8"))


def test_catalog_shares_one_configured_matching_policy(db) -> None:
    policy = MatchingPolicy(ambiguity_margin=0.01)
    catalog = Catalog(db, matching_policy=policy)

    assert catalog.matching.policy is policy
    assert catalog.works.matching_policy is policy
    assert catalog.agents.matching_policy is policy


def test_duplicate_exact_work_titles_are_ambiguous_and_block_creation(db) -> None:
    catalog = Catalog(db)
    title = f"Duplicate: Work {uuid.uuid4()}"
    first_id = catalog.works.create({"title": title})
    second_id = catalog.works.create({"title": title.replace(":", "")})
    candidate = MetadataCandidate({"title": title})

    result = catalog.works.match(candidate)

    assert result.decision == "ambiguous"
    assert result.entity_id is None
    assert result.alternatives == (first_id, second_id)
    with pytest.raises(CatalogAmbiguousMatchError) as raised:
        catalog.works.match_or_create(candidate)
    assert raised.value.result == result


def test_sparse_work_rows_and_corroborated_approximate_titles_match(db) -> None:
    catalog = Catalog(db)
    title = f"The Long Journey Home {uuid.uuid4()}"
    work_id = catalog.works.create({"title": title, "original_year": 1984})

    exact = catalog.works.match(
        MetadataCandidate({"title": title, "medium": "text"})
    )
    approximate_without_support = catalog.works.match(
        MetadataCandidate({"title": title.replace("Home", "Hom")})
    )
    corroborated = catalog.works.match(
        MetadataCandidate(
            {"title": title.replace("Home", "Hom"), "original_year": 1984}
        )
    )

    assert exact.entity_id == work_id
    assert exact.confidence == 1.0
    assert approximate_without_support.decision == "no_match"
    assert corroborated.entity_id == work_id
    assert "work_original_year" in corroborated.matched_on


def test_work_identifier_conflicts_are_explicit_and_block_creation(db) -> None:
    catalog = Catalog(db)
    first_title = f"Identifier Work One {uuid.uuid4()}"
    second_title = f"Identifier Work Two {uuid.uuid4()}"
    first_work_id = catalog.works.create({"title": first_title})
    second_work_id = catalog.works.create({"title": second_title})
    first_value = str(uuid.uuid4())
    second_value = str(uuid.uuid4())
    first_identifier_id = catalog.identifiers.match_or_create(
        IdentifierCandidate("uuid", first_value)
    )
    second_identifier_id = catalog.identifiers.match_or_create(
        IdentifierCandidate("uuid", second_value)
    )
    catalog.identifiers.link_to_wemi(
        identifier_id=first_identifier_id,
        level="work",
        entity_id=first_work_id,
    )
    catalog.identifiers.link_to_wemi(
        identifier_id=second_identifier_id,
        level="work",
        entity_id=second_work_id,
    )
    unique_result = catalog.works.match(
        MetadataCandidate(
            {"title": first_title},
            hints={"identifiers": {"uuid": first_value}},
        )
    )
    candidate = MetadataCandidate(
        {"title": first_title},
        hints={
            "identifiers": [
                {"scheme": "uuid", "value": first_value},
                {"scheme": "uuid", "value": second_value},
            ]
        },
    )

    result = catalog.matching.works.best(candidate)

    assert unique_result.entity_id == first_work_id
    assert unique_result.evidence[0].decisive
    assert result.decision == "conflict"
    assert result.evidence
    assert all(item.decisive for item in result.evidence)
    assert result.alternatives == (first_work_id, second_work_id)
    with pytest.raises(CatalogMatchConflictError) as raised:
        catalog.works.match_or_create(candidate)
    assert raised.value.result == result


def test_identifier_owner_rejects_a_radically_different_work_title(db) -> None:
    catalog = Catalog(db)
    title = f"A Treatise on Botany {uuid.uuid4()}"
    work_id = catalog.works.create({"title": title})
    value = str(uuid.uuid4())
    identifier_id = catalog.identifiers.match_or_create(
        IdentifierCandidate("uuid", value)
    )
    catalog.identifiers.link_to_wemi(
        identifier_id=identifier_id,
        level="work",
        entity_id=work_id,
    )

    result = catalog.works.match(
        MetadataCandidate(
            {"title": f"Interstellar Warfare {uuid.uuid4()}"},
            hints={"identifiers": {"uuid": value}},
        )
    )

    assert result.decision == "conflict"
    assert result.alternatives == (work_id,)


def test_duplicate_exact_agent_names_are_ambiguous(db) -> None:
    catalog = Catalog(db)
    name = f"Alex Example {uuid.uuid4()}"
    first_id = catalog.agents.create({"name": name, "type": "person"})
    second_id = catalog.agents.create({"name": name, "type": "person"})

    result = catalog.agents.match(MetadataCandidate({"name": name}))

    assert result.decision == "ambiguous"
    assert result.alternatives == (first_id, second_id)
    with pytest.raises(CatalogAmbiguousMatchError):
        catalog.agents.match_or_create(MetadataCandidate({"name": name}))


def test_agent_aliases_match_exactly_and_agent_type_can_reject_them(db) -> None:
    catalog = Catalog(db)
    canonical_name = f"Example Organisation {uuid.uuid4()}"
    alias = f"Example Press {uuid.uuid4()}"
    agent_id = catalog.agents.create(
        {
            "name": canonical_name,
            "type": "organisation",
            "aliases": f"Old name; {alias}",
        }
    )

    exact_alias = catalog.agents.match(
        MetadataCandidate({"name": alias, "type": "organisation"})
    )
    wrong_type = catalog.agents.match(
        MetadataCandidate({"name": alias, "type": "person"})
    )
    approximate_name = catalog.agents.match(
        MetadataCandidate({"name": alias.removesuffix(alias[-1])})
    )

    assert exact_alias.entity_id == agent_id
    assert "agent_aliases" in exact_alias.matched_on
    assert wrong_type.decision == "no_match"
    assert approximate_name.decision == "no_match"


def test_agent_hint_corroborates_an_approximate_work_title(db) -> None:
    catalog = Catalog(db)
    title = f"Collected Tales from Elsewhere {uuid.uuid4()}"
    work_id = catalog.works.create({"title": title})
    agent_name = f"Morgan Writer {uuid.uuid4()}"
    other_name = f"Another Writer {uuid.uuid4()}"
    agent_id = catalog.agents.create({"name": agent_name})
    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=work_id,
        role="aut",
    )
    approximate_title = title.replace("Elsewhere", "Elsewher")

    corroborated = catalog.works.match(
        MetadataCandidate(
            {"title": approximate_title},
            hints={"agents": [agent_name]},
        )
    )
    contradicted = catalog.works.match(
        MetadataCandidate(
            {"title": approximate_title},
            hints={"agents": [other_name]},
        )
    )

    assert corroborated.entity_id == work_id
    assert "agents" in corroborated.matched_on
    assert contradicted.decision == "no_match"


def test_contextual_duplicate_expressions_are_ambiguous(db) -> None:
    catalog = Catalog(db)
    work_id = catalog.works.create({"title": f"Scoped Work {uuid.uuid4()}"})
    label = f"English text {uuid.uuid4()}"
    first_id = catalog.expressions.create({"label": label})
    second_id = catalog.expressions.create({"label": label})
    catalog.expressions._link("works", work_id, "expressions", first_id)
    catalog.expressions._link("works", work_id, "expressions", second_id)

    result = catalog.expressions.match(work_id, MetadataCandidate({"label": label}))

    assert result.decision == "ambiguous"
    assert result.alternatives == (first_id, second_id)
    with pytest.raises(CatalogAmbiguousMatchError):
        catalog.expressions.match_or_create(
            work_id,
            MetadataCandidate({"label": label}),
        )


def test_identifier_repository_uses_normalized_equality(db) -> None:
    catalog = Catalog(db)
    identifier_id = catalog.identifiers.match_or_create(
        IdentifierCandidate("doi", "https://doi.org/10.1000/ABC")
    )

    result = catalog.identifiers.match(IdentifierCandidate("DOI", "doi:10.1000/abc"))

    assert result.entity_id == identifier_id
    assert result.evidence
    assert all(item.decisive for item in result.evidence)


def test_identifier_storage_copies_are_deterministic_but_owners_are_ambiguous(db) -> None:
    catalog = Catalog(db)
    first_work_id = catalog.works.create(
        {"title": f"Copied Identifier One {uuid.uuid4()}"}
    )
    second_work_id = catalog.works.create(
        {"title": f"Copied Identifier Two {uuid.uuid4()}"}
    )
    value = str(uuid.uuid4())
    original_id = catalog.identifiers.match_or_create(IdentifierCandidate("uuid", value))
    first_copy_id = catalog.identifiers.link_to_wemi(
        identifier_id=original_id,
        level="work",
        entity_id=first_work_id,
    )
    second_copy_id = catalog.identifiers.link_to_wemi(
        identifier_id=original_id,
        level="work",
        entity_id=second_work_id,
    )

    identifier_results = catalog.matching.identifiers.candidates(
        IdentifierCandidate("uuid", value)
    )
    work_result = catalog.works.match(
        MetadataCandidate(
            {},
            hints={"identifiers": {"uuid": value}},
        )
    )

    assert tuple(result.entity_id for result in identifier_results) == (
        first_copy_id,
        second_copy_id,
    )
    assert catalog.matching.identifiers.best(
        IdentifierCandidate("uuid", value)
    ).entity_id == first_copy_id
    assert work_result.decision == "ambiguous"
    assert work_result.alternatives == (first_work_id, second_work_id)


def test_descriptive_item_fields_do_not_establish_copy_identity(db) -> None:
    catalog = Catalog(db)
    work_id = catalog.works.create({"title": f"Item Work {uuid.uuid4()}"})
    expression_id = catalog.expressions.match_or_create(
        work_id,
        MetadataCandidate({"label": f"Item Expression {uuid.uuid4()}"}),
    )
    manifestation_id = catalog.manifestations.match_or_create(
        expression_id,
        MetadataCandidate({"edition_statement": f"Edition {uuid.uuid4()}"}),
    )
    catalog.items.create(
        {"manifestation_id": manifestation_id, "location": "shared shelf"}
    )

    result = catalog.items.match(
        manifestation_id,
        MetadataCandidate({"location": "shared shelf"}),
    )

    assert result.decision == "no_match"
