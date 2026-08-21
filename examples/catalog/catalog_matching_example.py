#!/usr/bin/env python3
"""Match Works, Agents, identifiers, and scoped WEMI candidates."""

from __future__ import annotations

import argparse

from _catalog_example_utils import (
    add_database_argument,
    dump_json,
    open_catalog_example,
)

from LiuXin_alpha.catalog.api import (
    CatalogAmbiguousMatchError,
    IdentifierCandidate,
    MatchResult,
    MetadataCandidate,
)


def parse_args() -> argparse.Namespace:
    """Return command-line arguments for the matching example."""

    parser = argparse.ArgumentParser(description="Catalog deterministic matching example")
    add_database_argument(parser)
    return parser.parse_args()


def compact_match(result: MatchResult | None) -> dict[str, object] | None:
    """Return the explanatory fields from one match result."""

    if result is None:
        return None
    return {
        "entity_id": result.entity_id,
        "decision": result.decision,
        "confidence": result.confidence,
        "reason": result.reason,
        "matched_on": result.matched_on,
        "alternatives": result.alternatives,
        "evidence": [
            {
                "field": item.field,
                "kind": item.kind,
                "score": item.score,
                "decisive": item.decisive,
            }
            for item in result.evidence
        ],
        "is_match": result.is_match,
    }


def main() -> int:
    """Run exact, candidate, scoped, and match-or-create examples."""

    args = parse_args()
    with open_catalog_example(args.database) as session:
        catalog = session.catalog

        work_id = catalog.works.create(
            {
                "title": "The Left Hand of Darkness",
                "original_year": 1969,
            }
        )
        work_candidate = MetadataCandidate(
            {
                "title": "The Left Hand of Darkness",
                "original_year": 1969,
            },
            source="imported metadata",
        )

        expression_candidate = MetadataCandidate(
            {
                "label": "English text",
                "year": 1969,
            }
        )
        expression_id = catalog.expressions.match_or_create(
            work_id,
            expression_candidate,
        )
        repeated_expression_id = catalog.expressions.match_or_create(
            work_id,
            expression_candidate,
        )

        agent_candidate = MetadataCandidate({"name": "Ursula K. Le Guin"})
        agent_id = catalog.agents.match_or_create(agent_candidate)
        identifier_candidate = IdentifierCandidate(
            "uuid",
            "0b2915c3-09a9-47b0-b36e-8aa02e84af5e",
        )
        identifier_id = catalog.identifiers.match_or_create(identifier_candidate)
        catalog.identifiers.link_to_wemi(
            identifier_id=identifier_id,
            level="work",
            entity_id=work_id,
        )
        identifier_backed_work = catalog.works.match(
            MetadataCandidate(
                {"title": "The Left Hand of Darkness"},
                hints={
                    "identifiers": {
                        identifier_candidate.identifier_type: (
                            identifier_candidate.value
                        )
                    }
                },
            )
        )

        tag_id = catalog.tags.create({"name": "Speculative Fiction"})
        near_tag = MetadataCandidate({"name": "Speculative Fictio"})
        exact_tag = catalog.tags.match(
            MetadataCandidate({"name": "SPECULATIVE FICTION"})
        )
        default_near_tag = catalog.tags.match(near_tag)
        policy_near_tag = catalog.tags.match(near_tag, use_policy=True)

        item_id = catalog.items.create({"type": "digital"})
        observed_identifier = IdentifierCandidate(
            "uuid",
            "7b7ca2c1-607e-4ab2-939f-1c4d06106915",
            source="device scan",
        )
        item_identifier_id = catalog.item_identifiers.match_or_create(
            item_id,
            observed_identifier,
        )

        create_candidate = MetadataCandidate({"title": "The Dispossessed"})
        first_created_id = catalog.works.match_or_create(create_candidate)
        second_created_id = catalog.works.match_or_create(create_candidate)

        ambiguous_title = "A Deliberately Duplicate Work"
        catalog.works.create({"title": ambiguous_title})
        catalog.works.create({"title": ambiguous_title})
        ambiguous_result = catalog.works.match(
            MetadataCandidate({"title": ambiguous_title})
        )
        try:
            catalog.works.match_or_create(
                MetadataCandidate({"title": ambiguous_title})
            )
        except CatalogAmbiguousMatchError as error:
            blocked_decision = error.result.decision
        else:  # pragma: no cover - the example treats this as an invariant
            blocked_decision = "not_blocked"

        payload = {
            "database_path": str(session.database_path),
            "database_retained": session.database_retained,
            "work_exact": compact_match(
                catalog.matching.works.exact("  THE LEFT HAND OF DARKNESS ")
            ),
            "work_best": compact_match(
                catalog.matching.works.best(work_candidate)
            ),
            "work_candidates": [
                compact_match(result)
                for result in catalog.matching.works.candidates(work_candidate)
            ],
            "work_non_match": compact_match(
                catalog.matching.works.best(
                    MetadataCandidate({"title": "An unrelated title"})
                )
            ),
            "agent_exact": compact_match(
                catalog.matching.agents.exact("ursula k. le guin")
            ),
            "agent_best": compact_match(
                catalog.matching.agents.best(agent_candidate)
            ),
            "identifier_exact": compact_match(
                catalog.matching.identifiers.exact(
                    identifier_candidate.value,
                    identifier_candidate.identifier_type,
                )
            ),
            "identifier_backed_work": compact_match(identifier_backed_work),
            "tag_exact_default": compact_match(exact_tag),
            "tag_near_default": compact_match(default_near_tag),
            "tag_near_policy_opt_in": compact_match(policy_near_tag),
            "item_identifier_exact": compact_match(
                catalog.matching.item_identifiers.best(
                    observed_identifier,
                    item_id=item_id,
                )
            ),
            "ambiguous_work": compact_match(ambiguous_result),
            "match_or_create_blocked_decision": blocked_decision,
            "expression_scoped_match": compact_match(
                catalog.expressions.match(work_id, expression_candidate)
            ),
            "match_or_create_reused_expression": (
                expression_id == repeated_expression_id
            ),
            "match_or_create_reused_work": first_created_id == second_created_id,
            "known_ids": {
                "work": work_id,
                "agent": agent_id,
                "identifier": identifier_id,
                "item": item_id,
                "item_identifier": item_identifier_id,
                "tag": tag_id,
            },
        }
        print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
