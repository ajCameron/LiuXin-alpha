#!/usr/bin/env python3
"""Attach related metadata and retrieve a coherent catalog bundle."""

from __future__ import annotations

import argparse

from _catalog_example_utils import add_database_argument, open_catalog_example
from _example_utils import dump_json

from LiuXin_alpha.catalog.api import IdentifierCandidate, MetadataCandidate


def parse_args() -> argparse.Namespace:
    """Return command-line arguments for the bundle example."""

    parser = argparse.ArgumentParser(
        description="Catalog Agents, identifiers, notes, bundles, and projections"
    )
    add_database_argument(parser)
    return parser.parse_args()


def main() -> int:
    """Attach related records and render an Item-rooted WEMI bundle."""

    args = parse_args()
    with open_catalog_example(args.database) as session:
        catalog = session.catalog

        work_id = catalog.works.create({"title": "A Room of One's Own"})
        expression_id = catalog.expressions.match_or_create(
            work_id,
            MetadataCandidate({"label": "English text"}),
        )
        manifestation_id = catalog.manifestations.match_or_create(
            expression_id,
            MetadataCandidate({"subtitle": "Example EPUB edition"}),
        )
        item_id = catalog.items.match_or_create(
            manifestation_id,
            MetadataCandidate(
                {
                    "location": "examples://a-room-of-ones-own.epub",
                    "lifecycle_status": "available",
                }
            ),
        )

        agent_id = catalog.agents.match_or_create(
            MetadataCandidate(
                {
                    "name": "Virginia Woolf",
                    "type": "person",
                }
            )
        )
        catalog.agents.link_to_wemi(
            agent_id=agent_id,
            level="work",
            entity_id=work_id,
            role="aut",
            priority=1,
        )

        identifier_id = catalog.identifiers.match_or_create(
            IdentifierCandidate(
                "uuid",
                "21cb6063-a9c9-4bc0-a217-2eedc46a2231",
                source="catalog bundle example",
            )
        )
        assigned_identifier_id = catalog.identifiers.link_to_wemi(
            identifier_id=identifier_id,
            level="work",
            entity_id=work_id,
            priority=0,
        )
        note_id = catalog.notes.add_for_wemi(
            level="work",
            entity_id=work_id,
            data={"text": "A note attached through the catalog repository."},
        )

        bundle = catalog.retrieval.bundles.for_item(item_id)
        payload = {
            "database_path": str(session.database_path),
            "database_retained": session.database_retained,
            "bundle": bundle,
            "display_title": catalog.retrieval.projections.display_title(
                level="item",
                entity_id=item_id,
            ),
            "item_summary": catalog.retrieval.projections.item_summary(item_id),
            "preferred_work_title": catalog.titles.preferred_for_wemi(
                level="work",
                entity_id=work_id,
            ),
            "linked_agent_ids": [row["agent_id"] for row in bundle.agents],
            "linked_identifier_id": assigned_identifier_id,
            "linked_note_id": note_id,
        }
        print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
