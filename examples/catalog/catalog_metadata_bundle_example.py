#!/usr/bin/env python3
"""
Attach an Agent, identifier, and note and render an Item-rooted metadata bundle.

Seed a WEMI chain for a demonstration edition, attach related metadata to its Work,
and retrieve both the bundle and display projections from the Item. Print linked
IDs and preferred title information through the shared diagnostic JSON renderer.
Database ownership and retention belong to the common example context.
"""

from __future__ import annotations

import argparse

from _catalog_example_utils import (
    add_database_argument,
    dump_json,
    open_catalog_example,
)

from LiuXin_alpha.catalog.api import IdentifierCandidate, MetadataCandidate


def parse_args() -> argparse.Namespace:
    """
    Parse process arguments for the metadata bundle demonstration. The shared --database option
    yields a Path when supplied and None otherwise. Directory expansion, refusal of an existing
    retained path, temporary allocation, and template handling occur only when open_catalog_example
    is entered.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Namespace with the optional database path; help or invalid syntax raises SystemExit.
    """

    parser = argparse.ArgumentParser(
        description="Catalog Agents, identifiers, notes, bundles, and projections"
    )
    add_database_argument(parser)
    return parser.parse_args()


def main() -> int:
    """
    Build a WEMI chain and report related Work metadata through Item-rooted retrieval. Create a
    Work, match or create an English Expression, EPUB-labelled Manifestation, and Item carrying a
    demonstration location. Match/create Virginia Woolf as an Agent and link her to the Work as
    author with priority one. Add a UUID identifier link at priority zero and a Work note, then
    retrieve the Item bundle, display title, Item summary, and preferred Work title. Report bundle
    Agent IDs and the returned identifier-link/note IDs before context cleanup. The location is
    metadata; no ebook contents are opened or verified.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero after reporting and context cleanup; uncaught parsing, catalogue, rendering, or cleanup failures propagate.
    """

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
