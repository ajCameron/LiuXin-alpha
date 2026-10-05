#!/usr/bin/env python3
"""
Demonstrate repository CRUD and traversal across a Work/Expression/Manifestation/Item chain.

Create a Work, update its canonical title, match or create its descendants, and
delete a separate disposable Work. Print IDs, title lookup, relationship lists,
and the deletion observation. The shared context chooses temporary or retained
catalogue storage and owns cleanup; no ebook payload is read or written here.
"""

from __future__ import annotations

import argparse

from _catalog_example_utils import (
    add_database_argument,
    dump_json,
    open_catalog_example,
)

from LiuXin_alpha.catalog.api import MetadataCandidate


def parse_args() -> argparse.Namespace:
    """
    Parse process arguments for the repository CRUD demonstration. The shared --database option
    yields a Path when supplied and None otherwise. Directory expansion, refusal of an existing
    retained path, temporary allocation, and template handling occur only when open_catalog_example
    is entered.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Namespace with the optional database path; help or invalid syntax raises SystemExit.
    """

    parser = argparse.ArgumentParser(
        description="Catalog repository and WEMI traversal example"
    )
    add_database_argument(parser)
    return parser.parse_args()


def main() -> int:
    """
    Create a Frankenstein WEMI chain and print repository reads and traversal results. In the shared
    catalogue context, create the Work with its original title/year, update canonical_title, and
    match or create an English Expression, digital Manifestation, and Item with a demonstration
    location string. Create/delete a second Work. Report the required Work, whitespace/case-varied
    original-title lookup, both Work/Expression traversal directions, descendant lists, and whether
    the disposable Work is absent. Print before leaving the context. These observations are not
    assertions and do not independently change the zero return status.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero after reporting and context cleanup; uncaught parsing, catalogue, rendering, or cleanup failures propagate.
    """

    args = parse_args()
    with open_catalog_example(args.database) as session:
        catalog = session.catalog

        work_id = catalog.works.create(
            {
                "title": "Frankenstein; or, The Modern Prometheus",
                "original_year": 1818,
            }
        )
        catalog.works.update(
            work_id,
            {"canonical_title": "Frankenstein"},
        )

        expression_id = catalog.expressions.match_or_create(
            work_id,
            MetadataCandidate(
                {
                    "label": "English text",
                    "year": 1818,
                },
                source="catalog CRUD example",
            ),
        )
        manifestation_id = catalog.manifestations.match_or_create(
            expression_id,
            MetadataCandidate(
                {
                    "subtitle": "Example digital edition",
                    "carrier_type": "online resource",
                }
            ),
        )
        item_id = catalog.items.match_or_create(
            manifestation_id,
            MetadataCandidate(
                {
                    "inventory_code": "DEMO-0001",
                    "location": "examples://frankenstein.epub",
                }
            ),
        )

        disposable_id = catalog.works.create({"title": "Delete me"})
        catalog.works.delete(disposable_id)

        payload = {
            "database_path": str(session.database_path),
            "database_retained": session.database_retained,
            "ids": {
                "work": work_id,
                "expression": expression_id,
                "manifestation": manifestation_id,
                "item": item_id,
            },
            "work": catalog.works.require(work_id),
            "case_insensitive_title_lookup": catalog.works.find_by_title(
                "  FRANKENSTEIN; OR, THE MODERN PROMETHEUS  "
            ),
            "expressions_for_work": catalog.expressions.list_for_work(work_id),
            "works_for_expression": catalog.expressions.list_works(expression_id),
            "manifestations_for_expression": (
                catalog.manifestations.list_for_expression(expression_id)
            ),
            "items_for_manifestation": (
                catalog.items.list_for_manifestation(manifestation_id)
            ),
            "deleted_work_is_absent": catalog.works.get(disposable_id) is None,
        }
        print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
