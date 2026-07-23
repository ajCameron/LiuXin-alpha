#!/usr/bin/env python3
"""Create, read, update, traverse, and delete catalog WEMI records."""

from __future__ import annotations

import argparse

from _catalog_example_utils import add_database_argument, open_catalog_example
from _example_utils import dump_json

from LiuXin_alpha.catalog.api import MetadataCandidate


def parse_args() -> argparse.Namespace:
    """Return command-line arguments for the CRUD example."""

    parser = argparse.ArgumentParser(
        description="Catalog repository and WEMI traversal example"
    )
    add_database_argument(parser)
    return parser.parse_args()


def main() -> int:
    """Run a complete repository-backed WEMI round trip."""

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
