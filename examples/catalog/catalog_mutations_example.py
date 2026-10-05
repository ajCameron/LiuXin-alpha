#!/usr/bin/env python3
"""
Demonstrate coordinated attachments, rejected writes, and a Work merge.

Attach fields and related metadata, attempt an invalid Agent-role attachment,
and merge a second Work into a target with existing canonical metadata. Print
selected rollback and transfer observations. A caught rejection is part of the
report, and false observation flags do not themselves cause a nonzero exit.
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
    Parse process arguments for the coordinated mutations demonstration. The shared --database
    option yields a Path when supplied and None otherwise. Directory expansion, refusal of an
    existing retained path, temporary allocation, and template handling occur only when
    open_catalog_example is entered.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Namespace with the optional database path; help or invalid syntax raises SystemExit.
    """

    parser = argparse.ArgumentParser(
        description="Catalog coordinated metadata mutation example"
    )
    add_database_argument(parser)
    return parser.parse_args()


def main() -> int:
    """
    Report a coordinated metadata attachment, rejected update, and entity merge. Attach Work fields,
    title, author, identifier, and note, then retrieve its bundle. Save the Work title before
    attempting an attachment with a replacement title and invalid Agent role. Catch any Exception
    from that attempt and record its type/message; the catch does not prove that role validation was
    the cause. Compare the subsequent title and Agent lookup to expose the selected rollback
    observations.

    Create source/target Works, link an Expression and note to the source, then merge into the
    target. Report source absence, target fields, Expression transfer, and whether the target has
    any notes. These are selected observations, not exhaustive merge/transaction assertions. Print
    within the shared context; unexpected errors outside the rejection block propagate, and report
    flags do not alter the zero return status.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero after reporting and context cleanup; uncaught parsing, catalogue, rendering, or cleanup failures propagate.
    """

    args = parse_args()
    with open_catalog_example(args.database) as session:
        catalog = session.catalog

        work_id = catalog.works.create({"title": "Working title"})
        catalog.mutations.writer.attach_metadata(
            level="work",
            entity_id=work_id,
            data={
                "fields": {
                    "canonical_title": "Orlando: A Biography",
                    "original_year": 1928,
                },
                "title": "Orlando",
                "agents": [
                    {
                        "name": "Virginia Woolf",
                        "role": "aut",
                        "priority": 1,
                    }
                ],
                "identifiers": [
                    {
                        "scheme": "uuid",
                        "value": "dd4a8738-9781-422c-b1ab-c7f24d11d2ab",
                        "source": "catalog mutation example",
                        "priority": 0,
                    }
                ],
                "notes": ["Attached with the rest of the metadata."],
            },
        )
        attached_bundle = catalog.retrieval.bundles.for_work(work_id)

        title_before_rejected_write = catalog.works.require(work_id)["work_title"]
        rejected_agent_name = "Agent who must be rolled back"
        rejected_error = None
        try:
            catalog.mutations.writer.attach_metadata(
                level="work",
                entity_id=work_id,
                data={
                    "fields": {"title": "This title must not persist"},
                    "agents": [
                        {
                            "name": rejected_agent_name,
                            "role": "not-a-marc-relator-code",
                        }
                    ],
                },
            )
        except Exception as exc:  # Expected database-backed role validation.
            rejected_error = f"{type(exc).__name__}: {exc}"

        source_id = catalog.works.create({"title": "Merged source title"})
        target_id = catalog.works.create(
            {"canonical_title": "Existing target canonical title"}
        )
        expression_id = catalog.expressions.match_or_create(
            source_id,
            MetadataCandidate({"label": "Expression transferred by merge"}),
        )
        catalog.notes.add_for_wemi(
            level="work",
            entity_id=source_id,
            data={"text": "This note follows the merge."},
        )
        catalog.mutations.writer.merge_entities(
            level="work",
            source_id=source_id,
            target_id=target_id,
        )

        payload = {
            "database_path": str(session.database_path),
            "database_retained": session.database_retained,
            "attached": {
                "work": attached_bundle.work,
                "agents": attached_bundle.agents,
                "identifiers": attached_bundle.identifiers,
                "notes": attached_bundle.notes,
            },
            "rejected_attachment": {
                "error": rejected_error,
                "title_rolled_back": (
                    catalog.works.require(work_id)["work_title"]
                    == title_before_rejected_write
                ),
                "agent_rolled_back": (
                    catalog.agents.resolve(name=rejected_agent_name) is None
                ),
            },
            "merge": {
                "source_was_deleted": catalog.works.get(source_id) is None,
                "target": catalog.works.require(target_id),
                "expression_was_transferred": expression_id
                in {
                    row["expression_id"]
                    for row in catalog.expressions.list_for_work(target_id)
                },
                "notes_were_transferred": bool(
                    catalog.notes.list_for_wemi(
                        level="work",
                        entity_id=target_id,
                    )
                ),
            },
        }
        print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
