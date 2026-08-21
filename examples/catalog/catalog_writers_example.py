#!/usr/bin/env python3
"""Use catalog writer factories, link views, extras, and type guards."""

from __future__ import annotations

import argparse

from _catalog_example_utils import (
    add_database_argument,
    dump_json,
    open_catalog_example,
)

from LiuXin_alpha.databases.macro_types import LinkValue


def parse_args() -> argparse.Namespace:
    """Return command-line arguments for the writer example."""

    parser = argparse.ArgumentParser(
        description="Catalog schema writer and inspectable link-update example"
    )
    add_database_argument(parser)
    return parser.parse_args()


def _extra_column_ending(writer: object, ending: str) -> str:
    """Return the schema-discovered link column with one semantic suffix."""

    link_spec = writer.link_spec
    return next(
        column.name
        for column in link_spec.extra_link_columns
        if column.name.endswith(ending)
    )


def main() -> int:
    """Run column, relationship, lazy-link, and guarded writer examples."""

    args = parse_args()
    with open_catalog_example(args.database) as session:
        catalog = session.catalog

        work_id = catalog.works.create({"title": "Writer example"})
        column_writer = catalog.create_writer("works", "work_sort_title")
        catalog.write_one(
            "works",
            "work_sort_title",
            work_id,
            "Writer example, The",
        )

        expression_id = catalog.expressions.create(
            {"label": "Expression linked by a normalized update"}
        )
        expression_writer = catalog.create_writer(
            "works",
            "expression_label",
        )
        origin_column = _extra_column_ending(expression_writer, "_origin")
        primary_column = _extra_column_ending(expression_writer, "_primary")
        update = expression_writer.build_update(
            {
                work_id: LinkValue(
                    expression_id,
                    priority=1,
                    extra={
                        origin_column: "catalog writer example",
                        primary_column: 1,
                    },
                )
            }
        )

        link = update.links(
            dst_value_for=lambda dst_id: catalog.expressions.require(dst_id)[
                "expression_label"
            ]
        )[0]
        link_before_loading = link.to_dict()
        extras_through_mapping = dict(link)
        origin_through_mapping = link[origin_column]
        destination_value = link.get_dst_value()
        link_after_loading = link.to_dict()
        written_expression_links = expression_writer.apply_update(update)

        agent_writer = catalog.create_writer(
            "works",
            "agent_canonical_name",
        )
        written_agent_links = catalog.write_one(
            "works",
            "agent_canonical_name",
            work_id,
            "Mary Shelley",
            link_type="aut",
        )

        rejected_agent = "Rejected writer-created Agent"
        rejected_error = None
        try:
            catalog.write_one(
                "works",
                "agent_canonical_name",
                work_id,
                rejected_agent,
                link_type="not-a-marc-relator-code",
            )
        except ValueError as exc:
            rejected_error = str(exc)

        payload = {
            "database_path": str(session.database_path),
            "database_retained": session.database_retained,
            "writer_types": {
                "same_table": type(column_writer).__name__,
                "work_to_expression": type(expression_writer).__name__,
                "work_to_agent": type(agent_writer).__name__,
            },
            "same_table_value": catalog.works.require(work_id)["work_sort_title"],
            "update_primary_ids": update.primary_ids,
            "update_entry_operations": update[work_id].operation_names,
            "link_view": {
                "before_lazy_load": link_before_loading,
                "destination_value": destination_value,
                "after_lazy_load": link_after_loading,
                "extras_via_dict_interface": extras_through_mapping,
                "origin_via_mapping_lookup": origin_through_mapping,
            },
            "written_expression_links": written_expression_links,
            "written_agent_links": written_agent_links,
            "allowed_agent_roles_sample": (
                agent_writer.link_spec.allowed_types[:5]
                or catalog.db.driver_wrapper.get_allowed_link_types(
                    agent_writer.link_spec
                )[:5]
            ),
            "invalid_role_guard": {
                "error": rejected_error,
                "destination_was_not_created": (
                    catalog.agents.resolve(name=rejected_agent) is None
                ),
            },
        }
        print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
