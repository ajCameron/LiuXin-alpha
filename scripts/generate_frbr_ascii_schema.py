#!/usr/bin/env python3
"""
Report the FRBR catalogue schema from the generator's own sources.

The report lists every table in ``table_sql/`` by group, the foreign keys those
files declare, the link tables the interlink and intralink TOML files request,
and the aggregate views. Link-table names are derived with the generator's own
singular forms, so they match a generated catalogue. A hand-drawn overview
sketch heads the report; only the lists below it are generated.

Usage::

    PYTHONPATH=src python3 scripts/generate_frbr_ascii_schema.py \\
        -o dev-docs/databases/frbr-schema-sketch.md

Salvaged from the 2026-05-06 commit ``1c2be689`` on ``metadata-non-wemi-containers``.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
GENERATOR = Path(
    "src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr"
)
CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE(?: IF NOT EXISTS)? `([^`]+)`", re.IGNORECASE
)
CREATE_VIEW_RE = re.compile(r"CREATE VIEW `([^`]+)`", re.IGNORECASE)
FK_RE = re.compile(
    r"FOREIGN KEY \(`([^`]+)`\)\s+REFERENCES `([^`]+)` \(`([^`]+)`\)",
    re.IGNORECASE | re.MULTILINE,
)
GROUPS = (
    ("frbr_metadata_core_tables", "core WEMI"),
    ("frbr_metadata_core_attributes", "core attributes"),
    ("metadata_additional", "additional metadata"),
    ("storage_tables", "storage"),
    ("workflow_tables", "workflow"),
    ("db_metadata_tables", "database metadata"),
    ("constants_tables", "constants"),
)
SKETCH = """\
                               [languages]
                                   ^   ^
                                   |   |
                work_original_language_id   expression_language_id
                                   |   |
[agents] <==> [works] <==> [expressions] <==> [manifestations] ---> [items]
   ||            ||                 ||                 ||               ||
   ||            ||                 ||                 ||               ++--> [images]
   ||            ||                 ||                 ||
   ||            ||                 ||                 ++<==> [agents]
   ||            ||                 ||
   ||            ||                 ++<==> [notes] / [labels] / [languages] / [images]
   ||            ||
   ||            ++<==> [agents] / [comments]
   ||
   ++<==> [works / expressions / manifestations / items] via generated agent_*_links

[entity_identifiers]  --> polymorphic attachment for work/expression/manifestation/item
[item_identifiers]    --> direct attachment to items
[comments]            --> attached to many entity tables via generated *_comment_links
[notes]               --> attached to selected entity tables via generated *_note_links
[genres/labels/ratings/series/subjects/synopses] --> further metadata on the graph

storage / file side:

[stores] ---> [folders] ---> [asset_replicas] ---> [digital_assets] <--> [digital_asset_derivations]
   ||            ||               ||
   ||            ||               ++--> [backup_presence_links] / [backup_workflow_outputs]
   ||            ||
   ||            ++--> [images]
   ||
   ++<==> [devices] / [comments]

[items] <==> [digital_assets]                     via digital_asset_item_links
[items] <==> [composite_digital_assets]           via composite_digital_asset_item_links
[composite_digital_assets] <==> [digital_assets]  via composite_digital_asset_digital_asset_links

workflow side:

[workflow_states] -> [workflow_steps] -> [transform_runs] -> [transform_run_inputs/outputs]
[digital_asset_workflow] -> [digital_asset_workflow_events]
[item_workflow]          -> [item_workflow_events]"""


def singular(table: str) -> str:
    """
    Return a table name's singular form, as the generator derives it.

    Example:
        >>> singular("synopses")
        'synopsis'


    :param table: Plural table name.
    :return: The generator's singular form.
    """
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))
    from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper

    return str(plural_singular_mapper(table))


def interlink_table_name(left: str, right: str) -> str:
    """
    Name the link table between two tables, as the generator does.

    Example:
        >>> interlink_table_name("works", "tags")
        'tag_work_links'


    :param left: One endpoint table.
    :param right: The other endpoint table.
    :return: Sorted singular forms joined into ``<a>_<b>_links``.
    """
    first, second = sorted((left, right))
    return f"{singular(first)}_{singular(second)}_links"


def intralink_table_name(table: str) -> str:
    """
    Name a table's self-link table, as the generator does.

    Example:
        >>> intralink_table_name("works")
        'work_work_intralinks'


    :param table: Table linked to itself.
    :return: ``<singular>_<singular>_intralinks``.
    """
    name = singular(table)
    return f"{name}_{name}_intralinks"


def _owner_table(text: str, position: int, tables: list[str]) -> str:
    """
    Find the table a foreign key belongs to: the nearest earlier CREATE TABLE.

    Example:
        >>> _owner_table("CREATE TABLE `a` (...); CREATE TABLE `b` (... FOREIGN KEY", 40, ["a", "b"])
        'b'


    :param text: SQL file contents.
    :param position: Offset of the foreign-key clause.
    :param tables: Tables the file creates, in order.
    :return: The owning table's name.
    """
    earlier = [
        m.group(1) for m in CREATE_TABLE_RE.finditer(text) if m.start() < position
    ]
    return earlier[-1] if earlier else tables[-1]


def load_tables(root: Path) -> tuple[dict[str, list[str]], list[tuple[str, str, str]]]:
    """
    Read the tables and declared foreign keys under ``table_sql/``.

    Example:
        >>> groups, keys = load_tables(ROOT / GENERATOR)  # doctest: +SKIP


    :param root: Generator source directory.
    :return: Sorted table names by group, and ``(owner, referenced table, column)``
        for each foreign key.
    """
    table_root = root / "table_sql"
    groups: dict[str, list[str]] = defaultdict(list)
    keys: list[tuple[str, str, str]] = []
    for path in sorted(table_root.rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        tables = CREATE_TABLE_RE.findall(text)
        groups[path.relative_to(table_root).parts[0]].extend(tables)
        for match in FK_RE.finditer(text):
            column, referenced, _ = match.groups()
            keys.append((_owner_table(text, match.start(), tables), referenced, column))
    return {group: sorted(names) for group, names in groups.items()}, sorted(keys)


def load_views(root: Path) -> list[str]:
    """
    Read the aggregate view names.

    Example:
        >>> "works" in " ".join(load_views(ROOT / GENERATOR))  # doctest: +SKIP
        True


    :param root: Generator source directory.
    :return: View names in file order.
    """
    text = (root / "aggregate_sql" / "wemi_views.sql").read_text(encoding="utf-8")
    return CREATE_VIEW_RE.findall(text)


def load_interlinks(root: Path) -> list[dict[str, Any]]:
    """
    Read the requested interlink tables.

    Example:
        >>> load_interlinks(ROOT / GENERATOR)[0]["table"]  # doctest: +SKIP
        'agent_expression_links'


    :param root: Generator source directory.
    :return: One mapping per link with its endpoints, table name, link type and
        extra columns.
    """
    data = tomllib.loads(
        (root / "interlink_table_requests.toml").read_text(encoding="utf-8")
    )
    return [
        {
            "left": entry["left_table"],
            "right": entry["right_table"],
            "table": interlink_table_name(entry["left_table"], entry["right_table"]),
            "type": entry["link_type"],
            "columns": tuple(entry.get("requested_columns", ())),
        }
        for entry in data.get("interlinks", [])
    ]


def load_intralinks(root: Path) -> list[dict[str, Any]]:
    """
    Read the requested intralink tables.

    Example:
        >>> load_intralinks(ROOT / GENERATOR)[0]["table"]  # doctest: +SKIP
        'compressed_file_compressed_file_intralinks'


    :param root: Generator source directory.
    :return: One mapping per link with its table name, base table and extra columns.
    """
    data = tomllib.loads(
        (root / "intralink_table_requests.toml").read_text(encoding="utf-8")
    )
    return [
        {
            "table": intralink_table_name(entry["table"]),
            "base": entry["table"],
            "columns": tuple(entry.get("requested_cols", ())),
        }
        for entry in data.get("intralinks", [])
    ]


def render_report(repo_root: Path = ROOT) -> str:
    """
    Render the schema report as Markdown.

    Example:
        >>> render_report().startswith("# FRBR catalogue schema")  # doctest: +SKIP
        True


    :param repo_root: Repository root holding the generator sources.
    :return: The report text.
    """
    root = repo_root / GENERATOR
    groups, keys = load_tables(root)
    views = load_views(root)
    interlinks = sorted(
        load_interlinks(root), key=lambda row: (row["left"], row["right"])
    )
    intralinks = load_intralinks(root)
    lines = [
        "# FRBR catalogue schema sketch",
        "",
        "Generated by `scripts/generate_frbr_ascii_schema.py` from the generator's sources in",
        f"`{GENERATOR.as_posix()}`.",
        "The overview sketch is drawn by hand in that script; the lists below it are",
        "generated. Re-run the script after a schema change:",
        "",
        "```bash",
        "PYTHONPATH=src python3 scripts/generate_frbr_ascii_schema.py -o dev-docs/databases/frbr-schema-sketch.md",
        "```",
        "",
        "## Counts",
        "",
        f"- Tables in `table_sql/`: {sum(len(names) for names in groups.values())}",
        f"- Interlink tables requested: {len(interlinks)}",
        f"- Intralink tables requested: {len(intralinks)}",
        f"- Aggregate views: {len(views)}",
        "",
        "Typed links also get a `<link table>__types` vocabulary table, and a catalogue",
        "has helper tables (the change log, maintenance, preferences) not listed here.",
        "",
        "## Overview",
        "",
        "```text",
        SKETCH,
        "```",
        "",
        "Work <=> Expression and Expression <=> Manifestation are both many-to-many",
        "through generated link tables, so the schema is a graph, not a strict pyramid.",
        "",
        "## Tables by group",
        "",
    ]
    for group, title in GROUPS:
        names = groups.get(group, [])
        if names:
            lines += [f"### {title.capitalize()} ({len(names)})", ""]
            lines += [f"- `{name}`" for name in names] + [""]
    others = sorted(set(groups) - {group for group, _ in GROUPS})
    for group in others:
        lines += [f"### {group} ({len(groups[group])})", ""]
        lines += [f"- `{name}`" for name in groups[group]] + [""]
    lines += ["## Foreign keys declared in `table_sql/`", ""]
    lines += [
        f"- `{owner}`.`{column}` → `{referenced}`" for owner, referenced, column in keys
    ]
    lines += ["", "## Interlink tables", ""]
    for row in interlinks:
        extra = ", ".join(row["columns"]) or "none"
        lines.append(
            f"- `{row['table']}`: `{row['left']}` <=> `{row['right']}` "
            f"({row['type']}; extra columns: {extra})"
        )
    lines += ["", "## Intralink tables", ""]
    for row in intralinks:
        extra = ", ".join(row["columns"]) or "none"
        lines.append(
            f"- `{row['table']}`: `{row['base']}` <=> `{row['base']}` (extra columns: {extra})"
        )
    lines += ["", "## Aggregate views", ""]
    lines += [f"- `{view}`" for view in views]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """
    Print the report, or write it to a file.

    Example:
        >>> main(["-o", "/tmp/sketch.md"])  # doctest: +SKIP
        0


    :param argv: Command-line arguments; None reads ``sys.argv``.
    :return: Process exit status.
    """
    parser = argparse.ArgumentParser(
        description="Report the FRBR catalogue schema from the generator's own sources."
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="Write the report to this file."
    )
    args = parser.parse_args(argv)
    report = render_report()
    if args.output:
        args.output.write_text(report, encoding="utf-8")
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
