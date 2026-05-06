#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import re
import sys
import tempfile
import sqlite3
import tomllib
from collections import defaultdict
from typing import Iterable

CREATE_TABLE_RE = re.compile(r"CREATE TABLE(?: IF NOT EXISTS)? `([^`]+)`", re.IGNORECASE)
CREATE_VIEW_RE = re.compile(r"CREATE VIEW `([^`]+)`", re.IGNORECASE)
FK_RE = re.compile(
    r"FOREIGN KEY \(`([^`]+)`\)\s+REFERENCES `([^`]+)` \(`([^`]+)`\)",
    re.IGNORECASE | re.MULTILINE,
)


def singularize(name: str) -> str:
    if name.endswith("series"):
        return name
    if name.endswith("ies"):
        return name[:-3] + "y"
    if name.endswith("sses"):
        return name[:-2]
    if name.endswith("s") and not name.endswith("ss"):
        return name[:-1]
    return name


def interlink_table_name(left: str, right: str) -> str:
    a, b = sorted([left, right])
    return f"{singularize(a)}_{singularize(b)}_links"


def intralink_table_name(table: str) -> str:
    s = singularize(table)
    return f"{s}_{s}_intralinks"


def frbr_root(repo_root: pathlib.Path) -> pathlib.Path:
    return repo_root / "src" / "LiuXin_alpha" / "databases" / "database_driver_plugins" / "SQL" / "database_generator_frbr"


def load_tables(root: pathlib.Path):
    table_root = root / "table_sql"
    groups: dict[str, list[str]] = defaultdict(list)
    direct_fks: list[tuple[str, str, str]] = []
    for sql_path in sorted(table_root.rglob("*.sql")):
        rel_group = sql_path.relative_to(table_root).parts[0]
        text = sql_path.read_text(encoding="utf-8")
        tables = CREATE_TABLE_RE.findall(text)
        for table in tables:
            groups[rel_group].append(table)
        for col, ref_table, ref_col in FK_RE.findall(text):
            owner = tables[-1] if len(tables) == 1 else _owner_table_for_fk(text, col, ref_table, ref_col, tables)
            direct_fks.append((owner, ref_table, col))
    for key in groups:
        groups[key] = sorted(groups[key])
    return groups, sorted(direct_fks)


def _owner_table_for_fk(text: str, col: str, ref_table: str, ref_col: str, tables: list[str]) -> str:
    # Cheap but stable fallback for multi-table files: choose the nearest preceding CREATE TABLE.
    fk_pat = re.compile(rf"FOREIGN KEY \(`{re.escape(col)}`\)\s+REFERENCES `({re.escape(ref_table)})` \(`{re.escape(ref_col)}`\)", re.IGNORECASE)
    m = fk_pat.search(text)
    if not m:
        return tables[-1]
    pos = m.start()
    prefixes = [(mt.start(), mt.group(1)) for mt in CREATE_TABLE_RE.finditer(text) if mt.start() < pos]
    return prefixes[-1][1] if prefixes else tables[-1]


def load_views(root: pathlib.Path) -> list[str]:
    text = (root / "aggregate_sql" / "wemi_views.sql").read_text(encoding="utf-8")
    return CREATE_VIEW_RE.findall(text)


def load_interlinks(root: pathlib.Path):
    data = tomllib.loads((root / "interlink_table_requests.toml").read_text(encoding="utf-8"))
    rows = []
    for entry in data.get("interlinks", []):
        left = entry["left_table"]
        right = entry["right_table"]
        rows.append({
            "left": left,
            "right": right,
            "table": interlink_table_name(left, right),
            "type": entry["link_type"],
            "requested_columns": tuple(entry.get("requested_columns", [])),
        })
    return rows


def load_intralinks(root: pathlib.Path):
    data = tomllib.loads((root / "intralink_table_requests.toml").read_text(encoding="utf-8"))
    rows = []
    for entry in data.get("intralinks", []):
        table = entry["table"]
        rows.append({
            "table": intralink_table_name(table),
            "base": table,
            "requested_cols": tuple(entry.get("requested_cols", [])),
        })
    return rows


def try_live_generation(repo_root: pathlib.Path) -> str:
    sys.path.insert(0, str(repo_root / "src"))
    try:
        from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
    except Exception as e:  # pragma: no cover - best-effort note only
        return f"live generation unavailable: import failed: {e!r}"
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            frbr_gen.create_new_database(conn)
        except Exception as e:  # pragma: no cover - best-effort note only
            msg = str(e).strip().replace("\n", " ")
            if "entity_identifier_scratch" in msg:
                msg = "near `entity_identifier_scratch`: syntax error while creating `entity_identifiers`"
            return f"live generation failed: {msg}"
        finally:
            conn.close()
    return "live generation succeeded"


def render_report(repo_root: pathlib.Path, try_live: bool = False) -> str:
    root = frbr_root(repo_root)
    groups, fks = load_tables(root)
    views = load_views(root)
    interlinks = load_interlinks(root)
    intralinks = load_intralinks(root)

    group_order = [
        "frbr_metadata_core_tables",
        "frbr_metadata_core_attributes",
        "metadata_additional",
        "storage_tables",
        "workflow_tables",
        "db_metadata_tables",
        "constants_tables",
    ]
    group_title = {
        "frbr_metadata_core_tables": "core_wemi",
        "frbr_metadata_core_attributes": "core_attributes",
        "metadata_additional": "additional_metadata",
        "storage_tables": "storage",
        "workflow_tables": "workflow",
        "db_metadata_tables": "db_metadata",
        "constants_tables": "constants",
    }

    lines: list[str] = []
    lines.append("# LiuXin current FRBR database (ASCII sketch)")
    lines.append("")
    lines.append("Generated from the FRBR generator source tree, not by hand.")
    lines.append(f"Source root: `{root}`")
    lines.append("")
    lines.append("## quick counts")
    lines.append("")
    total_tables = sum(len(v) for v in groups.values())
    lines.append(f"- tables from `table_sql/`: {total_tables}")
    lines.append(f"- aggregate views: {len(views)}")
    lines.append(f"- generated interlink tables requested in TOML: {len(interlinks)}")
    lines.append(f"- generated intralink tables requested in TOML: {len(intralinks)}")
    if try_live:
        lines.append(f"- live generator check: {try_live_generation(repo_root)}")
    lines.append("")
    lines.append("## sketch")
    lines.append("")
    lines.extend([
        "```text",
        "                               [languages]",
        "                                   ^   ^",
        "                                   |   |",
        "                work_original_language_id   expression_language_id",
        "                                   |   |",
        "[agents] <==> [works] <==> [expressions] <==> [manifestations] ---> [items]",
        "   ||            ||                 ||                 ||               ||",
        "   ||            ||                 ||                 ||               ||",
        "   ||            ||                 ||                 ||               ||",
        "   ||            ||                 ||                 ||               ++--> [images]",
        "   ||            ||                 ||                 ||",
        "   ||            ||                 ||                 ++<==> [agents]",
        "   ||            ||                 ||",
        "   ||            ||                 ++<==> [notes] / [labels] / [languages] / [images]",
        "   ||            ||",
        "   ||            ++<==> [agents] / [comments]",
        "   ||",
        "   ++<==> [works / expressions / manifestations / items] via generated agent_*_links",
        "",
        "[entity_identifiers]  --> polymorphic attachment for work/expression/manifestation/item",
        "[item_identifiers]    --> direct attachment to items",
        "[comments]            --> attached to many entity tables via generated *_comment_links",
        "[notes]               --> attached to selected entity tables via generated *_note_links",
        "[genres/labels/ratings/series/subjects/synopses] --> additional metadata hanging off the graph",
        "",
        "storage / file side:",
        "",
        "[stores] ---> [folders] ---> [asset_replicas] ---> [digital_assets] <--> [digital_asset_derivations]",
        "   ||            ||               ||",
        "   ||            ||               ++--> [backup_presence_links] / [backup_workflow_outputs]",
        "   ||            ||",
        "   ||            ++--> [images]",
        "   ||",
        "   ++<==> [devices] / [comments]",
        "",
        "[items] <==> [digital_assets]                 via digital_asset_item_links",
        "[items] <==> [composite_digital_assets]       via composite_digital_asset_item_links",
        "[composite_digital_assets] <==> [digital_assets] via composite_digital_asset_digital_asset_links",
        "",
        "workflow side:",
        "",
        "[workflow_states] -> [workflow_steps] -> [transform_runs] -> [transform_run_inputs/outputs]",
        "[digital_asset_workflow] -> [digital_asset_workflow_events]",
        "[item_workflow]          -> [item_workflow_events]",
        "```",
        "",
    ])

    lines.append("## tables by group")
    lines.append("")
    for key in group_order:
        vals = groups.get(key, [])
        if not vals:
            continue
        lines.append(f"### {group_title[key]} ({len(vals)})")
        lines.append("")
        for name in vals:
            lines.append(f"- `{name}`")
        lines.append("")

    lines.append("## direct foreign keys declared in main SQL")
    lines.append("")
    for owner, ref, col in fks:
        lines.append(f"- `{owner}` --{col}--> `{ref}`")
    lines.append("")

    lines.append("## generated interlink tables (from TOML)")
    lines.append("")
    for row in sorted(interlinks, key=lambda r: (r['left'], r['right'])):
        cols = ", ".join(row["requested_columns"]) if row["requested_columns"] else "(none)"
        lines.append(
            f"- `{row['table']}` : `{row['left']}` <=> `{row['right']}`  [{row['type']}; extra cols: {cols}]"
        )
    lines.append("")

    lines.append("## generated intralink tables (from TOML)")
    lines.append("")
    for row in intralinks:
        cols = ", ".join(row["requested_cols"]) if row["requested_cols"] else "(none)"
        lines.append(f"- `{row['table']}` : `{row['base']}` <=> `{row['base']}`  [extra cols: {cols}]")
    lines.append("")

    lines.append("## aggregate / compatibility views")
    lines.append("")
    for view in views:
        lines.append(f"- `{view}`")
    lines.append("")

    lines.append("## notes")
    lines.append("")
    lines.append("- This chart is source-derived. It does not require the generator to run successfully.")
    lines.append("- Some metadata families exist as tables now even where their higher-level container APIs are still evolving.")
    lines.append("- The current schema is graph-shaped, not a strict WEMI pyramid: Work<=>Expression and Expression<=>Manifestation are both many-to-many via generated link tables.")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate an ASCII sketch of the current FRBR database schema.")
    parser.add_argument("repo_root", nargs="?", default=".", help="Path to the LiuXin repo root (default: current directory).")
    parser.add_argument("-o", "--output", help="Write the report to this file instead of stdout.")
    parser.add_argument("--try-live", action="store_true", help="Also attempt to build a temporary sqlite database and report success/failure.")
    args = parser.parse_args()

    repo_root = pathlib.Path(args.repo_root).resolve()
    report = render_report(repo_root, try_live=args.try_live)
    if args.output:
        out = pathlib.Path(args.output)
        out.write_text(report, encoding="utf-8")
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
