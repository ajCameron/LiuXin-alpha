"""`off` command group for detaching metadata from existing rows."""

from __future__ import annotations

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.commands.on import (
    _parse_on_options_and_value_tokens,
    _parse_tag_values,
    _parse_target_rows,
    _resolve_source_row,
)


def _unlink_one_value(
    browser,
    *,
    target_table: str,
    target_row,
    target_id: int,
    source_table: str,
    source_row,
    kind_label: str,
) -> list[dict[str, object]]:
    source_id_column = browser.db.driver_wrapper.get_id_column(source_table)
    source_id = source_row[source_id_column]

    link_table = browser.db.driver_wrapper.get_link_table_name(source_table, target_table)
    if not link_table:
        raise ValueError(
            "No link table exists between {} and {} for `{}`.".format(
                source_table,
                target_table,
                kind_label,
            )
        )

    existing_links = browser.db.get_interlink_row(primary_row=source_row, secondary_row=target_row, onelink=False)
    if not existing_links:
        browser.emit(
            "{} not linked: {}={} -> {}:{}".format(
                kind_label.capitalize(),
                source_id_column,
                source_id,
                target_table,
                target_id,
            )
        )
        return []

    rows = existing_links if isinstance(existing_links, list) else [existing_links]
    deleted_snapshots: list[dict[str, object]] = []
    for link_row in rows:
        deleted_snapshots.append(dict(link_row.row_dict))
        browser.db.delete(link_row)

    browser.emit(
        "{} unlinked: {}={} -> {}:{} ({} row{})".format(
            kind_label.capitalize(),
            source_id_column,
            source_id,
            target_table,
            target_id,
            len(rows),
            "" if len(rows) == 1 else "s",
        )
    )
    return deleted_snapshots


def _restore_deleted_link_snapshots(browser, snapshots: list[dict[str, object]]) -> list[str]:
    errors: list[str] = []
    for snapshot in reversed(snapshots):
        row_dict = dict(snapshot)
        row_dict.pop("table", None)
        try:
            table = browser.db.driver_wrapper.identify_table_from_row_dict(row_dict)
        except Exception as exc:
            errors.append("restore failed: could not identify table ({})".format(exc))
            continue
        id_column = browser.db.driver_wrapper.get_id_column(table)
        row_dict.pop(id_column, None)
        try:
            Row.from_idless_row_dict(browser.db, row_dict=row_dict, table=table)
        except Exception as exc:
            errors.append("restore failed for table {} ({})".format(table, exc))
    return errors


class _OffBaseCommand(TerminalCommandAPI):
    """Common execution logic for `off <kind> ...` subcommands."""

    group = "off"
    expose_direct = False
    kind = ""
    usage = ""

    def execute(self, browser, args: list[str]) -> bool:
        target_table, target_rows, consumed = _parse_target_rows(browser, args, usage=self.usage)
        best_effort, value_tokens = _parse_on_options_and_value_tokens(args[consumed:])

        if self.kind == "tag":
            values = _parse_tag_values(value_tokens)
        else:
            value = " ".join(value_tokens).strip()
            if not value:
                raise ValueError("Value cannot be blank.")
            values = [value]

        deleted_snapshots: list[dict[str, object]] = []
        errors: list[str] = []

        for value in values:
            resolved = _resolve_source_row(browser, self.kind, value, create=False)
            if resolved is None:
                browser.emit("{} not found: {!r} (nothing to unlink)".format(self.kind.capitalize(), value))
                continue
            source_table, source_row, kind_label = resolved
            for target_id, target_row in target_rows:
                try:
                    deleted_snapshots.extend(
                        _unlink_one_value(
                            browser,
                            target_table=target_table,
                            target_row=target_row,
                            target_id=target_id,
                            source_table=source_table,
                            source_row=source_row,
                            kind_label=kind_label,
                        )
                    )
                except Exception as exc:
                    op_desc = "{}={!r} -> {}:{}".format(self.kind, value, target_table, target_id)
                    if best_effort:
                        browser.emit("ERROR (best-effort): {} ({})".format(op_desc, exc))
                        errors.append("{} ({})".format(op_desc, exc))
                        continue
                    rollback_errors = _restore_deleted_link_snapshots(browser, deleted_snapshots)
                    if rollback_errors:
                        browser.emit("Rollback encountered {} issue(s):".format(len(rollback_errors)))
                        for rollback_error in rollback_errors:
                            browser.emit("  - {}".format(rollback_error))
                    raise ValueError(
                        "Bulk `off` aborted on {} and restored {} link row(s).".format(
                            op_desc,
                            len(deleted_snapshots),
                        )
                    ) from exc

        if errors:
            browser.emit("Completed with {} best-effort error(s).".format(len(errors)))
        return True


class OffNoteCommand(_OffBaseCommand):
    """Detach note rows from one or more target rows."""

    name = "note"
    aliases = ("notes",)
    summary = "Detach note(s): off note <table> <id|selector> [--best-effort] <note text>"
    usage = "off note <table> <id|id,id|start-end> [--best-effort] <note text>"
    kind = "note"


class OffTagCommand(_OffBaseCommand):
    """Detach tags/labels from one or more target rows."""

    name = "tag"
    aliases = ("tags", "label", "labels")
    summary = "Detach tag(s): off tag <table> <id|selector> [--best-effort] <tag...>"
    usage = "off tag <table> <id|id,id|start-end> [--best-effort] <tag...>"
    kind = "tag"


class OffGenreCommand(_OffBaseCommand):
    """Detach genre rows from one or more target rows."""

    name = "genre"
    aliases = ("genres",)
    summary = "Detach genre: off genre <table> <id|selector> [--best-effort] <genre>"
    usage = "off genre <table> <id|id,id|start-end> [--best-effort] <genre>"
    kind = "genre"


class OffSubjectCommand(_OffBaseCommand):
    """Detach subject rows from one or more target rows."""

    name = "subject"
    aliases = ("subjects",)
    summary = "Detach subject: off subject <table> <id|selector> [--best-effort] <subject>"
    usage = "off subject <table> <id|id,id|start-end> [--best-effort] <subject>"
    kind = "subject"


class OffLanguageCommand(_OffBaseCommand):
    """Detach languages from one or more target rows."""

    name = "language"
    aliases = ("languages", "lang")
    summary = "Detach language: off language <table> <id|selector> [--best-effort] <language|code>"
    usage = "off language <table> <id|id,id|start-end> [--best-effort] <language|code>"
    kind = "language"


class OffSeriesCommand(_OffBaseCommand):
    """Detach series rows from one or more target rows."""

    name = "series"
    aliases = ()
    summary = "Detach series: off series <table> <id|selector> [--best-effort] <series>"
    usage = "off series <table> <id|id,id|start-end> [--best-effort] <series>"
    kind = "series"
