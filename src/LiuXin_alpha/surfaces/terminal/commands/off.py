"""
Detach existing metadata relations from selected rows without creating missing source records.

Default bulk recovery replays recorded relations after a per-target error; it is
not a transaction and does not restore exact link IDs/priorities. Best-effort mode
keeps successful removals and reports per-target failures. Source lookup failures
occur outside that handler and can leave earlier changes in place.
"""

from __future__ import annotations

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
    """
    Snapshot a source/target pair's existing links and request semantic removal through Core.

    Each snapshot retains row fields plus table and endpoint metadata; a discoverable
    non-null relation type is also recorded. The unlink request itself does not
    select a type. Output happens after unlinking, so an output failure can prevent
    the caller from receiving snapshots for a removal that already occurred.

    Example:
        >>> snapshots = _unlink_one_value(  # doctest: +SKIP
        ...     browser, target_table="works", target_row=work, target_id=1,
        ...     source_table="tags", source_row=tag, kind_label="tag"
        ... )


    :param browser: Host supplying schema/relation reads, Core unlink dispatch, and output.
    :param target_table: Target table used in schema lookup, command payload, and messages.
    :param target_row: Existing target row whose ``row_id`` supplies the relation endpoint.
    :param target_id: Target ID used in messages, independent of the row object's identifier.
    :param source_table: Metadata source table used for schema and endpoint identification.
    :param source_row: Existing metadata row with mapping access and a ``row_id`` attribute.
    :param kind_label: Human-readable metadata kind included in output and schema errors.
    :return: Snapshot dictionaries after unlinking, or an empty list when no relation was found.
    :raises ValueError: If no link table exists for the endpoint pair.
    """
    source_id_column = browser.db.driver_wrapper.get_id_column(source_table)
    source_id = source_row[source_id_column]

    link_table = browser.db.driver_wrapper.get_link_table_name(
        source_table, target_table
    )
    if not link_table:
        raise ValueError(
            "No link table exists between {} and {} for `{}`.".format(
                source_table,
                target_table,
                kind_label,
            )
        )

    existing_links = browser.db.get_interlink_row(
        primary_row=source_row, secondary_row=target_row, onelink=False
    )
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
        snapshot = dict(link_row.row_dict)
        snapshot["_table"] = str(link_row.table)
        relation: dict[str, object] = {
            "table": source_table,
            "row_id": int(source_row.row_id),
            "related_table": target_table,
            "related_row_id": int(target_row.row_id),
        }
        try:
            type_column = browser.db.driver_wrapper.get_link_column(
                source_table,
                target_table,
                "type",
            )
        except Exception:
            type_column = None
        if type_column is not None and snapshot.get(type_column) is not None:
            relation["type"] = snapshot[type_column]
        snapshot["_relation"] = relation
        deleted_snapshots.append(snapshot)
    browser.execute_core_command(
        "admin.relation.unlink",
        payload={
            "table": source_table,
            "row_id": int(source_row.row_id),
            "related_table": target_table,
            "related_row_id": int(target_row.row_id),
        },
    )

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


def _restore_deleted_link_snapshots(
    browser, snapshots: list[dict[str, object]]
) -> list[str]:
    """
    Replay removed relations in reverse order, preferring semantic endpoint/type restoration.

    Semantic replay lets the backend assign new priorities and does not restore
    arbitrary snapshot fields or original IDs. Legacy snapshots without relation
    metadata use row creation after dropping the old identity. Core write failures
    are collected; legacy ID-column discovery occurs outside its write handler
    and can still abort recovery. Original snapshot mappings are not modified.

    Example:
        >>> from unittest.mock import Mock
        >>> _restore_deleted_link_snapshots(Mock(), [])
        []


    :param browser: Host supplying Core relation/row creation and legacy ID-column lookup.
    :param snapshots: Link-row snapshots, optionally carrying ``_relation`` and ``_table`` metadata.
    :return: Collected restore-error messages, not proof of exact restoration of prior database state.
    """
    errors: list[str] = []
    for snapshot in reversed(snapshots):
        row_dict = dict(snapshot)
        relation = row_dict.pop("_relation", None)
        if isinstance(relation, dict):
            try:
                # Re-link semantically so the database can choose a currently
                # valid priority; replaying the deleted raw priority may collide
                # with links that remained in place during rollback.
                browser.execute_core_command(
                    "admin.relation.link",
                    payload=dict(relation),
                )
            except Exception as exc:
                errors.append(
                    "relationship restore failed for {}:{} -> {}:{} ({})".format(
                        relation.get("table"),
                        relation.get("row_id"),
                        relation.get("related_table"),
                        relation.get("related_row_id"),
                        exc,
                    )
                )
            continue
        table = str(row_dict.pop("_table", "") or "")
        if not table:
            errors.append("restore failed: missing link table")
            continue
        id_column = browser.db.driver_wrapper.get_id_column(table)
        row_dict.pop(id_column, None)
        try:
            browser.execute_core_command(
                "admin.row.create",
                payload={"table": table, "values": row_dict},
            )
        except Exception as exc:
            errors.append("restore failed for table {} ({})".format(table, exc))
    return errors


class _OffBaseCommand(TerminalCommandAPI):
    """
    Share source lookup, selected-target unlinking, and compensating recovery for detachment commands.

    Subclasses select a singular metadata kind. Missing source values produce
    messages without creation; best-effort mode applies only to per-target unlink
    failures, not to source-resolution or initial target errors.

    Example:
        >>> OffTagCommand().group, OffTagCommand().kind
        ('off', 'tag')
    """

    group = "off"
    expose_direct = False
    kind = ""
    usage = ""

    def execute(self, browser, args: list[str]) -> bool:
        """
        Resolve all targets, look up metadata values without creation, and remove matching relations.

        Tag values are split/deduplicated; other kinds join tokens into one value.
        Per-target failures either continue with diagnostics or trigger attempted
        replay of snapshots returned by earlier successful calls. The current
        failing call may already have effects not represented by those snapshots.
        Source-resolution failures bypass replay, and restore-error messages do
        not make the subsequent ``restored`` count a verified recovery claim.

        Example:
            >>> OffTagCommand().execute(browser, ["works:1-3", "--best-effort", "history"])  # doctest: +SKIP


        :param browser: Host supplying reads, Core relation mutations, and terminal output.
        :param args: Target selector, optional leading best-effort switches, and metadata value tokens.
        :return: ``True`` after completion, including missing sources or reported best-effort errors.
        :raises ValueError: For invalid targets/values or default-mode per-target unlink failure.
        """
        target_table, target_rows, consumed = _parse_target_rows(
            browser, args, usage=self.usage
        )
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
                browser.emit(
                    "{} not found: {!r} (nothing to unlink)".format(
                        self.kind.capitalize(), value
                    )
                )
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
                    op_desc = "{}={!r} -> {}:{}".format(
                        self.kind, value, target_table, target_id
                    )
                    if best_effort:
                        browser.emit(
                            "ERROR (best-effort): {} ({})".format(op_desc, exc)
                        )
                        errors.append("{} ({})".format(op_desc, exc))
                        continue
                    rollback_errors = _restore_deleted_link_snapshots(
                        browser, deleted_snapshots
                    )
                    if rollback_errors:
                        browser.emit(
                            "Rollback encountered {} issue(s):".format(
                                len(rollback_errors)
                            )
                        )
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
    """
    Detach the first exact note-text match from selected targets while retaining the note itself.

    Remaining value tokens form one space-joined note; absence is reported without creation.

    Example:
        >>> OffNoteCommand().kind
        'note'
    """

    name = "note"
    aliases = ("notes",)
    summary = (
        "Detach note(s): off note <table> <id|selector> [--best-effort] <note text>"
    )
    usage = "off note <table> <id|id,id|start-end> [--best-effort] <note text>"
    kind = "note"


class OffTagCommand(_OffBaseCommand):
    """
    Detach normalized tag values from selected targets through the preferred tag-like table.

    Values accept comma-separated pieces and retain first spellings after normalized
    deduplication. Source tag/label records are not deleted by this command.

    Example:
        >>> OffTagCommand().aliases
        ('tags', 'label', 'labels')
    """

    name = "tag"
    aliases = ("tags", "label", "labels")
    summary = "Detach tag(s): off tag <table> <id|selector> [--best-effort] <tag...>"
    usage = "off tag <table> <id|id,id|start-end> [--best-effort] <tag...>"
    kind = "tag"


class OffGenreCommand(_OffBaseCommand):
    """
    Detach one resolved genre value from selected targets without deleting the genre record.

    Resolution uses the available genre hash/sort/text column and does not create on a miss.

    Example:
        >>> OffGenreCommand().kind
        'genre'
    """

    name = "genre"
    aliases = ("genres",)
    summary = "Detach genre: off genre <table> <id|selector> [--best-effort] <genre>"
    usage = "off genre <table> <id|id,id|start-end> [--best-effort] <genre>"
    kind = "genre"


class OffSubjectCommand(_OffBaseCommand):
    """
    Detach one resolved subject value from selected targets, retaining the source subject.

    Remaining text tokens form a single subject rather than a list of subjects.

    Example:
        >>> OffSubjectCommand().kind
        'subject'
    """

    name = "subject"
    aliases = ("subjects",)
    summary = (
        "Detach subject: off subject <table> <id|selector> [--best-effort] <subject>"
    )
    usage = "off subject <table> <id|id,id|start-end> [--best-effort] <subject>"
    kind = "subject"


class OffLanguageCommand(_OffBaseCommand):
    """
    Detach an existing language resolved from a name or code, without modifying language records.

    An unresolved language is reported as nothing to unlink rather than created.

    Example:
        >>> OffLanguageCommand().aliases
        ('languages', 'lang')
    """

    name = "language"
    aliases = ("languages", "lang")
    summary = "Detach language: off language <table> <id|selector> [--best-effort] <language|code>"
    usage = "off language <table> <id|id,id|start-end> [--best-effort] <language|code>"
    kind = "language"


class OffSeriesCommand(_OffBaseCommand):
    """
    Detach one standardized series value from selected targets while retaining its source record.

    All matching pair relations returned by the unlink helper are removed; this
    command has no series-index or relation-type filter.

    Example:
        >>> OffSeriesCommand().kind
        'series'
    """

    name = "series"
    aliases = ()
    summary = "Detach series: off series <table> <id|selector> [--best-effort] <series>"
    usage = "off series <table> <id|id,id|start-end> [--best-effort] <series>"
    kind = "series"
