"""Command for adding a note and attaching it to a specific row."""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.commands.link import _split_row_ref


def _safe_int(value: str):
    try:
        return int(value)
    except Exception:
        return None


class NoteOnCommand(TerminalCommandAPI):
    """Create and link a note with one command."""

    name = "note-on"
    aliases = ("noteon",)
    summary = "Attach a note to a row: note-on <table> <id> <note text>"
    usage = "note-on <table> <id> <note text>"

    def execute(self, browser, args: list[str]) -> bool:
        if not args:
            raise ValueError("Usage: {}".format(self.usage))

        compact_ref = _split_row_ref(args[0])
        if compact_ref is not None:
            if len(args) < 2:
                raise ValueError("Usage: {}".format(self.usage))
            target_table = browser.resolve_table(compact_ref[0])
            target_id = _safe_int(compact_ref[1])
            note_tokens = args[1:]
        else:
            if len(args) < 3:
                raise ValueError("Usage: {}".format(self.usage))
            target_table = browser.resolve_table(args[0])
            target_id = _safe_int(args[1])
            note_tokens = args[2:]

        if target_id is None:
            raise ValueError("Row id must be an integer.")

        note_text = " ".join(note_tokens).strip()
        if not note_text:
            raise ValueError("Note text cannot be blank.")

        if "notes" not in set(browser.db.get_tables()):
            raise ValueError("Database schema does not contain `notes` table.")

        target_row = browser.db.get_row_from_id(target_table, target_id)
        if target_row is None:
            raise ValueError("No row found in {} for id {}.".format(target_table, target_id))

        link_table = browser.db.driver_wrapper.get_link_table_name("notes", target_table)
        if not link_table:
            raise ValueError("No note link table exists for target table {!r}.".format(target_table))

        note_row = None
        candidate_notes = browser.db.search("notes", "note", note_text)
        for row in candidate_notes:
            existing_link = browser.db.get_interlink_row(primary_row=row, secondary_row=target_row, onelink=False)
            if existing_link:
                browser.emit(
                    "Note already linked: note_id={} -> {}:{}".format(
                        row["note_id"],
                        target_table,
                        target_id,
                    )
                )
                return True
        if candidate_notes:
            note_row = candidate_notes[0]
        else:
            result = browser.execute_core_command(
                "catalog.entity.create",
                payload={
                    "repository": "notes",
                    "data": {"note": note_text},
                },
            )
            note_row = browser.db.get_row_from_id(
                "notes",
                int(result["entity_id"]),
            )
            if note_row is None:
                raise RuntimeError("Core did not return the created note row.")

        browser.execute_core_command(
            "admin.relation.link",
            payload={
                "table": "notes",
                "row_id": int(note_row.row_id),
                "related_table": target_table,
                "related_row_id": int(target_row.row_id),
                "priority": 0,
            },
        )

        browser.emit(
            "Note linked: note_id={} -> {}:{}".format(
                note_row["note_id"],
                target_table,
                target_id,
            )
        )
        return True
