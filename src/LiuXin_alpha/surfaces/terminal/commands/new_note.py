"""Interactive wizard command for adding note rows."""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class NewNoteWizardCommand(TerminalCommandAPI):
    """Create a note row through guided prompts."""

    group = "add"
    name = "note"
    aliases = (
        "new-note",
        "new_note",
        "add-note",
        "add_note",
    )
    summary = "Interactive wizard to add a note."
    usage = "add note"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        if "notes" not in set(browser.db.get_tables()):
            raise ValueError("Database schema does not contain `notes` table.")

        browser.emit("New note wizard")
        browser.emit("---------------")

        note_text = browser.prompt_text("Note text", default="").strip()
        if not note_text:
            raise ValueError("Note text cannot be blank.")

        existing = browser.db.search("notes", "note", note_text)
        if existing:
            browser.emit(
                "Possible duplicate note exists: note_id={} note={!r}".format(
                    existing[0]["note_id"],
                    existing[0]["note"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another identical note?", default=False)
            if not proceed_duplicate:
                raise ValueError("Note wizard canceled to avoid duplicate entry.")

        proceed = browser.prompt_yes_no("Create this note now?", default=True)
        if not proceed:
            raise ValueError("Note wizard canceled.")

        result = browser.execute_core_command(
            "catalog.entity.create",
            payload={
                "repository": "notes",
                "data": {"note": note_text},
            },
        )
        note_row = dict(result["entity"])
        browser.emit(
            "Note created: note_id={} note={!r}".format(
                note_row["note_id"],
                note_row["note"],
            )
        )
        return True
