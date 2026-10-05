"""
Prompt for a standalone note and create it through Core after advisory duplicate checks.

This wizard creates a note record without attaching it to another row. Declined
confirmations raise cancellation errors; post-write output failures do not undo creation.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class NewNoteWizardCommand(TerminalCommandAPI):
    """
    Expose an argument-free note-creation wizard with exact-text duplicate confirmation.

    Existing notes are not reused automatically. The duplicate prompt defaults to
    refusal, while the final creation prompt defaults to acceptance.

    Example:
        >>> NewNoteWizardCommand().usage
        'add note'
    """

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
        """
        Require nonblank note text, check exact matches, and submit one confirmed catalog creation.

        Table/read/command/output errors propagate. Duplicate checking and creation
        are separate operations and do not establish uniqueness under concurrency.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> host.db.get_tables.return_value = ["notes"]
            >>> host.db.search.return_value = []
            >>> host.prompt_text.return_value = "Check edition"
            >>> host.prompt_yes_no.return_value = True
            >>> host.execute_core_command.return_value = {"entity": {"note_id": 1, "note": "Check edition"}}
            >>> NewNoteWizardCommand().execute(host, [])
            True
            >>> host.emit.assert_called_with("Note created: note_id=1 note='Check edition'")


        :param browser: Host providing prompts, note reads, Core catalog creation, and output.
        :param args: Must be empty; all note data is collected through prompts.
        :return: ``True`` after reporting the created note.
        :raises ValueError: For arguments, absent notes table, blank text, or declined confirmation.
        """
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
            proceed_duplicate = browser.prompt_yes_no(
                "Create another identical note?", default=False
            )
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
