"""
Collect genre text, normalization, and optional hierarchy fields for a Core catalog creation.

Parent rows are checked before mutation. Optional payload fields follow available
schema columns, while duplicate checks are advisory and separate from creation.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.standardization import (
    make_title_search_term,
    standardize_genre,
)


def _clean_optional(value: str) -> Optional[str]:
    """
    Normalize optional prompt text to a stripped string or ``None`` when blank.

    Example:
        >>> _clean_optional(" Fiction/Science "), _clean_optional("")
        ('Fiction/Science', None)


    :param value: Prompt result stringified before whitespace removal.
    :return: Nonblank text or ``None``; nonblank null-like words remain text.
    """
    text = str(value).strip()
    return text or None


def _safe_int(value: str) -> Optional[int]:
    """
    Parse an optional integer prompt, returning ``None`` for blank or invalid integer text.

    String conversion happens outside the conversion handler. No positivity or
    hierarchy-position range is checked here.

    Example:
        >>> _safe_int("-2"), _safe_int("")
        (-2, None)


    :param value: Prompt text to stringify, strip, and parse.
    :return: Parsed integer or ``None`` for blank/invalid text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


class NewGenreWizardCommand(TerminalCommandAPI):
    """
    Prompt for a genre, editable sort/hash defaults, and optional parent/position/path data.

    The wizard checks parent existence and requests confirmation for a possible
    duplicate before the final creation confirmation.

    Example:
        >>> NewGenreWizardCommand().usage
        'add genre'
    """

    group = "add"
    name = "genre"
    aliases = (
        "new-genre",
        "new_genre",
        "add-genre",
        "add_genre",
    )
    summary = "Interactive wizard to add a genre."
    usage = "add genre"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Validate prompted genre fields and create a schema-filtered catalog payload after confirmation.

        Duplicate search uses the phash column when available, otherwise raw genre
        text. Parent lookup occurs even if no supported parent field can be stored.
        Optional positions are integers without a local range check. Cancellation
        raises; Core/result/output failures propagate without post-write rollback.

        Example:
            >>> NewGenreWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host providing schema/parent/search reads, prompts, Core creation, and output.
        :param args: Must be empty; all creation fields are prompted.
        :return: ``True`` after the created genre has been reported.
        :raises ValueError: For invalid arguments/schema/required text/integers/parent or declined confirmation.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        if "genres" not in tables:
            raise ValueError("Database schema does not contain `genres` table.")
        columns = set(browser.db.get_column_headings("genres"))

        browser.emit("New genre wizard")
        browser.emit("---------------")

        genre_text = browser.prompt_text("Genre", default="").strip()
        if not genre_text:
            raise ValueError("Genre cannot be blank.")

        default_sort = standardize_genre(genre_text)
        genre_sort = (
            browser.prompt_text("Genre sort", default=default_sort).strip()
            or default_sort
        )

        default_phash = make_title_search_term(genre_sort)
        genre_phash = (
            browser.prompt_text("Genre phash", default=default_phash).strip()
            or default_phash
        )

        parent_id_text = browser.prompt_text("Parent genre id (optional)", default="")
        parent_id = _safe_int(parent_id_text)
        if parent_id_text.strip() and parent_id is None:
            raise ValueError("Parent genre id must be an integer.")
        parent_row = None
        if parent_id is not None:
            parent_row = browser.db.get_row_from_id("genres", parent_id)
            if parent_row is None:
                raise ValueError("No genre exists with genre_id={}.".format(parent_id))

        position_text = browser.prompt_text("Genre position (optional)", default="")
        genre_position = _safe_int(position_text)
        if position_text.strip() and genre_position is None:
            raise ValueError("Genre position must be an integer.")

        genre_full = _clean_optional(
            browser.prompt_text("Genre full path (optional)", default="")
        )

        duplicate_column = "genre_phash" if "genre_phash" in columns else "genre"
        duplicate_term = (
            genre_phash if duplicate_column == "genre_phash" else genre_text
        )
        existing = browser.db.search("genres", duplicate_column, duplicate_term)
        if existing:
            browser.emit(
                "Possible duplicate genre exists: genre_id={} genre={!r}".format(
                    existing[0]["genre_id"],
                    existing[0]["genre"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no(
                "Create another genre with this phash?", default=False
            )
            if not proceed_duplicate:
                raise ValueError("Genre wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("genre", genre_text),
                        ("sort", genre_sort),
                        ("phash", genre_phash),
                        ("parent_id", parent_id if parent_id is not None else ""),
                    ],
                )
            ],
            title="Genre summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this genre now?", default=True)
        if not proceed:
            raise ValueError("Genre wizard canceled.")

        row_dict = {"genre": genre_text}
        if "genre_sort" in columns:
            row_dict["genre_sort"] = genre_sort
        if "genre_phash" in columns:
            row_dict["genre_phash"] = genre_phash
        if parent_row is not None:
            if "genre_parent_id" in columns:
                row_dict["genre_parent_id"] = parent_row.row_id
            elif "genre_parent" in columns:
                row_dict["genre_parent"] = parent_row.row_id
        if genre_position is not None and "genre_position" in columns:
            row_dict["genre_position"] = genre_position
        if genre_full is not None and "genre_full" in columns:
            row_dict["genre_full"] = genre_full

        result = browser.execute_core_command(
            "catalog.entity.create",
            payload={"repository": "genres", "data": row_dict},
        )
        genre_row = dict(result["entity"])

        browser.emit(
            "Genre created: genre_id={} genre={!r}".format(
                genre_row["genre_id"],
                genre_row["genre"],
            )
        )
        return True
