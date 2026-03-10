"""Interactive wizard command for adding genre rows."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.standardization import make_title_search_term, standardize_genre


def _clean_optional(value: str) -> Optional[str]:
    text = str(value).strip()
    return text or None


def _safe_int(value: str) -> Optional[int]:
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


class NewGenreWizardCommand(TerminalCommandAPI):
    """Create a genre row through guided prompts."""

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
        genre_sort = browser.prompt_text("Genre sort", default=default_sort).strip() or default_sort

        default_phash = make_title_search_term(genre_sort)
        genre_phash = browser.prompt_text("Genre phash", default=default_phash).strip() or default_phash

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

        genre_full = _clean_optional(browser.prompt_text("Genre full path (optional)", default=""))

        duplicate_column = "genre_phash" if "genre_phash" in columns else "genre"
        duplicate_term = genre_phash if duplicate_column == "genre_phash" else genre_text
        existing = browser.db.search("genres", duplicate_column, duplicate_term)
        if existing:
            browser.emit(
                "Possible duplicate genre exists: genre_id={} genre={!r}".format(
                    existing[0]["genre_id"],
                    existing[0]["genre"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another genre with this phash?", default=False)
            if not proceed_duplicate:
                raise ValueError("Genre wizard canceled to avoid duplicate entry.")

        browser.emit("Genre summary")
        browser.emit("  genre: {}".format(genre_text))
        browser.emit("  sort: {}".format(genre_sort))
        browser.emit("  phash: {}".format(genre_phash))
        browser.emit("  parent_id: {}".format(parent_id if parent_id is not None else ""))
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

        genre_row = Row.from_idless_row_dict(
            browser.db,
            row_dict=row_dict,
            table="genres",
        )

        browser.emit(
            "Genre created: genre_id={} genre={!r}".format(
                genre_row["genre_id"],
                genre_row["genre"],
            )
        )
        return True
