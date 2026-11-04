"""Persist auxiliary feed rows without bibliographic projection semantics.

Feed IDs address the physical ``feeds`` table. Recipe scripts are stored text,
never executed here. All writes use the shared portable macro transaction.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class FeedsSQLMixin:
    """Provide feed-row CRUD and atomic replacement using physical table IDs."""

    db: DatabaseAPI

    def add_feed(self, title: str, script: str) -> None:
        """Insert a feed title and recipe text, leaving commit to the transaction.

        This retained call shape returns None. Call ``macros.insert_row`` when
        the new row ID is needed.
        """
        self.db.macros.insert_row("feeds", {"feed_title": title, "feed_script": script})

    def delete_feed(
        self, feed_id: int | Iterable[int | tuple[int] | list[int]]
    ) -> None:
        """Delete one feed ID or an atomic batch of IDs/one-cell bindings.

        Validate all batch bindings before deletion. Missing IDs are no-ops;
        an enclosing portable transaction retains ownership of commit/rollback.
        """
        if isinstance(feed_id, int):
            self.db.macros.delete_row("feeds", feed_id)
        else:
            bindings = [
                (value,) if isinstance(value, int) else value for value in feed_id
            ]
            self.db.macros.bulk_delete_in_table("feeds", "feed_id", bindings)

    def update_feed(self, feed_id: int, script: str, title: str) -> None:
        """Update a feed's recipe text and display title together by row ID."""
        self.db.macros.update_row(
            "feeds", feed_id, {"feed_title": title, "feed_script": script}
        )

    def set_feeds(self, feeds: Iterable[tuple[str, str]]) -> None:
        """Atomically replace feeds with title/script pairs; IDs are not retained.

        Materialize input before deletion. A late insertion failure rolls back
        the replacement, and an outer transaction can roll back the whole call.
        """
        pairs = [(title, script) for title, script in feeds]
        with self.db.macros.transaction():
            ids = [(row["feed_id"],) for row in self.db.macros.get_rows("feeds")]
            self.db.macros.bulk_delete_in_table("feeds", "feed_id", ids)
            for title, script in pairs:
                self.db.macros.insert_row(
                    "feeds", {"feed_title": title, "feed_script": script}
                )
