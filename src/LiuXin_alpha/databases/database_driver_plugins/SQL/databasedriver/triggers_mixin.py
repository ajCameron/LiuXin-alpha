
"""
List persistent SQLite triggers and drop explicitly named triggers.
"""

from __future__ import annotations

from typing import Iterable

import sqlite3


class TriggersMixin:
    """
    Inspect sqlite_master trigger names and remove trusted trigger identifiers.

    Example:
        ``driver.direct_get_triggers()`` lists persistent triggers in the main schema.
    """

    def direct_get_triggers(self) -> list[str]:
        """
        Read trigger names from sqlite_master and close on success or OperationalError.

        TEMP triggers are not included and no ordering is specified.

        Example:
            ``driver.direct_get_triggers()`` returns an empty list when no persistent triggers exist.


        :return: A list of trigger names.
        """
        conn = self.get_connection()
        stmt = "SELECT name FROM sqlite_master WHERE type = 'trigger';"
        triggers = []
        try:
            for row in conn.execute(stmt):
                triggers.append(row[0])
            conn.close()
        except sqlite3.OperationalError:
            conn.close()
            raise
        return triggers

    def direct_drop_triggers(self, triggers: Iterable[str]) -> bool:
        """
        Drop each named trigger and commit after each removal.

        Names become SQL syntax and must be trusted. A missing trigger raises OperationalError; earlier removals stay committed. Close on success or that error.

        Example:
            ``driver.direct_drop_triggers(["example_audit"])`` drops the named trigger.


        :param triggers: Iterable of trusted trigger identifiers inserted into DROP TRIGGER statements.
        :return: ``True`` after all removals, including an empty input.
        """
        conn = self.get_connection()
        stmt = "DROP TRIGGER {};"
        try:
            for trigger in triggers:
                current_stmt = stmt.format(trigger)
                conn.execute(current_stmt)
                conn.commit()
            conn.close()
        except sqlite3.OperationalError:
            conn.close()
            raise
        return True
