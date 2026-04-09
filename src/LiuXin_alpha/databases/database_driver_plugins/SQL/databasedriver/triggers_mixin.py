
import sqlite3


class TriggersMixin:
    """
    Mixin for methods to handle triggers.
    """

    def direct_get_triggers(self) -> list[str]:
        """
        Returns a list of all triggers defined on the database.
        Returns an empty set if there are
        :return:
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

    def direct_drop_triggers(self, triggers):
        """
        Takes a list of triggers by name - drops all of them from the DatabasePing.
        :return:
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
