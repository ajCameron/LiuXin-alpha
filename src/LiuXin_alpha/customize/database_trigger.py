"""
Database_trigger - plugins implemented in python to be run on the database.

Allows the user to write functions in python which it would be too laborious to code in SQL.
As a rule, try not to use these.
They slow everything down significantly.
"""

from copy import deepcopy

from LiuXin_alpha.databases.database import Database


class Trigger:
    """
    A trigger to be run on the database.
    """

    def __init__(self, database=None):
        """
        Run initialisation tasks for the Trigger.
        Each instance of the trigger is attatched to a database - this is used to process the trigger_conditions -
        expanding the table names out into all the columns.
        :return:
        """
        if database is None:
            self.db = Database()
        else:
            self.db = database

        self._trigger_conditions = {
            "after delete": set([]),
            "after insert": set([]),
            "after update": set([]),
            "before delete": set([]),
            "before insert": set([]),
            "before update": set([]),
        }
        self.process_trigger_conditions()

        # The trigger should be run after operations occur on which of the tables?
        self.associated_tables = set()

    @property
    def trigger_conditions(self):
        """
        Which operations will invoke the trigger?

        This is a dictionary keyed by the trigger names and valued by a set of the columns they apply to.
        If the name of the table is provided in the set then the trigger will run for any column in that tables.
        E.g. if the key is "after update" and the value set is {'title', 'creator', 'identifiers'} then the trigger will
        run after an operation of the given type on any column in the table.
        :return:
        """
        return self._trigger_conditions

    def process_trigger_conditions(self):
        """
        Expands any tables names in any of the sets out into their full complement of columns.

        Also checks the values against the allowed tables and columns on the database.
        :return:
        """
        trigger_cons = deepcopy(self.trigger_conditions)
        new_trigger_cons = dict()
        for trigger_type in trigger_cons:

            new_trigger_cons[trigger_type] = set()
            trigger_columns = trigger_cons[trigger_type]
            for column in trigger_columns:
                # If the column is the name of a table then expand it to all the columns in that table
                if column in self.db.get_tables_and_columns().keys():
                    new_columns = set([c for c in self.db.get_tables_and_columns()[column]])
                    new_trigger_cons[trigger_type].union(new_columns)
                else:
                    new_trigger_cons[trigger_type].add(column)

        self._trigger_conditions = new_trigger_cons

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHOD TO RUN WHEN THE TRIGGER IS PULLED

    def pull(self, target_id, target_table):
        """
        Applies the trigger to the target_id in the target_table.

        :param target_id: The id of the row in the table
        :param target_table: The table the id is in.
        :return:
        """
        raise NotImplementedError


#
# ----------------------------------------------------------------------------------------------------------------------
