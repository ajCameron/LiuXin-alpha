"""
Define customization hooks invoked by database events.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise database trigger through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""

from copy import deepcopy

from LiuXin_alpha.databases.database import Database


class Trigger:
    """
    A trigger to be run on the database.

    Example:
        Exercise Trigger through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self, database=None):
        """
        Run initialisation tasks for the Trigger. Each instance of the trigger is attatched to a database - this is used to process the trigger_conditions - expanding the table names out into all the columns.

        Example:
            Exercise Trigger.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param database: Value supplied for database under the utility contract.
        :return: None; validated state is stored on the receiving object.
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

        Example:
            Exercise Trigger.trigger conditions through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._trigger_conditions

    def process_trigger_conditions(self):
        """
        Expands any tables names in any of the sets out into their full complement of columns.

        Example:
            Exercise Trigger.process trigger conditions through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

        Example:
            Exercise Trigger.pull through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_id: Value supplied for target id under the utility contract.
        :param target_table: Value supplied for target table under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


#
# ----------------------------------------------------------------------------------------------------------------------
