
"""
Holds database macros.

Macros make working with the database easier - they are common operations which can be usefully speeded up
by re-writing them in a backend dependent way.
E.g,
"""



# Holds the base class for the macros - should include an implementation of all the given macros using the basic db
# methods.
# Each of the individual macros implementations should subclass this - so all the necessary methods will be there and
# will have a functional implementation

from collections import defaultdict

from LiuXin_alpha.databases.api import DatabaseAPI


class MacrosBase:
    def __init__(self, db: DatabaseAPI):
        """
        Attaches to the underlying database to provide additional services.
        :param db:
        """
        self.db = db



    def get_link_data(self,
                      table1: str,
                      table2: str,
                      table1_id: int,
                      typed: bool = False,
                      priority: bool = False):
        """
        Return an object containing the data for a.

        :param table1:
        :param table2:
        :param table1_id:
        :param typed:
        :param priority:
        :return:
        """
        table2_id_col = self.db.driver_wrapper.get_id_column(table2)

        if not typed and not priority:
            table1_row = self.db.get_row_from_id(table1, table1_id)
            linked_rows = self.db.get_interlinked_rows(table1_row, table2)
            return set([lr[table2_id_col] for lr in linked_rows])

        elif not typed and priority:
            table1_row = self.db.get_row_from_id(table1, table1_id)
            linked_rows = self.db.get_interlinked_rows(table1_row, table2)
            return [lr[table2_id_col] for lr in linked_rows]

        elif typed and not priority:
            table1_row = self.db.get_row_from_id(table1, table1_id)
            linked_rows = self.db.get_interlinked_rows(table1_row, table2)

            link_table_type_col = self.db.driver_wrapper.get_link_column(table1, table2, "type")

            link_container = defaultdict(set)
            for lr in linked_rows:
                tlr = self.db.get_interlink_row(primary_row=table1_row, secondary_row=lr)
                link_container[tlr[link_table_type_col]].add(lr[table2_id_col])
            return link_container

        elif typed and priority:
            table1_row = self.db.get_row_from_id(table1, table1_id)
            linked_rows = self.db.get_interlinked_rows(table1_row, table2)

            link_table_type_col = self.db.driver_wrapper.get_link_column(table1, table2, "type")

            link_container = defaultdict(list)
            for lr in linked_rows:
                tlr = self.db.get_interlink_row(primary_row=table1_row, secondary_row=lr)
                link_container[tlr[link_table_type_col]].append(lr[table2_id_col])
            return link_container

        else:
            raise NotImplementedError
