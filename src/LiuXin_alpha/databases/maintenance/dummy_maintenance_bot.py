from __future__ import print_function, annotations


class DummyMaintenanceBot:
    """
    Is not a maintenance bot - but presents some of the same methods.
    """

    def __init__(self):
        pass

    def dirty_record(self, table, row_id):
        pass

    def new_dirty_record(self, table, row_id):
        pass

    def dirty_interlink_record(self, update_type, table1, table2, table1_id, table2_id):
        pass
