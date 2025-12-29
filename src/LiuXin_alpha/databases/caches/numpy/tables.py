# Classes to cache data from the database for active manipulation


from LiuXin.databases.caches import BaseTable


class NumpyTable(BaseTable):
    """
    Implementation of the table concept with everything stored (on the back end) in a numpy array.
    """
