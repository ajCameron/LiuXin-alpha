
"""
API for the database and related classes.
"""

import abc


class RowAPI(abc.ABC):
    """
    API for a row off the database.
    """



class DatabaseDriverAPI(abc.ABC):
    """
    Every database drive must descend from this class.
    """



class DatabaseAPI(abc.ABC):
    """
    API for the Database itself.
    """



class DatabaseCacheAPI(abc.ABC):
    """
    Every local cache containing data from the database must descend from this class.
    """



