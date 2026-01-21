
"""
Classes to cache tables in numpy for (hopefully substantial) performance benefits.
"""


from LiuXin_alpha.customize.cache.base_tables import BaseTable


class NumpyTable(BaseTable):
    """
    Implementation of the table concept with everything stored (on the back end) in a numpy array.
    """
