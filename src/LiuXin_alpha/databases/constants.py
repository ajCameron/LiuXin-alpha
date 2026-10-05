
"""
Define the shared field datatypes and 30 MiB spool-size threshold.

VALID_DATA_TYPES is an immutable set including the None sentinel and recognized legacy field datatype names. CUSTOM_DATA_TYPES contains the same names without None. SPOOL_SIZE is expressed in bytes; importing these constants does not construct a database.
"""


from __future__ import division, absolute_import, print_function, unicode_literals

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

SPOOL_SIZE = 30 * 1024 * 1024

# Core recognized metadata / field datatypes used across database, surface and
# custom-column compatibility layers.
VALID_DATA_TYPES = frozenset(
    [
        None,
        "rating",
        "text",
        "comments",
        "datetime",
        "int",
        "float",
        "bool",
        "series",
        "composite",
        "enumeration",
    ]
)

# Custom columns intentionally exclude the None sentinel used by VALID_DATA_TYPES.
CUSTOM_DATA_TYPES = frozenset(x for x in VALID_DATA_TYPES if x is not None)
