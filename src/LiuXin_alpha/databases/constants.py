from __future__ import division, absolute_import, print_function, unicode_literals

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

SPOOL_SIZE = 30 * 1024 * 1024

# Todo: This is a constant, and we've seen it before.

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