"""
Preserve compatibility imports for one-to-many link-table contracts.

Re-export public names from one_many_tables_api without wrapping or
copying the canonical classes and link-value types.
"""

from .one_many_tables_api import *  # noqa: F401,F403
