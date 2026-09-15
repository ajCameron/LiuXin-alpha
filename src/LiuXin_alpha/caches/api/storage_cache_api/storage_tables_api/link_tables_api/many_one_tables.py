"""
Preserve compatibility imports for many-to-one link-table contracts.

Re-export public names from many_one_tables_api without wrapping or
copying the canonical classes and link-value types.
"""

from .many_one_tables_api import *  # noqa: F401,F403
