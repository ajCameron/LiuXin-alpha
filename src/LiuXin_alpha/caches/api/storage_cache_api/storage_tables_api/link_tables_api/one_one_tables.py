"""
Preserve compatibility imports for one-to-one link-table contracts.

Re-export public names from one_one_tables_api without wrapping or
copying the canonical classes and link-value types.
"""

from .one_one_tables_api import *  # noqa: F401,F403
