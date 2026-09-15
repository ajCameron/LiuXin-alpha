"""
Preserve compatibility imports for the canonical link-table base API.

Re-export public names from link_table_base_api without adding a wrapper
class or changing identity of the shared generic base.
"""

from .link_table_base_api import *  # noqa: F401,F403
