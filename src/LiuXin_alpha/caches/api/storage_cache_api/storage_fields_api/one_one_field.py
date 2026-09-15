"""
Preserve compatibility imports for one-to-one field contracts.

Re-export the owning one_one_field_api module's public names with
class identity unchanged.
This module does not load fields or perform database operations.
"""

from .one_one_field_api import *  # noqa: F401,F403
