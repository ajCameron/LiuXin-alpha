"""
Preserve compatibility imports for one-to-many field contracts.

Re-export the owning one_many_field_api module's public names with
class identity unchanged. The legacy LinkDstUpdate spelling is explicitly
provided from util_mixins for replacement payloads.
This module does not load fields or perform database operations.
"""

from .one_many_field_api import *  # noqa: F401,F403
from .util_mixins import LinkDstUpdate
