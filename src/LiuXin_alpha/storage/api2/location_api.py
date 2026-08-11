"""Durable opaque Location values used throughout the storage stack.

``Location`` is the persistable address.  Operational facades such as the
manager's ``BoundLocation`` consume it without changing its value semantics.

Example:
    >>> location = Location("primary", "objects/42")
    >>> location.key
    'objects/42'
"""

from LiuXin_alpha.storage.api2.models import Location, StoreRef


__all__ = ["Location", "StoreRef"]
