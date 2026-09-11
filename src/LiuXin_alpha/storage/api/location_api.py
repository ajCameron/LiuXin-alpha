"""
Re-export the common opaque Location value and StoreUUID alias.

These are the same objects defined in storage.api.models. Location identifies
a Store-owned key without providing filesystem operations; the manager's
BoundLocation supplies a separate operational facade.

Example:
    >>> Location(StoreUUID(int=1), "objects/42").key
    'objects/42'
"""

from LiuXin_alpha.storage.api.models import Location, StoreUUID


__all__ = ["Location", "StoreUUID"]
