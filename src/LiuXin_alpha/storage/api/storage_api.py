"""
API contracts for stores.

The storage design is intentionally split:
- `StoreAPI` models one concrete store (disk, remote HTTP, tape, etc.).
- `StorageManagerAPI` models the manager/front-end that orchestrates many stores.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


