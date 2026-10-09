"""Compose the responsibility-specific manager convenience adapters.

Implementation lives in focused modules; this module preserves the established
public import path and complete composed convenience surface.
"""

from __future__ import annotations

from LiuXin_alpha.storage.api.storage_manager_api._convenience_support import (
    DigitalAssetFileIdentifier,
)
from LiuXin_alpha.storage.api.storage_manager_api.asset_convenience import (
    DigitalAssetConvenienceMixin,
)
from LiuXin_alpha.storage.api.storage_manager_api.composite_convenience import (
    CompositeConvenienceMixin,
)
from LiuXin_alpha.storage.api.storage_manager_api.derivation_convenience import (
    DerivationConvenienceMixin,
)
from LiuXin_alpha.storage.api.storage_manager_api.item_link_convenience import (
    ItemLinkConvenienceMixin,
)
from LiuXin_alpha.storage.api.storage_manager_api.policy_convenience import (
    StoragePolicyConvenienceMixin,
)


class StorageConvenienceBase(
    DigitalAssetConvenienceMixin,
    ItemLinkConvenienceMixin,
    CompositeConvenienceMixin,
    StoragePolicyConvenienceMixin,
    DerivationConvenienceMixin,
):
    """Compose the responsibility-specific convenience adapters.

    This compatibility base preserves the complete established surface while
    allowing focused hosts and readers to use one concern mixin at a time. It
    owns no state and adds no behavior beyond method-resolution composition.

    Example:
        >>> issubclass(StorageConvenienceBase, DigitalAssetConvenienceMixin)
        True
    """


class StorageConvenienceAPI(StorageConvenienceBase):
    """
    Preserve the established public manager-convenience name over its implementation base.

    The base composes responsibility-specific caller adapters spanning catalogue, ingest,
    retrieval, policy, Replica, Composite, and derivation contracts. Individual exact-contract
    modules remain focused and do not acquire these coercion dependencies.

    Example:
        >>> isinstance(manager, StorageConvenienceAPI)  # doctest: +SKIP
        True
    """


__all__ = [
    "CompositeConvenienceMixin",
    "DerivationConvenienceMixin",
    "DigitalAssetConvenienceMixin",
    "DigitalAssetFileIdentifier",
    "ItemLinkConvenienceMixin",
    "StoragePolicyConvenienceMixin",
    "StorageConvenienceAPI",
    "StorageConvenienceBase",
]
