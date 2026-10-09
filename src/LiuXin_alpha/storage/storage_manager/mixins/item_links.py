"""
Implement Item-role target assignment and removal over shared reference metadata.

Atomic/Composite links validate their target and use the shared setter. Unlinking
uses the exact mapping key directly. None of these operations changes target bytes
or verifies Item existence in a separate catalogue.
"""

from __future__ import annotations

from typing import override

from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class ItemDigitalAssetLinkMixin(_StorageManagerState):
    """
    Manage exact Item-role keys in the shared target-reference mapping.

    Link methods check the target before delegating to the common setter. That setter validates
    int-convertible Item-ID positivity and nonblank role text, retains their original values, and
    assigns under the lock/metadata transaction. This layer adds no Item catalogue lookup, physical
    selection, or revision precondition.

    Example:
        >>> manager.link_item_to_digital_asset(item_id, asset_id, role="cover")  # doctest: +SKIP
    """

    @override
    def link_item_to_digital_asset(
        self,
        item_id: manager_api.ItemID,
        digital_asset_id: manager_api.DigitalAssetID,
        *,
        role: str = "primary_payload",
    ) -> None:
        """
        Require the atomic Asset record, then set the exact Item-role target through the shared
        helper.

        Target lookup precedes Item/role validation. The assignment replaces either an atomic or
        Composite target already stored for the key, without resolving readability. Repository and
        transaction failures propagate.

        Example:
            >>> manager.link_item_to_digital_asset(item_id, asset_id, role="cover")  # doctest: +SKIP


        :param item_id: Item ID validated by int(item_id) > 0 in the shared setter but retained without coercion.
        :param digital_asset_id: Atomic Asset ID that must already be registered.
        :param role: Exact Item-role key, defaulting to primary_payload; spelling and whitespace are significant.
        :return: None after the target tuple is stored by _set_item_target.
        """

        self.get_digital_asset_record(digital_asset_id)
        self._set_item_target(item_id, role, "digital_asset", digital_asset_id)

    @override
    def link_item_to_composite_digital_asset(
        self,
        item_id: manager_api.ItemID,
        composite_digital_asset_id: manager_api.CompositeDigitalAssetID,
        *,
        role: str = "primary_payload",
    ) -> None:
        """
        Require the Composite record, then replace the exact Item-role target through the shared
        helper.

        Member availability is not inspected, and target lookup occurs before Item/role validation.
        Metadata persistence belongs to the shared transaction and mapping implementation.

        Example:
            >>> manager.link_item_to_composite_digital_asset(item_id, composite_id)  # doctest: +SKIP


        :param item_id: Item ID checked for positive int-convertibility and retained unchanged by the shared setter.
        :param composite_digital_asset_id: Composite ID that must already be registered.
        :param role: Exact Item-role key, defaulting to primary_payload; spelling and whitespace are significant.
        :return: None after assigning the Composite target tuple.
        """

        self.get_composite_digital_asset_record(composite_digital_asset_id)
        self._set_item_target(
            item_id,
            role,
            "composite_digital_asset",
            composite_digital_asset_id,
        )

    @override
    def unlink_item_digital_asset(
        self,
        item_id: manager_api.ItemID,
        *,
        role: str = "primary_payload",
    ) -> bool:
        """
        Pop the exact Item-role key under the manager lock and metadata transaction.

        Unlike linking, this method does not validate or normalize the ID or role first. It does not
        inspect or delete the target, and it uses no revision precondition. Malformed/unhashable
        keys and repository errors propagate.

        Example:
            >>> removed = manager.unlink_item_digital_asset(item_id, role="cover")  # doctest: +SKIP


        :param item_id: Exact Item key component used in the target mapping.
        :param role: Exact Item-role key, defaulting to primary_payload; spelling and whitespace are significant.
        :return: True when the removed mapping value was not None; absent keys return False.
        """

        with self._lock, self._metadata_transaction():
            return self._item_targets.pop((item_id, role), None) is not None


__all__ = ["ItemDigitalAssetLinkMixin"]
