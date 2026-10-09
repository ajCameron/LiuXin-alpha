"""Exercise each manager convenience mixin against a minimal responsibility host."""

from __future__ import annotations

from types import SimpleNamespace

from LiuXin_alpha.storage.api.storage_manager_api import (
    CompositeDigitalAssetDeclaration,
    DigitalAssetDerivationDeclaration,
    DigitalAssetID,
    ItemID,
    ReplicaMode,
    ReplicationPolicy,
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


def test_asset_convenience_needs_only_the_ingest_operation_it_calls() -> None:
    """Use the Asset adapter without composing the complete manager.

    Example:
        >>> test_asset_convenience_needs_only_the_ingest_operation_it_calls()
    """

    class Host(DigitalAssetConvenienceMixin):
        def ingest_bytes(self, data, **kwargs):
            self.call = (data, kwargs)
            return SimpleNamespace(asset_record="asset")

    host = Host()
    assert host.store_bytes(b"book", original_name="book.epub") == "asset"
    assert host.call[0] == b"book"
    assert host.call[1]["metadata"].original_name == "book.epub"
    assert host.call[1]["replica_mode"] is ReplicaMode.ACTIVE


def test_item_link_convenience_needs_only_the_link_contract() -> None:
    """Use Item-link normalization with a host implementing only link operations.

    Example:
        >>> test_item_link_convenience_needs_only_the_link_contract()
    """

    class Host(ItemLinkConvenienceMixin):
        def link_item_to_digital_asset(self, item_id, asset_id, *, role):
            self.linked = (item_id, asset_id, role)

        def link_item_to_composite_digital_asset(self, item_id, composite_id, *, role):
            raise AssertionError("wrong link path")

        def unlink_item_digital_asset(self, item_id, *, role):
            self.unlinked = (item_id, role)
            return True

    host = Host()
    host.link(7, 9, role="cover")
    assert host.linked == (ItemID(7), DigitalAssetID(9), "cover")
    assert host.unlink(7, role="cover")


def test_composite_convenience_needs_only_the_composite_contract() -> None:
    """Construct a declaration on a host with no ingest, policy, or derivation mixin.

    Example:
        >>> test_composite_convenience_needs_only_the_composite_contract()
    """

    class Host(CompositeConvenienceMixin):
        def declare_composite_digital_asset(self, declaration):
            self.declaration = declaration
            return declaration

    host = Host()
    result = host.create_composite([DigitalAssetID(3)], name="package")
    assert isinstance(result, CompositeDigitalAssetDeclaration)
    assert host.declaration.members[0].digital_asset_id == DigitalAssetID(3)


def test_policy_convenience_needs_only_the_policy_contract() -> None:
    """Build a policy on a host implementing only policy registration.

    Example:
        >>> test_policy_convenience_needs_only_the_policy_contract()
    """

    class Host(StoragePolicyConvenienceMixin):
        def create_replication_policy(self, policy):
            self.policy = policy
            return policy

    host = Host()
    result = host.define_replication_policy("durable", copies=2)
    assert isinstance(result, ReplicationPolicy)
    assert host.policy.min_copies == 2


def test_derivation_convenience_needs_only_the_derivation_contract() -> None:
    """Build provenance on a host implementing only derivation registration.

    Example:
        >>> test_derivation_convenience_needs_only_the_derivation_contract()
    """

    class Host(DerivationConvenienceMixin):
        def record_digital_asset_derivation(self, declaration):
            self.declaration = declaration
            return declaration

    host = Host()
    result = host.record_derivation(DigitalAssetID(2), [DigitalAssetID(1)])
    assert isinstance(result, DigitalAssetDerivationDeclaration)
    assert host.declaration.result_digital_asset_id == DigitalAssetID(2)
