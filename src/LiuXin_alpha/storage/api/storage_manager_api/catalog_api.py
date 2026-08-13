"""Digital Asset identity registry facade."""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.models import Digest
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAsset,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetSpec,
)


class AssetRegistryAPI(abc.ABC):
    """Domain operations for known atomic byte identities.

    Implementations may use repositories internally, but this facade accepts
    and returns domain values rather than database records.

    Example:
        >>> def lookup(
        ...     registry: AssetRegistryAPI, asset_id: DigitalAssetID,
        ... ) -> DigitalAsset:
        ...     return registry.get_digital_asset(asset_id)
    """

    @abc.abstractmethod
    def declare_digital_asset(self, spec: DigitalAssetSpec) -> DigitalAsset:
        """Register a known expected byte sequence without creating a Replica.

        Ingest is the usual operation when bytes are available. Declaration is
        useful for manifests, restoration catalogues, or a known-but-missing
        Asset.

        Example:
            >>> asset = registry.declare_digital_asset(spec)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAsset:
        """Return one domain snapshot or raise ``DigitalAssetNotFound``.

        Example:
            >>> asset = registry.get_digital_asset(DigitalAssetID(7))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def update_digital_asset_metadata(
        self,
        digital_asset_id: DigitalAssetID,
        metadata: DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> DigitalAsset:
        """Replace descriptive metadata without changing byte identity.

        A stale ``if_revision`` raises ``StoragePreconditionFailed``.

        Example:
            >>> asset = registry.update_digital_asset_metadata(  # doctest: +SKIP
            ...     DigitalAssetID(7), metadata, if_revision="v2",
            ... )
        """
        ...

    @abc.abstractmethod
    def iter_digital_assets(self) -> Iterator[DigitalAsset]:
        """Iterate over known Digital Asset snapshots.

        Example:
            >>> assets = list(registry.iter_digital_assets())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def find_digital_asset_by_digest(
        self,
        digest: Digest,
        *,
        size_bytes: int | None = None,
    ) -> DigitalAsset | None:
        """Find a deduplication candidate by digest and optional exact size.

        Only genuine absence returns ``None``; repository and connection
        failures remain visible.

        Example:
            >>> asset = registry.find_digital_asset_by_digest(  # doctest: +SKIP
            ...     Digest("sha256", "a" * 64), size_bytes=42,
            ... )
        """
        ...

    @abc.abstractmethod
    def forget_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        require_no_replicas: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """Forget an Asset identity without implying physical byte deletion.

        The safe default refuses to forget an Asset with Replica claims.

        Example:
            >>> forgotten = registry.forget_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), require_no_replicas=True,
            ... )
        """
        ...


__all__ = ["AssetRegistryAPI"]
