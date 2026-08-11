"""Digital Asset catalogue facade."""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api2.storage_manager_api.models import (
    DigitalAssetID, DigitalAssetRecordAPI,
)


class DigitalAssetCatalogAPI(abc.ABC):
    """Catalogue persistence contract for byte-independent asset metadata.

    Implementations keep asset identity separate from the locations of its
    physical replicas.

    Example:
        >>> def lookup(catalog: DigitalAssetCatalogAPI, asset_id: DigitalAssetID):
        ...     return catalog.get_digital_asset(asset_id)
    """

    @abc.abstractmethod
    def create_digital_asset(self, asset: DigitalAssetRecordAPI) -> DigitalAssetRecordAPI:
        """Persist a new Digital Asset record.

        Example:
            >>> created = catalog.create_digital_asset(asset)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_digital_asset(self, digital_asset_id: DigitalAssetID) -> DigitalAssetRecordAPI:
        """Return one Digital Asset record by identifier.

        Example:
            >>> asset = catalog.get_digital_asset(7)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def update_digital_asset(self, asset: DigitalAssetRecordAPI) -> DigitalAssetRecordAPI:
        """Persist changes to an existing Digital Asset record.

        Example:
            >>> updated = catalog.update_digital_asset(asset)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_digital_assets(self) -> Iterator[DigitalAssetRecordAPI]:
        """Iterate over known Digital Asset records.

        Example:
            >>> assets = list(catalog.iter_digital_assets())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def find_digital_asset_by_digest(
        self, sha256: str, *, size_bytes: int | None = None,
    ) -> DigitalAssetRecordAPI | None:
        """Find a deduplication candidate by SHA-256 and optional size.

        Example:
            >>> asset = catalog.find_digital_asset_by_digest(  # doctest: +SKIP
            ...     "a" * 64, size_bytes=42,
            ... )
        """
        ...

    @abc.abstractmethod
    def delete_digital_asset_metadata(
        self, digital_asset_id: DigitalAssetID, *, require_no_replicas: bool = True,
    ) -> bool:
        """Delete asset metadata, optionally requiring zero replica records.

        Example:
            >>> deleted = catalog.delete_digital_asset_metadata(  # doctest: +SKIP
            ...     7, require_no_replicas=True,
            ... )
        """
        ...


__all__ = ["DigitalAssetCatalogAPI"]
