"""
Digital Asset identity registry facade.
"""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.models import Digest
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetDeclaration,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetRecord,
)


class DigitalAssetRegistryAPI(abc.ABC):
    """
    Domain operations for known atomic byte identities.

    Implementations may use repositories internally, but this facade accepts
    and returns domain values rather than database records.
    Registry operations own content identity and descriptive metadata; they do
    not publish bytes or assert that a physical Replica currently exists.

    Example:
        >>> def lookup(
        ...     registry: DigitalAssetRegistryAPI,
        ...     asset_id: DigitalAssetID,
        ... ) -> DigitalAssetRecord:
        ...     return registry.get_digital_asset_record(asset_id)
    """

    @abc.abstractmethod
    def declare_digital_asset(
        self,
        declaration: DigitalAssetDeclaration,
    ) -> DigitalAssetRecord:
        """
        Register a known expected byte sequence without creating a Replica.

        Ingest is the usual operation when bytes are available. Declaration is
        useful for manifests, restoration catalogues, or a known-but-missing
        Asset.

        Example:
            >>> record = registry.declare_digital_asset(  # doctest: +SKIP
            ...     declaration,
            ... )


        :param declaration:
        :return:
        """
        ...

    @abc.abstractmethod
    def get_digital_asset_record(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetRecord:
        """
        Return the manager record or raise ``DigitalAssetNotFound``.

        Example:
            >>> record = registry.get_digital_asset_record(  # doctest: +SKIP
            ...     DigitalAssetID(7),
            ... )


        :param digital_asset_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def update_digital_asset_metadata(
        self,
        digital_asset_id: DigitalAssetID,
        metadata: DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """
        Replace descriptive metadata without changing byte identity.

        A stale ``if_revision`` raises ``StoragePreconditionFailed``.

        Example:
            >>> record = registry.update_digital_asset_metadata(  # doctest: +SKIP
            ...     DigitalAssetID(7), metadata, if_revision="v2",
            ... )


        :param digital_asset_id:
        :param metadata:
        :param if_revision:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_digital_asset_records(self) -> Iterator[DigitalAssetRecord]:
        """
        Iterate over known Digital Asset records.

        Example:
            >>> records = list(  # doctest: +SKIP
            ...     registry.iter_digital_asset_records(),
            ... )


        :return:
        """
        ...

    @abc.abstractmethod
    def find_digital_asset_record_by_digest(
        self,
        digest: Digest,
        *,
        size_bytes: int | None = None,
    ) -> DigitalAssetRecord | None:
        """
        Find a deduplication candidate by digest and optional exact size.

        Only genuine absence returns ``None``; repository and connection
        failures remain visible.

        Example:
            >>> record = registry.find_digital_asset_record_by_digest(  # doctest: +SKIP
            ...     Digest("sha256", "a" * 64), size_bytes=42,
            ... )


        :param digest:
        :param size_bytes:
        :return:
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
        """
        Forget an Asset identity without implying physical byte deletion.

        The safe default refuses to forget an Asset with Replica claims.

        Example:
            >>> forgotten = registry.forget_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), require_no_replicas=True,
            ... )


        :param digital_asset_id:
        :param require_no_replicas:
        :param if_revision:
        :return:
        """
        ...


__all__ = ["DigitalAssetRegistryAPI"]
