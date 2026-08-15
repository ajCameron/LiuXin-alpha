"""
Asset derivation and reproducibility facade.
"""

import abc

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationID,
    DigitalAssetDerivationRecord,
    CompositeDigitalAssetID,
    DigitalAssetID,
    ReproductionRecipeArtifactReference,
)


@runtime_checkable
class ReproductionRecipeArtifactResolverAPI(Protocol):
    """
    Verify that an externally referenced pinned artefact is retrievable.

    Example:
        >>> isinstance(resolver, ReproductionRecipeArtifactResolverAPI)  # doctest: +SKIP
        True
    """

    def is_available(
        self,
        reference: ReproductionRecipeArtifactReference,
    ) -> bool:
        """
        Return whether bytes matching ``reference.digest`` are retrievable.

        Example:
            >>> resolver.is_available(reference)  # doctest: +SKIP
            True


        :param reference:
        :return:
        """
        ...


class DigitalAssetDerivationRegistryAPI(abc.ABC):
    """
    Record and query provenance without changing Asset byte identity.

    A derived result is an ordinary atomic Digital Asset. This facade
    records how it was produced and whether it can be recreated exactly.

    Example:
        >>> record = manager.record_digital_asset_derivation(  # doctest: +SKIP
        ...     declaration,
        ... )
    """

    @abc.abstractmethod
    def record_digital_asset_derivation(
        self,
        declaration: DigitalAssetDerivationDeclaration,
    ) -> DigitalAssetDerivationRecord:
        """
        Persist provenance after validating all referenced Assets.

        For an exact recipe, the expected output size and digests must match
        the registered result Asset. Implementations must reject derivation
        cycles and must not infer replayability from a transformation name.

        Example:
            >>> record = manager.record_digital_asset_derivation(  # doctest: +SKIP
            ...     declaration,
            ... )


        :param declaration:
        :return:
        """
        ...

    @abc.abstractmethod
    def get_digital_asset_derivation_record(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
    ) -> DigitalAssetDerivationRecord:
        """
        Return one derivation or raise ``DigitalAssetDerivationNotFound``.

        Example:
            >>> record = manager.get_digital_asset_derivation_record(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(11),
            ... )


        :param digital_asset_derivation_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_digital_asset_derivation_records(
        self,
        *,
        result_digital_asset_id: DigitalAssetID | None = None,
        source_digital_asset_id: DigitalAssetID | None = None,
        source_composite_digital_asset_id: CompositeDigitalAssetID | None = None,
        exact_only: bool = False,
    ) -> Iterator[DigitalAssetDerivationRecord]:
        """
        Iterate over derivations matching provenance or replay filters.

        ``exact_only`` returns only complete recipes that claim byte-identical
        recreation. Other repository and connection failures remain visible.

        Example:
            >>> records = list(manager.iter_digital_asset_derivation_records(  # doctest: +SKIP
            ...     result_digital_asset_id=DigitalAssetID(8), exact_only=True,
            ... ))


        :param result_digital_asset_id:
        :param source_digital_asset_id:
        :param source_composite_digital_asset_id:
        :param exact_only:
        :return:
        """
        ...

    @abc.abstractmethod
    def forget_digital_asset_derivation(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget an erroneous provenance assertion without deleting Assets.

        Derivation records are immutable; correction means replacing an
        erroneous assertion through an explicitly coordinated remove and new
        record, never mutating historical recipe fields in place.

        Example:
            >>> forgotten = manager.forget_digital_asset_derivation(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(11), if_revision="v1",
            ... )


        :param digital_asset_derivation_id:
        :param if_revision:
        :return:
        """
        ...


__all__ = [
    "DigitalAssetDerivationRegistryAPI",
    "ReproductionRecipeArtifactResolverAPI",
]
