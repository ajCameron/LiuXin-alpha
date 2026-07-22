"""
Inheritance-oriented foundations for catalog writers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.databases.db_types import SrcTableID

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api import CatalogAPI


class BaseCatalogWriter[UpdateT, ResultT](ABC):
    """
    Coordinate construction and application of one catalog update.

    This base owns only the stable writer lifecycle. Storage-specific
    subclasses decide what an update contains and which catalog operation
    applies it. Consequently the same base can support a column on the source
    row, a value stored in another table, or any link-table cardinality.

    Subclasses should implement :meth:`build_update`,
    :meth:`build_one_update`, and :meth:`apply_update`. They should not
    override :meth:`write` or :meth:`write_one` merely to reproduce those
    lifecycles.

    :param catalog: Catalog facade through which the update is applied.
    """

    def __init__(self, catalog: CatalogAPI) -> None:
        """
        Store the catalog dependency used by the concrete writer.

        :param catalog: Catalog facade through which the update is applied.
        :return: None.
        """

        self._catalog = catalog

    @property
    def catalog(self) -> CatalogAPI:
        """
        Return the catalog facade used by this writer.

        :return: Configured catalog facade.
        """

        return self._catalog

    @abstractmethod
    def build_update(self, *args: Any, **kwargs: Any) -> UpdateT:
        """
        Normalize caller intent into the concrete writer's update type.

        :param args: Positional inputs defined by the concrete writer.
        :param kwargs: Keyword inputs defined by the concrete writer.
        :return: Storage-specific update ready for application.
        """

        raise NotImplementedError

    @abstractmethod
    def build_one_update(
        self,
        src_id: SrcTableID,
        dst_value: Any,
        **kwargs: Any,
    ) -> UpdateT:
        """
        Normalize one source-to-destination instruction.

        Storage-specific subclasses decide how a single pair is represented
        in their normal update type. Implementations must preserve the same
        semantics as passing a one-entry mapping to :meth:`build_update`.

        :param src_id: Source-table ID to update.
        :param dst_value: One raw destination value or unlink instruction.
        :param kwargs: Additional options accepted by :meth:`build_update`.
        :return: Storage-specific update ready for application.
        """

        raise NotImplementedError

    @abstractmethod
    def apply_update(self, update: UpdateT) -> ResultT:
        """
        Apply one normalized update through the catalog boundary.

        :param update: Storage-specific update built by this writer.
        :return: Concrete writer result.
        """

        raise NotImplementedError

    def write(self, *args: Any, **kwargs: Any) -> ResultT:
        """
        Build and apply exactly one update.

        Database failures are deliberately allowed to propagate. The base
        neither retries a partial operation nor performs cache reconciliation.

        :param args: Positional inputs accepted by :meth:`build_update`.
        :param kwargs: Keyword inputs accepted by :meth:`build_update`.
        :return: Result returned by :meth:`apply_update`.
        """

        return self.apply_update(self.build_update(*args, **kwargs))

    def write_one(
        self,
        src_id: SrcTableID,
        dst_value: Any,
        **kwargs: Any,
    ) -> ResultT:
        """
        Build and apply one source-to-destination instruction.

        This is the single-pair form of :meth:`write`; it returns the normal
        writer result without unwrapping its source-keyed mapping. Database
        failures propagate without retries or partial fallback writes.

        :param src_id: Source-table ID to update.
        :param dst_value: One raw destination value or unlink instruction.
        :param kwargs: Additional options accepted by
            :meth:`build_one_update`.
        :return: Result returned by :meth:`apply_update`.
        """

        return self.apply_update(
            self.build_one_update(src_id, dst_value, **kwargs)
        )


class CatalogValueWriter[
    RawValueT,
    ValueT,
    UpdateT,
    ResultT,
](BaseCatalogWriter[UpdateT, ResultT], ABC):
    """
    Add reusable metadata value preparation to a catalog writer.

    Same-table scalar writers, other-table one-to-one writers, and link
    writers can all inherit this layer without inheriting one another's
    persistence behavior.
    """

    def build_one_update(
        self,
        src_id: SrcTableID,
        dst_value: RawValueT,
        **kwargs: Any,
    ) -> UpdateT:
        """
        Build the normal update from one source/value pair.

        The one-pair form deliberately has the same meaning as passing
        ``{src_id: dst_value}`` as the first argument to :meth:`build_update`.

        :param src_id: Source-table ID to update.
        :param dst_value: Raw destination value supplied by the caller.
        :param kwargs: Additional options accepted by :meth:`build_update`.
        :return: Storage-specific update ready for application.
        """

        return self.build_update({src_id: dst_value}, **kwargs)

    @abstractmethod
    def adapt(self, raw_value: RawValueT) -> ValueT:
        """
        Convert one caller value into the field's domain representation.

        :param raw_value: Raw metadata value supplied by the caller.
        :return: Adapted field value.
        """

        raise NotImplementedError

    def validate(self, value: ValueT) -> None:
        """
        Validate one adapted field value.

        The default accepts every value. Concrete field writers may raise a
        domain-specific exception to reject a value before persistence.

        :param value: Adapted field value.
        :return: None.
        """

    def prepare_value(self, raw_value: RawValueT) -> ValueT:
        """
        Adapt and validate one caller value in the stable order.

        :param raw_value: Raw metadata value supplied by the caller.
        :return: Adapted and validated field value.
        """

        value = self.adapt(raw_value)
        self.validate(value)
        return value


__all__ = ["BaseCatalogWriter", "CatalogValueWriter"]
