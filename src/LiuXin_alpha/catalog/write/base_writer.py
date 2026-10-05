"""
Define shared catalog writer adaptation, validation and application flow.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise base writer through its owning regression module::

        python -m pytest -q tests/catalog/test_writer_factory.py
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from LiuXin_alpha.catalog.write.host_api import CatalogWriterHostAPI
from LiuXin_alpha.databases.db_types import SrcTableID


class BaseCatalogWriter[UpdateT, ResultT](ABC):
    """
    Coordinate construction and application of one catalog update.

    Example:
        Exercise BaseCatalogWriter through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py
    """

    def __init__(self, catalog: CatalogWriterHostAPI) -> None:
        """
        Store the catalog dependency used by the concrete writer.

        Example:
            Exercise BaseCatalogWriter.init through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param catalog: Catalog host or facade supplying metadata and mutation services.
        :return: None; the function records state or raises through its assertions.
        """

        self._catalog = catalog

    @property
    def catalog(self) -> CatalogWriterHostAPI:
        """
        Return the catalog facade used by this writer.

        Example:
            Exercise BaseCatalogWriter.catalog through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._catalog

    @abstractmethod
    def build_update(self, *args: Any, **kwargs: Any) -> UpdateT:
        """
        Normalize caller intent into the concrete writer's update type.

        Example:
            Exercise BaseCatalogWriter.build update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param args: Value supplied for args under the catalog contract.
        :param kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise BaseCatalogWriter.build one update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param src_id: Value supplied for src id under the catalog contract.
        :param dst_value: Value supplied for dst value under the catalog contract.
        :param kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        raise NotImplementedError

    @abstractmethod
    def apply_update(self, update: UpdateT) -> ResultT:
        """
        Apply one normalized update through the catalog boundary.

        Example:
            Exercise BaseCatalogWriter.apply update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """

        raise NotImplementedError

    def write(self, *args: Any, **kwargs: Any) -> ResultT:
        """
        Build and apply exactly one update.

        Example:
            Exercise BaseCatalogWriter.write through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param args: Value supplied for args under the catalog contract.
        :param kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise BaseCatalogWriter.write one through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param src_id: Value supplied for src id under the catalog contract.
        :param dst_value: Value supplied for dst value under the catalog contract.
        :param kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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

    Example:
        Exercise CatalogValueWriter through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py
    """

    def build_one_update(
        self,
        src_id: SrcTableID,
        dst_value: RawValueT,
        **kwargs: Any,
    ) -> UpdateT:
        """
        Build the normal update from one source/value pair.

        Example:
            Exercise CatalogValueWriter.build one update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param src_id: Value supplied for src id under the catalog contract.
        :param dst_value: Value supplied for dst value under the catalog contract.
        :param kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return self.build_update({src_id: dst_value}, **kwargs)

    @abstractmethod
    def adapt(self, raw_value: RawValueT) -> ValueT:
        """
        Convert one caller value into the field's domain representation.

        Example:
            Exercise CatalogValueWriter.adapt through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        raise NotImplementedError

    def validate(self, value: ValueT) -> None:
        """
        Validate one adapted field value.

        Example:
            Exercise CatalogValueWriter.validate through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """

    def prepare_value(self, raw_value: RawValueT) -> ValueT:
        """
        Adapt and validate one caller value in the stable order.

        Example:
            Exercise CatalogValueWriter.prepare value through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        value = self.adapt(raw_value)
        self.validate(value)
        return value


__all__ = ["BaseCatalogWriter", "CatalogValueWriter"]
