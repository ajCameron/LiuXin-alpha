"""
Resolve table values into destination rows before linking catalog records.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise table value link writer through its owning regression module::

        python -m pytest -q tests/catalog/test_link_writer.py
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from typing import Any, cast

from LiuXin_alpha.catalog.write.host_api import CatalogWriterHostAPI
from LiuXin_alpha.catalog.write.link_update import LinkUpdate
from LiuXin_alpha.catalog.write.link_writer import (
    CatalogLinkMap,
    CatalogLinkTypeScope,
    CatalogLinkWriter,
)
from LiuXin_alpha.databases.db_types import DstTableID, SrcTableID
from LiuXin_alpha.databases.macro_types import LINK_TYPE_UNSET, LinkRow
from LiuXin_alpha.databases.schema_specs import (
    StorageColumnSpec,
    StorageLinkSpec,
    StorageTableSpec,
)

@dataclass(frozen=True, slots=True)
class _TableValueReference:
    """
    Hashable, non-persistent reference to one raw destination value.

    Example:
        Exercise TableValueReference through its owning regression module::

            python -m pytest -q tests/catalog/test_link_writer.py
    """

    identity: tuple[str, str, str]
    value: Any = field(compare=False, hash=False, repr=False)

    @classmethod
    def from_value(cls, value: Any) -> "_TableValueReference":
        """
        Construct a canonical link update from value.

        Example:
            Exercise TableValueReference.from value through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
        value_type = type(value)
        return cls(
            (
                value_type.__module__,
                value_type.__qualname__,
                repr(value),
            ),
            value,
        )


class CatalogTableValueLinkWriter(CatalogLinkWriter[Any, Any]):
    """
    Link source rows to values ensured in a destination-table column.

    Example:
        Exercise CatalogTableValueLinkWriter through its owning regression module::

            python -m pytest -q tests/catalog/test_link_writer.py
    """

    def __init__(
        self,
        catalog: CatalogWriterHostAPI,
        link_spec: StorageLinkSpec,
        destination_table: StorageTableSpec,
        destination_column: StorageColumnSpec,
    ) -> None:
        """
        Validate and store the schema-backed destination configuration.

        Example:
            Exercise CatalogTableValueLinkWriter.init through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param catalog: Catalog host or facade supplying metadata and mutation services.
        :param link_spec: Value supplied for link spec under the catalog contract.
        :param destination_table: Value supplied for destination table under the catalog
            contract.
        :param destination_column: Value supplied for destination column under the catalog
            contract.
        :return: None; the function records state or raises through its assertions.
        """

        if not isinstance(destination_table, StorageTableSpec):
            raise TypeError("destination_table must be a StorageTableSpec")
        if not isinstance(destination_column, StorageColumnSpec):
            raise TypeError("destination_column must be a StorageColumnSpec")
        if destination_table.name != link_spec.secondary_table:
            raise ValueError(
                "destination_table must match link_spec.secondary_table"
            )
        if destination_column not in destination_table.columns:
            raise ValueError("destination_column must belong to destination_table")
        if destination_column.is_primary_key:
            raise ValueError("destination value column cannot be a primary key")
        super().__init__(catalog, link_spec)
        self._destination_table = destination_table
        self._destination_column = destination_column

    @property
    def destination_table(self) -> StorageTableSpec:
        """
        Return the destination-table specification.

        Example:
            Exercise CatalogTableValueLinkWriter.destination table through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._destination_table

    @property
    def destination_column(self) -> StorageColumnSpec:
        """
        Return the destination value-column specification.

        Example:
            Exercise CatalogTableValueLinkWriter.destination column through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._destination_column

    def adapt(self, raw_value: Any) -> Any:
        """
        Preserve one raw destination value by default.

        Example:
            Exercise CatalogTableValueLinkWriter.adapt through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return raw_value

    def resolve_destination(self, value: Any) -> DstTableID:
        """
        Match or create one destination value through portable macros.

        Example:
            Exercise CatalogTableValueLinkWriter.resolve destination through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """

        return self.catalog.db.macros.ensure_table_value(
            self.destination_table.name,
            self.destination_column.name,
            value,
            id_column=self.link_spec.secondary_id_col,
        )

    def find_destination(self, value: Any) -> DstTableID | None:
        """
        Find one destination value without creating a database row.

        Example:
            Exercise CatalogTableValueLinkWriter.find destination through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """

        return self.catalog.db.macros.find_table_value(
            self.destination_table.name,
            self.destination_column.name,
            value,
            id_column=self.link_spec.secondary_id_col,
        )

    def _existing_destination_id_for(self, raw_value: Any) -> DstTableID:
        """
        Resolve a deletion value while retaining a missing-value sentinel.

        Example:
            Exercise CatalogTableValueLinkWriter.existing destination id for through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return cast(
            DstTableID,
            self.find_destination(self.prepare_value(raw_value)),
        )

    def _reference_for(self, raw_value: Any) -> _TableValueReference:
        """
        Adapt a value into a pure, lazily resolved reference.

        Example:
            Exercise CatalogTableValueLinkWriter.reference for through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return _TableValueReference.from_value(self.prepare_value(raw_value))

    def build_update(
        self,
        replacements: CatalogLinkMap[Any] | None = None,
        *,
        additions: CatalogLinkMap[Any] | None = None,
        deletions: CatalogLinkMap[Any] | None = None,
        link_type: CatalogLinkTypeScope = LINK_TYPE_UNSET,
    ) -> LinkUpdate:
        """
        Build an update with operation-aware destination-value resolution.

        Example:
            Exercise CatalogTableValueLinkWriter.build update through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param replacements: Value supplied for replacements under the catalog contract.
        :param additions: Value supplied for additions under the catalog contract.
        :param deletions: Value supplied for deletions under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :return: The deterministic value, row, identity or collection described above.
        """

        replacements, additions, deletions = self._validate_link_type_inputs(
            replacements,
            additions=additions,
            deletions=deletions,
            link_type=link_type,
        )
        update = LinkUpdate.from_values(
            self.link_spec,
            replacements,
            additions=additions,
            deletions=deletions,
            secondary_id_for=self._reference_for,
            link_type=link_type,
        )
        self._validate_cardinality(update)
        return update

    def _resolve_update(self, update: LinkUpdate) -> LinkUpdate:
        """
        Resolve raw value references inside the active write transaction.

        Example:
            Exercise CatalogTableValueLinkWriter.resolve update through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """

        resolved: dict[_TableValueReference, DstTableID] = {}

        def operation(
            links_by_source: Any,
            *,
            create: bool,
        ) -> dict[Any, tuple[Any, ...]]:
            """
            Perform the catalog operation operation under explicit validation and ordering rules.

            Example:
                Exercise CatalogTableValueLinkWriter.resolve update.operation through its owning regression module::

                    python -m pytest -q tests/catalog/test_link_writer.py


            :param links_by_source: Value supplied for links by source under the catalog
                contract.
            :param create: Value supplied for create under the catalog contract.
            :return: The deterministic value, row, identity or collection described above.
            """
            result: dict[Any, tuple[Any, ...]] = {}
            for source_id, links in links_by_source.items():
                resolved_links = []
                for link in links:
                    reference = link.secondary_id
                    if not isinstance(reference, _TableValueReference):
                        resolved_links.append(link)
                        continue
                    destination_id = resolved.get(reference)
                    if destination_id is None:
                        destination_id = (
                            self.resolve_destination(reference.value)
                            if create
                            else self.find_destination(reference.value)
                        )
                        if destination_id is not None:
                            resolved[reference] = destination_id
                    if destination_id is not None:
                        resolved_links.append(
                            replace(link, secondary_id=destination_id)
                        )
                result[source_id] = tuple(resolved_links)
            return result

        return LinkUpdate(
            link_spec=self.link_spec,
            replacements=operation(update.replacements, create=True),
            additions=operation(update.additions, create=True),
            deletions=operation(update.deletions, create=False),
            link_type=update.link_type,
        )

    def apply_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Resolve values and apply links in one portable transaction.

        Example:
            Exercise CatalogTableValueLinkWriter.apply update through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """

        if not isinstance(update, LinkUpdate):
            raise TypeError("update must be a LinkUpdate")
        if update.link_spec != self.link_spec:
            raise ValueError("update link_spec does not match writer link_spec")
        macros = self.catalog.db.macros
        transaction = getattr(macros, "transaction", None)
        context = transaction() if callable(transaction) else nullcontext()
        with context:
            resolved = self._resolve_update(update)
            replacement = resolved.as_replacement_update(macros)
            self._validate_cardinality(replacement)
            return self.catalog.write_link_update(replacement)


__all__ = ["CatalogTableValueLinkWriter"]
