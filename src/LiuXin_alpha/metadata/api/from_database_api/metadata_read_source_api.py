"""
Describe the minimal database-like schema and row-reading surface consumed by metadata hydrators.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise metadata read source api with the owning regression module::

        python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, TypeAlias, runtime_checkable

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MetadataValue,
    SupportsRowMapping,
)


MetadataSearchTerm: TypeAlias = MetadataValue
MetadataTableColumns: TypeAlias = Mapping[str, Sequence[str]]
MetadataRowSequence: TypeAlias = Sequence[SupportsRowMapping]
MetadataLinkRow: TypeAlias = MetadataRecord | SupportsRowMapping
MetadataLinkRowSequence: TypeAlias = Sequence[MetadataLinkRow]


@runtime_checkable
class MetadataDriverWrapperAPI(Protocol):
    """
    Describe schema-name and link-table helpers required by metadata reads.

    Example:
        Exercise MetadataDriverWrapperAPI with the owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
    """

    def get_allowed_tables_snapshot(self) -> Sequence[str]:
        """
        Return the schema tables currently permitted for metadata reads.

        Example:
            Exercise MetadataDriverWrapperAPI.get allowed tables snapshot with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: The normalized row, metadata object or value described above.
        """
        ...

    def identify_table_from_row_dict(self, row_dict: MetadataRecord) -> str:
        """
        Infer the owning metadata table from the keys in a row-shaped mapping.

        Example:
            Exercise MetadataDriverWrapperAPI.identify table from row dict with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param row_dict: Row-shaped metadata mapping used to infer its owning table.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_id_column(self, table: str) -> str:
        """
        Return the primary identifier column for a metadata table.

        Example:
            Exercise MetadataDriverWrapperAPI.get id column with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Readable schema table name.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def check_for_intralink_table(self, table: str) -> bool:
        """
        Return whether a table represents links between rows of the same entity table.

        Example:
            Exercise MetadataDriverWrapperAPI.check for intralink table with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Readable schema table name.
        :return: True when the described condition is satisfied; otherwise False.
        """
        ...

    def get_interlinked_tables(self, table: str) -> Sequence[str]:
        """
        Return entity tables reachable through link tables from the supplied table.

        Example:
            Exercise MetadataDriverWrapperAPI.get interlinked tables with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Readable schema table name.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_link_table_name(self, table1: str, table2: str) -> str | None:
        """
        Return the schema link table joining two entity tables when one exists.

        Example:
            Exercise MetadataDriverWrapperAPI.get link table name with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table1: First entity table participating in a schema link.
        :param table2: Second entity table participating in a schema link.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_column_base(self, table_name: str) -> str:
        """
        Return the schema column prefix associated with a table.

        Example:
            Exercise MetadataDriverWrapperAPI.get column base with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table_name: Readable schema table whose column prefix is requested.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_link_column(
        self,
        table1: str,
        table2: str,
        secondary_id_column: str,
    ) -> str:
        """
        Return the link-table column targeting the requested secondary identifier.

        Example:
            Exercise MetadataDriverWrapperAPI.get link column with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table1: First entity table participating in a schema link.
        :param table2: Second entity table participating in a schema link.
        :param secondary_id_column: Identifier column targeted on the secondary link side.
        :return: The normalized row, metadata object or value described above.
        """
        ...


@runtime_checkable
class MetadataReadSourceAPI(Protocol):
    """
    Describe the narrow row, search and interlink read surface required by hydrators.

    Example:
        Exercise MetadataReadSourceAPI with the owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
    """

    driver_wrapper: MetadataDriverWrapperAPI

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]:
        """
        Return readable table names, optionally refreshing the source's schema snapshot.

        Example:
            Exercise MetadataReadSourceAPI.get tables with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param force_refresh: Refresh cached schema state before returning tables when true.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_tables_and_columns(self) -> MetadataTableColumns:
        """
        Return readable tables mapped to their current column names.

        Example:
            Exercise MetadataReadSourceAPI.get tables and columns with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_column_headings(self, table: str) -> set[str]:
        """
        Return the column-name set for one readable table.

        Example:
            Exercise MetadataReadSourceAPI.get column headings with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Readable schema table name.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_row_from_id(self, table: str, row_id: int) -> SupportsRowMapping | None:
        """
        Return one row by table identifier, or None when no such row exists.

        Example:
            Exercise MetadataReadSourceAPI.get row from id with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Readable schema table name.
        :param row_id: Primary identifier of the requested row.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> MetadataRowSequence:
        """
        Return all rows from a table using the source's sequence contract.

        Example:
            Exercise MetadataReadSourceAPI.get all rows with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Readable schema table name.
        :param iterator_return: Request the source's iterator-compatible row representation.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_record_count(self, table: str) -> int:
        """
        Return the number of rows currently stored in a readable table.

        Example:
            Exercise MetadataReadSourceAPI.get record count with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Readable schema table name.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def search(
        self,
        table: str,
        column: str,
        search_term: MetadataSearchTerm,
    ) -> MetadataRowSequence:
        """
        Return rows whose selected column matches the supplied metadata search term.

        Example:
            Exercise MetadataReadSourceAPI.search with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Readable schema table name.
        :param column: Column to compare during the metadata search.
        :param search_term: Typed metadata value to match.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_interlink_rows(
        self,
        primary_row: SupportsRowMapping,
        secondary_table: str,
    ) -> MetadataLinkRowSequence:
        """
        Return raw link rows connecting a primary row to a secondary entity table.

        Example:
            Exercise MetadataReadSourceAPI.get interlink rows with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param primary_row: Primary entity row whose raw link rows are requested.
        :param secondary_table: Entity table expected on the other side of the relation.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def get_interlinked_rows(
        self,
        target_row: SupportsRowMapping,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> MetadataRowSequence:
        """
        Return secondary entity rows linked to the target row, optionally filtered by relation type.

        Example:
            Exercise MetadataReadSourceAPI.get interlinked rows with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param target_row: Entity row whose linked secondary entities are requested.
        :param secondary_table: Entity table expected on the other side of the relation.
        :param type_filter: Agent role type required for the selected credit.
        :return: The normalized row, metadata object or value described above.
        """
        ...

    def refresh(self) -> bool:
        """
        Refresh source state and report whether the operation succeeded.

        Example:
            Exercise MetadataReadSourceAPI.refresh with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: True when source state was refreshed successfully; otherwise False.
        """
        ...


__all__ = [
    "MetadataDriverWrapperAPI",
    "MetadataLinkRow",
    "MetadataLinkRowSequence",
    "MetadataRowSequence",
    "MetadataReadSourceAPI",
    "MetadataSearchTerm",
    "MetadataTableColumns",
]
