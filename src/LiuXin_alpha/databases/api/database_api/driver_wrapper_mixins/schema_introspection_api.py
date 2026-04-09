# Suggested additions to DatabaseDriverWrapperAPI (or a new SchemaIntrospectionAPI)

import abc
from abc import abstractmethod
from typing import Iterator, Optional

from LiuXin_alpha.databases.schema_specs import (
    StorageSchemaSpec,
    StorageTableSpec,
    StorageLinkSpec,
)


class SchemaIntrospectionAPI(abc.ABC):

    @abstractmethod
    def get_table_spec(self, table: str, force_refresh: bool = False) -> StorageTableSpec:
        """
        Return a dataclass describing one table or view.
        Raises if the relation does not exist.
        """

    @abstractmethod
    def iter_table_specs(
        self,
        *,
        force_refresh: bool = False,
        include_views: bool = True,
    ) -> Iterator[StorageTableSpec]:
        """
        Yield specs for all known tables (and optionally views).
        """

    @abstractmethod
    def get_link_spec(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> Optional[StorageLinkSpec]:
        """
        Return the interlink spec between two tables, or None if no link exists.
        """

    @abstractmethod
    def get_intralink_spec(
        self,
        table: str,
        *,
        force_refresh: bool = False,
    ) -> Optional[StorageLinkSpec]:
        """
        Return the self-link spec for a table, or None if no intralink exists.
        """

    @abstractmethod
    def iter_link_specs(
        self,
        *,
        force_refresh: bool = False,
        include_intralinks: bool = True,
    ) -> Iterator[StorageLinkSpec]:
        """
        Yield all discovered link specs.
        """

    @abstractmethod
    def get_schema_spec(self, force_refresh: bool = False) -> StorageSchemaSpec:
        """
        Return a snapshot of the whole schema as dataclasses.
        """

    @abstractmethod
    def get_row_dataclass(
        self,
        table: str,
        *,
        force_refresh: bool = False,
    ) -> type:
        """
        Build and return a dataclass type for rows in the given table/view.
        """

    @abstractmethod
    def get_link_row_dataclass(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> Optional[type]:
        """
        Build and return a dataclass type for rows in the relevant link table.
        """