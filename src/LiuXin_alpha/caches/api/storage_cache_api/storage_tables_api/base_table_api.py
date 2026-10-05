
"""
Define cache-table metadata, cardinality values and abstract load/column contracts.

Legacy integer constants run ONE_ONE=0, MANY_ONE=1, MANY_MANY=2 and
ONE_MANY=3; TableTypes wraps those values as Enum members. null is a unique
object sentinel, distinct from None. Table instances retain a borrowed
database reference, name and metadata without loading rows.
"""

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI

ONE_ONE, MANY_ONE, MANY_MANY, ONE_MANY = range(4)

null = object()

from dataclasses import dataclass

from enum import Enum

class TableTypes(Enum):
    """
    Represent relation cardinality with the established integer-valued enum members.

    These are Enum members rather than integer aliases. Use .value when
    interoperating with the module's legacy integer constants; cardinality
    declarations do not validate physical rows by themselves.

    Example:
        >>> TableTypes.ONE_ONE.value == ONE_ONE
        True
    """
    ONE_ONE = ONE_ONE
    MANY_ONE = MANY_ONE
    MANY_MANY = MANY_MANY
    ONE_MANY = ONE_MANY


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """
    Describe a cached table's name and main/interlink/intralink roles.

    Frozen slots metadata requires table_name and main_table; both link-role
    flags default false. Construction does not check name consistency or reject
    conflicting role flags.

    Example:
        >>> TableMetadata("books", main_table=True).is_interlink
        False
    """
    table_name: str

    main_table: bool
    
    is_interlink: bool = False
    is_intralink: bool = False



class StorageCacheBaseTableAPI(abc.ABC):
    """
    Retain common table identity and require loading and column metadata operations.

    The constructor stores its arguments without validation or copying.
    Concrete subclasses implement row loading and column discovery; this base
    does not manage transactions, readiness or database shutdown.

    Example:
        A main-table implementation and a directed link view both inherit this
        base while supplying different row storage and column metadata.
    """

    table: str
    db: "DatabaseAPI"
    metadata: TableMetadata

    def __init__(self, table: str, db: "DatabaseAPI", metadata: TableMetadata) -> None:
        """
        Retain a table name, borrowed database handle and metadata object.

        Example:
            Construction does not reject metadata whose table_name differs from table.


        :param table: Table name stored without normalization.
        :param db: Database reference retained without validation.
        :param metadata: Metadata value stored independently of the table name.
        :return: None; assigns the three references without reading storage.
        """
        self.table = table
        self.db = db
        self.metadata = metadata


    @abc.abstractmethod
    def read(self, db: "DatabaseAPI") -> None:
        """
        Load table state using the concrete backend's database access path.

        Abstract contract; row copying, attachment and error recovery belong to
        the implementation.

        Example:
            A schema main table loads row dictionaries and builds value indexes.


        :param db: Database reference or override interpreted by the backend.
        :return: None; the backend loads its cached table representation.
        """

    @abc.abstractmethod
    def reload(self, db: "DatabaseAPI") -> None:
        """
        Refresh complete table state through the concrete backend.

        Abstract contract. Held child views and field dependencies may require
        separate coordinated refresh by the root cache.

        Example:
            Reload physical link records after associations change externally.


        :param db: Database reference or override interpreted by the backend.
        :return: None; the backend refreshes its table representation.
        """

    # ------------------
    # - TABLE PROPERTIES

    @property
    @abc.abstractmethod
    def column_headings(self) -> list[str]:
        """
        Expose column names supported by this table view.

        Abstract contract. The backend can derive headings from retained schema
        or query its database; this property does not guarantee either strategy.

        Example:
            A main table can expose ["id", "title"] even when it has no rows.


        :return: List of column names in backend-defined order.
        """

    @property
    @abc.abstractmethod
    def column_types(self) -> dict[str, str]:
        """
        Map column names to backend-provided type descriptions.

        Abstract contract. Declared types and fallback spelling are selected
        by the backend; the mapping does not coerce stored values.

        Example:
            A backend can describe an unknown column type as UNKNOWN.


        :return: Dictionary of column names to type-name strings.
        """

