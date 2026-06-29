"""
Custom types which are used in the db.

May be superseded by a global typing module in utils later.
"""


from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable, Iterator, Sequence
from types import TracebackType
from typing import Any, Protocol, Self

from enum import Enum, StrEnum

from typing import Final, Optional, Any, Literal, Union

try:
    from typing_extensions import TypedDict, NotRequired
except ImportError:
    from typing import TypedDict, NotRequired

TriStateBool = Optional[bool]

# Keyed with the book_id and valued with a list of the languages for that book - or None
LangMap = dict[int, Optional[list[str]]]

# Table classification types
# - Main
MainTableID = int
MainTableName = str
MainTableColumnName = str

# - Interlink
InterlinkTableID = int
InterLinkTableName = str
InterlinkTableColumnName = str

# - Intralink
IntraLinkTableID = int
IntraLinkTableName = str
IntraLinkTableColumnName = str

# - Helper
HelperTableID = int
HelperTableName = str
HelperTableColumnName = str


TableColumnName = str


# Fields are a mapping between two tables - each of these tables has IDs
# - The "main" table the field is in
SrcTableID = MainTableID
# - The "secondary" table that the main table is linked to
DstTableID = MainTableID



# Some of the specific tables MUST return, for some of their functions, an id in a specific table
# (e.g. The "covers" tables).
# These classes represent ids in these tables
CoverID = MainTableID



AgentID = int

# e.g. for the "Tags" field - which is a ManyToManyField
# - The "main" table for the field should be "titles"
# - The "secondary" table for the field should be "tags"

# In calibre - eerything is centered around the books view - so the "main" table is always


class CreatorDataDict(TypedDict):
    """
    Creator data - data about creators.
    """

    name: str
    sort: str
    link: str


# When you are specifying which format you want - either to add to a book or to get from a book - you have two
# options.
#  - Specific format - something like "EPUB_1" or something like that - a format and it's priority
SpecificFormat = str
# - Generic format - something like "EPUB" or something like that
#                    Depending on context, will return either the highest priority format of that type or, in some way
#                    all formats of that type.
GenericFormat = str


# Used to inform interfaces how to display imformation in a table
MetadataDisplayDict = dict[Any, Any]


class MetadataDict(TypedDict):
    """
    Creator data - data about creators.
    """

    table: Optional[str]
    column: NotRequired[Optional[str]]  # Not needed for virtual tables
    link_column: NotRequired[str]
    datatype: str
    is_multiple: NotRequired[dict[Any, Any]]  # Not needed for virtual tables
    kind: NotRequired[str]
    name: NotRequired[str]
    search_terms: NotRequired[list[str, ...]]
    is_custom: NotRequired[bool]
    is_category: NotRequired[bool]
    is_csp: NotRequired[bool]
    display: NotRequired[MetadataDisplayDict]
    val_unique: NotRequired[bool]

    # Used in composite columns
    contains_html: NotRequired[bool]
    make_category: NotRequired[bool]
    composite_sort: NotRequired[bool]
    use_decorations: NotRequired[bool]


DataTypes = Literal["json", "text"]


# Todo: How do we properly do type hints - a protocol?
class DataTypesEnum(Enum):
    """
    Valid enums for the database.
    """

    JSON: str = "json"
    TEXT: str = "text"


TableTypes = Literal[0, 1, 2, 3]


class TableTypesEnum(Enum):
    """
    Valid and recognized table types.
    """

    ONE_ONE: int = 0
    MANY_ONE: int = 1
    MANY_MANY: int = 2
    ONE_MANY: int = 3


ONE_ONE = TableTypesEnum.ONE_ONE.value
MANY_ONE = TableTypesEnum.MANY_ONE.value
MANY_MANY = TableTypesEnum.MANY_MANY.value
ONE_MANY = TableTypesEnum.ONE_MANY.value


UUIDStr = str


IdentifiersStr = str


IdentifierEntityTypeStr = Literal["work", "expression", "manifestation", "item", "agent"]


# Todo: We need to type this.
class IdentifierEntityType(StrEnum):
    """Supported curated-identifier attachment targets in the FRBR graph."""

    WORK = "work"
    EXPRESSION = "expression"
    MANIFESTATION = "manifestation"
    ITEM = "item"
    AGENT = "agent"


IdentifierSchemeStr = Literal[
    "isbn_10",
    "isbn_13",
    "isbn10",
    "isbn13",
    "asin",
    "uuid",
    "calibre_uuid",
    "doi",
    "oclc",
    "uri",
    "urn",
    "handle",
    "asset-id",
    "archive-id",
    "local-call",
    "barcode",
    "vendor",
    "uuid-ish",
    "shortcode",
    "url",
    "wikipedia_url",
    "imdb_id",
    "publisher_phash",
]


class IdentifierScheme(StrEnum):
    """Canonical identifier scheme names for the FRBR identifier tables."""

    ISBN_10 = "isbn_10"
    ISBN_13 = "isbn_13"
    ISBN10 = "isbn10"
    ISBN13 = "isbn13"
    ASIN = "asin"
    UUID = "uuid"
    CALIBRE_UUID = "calibre_uuid"
    DOI = "doi"
    OCLC = "oclc"
    URI = "uri"
    URN = "urn"
    HANDLE = "handle"
    ASSET_ID = "asset-id"
    ARCHIVE_ID = "archive-id"
    LOCAL_CALL = "local-call"
    BARCODE = "barcode"
    VENDOR = "vendor"
    UUID_ISH = "uuid-ish"
    SHORTCODE = "shortcode"
    URL = "url"
    WIKIPEDIA_URL = "wikipedia_url"
    IMDB_ID = "imdb_id"
    PUBLISHER_PHASH = "publisher_phash"


ALL_IDENTIFIER_ENTITY_TYPES: Final[tuple[str, ...]] = tuple(
    entity_type.value for entity_type in IdentifierEntityType
)

ALL_IDENTIFIER_SCHEMES: Final[tuple[str, ...]] = tuple(
    scheme.value for scheme in IdentifierScheme
)


WORK_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.ISBN_10,
    IdentifierScheme.ISBN_13,
    IdentifierScheme.ISBN10,
    IdentifierScheme.ISBN13,
    IdentifierScheme.UUID,
    IdentifierScheme.CALIBRE_UUID,
    IdentifierScheme.DOI,
    IdentifierScheme.OCLC,
})

EXPRESSION_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.UUID,
    IdentifierScheme.CALIBRE_UUID,
    IdentifierScheme.URI,
    IdentifierScheme.URN,
})

MANIFESTATION_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.ISBN_10,
    IdentifierScheme.ISBN_13,
    IdentifierScheme.ISBN10,
    IdentifierScheme.ISBN13,
    IdentifierScheme.ASIN,
    IdentifierScheme.UUID,
    IdentifierScheme.CALIBRE_UUID,
    IdentifierScheme.OCLC,
    IdentifierScheme.HANDLE,
    IdentifierScheme.LOCAL_CALL,
})

ITEM_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.UUID,
    IdentifierScheme.CALIBRE_UUID,
    IdentifierScheme.ASSET_ID,
    IdentifierScheme.ARCHIVE_ID,
})

AGENT_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.URL,
    IdentifierScheme.WIKIPEDIA_URL,
    IdentifierScheme.IMDB_ID,
    IdentifierScheme.PUBLISHER_PHASH,
})

# Item-observed identifiers are intentionally broader than curated item identifiers.
# A specific copy may physically carry manifestation-level identifiers such as ISBNs/ASINs.
OBSERVED_ITEM_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset(
    IdentifierScheme
)

ENTITY_IDENTIFIER_SCHEMES_BY_TYPE: Final[dict[IdentifierEntityType, frozenset[IdentifierScheme]]] = {
    IdentifierEntityType.WORK: WORK_IDENTIFIER_SCHEMES,
    IdentifierEntityType.EXPRESSION: EXPRESSION_IDENTIFIER_SCHEMES,
    IdentifierEntityType.MANIFESTATION: MANIFESTATION_IDENTIFIER_SCHEMES,
    IdentifierEntityType.ITEM: ITEM_IDENTIFIER_SCHEMES,
    IdentifierEntityType.AGENT: AGENT_IDENTIFIER_SCHEMES,
}


ValidLinkAttributes = Literal["index", "datestamp", "sequence_number", "is_required"]


# Ratings are normalized to one of these values
RatingInt = Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


# Todo: Translated string - string which can be translated - stores original value as well

InterlinkExtraTypes = Union[
    Literal["priority"],
    Literal["primary"],
    Literal["type"],
    Literal["origin"],
    Literal["policy"],
    Literal["data"],
    Literal["index"],
    Literal["sequence_number"],
    Literal["is_required"]]


MarcRelatorRoleStr = Literal[
    "abr",
    "act",
    "adp",
    "ann",
    "arr",
    "art",
    "auc",
    "aui",
    "aus",
    "aut",
    "bnd",
    "com",
    "cmp",
    "cre",
    "ctb",
    "ctr",
    "cur",
    "dpt",
    "drt",
    "edt",
    "fmo",
    "ill",
    "ins",
    "itr",
    "ive",
    "ivr",
    "lyr",
    "nrt",
    "own",
    "pbl",
    "prf",
    "prt",
    "red",
    "res",
    "rev",
    "spk",
    "ths",
    "trl",
]


class MarcRelatorRole(StrEnum):
    """Small curated MARC relator code set for agent links."""

    ABRIDGER = "abr"
    ACTOR = "act"
    ADAPTER = "adp"
    ANNOTATOR = "ann"
    ARRANGER = "arr"
    ARTIST = "art"
    AUTHOR_OF_DIALOG = "auc"
    AUTHOR_OF_INTRODUCTION = "aui"
    AUTHOR_OF_SCREENPLAY = "aus"
    AUTHOR = "aut"
    BINDER = "bnd"
    COMPILER = "com"
    COMPOSER = "cmp"
    CREATOR = "cre"
    CONTRIBUTOR = "ctb"
    CONTRACTOR = "ctr"
    CURATOR = "cur"
    DEGREE_SUPERVISOR = "dpt"
    DIRECTOR = "drt"
    EDITOR = "edt"
    FORMER_OWNER = "fmo"
    ILLUSTRATOR = "ill"
    INSCRIBER = "ins"
    INSTRUMENTALIST = "itr"
    INTERVIEWEE = "ive"
    INTERVIEWER = "ivr"
    LYRICIST = "lyr"
    NARRATOR = "nrt"
    OWNER = "own"
    PUBLISHER = "pbl"
    PERFORMER = "prf"
    PRINTER = "prt"
    REDACTOR = "red"
    RESEARCHER = "res"
    REVIEWER = "rev"
    SPEAKER = "spk"
    THESIS_ADVISOR = "ths"
    TRANSLATOR = "trl"


ALL_MARC_RELATOR_ROLES: Final[tuple[str, ...]] = tuple(
    role.value for role in MarcRelatorRole
)

WORK_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset({
    MarcRelatorRole.ABRIDGER,
    MarcRelatorRole.ADAPTER,
    MarcRelatorRole.ARRANGER,
    MarcRelatorRole.ARTIST,
    MarcRelatorRole.AUTHOR,
    MarcRelatorRole.AUTHOR_OF_DIALOG,
    MarcRelatorRole.AUTHOR_OF_INTRODUCTION,
    MarcRelatorRole.AUTHOR_OF_SCREENPLAY,
    MarcRelatorRole.COMPOSER,
    MarcRelatorRole.CREATOR,
    MarcRelatorRole.CONTRIBUTOR,
    MarcRelatorRole.DIRECTOR,
    MarcRelatorRole.EDITOR,
    MarcRelatorRole.ILLUSTRATOR,
    MarcRelatorRole.LYRICIST,
})

EXPRESSION_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset({
    MarcRelatorRole.ABRIDGER,
    MarcRelatorRole.ADAPTER,
    MarcRelatorRole.ANNOTATOR,
    MarcRelatorRole.ARRANGER,
    MarcRelatorRole.AUTHOR,
    MarcRelatorRole.COMPOSER,
    MarcRelatorRole.CONTRIBUTOR,
    MarcRelatorRole.EDITOR,
    MarcRelatorRole.ILLUSTRATOR,
    MarcRelatorRole.NARRATOR,
    MarcRelatorRole.PERFORMER,
    MarcRelatorRole.REDACTOR,
    MarcRelatorRole.TRANSLATOR,
})

MANIFESTATION_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset({
    MarcRelatorRole.CONTRIBUTOR,
    MarcRelatorRole.EDITOR,
    MarcRelatorRole.PUBLISHER,
    MarcRelatorRole.PRINTER,
})

ITEM_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset({
    MarcRelatorRole.ANNOTATOR,
    MarcRelatorRole.BINDER,
    MarcRelatorRole.FORMER_OWNER,
    MarcRelatorRole.INSCRIBER,
    MarcRelatorRole.OWNER,
})

ENTITY_MARC_RELATOR_ROLES_BY_TYPE: Final[
    dict[IdentifierEntityType, frozenset[MarcRelatorRole]]
] = {
    IdentifierEntityType.WORK: WORK_MARC_RELATOR_ROLES,
    IdentifierEntityType.EXPRESSION: EXPRESSION_MARC_RELATOR_ROLES,
    IdentifierEntityType.MANIFESTATION: MANIFESTATION_MARC_RELATOR_ROLES,
    IdentifierEntityType.ITEM: ITEM_MARC_RELATOR_ROLES,
}



SQLiteParams = Sequence[Any] | dict[str, Any]
SQLiteManyParams = Iterable[SQLiteParams]

RowFactory = Callable[[sqlite3.Cursor, tuple[Any, ...]], Any]
TextFactory = Callable[[bytes], Any]
AuthorizerCallback = Callable[
    [int, str | None, str | None, str | None, str | None],
    int,
]
ProgressCallback = Callable[[], int]
TraceCallback = Callable[[str], None]
BackupProgressCallback = Callable[[int, int, int], None]


class SQLiteConnectionProtocol(Protocol):
    """
    Structural protocol for objects compatible with ``sqlite3.Connection``.

    This is intended for wrappers, adapters, mixins, and test doubles that expose
    the practical public API of Python's standard-library SQLite connection.

    It is deliberately structural: an object does not need to inherit from
    ``sqlite3.Connection`` as long as it provides the same attributes and methods.
    """

    # Exception classes exposed on Connection instances.
    Error: type[sqlite3.Error]
    Warning: type[sqlite3.Warning]
    InterfaceError: type[sqlite3.InterfaceError]
    DatabaseError: type[sqlite3.DatabaseError]
    DataError: type[sqlite3.DataError]
    OperationalError: type[sqlite3.OperationalError]
    IntegrityError: type[sqlite3.IntegrityError]
    InternalError: type[sqlite3.InternalError]
    ProgrammingError: type[sqlite3.ProgrammingError]
    NotSupportedError: type[sqlite3.NotSupportedError]

    # Public attributes / properties.
    isolation_level: str | None
    """Current transaction isolation level, or ``None`` for autocommit-style behaviour."""

    row_factory: RowFactory | None
    """Optional callable used to convert result rows returned by cursors."""

    text_factory: TextFactory
    """Callable used to convert SQLite TEXT values into Python objects."""

    total_changes: int
    """Total number of database rows modified, inserted, or deleted since connection open."""

    in_transaction: bool
    """Whether a transaction is currently active on the connection."""

    autocommit: bool | Any
    """
    Autocommit setting for the connection.

    On newer Python versions this may also be ``sqlite3.LEGACY_TRANSACTION_CONTROL``.
    """

    def __enter__(self) -> Self:
        """
        Enter the connection context manager.

        Returns the connection object itself. The context manager commits on
        successful exit and rolls back if an exception is raised.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """
        Exit the connection context manager.

        Commits if no exception occurred; rolls back if an exception occurred.
        Returning false-ish allows any exception to propagate.
        """
        ...

    def close(self) -> None:
        """
        Close the database connection.

        Further operations on the connection should fail after this is called.
        """
        ...

    def commit(self) -> None:
        """
        Commit the current transaction.

        Has no effect if there is no open transaction.
        """
        ...

    def rollback(self) -> None:
        """
        Roll back the current transaction.

        Has no effect if there is no open transaction.
        """
        ...

    def interrupt(self) -> None:
        """
        Interrupt any currently executing SQLite operation on this connection.

        This is usually called from another thread to abort a long-running query.
        """
        ...

    def cursor(self, factory: type[sqlite3.Cursor] | None = None) -> sqlite3.Cursor:
        """
        Create and return a new cursor object.

        A custom cursor factory may be supplied to override the default cursor type.
        """
        ...

    def execute(
        self,
        sql: str,
        parameters: SQLiteParams = (),
        /,
    ) -> sqlite3.Cursor:
        """
        Execute a single SQL statement and return a cursor for its results.

        This is a convenience shortcut equivalent to creating a cursor and calling
        ``cursor.execute(...)``.
        """
        ...

    def executemany(
        self,
        sql: str,
        parameters: SQLiteManyParams,
        /,
    ) -> sqlite3.Cursor:
        """
        Execute one SQL statement repeatedly using multiple parameter sets.

        Typically used for bulk inserts or updates.
        """
        ...

    def executescript(
        self,
        sql_script: str,
        /,
    ) -> sqlite3.Cursor:
        """
        Execute a script containing one or more SQL statements.

        The script is passed to SQLite as a batch rather than as a single prepared
        statement.
        """
        ...

    def create_function(
        self,
        name: str,
        narg: int,
        func: Callable[..., Any] | None,
        /,
        *,
        deterministic: bool = False,
    ) -> None:
        """
        Register or remove a scalar SQL function.

        Passing ``None`` as ``func`` removes the function. ``narg`` is the number
        of arguments accepted by the SQL function; ``-1`` means variable arity.
        """
        ...

    def create_aggregate(
        self,
        name: str,
        n_arg: int,
        aggregate_class: type[Any] | None,
        /,
    ) -> None:
        """
        Register or remove an aggregate SQL function.

        The aggregate class should provide SQLite-compatible ``step`` and
        ``finalize`` methods. Passing ``None`` removes the aggregate.
        """
        ...

    def create_window_function(
        self,
        name: str,
        num_params: int,
        aggregate_class: type[Any] | None,
        /,
    ) -> None:
        """
        Register or remove an aggregate window SQL function.

        The class should provide SQLite-compatible window aggregate methods such
        as ``step``, ``value``, ``inverse``, and ``finalize``.
        """
        ...

    def create_collation(
        self,
        name: str,
        callback: Callable[[str, str], int] | None,
        /,
    ) -> None:
        """
        Register or remove a custom SQLite collation.

        The callback compares two strings and returns a negative integer, zero,
        or a positive integer, following normal comparison semantics.
        """
        ...

    def set_authorizer(
        self,
        authorizer_callback: AuthorizerCallback | None,
        /,
    ) -> None:
        """
        Set or clear the SQLite authorizer callback.

        The callback is invoked by SQLite when SQL statements attempt operations
        that can be allowed, denied, or ignored.
        """
        ...

    def set_progress_handler(
        self,
        progress_handler: ProgressCallback | None,
        n: int,
        /,
    ) -> None:
        """
        Set or clear the progress handler callback.

        SQLite calls the handler roughly every ``n`` virtual-machine instructions.
        Returning a non-zero value aborts the current query.
        """
        ...

    def set_trace_callback(
        self,
        trace_callback: TraceCallback | None,
        /,
    ) -> None:
        """
        Set or clear a callback invoked for each SQL statement executed.

        Useful for logging, debugging, and lightweight query tracing.
        """
        ...

    def backup(
        self,
        target: sqlite3.Connection,
        *,
        pages: int = -1,
        progress: BackupProgressCallback | None = None,
        name: str = "main",
        sleep: float = 0.25,
    ) -> None:
        """
        Back up this database into another SQLite connection.

        ``pages`` controls how many pages are copied per step. ``name`` selects
        the source database, usually ``"main"``.
        """
        ...

    def iterdump(
        self,
        *,
        filter: str | None = None,
    ) -> Iterator[str]:
        """
        Return an iterator over SQL text that can recreate the database.

        The optional filter restricts dumped objects by name on Python versions
        that support it.
        """
        ...

    def serialize(
        self,
        /,
        *,
        name: str = "main",
    ) -> bytes:
        """
        Serialize a database into a bytes object.

        ``name`` selects the database to serialize, usually ``"main"``.
        """
        ...

    def deserialize(
        self,
        data: bytes,
        /,
        *,
        name: str = "main",
    ) -> None:
        """
        Replace a database with the contents of a serialized SQLite database.

        ``name`` selects the database to replace, usually ``"main"``.
        """
        ...

    def blobopen(
        self,
        table: str,
        column: str,
        row: int,
        /,
        *,
        readonly: bool = False,
        name: str = "main",
    ) -> sqlite3.Blob:
        """
        Open a BLOB column for incremental I/O.

        ``table``, ``column``, and ``row`` identify the BLOB value. Set
        ``readonly`` to prevent writes.
        """
        ...

    def enable_load_extension(
        self,
        enable: bool,
        /,
    ) -> None:
        """
        Enable or disable loading SQLite extensions.

        Extension loading is disabled by default in many environments for
        security reasons.
        """
        ...

    def load_extension(
        self,
        name: str,
        /,
        *,
        entrypoint: str | None = None,
    ) -> None:
        """
        Load a SQLite extension library.

        ``entrypoint`` may be supplied on Python versions that support explicit
        extension entry points.
        """
        ...

    def getlimit(
        self,
        category: int,
        /,
    ) -> int:
        """
        Return the current SQLite runtime limit for a limit category.

        Categories are SQLite limit constants such as ``sqlite3.SQLITE_LIMIT_SQL_LENGTH``.
        """
        ...

    def setlimit(
        self,
        category: int,
        limit: int,
        /,
    ) -> int:
        """
        Set a SQLite runtime limit and return the previous value.

        SQLite silently truncates values above its hard upper bound.
        """
        ...

    def getconfig(
        self,
        op: int,
        /,
    ) -> bool:
        """
        Return the current boolean state of a SQLite database configuration option.

        Available only on Python versions that expose ``Connection.getconfig``.
        """
        ...

    def setconfig(
        self,
        op: int,
        enable: bool = True,
        /,
    ) -> None:
        """
        Set a SQLite database configuration option.

        Available only on Python versions that expose ``Connection.setconfig``.
        """
        ...

# ---------------------------------------------------------------------------
# Narrow SQLite-ish support protocols
# ---------------------------------------------------------------------------

class SupportsCursor(Protocol):
    """Provides cursor creation."""

    def cursor(self, factory: type[sqlite3.Cursor] | None = None) -> sqlite3.Cursor:
        """Create and return a new SQLite cursor."""
        ...


class SupportsExecute(Protocol):
    """Provides single-statement SQL execution."""

    def execute(
        self,
        sql: str,
        parameters: SQLiteParams = (),
        /,
    ) -> sqlite3.Cursor:
        """Execute a single SQL statement and return a cursor."""
        ...


class SupportsExecutemany(Protocol):
    """Provides repeated execution of one SQL statement."""

    def executemany(
        self,
        sql: str,
        parameters: SQLiteManyParams,
        /,
    ) -> sqlite3.Cursor:
        """Execute one SQL statement against multiple parameter sets."""
        ...


class SupportsExecutescript(Protocol):
    """Provides SQL script execution."""

    def executescript(
        self,
        sql_script: str,
        /,
    ) -> sqlite3.Cursor:
        """Execute a script containing one or more SQL statements."""
        ...


class SupportsSQLExecution(
    SupportsExecute,
    SupportsExecutemany,
    SupportsExecutescript,
    Protocol,
):
    """Provides the common connection-level SQL execution shortcuts."""


class SupportsTransactions(Protocol):
    """Provides explicit transaction control."""

    in_transaction: bool
    """Whether a transaction is currently active."""

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Roll back the current transaction."""
        ...


class SupportsConnectionLifecycle(Protocol):
    """Provides basic connection lifecycle operations."""

    def close(self) -> None:
        """Close the connection."""
        ...

    def interrupt(self) -> None:
        """Interrupt any currently executing operation on the connection."""
        ...


class SupportsConnectionContext(Protocol):
    """Provides sqlite3-style connection context manager behaviour."""

    def __enter__(self) -> Self:
        """Enter the connection context manager and return the connection."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Exit the connection context manager."""
        ...


class SupportsRowFactory(Protocol):
    """Provides row factory configuration."""

    row_factory: RowFactory | None
    """Callable used to transform rows returned by cursors."""


class SupportsTextFactory(Protocol):
    """Provides text factory configuration."""

    text_factory: TextFactory
    """Callable used to convert SQLite TEXT values."""


class SupportsSQLiteState(Protocol):
    """Provides common read-only SQLite connection state."""

    total_changes: int
    """Total number of changed rows since the connection was opened."""

    in_transaction: bool
    """Whether a transaction is currently active."""


class SupportsFunctionRegistration(Protocol):
    """Provides registration of custom SQL functions."""

    def create_function(
        self,
        name: str,
        narg: int,
        func: Callable[..., Any] | None,
        /,
        *,
        deterministic: bool = False,
    ) -> None:
        """Register or remove a scalar SQL function."""
        ...

    def create_aggregate(
        self,
        name: str,
        n_arg: int,
        aggregate_class: type[Any] | None,
        /,
    ) -> None:
        """Register or remove an aggregate SQL function."""
        ...

    def create_window_function(
        self,
        name: str,
        num_params: int,
        aggregate_class: type[Any] | None,
        /,
    ) -> None:
        """Register or remove an aggregate window SQL function."""
        ...

    def create_collation(
        self,
        name: str,
        callback: Callable[[str, str], int] | None,
        /,
    ) -> None:
        """Register or remove a custom SQLite collation."""
        ...


class SupportsSQLiteHooks(Protocol):
    """Provides SQLite callback hook registration."""

    def set_authorizer(
        self,
        authorizer_callback: AuthorizerCallback | None,
        /,
    ) -> None:
        """Set or clear the SQLite authorizer callback."""
        ...

    def set_progress_handler(
        self,
        progress_handler: ProgressCallback | None,
        n: int,
        /,
    ) -> None:
        """Set or clear the SQLite progress handler."""
        ...

    def set_trace_callback(
        self,
        trace_callback: TraceCallback | None,
        /,
    ) -> None:
        """Set or clear the SQLite trace callback."""
        ...


class SupportsBackup(Protocol):
    """Provides SQLite online backup support."""

    def backup(
        self,
        target: sqlite3.Connection,
        *,
        pages: int = -1,
        progress: BackupProgressCallback | None = None,
        name: str = "main",
        sleep: float = 0.25,
    ) -> None:
        """Back up this database into another SQLite connection."""
        ...


class SupportsIterdump(Protocol):
    """Provides SQL dump generation."""

    def iterdump(
        self,
        *,
        filter: str | None = None,
    ) -> Iterator[str]:
        """Yield SQL statements that can recreate the database."""
        ...


class SupportsSerialization(Protocol):
    """Provides SQLite database serialization and deserialization."""

    def serialize(
        self,
        /,
        *,
        name: str = "main",
    ) -> bytes:
        """Serialize a database to bytes."""
        ...

    def deserialize(
        self,
        data: bytes,
        /,
        *,
        name: str = "main",
    ) -> None:
        """Replace a database with serialized SQLite database bytes."""
        ...


class SupportsBlobOpen(Protocol):
    """Provides incremental BLOB I/O."""

    def blobopen(
        self,
        table: str,
        column: str,
        row: int,
        /,
        *,
        readonly: bool = False,
        name: str = "main",
    ) -> sqlite3.Blob:
        """Open a BLOB column for incremental reading or writing."""
        ...


class SupportsExtensionLoading(Protocol):
    """Provides SQLite extension loading controls."""

    def enable_load_extension(
        self,
        enable: bool,
        /,
    ) -> None:
        """Enable or disable SQLite extension loading."""
        ...

    def load_extension(
        self,
        name: str,
        /,
        *,
        entrypoint: str | None = None,
    ) -> None:
        """Load a SQLite extension library."""
        ...


class SupportsSQLiteLimits(Protocol):
    """Provides SQLite runtime limit access."""

    def getlimit(
        self,
        category: int,
        /,
    ) -> int:
        """Return the current SQLite runtime limit for a category."""
        ...

    def setlimit(
        self,
        category: int,
        limit: int,
        /,
    ) -> int:
        """Set a SQLite runtime limit and return the previous value."""
        ...


class SupportsSQLiteConfig(Protocol):
    """Provides SQLite database configuration access."""

    def getconfig(
        self,
        op: int,
        /,
    ) -> bool:
        """Return the state of a SQLite database configuration option."""
        ...

    def setconfig(
        self,
        op: int,
        enable: bool = True,
        /,
    ) -> None:
        """Set a SQLite database configuration option."""
        ...


class SupportsSQLiteCore(
    SupportsCursor,
    SupportsSQLExecution,
    SupportsTransactions,
    SupportsConnectionLifecycle,
    SupportsConnectionContext,
    SupportsRowFactory,
    SupportsTextFactory,
    SupportsSQLiteState,
    Protocol,
):
    """
    Provides the core sqlite3.Connection surface most wrappers actually need.

    This deliberately excludes backup, extension loading, custom SQL functions,
    serialization, BLOB I/O, hooks, limits, and low-level config.
    """


class SupportsFullSQLiteConnection(
    SupportsSQLiteCore,
    SupportsFunctionRegistration,
    SupportsSQLiteHooks,
    SupportsBackup,
    SupportsIterdump,
    SupportsSerialization,
    SupportsBlobOpen,
    SupportsExtensionLoading,
    SupportsSQLiteLimits,
    SupportsSQLiteConfig,
    Protocol,
):
    """
    Provides the broad sqlite3.Connection-like surface.

    Prefer narrower protocols for mixins unless the code genuinely needs the
    whole beast.
    """