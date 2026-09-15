"""
Define legacy database aliases, metadata vocabularies and SQLite capability protocols.

IDs and names are ordinary int/str aliases, and TypedDict records remain ordinary dictionaries at runtime. Identifier and relator enums feed curated attachment-policy collections; observed Item identifiers allow every listed scheme. The SQLite protocols describe broad or narrow host interfaces without constructing connections or checking optional build/version capabilities. Attribute documentation strings and vocabulary constants are retained separately from declaration docs.
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
    Describe a legacy creator mapping with required name, sort and link strings.

    The three keys are required by static typing. Construction produces an ordinary mutable dict and performs no runtime key or value validation.

    Example:
        >>> creator = CreatorDataDict(name="Example", sort="Example", link="")
        >>> creator["name"]
        'Example'
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
    Describe legacy field metadata with required table and datatype entries.

    table may be None for virtual fields. Optional keys describe physical/link columns, multiplicity, labels/search terms, display/category flags and composite behavior. These annotations do not fill defaults or validate dictionary contents.

    Example:
        >>> metadata: MetadataDict = {"table": None, "datatype": "composite"}
        >>> sorted(metadata)
        ['datatype', 'table']
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
    Name the narrow JSON/text payload categories used by this type module.

    This ordinary Enum has string values but its members are not StrEnum values. It does not enumerate every datatype accepted by the wider legacy field system.

    Example:
        >>> DataTypesEnum.JSON.value
        'json'
    """

    JSON: str = "json"
    TEXT: str = "text"


TableTypes = Literal[0, 1, 2, 3]


class TableTypesEnum(Enum):
    """
    Name the four legacy relation-cardinality flags and their integer values.

    ONE_ONE, MANY_ONE, MANY_MANY and ONE_MANY map to 0, 1, 2 and 3. Module-level constants expose those integer values directly; Enum members themselves remain ordinary Enum objects.

    Example:
        >>> (TableTypesEnum.MANY_MANY.value, MANY_MANY)
        (2, 2)
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
    """
    Name the five curated identifier attachment targets as string-compatible members.

    The targets are Work, Expression, Manifestation, Item and Agent. The enum identifies a target category; scheme eligibility is stored in the separate per-target collections.

    Example:
        >>> IdentifierEntityType.WORK == "work"
        True
    """

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
    """
    Name the project identifier schemes while preserving legacy spelling variants.

    Members are string-compatible labels. ISBN spellings with and without underscores remain separate members; this enum neither canonicalizes scheme names nor validates identifier values. Curated and observed eligibility is defined by separate collections.

    Example:
        >>> IdentifierScheme.ISBN_10 == IdentifierScheme.ISBN10
        False
        >>> IdentifierScheme.DOI == "doi"
        True
    """

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
    """
    Name the curated relator codes used for Agent credits.

    This is the project subset of MARC relator labels, with string-compatible members such as AUTHOR="aut" and PUBLISHER="pbl". WEMI eligibility comes from the separate role collections; the enum itself does not validate a link.

    Example:
        >>> MarcRelatorRole.AUTHOR == "aut"
        True
    """

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
    Describe a broad standard-library-style SQLite connection interface.

    This is a structural typing declaration, not an operational implementation or runtime capability check. The declaration includes exception classes, transaction/factory/state attributes and connection methods. Some advertised methods or keyword options depend on the host Python/SQLite build. This protocol is not runtime_checkable, and conformance must not be inferred merely from an object being a SQLite connection. Prefer a narrow Supports protocol when only part of the interface is consumed.

    Example:
        An adapter may annotate a dependency as ``SQLiteConnectionProtocol`` when it needs both connection state and the declared optional APIs; ordinary SQL-only code can use ``SupportsExecute``.
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
        Enter a SQLite-style transaction context and return the connection.

        Entering does not by itself start a transaction or arrange connection closure. Exit behavior depends on the transaction mode.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> with connection as entered:
            ...     same = entered is connection
            >>> same
            True
            >>> connection.close()


        :return: The same connection object, typed as Self.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """
        Finish a SQLite-style transaction context while allowing failures to propagate.

        Active transactions are committed after successful bodies and rolled back after failures, subject to autocommit mode. Context exit leaves the connection open.

        Example:
            Use ``with connection:`` around related writes and close the connection separately after the block; a raised body exception requests rollback.


        :param exc_type: Exception class from the context body, or None.
        :param exc: Exception instance from the context body, or None.
        :param tb: Associated traceback, or None.
        :return: False or None for standard SQLite behavior, so a body exception is not suppressed.
        """
        ...

    def close(self) -> None:
        """
        Release the SQLite connection and make it unavailable for further operations.

        Closing is separate from transaction commit; callers should complete the intended transaction explicitly before closing. The protocol does not implement resource cleanup.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.close()


        :return: None after the concrete connection close operation.
        """
        ...

    def commit(self) -> None:
        """
        Request commit of the connection current transaction.

        Behavior follows the connection transaction-control mode; in particular, committing an idle legacy connection is harmless. This declaration adds no transaction boundary of its own.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.commit()
            >>> connection.close()


        :return: None after the host commit operation.
        """
        ...

    def rollback(self) -> None:
        """
        Request rollback of the connection current transaction.

        Behavior follows the connection transaction-control mode. Callers should not treat this declaration as a savepoint or nested-transaction implementation.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.rollback()
            >>> connection.close()


        :return: None after the host rollback operation.
        """
        ...

    def interrupt(self) -> None:
        """
        Request interruption of currently running SQL on this connection.

        A controlling thread can use the concrete method to cancel a long operation; the executing query reports the resulting database error. This does not close the connection.

        Example:
            A cancellation handler holding the active connection can call ``connection.interrupt()`` while another thread runs a long query.


        :return: None after the interruption request.
        """
        ...

    def cursor(self, factory: type[sqlite3.Cursor] | None = None) -> sqlite3.Cursor:
        """
        Create a cursor through the host connection cursor factory.

        The declaration allows a None default, but concrete factories can require omission rather than an explicit None argument. Runtime constructor errors belong to the host.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> cursor = connection.cursor()
            >>> cursor.connection is connection
            True
            >>> cursor.close()
            >>> connection.close()


        :param factory: Optional cursor subclass/factory; omit this argument for the standard cursor.
        :return: New SQLite cursor associated with the connection.
        """
        ...

    def execute(
        self,
        sql: str,
        parameters: SQLiteParams = (),
        /,
    ) -> sqlite3.Cursor:
        """
        Execute one parameterized SQL statement using a connection-level shortcut.

        The connection creates/uses a cursor for the operation. This declaration does not add commit, result fetching or parameter interpolation.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.execute("SELECT ?", (7,)).fetchone()
            (7,)
            >>> connection.close()


        :param sql: One SQL statement, passed positionally.
        :param parameters: Positional values or named-parameter mapping, passed separately from SQL text.
        :return: Cursor containing the statement result state.
        """
        ...

    def executemany(
        self,
        sql: str,
        parameters: SQLiteManyParams,
        /,
    ) -> sqlite3.Cursor:
        """
        Execute one SQL statement repeatedly with an iterable of parameter collections.

        This is a connection shortcut for repeated cursor execution. Transaction completion remains the responsibility of the configured host/caller.

        Example:
            With a prepared table, ``connection.executemany("INSERT INTO choices(value) VALUES (?)", [("A",), ("B",)])`` binds each value separately.


        :param sql: Repeated SQL statement, passed positionally.
        :param parameters: Iterable supplying one positional sequence or named mapping per execution.
        :return: Cursor for the batch operation; the protocol does not promise a fetched result collection.
        """
        ...

    def executescript(
        self,
        sql_script: str,
        /,
    ) -> sqlite3.Cursor:
        """
        Execute SQL script text containing one or more statements.

        Transaction handling is host/mode dependent; this declaration does not make a script atomic or add an enclosing transaction. Supply transaction statements explicitly when the script requires them.

        Example:
            For an isolated connection, ``connection.executescript("CREATE TABLE choices(value TEXT); INSERT INTO choices VALUES ('A');")`` runs both statements.


        :param sql_script: Complete script text; this method has no separate parameter-binding argument.
        :return: Cursor returned by the concrete script execution.
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
        Register or remove a scalar SQL function on the connection.

        Callback invocation and SQL errors are handled by the concrete connection; the protocol only describes registration.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.create_function("twice", 1, lambda value: value * 2)
            >>> connection.execute("SELECT twice(?)", (4,)).fetchone()
            (8,)
            >>> connection.close()


        :param name: SQL function name.
        :param narg: Argument count, or -1 for variable arity.
        :param func: Python callable returning a SQLite-compatible value, or None to remove it.
        :param deterministic: Whether the function is declared deterministic for SQLite optimization.
        :return: None after host registration.
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
        Register or remove a SQL aggregate implemented by a Python class.

        Example:
            Given ``Total`` with ``step(value)`` and ``finalize()``, ``connection.create_aggregate("total_values", 1, Total)`` exposes the aggregate to SQL.


        :param name: SQL aggregate name.
        :param n_arg: SQL argument count, or -1 for variable arity.
        :param aggregate_class: Class implementing step and finalize, or None to remove registration.
        :return: None after host registration.
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
        Register or remove a window aggregate on a capable connection.

        Window-function support depends on the underlying SQLite API; declaring this protocol does not supply missing support.

        Example:
            Given a compatible ``RollingTotal`` class, ``connection.create_window_function("rolling_total", 1, RollingTotal)`` installs the window aggregate.


        :param name: SQL window-function name.
        :param num_params: SQL argument count.
        :param aggregate_class: Class supplying step, value, inverse and finalize, or None to remove it.
        :return: None after host registration.
        """
        ...

    def create_collation(
        self,
        name: str,
        callback: Callable[[str, str], int] | None,
        /,
    ) -> None:
        """
        Register or remove a named SQLite text collation.

        Example:
            With a comparator named ``compare_names``, ``connection.create_collation("name_order", compare_names)`` makes that ordering available to SQL COLLATE clauses.


        :param name: SQL collation name.
        :param callback: Two-string comparator returning negative/zero/positive, or None to remove it.
        :return: None after host registration.
        """
        ...

    def set_authorizer(
        self,
        authorizer_callback: AuthorizerCallback | None,
        /,
    ) -> None:
        """
        Install or clear the SQLite operation-authorization callback.

        Example:
            ``connection.set_authorizer(None)`` clears an existing authorizer on a connection supporting this hook.


        :param authorizer_callback: Callback taking action code, two optional arguments, database name and trigger/view name; return an SQLite authorization result, or pass None to clear it.
        :return: None after host callback configuration.
        """
        ...

    def set_progress_handler(
        self,
        progress_handler: ProgressCallback | None,
        n: int,
        /,
    ) -> None:
        """
        Install or clear the callback for periodic SQLite execution progress.

        The handler controls query continuation; this declaration does not schedule Python-side timers or background work.

        Example:
            ``connection.set_progress_handler(lambda: int(cancelled), 1000)`` lets a surrounding cancellation flag stop a long query.


        :param progress_handler: Zero-argument callback returning zero to continue or nonzero to abort; None clears it.
        :param n: Approximate virtual-machine instruction interval between callbacks.
        :return: None after host callback configuration.
        """
        ...

    def set_trace_callback(
        self,
        trace_callback: TraceCallback | None,
        /,
    ) -> None:
        """
        Install or clear a callback observing SQL statements executed by SQLite.

        Trace callbacks are observational hooks; their return value does not replace a query result.

        Example:
            ``connection.set_trace_callback(statements.append)`` collects executed statement text in a supplied list.


        :param trace_callback: Callback receiving SQL text, or None to clear tracing.
        :return: None after host callback configuration.
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
        Copy a selected source database into another SQLite connection.

        The source is the connection on which backup is called. This protocol does not create or close either connection.

        Example:
            With a committed source and open destination, ``source.backup(destination, pages=64)`` copies the selected database.


        :param target: Destination connection receiving the backup.
        :param pages: Page count per step; a nonpositive value requests all remaining pages.
        :param progress: Optional callback receiving status, remaining pages and total pages.
        :param name: Source database name, normally main.
        :param sleep: Delay in seconds between retry attempts.
        :return: None after the concrete backup completes.
        """
        ...

    def iterdump(
        self,
        *,
        filter: str | None = None,
    ) -> Iterator[str]:
        """
        Yield SQL text that can recreate the selected database contents.

        The filter keyword requires Python 3.13 or a compatible wrapper.

        Example:
            ``list(connection.iterdump())`` obtains SQL text without writing a dump file; use filter only when the host supports it.


        :param filter: Optional SQL LIKE pattern restricting object names; None selects all objects.
        :return: Iterator of SQL statement strings.
        """
        ...

    def serialize(
        self,
        /,
        *,
        name: str = "main",
    ) -> bytes:
        """
        Return a byte representation of a selected SQLite database.

        This capability depends on SQLite serialization support.

        Example:
            For a populated supporting connection, ``payload = connection.serialize(name="main")`` obtains a database image.


        :param name: Database name to serialize, normally main.
        :return: Serialized database bytes from the concrete host.
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
        Load a serialized database image into a named connection database.

        Standard SQLite deserialization reopens the selected database in memory.

        Example:
            Given a database image from serialize, ``connection.deserialize(payload, name="main")`` loads it into the selected connection database.


        :param data: SQLite database image bytes.
        :param name: Database name replaced by the loaded image.
        :return: None after host deserialization.
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
        Open one stored BLOB for incremental reading or writing.

        The requested row/value must support incremental BLOB access in the host. Opening a handle does not create the table or value.

        Example:
            For an existing BLOB row, ``with connection.blobopen("payloads", "data", 7, readonly=True) as blob:`` provides a scoped handle for ``blob.read()``.


        :param table: Table containing the BLOB.
        :param column: BLOB column name.
        :param row: Integer row identifier selecting the value.
        :param readonly: Whether the returned handle permits only reads.
        :param name: Database name containing the table.
        :return: SQLite Blob handle; the caller manages its lifetime separately from the connection.
        """
        ...

    def enable_load_extension(
        self,
        enable: bool,
        /,
    ) -> None:
        """
        Enable or disable extension loading on a capable connection.

        Extension-loading support is optional in SQLite builds. This declaration does not enable it automatically.

        Example:
            After an explicitly managed extension load, ``connection.enable_load_extension(False)`` turns further loading off on a supporting host.


        :param enable: True to enable loading, False to disable it.
        :return: None after host configuration.
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
        Load a SQLite extension library through the connection.

        Loading must be enabled and supported by the host; this protocol neither locates libraries nor manages their deployment.

        Example:
            With a configured library path and loading enabled, ``connection.load_extension(extension_path)`` requests its registration.


        :param name: Extension library name/path passed positionally.
        :param entrypoint: Optional explicit initialization entry point; None lets the host choose.
        :return: None after the concrete extension load succeeds.
        """
        ...

    def getlimit(
        self,
        category: int,
        /,
    ) -> int:
        """
        Read one SQLite runtime limit category from the connection.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.getlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH) > 0
            True
            >>> connection.close()


        :param category: SQLite limit-category integer constant.
        :return: Current limit value reported by the host.
        """
        ...

    def setlimit(
        self,
        category: int,
        limit: int,
        /,
    ) -> int:
        """
        Set one SQLite runtime limit and return its previous value.

        Values above the underlying hard maximum are capped by SQLite.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> previous = connection.getlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH)
            >>> connection.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, -1) == previous
            True
            >>> connection.close()


        :param category: SQLite limit-category integer constant.
        :param limit: Requested limit; negative values leave the limit unchanged.
        :return: Previous limit value, even when the requested value is capped or leaves it unchanged.
        """
        ...

    def getconfig(
        self,
        op: int,
        /,
    ) -> bool:
        """
        Read the boolean state of a SQLite database configuration option.

        Availability of this method and individual option constants depends on the runtime/build.

        Example:
            On a host exposing the option, ``connection.getconfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY)`` reports foreign-key enforcement state.


        :param op: Supported SQLITE_DBCONFIG option integer.
        :return: Boolean option state returned by the host.
        """
        ...

    def setconfig(
        self,
        op: int,
        enable: bool = True,
        /,
    ) -> None:
        """
        Set a SQLite database configuration option on a supporting connection.

        The protocol does not add support for options omitted by a particular runtime/build.

        Example:
            On a compatible host, ``connection.setconfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY, True)`` enables the option.


        :param op: Supported SQLITE_DBCONFIG option integer.
        :param enable: Whether to enable the option; defaults to True.
        :return: None after the host applies the option.
        """
        ...

# ---------------------------------------------------------------------------
# Narrow SQLite-ish support protocols
# ---------------------------------------------------------------------------

class SupportsCursor(Protocol):
    """
    Require only cursor creation.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A helper accepting ``connection: SupportsCursor`` can obtain a cursor with ``connection.cursor()``.
    """

    def cursor(self, factory: type[sqlite3.Cursor] | None = None) -> sqlite3.Cursor:
        """
        Create a cursor through the host connection cursor factory.

        The declaration allows a None default, but concrete factories can require omission rather than an explicit None argument. Runtime constructor errors belong to the host.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> cursor = connection.cursor()
            >>> cursor.connection is connection
            True
            >>> cursor.close()
            >>> connection.close()


        :param factory: Optional cursor subclass/factory; omit this argument for the standard cursor.
        :return: New SQLite cursor associated with the connection.
        """
        ...


class SupportsExecute(Protocol):
    """
    Require the connection shortcut for one SQL statement.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A query helper can accept ``connection: SupportsExecute`` and call ``connection.execute("SELECT ?", (7,))``.
    """

    def execute(
        self,
        sql: str,
        parameters: SQLiteParams = (),
        /,
    ) -> sqlite3.Cursor:
        """
        Execute one parameterized SQL statement using a connection-level shortcut.

        The connection creates/uses a cursor for the operation. This declaration does not add commit, result fetching or parameter interpolation.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.execute("SELECT ?", (7,)).fetchone()
            (7,)
            >>> connection.close()


        :param sql: One SQL statement, passed positionally.
        :param parameters: Positional values or named-parameter mapping, passed separately from SQL text.
        :return: Cursor containing the statement result state.
        """
        ...


class SupportsExecutemany(Protocol):
    """
    Require the connection shortcut for repeated parameterized execution.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A bulk writer can accept ``connection: SupportsExecutemany`` and invoke ``executemany`` with its prepared statement and parameter iterable.
    """

    def executemany(
        self,
        sql: str,
        parameters: SQLiteManyParams,
        /,
    ) -> sqlite3.Cursor:
        """
        Execute one SQL statement repeatedly with an iterable of parameter collections.

        This is a connection shortcut for repeated cursor execution. Transaction completion remains the responsibility of the configured host/caller.

        Example:
            With a prepared table, ``connection.executemany("INSERT INTO choices(value) VALUES (?)", [("A",), ("B",)])`` binds each value separately.


        :param sql: Repeated SQL statement, passed positionally.
        :param parameters: Iterable supplying one positional sequence or named mapping per execution.
        :return: Cursor for the batch operation; the protocol does not promise a fetched result collection.
        """
        ...


class SupportsExecutescript(Protocol):
    """
    Require connection-level SQL script execution.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A schema loader needing only scripts can annotate its connection as ``SupportsExecutescript``.
    """

    def executescript(
        self,
        sql_script: str,
        /,
    ) -> sqlite3.Cursor:
        """
        Execute SQL script text containing one or more statements.

        Transaction handling is host/mode dependent; this declaration does not make a script atomic or add an enclosing transaction. Supply transaction statements explicitly when the script requires them.

        Example:
            For an isolated connection, ``connection.executescript("CREATE TABLE choices(value TEXT); INSERT INTO choices VALUES ('A');")`` runs both statements.


        :param sql_script: Complete script text; this method has no separate parameter-binding argument.
        :return: Cursor returned by the concrete script execution.
        """
        ...


class SupportsSQLExecution(
    SupportsExecute,
    SupportsExecutemany,
    SupportsExecutescript,
    Protocol,
):
    """
    Combine single, repeated and script SQL execution capabilities.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A SQL utility supporting all three shortcuts can accept ``connection: SupportsSQLExecution`` without requiring backup or BLOB methods.
    """


class SupportsTransactions(Protocol):
    """
    Require commit, rollback and an in_transaction flag.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A transaction helper can inspect ``connection.in_transaction`` before deciding whether to call ``connection.rollback()``.
    """

    in_transaction: bool
    """Whether a transaction is currently active."""

    def commit(self) -> None:
        """
        Request commit of the connection current transaction.

        Behavior follows the connection transaction-control mode; in particular, committing an idle legacy connection is harmless. This declaration adds no transaction boundary of its own.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.commit()
            >>> connection.close()


        :return: None after the host commit operation.
        """
        ...

    def rollback(self) -> None:
        """
        Request rollback of the connection current transaction.

        Behavior follows the connection transaction-control mode. Callers should not treat this declaration as a savepoint or nested-transaction implementation.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.rollback()
            >>> connection.close()


        :return: None after the host rollback operation.
        """
        ...


class SupportsConnectionLifecycle(Protocol):
    """
    Require connection closure and query interruption.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A connection owner can accept ``connection: SupportsConnectionLifecycle`` and close it when its work is complete.
    """

    def close(self) -> None:
        """
        Release the SQLite connection and make it unavailable for further operations.

        Closing is separate from transaction commit; callers should complete the intended transaction explicitly before closing. The protocol does not implement resource cleanup.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.close()


        :return: None after the concrete connection close operation.
        """
        ...

    def interrupt(self) -> None:
        """
        Request interruption of currently running SQL on this connection.

        A controlling thread can use the concrete method to cancel a long operation; the executing query reports the resulting database error. This does not close the connection.

        Example:
            A cancellation handler holding the active connection can call ``connection.interrupt()`` while another thread runs a long query.


        :return: None after the interruption request.
        """
        ...


class SupportsConnectionContext(Protocol):
    """
    Require a SQLite-style transaction context-manager interface.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A helper using ``with connection:`` can require ``SupportsConnectionContext`` while its caller retains responsibility for connection closure.
    """

    def __enter__(self) -> Self:
        """
        Enter a SQLite-style transaction context and return the connection.

        Entering does not by itself start a transaction or arrange connection closure. Exit behavior depends on the transaction mode.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> with connection as entered:
            ...     same = entered is connection
            >>> same
            True
            >>> connection.close()


        :return: The same connection object, typed as Self.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """
        Finish a SQLite-style transaction context while allowing failures to propagate.

        Active transactions are committed after successful bodies and rolled back after failures, subject to autocommit mode. Context exit leaves the connection open.

        Example:
            Use ``with connection:`` around related writes and close the connection separately after the block; a raised body exception requests rollback.


        :param exc_type: Exception class from the context body, or None.
        :param exc: Exception instance from the context body, or None.
        :param tb: Associated traceback, or None.
        :return: False or None for standard SQLite behavior, so a body exception is not suppressed.
        """
        ...


class SupportsRowFactory(Protocol):
    """
    Require configurable conversion of cursor result rows.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A host typed as ``SupportsRowFactory`` can accept ``connection.row_factory = sqlite3.Row`` for subsequently created cursors.
    """

    row_factory: RowFactory | None
    """Callable used to transform rows returned by cursors."""


class SupportsTextFactory(Protocol):
    """
    Require configurable conversion of SQLite TEXT bytes.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A host typed as ``SupportsTextFactory`` can use ``connection.text_factory = bytes`` when its caller needs raw text bytes.
    """

    text_factory: TextFactory
    """Callable used to convert SQLite TEXT values."""


class SupportsSQLiteState(Protocol):
    """
    Expose total_changes and in_transaction state for observation.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support. The intended use is observation; ordinary annotated Protocol attributes do not enforce read-only access at runtime.

    Example:
        A status helper can read ``connection.total_changes`` from a ``SupportsSQLiteState`` dependency.
    """

    total_changes: int
    """Total number of changed rows since the connection was opened."""

    in_transaction: bool
    """Whether a transaction is currently active."""


class SupportsFunctionRegistration(Protocol):
    """
    Combine scalar, aggregate, window-function and collation registration.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A connection setup helper requiring these registrations can declare ``connection: SupportsFunctionRegistration``.
    """

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
        Register or remove a scalar SQL function on the connection.

        Callback invocation and SQL errors are handled by the concrete connection; the protocol only describes registration.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.create_function("twice", 1, lambda value: value * 2)
            >>> connection.execute("SELECT twice(?)", (4,)).fetchone()
            (8,)
            >>> connection.close()


        :param name: SQL function name.
        :param narg: Argument count, or -1 for variable arity.
        :param func: Python callable returning a SQLite-compatible value, or None to remove it.
        :param deterministic: Whether the function is declared deterministic for SQLite optimization.
        :return: None after host registration.
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
        Register or remove a SQL aggregate implemented by a Python class.

        Example:
            Given ``Total`` with ``step(value)`` and ``finalize()``, ``connection.create_aggregate("total_values", 1, Total)`` exposes the aggregate to SQL.


        :param name: SQL aggregate name.
        :param n_arg: SQL argument count, or -1 for variable arity.
        :param aggregate_class: Class implementing step and finalize, or None to remove registration.
        :return: None after host registration.
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
        Register or remove a window aggregate on a capable connection.

        Window-function support depends on the underlying SQLite API; declaring this protocol does not supply missing support.

        Example:
            Given a compatible ``RollingTotal`` class, ``connection.create_window_function("rolling_total", 1, RollingTotal)`` installs the window aggregate.


        :param name: SQL window-function name.
        :param num_params: SQL argument count.
        :param aggregate_class: Class supplying step, value, inverse and finalize, or None to remove it.
        :return: None after host registration.
        """
        ...

    def create_collation(
        self,
        name: str,
        callback: Callable[[str, str], int] | None,
        /,
    ) -> None:
        """
        Register or remove a named SQLite text collation.

        Example:
            With a comparator named ``compare_names``, ``connection.create_collation("name_order", compare_names)`` makes that ordering available to SQL COLLATE clauses.


        :param name: SQL collation name.
        :param callback: Two-string comparator returning negative/zero/positive, or None to remove it.
        :return: None after host registration.
        """
        ...


class SupportsSQLiteHooks(Protocol):
    """
    Combine authorizer, progress and SQL trace callback registration.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        An instrumented query runner can require ``SupportsSQLiteHooks`` before installing its progress and trace callbacks.
    """

    def set_authorizer(
        self,
        authorizer_callback: AuthorizerCallback | None,
        /,
    ) -> None:
        """
        Install or clear the SQLite operation-authorization callback.

        Example:
            ``connection.set_authorizer(None)`` clears an existing authorizer on a connection supporting this hook.


        :param authorizer_callback: Callback taking action code, two optional arguments, database name and trigger/view name; return an SQLite authorization result, or pass None to clear it.
        :return: None after host callback configuration.
        """
        ...

    def set_progress_handler(
        self,
        progress_handler: ProgressCallback | None,
        n: int,
        /,
    ) -> None:
        """
        Install or clear the callback for periodic SQLite execution progress.

        The handler controls query continuation; this declaration does not schedule Python-side timers or background work.

        Example:
            ``connection.set_progress_handler(lambda: int(cancelled), 1000)`` lets a surrounding cancellation flag stop a long query.


        :param progress_handler: Zero-argument callback returning zero to continue or nonzero to abort; None clears it.
        :param n: Approximate virtual-machine instruction interval between callbacks.
        :return: None after host callback configuration.
        """
        ...

    def set_trace_callback(
        self,
        trace_callback: TraceCallback | None,
        /,
    ) -> None:
        """
        Install or clear a callback observing SQL statements executed by SQLite.

        Trace callbacks are observational hooks; their return value does not replace a query result.

        Example:
            ``connection.set_trace_callback(statements.append)`` collects executed statement text in a supplied list.


        :param trace_callback: Callback receiving SQL text, or None to clear tracing.
        :return: None after host callback configuration.
        """
        ...


class SupportsBackup(Protocol):
    """
    Require copying a database into another SQLite connection.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A backup helper can accept ``source: SupportsBackup`` and call ``source.backup(destination)``.
    """

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
        Copy a selected source database into another SQLite connection.

        The source is the connection on which backup is called. This protocol does not create or close either connection.

        Example:
            With a committed source and open destination, ``source.backup(destination, pages=64)`` copies the selected database.


        :param target: Destination connection receiving the backup.
        :param pages: Page count per step; a nonpositive value requests all remaining pages.
        :param progress: Optional callback receiving status, remaining pages and total pages.
        :param name: Source database name, normally main.
        :param sleep: Delay in seconds between retry attempts.
        :return: None after the concrete backup completes.
        """
        ...


class SupportsIterdump(Protocol):
    """
    Require SQL dump generation with the declared optional filter.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A dump helper may accept ``connection: SupportsIterdump`` and iterate ``connection.iterdump()``; filtered dumping additionally needs host support.
    """

    def iterdump(
        self,
        *,
        filter: str | None = None,
    ) -> Iterator[str]:
        """
        Yield SQL text that can recreate the selected database contents.

        The filter keyword requires Python 3.13 or a compatible wrapper.

        Example:
            ``list(connection.iterdump())`` obtains SQL text without writing a dump file; use filter only when the host supports it.


        :param filter: Optional SQL LIKE pattern restricting object names; None selects all objects.
        :return: Iterator of SQL statement strings.
        """
        ...


class SupportsSerialization(Protocol):
    """
    Require database byte-image serialization and loading.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A supporting connection typed as ``SupportsSerialization`` can expose a database image through ``connection.serialize()``.
    """

    def serialize(
        self,
        /,
        *,
        name: str = "main",
    ) -> bytes:
        """
        Return a byte representation of a selected SQLite database.

        This capability depends on SQLite serialization support.

        Example:
            For a populated supporting connection, ``payload = connection.serialize(name="main")`` obtains a database image.


        :param name: Database name to serialize, normally main.
        :return: Serialized database bytes from the concrete host.
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
        Load a serialized database image into a named connection database.

        Standard SQLite deserialization reopens the selected database in memory.

        Example:
            Given a database image from serialize, ``connection.deserialize(payload, name="main")`` loads it into the selected connection database.


        :param data: SQLite database image bytes.
        :param name: Database name replaced by the loaded image.
        :return: None after host deserialization.
        """
        ...


class SupportsBlobOpen(Protocol):
    """
    Require incremental access to an existing BLOB value.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A BLOB reader can accept ``connection: SupportsBlobOpen`` and request a read-only handle for its selected row.
    """

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
        Open one stored BLOB for incremental reading or writing.

        The requested row/value must support incremental BLOB access in the host. Opening a handle does not create the table or value.

        Example:
            For an existing BLOB row, ``with connection.blobopen("payloads", "data", 7, readonly=True) as blob:`` provides a scoped handle for ``blob.read()``.


        :param table: Table containing the BLOB.
        :param column: BLOB column name.
        :param row: Integer row identifier selecting the value.
        :param readonly: Whether the returned handle permits only reads.
        :param name: Database name containing the table.
        :return: SQLite Blob handle; the caller manages its lifetime separately from the connection.
        """
        ...


class SupportsExtensionLoading(Protocol):
    """
    Require extension-loading controls and explicit library loading.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        Extension setup code can declare ``connection: SupportsExtensionLoading`` when it manages loading on a supporting host.
    """

    def enable_load_extension(
        self,
        enable: bool,
        /,
    ) -> None:
        """
        Enable or disable extension loading on a capable connection.

        Extension-loading support is optional in SQLite builds. This declaration does not enable it automatically.

        Example:
            After an explicitly managed extension load, ``connection.enable_load_extension(False)`` turns further loading off on a supporting host.


        :param enable: True to enable loading, False to disable it.
        :return: None after host configuration.
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
        Load a SQLite extension library through the connection.

        Loading must be enabled and supported by the host; this protocol neither locates libraries nor manages their deployment.

        Example:
            With a configured library path and loading enabled, ``connection.load_extension(extension_path)`` requests its registration.


        :param name: Extension library name/path passed positionally.
        :param entrypoint: Optional explicit initialization entry point; None lets the host choose.
        :return: None after the concrete extension load succeeds.
        """
        ...


class SupportsSQLiteLimits(Protocol):
    """
    Require querying and changing SQLite runtime limits.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A connection policy helper can accept ``SupportsSQLiteLimits`` to inspect SQLITE_LIMIT_SQL_LENGTH.
    """

    def getlimit(
        self,
        category: int,
        /,
    ) -> int:
        """
        Read one SQLite runtime limit category from the connection.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> connection.getlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH) > 0
            True
            >>> connection.close()


        :param category: SQLite limit-category integer constant.
        :return: Current limit value reported by the host.
        """
        ...

    def setlimit(
        self,
        category: int,
        limit: int,
        /,
    ) -> int:
        """
        Set one SQLite runtime limit and return its previous value.

        Values above the underlying hard maximum are capped by SQLite.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> previous = connection.getlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH)
            >>> connection.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, -1) == previous
            True
            >>> connection.close()


        :param category: SQLite limit-category integer constant.
        :param limit: Requested limit; negative values leave the limit unchanged.
        :return: Previous limit value, even when the requested value is capped or leaves it unchanged.
        """
        ...


class SupportsSQLiteConfig(Protocol):
    """
    Require boolean SQLite database-configuration access.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support.

    Example:
        A setup helper can accept ``connection: SupportsSQLiteConfig`` when its host exposes the required DBCONFIG operations.
    """

    def getconfig(
        self,
        op: int,
        /,
    ) -> bool:
        """
        Read the boolean state of a SQLite database configuration option.

        Availability of this method and individual option constants depends on the runtime/build.

        Example:
            On a host exposing the option, ``connection.getconfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY)`` reports foreign-key enforcement state.


        :param op: Supported SQLITE_DBCONFIG option integer.
        :return: Boolean option state returned by the host.
        """
        ...

    def setconfig(
        self,
        op: int,
        enable: bool = True,
        /,
    ) -> None:
        """
        Set a SQLite database configuration option on a supporting connection.

        The protocol does not add support for options omitted by a particular runtime/build.

        Example:
            On a compatible host, ``connection.setconfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY, True)`` enables the option.


        :param op: Supported SQLITE_DBCONFIG option integer.
        :param enable: Whether to enable the option; defaults to True.
        :return: None after the host applies the option.
        """
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
    Combine cursor, SQL, transaction, lifecycle, context, factory and state capabilities.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support. Its bases omit custom-function registration, callback hooks, backup/dump, serialization, BLOB access, extension loading, limits and configuration.

    Example:
        A wrapper using the common SQLite surface can annotate its connection as ``SupportsSQLiteCore`` and leave optional specialized capabilities to separate dependencies.
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
    Combine the core interface with all declared optional SQLite capabilities.

    This is a structural typing declaration, not an operational implementation or runtime capability check. These Protocol classes are not runtime_checkable; annotations alone do not establish host support. Unlike SQLiteConnectionProtocol, this composition does not declare exception-class attributes, isolation_level or autocommit.

    Example:
        A wrapper requiring backup, callbacks and BLOB access together can declare ``connection: SupportsFullSQLiteConnection`` after selecting a compatible host.
    """
