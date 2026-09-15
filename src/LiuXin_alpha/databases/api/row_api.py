"""
Declare generic row interfaces and optional interlink property adapters.

RowAPI defines the abstract row surface and a minimal state initializer. InterlinkRowAPI adds concrete guarded descriptors but remains abstract. IntralinkRowAPI and ViewRowAPI currently add only marker types. Reference behavior described for generic rows comes from databases.row.Row; abstract method bodies do not perform those operations.
"""

from __future__ import annotations

import abc
import datetime

from typing import Any, Iterator, Optional, Union, TYPE_CHECKING

from LiuXin_alpha.errors import NoSuchPropertyForLinkException

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName, InterlinkTableID



class RowAPI(abc.ABC):
    """
    Specify local row access, identity, loading and explicit persistence.

    Implementations provide the abstract operations; the base initializer only stores database, read_only and a shallow payload copy. The concrete Row implementation has legacy return and identity semantics documented on the individual methods. This interface does not make a row immutable or enforce database constraints by itself.

    Example:
        A consumer accepting RowAPI can stage a known field through row[column] and then call row.sync() to request persistence.
    """

    @classmethod
    @abc.abstractmethod
    def from_idless_row_dict(cls,
                             database: "DatabaseAPI",
                             row_dict: dict[str, Any],
                             *,
                             table: Optional[str] = None,
                             read_only: bool = False,
                             reload_from_db: bool = True) -> "RowAPI":
        """
        Insert an ID-less payload and return a row representing the inserted record.

        A None ID field is removed before insertion; an explicit non-None ID is retained. Construction with read_only=True still inserts first. Insertion and reload are separate driver operations, so a later reload/construction failure need not undo the insert.

        Example:
            Given db, Row.from_idless_row_dict(db, {"work_title": "A journey"}) creates a Work and normally reloads its generated ID and defaults.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param row_dict: Column/value mapping; copied before being retained by the row.
        :param table: Optional target hint for view checks, ID-column lookup and reload; insertion still delegates to the wrapper’s payload-based table inference.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :param reload_from_db: Reload a nonzero returned ID when its ID column is known, to include defaults and trigger changes.
        :return: New instance of the requested row class, reloaded when possible or built from the copied payload.
        :raises DatabaseDriverError: No database is supplied, a SQLite view is targeted, or the underlying insert fails.
        :raises TypeError: row_dict is None.
        :raises DatabaseIntegrityError: Payload inference or a database constraint prevents insertion.
        """

    def __init__(
            self,
            database: "DatabaseAPI",
            row_dict: Optional[dict[str, str]] = None,
            *,
            read_only: bool=False) -> None:
        """
        Store the database, read-only flag and a shallow copy of supplied values.

        Example:
            A concrete subclass can call RowAPI.__init__(self, database, {"work_title": "Draft"}) before adding its own metadata validation.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param row_dict: Mapping copied with dict(row_dict or {}); nested values remain shared.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :return: None; no schema lookup or persistence is performed by this base initializer.
        """
        self.db = database
        self.read_only = read_only
        self.int_row_dict = dict(row_dict or {})

    @abc.abstractmethod
    def __contains__(self, item: str) -> bool:
        """
        Test whether a column is already present in the local row mapping.

        A recognized schema column may still be absent until loaded, assigned or accessed through __getitem__. No database query occurs.

        Example:
            For a Row containing only work_title, "work_title" in row is true even before sync assigns an ID.


        :param item: Column key to test without materializing a missing value.
        :return: True only for keys currently stored in row_dict.
        """

    @abc.abstractmethod
    def __deepcopy__(self, memo: dict[Any, Any]) -> RowAPI:
        """
        Copy local values into a new writable Row attached to the same database.

        The database object is shared. The concrete method does not preserve subclasses, the read_only flag, or an ID cached separately from row_dict. It starts a fresh deepcopy for the payload rather than using memo; recursive references to the Row are therefore unsupported. No new database record is inserted by copying.

        Example:
            Given a persisted row, copy.deepcopy(row) produces an independent value snapshot whose sync() updates the same stored ID.


        :param memo: Copy-protocol memo argument, ignored by the concrete implementation.
        :return: New base Row with deeply copied values and identity inferred again from those values.
        """

    @abc.abstractmethod
    def __eq__(self, other: RowAPI) -> bool:
        """
        Compare this row’s hash with the other object’s hash.

        The concrete Row implementation compares hashes rather than comparing identity tuples directly. Hash collisions, including with a non-Row object, can therefore compare equal; local non-identity field values are ignored.

        Example:
            Two loaded rows with the same database UUID, table and ID compare equal even when their local display values differ.


        :param other: Object accepted by hash(); no Row type check is performed.
        :return: True when the two hash integers are equal.
        :raises TypeError: other is unhashable.
        :raises InputIntegrityError: This row’s ID cannot be resolved because its table is unknown.
        """

    @abc.abstractmethod
    def __getitem__(self, item: str) -> Union[str, int, float, datetime.datetime]:
        """
        Read a local value, materializing None for an absent recognized column.

        An already stored key wins without schema revalidation. A missing allowed key is inserted into row_dict with value None; this is not a database refresh.

        Example:
            For a partial Work row, row["work_id"] can return None and add that key to the local mapping before the row has been inserted.


        :param item: Existing payload key or recognized column for the inferred table.
        :return: Stored value, including None despite the narrower return annotation.
        :raises KeyError: The key is neither stored nor allowed for the table.
        """

    # Todo: There's probably a standard way to do this we should just use
    #       So that two different row implementations have the same hash for the same row
    @abc.abstractmethod
    def __hash__(self) -> int:
        """
        Hash the database UUID, resolved row ID and table as one tuple.

        Non-identity payload changes leave the hash alone. Changing the table, ID or database UUID can change it, so assign a stable identity before using a row as a dictionary key. An identified but unsaved row may hash with a None ID.

        Example:
            Given a persisted row and a separately loaded copy, hash(row) equals hash(copy) when database UUID, table and ID match.


        :return: Process-local hash integer for the current identity fields.
        :raises InputIntegrityError: The row has neither a cached ID nor an identified table.
        """

    # Todo: Does this mean column headings, or values, or tuples? Not clear. Follow dict.
    #       Dict's print "column headings" - so do that
    @abc.abstractmethod
    def __iter__(self) -> Iterator[str]:
        """
        Iterate the column keys currently stored in the local mapping.

        Partial rows expose only materialized keys. This does not enumerate all schema columns, fetch missing values or snapshot the mapping before iteration.

        Example:
            For a Row constructed from {"work_title": "A journey"}, tuple(row) initially contains only "work_title".


        :return: Iterator yielding keys in dictionary insertion order.
        """

    # Todo: Iterkeys and itervalues

    # Todo: Actually make this an ASCII rep of the object?
    @abc.abstractmethod
    def __repr__(self) -> str:
        """
        Format a compact diagnostic label containing database, table and ID.

        Example:
            For a loaded row, repr(row) produces an LX ROW OBJECT label suitable for inspecting its database identity.


        :return: Text label using repr(db), the cached table and resolved row ID.
        :raises InputIntegrityError: An unidentified row cannot resolve its ID.
        """

    @abc.abstractmethod
    def __setitem__(self, key: str, value: Union[str, int, float, datetime.datetime]) -> None:
        """
        Stage a column value locally after checking its schema membership.

        The first assignment to an empty mapping triggers metadata inference. Later assignments require an allowed column but do not refresh cached identity, even when assigning the ID field. Read-only rows still permit this local operation.

        Example:
            Given a Work row, row["work_title"] = "Revised" stages a value; row.sync() is the separate persistence step.


        :param key: Known column belonging to the row’s table, or a column that identifies an empty row.
        :param value: Value retained as supplied; no database write occurs here.
        :return: None; the local mapping is changed.
        :raises KeyError: No table recognizes the key, or it is not allowed on this row.
        """

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Return the legacy UTF-8 byte encoding of the detailed row representation.

        Python 3 str(row) requires __str__ to return text and therefore raises TypeError with this implementation. Use row.__unicode__() for the detailed text or repr(row) for a compact label.

        Example:
            Given a loaded row, row.__str__() returns encoded bytes; row.__unicode__() returns the corresponding text.


        :return: bytes from the concrete Row, despite the API’s str annotation.
        """

    @abc.abstractmethod
    def __unicode__(self) -> str:
        """
        Render the local payload and cached row/schema metadata as diagnostic text.

        The representation is unbounded and includes payload values. It reads local state rather than reloading the row.

        Example:
            For a loaded row, details = row.__unicode__() includes its row_dict and relationship metadata.


        :return: Multiline string with values, table, allowed tables/columns, ID and linkability.
        :raises InputIntegrityError: An unidentified row cannot resolve its ID.
        """

    # Todo: "name" should be "column_name
    # Todo: The return is going to be Literal strings - work out what they are
    @staticmethod
    @abc.abstractmethod
    def _best_effort_sqlite_object_type(database: "DatabaseAPI", name: str) -> Optional[str]:
        """
        Probe sqlite_master to classify a named schema object when possible.

        Open a driver connection through database.driver_wrapper.driver and close it in a finally block. Unsupported drivers, unavailable metadata and lookup failures are deliberately treated as unknown; cleanup errors are suppressed.

        Example:
            Given a SQLite-backed db, Row._best_effort_sqlite_object_type(db, "works") normally returns "table".


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param name: Object name bound to the sqlite_master lookup.
        :return: Stored schema type such as table, view or index, or None on no match or any probe error.
        """

    @abc.abstractmethod
    def to_jsonable(
        self,
        *,
        include_values: bool = True,
        max_cols: int = 50,
        max_text: int = 500,
        include_db_uuid: bool = True,
    ) -> dict[str, Any]:
        """
        Build a lossy diagnostic mapping of row identity and optional local values.

        The concrete Row sanitizer handles common value types, but this is not a strict JSON or size guarantee: identity/UUID fields pass through, non-finite floats remain, and depth-limit repr strings are unbounded. Values are not redacted. Zero/negative limits are not rejected, and truncation markers can exceed the nominal item/text limits. No database refresh occurs.

        Example:
            For a loaded row with ordinary scalar identity fields, json.dumps(row.to_jsonable(include_values=False)) encodes its diagnostic identity without row values.


        :param include_values: Include the local row_dict snapshot, default True.
        :param max_cols: Maximum top-level columns and nested collection items before truncation markers.
        :param max_text: Text/byte-hex truncation threshold forwarded to the sanitizer.
        :param include_db_uuid: Include the database uuid attribute, or None when unavailable.
        :return: Dictionary with type, table, row_id and read_only, plus optional db_uuid and sanitized row_dict.
        :raises InputIntegrityError: An unidentified row cannot resolve its ID.
        """
        ...

    @abc.abstractmethod
    def ensure_row_has_id(self) -> None:
        """
        Allocate a database-backed ID when the local payload has none.

        The wrapper preserves a non-None ID, including zero, without checking row existence. Otherwise it inserts a blank row and copies only that ID into the payload; the payload’s other values are written later by sync. This method itself is not disabled by read_only mode.

        Example:
            For an unsaved Row with work_title set, row.ensure_row_has_id() allocates its database ID before a later row.sync().


        :return: None; replace the local payload with the wrapper result and cache its extracted ID.
        """

    @abc.abstractmethod
    def keys(self) -> None:
        """
        Expose the live dictionary view of materialized local column names.

        The view reflects later mapping mutations and does not enumerate unmaterialized schema columns.

        Example:
            For a partial row, tuple(row.keys()) lists only the fields currently in row.row_dict.


        :return: dict_keys view in the concrete Row, despite the None return annotation.
        """

    # Todo: Corresponding factory method?
    @abc.abstractmethod
    def load_blank_row(self, table: Optional[str] = None) -> None:
        """
        Create a blank database row and replace this object’s local state with it.

        This allocates a stored row with an ID through the wrapper; it is not an empty in-memory constructor. Existing local contents are overwritten without a guard. Read-only mode does not prevent this factory operation. The wrapper clears its scratch marker in the returned mapping; persistence of that clearing requires a later sync.

        Example:
            Given row = Row(db), row.load_blank_row("works") allocates and loads a new blank Work.


        :param table: Target table, or None to reuse the cached table.
        :return: None; load the new blank mapping and refresh metadata.
        :raises InputIntegrityError: The target is invalid or is a view rather than a writable base table.
        """

    @abc.abstractmethod
    def load_row_from_id(self, row_id: int=None, table: str=None) -> None:
        """
        Replace local values with a driver lookup for the selected table and ID.

        Supplied identity values change the object before lookup, and unsynced local values are discarded. The concrete method does not translate a missing-row sentinel: the SQLite wrapper can leave row_dict=False and retain cached identity. Use Database.get_row_from_id for its higher-level missing-row handling.

        Example:
            Given row = Row(db), row.load_row_from_id(work_id, "works") loads an existing Work into that object.


        :param row_id: ID to cache before lookup; None reuses the existing ID.
        :param table: Table to cache before lookup; None reuses the existing table.
        :return: None after replacing row_dict and refreshing metadata.
        :raises InputIntegrityError: ID resolution fails while the table is still unknown.
        :raises TypeError: A required ID or table remains unset after resolution.
        """

    @abc.abstractmethod
    def make_read_only(self) -> None:
        """
        Mark the row read-only and replace its instance sync method with no_sync.

        Local edits and other methods that write directly through the wrapper remain available. Changing the flag back to False does not restore the replaced sync method.

        Example:
            After row.make_read_only(), assigning row["work_title"] is still local, while row.sync() raises RowReadOnlyError.


        :return: None; future normal sync() calls raise RowReadOnlyError.
        """

    @abc.abstractmethod
    def no_sync(self) -> None:
        """
        Reject a synchronization request for a row in read-only mode.

        Example:
            A read-only row routes row.sync() to this method and raises RowReadOnlyError.


        :return: Never returns normally.
        :raises RowReadOnlyError: Always, regardless of current payload or flag value.
        """

    @abc.abstractmethod
    def refresh_db_properties(self) -> None:
        """
        Infer row identity and cache schema/link metadata from the current payload.

        For an empty or false payload, refresh only allowed_tables and retain other cached fields. Otherwise infer the table, read its ID column and linkability, and cache allowed columns. False ID values become None except values comparing equal to zero, which are cached as zero. Inference errors can leave earlier metadata changes in place.

        Example:
            After replacing row.row_dict with a new valid table payload, row.refresh_db_properties() recomputes its table, ID and allowed columns.


        :return: None; update cached properties without fetching stored row values.
        """

    @property
    @abc.abstractmethod
    def row_id(self) -> Optional[int]:
        """
        Read a cached row ID or fall back to the payload’s discovered ID column.

        A non-None cached ID wins over row_dict, including zero. The concrete getter does not coerce values to int or check database existence.

        Example:
            For a Row with a Work payload but no work_id, row.row_id is None until an ID is assigned or allocated.


        :return: Cached ID, or the payload value; None when a known table has no supplied ID.
        :raises InputIntegrityError: Neither a cached ID nor an identified table is available.
        """

    # Todo: We can _probably_ tighten this Any - with abuse of protocols and overloads perhaps for all rows?
    @property
    @abc.abstractmethod
    def row_dict(self) -> dict[str, Any]:
        """
        Expose the mutable local column/value mapping directly.

        Direct dictionary mutation bypasses __setitem__ schema checks and does not refresh cached identity. Read-only mode does not freeze this object.

        Example:
            For a loaded row, row.row_dict["work_title"] = "Revised" changes only the local mapping until sync.


        :return: The internal mapping itself, not a copy; legacy missing-row loads may retain a false sentinel.
        """

    @abc.abstractmethod
    def sync(self) -> None:
        """
        Ensure an ID and write the local payload through the driver wrapper.

        When ID allocation is needed, blank-row insertion and payload update are separate wrapper calls without a transaction composed by Row. A failed update can therefore leave an allocated blank row. The method does not reload defaults or trigger changes afterward. Read-only instances normally replace this method with no_sync.

        Example:
            Given row = Row(db, {"work_title": "A journey"}), row.sync() allocates an ID if necessary and writes the staged title.


        :return: None; the driver’s update status is discarded.
        :raises RowReadOnlyError: An instance in read-only mode routes sync to no_sync.
        """

    @property
    @abc.abstractmethod
    def table(self) -> str:
        """
        Return the table name currently cached for this row.

        Example:
            Row(db, {"work_title": "A journey"}).table is "works" when those fields identify the Work table.


        :return: Inferred or explicitly loaded table name; None for an unidentified empty Row despite the str annotation.
        """

    @abc.abstractmethod
    def update_and_check(self) -> None:
        """
        Refresh cached schema and identity metadata from the local payload.

        Despite its name, this does not fetch database values, persist edits, or perform additional value validation.

        Example:
            After a direct row_dict replacement, row.update_and_check() recalculates the table/ID metadata.


        :return: None; delegates to refresh_db_properties.
        """


# Todo: ensure driver has a direct_get_allowed_types method.
class InterlinkRowAPI(RowAPI):
    """
    Add guarded optional properties for a relationship between two tables.

    Subclasses provide row storage and configure each _has_* flag and column name. The descriptors generally mutate row_dict directly without syncing. The type getter remains abstract, and its provided setter only checks allowed membership without assigning the value.

    Example:
        A concrete interlink subclass with _has_priority=True and priority_col configured can expose link.priority as an integer backed by row_dict.
    """
    src_table: MainTableName
    dst_table: MainTableName

    # Properties of the link
    _has_priority: bool = False
    priority_col: str

    _has_primary: bool = False
    primary_col: str

    _has_type: bool = False
    _allowed_types: list[str]
    type_col: str

    _has_origin: bool = False
    origin_col: str

    _has_policy: bool = False
    policy_col: str

    _has_data: bool = False
    data_col: str

    _has_index: bool = False
    index_col: str

    _has_sequence_number: bool = False
    sequence_number_col: str

    _has_is_required: bool = False
    is_required_col: str

    def __init__(
            self,
            database: "DatabaseAPI",
            row_dict: Optional[dict[str, str]] = None,
            *,
            read_only: bool=False,
            src_table: MainTableName,
            dst_table: MainTableName
    ) -> None:
        """
        Initialize local row state and retain the source/destination table labels.

        Example:
            A concrete subclass calls super().__init__(db, payload, src_table="works", dst_table="tags") before binding its optional property columns.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param row_dict: Payload shallow-copied by RowAPI initialization.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :param src_table: Source table label retained without schema validation.
        :param dst_table: Destination table label retained without schema validation.
        :return: None; property support flags and column bindings are left to the subclass.
        """
        super().__init__(database=database, row_dict=row_dict, read_only=read_only)

        self.src_table = src_table
        self.dst_table = dst_table

    @property
    def priority(self) -> int:
        """
        Read the enabled link priority property as int.

        Convert the stored value with int; missing or None values raise rather than defaulting to zero.

        Example:
            For a concrete link with this property enabled and its column configured, link.priority reads the local stored value without fetching or syncing.


        :return: int conversion of row_dict.get(priority_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_priority.
        :raises TypeError: The stored value is missing, None or otherwise not int-convertible.
        :raises ValueError: Stored text is not a valid integer.
        """
        if not self._has_priority:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support priority.")
        return int(self.row_dict.get(self.priority_col))

    @priority.setter
    def priority(self, new_priority: int) -> None:
        """
        Assign the enabled link priority property in the local row mapping.

        No automatic sync or cached row metadata refresh occurs.

        Example:
            On a concrete link with priority enabled, assigning link.priority invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_priority: Proposed priority value, converted with int.
        :return: None; only the local mapping is updated.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_priority.
        :raises TypeError: The proposed value cannot be converted to int.
        :raises ValueError: The proposed text is not a valid integer.
        """
        if not self._has_priority:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support priority.")
        self.row_dict[self.priority_col] = int(new_priority)

    @property
    def primary(self) -> bool:
        """
        Read the enabled link primary property as bool.

        Apply bool to the stored value; missing/None becomes False, while nonempty text such as "0" is True.

        Example:
            For a concrete link with this property enabled and its column configured, link.primary reads the local stored value without fetching or syncing.


        :return: bool conversion of row_dict.get(primary_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_primary.
        """
        if not self._has_primary:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support primary.")
        return bool(self.row_dict.get(self.primary_col))

    @primary.setter
    def primary(self, new_primary: bool) -> None:
        """
        Assign the enabled link primary property in the local row mapping.

        No automatic sync or cached row metadata refresh occurs.

        Example:
            On a concrete link with primary enabled, assigning link.primary invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_primary: Proposed primary value, retained unchanged.
        :return: None; only the local mapping is updated.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_primary.
        """
        if not self._has_primary:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support primary.")
        self.row_dict[self.primary_col] = new_primary

    @property
    @abc.abstractmethod
    def type(self) -> str:
        """
        Read the enabled link type property as str.

        Apply str to the stored value; missing/None therefore yields the literal text "None". This getter has an implementation but remains abstract for subclasses.

        Example:
            For a concrete link with this property enabled and its column configured, link.type reads the local stored value without fetching or syncing.


        :return: str conversion of row_dict.get(type_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_type.
        """
        if not self._has_type:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support type.")
        return str(self.row_dict.get(self.type_col))

    @type.setter
    def type(self, new_type: str) -> None:
        """
        Check a proposed enabled link type against the allowed registry.

        The provided setter performs an assert membership check only: it does not assign row_dict[type_col] or synchronize. Assertions may be disabled by the interpreter.

        Example:
            On a concrete link with type enabled, assigning link.type invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_type: Proposed type value, checked against _allowed_types without coercion or storage.
        :return: None; the stored type is unchanged.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_type.
        :raises AssertionError: The value is absent from _allowed_types while assertions are enabled.
        """
        if not self._has_type:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support type.")

        assert new_type in self._allowed_types, f"{new_type = } not in allowed_types = {self._allowed_types}"

    @property
    def origin(self) -> str:
        """
        Read the enabled link origin property as str.

        Apply str to the stored value; missing/None therefore yields the literal text "None".

        Example:
            For a concrete link with this property enabled and its column configured, link.origin reads the local stored value without fetching or syncing.


        :return: str conversion of row_dict.get(origin_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_origin.
        """
        if not self._has_origin:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support origin.")

        return str(self.row_dict.get(self.origin_col))

    @origin.setter
    def origin(self, new_origin: str) -> None:
        """
        Assign the enabled link origin property in the local row mapping.

        No automatic sync or cached row metadata refresh occurs.

        Example:
            On a concrete link with origin enabled, assigning link.origin invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_origin: Proposed origin value, converted with str.
        :return: None; only the local mapping is updated.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_origin.
        """
        if not self._has_origin:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support origin.")

        self.row_dict[self.origin_col] = str(new_origin)

    @property
    def policy(self) -> str:
        """
        Read the enabled link policy property as str.

        Apply str to the stored value; missing/None therefore yields the literal text "None".

        Example:
            For a concrete link with this property enabled and its column configured, link.policy reads the local stored value without fetching or syncing.


        :return: str conversion of row_dict.get(policy_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_policy.
        """
        if not self._has_policy:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support policy.")

        return str(self.row_dict.get(self.policy_col))

    @policy.setter
    def policy(self, new_policy: str) -> None:
        """
        Assign the enabled link policy property in the local row mapping.

        No automatic sync or cached row metadata refresh occurs.

        Example:
            On a concrete link with policy enabled, assigning link.policy invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_policy: Proposed policy value, converted with str.
        :return: None; only the local mapping is updated.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_policy.
        """
        if not self._has_policy:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support policy.")

        self.row_dict[self.policy_col] = str(new_policy)

    @property
    def data(self) -> str:
        """
        Read the enabled link data property as str.

        Apply str to the stored value; missing/None therefore yields the literal text "None".

        Example:
            For a concrete link with this property enabled and its column configured, link.data reads the local stored value without fetching or syncing.


        :return: str conversion of row_dict.get(data_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_data.
        """
        if not self._has_data:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support data.")

        return str(self.row_dict.get(self.data_col))

    @data.setter
    def data(self, new_data: str) -> None:
        """
        Assign the enabled link data property in the local row mapping.

        No automatic sync or cached row metadata refresh occurs.

        Example:
            On a concrete link with data enabled, assigning link.data invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_data: Proposed data value, converted with str.
        :return: None; only the local mapping is updated.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_data.
        """
        if not self._has_data:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support data.")

        self.row_dict[self.data_col] = str(new_data)

    @property
    def index(self) -> str:
        """
        Read the enabled link index property as str.

        Apply str to the stored value; missing/None therefore yields the literal text "None".

        Example:
            For a concrete link with this property enabled and its column configured, link.index reads the local stored value without fetching or syncing.


        :return: str conversion of row_dict.get(index_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_index.
        """
        if not self._has_index:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support index.")

        return str(self.row_dict.get(self.index_col))

    @index.setter
    def index(self, new_index: str) -> None:
        """
        Assign the enabled link index property in the local row mapping.

        No automatic sync or cached row metadata refresh occurs.

        Example:
            On a concrete link with index enabled, assigning link.index invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_index: Proposed index value, converted with str.
        :return: None; only the local mapping is updated.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_index.
        """
        if not self._has_index:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support index.")

        self.row_dict[self.index_col] = str(new_index)

    @property
    def sequence_number(self) -> int:
        """
        Read the enabled link sequence_number property as int.

        Convert the stored value with int; missing or None values raise rather than defaulting to zero.

        Example:
            For a concrete link with this property enabled and its column configured, link.sequence_number reads the local stored value without fetching or syncing.


        :return: int conversion of row_dict.get(sequence_number_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_sequence_number.
        :raises TypeError: The stored value is missing, None or otherwise not int-convertible.
        :raises ValueError: Stored text is not a valid integer.
        """
        if not self._has_sequence_number:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support sequence_number.")
        return int(self.row_dict.get(self.sequence_number_col))

    @sequence_number.setter
    def sequence_number(self, new_sequence_number: int) -> None:
        """
        Assign the enabled link sequence_number property in the local row mapping.

        No automatic sync or cached row metadata refresh occurs.

        Example:
            On a concrete link with sequence_number enabled, assigning link.sequence_number invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_sequence_number: Proposed sequence_number value, converted with int.
        :return: None; only the local mapping is updated.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_sequence_number.
        :raises TypeError: The proposed value cannot be converted to int.
        :raises ValueError: The proposed text is not a valid integer.
        """
        if not self._has_sequence_number:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support sequence_number.")
        self.row_dict[self.sequence_number_col] = int(new_sequence_number)

    @property
    def is_required(self) -> bool:
        """
        Read the enabled link is_required property as bool.

        Apply bool to the stored value; missing/None becomes False, while nonempty text such as "0" is True.

        Example:
            For a concrete link with this property enabled and its column configured, link.is_required reads the local stored value without fetching or syncing.


        :return: bool conversion of row_dict.get(is_required_col).
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_is_required.
        """
        if not self._has_is_required:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support is_required.")
        return bool(self.row_dict.get(self.is_required_col))

    @is_required.setter
    def is_required(self, new_is_required: bool) -> None:
        """
        Assign the enabled link is_required property in the local row mapping.

        No automatic sync or cached row metadata refresh occurs.

        Example:
            On a concrete link with is_required enabled, assigning link.is_required invokes this descriptor; call sync separately to persist ordinary property edits.


        :param new_is_required: Proposed is_required value, converted with int(bool(value)).
        :return: None; only the local mapping is updated.
        :raises NoSuchPropertyForLinkException: The subclass has not enabled _has_is_required.
        """
        if not self._has_is_required:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support is_required.")
        self.row_dict[self.is_required_col] = int(bool(new_is_required))


# Todo: Actually write this
class IntralinkRowAPI(RowAPI):
    """
    Mark the abstract row interface for links within one table.

    This class currently adds no fields, methods or validation beyond RowAPI and remains abstract.

    Example:
        Use IntralinkRowAPI as a type contract when defining a concrete same-table relationship row.
    """


# Todo: Actually write this
class ViewRowAPI(RowAPI):
    """
    Mark the abstract row interface for rows exposed by a database view.

    This class does not force read_only mode or prevent writes; implementations must supply the inherited abstract operations and any view-specific policy.

    Example:
        A view-backed row implementation can inherit ViewRowAPI and define its own loading and write restrictions.
    """
