"""
Exercise the named Core application boundary with in-memory doubles, loopback RPC, and fixture databases.

The doubles intentionally implement only the schema, row, repository, and storage
operations needed by these tests; they do not model production policy or persistence.
Tests cover discovery, wire projection, command receipts, events, ownership, error
parity, and real Catalog/cache/WEMI round trips. RPC cases bind a local HTTP daemon;
database cases mutate the pytest-provided library rather than a user library.
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import threading
import uuid

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from LiuXin_alpha.core import (
    CoreClientAPI,
    CoreCommand,
    CoreHttpDaemon,
    CoreQuery,
    CoreRuntime,
    LocalCoreClient,
    RemoteCoreClient,
    core_client,
    create_core,
)
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.proxies.remote import RemoteProxyError
from LiuXin_alpha.core.services import CoreServiceReconciliationError
from LiuXin_alpha.library import Library
from LiuXin_alpha.storage.api import (
    Digest,
    DigitalAssetRecord,
    FileInfo,
    Location,
    ReplicaRemovalReport,
    StoreConfiguration,
    StoreStatus,
)


class _DriverWrapper:
    """
    Supply minimal Work/Tag identifier and relationship metadata without a database driver.

    Example:
        >>> _DriverWrapper.get_id_column("works")
        'work_id'
    """

    @staticmethod
    def get_id_column(table: str) -> str:
        """
        Return fixed Work/Tag ID columns or a simplistic trailing-s-stripped fallback.

        Example:
            >>> _DriverWrapper.get_id_column("tags")
            'tag_id'


        :param table: Table label, stringified before fixed lookup or fallback construction.
        :return: Identifier-column hint; no schema existence check is performed.
        """
        return {
            "works": "work_id",
            "tags": "tag_id",
        }.get(str(table), "{}_id".format(str(table).rstrip("s")))

    @staticmethod
    def is_view(table: str) -> bool:
        """
        Advertise every requested table as a non-view in this schema double.

        Example:
            >>> _DriverWrapper.is_view("works")
            False


        :param table: Ignored table name accepted for driver-call compatibility.
        :return: False, including for unknown table names.
        """
        del table
        return False

    @staticmethod
    def get_interlinked_tables(table: str) -> list[str]:
        """
        Advertise the Work/Tag pair or only the requested name for other tables.

        Example:
            >>> _DriverWrapper.get_interlinked_tables("tags")
            ['works', 'tags']


        :param table: Exact table name used to select the fixed relationship family.
        :return: New name list including the source table, exercising Core's self-name filtering.
        """
        if table == "works":
            return ["works", "tags"]
        if table == "tags":
            return ["works", "tags"]
        return [table]


@dataclass
class _Database:
    """
    Provide fixed schema metadata while keeping row reads in a separate injected read source.

    driver_wrapper defaults to a fresh _DriverWrapper and may be replaced by a test.

    Example:
        >>> _Database().get_tables()
        ('works', 'tags')
    """

    driver_wrapper: _DriverWrapper = field(default_factory=_DriverWrapper)

    @staticmethod
    def get_tables(force_refresh: bool = False) -> tuple[str, ...]:
        """
        Return the two advertised fake tables without consulting refresh state.

        Example:
            >>> _Database.get_tables(force_refresh=True)
            ('works', 'tags')


        :param force_refresh: Ignored compatibility switch; the schema is fixed.
        :return: Work then Tag table names in a tuple.
        """
        del force_refresh
        return ("works", "tags")

    @staticmethod
    def get_column_headings(table: str) -> set[str]:
        """
        Supply Work fields containing wire-sensitive values or the two Tag fields.

        Example:
            >>> sorted(_Database.get_column_headings("tags"))
            ['tag', 'tag_id']


        :param table: Exact Work/Tag table label; unknown names are permitted.
        :return: Unordered heading set, empty for an unknown table.
        """
        return {
            "works": {
                "work_id",
                "work_title",
                "work_year",
                "work_blob",
                "work_date",
            },
            "tags": {"tag_id", "tag"},
        }.get(table, set())


class _ReadSource:
    """
    Hold two Works, two Tags, and fixed links for database-path application query tests.

    Rows are copied shallowly on read. No query_cache method is supplied, forcing
    Core's structured-query adapter to use its materialized database-side path.

    Example:
        >>> _ReadSource().get_row_from_id("works", 1)["work_title"]
        'Álpha'
    """

    def __init__(self) -> None:
        """
        Seed independent row dictionaries with Unicode text, bytes, dates, and Work-to-Tag links.

        Example:
            >>> source = _ReadSource()
            >>> source.links
            {1: [10], 2: [11]}


        :return: None after setting this instance's mutable rows and links.
        """
        self.rows: dict[str, list[dict[str, Any]]] = {
            "works": [
                {
                    "work_id": 1,
                    "work_title": "Álpha",
                    "work_year": 2020,
                    "work_blob": b"\x00\x01",
                    "work_date": datetime.date(2020, 1, 2),
                },
                {
                    "work_id": 2,
                    "work_title": "Beta",
                    "work_year": 2024,
                    "work_blob": b"\x02",
                    "work_date": datetime.date(2024, 2, 3),
                },
            ],
            "tags": [
                {"tag_id": 10, "tag": "science"},
                {"tag_id": 11, "tag": "history"},
            ],
        }
        self.links = {1: [10], 2: [11]}

    def get_tables(self, force_refresh: bool = False):
        """
        Enumerate current row-map keys without refreshing or sorting them.

        Example:
            >>> _ReadSource().get_tables()
            ('works', 'tags')


        :param force_refresh: Ignored read-source compatibility switch.
        :return: Tuple of table names in dictionary insertion order.
        """
        del force_refresh
        return tuple(self.rows)

    def get_column_headings(self, table: str):
        """
        Infer available headings from the first current row rather than the schema double.

        Example:
            >>> sorted(_ReadSource().get_column_headings("tags"))
            ['tag', 'tag_id']


        :param table: Table key whose current rows should be inspected.
        :return: First-row key set, or empty when the table has no rows or is unknown.
        """
        rows = self.rows.get(table, [])
        return set(rows[0]) if rows else set()

    def get_row_from_id(self, table: str, row_id: int):
        """
        Find the first row whose fake-driver ID equals the requested ID after integer conversion.

        Example:
            >>> _ReadSource().get_row_from_id("tags", 10)
            {'tag_id': 10, 'tag': 'science'}


        :param table: Row-map key and fake-driver identifier-column selector.
        :param row_id: Integer-convertible identifier to compare with each row.
        :return: Shallow row copy or None; malformed stored IDs raise instead of being skipped.
        """
        id_column = _DriverWrapper.get_id_column(table)
        return next(
            (
                dict(row)
                for row in self.rows.get(table, [])
                if int(row[id_column]) == int(row_id)
            ),
            None,
        )

    def get_all_rows(self, table: str, iterator_return: bool = False):
        """
        Copy all current rows in insertion order, always materializing a tuple.

        Example:
            >>> len(_ReadSource().get_all_rows("works", iterator_return=True))
            2


        :param table: Row-map key to enumerate; absent keys contain no rows.
        :param iterator_return: Ignored compatibility option; it never changes the return type.
        :return: Tuple of shallow row copies, empty for unknown tables.
        """
        del iterator_return
        return tuple(dict(row) for row in self.rows.get(table, []))

    def get_interlinked_rows(
        self,
        target_row,
        secondary_table: str,
        type_filter: str | None = None,
    ):
        """
        Follow fixed Work/Tag links in either direction and return matching secondary rows.

        Example:
            >>> _ReadSource().get_interlinked_rows({"work_id": 1}, "tags")
            ({'tag_id': 10, 'tag': 'science'},)


        :param target_row: Mapping identified by a work_id or tag_id key.
        :param secondary_table: tags for Work-to-Tag traversal or works for the reverse direction.
        :param type_filter: Ignored; the double has no typed-link policy.
        :return: Shallow row copies in secondary-table order, or empty for unsupported directions.
        """
        del type_filter
        if "work_id" in target_row and secondary_table == "tags":
            wanted = set(self.links.get(int(target_row["work_id"]), ()))
            return tuple(
                dict(row) for row in self.rows["tags"] if int(row["tag_id"]) in wanted
            )
        if "tag_id" in target_row and secondary_table == "works":
            tag_id = int(target_row["tag_id"])
            return tuple(
                dict(row)
                for row in self.rows["works"]
                if tag_id in self.links.get(int(row["work_id"]), ())
            )
        return ()

    def get_interlink_rows(self, primary_row, secondary_table: str):
        """
        Synthesize raw Work-to-Tag link rows with IDs numbered from one per requested Work.

        Example:
            >>> _ReadSource().get_interlink_rows({"work_id": 1}, "tags")
            ({'work_tag_link_id': 1, 'work_id': 1, 'tag_id': 10},)


        :param primary_row: Mapping containing work_id for the supported link direction.
        :param secondary_table: Must be tags to receive synthetic links.
        :return: Tuple of new link dictionaries, empty for unsupported endpoints or no links.
        """
        if "work_id" not in primary_row or secondary_table != "tags":
            return ()
        return tuple(
            {
                "work_tag_link_id": index,
                "work_id": int(primary_row["work_id"]),
                "tag_id": tag_id,
            }
            for index, tag_id in enumerate(
                self.links.get(int(primary_row["work_id"]), ()),
                start=1,
            )
        )


class _Repository:
    """
    Implement minimal mutable Work CRUD for Core receipt tests, without Catalog validation or transactions.

    Create translates title to work_title; update applies raw keys as supplied.
    Reads return shallow copies, preventing a receipt consumer from replacing stored fields.

    Example:
        >>> _Repository().require(1)["work_title"]
        'Existing'
    """

    def __init__(self) -> None:
        """
        Seed Work 1 and initialize the monotonically increasing creation counter at two.

        Example:
            >>> _Repository().next_id
            2


        :return: None after creating independent mutable repository state.
        """
        self.rows = {
            1: {
                "work_id": 1,
                "work_title": "Existing",
            }
        }
        self.next_id = 2

    def get(self, entity_id: int):
        """
        Retrieve an optional Work by integer-converted key without exposing its stored dictionary.

        Example:
            >>> _Repository().get(9) is None
            True


        :param entity_id: Integer-convertible Work key.
        :return: Shallow Work dictionary copy, or None when absent.
        """
        row = self.rows.get(int(entity_id))
        return None if row is None else dict(row)

    def require(self, entity_id: int):
        """
        Retrieve a Work copy or raise with the caller's identifier when it is absent.

        Example:
            >>> _Repository().require(1)["work_id"]
            1


        :param entity_id: Work key delegated to get.
        :return: Shallow dictionary copy of an existing Work.
        :raises KeyError: If get reports no matching Work.
        """
        row = self.get(entity_id)
        if row is None:
            raise KeyError(entity_id)
        return row

    def list(self, *, limit: int = 100, offset: int = 0):
        """
        Sort Works by dictionary key, shallow-copy all rows, and apply Python slice bounds.

        Example:
            >>> _Repository().list(limit=0)
            ()


        :param limit: Requested slice length; this double does not validate nonnegativity.
        :param offset: Start index used directly in the tuple slice.
        :return: Tuple of Work copies within offset:offset+limit.
        """
        return tuple(dict(row) for _row_id, row in sorted(self.rows.items()))[
            offset : offset + limit
        ]

    def create(self, data):
        """
        Allocate a Work ID and store a work_title from the required input title key.

        The counter advances before title lookup; a missing title consumes an ID.

        Example:
            >>> repository = _Repository()
            >>> repository.create({"title": "New"})
            2


        :param data: Mapping-like input with a title value; other fields are ignored.
        :return: Assigned integer Work ID after its row has been stored.
        """
        entity_id = self.next_id
        self.next_id += 1
        self.rows[entity_id] = {
            "work_id": entity_id,
            "work_title": data["title"],
        }
        return entity_id

    def update(self, entity_id: int, data):
        """
        Update an existing stored Work dictionary directly without translating field names.

        Example:
            >>> repository = _Repository()
            >>> repository.update(1, {"work_title": "Changed"})
            >>> repository.require(1)["work_title"]
            'Changed'


        :param entity_id: Integer-convertible key of an existing Work.
        :param data: Values accepted by dict.update, applied to the stored row.
        :return: None after the update; missing keys and invalid update values raise.
        """
        self.rows[int(entity_id)].update(data)

    def delete(self, entity_id: int):
        """
        Remove one Work dictionary without cascades or ID-counter adjustment.

        Example:
            >>> repository = _Repository()
            >>> repository.delete(1)
            >>> repository.get(1) is None
            True


        :param entity_id: Integer-convertible existing Work key.
        :return: None after removal.
        :raises KeyError: If no row exists at the converted key.
        """
        del self.rows[int(entity_id)]


@dataclass
class _TrackingJobManager:
    """
    Record requested shutdown options without creating, cancelling, or joining real jobs.

    Example:
        >>> manager = _TrackingJobManager()
        >>> manager.shutdown(wait=False, cancel_pending=True)
        >>> manager.shutdown_calls
        [{'wait': False, 'cancel_pending': True}]
    """

    shutdown_calls: list[dict[str, bool]] = field(default_factory=list)

    def shutdown(
        self,
        *,
        wait: bool = True,
        cancel_pending: bool = False,
    ) -> None:
        """
        Append truth-tested shutdown options for lifecycle-ownership assertions.

        Example:
            >>> manager = _TrackingJobManager()
            >>> manager.shutdown()
            >>> manager.shutdown_calls
            [{'wait': True, 'cancel_pending': False}]


        :param wait: Requested join behavior, recorded but not executed.
        :param cancel_pending: Requested cancellation behavior, recorded but not executed.
        :return: None after appending one independent call dictionary.
        """
        self.shutdown_calls.append(
            {
                "wait": bool(wait),
                "cancel_pending": bool(cancel_pending),
            }
        )


@dataclass
class _Library:
    """
    Hold in-memory file bytes and one Store for Core storage/wire tests.

    files maps Asset IDs to Asset/Location/bytes tuples. Replica IDs intentionally
    equal Asset IDs; this simplification is not a production identity guarantee.
    database is an injected schema double and store_ref defaults to UUID(int=1).

    Example:
        >>> library = _Library(_Database())
        >>> asset = library.add_file(b"book")
        >>> library.read_file(asset)
        b'book'
    """

    database: _Database
    files: dict[int, tuple[DigitalAssetRecord, Location, bytes]] = field(
        default_factory=dict
    )
    store_ref: UUID = UUID(int=1)

    def __post_init__(self) -> None:
        """
        Attach a storage adapter referencing this Library and create its single fake Store.

        Example:
            >>> library = _Library(_Database())
            >>> library.storage.library is library
            True


        :return: None after assigning storage and _store; existing file tuples are retained.
        """
        self.storage = _Storage(self)
        self._store = _Store(self.store_ref)

    def add_file(
        self,
        file_bytes: bytes,
        metadata=None,
        *,
        store=None,
        name=None,
        original_name=None,
        media_type=None,
    ):
        """
        Store copied bytes under a length-derived Asset ID with a SHA-256 digest and synthetic key.

        ID allocation uses len(files)+1, so deletions can cause reuse or collision;
        this deliberately small double is not a persistent allocator or policy engine.

        Example:
            >>> library = _Library(_Database())
            >>> library.add_file(b"abc").digital_asset_id
            1


        :param file_bytes: Byte content to hash, measure, and copy into the in-memory registry.
        :param metadata: Ignored metadata placeholder accepted for library-call compatibility.
        :param store: Optional object supplying store_ref; None chooses this Library's default UUID.
        :param name: Ignored requested name; location keys are always objects/file-<id>.
        :param original_name: Ignored source filename.
        :param media_type: Ignored media-type declaration.
        :return: Stored DigitalAssetRecord; no filesystem or backend Store write occurs.
        """
        del metadata, name, original_name, media_type
        asset_id = len(self.files) + 1
        selected_ref = self.store_ref if store is None else store.store_ref
        location = Location(selected_ref, f"objects/file-{asset_id}")
        asset = DigitalAssetRecord(
            asset_id,
            len(file_bytes),
            (Digest("sha256", hashlib.sha256(file_bytes).hexdigest()),),
        )
        self.files[asset_id] = (asset, location, bytes(file_bytes))
        return asset

    def get_store(self, store_ref: UUID):
        """
        Resolve only the Library's single configured Store UUID by direct equality.

        Example:
            >>> _Library(_Database()).get_store(UUID(int=1)).configuration.store_name
            'archive'


        :param store_ref: UUID to compare with the configured store_ref, without coercion.
        :return: Existing _Store instance on equality.
        :raises KeyError: If the requested reference is different.
        """
        if store_ref != self.store_ref:
            raise KeyError(store_ref)
        return self._store

    def iter_stores(self):
        """
        Iterate the one configured fake Store without discovery or status refresh.

        Example:
            >>> len(tuple(_Library(_Database()).iter_stores()))
            1


        :return: Fresh iterator over a one-element tuple containing _store.
        """
        return iter((self._store,))

    def locate_file(self, asset, *, store=None):
        """
        Return a registered Asset's stored Location without honoring a Store preference.

        Example:
            >>> library = _Library(_Database())
            >>> asset = library.add_file(b"abc")
            >>> library.locate_file(asset).key
            'objects/file-1'


        :param asset: DigitalAssetRecord or integer-convertible Asset key.
        :param store: Ignored preference retained only for call compatibility.
        :return: Original Location from the registered tuple; absent Asset IDs raise KeyError.
        """
        del store
        asset_id = (
            asset.digital_asset_id
            if isinstance(asset, DigitalAssetRecord)
            else int(asset)
        )
        return self.files[int(asset_id)][1]

    def read_file(self, asset, *, store=None):
        """
        Return the registered bytes for an Asset without performing a storage read.

        Example:
            >>> library = _Library(_Database())
            >>> asset = library.add_file(b"abc")
            >>> library.read_file(asset.digital_asset_id)
            b'abc'


        :param asset: DigitalAssetRecord or integer-convertible Asset key.
        :param store: Ignored Store preference.
        :return: Stored bytes object; absent Asset IDs raise KeyError.
        """
        del store
        asset_id = (
            asset.digital_asset_id
            if isinstance(asset, DigitalAssetRecord)
            else int(asset)
        )
        return self.files[int(asset_id)][2]

    def delete_file(self, replica_id: int):
        """
        Remove the file tuple at the replica-as-Asset key and synthesize a deletion report.

        tombstone_retained mirrors successful removal in the report, but this double
        does not actually persist tombstones. Repeating a deletion reports no change.

        Example:
            >>> _Library(_Database()).delete_file(99).bytes_deleted
            False


        :param replica_id: Integer-convertible registry key; replica and Asset IDs coincide only in this double.
        :return: ReplicaRemovalReport marking bytes_deleted/tombstone_retained on removal and replica_forgotten=False.
        """
        removed = self.files.pop(int(replica_id), None) is not None
        return ReplicaRemovalReport(
            replica_id,
            bytes_deleted=removed,
            replica_forgotten=False,
            tombstone_retained=removed,
        )

    def iter_files(self):
        """
        Snapshot registered Asset objects in file-dictionary insertion order and iterate them.

        Example:
            >>> tuple(_Library(_Database()).iter_files())
            ()


        :return: Iterator over a tuple of current Asset references, independent of later dictionary edits.
        """
        return iter(tuple(asset for asset, _location, _data in self.files.values()))


class _Store:
    """
    Expose fixed in-memory Store configuration and an always-available status.

    Example:
        >>> _Store(UUID(int=1)).configuration.store_root_uri
        'memory://archive'
    """

    def __init__(self, store_ref: UUID) -> None:
        """
        Retain the supplied UUID and declare an archive Store at memory://archive.

        Example:
            >>> _Store(UUID(int=1)).store_ref == UUID(int=1)
            True


        :param store_ref: Store identity retained on the instance and in its configuration.
        :return: None after creating fixed name/kind/root configuration; no backend is opened.
        """
        self.store_ref = store_ref
        self.configuration = StoreConfiguration(
            store_ref,
            "archive",
            "memory",
            "memory://archive",
        )

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Return a fixed available, writable status without probing or changing the Store.

        Example:
            >>> _Store(UUID(int=1)).status() == StoreStatus(True, True)
            True


        :param refresh: Ignored compatibility switch; no physical observations are made.
        :return: Fresh StoreStatus with available/writable True and remaining default fields.
        """
        del refresh
        return StoreStatus(True, True)


class _Storage:
    """
    Resolve and read the Library double's in-memory file tuples without a physical storage driver.

    Example:
        >>> library = _Library(_Database())
        >>> asset = library.add_file(b"abc")
        >>> library.storage.read_bytes(library.locate_file(asset))
        b'abc'
    """

    def __init__(self, library: _Library) -> None:
        """
        Retain the Library by reference so later file additions and removals remain visible.

        Example:
            >>> library = _Library(_Database())
            >>> _Storage(library).library is library
            True


        :param library: Mutable file registry used by stat, reads, and Asset resolution.
        :return: None after storing the registry owner; no file snapshot is created.
        """
        self.library = library

    def stat(self, location: Location) -> FileInfo:
        """
        Scan registered locations for equality and return the matching byte length.

        Example:
            >>> library = _Library(_Database())
            >>> asset = library.add_file(b"abc")
            >>> library.storage.stat(library.locate_file(asset)).size
            3


        :param location: Location value compared with each registered candidate in insertion order.
        :return: FileInfo for the first equal Location with only location/size explicitly supplied.
        :raises FileNotFoundError: If no registered Location compares equal.
        """
        for _asset, candidate, data in self.library.files.values():
            if candidate == location:
                return FileInfo(location, len(data))
        raise FileNotFoundError(location.key)

    def read_bytes(self, location: Location) -> bytes:
        """
        Scan for the first equal registered Location and return its stored bytes.

        Example:
            >>> library = _Library(_Database())
            >>> asset = library.add_file(b"abc")
            >>> library.storage.read_bytes(library.locate_file(asset))
            b'abc'


        :param location: Complete Location value used for equality matching.
        :return: Original registered byte value without backend I/O or a new copy.
        :raises FileNotFoundError: If no matching Location exists in the registry.
        """
        for _asset, candidate, data in self.library.files.values():
            if candidate == location:
                return data
        raise FileNotFoundError(location.key)

    def resolve_digital_asset(
        self,
        digital_asset_id: int,
        *,
        preferred_store_ref=None,
    ):
        """
        Build a minimal Asset/replica/location resolution directly from the file registry.

        Example:
            >>> library = _Library(_Database())
            >>> asset = library.add_file(b"abc")
            >>> library.storage.resolve_digital_asset(asset.digital_asset_id).replica_record.replica_id
            1


        :param digital_asset_id: Integer-convertible Asset key, also reused as the fake replica ID.
        :param preferred_store_ref: Ignored preference; only one stored Location is available per Asset.
        :return: SimpleNamespace with asset_record, minimal replica_record, and stored location.
        """
        del preferred_store_ref
        asset, location, _data = self.library.files[int(digital_asset_id)]
        return SimpleNamespace(
            asset_record=asset,
            replica_record=SimpleNamespace(replica_id=int(digital_asset_id)),
            location=location,
        )


def _fake_runtime() -> CoreRuntime:
    """
    Compose a fresh uncached Core with schema/read/storage doubles and a single Work repository.

    Fixed Core UUID/version make local and remote descriptions comparable. This
    is not a full Catalog implementation; tests requiring real semantics use db.

    Example:
        >>> runtime = _fake_runtime()  # doctest: +SKIP
        >>> runtime.shutdown()  # doctest: +SKIP


    :return: Live CoreRuntime whose job/lifecycle resources remain the caller's responsibility.
    """
    database = _Database()
    repository = _Repository()
    catalog = SimpleNamespace(
        db=database,
        repositories=SimpleNamespace(works=repository),
    )
    return CoreRuntime(
        library=_Library(database=database),
        catalog=catalog,
        read_source=_ReadSource(),
        core_uuid="core-application-test",
        core_version="core-test",
    )


@pytest.mark.parametrize("sort", ([], [{"field": "work_title", "ascending": False}]))
@pytest.mark.parametrize("offset", (0, 20))
def test_count_only_query_does_not_coerce_or_order_text_row_identifiers(
    sort,
    offset,
) -> None:
    """
    Prove zero-limit database queries filter/count text-ID rows without sorting or projecting IDs.

    The fake read source contains nonnumeric bookkeeping-style IDs that would
    fail ordinary record construction. Both requested sort modes and offsets must
    retain the matching count while returning no records.

    Example:
        >>> test_count_only_query_does_not_coerce_or_order_text_row_identifiers([], 0)  # doctest: +SKIP


    :param sort: Parametrized empty or descending-title structured sort list.
    :param offset: Parametrized zero or beyond-end page offset.
    :return: None after count, empty records, completeness, and echoed bounds are verified; the runtime is shut down.
    """
    runtime = _fake_runtime()
    runtime.services.read_source.rows["works"] = [
        {"work_id": "migration-first", "work_title": "ledger applied"},
        {"work_id": "migration-second", "work_title": "ledger pending"},
    ]
    try:
        result = runtime.query(
            "rows.query",
            {
                "table": "works",
                "limit": 0,
                "offset": offset,
                "sort": sort,
                "predicates": [
                    {"field": "work_title", "operator": "contains", "value": "applied"},
                ],
            },
        )
        assert result["records"] == []
        assert result["total_count"] == 1
        assert result["complete"] is True
        assert result["offset"] == offset
        assert result["limit"] == 0
    finally:
        runtime.shutdown()


def test_core_describes_complete_named_application_api() -> None:
    """
    Check named operation discovery, v2 metadata, capability registration, and transport-stability flags.

    The operation assertions are minimum subsets, not a claim that this double
    implements every advertised service. Only generic invoke remains transport-unstable.

    Example:
        >>> test_core_describes_complete_named_application_api()  # doctest: +SKIP


    :return: None after checking discovery metadata against the expected boundary families.
    """
    runtime = _fake_runtime()

    described = runtime.describe_api(include_targets=False)
    command_entries = {entry["name"]: entry for entry in described["commands"]}
    query_entries = {entry["name"]: entry for entry in described["queries"]}

    assert described["api_version"] == "2.0"
    assert described["services"]["catalog"] == "SimpleNamespace"
    assert {
        "invoke",
        "shutdown",
        "sync.store.start",
        "sync.store.cancel",
        "jobs.cancel",
        "metadata.write",
        "metadata.tags.replace",
        "metadata.labels.replace",
        "metadata.genre.replace",
        "metadata.series.replace",
        "metadata.identifiers.replace",
        "catalog.entity.create",
        "catalog.entity.update",
        "catalog.entity.delete",
        "catalog.entity.match-or-create",
        "catalog.agent.create-person",
        "catalog.agent.create-organisation",
        "catalog.wemi.create",
        "catalog.wemi.link",
        "catalog.wemi.unlink",
        "catalog.metadata.attach",
        "catalog.metadata.replace",
        "catalog.metadata.merge",
        "catalog.field.write",
        "catalog.field.write-one",
        "admin.row.create",
        "admin.row.update",
        "admin.row.delete",
        "admin.relation.link",
        "admin.relation.unlink",
        "storage.store.save",
        "storage.refresh",
        "storage.file.put",
        "storage.file.delete",
        "cache.reload",
        "read-source.refresh",
    } <= set(command_entries)
    assert {
        "invoke",
        "health",
        "api.describe",
        "jobs.list",
        "jobs.get",
        "jobs.wait",
        "schema.tables",
        "schema.table",
        "rows.get",
        "rows.query",
        "relations.list",
        "admin.row.delete-impact",
        "catalog.entity.get",
        "catalog.entity.list",
        "catalog.bundle.get",
        "catalog.graph.get",
        "catalog.item.summary",
        "catalog.match",
        "catalog.agent.resolve",
        "catalog.annotations.list",
        "metadata.get",
        "metadata.opf.export",
        "cache.status",
        "storage.stores.list",
        "storage.files.list",
        "storage.file.locate",
        "storage.file.read",
    } <= set(query_entries)
    capabilities = runtime.query("capabilities.list")
    assert capabilities["complete_program_boundary"] is True
    declared_program_operations = {
        operation
        for family in capabilities["families"].values()
        for operation in family["operations"]
    }
    assert declared_program_operations <= (set(command_entries) | set(query_entries))
    assert all(
        detail["call_modes"] == ["direct", "rpc"]
        for detail in capabilities["operations"].values()
    )
    assert command_entries["invoke"]["transport_stable"] is False
    assert query_entries["invoke"]["transport_stable"] is False
    assert all(
        entry["transport_stable"] is True
        for name, entry in command_entries.items()
        if name != "invoke"
    )
    assert all(
        entry["transport_stable"] is True
        for name, entry in query_entries.items()
        if name != "invoke"
    )


def test_core_factory_and_client_selector_preserve_one_contract() -> None:
    """
    Verify factory composition satisfies CoreClientAPI and client selection rejects absent or competing choices.

    The example.test endpoint is used only in an invalid argument combination;
    the test does not connect to it.

    Example:
        >>> test_core_factory_and_client_selector_preserve_one_contract()  # doctest: +SKIP


    :return: None after runtime identity, protocol conformance, and selector ValueErrors are asserted.
    """
    database = _Database()
    runtime = create_core(
        library=_Library(database=database),
        catalog=SimpleNamespace(
            db=database,
            repositories=SimpleNamespace(works=_Repository()),
        ),
        read_source=_ReadSource(),
        core_uuid="factory-core",
    )

    assert core_client(runtime=runtime) is runtime
    assert isinstance(runtime, CoreClientAPI)
    with pytest.raises(ValueError):
        core_client()
    with pytest.raises(ValueError):
        core_client(
            runtime=runtime,
            endpoint="http://example.test",
        )


def test_core_shutdown_respects_injected_job_manager_ownership() -> None:
    """
    Distinguish a borrowed job manager from an explicitly owned injected manager during shutdown.

    Recording doubles prove borrowed managers are untouched while owned managers
    receive nonblocking shutdown with pending-job cancellation.

    Example:
        >>> test_core_shutdown_respects_injected_job_manager_ownership()  # doctest: +SKIP


    :return: None after verifying both managers' recorded shutdown calls.
    """
    database = _Database()
    supplied = _TrackingJobManager()
    runtime = CoreRuntime(
        library=_Library(database=database),
        catalog=SimpleNamespace(
            db=database,
            repositories=SimpleNamespace(works=_Repository()),
        ),
        read_source=_ReadSource(),
        job_manager=supplied,
    )
    runtime.shutdown()
    assert supplied.shutdown_calls == []

    database = _Database()
    owned = _TrackingJobManager()
    runtime = CoreRuntime(
        library=_Library(database=database),
        catalog=SimpleNamespace(
            db=database,
            repositories=SimpleNamespace(works=_Repository()),
        ),
        read_source=_ReadSource(),
        job_manager=owned,
        close_job_manager_on_shutdown=True,
    )
    runtime.shutdown()
    assert owned.shutdown_calls == [
        {
            "wait": False,
            "cancel_pending": True,
        }
    ]


def test_core_rows_query_and_relations_are_transport_shaped() -> None:
    """
    Verify database-path filtering, text search, sort, paging, ID retention, and bytes/date wire projection.

    Also request related Tags and raw link rows from the fixed Work/Tag double.
    Local runtime calls must already return wire-shaped values without an HTTP hop.

    Example:
        >>> test_core_rows_query_and_relations_are_transport_shaped()  # doctest: +SKIP


    :return: None after exact page shape and related/link field values are asserted.
    """
    runtime = _fake_runtime()

    page = runtime.query(
        "rows.query",
        {
            "table": "works",
            "predicates": [
                {
                    "field": "work_year",
                    "operator": "gte",
                    "value": 2020,
                }
            ],
            "text": "a",
            "text_fields": ["work_title"],
            "sort": [
                {
                    "field": "work_year",
                    "ascending": False,
                }
            ],
            "projection": [
                "work_title",
                "work_blob",
                "work_date",
            ],
            "offset": 0,
            "limit": 1,
        },
    )

    assert page["source"] == "database"
    assert page["total_count"] == 2
    assert page["records"] == [
        {
            "table": "works",
            "row_id": 2,
            "values": {
                "work_title": "Beta",
                "work_blob": {
                    "$type": "bytes",
                    "base64": "Ag==",
                },
                "work_date": {
                    "$type": "date",
                    "iso": "2024-02-03",
                },
                "work_id": 2,
            },
        }
    ]

    related = runtime.query(
        "relations.list",
        {
            "table": "works",
            "row_id": 1,
            "related_table": "tags",
            "include_link_rows": True,
        },
    )
    assert related["records"][0]["values"]["tag"] == "science"
    assert related["link_records"][0]["values"]["tag_id"] == 10


def test_core_catalog_repository_commands_return_receipts() -> None:
    """
    Check Work creation readback, uncached reconciliation flags, and command-correlated write events.

    Example:
        >>> test_core_catalog_repository_commands_return_receipts()  # doctest: +SKIP


    :return: None after the fake repository receipt, subsequent fetch, and emitted command identity agree.
    """
    runtime = _fake_runtime()
    events = []
    runtime.subscribe(events.append)

    created = runtime.command(
        "catalog.entity.create",
        {
            "repository": "works",
            "data": {"title": "Created through Core"},
        },
        command_id="create-work-2",
    )

    assert created == {
        "repository": "works",
        "entity_id": 2,
        "entity": {
            "work_id": 2,
            "work_title": "Created through Core",
        },
        "cache": {
            "configured": False,
            "reconciled": False,
        },
    }
    fetched = runtime.query(
        "catalog.entity.get",
        {
            "repository": "works",
            "entity_id": 2,
        },
    )
    assert fetched["entity"]["work_title"] == "Created through Core"
    write_events = [event for event in events if event.event_type == "write.completed"]
    assert write_events[-1].payload["name"] == "catalog.entity.create"
    assert write_events[-1].payload["command_id"] == "create-work-2"


def test_core_storage_file_api_uses_explicit_wire_bytes() -> None:
    """
    Round-trip NUL-containing file bytes through put/list/read/delete using the in-memory Library.

    The read response must use the explicit base64 wire tag, and deletion addresses
    the returned replica ID rather than assuming production Asset/replica identity.

    Example:
        >>> test_core_storage_file_api_uses_explicit_wire_bytes()  # doctest: +SKIP


    :return: None after size, location, listing identity, encoded bytes, and deletion outcome checks.
    """
    runtime = _fake_runtime()

    stored = runtime.command(
        "storage.file.put",
        {
            "content_base64": "aGVsbG8AY29yZQ==",
            "store_uuid": str(UUID(int=1)),
        },
    )
    asset_id = int(stored["asset"]["digital_asset_id"])
    replica_id = int(stored["replica_id"])

    assert stored["size"] == 10
    assert stored["location"]["key"] == "objects/file-1"
    listed = runtime.query(
        "storage.files.list",
        {
            "limit": 10,
            "offset": 0,
        },
    )
    assert listed["total_count"] == 1
    assert int(listed["files"][0]["digital_asset_id"]) == asset_id
    read = runtime.query(
        "storage.file.read",
        {
            "asset_id": asset_id,
            "store_uuid": str(UUID(int=1)),
        },
    )
    assert read["content"] == {
        "$type": "bytes",
        "base64": "aGVsbG8AY29yZQ==",
    }
    assert (
        runtime.command(
            "storage.file.delete",
            {
                "replica_id": replica_id,
            },
        )["deleted"]
        is True
    )


def test_local_and_rpc_core_clients_have_envelope_and_result_parity() -> None:
    """
    Compare local and loopback HTTP clients for protocol identity, envelopes, results, and API descriptions.

    A fixed query/correlation ID must survive both paths. The test starts a local
    daemon and stops it in the comparison block's finally clause.

    Example:
        >>> test_local_and_rpc_core_clients_have_envelope_and_result_parity()  # doctest: +SKIP


    :return: None after direct/RPC values and metadata compare equal.
    """
    runtime = _fake_runtime()
    local = LocalCoreClient(runtime)
    daemon = CoreHttpDaemon(runtime, endpoint_namespace="core-v1")
    daemon.start()
    remote = RemoteCoreClient(endpoint=daemon.base_url)
    try:
        assert isinstance(runtime, CoreClientAPI)
        assert isinstance(local, CoreClientAPI)
        assert isinstance(remote, CoreClientAPI)
        assert remote.core_uuid == local.core_uuid == runtime.core_uuid
        assert remote.core_version == local.core_version == "core-test"
        assert remote.api_version == local.api_version == "2.0"

        envelope = CoreQuery(
            name="rows.get",
            payload={
                "table": "works",
                "row_id": 1,
            },
            query_id="query-envelope-id",
            correlation_id="correlation-id",
        )
        local_result = local.execute_query(envelope)
        remote_result = remote.execute_query(envelope)

        assert remote_result == local_result
        assert remote_result.query_id == "query-envelope-id"
        assert remote_result.correlation_id == "correlation-id"
        assert remote.describe_api(include_targets=False) == local.describe_api(
            include_targets=False
        )
    finally:
        daemon.stop()


def test_rpc_core_client_subscribes_to_typed_events() -> None:
    """
    Observe command completion and a typed write event through a loopback RPC subscription.

    The subscription has a three-second completion wait and is unsubscribed before
    the local daemon is stopped, including assertion failures inside the try block.

    Example:
        >>> test_rpc_core_client_subscribes_to_typed_events()  # doctest: +SKIP


    :return: None after receiving completion and the expected Catalog write event.
    """
    runtime = _fake_runtime()
    daemon = CoreHttpDaemon(runtime, endpoint_namespace="core-events")
    daemon.start()
    remote = RemoteCoreClient(endpoint=daemon.base_url)
    received = []
    finished = threading.Event()

    def _capture(event) -> None:
        """
        Record each remote event and release the waiting assertion on command.finished.

        Example:
            >>> _capture(event)  # doctest: +SKIP


        :param event: Typed Core event delivered by the active remote subscription.
        :return: None after appending to received and conditionally setting finished.
        """
        received.append(event)
        if event.event_type == "command.finished":
            finished.set()

    unsubscribe = remote.subscribe(_capture)
    try:
        remote.command(
            "catalog.entity.create",
            {
                "repository": "works",
                "data": {"title": "Event through RPC"},
            },
        )
        assert finished.wait(timeout=3.0) is True
        assert any(
            event.event_type == "write.completed"
            and event.payload["name"] == "catalog.entity.create"
            for event in received
        )
    finally:
        unsubscribe()
        daemon.stop()


def test_reconciliation_receipt_survives_direct_and_rpc_errors() -> None:
    """
    Preserve a committed-write receipt and reconciliation error code across local and HTTP error wrapping.

    The handler synthesizes failure without performing a real write; the test
    verifies error transport, not rollback or cache recovery behavior.

    Example:
        >>> test_reconciliation_receipt_survives_direct_and_rpc_errors()  # doctest: +SKIP


    :return: None after matching direct/remote error details, with the local daemon stopped.
    """
    runtime = _fake_runtime()

    def _fail_reconciliation(runtime, command):
        """
        Raise a fixed reconciliation failure carrying a synthetic already-committed entity receipt.

        Example:
            >>> _fail_reconciliation(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored handler runtime; no services are called.
        :param command: Ignored command envelope; the failure is unconditional.
        :return: Never returns normally.
        :raises CoreServiceReconciliationError: Always, with entity_id=7 and committed=True in its receipt.
        """
        del runtime, command
        raise CoreServiceReconciliationError(
            "cache refresh failed",
            receipt={"entity_id": 7, "committed": True},
        )

    runtime.register_command_handler(
        "test.reconciliation-failure",
        _fail_reconciliation,
    )
    with pytest.raises(CoreHandlerError) as direct_error:
        runtime.command("test.reconciliation-failure")
    assert direct_error.value.code == "cache_reconciliation_failed"
    assert direct_error.value.details == {
        "receipt": {
            "entity_id": 7,
            "committed": True,
        },
        "canonical_write_committed": True,
    }

    daemon = CoreHttpDaemon(runtime, endpoint_namespace="core-errors")
    daemon.start()
    try:
        remote = RemoteCoreClient(endpoint=daemon.base_url)
        with pytest.raises(RemoteProxyError) as remote_error:
            remote.command("test.reconciliation-failure")
        assert remote_error.value.code == "cache_reconciliation_failed"
        assert remote_error.value.details == direct_error.value.details
    finally:
        daemon.stop()


def test_stable_handler_rejects_non_wire_results_as_structured_errors() -> None:
    """
    Reject arbitrary object results from stable query/command handlers through structured Core errors.

    Both paths must report CoreWireError inside handler_error; the command path
    must also publish command.failed for the registered name.

    Example:
        >>> test_stable_handler_rejects_non_wire_results_as_structured_errors()  # doctest: +SKIP


    :return: None after error-code/detail parity and the failure event are checked.
    """
    runtime = _fake_runtime()
    events = []
    runtime.subscribe(events.append)
    runtime.register_query_handler(
        "test.non-wire-query",
        lambda _runtime, _query: object(),
    )
    runtime.register_command_handler(
        "test.non-wire-command",
        lambda _runtime, _command: object(),
    )

    with pytest.raises(CoreHandlerError) as query_error:
        runtime.query("test.non-wire-query")
    assert query_error.value.code == "handler_error"
    assert query_error.value.details == {
        "exception_type": "CoreWireError",
    }

    with pytest.raises(CoreHandlerError) as command_error:
        runtime.command("test.non-wire-command")
    assert command_error.value.code == "handler_error"
    assert command_error.value.details == query_error.value.details
    assert any(
        event.event_type == "command.failed"
        and event.payload["name"] == "test.non-wire-command"
        for event in events
    )


def test_core_catalog_and_cache_api_round_trip_real_database(db) -> None:
    """
    Exercise real Catalog writes, WEMI hydration, OPF export, matching, and field updates through a schema-backed cache.

    A fixture Library shares Core's cache with storage. The test verifies mutation
    receipts and subsequent cache reads, including deletion and cache detachment on
    shutdown. Data is created in the supplied test database; the Library does not
    take ownership of closing that database.

    Example:
        >>> test_core_catalog_and_cache_api_round_trip_real_database(db)  # doctest: +SKIP


    :param db: Pytest-provided writable database with the production schema and storage integration.
    :return: None after semantic/readback assertions and runtime shutdown detach the owned cache.
    """
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
        cache_type="schema_backed",
        close_cache_on_shutdown=True,
    )
    assert db.storage is not None
    assert db.storage.metadata_cache is runtime.cache
    title = "Core API {}".format(uuid.uuid4())
    try:
        created = runtime.command(
            "catalog.entity.create",
            {
                "repository": "works",
                "data": {
                    "title": title,
                    "canonical_title": title,
                },
            },
        )
        work_id = int(created["entity_id"])
        assert created["cache"]["configured"] is True
        assert created["cache"]["reconciled"] is True

        cached = runtime.query(
            "rows.query",
            {
                "table": "works",
                "predicates": [
                    {
                        "field": "work_title",
                        "operator": "eq",
                        "value": title,
                    }
                ],
            },
        )
        assert cached["source"] == "cache"
        assert cached["complete"] is True
        assert [record["row_id"] for record in cached["records"]] == [work_id]

        updated = runtime.command(
            "catalog.entity.update",
            {
                "repository": "works",
                "entity_id": work_id,
                "data": {
                    "canonical_title": "{} updated".format(title),
                },
            },
        )
        assert updated["entity"]["work_canonical_title"] == "{} updated".format(title)
        assert runtime.query(
            "rows.get",
            {
                "table": "works",
                "row_id": work_id,
            },
        )["record"]["values"]["work_canonical_title"] == "{} updated".format(title)

        stack_title = "{} stack".format(title)
        stack = runtime.command(
            "catalog.wemi.create",
            {
                "work": {"title": stack_title},
                "expression": {"label": "Core API expression"},
                "manifestation": {"subtitle": "Core API manifestation"},
                "items": [{"inventory_code": "core-api-item"}],
                "origin": "core-api-test",
            },
        )
        assert len(stack["item_ids"]) == 1
        bundle = runtime.query(
            "catalog.bundle.get",
            {
                "level": "item",
                "entity_id": stack["item_ids"][0],
            },
        )
        assert bundle["work"]["work_id"] == stack["work_id"]
        assert bundle["work"]["work_title"] == stack_title

        metadata = runtime.query(
            "metadata.get",
            {
                "item_id": stack["item_ids"][0],
            },
        )
        assert metadata["database_ids"]["item_id"] == stack["item_ids"][0]
        opf = runtime.query(
            "metadata.opf.export",
            {
                "item_id": stack["item_ids"][0],
            },
        )
        assert opf["content"]["$type"] == "bytes"
        assert b"<package" in base64.b64decode(opf["content"]["base64"])

        metadata_write = runtime.command(
            "metadata.tags.replace",
            {
                "item_id": stack["item_ids"][0],
                "tags": ["Core API Tag"],
                "kind": "liuxin",
            },
        )
        assert metadata_write["changed"] is True
        assert metadata_write["cache"]["configured"] is True
        assert metadata_write["cache"]["reconciled"] is True
        assert "Core API Tag" in str(
            runtime.query(
                "metadata.get",
                {
                    "item_id": stack["item_ids"][0],
                },
            )
        )

        matched = runtime.command(
            "catalog.entity.match-or-create",
            {
                "repository": "works",
                "candidate": {"title": stack_title},
            },
        )
        assert matched["entity_id"] == stack["work_id"]

        person = runtime.command(
            "catalog.agent.create-person",
            {
                "data": {
                    "name": "Core Person {}".format(uuid.uuid4()),
                },
                "details": {},
            },
        )
        assert person["kind"] == "person"
        assert person["agent"]["agent_type"] == "person"

        field_tag = "Core Field Tag {}".format(uuid.uuid4())
        field_write = runtime.command(
            "catalog.field.write-one",
            {
                "src_table": "works",
                "dst_column": "tag",
                "src_id": stack["work_id"],
                "dst_value": field_tag,
            },
        )
        assert field_write["cache"] == {
            "configured": True,
            "reconciled": True,
        }
        related_tags = runtime.query(
            "relations.list",
            {
                "table": "works",
                "row_id": stack["work_id"],
                "related_table": "tags",
            },
        )
        assert related_tags["source"] == "cache"
        assert [record["values"]["tag"] for record in related_tags["records"]] == [
            field_tag
        ]

        deleted = runtime.command(
            "catalog.entity.delete",
            {
                "repository": "works",
                "entity_id": work_id,
            },
        )
        assert deleted["deleted"]["work_id"] == work_id
        assert (
            runtime.query(
                "rows.get",
                {
                    "table": "works",
                    "row_id": work_id,
                },
            )["record"]
            is None
        )
    finally:
        runtime.shutdown()
    assert db.storage.metadata_cache is None


def test_catalog_conveniences_have_direct_and_rpc_parity(db) -> None:
    """
    Verify Catalog convenience operations retain one transport-stable contract over a real fixture database.

    Create a WEMI path, annotation, and Work metadata directly, then link/unlink
    over loopback RPC. Compare graph, hierarchy, and identifier reads across both
    clients and filter remote annotations. The daemon and runtime are closed in
    finally without taking ownership of the fixture database.

    Example:
        >>> test_catalog_conveniences_have_direct_and_rpc_parity(db)  # doctest: +SKIP


    :param db: Pytest-provided writable production-schema database for the semantic round trip.
    :return: None after local/RPC parity, annotation filtering, and unlink outcome checks.
    """

    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    daemon = None
    title = "Core convenience {}".format(uuid.uuid4().hex)
    try:
        stack = runtime.command(
            "catalog.wemi.create",
            {
                "work": {"title": title},
                "expression": {"label": "Primary expression"},
                "manifestation": {"subtitle": "Primary manifestation"},
                "items": [{"inventory_code": uuid.uuid4().hex}],
            },
        )
        expression = runtime.command(
            "catalog.entity.create",
            {
                "repository": "expressions",
                "data": {"label": "RPC expression"},
            },
        )
        annotation = runtime.command(
            "catalog.entity.create",
            {
                "repository": "annotations",
                "data": {
                    "item_id": stack["item_ids"][0],
                    "user_id": 23,
                    "kind": "highlight",
                    "anchor_type": "offset",
                    "anchor_start": "4",
                    "source": "core-convenience-test",
                },
            },
        )
        runtime.command(
            "catalog.metadata.replace",
            {
                "level": "work",
                "entity_id": stack["work_id"],
                "data": {
                    "identifiers": {
                        "doi": "10.1000/core-convenience",
                    },
                    "notes": ["Written through direct Core"],
                },
            },
        )

        daemon = CoreHttpDaemon(
            runtime,
            endpoint_namespace="catalog-conveniences",
        )
        daemon.start()
        remote = RemoteCoreClient(endpoint=daemon.base_url)
        linked = remote.command(
            "catalog.wemi.link",
            {
                "parent_level": "work",
                "parent_id": stack["work_id"],
                "child_level": "expression",
                "child_id": expression["entity_id"],
                "primary": True,
                "origin": "rpc",
            },
        )
        assert linked["child_id"] == expression["entity_id"]
        assert linked["cache"] == {
            "configured": False,
            "reconciled": False,
        }

        graph_payload = {
            "work_id": stack["work_id"],
            "max_expressions": 10,
            "max_manifestations": 10,
            "max_items": 10,
        }
        assert remote.query("catalog.graph.get", graph_payload) == (
            runtime.query("catalog.graph.get", graph_payload)
        )
        assert remote.query(
            "catalog.hierarchy.list",
            {
                "level": "work",
                "entity_id": stack["work_id"],
                "direction": "children",
            },
        ) == runtime.query(
            "catalog.hierarchy.list",
            {
                "level": "work",
                "entity_id": stack["work_id"],
                "direction": "children",
            },
        )
        primary_payload = {
            "level": "work",
            "entity_id": stack["work_id"],
        }
        assert remote.query(
            "catalog.identifiers.primary-values",
            primary_payload,
        ) == runtime.query(
            "catalog.identifiers.primary-values",
            primary_payload,
        )
        annotations = remote.query(
            "catalog.annotations.list",
            {
                "item_id": stack["item_ids"][0],
                "user_id": 23,
                "kind": "highlight",
            },
        )
        assert annotations["count"] == 1
        assert (
            annotations["annotations"][0]["annotation_id"] == (annotation["entity_id"])
        )

        unlinked = remote.command(
            "catalog.wemi.unlink",
            {
                "parent_level": "work",
                "parent_id": stack["work_id"],
                "child_level": "expression",
                "child_id": expression["entity_id"],
            },
        )
        assert unlinked["unlinked"] is True
    finally:
        if daemon is not None:
            daemon.stop()
        runtime.shutdown()
