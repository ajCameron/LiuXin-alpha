"""Current orchestration contracts for :mod:`storage.store_manager`."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import LiuXin_alpha.storage.store_manager as store_manager_module
from LiuXin_alpha.storage import StoreContainer
from LiuXin_alpha.storage.api import ItemStorageHints, StoreSpec
from LiuXin_alpha.storage.store_manager import (
    StorageBootstrapReport,
    StorageManager,
)
from tests.storage.api.test_storage_api_redraft import _DummyPlugin


def _plugin(
    name: str,
    *,
    url: str | None = None,
    uuid: str | None = None,
    writable: bool = True,
    supports_location: bool = True,
) -> _DummyPlugin:
    return _DummyPlugin(
        url or f"dummy://{name}",
        name=name,
        uuid=uuid if uuid is not None else f"uuid-{name}",
        writable=writable,
        supports_location=supports_location,
    )


def _container(
    name: str,
    *,
    url: str | None = None,
    uuid: str | None = None,
) -> StoreContainer:
    return StoreContainer.from_plugin(_plugin(name, url=url, uuid=uuid))


def _spec(
    *,
    store_id: int | None = 1,
    name: str = "store",
    kind: str = "ftp_readonly",
    url: str = "ftp://example.test/library",
    protocol: str | None = "ftp",
    root_uri: str | None = None,
    read_only: bool = True,
    policy: str | None = None,
) -> StoreSpec:
    return StoreSpec(
        store_id=store_id,
        store_uuid=f"uuid-{name}",
        store_name=name,
        store_kind=kind,
        store_url=url,
        store_access_protocol=protocol,
        store_root_uri=root_uri if root_uri is not None else url,
        store_is_read_only=read_only,
        store_policy_json=policy,
    )


def test_bootstrap_report_ok_tracks_failed_rows_only() -> None:
    report = StorageBootstrapReport(skipped_rows=2)
    assert report.ok

    report.failed_rows = 1
    assert not report.ok


def test_manager_initialization_accepts_plugins_and_containers_and_starts_them() -> None:
    plugin = _plugin("plugin")
    container = _container("container")

    manager = StorageManager(stores=[plugin, container], startup_on_add=True)

    assert [store.store_name for store in manager.iter_store_containers()] == [
        "plugin",
        "container",
    ]
    assert plugin.startup_calls == 1
    assert container.plugin.startup_calls == 1

    with pytest.raises(TypeError, match="StoreContainerAPI or StorePluginAPI"):
        StorageManager(stores=[object()])


@pytest.mark.parametrize(
    ("first", "second", "message"),
    (
        (
            _container("same-name", url="dummy://one", uuid="uuid-one"),
            _container("same-name", url="dummy://two", uuid="uuid-two"),
            "Duplicate store name",
        ),
        (
            _container("one", url="dummy://same-url", uuid="uuid-one"),
            _container("two", url="dummy://same-url", uuid="uuid-two"),
            "Duplicate store url",
        ),
        (
            _container("one", url="dummy://one", uuid="uuid-same"),
            _container("two", url="dummy://two", uuid="uuid-same"),
            "Duplicate store uuid",
        ),
    ),
)
def test_registration_rejects_duplicate_identifiers(
    first: StoreContainer,
    second: StoreContainer,
    message: str,
) -> None:
    manager = StorageManager(stores=[first], startup_on_add=False)

    with pytest.raises(ValueError, match=message):
        manager.register_store_container(second)


def test_store_lookup_supports_all_identifiers_and_detects_ambiguity() -> None:
    first = _container("shared", url="dummy://first", uuid="uuid-first")
    second = _container("second", url="dummy://second", uuid="shared")
    manager = StorageManager(stores=[first, second], startup_on_add=False)

    assert manager.get_store_container("uuid-first") is first
    assert manager.get_store_container("dummy://first") is first
    with pytest.raises(KeyError, match="Ambiguous"):
        manager.get_store_container("shared")
    with pytest.raises(KeyError, match="Unknown store"):
        manager.get_store_container("missing")


def test_store_lookup_deduplicates_one_container_matching_multiple_keys() -> None:
    plugin = _DummyPlugin(
        "dummy://same",
        name="same",
        uuid="same",
    )
    container = StoreContainer.from_plugin(plugin)
    manager = StorageManager(stores=[container], startup_on_add=False)

    assert manager.get_store_container("same") is container


def test_default_store_selection_and_empty_registry_errors() -> None:
    manager = StorageManager(startup_on_add=False)
    with pytest.raises(RuntimeError, match="No default store"):
        manager.get_default_store_container()

    first = _container("first")
    second = _container("second")
    manager.register_store_container(first)
    manager.register_store_container(second)
    assert manager.get_default_store_container() is first

    manager.set_default_store("second")
    assert manager.get_default_store_container() is second


class _TrackingDeleteContainer(StoreContainer):
    def delete_from_db(self) -> bool:
        self.plugin.deleted_from_db = True  # type: ignore[attr-defined]
        return True


class _UnsupportedDeleteContainer(StoreContainer):
    def delete_from_db(self) -> bool:
        raise NotImplementedError


def test_unregister_updates_default_id_bindings_and_database_state() -> None:
    first = _TrackingDeleteContainer.from_plugin(_plugin("first"))
    second = _container("second")
    manager = StorageManager(stores=[first, second], startup_on_add=False)
    manager.set_default_store("first")
    manager.bind_store_id(7, "first")

    assert manager.unregister_store_container("uuid-first", delete_from_db=True)
    assert first.plugin.deleted_from_db is True  # type: ignore[attr-defined]
    assert manager.get_default_store_container() is second
    assert 7 not in manager._store_ids
    assert not manager.unregister_store_container("missing")

    unsupported = _UnsupportedDeleteContainer.from_plugin(_plugin("unsupported"))
    manager.register_store_container(unsupported)
    assert manager.unregister_store_container("unsupported", delete_from_db=True)

    assert manager.unregister_store_container("second")
    with pytest.raises(RuntimeError, match="No default store"):
        manager.get_default_store_container()


def test_containers_without_uuids_can_be_registered_and_unregistered() -> None:
    plugin = _DummyPlugin("dummy://anonymous", name="anonymous", uuid=None)
    container = StoreContainer.from_plugin(plugin)
    manager = StorageManager(startup_on_add=False)

    assert manager.register_store_container(container)
    assert manager.get_store_container("anonymous") is container
    assert manager.unregister_store_container("anonymous")


class _OperationPlugin(_DummyPlugin):
    def __init__(
        self,
        name: str,
        *,
        write_error: Exception | None = None,
        exists_error: Exception | None = None,
        location_error: Exception | None = None,
        iter_error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        super().__init__(f"dummy://{name}", name=name, uuid=f"uuid-{name}")
        self.write_error = write_error
        self.exists_error = exists_error
        self.location_error = location_error
        self.iter_error = iter_error
        self.close_error = close_error

    def write_bytes(self, file_bytes: bytes, *, metadata=None, location=None):
        if self.write_error is not None:
            raise self.write_error
        return super().write_bytes(
            file_bytes,
            metadata=metadata,
            location=location,
        )

    def exists(self, file_identifier) -> bool:
        if self.exists_error is not None:
            raise self.exists_error
        return super().exists(file_identifier)

    def location(self, *tokens: str):
        if self.location_error is not None:
            raise self.location_error
        return super().location(*tokens)

    def iter_locations(self):
        if self.iter_error is not None:
            raise self.iter_error
        return super().iter_locations()

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error


def test_file_operations_report_empty_or_incapable_registries() -> None:
    empty = StorageManager(startup_on_add=False)
    with pytest.raises(RuntimeError, match="No stores"):
        empty.store_bytes(b"payload")
    with pytest.raises(ValueError, match="requires file_url"):
        empty.locate_file()
    with pytest.raises(RuntimeError, match="No stores"):
        empty.locate_file("dummy://missing/book.epub")
    with pytest.raises(RuntimeError, match="No stores"):
        empty.locate_folder("books")
    with pytest.raises(ValueError, match="requires file_url"):
        empty.delete_location()

    denied = _OperationPlugin(
        "denied",
        write_error=PermissionError("read-only"),
        location_error=NotImplementedError("no folders"),
    )
    unsupported = _OperationPlugin(
        "unsupported",
        write_error=NotImplementedError("no writes"),
        location_error=NotImplementedError("no folders"),
    )
    broken = _OperationPlugin(
        "broken",
        write_error=RuntimeError("offline"),
        exists_error=RuntimeError("offline"),
        location_error=RuntimeError("offline"),
    )
    manager = StorageManager(
        stores=[denied, unsupported, broken],
        startup_on_add=False,
    )

    with pytest.raises(RuntimeError, match="No writable store accepted"):
        manager.store_bytes(b"payload")
    with pytest.raises(FileNotFoundError, match="could not be resolved"):
        manager.locate_file("dummy://missing/book.epub")
    with pytest.raises(NotImplementedError, match="folder/location"):
        manager.locate_folder("books")
    assert not manager.delete_location(file_url="dummy://missing/book.epub")


def test_iteration_and_close_skip_broken_stores() -> None:
    good = _OperationPlugin("good")
    good.write_bytes(b"payload", location="book.epub")
    broken = _OperationPlugin(
        "broken",
        iter_error=RuntimeError("cannot list"),
        close_error=RuntimeError("cannot close"),
    )
    manager = StorageManager(stores=[broken, good], startup_on_add=False)

    assert [location.read_bytes() for location in manager.iter_locations()] == [
        b"payload"
    ]
    manager.close()


def test_delete_location_without_a_store_name_still_uses_its_url() -> None:
    plugin = _plugin("store")
    location = plugin.write_bytes(b"payload", location="book.epub")
    manager = StorageManager(stores=[plugin], startup_on_add=False)
    anonymous_location = SimpleNamespace(
        file_url=location.file_url,
        store=object(),
    )

    assert manager.delete_location(location=anonymous_location)


class _BootstrapDb:
    def __init__(
        self,
        rows: list[dict[str, object]] | None,
        *,
        has_stores_table: bool = True,
    ) -> None:
        self.rows = rows
        self.has_stores_table = has_stores_table

    def get_tables(self) -> list[str]:
        return ["stores"] if self.has_stores_table else ["works"]

    def get_all_rows(self, table: str, *, iterator_return: bool):
        assert table == "stores"
        assert iterator_return is False
        return self.rows


class _BootstrapManager(StorageManager):
    specs: dict[int, StoreSpec | Exception] = {}
    build_failures: set[int] = set()

    def get_store_spec_from_db(self, store_id: int) -> StoreSpec:
        result = self.specs[store_id]
        if isinstance(result, Exception):
            raise result
        return result

    def build_store_container(self, store_spec: StoreSpec):
        if store_spec.store_id in self.build_failures:
            raise RuntimeError("backend unavailable")
        return super().build_store_container(store_spec)


def test_database_bootstrap_reports_loaded_skipped_and_failed_rows() -> None:
    rows = [
        {"store_id": 1, "store_name": "offline", "store_online_status": "offline"},
        {"store_id": "bad", "store_name": "missing-id"},
        {"store_id": 2, "store_name": "bad-spec"},
        {"store_id": 3, "store_name": "missing-root"},
        {"store_id": 4, "store_name": "loaded"},
        {"store_id": 5, "store_name": "bad-backend"},
    ]
    manager = _BootstrapManager(
        stores=[_plugin("existing")],
        startup_on_add=False,
    )
    manager.specs = {
        2: RuntimeError("bad row"),
        3: _spec(store_id=3, name="missing-root", url="", root_uri=""),
        4: _spec(store_id=4, name="loaded"),
        5: _spec(store_id=5, name="bad-backend"),
    }
    manager.build_failures = {5}

    report = manager.load_from_database(_BootstrapDb(rows))

    assert report.discovered_rows == 6
    assert report.loaded_stores == 1
    assert report.skipped_rows == 3
    assert report.failed_rows == 2
    assert not report.ok
    assert [issue.store_name for issue in report.issues] == [
        "offline",
        "missing-id",
        "bad-spec",
        "missing-root",
        "bad-backend",
    ]
    assert [store.store_name for store in manager.iter_store_containers()] == [
        "loaded"
    ]


def test_database_bootstrap_handles_absent_rows_and_can_include_offline() -> None:
    manager = _BootstrapManager(startup_on_add=False)

    assert manager.load_from_database(
        _BootstrapDb([], has_stores_table=False)
    ).discovered_rows == 0
    assert manager.load_from_database(_BootstrapDb(None)).discovered_rows == 0

    manager.specs = {1: _spec(store_id=1, name="offline")}
    manager.build_failures = set()
    manager.register_store_container(_container("existing"))
    report = manager.load_from_database(
        _BootstrapDb(
            [
                {
                    "store_id": 1,
                    "store_name": "offline",
                    "store_online_status": "OFFLINE",
                }
            ]
        ),
        include_offline=True,
        clear_existing=False,
    )

    assert report.ok
    assert report.loaded_stores == 1
    assert [store.store_name for store in manager.iter_store_containers()] == [
        "existing",
        "offline",
    ]


def test_from_database_constructs_and_loads_the_manager() -> None:
    _BootstrapManager.specs = {1: _spec(store_id=1, name="loaded")}
    _BootstrapManager.build_failures = set()
    db = _BootstrapDb([{"store_id": 1, "store_name": "loaded"}])

    manager, report = _BootstrapManager.from_database(db)

    assert report.loaded_stores == 1
    assert manager.db is db
    assert manager.get_store_container("loaded").store_id == 1


class _NoStoreDb:
    def __init__(self, *, has_table: bool = True, row: object | None = None) -> None:
        self.has_table = has_table
        self.row = row

    def get_tables(self) -> list[str]:
        return ["stores"] if self.has_table else []

    def get_row_from_id(self, table: str, row_id: int):
        assert table == "stores"
        return self.row


def test_store_spec_lookup_reports_unbound_missing_table_and_unknown_rows() -> None:
    with pytest.raises(RuntimeError, match="not bound"):
        StorageManager().get_store_spec_from_db(1)
    with pytest.raises(KeyError, match="stores"):
        StorageManager(db=_NoStoreDb(has_table=False)).get_store_spec_from_db(1)
    with pytest.raises(KeyError, match="Unknown store id"):
        StorageManager(db=_NoStoreDb(row=None)).get_store_spec_from_db(1)


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (None, None),
        ("  value  ", "value"),
        (" ", None),
    ),
)
def test_optional_string_coercion(value: object, expected: str | None) -> None:
    assert StorageManager._coerce_optional_str(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    ((None, None), ("2.5", 2.5), ("bad", None), (object(), None)),
)
def test_optional_float_coercion(value: object, expected: float | None) -> None:
    assert StorageManager._coerce_optional_float(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    ((None, None), ("7", 7), ("bad", None), (object(), None)),
)
def test_integer_coercion(value: object, expected: int | None) -> None:
    assert StorageManager._to_int(value) == expected


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    (
        (None, True, True),
        (True, False, True),
        (0, True, False),
        (2.5, False, True),
        ("YES", False, True),
        ("off", True, False),
        ("unknown", True, True),
    ),
)
def test_boolean_coercion(value: object, default: bool, expected: bool) -> None:
    assert StorageManager._to_boolish(value, default=default) is expected


def test_row_helpers_support_mapping_attribute_and_row_id_shapes() -> None:
    @dataclass
    class AttributeRow:
        store_id: object = "9"
        row_id: object | None = None

    @dataclass
    class RowId:
        row_id: object = "10"

    manager = StorageManager()
    assert manager._row_get({"store_name": "mapped"}, "store_name") == "mapped"
    assert manager._row_get(AttributeRow(), "store_id") == "9"
    assert manager._row_get(object(), "missing") is None
    assert manager._row_store_id(AttributeRow()) == 9
    assert manager._row_store_id(RowId()) == 10


@pytest.mark.parametrize(
    ("row", "expected_name"),
    (
        ({"store_kind": "filesystem"}, "OnDiskExistingManagedStorageBackend"),
        (
            {
                "store_access_protocol": "file",
                "store_is_read_only": True,
            },
            "OnDiskUnmanagedStorageBackend",
        ),
        ({"store_access_protocol": "https"}, "RcloneHttpReadOnlyStorageBackend"),
        ({"store_access_protocol": "wget"}, "WgetHtmlReadOnlyStorageBackend"),
        (
            {"store_access_protocol": "native_html"},
            "NativeHtmlReadOnlyStorageBackend",
        ),
        ({"store_access_protocol": "squashfs"}, "SquashfsReadOnlyStorageBackend"),
        ({"store_access_protocol": "sqlite3"}, "SingleFileSqliteStorageBackend"),
        ({"store_access_protocol": "ftps"}, "FtpReadOnlyStorageBackend"),
    ),
)
def test_backend_resolution_supports_kind_aliases_and_protocol_fallbacks(
    row: dict[str, object],
    expected_name: str,
) -> None:
    backend_cls = StorageManager()._resolve_backend_cls(row)
    assert backend_cls is not None
    assert backend_cls.__name__ == expected_name


def test_backend_resolution_returns_none_for_unknown_or_unmapped_kinds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = StorageManager()
    assert manager._resolve_backend_cls(
        {"store_kind": "quantum", "store_access_protocol": "warp"}
    ) is None

    monkeypatch.setitem(
        store_manager_module.StorageManager._STORE_KIND_ALIASES,
        "known-but-unmapped",
        "not-importable",
    )
    assert manager._resolve_backend_cls({"store_kind": "known-but-unmapped"}) is None


def test_create_store_plugin_reports_unsupported_missing_and_legacy_backends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = StorageManager(startup_on_add=False)
    with pytest.raises(ValueError, match="Unsupported store kind"):
        manager.create_store_plugin(
            _spec(kind="unknown", protocol="warp")
        )

    missing_root = _spec(url="", root_uri="", kind="ftp_readonly")
    with pytest.raises(ValueError, match="store_root_uri or store_url"):
        manager.create_store_plugin(missing_root)

    class LegacyBackend:
        def __init__(self, **_kwargs: object) -> None:
            pass

    monkeypatch.setattr(manager, "_resolve_backend_cls", lambda _row: LegacyBackend)
    with pytest.raises(TypeError, match="not yet migrated"):
        manager.create_store_plugin(_spec())


def test_create_calibre_like_plugin_receives_database_and_store_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    class OnDiskCalibreLikeStorageBackend(_DummyPlugin):
        def __init__(
            self,
            *,
            database: object,
            store_id: int | None = None,
            **kwargs: object,
        ) -> None:
            seen.setdefault("calls", []).append((database, store_id))
            super().__init__(**kwargs)  # type: ignore[arg-type]

    db = object()
    manager = StorageManager(db=db, startup_on_add=False)
    monkeypatch.setattr(
        manager,
        "_resolve_backend_cls",
        lambda _row: OnDiskCalibreLikeStorageBackend,
    )

    plugin = manager.create_store_plugin(
        _spec(store_id=8, kind="on_disk_calibre_like", url="/library")
    )

    assert isinstance(plugin, OnDiskCalibreLikeStorageBackend)
    assert seen["calls"] == [(db, 8)]

    manager.create_store_plugin(
        _spec(
            store_id=None,
            name="calibre-no-id",
            kind="on_disk_calibre_like",
            url="/library-two",
        )
    )
    assert seen["calls"] == [(db, 8), (db, None)]


@pytest.mark.parametrize(
    ("kind", "protocol", "policy_key", "policy"),
    (
        (
            "rclone_http_readonly",
            "https",
            "rclone",
            {"timeout_s": 2},
        ),
        (
            "wget_html_readonly",
            "wget",
            "wget",
            {"timeout_s": 3},
        ),
        (
            "native_html_readonly",
            "native_html",
            "native_html",
            {"timeout_s": 4},
        ),
    ),
)
@pytest.mark.parametrize("with_options", (False, True))
def test_create_remote_plugins_with_default_or_database_policy_options(
    kind: str,
    protocol: str,
    policy_key: str,
    policy: dict[str, object],
    with_options: bool,
) -> None:
    raw_policy = (
        json.dumps({policy_key: policy})
        if with_options
        else None
    )
    manager = StorageManager(startup_on_add=False)

    plugin = manager.create_store_plugin(
        _spec(
            name=f"{kind}-{with_options}",
            kind=kind,
            url="https://example.test/library",
            protocol=protocol,
            policy=raw_policy,
        )
    )

    assert plugin.name == f"{kind}-{with_options}"
    if with_options:
        assert plugin.options.timeout_s == policy["timeout_s"]  # type: ignore[attr-defined]


def test_build_store_container_without_database_id_skips_id_binding() -> None:
    manager = StorageManager(startup_on_add=False)

    container = manager.build_store_container(
        _spec(
            store_id=None,
            name="ftp-without-id",
        )
    )

    assert container.store_id is None
    assert manager._store_ids == {}


@pytest.mark.parametrize(
    "raw_policy",
    (None, "", "not-json", "[]", "{}"),
)
def test_empty_or_invalid_backend_policies_produce_no_options(
    raw_policy: str | None,
) -> None:
    manager = StorageManager()
    row = {"store_policy_json": raw_policy}

    assert manager._build_rclone_options_from_row(row) is None
    assert manager._build_wget_options_from_row(row) is None
    assert manager._build_native_html_options_from_row(row) is None


def test_rclone_policy_options_are_coerced_and_clamped() -> None:
    manager = StorageManager()
    policy = {
        "rclone": {
            "rclone_exe": " custom-rclone ",
            "rclone_args": ["--fast-list", 3],
            "timeout_s": "2.5",
            "max_http_requests_per_hour": "120",
            "apply_rclone_tpslimit": "no",
            "rclone_tpslimit_burst": 0,
            "enforce_global_rate_limit": "yes",
        }
    }

    options = manager._build_rclone_options_from_row(
        {"store_policy_json": json.dumps(policy)}
    )

    assert options is not None
    assert options.rclone_exe == "custom-rclone"
    assert options.rclone_args == ("--fast-list", "3")
    assert options.timeout_s == 2.5
    assert options.max_http_requests_per_hour == 120
    assert options.apply_rclone_tpslimit is False
    assert options.rclone_tpslimit_burst == 1
    assert options.enforce_global_rate_limit is True


def test_wget_and_native_policy_options_use_only_supported_fields() -> None:
    manager = StorageManager()
    common = {
        "timeout_s": "3",
        "max_http_requests_per_hour": "60",
        "recurse": "false",
        "max_depth": "4",
        "no_parent": "yes",
        "span_hosts": 1,
        "respect_robots": 0,
        "user_agent": " LiuXin Test ",
        "max_html_bytes": 10,
    }
    wget_policy = {
        "wget": {
            **common,
            "wget_exe": " custom-wget ",
            "wget_args": ["--quiet", 2],
        }
    }
    native_policy = {"native_html": common}

    wget = manager._build_wget_options_from_row(
        {"store_policy_json": json.dumps(wget_policy)}
    )
    native = manager._build_native_html_options_from_row(
        {"store_policy_json": json.dumps(native_policy)}
    )

    assert wget is not None
    assert wget.wget_exe == "custom-wget"
    assert wget.wget_args == ("--quiet", "2")
    assert wget.timeout_s == 3
    assert wget.max_depth == 4
    assert wget.recurse is False
    assert not hasattr(wget, "max_html_bytes")
    assert native is not None
    assert native.timeout_s == 3
    assert native.max_depth == 4
    assert native.recurse is False
    assert native.max_html_bytes == 1024


def test_backend_policy_ignores_unusable_optional_values() -> None:
    manager = StorageManager()
    rclone = manager._build_rclone_options_from_row(
        {
            "store_policy_json": json.dumps(
                {
                    "rclone_exe": " ",
                    "rclone_args": "not-a-list",
                    "rclone_tpslimit_burst": "bad",
                }
            )
        }
    )
    wget = manager._build_wget_options_from_row(
        {
            "store_policy_json": json.dumps(
                {
                    "wget_exe": "",
                    "wget_args": "not-a-list",
                    "max_html_bytes": "bad",
                }
            )
        }
    )
    native = manager._build_native_html_options_from_row(
        {
            "store_policy_json": json.dumps(
                {
                    "max_html_bytes": "bad",
                }
            )
        }
    )

    assert rclone is None
    assert wget is None
    assert native is None


class _HintProvider:
    def storage_hints(self) -> ItemStorageHints:
        return ItemStorageHints(
            extra={
                "store_name": "hint-store",
                "file_storage_key": "hint/book.epub",
            }
        )


class _HintProviderWithoutMappingExtra:
    def storage_hints(self) -> ItemStorageHints:
        return ItemStorageHints(extra=[])  # type: ignore[arg-type]


def test_metadata_sources_values_urls_and_store_identifiers() -> None:
    manager = StorageManager(startup_on_add=False)
    manager.bind_store_id(7, "bound-store")
    provider = _HintProvider()

    sources = manager._metadata_sources(provider)
    assert provider in sources
    assert isinstance(sources[1], ItemStorageHints)
    assert sources[2]["store_name"] == "hint-store"
    assert manager._get_metadata_value(provider, "store_name") == "hint-store"
    assert manager._metadata_file_url(provider) == "hint/book.epub"
    assert len(manager._metadata_sources(_HintProviderWithoutMappingExtra())) == 2

    metadata = {
        "preferred_store": ["first", "second"],
        "store_uuid": "uuid-third",
        "file_store_id": "7",
        "extra": {"store_name": "extra-store"},
    }
    assert manager._metadata_sources(None) == []
    assert list(manager._metadata_store_identifiers(metadata)) == [
        "first",
        "second",
        "extra-store",
        "uuid-third",
        "bound-store",
    ]
    assert list(
        manager._metadata_store_identifiers({"file_store_id": "invalid"})
    ) == []
    assert list(manager._metadata_store_identifiers({})) == []


@pytest.mark.parametrize(
    ("file_row", "expected"),
    (
        ({"file_url": "dummy://store/url.epub"}, "dummy://store/url.epub"),
        ({"file_path": "/path/book.epub"}, "/path/book.epub"),
        ({"file_storage_key": "nested/book.epub"}, "nested/book.epub"),
        ({}, None),
    ),
)
def test_file_urls_fall_back_through_nested_file_rows(
    file_row: dict[str, object],
    expected: str | None,
) -> None:
    assert StorageManager()._metadata_file_url({"file_row": file_row}) == expected


def test_candidate_store_order_uses_hints_urls_default_and_deduplication() -> None:
    first = _container("first")
    second = _container("second")
    third = _container("third")
    manager = StorageManager(
        stores=[first, second, third],
        startup_on_add=False,
    )
    manager.set_default_store("third")

    candidates = manager._candidate_store_containers(
        preferred_store="second",
        metadata={"store_name": ["third", "missing", "second"]},
        file_url="dummy://first/book.epub",
    )
    assert candidates == [second, third, first]

    manager._default_store_key = "missing"
    assert manager._candidate_store_containers(
        preferred_store=None,
        metadata=None,
        file_url=None,
    ) == [first, second, third]


def test_file_url_store_membership_handles_urls_paths_and_path_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _container("store", url=str(tmp_path))
    assert StorageManager._file_url_belongs_to_store(str(tmp_path), store)
    assert StorageManager._file_url_belongs_to_store(
        str(tmp_path / "book.epub"),
        store,
    )
    assert not StorageManager._file_url_belongs_to_store(
        str(tmp_path.parent / "elsewhere.epub"),
        store,
    )

    def fail_path(_value: object) -> Path:
        raise OSError("bad path")

    monkeypatch.setattr(store_manager_module.pathlib, "Path", fail_path)
    assert not StorageManager._file_url_belongs_to_store("other", store)
