from __future__ import annotations

import hashlib
import io
import subprocess
import threading

from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    Location,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageUnavailable,
    StoreReadOnly,
)
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly.rclone_http_location import (
    RcloneHttpReadOnlyStoreLocation,
)
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def _extract_tpslimit(extra_args: tuple[str, ...]) -> float | None:
    for argument in extra_args:
        if argument.startswith("--tpslimit="):
            return float(argument.split("=", 1)[1])
    return None


def test_rclone_readonly_preserves_unicode_inventory_hints_and_bytes(
    monkeypatch,
    tmp_path,
) -> None:
    digest = hashlib.sha256(UNICODE_PAYLOAD).hexdigest()

    def _fake_json(args, **kwargs):
        del kwargs
        if "--stat" in args:
            return {
                "Name": UNICODE_FILENAME,
                "Path": UNICODE_KEY,
                "Size": len(UNICODE_PAYLOAD),
                "Hashes": {"SHA-256": digest},
                "ID": "unicode-v1",
                "IsDir": False,
            }
        return [
            {
                "Name": UNICODE_FILENAME,
                "Path": UNICODE_KEY,
                "Size": len(UNICODE_PAYLOAD),
                "Hashes": {"SHA-256": digest},
            }
        ]

    def _fake_spawn(self, args):
        del self
        if args[0] == "lsjson":
            raise RuntimeError("use the injected JSON runner")
        return SimpleNamespace(
            stdout=io.BytesIO(UNICODE_PAYLOAD),
            stderr=io.BytesIO(),
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_json)
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _fake_spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    [location] = list(store.iter_locations())
    info = store.stat_file(location)

    assert location.key == UNICODE_KEY
    assert store.location_uri(location) == f"remote:{UNICODE_KEY}"
    assert info.hints.suggested_filename == UNICODE_FILENAME
    assert info.digest is not None and info.digest.value == digest
    assert store.read_file(info) == UNICODE_PAYLOAD
    assert store.characteristics.publication_model is StoragePublicationModel.READ_ONLY
    assert (
        store.characteristics.temporary_space
        is StorageTemporarySpaceRequirement.NONE
    )

    destination = FilesystemStore(tmp_path / "rclone-ingest-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )
    report = ingest_store(manager, store)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert item.source_info.location.key == UNICODE_KEY
    assert item.result.asset_record.metadata.original_name == UNICODE_FILENAME
    assert manager.read_file(item.result.asset_record) == UNICODE_PAYLOAD


def test_truncated_rclone_ingest_publishes_no_manager_state(
    monkeypatch,
    tmp_path,
) -> None:
    expected = b"authoritative payload"
    record = {
        "Name": "book.epub",
        "Path": "book.epub",
        "Size": len(expected),
        "Hashes": {"SHA-256": hashlib.sha256(expected).hexdigest()},
        "ID": "v1",
        "IsDir": False,
    }

    def _fake_json(args, **kwargs):
        del kwargs
        return record if "--stat" in args else [record]

    def _fake_spawn(self, args):
        del self
        if args[0] == "lsjson":
            raise RuntimeError("use the injected JSON runner")
        return SimpleNamespace(
            stdout=io.BytesIO(b"short"),
            stderr=io.BytesIO(),
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_json)
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _fake_spawn,
    )
    source = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )
    destination = FilesystemStore(tmp_path / "rclone-truncated-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert not report.ok and report.ingested_files == 0
    assert report.failures[0].error_type == "StorageIntegrityError"
    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()


def test_rclone_readonly_reads_tortured_unicode_paths_exactly(monkeypatch) -> None:
    payloads = {
        case.key: case.payload for case in TORTURED_UNICODE_PATH_CASES
    }

    def _record(key: str):
        payload = payloads[key]
        return {
            "Name": key.rsplit("/", 1)[-1],
            "Path": key,
            "Size": len(payload),
            "Hashes": {"SHA-256": hashlib.sha256(payload).hexdigest()},
            "ID": f"id-{len(payload)}",
            "IsDir": False,
        }

    def _fake_json(args, **kwargs):
        del kwargs
        if "--stat" in args:
            return _record(args[-1].removeprefix("remote:"))
        return [_record(key) for key in payloads]

    def _fake_spawn(self, args):
        del self
        payload = payloads[args[1].removeprefix("remote:")]
        if "--offset" in args:
            payload = payload[int(args[args.index("--offset") + 1]) :]
        if "--count" in args:
            payload = payload[: int(args[args.index("--count") + 1])]
        return SimpleNamespace(
            stdout=io.BytesIO(payload),
            stderr=io.BytesIO(),
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_json)
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _fake_spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
        ),
    )

    results = exercise_unicode_path_cases(
        store,
        TORTURED_UNICODE_PATH_CASES,
        check_uri_round_trip=True,
    )

    assert {result.location.key for result in results} == set(payloads)


def test_rclone_backend_default_rate_limit_is_20_per_minute(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        captured["args"] = list(args)
        captured["extra_args"] = tuple(kwargs.get("extra_args", ()))
        return {}

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    store = RcloneHttpReadOnlyStorageBackend(url="remote:")

    store.run_rclone_json(["lsjson", "--max-depth", "1", "remote:"], check=True)

    extra_args = captured["extra_args"]
    assert any(argument.startswith("--tpslimit=") for argument in extra_args)
    assert "--tpslimit-burst=1" in extra_args
    assert _extract_tpslimit(extra_args) == pytest.approx(1200.0 / 3600.0)


def test_rclone_backend_default_rate_limit_reads_preferences(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        captured["extra_args"] = tuple(kwargs.get("extra_args", ()))
        return {}

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    monkeypatch.setattr(
        backend_module,
        "get_default_rclone_http_requests_per_hour",
        lambda: 300.0,
    )
    store = RcloneHttpReadOnlyStorageBackend(url="remote:")
    store.run_rclone_json(["lsjson", "remote:"], check=True)

    assert _extract_tpslimit(captured["extra_args"]) == pytest.approx(300.0 / 3600.0)


def test_rclone_backend_custom_rate_limit_is_settable(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        captured["extra_args"] = tuple(kwargs.get("extra_args", ()))
        return {}

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    store = RcloneHttpReadOnlyStorageBackend(
        url="remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=120.0),
    )
    store.run_rclone_json(["lsjson", "remote:"], check=True)

    assert _extract_tpslimit(captured["extra_args"]) == pytest.approx(120.0 / 3600.0)


def test_get_default_rclone_http_requests_per_hour_falls_back_on_invalid_value(monkeypatch) -> None:
    import LiuXin_alpha.preferences as preferences_module

    original_get = preferences_module.preferences.get

    def _fake_get(option: str, default=None):
        if option == backend_module.RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY:
            return "not-a-number"
        return original_get(option, default)

    monkeypatch.setattr(preferences_module.preferences, "get", _fake_get)
    assert (
        backend_module.get_default_rclone_http_requests_per_hour()
        == backend_module.RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT
    )


def test_rclone_backend_can_disable_rate_limit_flags(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        captured["extra_args"] = tuple(kwargs.get("extra_args", ()))
        return {}

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    store = RcloneHttpReadOnlyStorageBackend(
        url="remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0.0),
    )
    store.run_rclone_json(["lsjson", "remote:"], check=True)

    assert all(
        not argument.startswith(("--tpslimit=", "--tpslimit-burst="))
        for argument in captured["extra_args"]
    )


def test_rclone_backend_global_rate_limit_spaces_commands(monkeypatch) -> None:
    calls: list[list[str]] = []
    sleeps: list[float] = []
    monotonic_values = iter((0.0, 1.0))

    def _fake_run_rclone_json(args, **kwargs):
        del kwargs
        calls.append(list(args))
        return {}

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    monkeypatch.setattr(backend_module, "_monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(
        backend_module,
        "_sleep",
        lambda seconds: sleeps.append(float(seconds)),
    )
    store = RcloneHttpReadOnlyStorageBackend(
        url="remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=1200.0,
            apply_rclone_tpslimit=False,
        ),
    )

    store.run_rclone_json(["lsjson", "remote:"], check=True)
    store.run_rclone_json(["lsjson", "remote:"], check=True)

    assert len(calls) == 2
    assert sleeps == pytest.approx([2.0])


def test_rclone_store_enumerates_real_files_with_complete_semantics(monkeypatch) -> None:
    payload = [
        {"Path": "alpha/book1.epub", "Size": 3},
        {"Name": "book2.mobi", "Size": 4},
        {"Path": ""},
    ]

    monkeypatch.setattr(backend_module, "run_rclone_json", lambda args, **kwargs: payload)
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
        ),
    )

    locations = list(store.iter_locations())
    assert [location.key for location in locations] == [
        "alpha/book1.epub",
        "book2.mobi",
    ]
    assert all(location.store_ref == store.store_ref for location in locations)
    assert store.capabilities.enumeration is EnumerationCompleteness.COMPLETE


def test_rclone_backend_normalizes_plain_https_root_to_configless_fs(monkeypatch) -> None:
    captured_args: list[list[str]] = []

    def _fake_run_rclone_json(args, **kwargs):
        del kwargs
        captured_args.append(list(args))
        return []

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        lambda self, args: (_ for _ in ()).throw(RuntimeError("use JSON fixture")),
    )
    store = RcloneHttpReadOnlyStorageBackend(
        url="https://www.fadedpage.com/",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
        ),
    )

    assert store.url == ':http,url="https://www.fadedpage.com":'
    list(store.iter_locations())
    assert captured_args[0] == [
        "lsjson",
        "-R",
        "--files-only",
        "--hash",
        ':http,url="https://www.fadedpage.com":',
    ]


def test_rclone_backend_rejects_inline_secret_configuration() -> None:
    with pytest.raises(StorageInvalidAddress):
        RcloneHttpReadOnlyStorageBackend(
            ':s3,provider="AWS",secret_access_key="do-not-persist":bucket',
            options=RcloneBackendOptions(max_http_requests_per_hour=0),
        )


def test_rclone_stat_read_digest_and_range_use_new_store_api(monkeypatch) -> None:
    payload = b"0123456789"
    json_calls: list[list[str]] = []
    process_calls: list[list[str]] = []

    def _fake_json(args, **kwargs):
        del kwargs
        json_calls.append(list(args))
        return {
            "Name": "book.epub",
            "Size": len(payload),
            "ModTime": "2026-08-16T10:00:00Z",
            "Hashes": {"SHA-256": "ab" * 32},
            "ID": "remote-v3",
            "IsDir": False,
        }

    def _fake_spawn(self, args):
        del self
        arguments = list(args)
        process_calls.append(arguments)
        selected = payload
        if "--offset" in arguments:
            selected = selected[int(arguments[arguments.index("--offset") + 1]) :]
        if "--count" in arguments:
            selected = selected[: int(arguments[arguments.index("--count") + 1])]
        return SimpleNamespace(
            stdout=io.BytesIO(selected),
            stderr=io.BytesIO(),
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_json)
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _fake_spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )
    location = store.locate("path/book.epub")

    assert isinstance(location, RcloneHttpReadOnlyStoreLocation)
    assert isinstance(location, Location)
    info = store.stat_file(location)
    assert info.size == 10
    assert info.digest is not None and info.digest.algorithm == "sha256"
    assert info.version == "remote-v3"
    assert store.read_file(info) == payload
    assert store.read_file(info, offset=2, length=4) == b"2345"
    assert ["lsjson", "--stat", "--hash", "remote:path/book.epub"] in json_calls
    assert ["cat", "remote:path/book.epub", "--offset", "2", "--count", "4"] in process_calls


def test_rclone_prefix_inventory_and_read_only_enforcement(monkeypatch) -> None:
    payload = [
        {"Path": "alpha/one.epub", "Size": 1},
        {"Path": "alpha/two.epub", "Size": 2},
        {"Path": "beta/three.epub", "Size": 3},
    ]
    monkeypatch.setattr(backend_module, "run_rclone_json", lambda args, **kwargs: payload)
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    assert [
        location.key
        for location in store.iter_locations(prefix=store.locate("alpha"))
    ] == ["alpha/one.epub", "alpha/two.epub"]
    with pytest.raises(StoreReadOnly):
        store.store_bytes(b"x", location="alpha/new.epub")
    with pytest.raises(StoreReadOnly):
        store.delete_file("alpha/one.epub")


def test_rclone_inventory_enforces_stream_token_and_entry_limits(monkeypatch) -> None:
    oversized_process = SimpleNamespace(
        stdout=io.BytesIO(b'[{"Path":"' + (b"x" * 128)),
        stderr=io.BytesIO(),
        wait=lambda timeout=None: 0,
        poll=lambda: 0,
        terminate=lambda: None,
        kill=lambda: None,
    )
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        lambda self, args: oversized_process,
    )
    token_limited = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
            max_json_token_chars=32,
        ),
    )
    with pytest.raises(StorageUnavailable, match="JSON token size limit"):
        list(token_limited.iter_locations())

    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        lambda self, args: (_ for _ in ()).throw(RuntimeError("no process")),
    )
    monkeypatch.setattr(
        backend_module,
        "run_rclone_json",
        lambda args, **kwargs: [
            {"Path": f"book-{index}.epub", "Size": 1}
            for index in range(3)
        ],
    )
    entry_limited = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
            max_inventory_entries=2,
        ),
    )
    with pytest.raises(StorageUnavailable, match="inventory entry limit"):
        list(entry_limited.iter_locations())


def test_rclone_hostile_process_cleanup_cannot_mask_a_successful_read(
    monkeypatch,
) -> None:
    payload = b"book"

    class _CloseBomb(io.BytesIO):
        def close(self) -> None:
            super().close()
            raise RuntimeError("attacker-controlled stream close")

    process = SimpleNamespace(
        stdout=_CloseBomb(payload),
        stderr=_CloseBomb(),
        wait=lambda timeout=None: 0,
        poll=lambda: (_ for _ in ()).throw(RuntimeError("hostile poll")),
        terminate=lambda: (_ for _ in ()).throw(RuntimeError("hostile terminate")),
        kill=lambda: (_ for _ in ()).throw(RuntimeError("hostile kill")),
    )
    monkeypatch.setattr(
        backend_module,
        "run_rclone_json",
        lambda args, **kwargs: {
            "Name": "book.epub",
            "Path": "book.epub",
            "Size": len(payload),
            "IsDir": False,
        },
    )
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        lambda self, args: process,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
        ),
    )

    assert store.read_file(store.stat_file("book.epub")) == payload
    assert process.stdout.closed


def test_rclone_translates_and_redacts_arbitrary_hostile_stream_failures(
    monkeypatch,
) -> None:
    class _HostileStdout:
        def read(self, size=-1):
            del size
            raise RuntimeError("token=supersecret " + ("noise " * 200))

        def close(self):
            return None

    process = SimpleNamespace(
        stdout=_HostileStdout(),
        stderr=io.BytesIO(),
        wait=lambda timeout=None: 0,
        poll=lambda: 0,
        terminate=lambda: None,
        kill=lambda: None,
    )
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        lambda self, args: process,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
        ),
    )

    with store.driver.open_read(store.driver.parse_object_address("book.epub")) as stream:
        with pytest.raises(StorageUnavailable) as failure:
            stream.read()

    assert "rclone read object failed" in str(failure.value)
    assert "supersecret" not in str(failure.value)
    assert "<redacted>" in str(failure.value)
    assert len(str(failure.value)) < 700


@pytest.mark.parametrize(
    "invalid",
    ["", "../escape", "/absolute", "alpha//book", "alpha/./book", "alpha\\book"],
)
def test_rclone_store_rejects_noncanonical_keys(invalid: str) -> None:
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )
    with pytest.raises((StorageInvalidAddress, ValueError)):
        store.locate(invalid)


def test_rclone_missing_errors_are_not_flattened_to_false_metadata(monkeypatch) -> None:
    def _missing(args, **kwargs):
        del args, kwargs
        raise RuntimeError("object not found")

    monkeypatch.setattr(backend_module, "run_rclone_json", _missing)
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    assert store.file_exists("missing.epub") is False
    with pytest.raises(StorageNotFound):
        store.stat_file("missing.epub")


def test_rclone_locate_accepts_its_full_backend_identifier() -> None:
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:base",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    assert store.locate("remote:base/path/book.epub").key == "path/book.epub"
    with pytest.raises(StorageInvalidAddress):
        store.driver.object_address_from_uri("other:path/book.epub")


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"[\xff]", "malformed UTF-8"),
        (b'[{"Path":"book.epub"}', "truncated JSON"),
        (b"[] trailing", "trailing JSON"),
        (b"{}", "JSON array"),
        (
            b'[{"Path":"bad\\ud800.epub","Size":1}]',
            "malformed Unicode",
        ),
    ],
)
def test_rclone_streaming_inventory_rejects_malformed_remote_output(
    monkeypatch,
    payload: bytes,
    message: str,
) -> None:
    monkeypatch.setattr(
        backend_module,
        "run_rclone_json",
        lambda args, **kwargs: pytest.fail(
            "malformed streaming output must not be retried through fallback"
        ),
    )

    def _spawn(self, args):
        del self, args
        return SimpleNamespace(
            stdout=io.BytesIO(payload),
            stderr=io.BytesIO(),
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with pytest.raises(StorageUnavailable, match=message):
        list(store.driver.iter_inventory())


def test_rclone_streaming_inventory_rejects_nonbinary_stdout(monkeypatch) -> None:
    def _spawn(self, args):
        del self, args
        return SimpleNamespace(
            stdout=io.StringIO("[]"),
            stderr=io.BytesIO(),
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with pytest.raises(StorageUnavailable, match="non-byte"):
        list(store.driver.iter_inventory())


def test_rclone_inventory_rejects_an_invalid_process_adapter(monkeypatch) -> None:
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        lambda self, args: SimpleNamespace(
            stdout=None,
            stderr=None,
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
        ),
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with pytest.raises(StorageUnavailable, match="invalid process stream"):
        list(store.driver.iter_inventory())


def test_rclone_inventory_translates_process_finish_timeout(monkeypatch) -> None:
    def _wait(timeout=None):
        raise subprocess.TimeoutExpired(["rclone", "lsjson"], timeout)

    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        lambda self, args: SimpleNamespace(
            stdout=io.BytesIO(b"[]"),
            stderr=io.BytesIO(),
            wait=_wait,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        ),
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with pytest.raises(StorageTimeout, match="timed out"):
        list(store.driver.iter_inventory())


def test_rclone_inventory_checks_nonzero_exit_after_valid_json(monkeypatch) -> None:
    def _spawn(self, args):
        del self, args
        return SimpleNamespace(
            stdout=io.BytesIO(b'[{"Path":"book.epub","Size":4}]'),
            stderr=io.BytesIO(b"connection reset by remote"),
            wait=lambda timeout=None: 9,
            poll=lambda: 9,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with pytest.raises(StorageUnavailable, match="connection reset"):
        list(store.driver.iter_inventory())


def test_rclone_read_detects_truncated_count_and_nonzero_exit(monkeypatch) -> None:
    payloads = iter(
        (
            (b"ab", b"", 0),
            (b"book", b"connection reset", 8),
        )
    )

    def _spawn(self, args):
        del self, args
        payload, error, returncode = next(payloads)
        return SimpleNamespace(
            stdout=io.BytesIO(payload),
            stderr=io.BytesIO(error),
            wait=lambda timeout=None: returncode,
            poll=lambda: returncode,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )
    address = store.driver.parse_object_address("book.epub")

    with store.driver.open_read(address, length=4) as stream:
        with pytest.raises(StorageUnavailable, match="requested byte count"):
            stream.read()
    with store.driver.open_read(address) as stream:
        with pytest.raises(StorageUnavailable, match="connection reset"):
            stream.read()


def test_rclone_read_translates_process_finish_timeout(monkeypatch) -> None:
    def _wait(timeout=None):
        del timeout
        raise TimeoutError("hung process")

    def _spawn(self, args):
        del self, args
        return SimpleNamespace(
            stdout=io.BytesIO(b""),
            stderr=io.BytesIO(),
            wait=_wait,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with store.driver.open_read(
        store.driver.parse_object_address("book.epub")
    ) as stream:
        with pytest.raises(StorageTimeout, match="timed out"):
            stream.read()


def test_rclone_streaming_process_enforces_backend_timeout(monkeypatch) -> None:
    class _BlockedProcess:
        def __init__(self) -> None:
            self.finished = threading.Event()
            self.returncode = None
            self.stdout = self
            self.stderr = io.BytesIO()
            self.killed = False

        def read(self, size: int = -1) -> bytes:
            del size
            assert self.finished.wait(timeout=1)
            return b""

        def close(self) -> None:
            return None

        def wait(self, timeout=None) -> int:
            if not self.finished.wait(timeout=timeout or 1):
                raise subprocess.TimeoutExpired(["rclone"], timeout)
            assert self.returncode is not None
            return self.returncode

        def poll(self):
            return self.returncode

        def terminate(self) -> None:
            self.kill()

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9
            self.finished.set()

    process = _BlockedProcess()
    monkeypatch.setattr(backend_module, "which_rclone", lambda exe: exe)
    monkeypatch.setattr(backend_module.subprocess, "Popen", lambda *a, **k: process)
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            timeout_s=0.01,
            max_http_requests_per_hour=0,
        ),
    )

    with store.driver.open_read(
        store.driver.parse_object_address("book.epub")
    ) as stream:
        with pytest.raises(StorageTimeout, match="timed out"):
            stream.read()

    assert process.killed


@pytest.mark.parametrize(
    "invalid",
    ["bad\ud800.epub", "folder/bad\udfff.epub"],
)
def test_rclone_rejects_unpaired_surrogate_object_paths(invalid: str) -> None:
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with pytest.raises(StorageInvalidAddress, match="malformed Unicode"):
        store.locate(invalid)


@pytest.mark.parametrize(
    "stat_blob",
    [
        {"Name": "bad\ud800.epub", "Size": 1, "IsDir": False},
        {"Name": "book.epub", "Size": "not-a-number", "IsDir": False},
        {"Name": "book.epub", "Size": -1, "IsDir": False},
        ["not", "an", "object"],
    ],
)
def test_rclone_stat_rejects_malformed_remote_metadata(
    monkeypatch,
    stat_blob: object,
) -> None:
    monkeypatch.setattr(
        backend_module,
        "run_rclone_json",
        lambda args, **kwargs: stat_blob,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with pytest.raises((StorageUnavailable, StorageNotFound)):
        store.driver.stat(store.driver.parse_object_address("book.epub"))


def test_rclone_open_read_rejects_an_invalid_process_adapter(monkeypatch) -> None:
    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        lambda self, args: None,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with pytest.raises(StorageUnavailable, match="invalid process stream"):
        store.driver.open_read(
            store.driver.parse_object_address("book.epub")
        )


def test_rclone_read_rejects_nonbyte_process_output(monkeypatch) -> None:
    def _spawn(self, args):
        del self, args
        return SimpleNamespace(
            stdout=io.StringIO("not bytes"),
            stderr=io.BytesIO(),
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )

    monkeypatch.setattr(
        backend_module.RcloneHttpReadOnlyStorageBackend,
        "spawn_rclone_process",
        _spawn,
    )
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )

    with store.driver.open_read(
        store.driver.parse_object_address("book.epub")
    ) as stream:
        with pytest.raises(StorageUnavailable, match="non-byte"):
            stream.read()
