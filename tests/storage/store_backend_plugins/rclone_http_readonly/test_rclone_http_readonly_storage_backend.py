from __future__ import annotations

import hashlib
import io

from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    Location,
    StorageInvalidAddress,
    StorageNotFound,
    StoreReadOnly,
)
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


def _extract_tpslimit(extra_args: tuple[str, ...]) -> float | None:
    for argument in extra_args:
        if argument.startswith("--tpslimit="):
            return float(argument.split("=", 1)[1])
    return None


def test_rclone_readonly_preserves_unicode_inventory_hints_and_bytes(
    monkeypatch,
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

    discovered = {location.key: location for location in store.iter_locations()}

    assert set(discovered) == set(payloads)
    for case in TORTURED_UNICODE_PATH_CASES:
        location = discovered[case.key]
        info = store.stat_file(location)
        assert info.hints.suggested_filename == case.filename
        assert store.read_file(info) == case.payload
        assert store.location_from_uri(store.location_uri(location)) == location


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
    monkeypatch.setattr(backend_module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(
        backend_module.time,
        "sleep",
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
