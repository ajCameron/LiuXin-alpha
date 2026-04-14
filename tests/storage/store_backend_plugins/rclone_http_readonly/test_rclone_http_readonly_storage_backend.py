from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly.rclone_http_location import (
    RcloneHttpReadOnlyStoreLocation,
)

def _extract_tpslimit(extra_args: tuple[str, ...]) -> float | None:
    for arg in extra_args:
        if arg.startswith("--tpslimit="):
            try:
                return float(arg.split("=", 1)[1])
            except Exception:
                return None
    return None


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
    assert any(arg.startswith("--tpslimit=") for arg in extra_args)
    assert "--tpslimit-burst=1" in extra_args

    tpslimit = _extract_tpslimit(extra_args)
    assert tpslimit is not None
    assert abs(tpslimit - (1200.0 / 3600.0)) < 1e-8


def test_rclone_backend_default_rate_limit_reads_preferences(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        captured["extra_args"] = tuple(kwargs.get("extra_args", ()))
        return {}

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    monkeypatch.setattr(backend_module, "get_default_rclone_http_requests_per_hour", lambda: 300.0)

    store = RcloneHttpReadOnlyStorageBackend(url="remote:")
    store.run_rclone_json(["lsjson", "remote:"], check=True)

    tpslimit = _extract_tpslimit(captured["extra_args"])
    assert tpslimit is not None
    assert abs(tpslimit - (300.0 / 3600.0)) < 1e-8


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

    store.run_rclone_json(["lsjson", "--max-depth", "1", "remote:"], check=True)

    tpslimit = _extract_tpslimit(captured["extra_args"])
    assert tpslimit is not None
    assert abs(tpslimit - (120.0 / 3600.0)) < 1e-8


def test_get_default_rclone_http_requests_per_hour_falls_back_on_invalid_value(monkeypatch) -> None:
    import LiuXin_alpha.preferences as preferences_module

    original_get = preferences_module.preferences.get

    def _fake_get(option: str, default=None):
        if option == backend_module.RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY:
            return "not-a-number"
        return original_get(option, default)

    monkeypatch.setattr(preferences_module.preferences, "get", _fake_get)
    value = backend_module.get_default_rclone_http_requests_per_hour()
    assert value == backend_module.RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT


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

    store.run_rclone_json(["lsjson", "--max-depth", "1", "remote:"], check=True)

    extra_args = captured["extra_args"]
    assert all(not arg.startswith("--tpslimit=") for arg in extra_args)
    assert all(not arg.startswith("--tpslimit-burst=") for arg in extra_args)


def test_rclone_backend_global_rate_limit_spaces_commands(monkeypatch) -> None:
    calls: list[tuple[list[str], tuple[str, ...]]] = []
    sleeps: list[float] = []
    monotonic_values = iter((0.0, 1.0))

    def _fake_run_rclone_json(args, **kwargs):
        calls.append((list(args), tuple(kwargs.get("extra_args", ()))))
        return {}

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    monkeypatch.setattr(backend_module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(backend_module.time, "sleep", lambda seconds: sleeps.append(float(seconds)))

    store = RcloneHttpReadOnlyStorageBackend(
        url="remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=1200.0, apply_rclone_tpslimit=False),
    )

    store.run_rclone_json(["lsjson", "remote:"], check=True)
    store.run_rclone_json(["lsjson", "remote:"], check=True)

    assert len(calls) == 2
    assert len(sleeps) == 1
    # 1200 requests/hour -> 3s spacing; second call at t=1 should wait ~2s.
    assert abs(sleeps[0] - 2.0) < 1e-6


def test_rclone_backend_true_files_iterates_remote_entries(monkeypatch) -> None:
    payload = [
        {"Path": "alpha/book1.epub"},
        {"Name": "book2.mobi"},
        {"Path": ""},
    ]

    def _fake_run_rclone_json(args, **kwargs):
        return payload

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    store = RcloneHttpReadOnlyStorageBackend(
        url="remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=None, enforce_global_rate_limit=False),
    )

    file_urls = [one.file_url for one in store.true_files()]
    assert file_urls == [
        "remote:alpha/book1.epub",
        "remote:book2.mobi",
    ]


def test_rclone_backend_normalizes_plain_https_root_to_configless_fs(monkeypatch) -> None:
    captured_args: list[list[str]] = []

    def _fake_run_rclone_json(args, **kwargs):
        captured_args.append(list(args))
        return []

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)
    store = RcloneHttpReadOnlyStorageBackend(
        url="https://www.fadedpage.com/",
        options=RcloneBackendOptions(max_http_requests_per_hour=None, enforce_global_rate_limit=False),
    )

    assert store.url == ':http,url="https://www.fadedpage.com":'
    list(store.true_files())

    assert captured_args
    assert captured_args[0][:3] == ["lsjson", "-R", "--files-only"]
    assert captured_args[0][3] == ':http,url="https://www.fadedpage.com":'


def test_rclone_location_uses_store_wrappers() -> None:
    calls: list[tuple[str, tuple[str, ...], bool]] = []

    class _DummyStore:
        url = "remote:"

        def run_rclone_json(self, args, *, check: bool = True):
            calls.append(("json", tuple(args), check))
            return {"Size": 12, "Hashes": {"sha256": "abc"}, "IsDir": False}

        def get_file_status(self, file_url: str) -> SingleFileStatus:
            blob = self.run_rclone_json(["lsjson", "--stat", file_url], check=False)
            hashes = blob.get("Hashes") or {}
            return SingleFileStatus(
                url=file_url,
                exists=True,
                size=int(blob.get("Size") or 0),
                file_hash=str(hashes.get("sha256") or ""),
                check_exists_function=lambda _url: True,
                check_size_function=lambda _url: int(blob.get("Size") or 0),
                check_hash_function=lambda _url: str(hashes.get("sha256") or ""),
            )

        def spawn_rclone_process(self, args):
            import io

            calls.append(("raw", tuple(args), True))
            return SimpleNamespace(stdout=io.BytesIO(b"payload"), stderr=io.BytesIO(b""), wait=lambda: 0, poll=lambda: 0, terminate=lambda: None, kill=lambda: None)

    loc = RcloneHttpReadOnlyStoreLocation("path", "file.epub", store=_DummyStore())
    assert loc.as_string() == "payload"
    status = loc.recheck_status()
    assert status.size == 12

    assert ("raw", ("cat", "remote:path/file.epub"), True) in calls
    assert any(kind == "json" and args[:2] == ("lsjson", "--stat") and check is False for kind, args, check in calls)


def test_rclone_location_prefers_store_json_runner() -> None:
    calls: list[tuple[tuple[str, ...], bool]] = []

    class _DummyStore:
        url = "remote:"

        def run_rclone_json(self, args, *, check: bool = True):
            calls.append((tuple(args), check))
            return {"IsDir": False, "Size": 1}

    location = RcloneHttpReadOnlyStoreLocation("alpha", "file.epub", store=_DummyStore())
    assert location.exists() is True
    assert location.is_file() is True

    assert calls
    assert calls[0][0] == ("lsjson", "--stat", "remote:alpha/file.epub")
