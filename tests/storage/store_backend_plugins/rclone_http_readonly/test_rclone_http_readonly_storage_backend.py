"""
Exercise rclone Store reads, metadata, invocation policy, ingest, and process failures.

Fixtures inject decoded records and process-shaped streams for Unicode, malformed
output, range/count, cleanup, and timeout checks. Ingest destinations use real local
files and memory-manager records. Rate tests inspect arguments or simulated clocks;
the deadline test uses a real timer around an event-backed fake rather than a child
process. These tests do not establish live rclone service or TLS compatibility.
"""

from __future__ import annotations

import hashlib
import io
import subprocess
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.ingest.stores import ingest_store
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
from LiuXin_alpha.storage.storage_manager.manager import TransientStorageManager
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as backend_module,
)
from LiuXin_alpha.storage.stores import FilesystemStore
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def _extract_tpslimit(extra_args: tuple[str, ...]) -> float | None:
    """
    Find the first equals-form TPS limit in generated arguments and convert its suffix to float.

    Example:
        >>> _extract_tpslimit(("--tpslimit=0.5", "--tpslimit-burst=1"))
        0.5


    :param extra_args: Argument tuple searched in order; separate flag/value forms are not recognized by this test helper.
    :return: First matching float value or None; invalid matched numeric text raises rather than being skipped.
    """
    for argument in extra_args:
        if argument.startswith("--tpslimit="):
            return float(argument.split("=", 1)[1])
    return None


def test_rclone_readonly_preserves_unicode_inventory_hints_and_bytes(
    monkeypatch,
    tmp_path,
) -> None:
    """
    Preserve Unicode identity, reported hashes, and bytes through read-only rclone and real local
    ingest.

    Inject a decoded listing/stat runner and memory byte processes. Destination files are real local
    bytes, while the ingest manager keeps asset/replica metadata in memory. Assert exact source key,
    original filename, and readback.

    Example:
        >>> test_rclone_readonly_preserves_unicode_inventory_hints_and_bytes(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :param tmp_path: Temporary parent of a real filesystem ingest destination used with memory-manager records.
    :return: None after the stated regression assertions pass.
    """
    digest = hashlib.sha256(UNICODE_PAYLOAD).hexdigest()

    def _fake_json(args, **kwargs):
        """
        Return the Unicode fixture as stat metadata or a one-entry listing, including the
        independently computed payload hash.

        Example:
            >>> result = _fake_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Stat dictionary when --stat is present, otherwise a one-element list.
        """
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
        """
        Force inventory through the JSON fallback and provide the complete Unicode bytes for cat.

        Example:
            >>> result = _fake_spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: Successful memory process for reads; lsjson raises RuntimeError to select fallback.
        """
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
    manager = TransientStorageManager(
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
    """
    Reject short source bytes against reported identity before publishing local destination or
    manager records.

    The fake process exits successfully with five bytes, while metadata describes a longer payload
    and its hash. The ingest report must contain StorageIntegrityError, and destination, asset, and
    replica inventories must remain empty.

    Example:
        >>> test_truncated_rclone_ingest_publishes_no_manager_state(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :param tmp_path: Temporary parent of a real filesystem ingest destination used with memory-manager records.
    :return: None after the stated regression assertions pass.
    """
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
        """
        Return the retained authoritative-size/hash record in stat or listing form.

        Example:
            >>> result = _fake_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Shared record when --stat is present, otherwise a list containing that record.
        """
        del kwargs
        return record if "--stat" in args else [record]

    def _fake_spawn(self, args):
        """
        Force listing fallback and return a successful process carrying only the short transfer
        bytes.

        Example:
            >>> result = _fake_spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: Memory process with five-byte stdout for cat; lsjson raises RuntimeError.
        """
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
    manager = TransientStorageManager(
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
    """
    Exercise the shared hostile-Unicode corpus with exact keys, generated hashes, sliced fake reads,
    and URI round trips.

    Example:
        >>> test_rclone_readonly_reads_tortured_unicode_paths_exactly(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    payloads = {
        case.key: case.payload for case in TORTURED_UNICODE_PATH_CASES
    }

    def _record(key: str):
        """
        Derive filename, length, SHA-256, and a length-based synthetic ID for one corpus key.

        Example:
            >>> result = _record(key)  # doctest: +SKIP


        :param key: Exact key used to find the enclosing corpus payload.
        :return: New stat/listing dictionary derived from that key and payload.
        """
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
        """
        Build one exact-key stat record or all corpus records for the decoded listing path.

        Example:
            >>> result = _fake_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Record for a --stat target with remote: removed, otherwise the complete corpus record list.
        """
        del kwargs
        if "--stat" in args:
            return _record(args[-1].removeprefix("remote:"))
        return [_record(key) for key in payloads]

    def _fake_spawn(self, args):
        """
        Resolve the command target in the corpus and apply optional offset/count slices to its
        memory payload.

        Example:
            >>> result = _fake_spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: Successful byte-stream process; unsupported or unknown targets fail through the lookup rather than launching rclone.
        """
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
    """
    Verify that default invocation arguments encode 1200 requests per hour as a per-second TPS flag
    with burst one.

    Example:
        >>> test_rclone_backend_default_rate_limit_is_20_per_minute(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        """
        Capture both command and effective global arguments without running rclone.

        Example:
            >>> result = _fake_run_rclone_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Empty dictionary after updating the enclosing argument recorder.
        """
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
    """
    Use an injected 300-per-hour preference default when deriving the generated TPS limit.

    Example:
        >>> test_rclone_backend_default_rate_limit_reads_preferences(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        """
        Capture the effective global arguments generated from the injected preference default.

        Example:
            >>> result = _fake_run_rclone_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Empty dictionary after retaining extra_args as a tuple.
        """
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
    """
    Convert an explicit 120-per-hour runtime option to its corresponding per-second TPS flag.

    Example:
        >>> test_rclone_backend_custom_rate_limit_is_settable(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        """
        Capture generated global arguments for the explicit custom rate.

        Example:
            >>> result = _fake_run_rclone_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Empty dictionary after retaining extra_args as a tuple.
        """
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
    """
    Return the configured constant when the preference lookup supplies nonnumeric text.

    Example:
        >>> test_get_default_rclone_http_requests_per_hour_falls_back_on_invalid_value(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    import LiuXin_alpha.preferences as preferences_module

    original_get = preferences_module.preferences.get

    def _fake_get(option: str, default=None):
        """
        Return invalid rate text for the targeted preference and delegate all other options to the
        original getter.

        Example:
            >>> value = _fake_get(backend_module.RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY)  # doctest: +SKIP


        :param option: Preference name compared to the rclone request-rate key.
        :param default: Fallback value forwarded unchanged for other preference lookups.
        :return: The literal not-a-number for the target option, otherwise the original getter result.
        """
        if option == backend_module.RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY:
            return "not-a-number"
        return original_get(option, default)

    monkeypatch.setattr(preferences_module.preferences, "get", _fake_get)
    assert (
        backend_module.get_default_rclone_http_requests_per_hour()
        == backend_module.RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT
    )


def test_rclone_backend_can_disable_rate_limit_flags(monkeypatch) -> None:
    """
    Omit generated TPS and burst flags when the configured request rate is zero.

    Example:
        >>> test_rclone_backend_can_disable_rate_limit_flags(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    captured: dict[str, Any] = {}

    def _fake_run_rclone_json(args, **kwargs):
        """
        Capture global arguments so the test can prove that zero rate adds no TPS flags.

        Example:
            >>> result = _fake_run_rclone_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Empty dictionary after updating the argument recorder.
        """
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
    """
    Reserve two command slots on one Store and request a two-second delay using a fake monotonic
    clock.

    The sleep seam only records the delay. This checks per-instance scheduling, without measuring
    elapsed time or coordinating multiple Store instances.

    Example:
        >>> test_rclone_backend_global_rate_limit_spaces_commands(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    calls: list[list[str]] = []
    sleeps: list[float] = []
    monotonic_values = iter((0.0, 1.0))

    def _fake_run_rclone_json(args, **kwargs):
        """
        Append each invoked command to the enclosing recorder without real command or timing I/O.

        Example:
            >>> result = _fake_run_rclone_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Empty dictionary after recording the argument list.
        """
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
    """
    Convert decoded fixture entries into two owned Locations, skipping the entry without a usable
    path.

    Assert the declared complete-enumeration capability. The fixture describes synthetic file
    records rather than proving a live remote snapshot; process startup precedes the driver's
    decoded-runner fallback when applicable.

    Example:
        >>> test_rclone_store_enumerates_real_files_with_complete_semantics(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Normalize a plain HTTPS root and send the exact recursive files/hash argument vector to the
    forced JSON fallback.

    Example:
        >>> test_rclone_backend_normalizes_plain_https_root_to_configless_fs(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    captured_args: list[list[str]] = []

    def _fake_run_rclone_json(args, **kwargs):
        """
        Record the normalized-root inventory command and return an empty decoded listing.

        Example:
            >>> result = _fake_run_rclone_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Empty list after appending the command vector.
        """
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
    """
    Reject the recognized inline S3 secret_access_key assignment during Store construction.

    Example:
        >>> test_rclone_backend_rejects_inline_secret_configuration()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(StorageInvalidAddress):
        RcloneHttpReadOnlyStorageBackend(
            ':s3,provider="AWS",secret_access_key="do-not-persist":bucket',
            options=RcloneBackendOptions(max_http_requests_per_hour=0),
        )


def test_rclone_stat_read_digest_and_range_use_new_store_api(monkeypatch) -> None:
    """
    Check Location compatibility, metadata conversion, full/ranged reads, and exact rclone argument
    vectors.

    The fake hash is deliberately independent of the payload. Assertions check reported SHA-256
    selection, version, byte range, and commands without establishing that stat's reported hash
    authenticates those bytes.

    Example:
        >>> test_rclone_stat_read_digest_and_range_use_new_store_api(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    payload = b"0123456789"
    json_calls: list[list[str]] = []
    process_calls: list[list[str]] = []

    def _fake_json(args, **kwargs):
        """
        Record stat arguments and return fixed metadata whose reported hash is independent of the
        payload bytes.

        Example:
            >>> result = _fake_json(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: File metadata dictionary with size ten, fixed SHA-256 text, timestamp, and version.
        """
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
        """
        Record cat arguments and slice the fixed ten-byte payload according to offset/count options.

        Example:
            >>> result = _fake_spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: Successful memory process carrying the selected exact payload slice.
        """
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

    assert isinstance(location, Location)
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
    """
    Select the two alpha fixture keys through prefix inventory and reject both byte publication and
    deletion.

    Example:
        >>> test_rclone_prefix_inventory_and_read_only_enforcement(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject an oversized buffered streaming fragment and, separately, a three-entry fallback list
    under a two-entry limit.

    Example:
        >>> test_rclone_inventory_enforces_stream_token_and_entry_limits(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Preserve a successful read while selected fake close/poll/terminate calls raise ordinary
    exceptions.

    The stdout close marker must still be set. The fixture does not exercise hostile attribute
    lookup, every kill path, or BaseException cleanup failures.

    Example:
        >>> test_rclone_hostile_process_cleanup_cannot_mask_a_successful_read(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    payload = b"book"

    class _CloseBomb(io.BytesIO):
        """
        Close an in-memory byte stream normally, then raise an ordinary exception from the same
        close call.

        Example:
            >>> stream = _CloseBomb(b"book")  # doctest: +SKIP
        """
        def close(self) -> None:
            """
            Set the BytesIO closed state before raising the injected cleanup error.

            Example:
                >>> stream.close()  # doctest: +SKIP


            :return: Does not return normally; raises RuntimeError after the base stream closes.
            """
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
    """
    Translate the injected read failure, redact its specific token assignment, and bound the
    resulting diagnostic length.

    Example:
        >>> test_rclone_translates_and_redacts_arbitrary_hostile_stream_failures(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    class _HostileStdout:
        """
        Supply a failing read method with a token-bearing long diagnostic and a harmless close
        method.

        Example:
            >>> stream = _HostileStdout()  # doctest: +SKIP
        """
        def read(self, size=-1):
            """
            Raise a fixed token assignment followed by repeated noise for diagnostic
            translation/redaction assertions.

            Example:
                >>> stream.read(1)  # doctest: +SKIP


            :param size: Requested byte count ignored because every read fails.
            :return: Does not return; raises RuntimeError containing the specific fake secret and long diagnostic.
            """
            del size
            raise RuntimeError("token=supersecret " + ("noise " * 200))

        def close(self):
            """
            Accept cleanup without changing state or raising an additional error.

            Example:
                >>> stream.close()  # doctest: +SKIP


            :return: None; this double has no separate underlying resource to release.
            """
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
    """
    Reject the parameterized empty, absolute, parent, dot, repeated-separator, or backslash key
    form.

    Example:
        >>> test_rclone_store_rejects_noncanonical_keys(invalid)  # doctest: +SKIP


    :param invalid: Parameterized malformed relative key supplied directly to Store.locate.
    :return: None after the stated regression assertions pass.
    """
    store = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )
    with pytest.raises((StorageInvalidAddress, ValueError)):
        store.locate(invalid)


def test_rclone_missing_errors_are_not_flattened_to_false_metadata(monkeypatch) -> None:
    """
    Return false from the existence helper while preserving StorageNotFound from stat for the same
    missing fixture object.

    Example:
        >>> test_rclone_missing_errors_are_not_flattened_to_false_metadata(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    def _missing(args, **kwargs):
        """
        Reject every JSON request with the text marker used by the raw driver not-found classifier.

        Example:
            >>> result = _missing(args)  # doctest: +SKIP


        :param args: Rclone command vector accepted by the patched runner.
        :param kwargs: Invocation options accepted by the patched runner; unused unless explicitly captured.
        :return: Does not return; raises RuntimeError with an object-not-found message.
        """
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
    """
    Resolve an exact root-owned full identifier and reject a foreign root through the raw URI
    parser.

    Example:
        >>> test_rclone_locate_accepts_its_full_backend_identifier()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject the selected malformed UTF-8/JSON/path payload with the expected diagnostic and forbid
    fallback.

    Each BytesIO process reports successful exit. The cases cover malformed output already read by
    the parser, rather than every possible chunk boundary or unread trailing-output condition.

    Example:
        >>> test_rclone_streaming_inventory_rejects_malformed_remote_output(monkeypatch, payload, message)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :param payload: Parameterized bytes returned by the fake successful inventory process.
    :param message: Expected diagnostic substring identifying the selected malformed-output failure.
    :return: None after the stated regression assertions pass.
    """
    monkeypatch.setattr(
        backend_module,
        "run_rclone_json",
        lambda args, **kwargs: pytest.fail(
            "malformed streaming output must not be retried through fallback"
        ),
    )

    def _spawn(self, args):
        """
        Wrap the enclosing malformed byte payload in a process reporting successful completion.

        Example:
            >>> result = _spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: New BytesIO-backed process with exit status zero.
        """
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
    """
    Reject a StringIO inventory stream even when the fake process otherwise reports success.

    Example:
        >>> test_rclone_streaming_inventory_rejects_nonbinary_stdout(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    def _spawn(self, args):
        """
        Return text-mode empty-array output to violate the inventory byte-stream contract.

        Example:
            >>> result = _spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: Process-shaped object with StringIO stdout and a successful exit.
        """
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
    """
    Reject an inventory process whose stdout is missing before attempting streamed or fallback
    parsing.

    Example:
        >>> test_rclone_inventory_rejects_an_invalid_process_adapter(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Translate TimeoutExpired from process wait after a complete empty JSON array into
    StorageTimeout.

    Example:
        >>> test_rclone_inventory_translates_process_finish_timeout(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    def _wait(timeout=None):
        """
        Raise TimeoutExpired for the synthetic inventory command on every wait.

        Example:
            >>> _wait(timeout=1)  # doctest: +SKIP


        :param timeout: Optional wait duration retained in the constructed TimeoutExpired exception.
        :return: Does not return; raises subprocess.TimeoutExpired.
        """
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
    """
    Reject the nonzero process exit and connection-reset diagnostic after a valid listing entry has
    been yielded.

    Example:
        >>> test_rclone_inventory_checks_nonzero_exit_after_valid_json(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    def _spawn(self, args):
        """
        Return one valid JSON record followed by a reported process failure with connection-reset
        diagnostics.

        Example:
            >>> result = _spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: Memory process with exit status nine and the fixed stderr message.
        """
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
    """
    Reject a two-byte successful response to a four-byte request, then independently reject a failed
    unbounded read process.

    Example:
        >>> test_rclone_read_detects_truncated_count_and_nonzero_exit(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    payloads = iter(
        (
            (b"ab", b"", 0),
            (b"book", b"connection reset", 8),
        )
    )

    def _spawn(self, args):
        """
        Consume the next payload/diagnostic/status triple to model the two independent read
        failures.

        Example:
            >>> result = _spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: New memory process carrying the next scenario; the enclosing iterator determines call order.
        """
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
    """
    Translate a wait-time TimeoutError after empty stdout into a typed read timeout.

    Example:
        >>> test_rclone_read_translates_process_finish_timeout(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    def _wait(timeout=None):
        """
        Raise the fixed hung-process TimeoutError without waiting.

        Example:
            >>> _wait()  # doctest: +SKIP


        :param timeout: Process-compatible wait argument deliberately ignored.
        :return: Does not return; raises TimeoutError to exercise read completion translation.
        """
        del timeout
        raise TimeoutError("hung process")

    def _spawn(self, args):
        """
        Provide empty binary stdout and the enclosing wait function that always raises TimeoutError.

        Example:
            >>> result = _spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: Process-shaped object whose EOF completion check times out.
        """
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
    """
    Use a real deadline timer around an event-blocked fake process and require a typed read timeout
    plus kill marker.

    Executable lookup and Popen are replaced; no operating-system child is created. Event waits have
    bounded test deadlines to expose a missing kill.

    Example:
        >>> test_rclone_streaming_process_enforces_backend_timeout(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    class _BlockedProcess:
        """
        Model a process whose stdout blocks on an Event until a kill marks it finished.

        stdout is the fake itself and stderr is an in-memory stream. A real adapter timer drives
        kill; no operating-system process or signal is involved.

        Example:
            >>> process = _BlockedProcess()  # doctest: +SKIP
        """
        def __init__(self) -> None:
            """
            Initialize the completion Event, running status, self-backed stdout, stderr, and kill
            marker.

            Example:
                >>> process = _BlockedProcess()  # doctest: +SKIP


            :return: None after creating the initially unfinished process state.
            """
            self.finished = threading.Event()
            self.returncode = None
            self.stdout = self
            self.stderr = io.BytesIO()
            self.killed = False

        def read(self, size: int = -1) -> bytes:
            """
            Wait up to one second for completion, then return EOF instead of payload bytes.

            Example:
                >>> data = process.read()  # doctest: +SKIP


            :param size: Read-size argument ignored by the event-blocking fake.
            :return: Empty bytes after the Event is set; an assertion fails if the one-second test guard expires.
            """
            del size
            assert self.finished.wait(timeout=1)
            return b""

        def close(self) -> None:
            """
            Accept stdout cleanup without setting the completion Event or changing process status.

            Example:
                >>> process.close()  # doctest: +SKIP


            :return: None; this method intentionally does not unblock the fake process.
            """
            return None

        def wait(self, timeout=None) -> int:
            """
            Wait for the Event using the requested duration or a one-second falsey-value default.

            Example:
                >>> code = process.wait(timeout=1)  # doctest: +SKIP


            :param timeout: Wait duration passed to Event.wait after replacing falsey values with one second.
            :return: Retained non-None return code after completion; otherwise TimeoutExpired, or an assertion if state is inconsistent.
            """
            if not self.finished.wait(timeout=timeout or 1):
                raise subprocess.TimeoutExpired(["rclone"], timeout)
            assert self.returncode is not None
            return self.returncode

        def poll(self):
            """
            Inspect the retained fake exit status without blocking.

            Example:
                >>> running = process.poll() is None  # doctest: +SKIP


            :return: None until killed, then the simulated negative-nine status.
            """
            return self.returncode

        def terminate(self) -> None:
            """
            Delegate graceful termination to the fake immediate kill operation.

            Example:
                >>> process.terminate()  # doctest: +SKIP


            :return: None after setting the same markers as kill.
            """
            self.kill()

        def kill(self) -> None:
            """
            Record a forced exit and set the Event to release blocked readers and waiters.

            Example:
                >>> process.kill()  # doctest: +SKIP


            :return: None after setting killed=True, returncode=-9, and the completion Event.
            """
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
    """
    Reject an unpaired surrogate in text key parsing with a malformed-Unicode address diagnostic.

    Example:
        >>> test_rclone_rejects_unpaired_surrogate_object_paths(invalid)  # doctest: +SKIP


    :param invalid: Parameterized malformed relative key supplied directly to Store.locate.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject malformed filename Unicode, invalid or negative size, and a non-object stat response.

    Example:
        >>> test_rclone_stat_rejects_malformed_remote_metadata(monkeypatch, stat_blob)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :param stat_blob: Parameterized malformed decoded stat result supplied by the injected JSON runner.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject a None process returned by the injected cat spawner.

    Example:
        >>> test_rclone_open_read_rejects_an_invalid_process_adapter(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject text returned by the cat stdout stream while preserving caller context cleanup.

    Example:
        >>> test_rclone_read_rejects_nonbyte_process_output(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected command, process, preference, or timing seams after the regression.
    :return: None after the stated regression assertions pass.
    """
    def _spawn(self, args):
        """
        Return a successful process shape whose stdout supplies text instead of bytes.

        Example:
            >>> result = _spawn(store, args)  # doctest: +SKIP


        :param self: Injected Store receiver accepted by this nested replacement method and otherwise ignored.
        :param args: Rclone command vector supplied to the injected process method.
        :return: StringIO-backed process that exercises the raw reader byte-type rejection.
        """
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
