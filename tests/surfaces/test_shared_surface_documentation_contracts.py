"""
Exercise shared surface documentation against profile selection/persistence and presentation edge behavior.

An autouse fixture confines configuration, manifests, and connection pointers to
pytest temporary directories. Tests do not use the real user's profiles, modify
databases, or connect to the example endpoint. Failure injection distinguishes
pre-replacement cleanup from errors after a pointer is already installed.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from LiuXin_alpha.surfaces import presentation
from LiuXin_alpha.surfaces import system_profile as profiles
from LiuXin_alpha.surfaces.acquisition_types import CoreStoredFile


@pytest.fixture(autouse=True)
def isolated_profiles(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """
    Confine config lookup and relative-path resolution to a fresh test directory with no environment selectors.

    Example:
        >>> root = request.getfixturevalue("isolated_profiles")  # doctest: +SKIP


    :param monkeypatch: Fixture restoring the config/selector environment and current directory after each test.
    :param tmp_path: Temporary workspace used for all profile and pointer artifacts.
    :return: The temporary workspace path, without creating any real-user configuration entries.
    """
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("LIUXIN_SYSTEM_ROOT", raising=False)
    monkeypatch.delenv("LIUXIN_PROFILE", raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _json_file(path: Path, value: Any) -> Path:
    """
    Write one UTF-8 JSON fixture under a caller-supplied temporary path, creating its parents as needed.

    Example:
        >>> path = _json_file(temporary_directory / "fixture.json", {})  # doctest: +SKIP


    :param path: Test-owned file to create or replace, never a real profile path.
    :param value: JSON-serializable fixture value, which may deliberately violate manifest structure.
    :return: The same fixture path after writing its JSON representation.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _manifest(path: Path, **overrides: Any) -> Path:
    """
    Write a minimal remote system manifest with explicit field overrides for validation tests.

    Example:
        >>> path = _manifest(temporary_directory / "manifest.json", version="1")  # doctest: +SKIP


    :param path: Temporary manifest path, with parents created by the JSON fixture writer.
    :param overrides: Fields replacing or extending the default system format/version/endpoint mapping.
    :return: Written fixture path; the endpoint is only example data and is never contacted.
    """
    return _json_file(
        path,
        {
            "format": profiles.SYSTEM_MANIFEST_FORMAT,
            "version": 1,
            "core_endpoint": "http://example.invalid:8080",
            **overrides,
        },
    )


def test_named_path_normalization_and_relative_xdg_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Strip simple names without removing existing suffixes and distinguish unresolved named paths from resolved active paths.

    Example:
        >>> test_named_path_normalization_and_relative_xdg_root(patch)  # doctest: +SKIP


    :param monkeypatch: Fixture selecting a relative config root inside the isolated temporary current directory.
    :return: None after name rejection, suffix appending, and named/active path-resolution assertions.
    """
    monkeypatch.setenv("XDG_CONFIG_HOME", "relative-config")
    assert profiles.default_named_profile_path(" reading ") == Path(
        "relative-config/liuxin/profiles/reading.json"
    )
    assert (
        profiles.default_named_profile_path("reading.json").name == "reading.json.json"
    )
    assert not profiles.named_profiles_directory().is_absolute()
    assert profiles.active_connection_path() == (
        Path.cwd() / "relative-config/liuxin/active-connection.json"
    )
    for name in ("", " ", ".", "..", "a/b", "a\\b"):
        with pytest.raises(ValueError, match="simple name"):
            profiles.default_named_profile_path(name)


def test_named_profile_listing_filters_direct_files_without_loading_json() -> None:
    """
    List only direct non-symlink JSON files, preserving case-sensitive filename order and ignoring malformed contents.

    Example:
        >>> test_named_profile_listing_filters_direct_files_without_loading_json()  # doctest: +SKIP


    :return: None after missing-directory, suffix, recursion, content-independence, and symlink-filter checks.
    """
    assert profiles.iter_named_profile_paths() == ()
    root = profiles.named_profiles_directory()
    _json_file(root / "z.json", {})
    (root / "A.JSON").write_text("not json", encoding="utf-8")
    _json_file(root / "ignore.txt", {})
    _json_file(root / "nested" / "inside.json", {})
    if os.name == "posix":
        (root / "alias.json").symlink_to(root / "z.json")
    assert profiles.iter_named_profile_paths() == (
        (root / "A.JSON").absolute(),
        (root / "z.json").absolute(),
    )


def test_selector_precedence_and_simple_directory_names(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    Prefer explicit over environment over persisted selection and treat a simple directory token as a named profile.

    Example:
        >>> test_selector_precedence_and_simple_directory_names(patch, temporary_directory)  # doctest: +SKIP


    :param monkeypatch: Fixture replacing persisted lookup and selecting conflicting environment values.
    :param tmp_path: Workspace supplying real directories without opening any manifest contents.
    :return: None after exact selector paths/source labels, suppression, and conflict assertions.
    """
    persisted = Mock(return_value=tmp_path / "persisted.json")
    monkeypatch.setattr(profiles, "persisted_manifest_path", persisted)
    assert profiles.selected_manifest_path() == (
        tmp_path / "persisted.json",
        "active-connection",
    )
    monkeypatch.setenv("LIUXIN_SYSTEM_ROOT", str(tmp_path / "environment"))
    assert profiles.selected_manifest_path() == (
        tmp_path / "environment/liuxin-system.json",
        "LIUXIN_SYSTEM_ROOT",
    )
    monkeypatch.setenv("LIUXIN_PROFILE", "also-selected")
    with pytest.raises(ValueError, match="mutually exclusive"):
        profiles.selected_manifest_path()
    assert profiles.selected_manifest_path(system_root="explicit") == (
        tmp_path / "explicit/liuxin-system.json",
        "system-root",
    )
    assert persisted.call_count == 1
    (tmp_path / "reading").mkdir()
    selected, source = profiles.selected_manifest_path(profile="reading")
    assert (
        selected == profiles.default_named_profile_path("reading").resolve()
        and source == "profile"
    )
    assert (
        profiles.selected_manifest_path(profile=tmp_path / "reading")[0]
        == tmp_path / "reading/liuxin-system.json"
    )
    assert (
        profiles.selected_manifest_path(profile="local.JSON")[0]
        == tmp_path / "local.JSON"
    )
    with pytest.raises(ValueError, match="not both"):
        profiles.selected_manifest_path(system_root="one", profile="two")
    assert profiles.selected_manifest_path(
        use_environment=False, use_persisted=False
    ) == (None, None)


@pytest.mark.parametrize("version", [1, "1", 1.9, True])
def test_manifest_versions_keep_integer_coercion_and_path_resolution(
    version: Any, tmp_path: Path
) -> None:
    """
    Retain permissive system-version coercion, relative SQLite paths, and unvalidated extra/endpoint values.

    Example:
        >>> test_manifest_versions_keep_integer_coercion_and_path_resolution(1.9, temporary_directory)  # doctest: +SKIP


    :param version: Version accepted because int conversion produces the supported value one.
    :param tmp_path: Workspace holding manifests and deliberately nonexistent path targets.
    :return: None after normalized local paths, retained source values, and non-probing remote behavior checks.
    """
    path = _manifest(
        tmp_path / "deployment/manifest.json",
        version=version,
        core_endpoint=None,
        database="../catalogue.sqlite",
        db_type=" APSW ",
        system_root=".",
        store_root="../store",
        materialization_root="materialized",
        log_directory="logs",
        extra={"retained": True},
    )
    result = profiles.load_system_profile(profile=path)
    assert result is not None and result.source == "profile"
    assert result.values["version"] == version and result.values["db_type"] == "APSW"
    assert result.values["database"] == str(tmp_path / "catalogue.sqlite")
    assert result.values["store_root"] == str(tmp_path / "store")
    assert result.values["materialization_root"] == str(path.parent / "materialized")
    assert result.system_root == path.parent and result.values["extra"] == {
        "retained": True
    }
    assert not Path(result.values["database"]).exists()
    remote_path = _manifest(
        tmp_path / "remote.json", core_endpoint="not a validated URL"
    )
    remote = profiles.load_system_profile(profile=remote_path)
    assert (
        remote is not None and remote.values["core_endpoint"] == "not a validated URL"
    )
    server_path = _manifest(
        tmp_path / "server.json",
        core_endpoint=None,
        database="service=~catalogue",
        db_type=" pg ",
    )
    server = profiles.load_system_profile(profile=server_path)
    assert server is not None and server.values["database"] == "service=~catalogue"


def test_profile_pointer_chains_preserve_outer_source_and_reject_cycles(
    tmp_path: Path,
) -> None:
    """
    Follow absolute pointers to the final manifest while retaining the outer selector's source and detecting resolved cycles.

    Example:
        >>> test_profile_pointer_chains_preserve_outer_source_and_reject_cycles(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Workspace containing two pointer files and their final remote-manifest fixture.
    :return: None after final-path/source selection, strict pointer-version, and cycle assertions.
    """
    manifest = _manifest(tmp_path / "final/manifest.json")
    second = _json_file(
        tmp_path / "second.json",
        {
            "format": profiles.PROFILE_POINTER_FORMAT,
            "version": 1,
            "manifest": str(manifest),
        },
    )
    first = _json_file(
        tmp_path / "root/liuxin-system.json",
        {
            "format": profiles.PROFILE_POINTER_FORMAT,
            "version": 1,
            "manifest": str(second),
        },
    )
    result = profiles.load_system_profile(system_root=first.parent)
    assert (
        result is not None
        and result.path == manifest
        and result.source == "system-root"
    )
    for version in (True, "1", 1.0):
        _json_file(
            second,
            {
                "format": profiles.PROFILE_POINTER_FORMAT,
                "version": version,
                "manifest": str(manifest),
            },
        )
        with pytest.raises(ValueError, match="pointer version"):
            profiles.load_system_profile(profile=first)
    _json_file(
        second,
        {
            "format": profiles.PROFILE_POINTER_FORMAT,
            "version": 1,
            "manifest": str(first),
        },
    )
    with pytest.raises(ValueError, match="cycle"):
        profiles.load_system_profile(profile=first)


@pytest.mark.parametrize(
    "content, message",
    [
        (b"not JSON", "Invalid UTF-8 JSON"),
        (b"\xff", "Invalid UTF-8 JSON"),
        (b"[]", "JSON object"),
        (b"{}", "Unsupported.*format"),
    ],
)
def test_manifest_read_failures_are_not_optional_absence(
    content: bytes, message: str, tmp_path: Path
) -> None:
    """
    Treat malformed selected manifests as failures even when selection itself is optional.

    Example:
        >>> test_manifest_read_failures_are_not_optional_absence(b"[]", "JSON object", temporary_directory)  # doctest: +SKIP


    :param content: Deliberately malformed UTF-8, JSON, or manifest-format bytes.
    :param message: Expected error-message pattern for that malformed content.
    :param tmp_path: Workspace receiving the selected fixture file.
    :return: None after malformed, missing-selected-file, and required-no-selection checks.
    """
    path = tmp_path / "bad.json"
    path.write_bytes(content)
    with pytest.raises(ValueError, match=message):
        profiles.load_system_profile(profile=path, required=False)
    with pytest.raises(FileNotFoundError, match="does not exist"):
        profiles.load_system_profile(profile=tmp_path / "missing.json")
    with pytest.raises(ValueError, match="Select a LiuXin system"):
        profiles.load_system_profile(
            use_environment=False, use_persisted=False, required=True
        )


def test_manifest_and_active_pointer_size_bounds_and_stale_target(
    tmp_path: Path,
) -> None:
    """
    Reject over-limit byte reads and keep persisted target resolution separate from target existence validation.

    Example:
        >>> test_manifest_and_active_pointer_size_bounds_and_stale_target(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Workspace for oversized files and an intentionally stale absolute target.
    :return: None after both size guards, stale-pointer resolution, and load-time guidance assertions.
    """
    path = tmp_path / "huge.json"
    path.write_bytes(b" " * (profiles.MAX_SYSTEM_MANIFEST_BYTES + 1))
    with pytest.raises(ValueError, match="1 MiB"):
        profiles.load_system_profile(profile=path)
    pointer = _json_file(profiles.active_connection_path(), {})
    pointer.write_bytes(b" " * (profiles.MAX_ACTIVE_CONNECTION_BYTES + 1))
    with pytest.raises(ValueError, match="64 KiB"):
        profiles.persisted_manifest_path()
    target = tmp_path / "missing.json"
    _json_file(
        pointer,
        {
            "format": profiles.ACTIVE_CONNECTION_FORMAT,
            "version": 1,
            "manifest": str(target),
        },
    )
    assert profiles.persisted_manifest_path() == target
    with pytest.raises(FileNotFoundError, match="persisted connection target is stale"):
        profiles.load_system_profile()


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"version": True}, "version"),
        ({"version": "1"}, "version"),
        ({"version": 1.0}, "version"),
        ({"format": "other"}, "format"),
        ({"manifest": "relative.json"}, "absolute"),
        ({"manifest": " "}, "no manifest path"),
    ],
)
def test_active_pointer_validation_is_stricter_than_manifest_versions(
    overrides: dict[str, Any], message: str, tmp_path: Path
) -> None:
    """
    Require exact pointer format, actual integer version, and nonblank absolute target text.

    Example:
        >>> test_active_pointer_validation_is_stricter_than_manifest_versions({"version": True}, "version", temporary_directory)  # doctest: +SKIP


    :param overrides: Fields invalidating an otherwise well-formed active-connection pointer.
    :param message: Expected validation-error pattern for the overridden field.
    :param tmp_path: Workspace supplying an absolute but not necessarily existing default target.
    :return: None after the active-pointer parser rejects the supplied fixture.
    """
    _json_file(
        profiles.active_connection_path(),
        {
            "format": profiles.ACTIVE_CONNECTION_FORMAT,
            "version": 1,
            "manifest": str(tmp_path / "manifest.json"),
            **overrides,
        },
    )
    with pytest.raises(ValueError, match=message):
        profiles.persisted_manifest_path()


def test_persistence_writes_only_pointer_and_clear_leaves_manifest_intact(
    tmp_path: Path,
) -> None:
    """
    Persist an existing file without validating its contents, enforce pointer mode on POSIX, and remove only the selector.

    Example:
        >>> test_persistence_writes_only_pointer_and_clear_leaves_manifest_intact(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Workspace for an intentionally non-JSON target and isolated config directory.
    :return: None after pointer fields/permissions, no-content-copy, clearing, and missing-target checks.
    """
    manifest = tmp_path / "not-a-manifest.json"
    manifest.write_text("not JSON: example credential", encoding="utf-8")
    pointer = profiles.persist_manifest_path(manifest)
    assert json.loads(pointer.read_text(encoding="utf-8")) == {
        "format": profiles.ACTIVE_CONNECTION_FORMAT,
        "version": 1,
        "manifest": str(manifest),
    }
    if os.name == "posix":
        assert stat.S_IMODE(pointer.stat().st_mode) == 0o600
        assert stat.S_IMODE(pointer.parent.stat().st_mode) == 0o700
    assert profiles.persisted_manifest_path() == manifest
    assert profiles.clear_persisted_connection() is True
    assert profiles.clear_persisted_connection() is False and manifest.is_file()
    assert profiles.persisted_manifest_path() is None
    with pytest.raises(FileNotFoundError, match="missing manifest"):
        profiles.persist_manifest_path(tmp_path / "absent.json")


def test_failed_pointer_replacement_preserves_old_pointer_and_cleans_temporary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    Propagate a pre-replacement failure while leaving the previous selector intact and removing the staged file.

    Example:
        >>> test_failed_pointer_replacement_preserves_old_pointer_and_cleans_temporary(patch, temporary_directory)  # doctest: +SKIP


    :param monkeypatch: Fixture making os.replace fail after the temporary pointer has been written.
    :param tmp_path: Workspace containing old/new manifest targets and isolated pointer storage.
    :return: None after exact exception identity, old-pointer preservation, and staged-file cleanup assertions.
    """
    old = _manifest(tmp_path / "old.json")
    new = _manifest(tmp_path / "new.json")
    pointer = profiles.persist_manifest_path(old)
    before = pointer.read_bytes()
    failure = OSError("replace failed")
    monkeypatch.setattr(profiles.os, "replace", Mock(side_effect=failure))
    with pytest.raises(OSError) as raised:
        profiles.persist_manifest_path(new)
    assert raised.value is failure and pointer.read_bytes() == before
    assert list(pointer.parent.glob(".active-connection-*.tmp")) == []


def test_directory_sync_failure_occurs_after_pointer_installation_or_removal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    Expose post-commit durability failures without falsely promising pointer rollback or reconstruction.

    Example:
        >>> test_directory_sync_failure_occurs_after_pointer_installation_or_removal(patch, temporary_directory)  # doctest: +SKIP


    :param monkeypatch: Fixture making the directory-sync step fail without changing file write/replace behavior.
    :param tmp_path: Workspace holding the target manifest and isolated persisted selector.
    :return: None after visible errors accompany an installed pointer and, later, an already removed pointer.
    """
    manifest = _manifest(tmp_path / "manifest.json")
    failure = OSError("directory sync failed")
    monkeypatch.setattr(profiles, "_fsync_directory", Mock(side_effect=failure))
    with pytest.raises(OSError) as raised:
        profiles.persist_manifest_path(manifest)
    pointer = profiles.active_connection_path()
    assert raised.value is failure and profiles.persisted_manifest_path() == manifest
    assert list(pointer.parent.glob(".active-connection-*.tmp")) == []
    with pytest.raises(OSError) as raised:
        profiles.clear_persisted_connection()
    assert raised.value is failure and not pointer.exists() and manifest.exists()


def test_directory_sync_ignores_only_open_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """
    Ignore opening OSError but propagate sync failure while still closing an acquired directory descriptor.

    Example:
        >>> test_directory_sync_ignores_only_open_failure(patch, temporary_directory)  # doctest: +SKIP


    :param monkeypatch: Fixture installing recording os.open/fsync/close doubles with no real descriptors.
    :param tmp_path: Directory argument supplied to the sync helper.
    :return: None after the open-error early return and post-open cleanup/error identity checks.
    """
    opener = Mock(side_effect=OSError("open failed"))
    sync = Mock()
    closer = Mock()
    monkeypatch.setattr(profiles.os, "open", opener)
    monkeypatch.setattr(profiles.os, "fsync", sync)
    monkeypatch.setattr(profiles.os, "close", closer)
    profiles._fsync_directory(tmp_path)
    sync.assert_not_called()
    closer.assert_not_called()
    opener.side_effect = None
    opener.return_value = 71
    failure = OSError("sync failed")
    sync.side_effect = failure
    with pytest.raises(OSError) as raised:
        profiles._fsync_directory(tmp_path)
    assert raised.value is failure
    sync.assert_called_once_with(71)
    closer.assert_called_once_with(71)


def test_namespace_application_bypasses_explicit_transport_and_copies_only_selected_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    Preserve explicit-transport bypass, marker-based conflict allowance, and selective namespace mutation.

    Example:
        >>> test_namespace_application_bypasses_explicit_transport_and_copies_only_selected_fields(patch, temporary_directory)  # doctest: +SKIP


    :param monkeypatch: Fixture replacing profile loading with a known mutable resolved profile.
    :param tmp_path: Workspace supplying the recorded manifest identity.
    :return: None after bypass/conflict rules, copied metadata, retained paths, and omitted-field assertions.
    """
    values = {
        "database": "/example/catalogue.sqlite",
        "db_type": "SQLite",
        "database_metadata": {"nested": []},
        "materialization_root": "/example/materialized",
        "log_directory": "/example/logs",
        "system_root": "/example",
        "store_root": "/example/store",
    }
    profile = profiles.ResolvedSystemProfile(
        tmp_path / "manifest.json", values, "profile"
    )
    loader = Mock(return_value=profile)
    monkeypatch.setattr(profiles, "load_system_profile", loader)
    assert (
        profiles.apply_system_profile(argparse.Namespace(database="explicit")) is None
    )
    assert (
        profiles.apply_system_profile(
            argparse.Namespace(
                database="explicit",
                profile="choice",
                resolved_system_manifest="unverified-marker",
            )
        )
        is None
    )
    loader.assert_not_called()
    with pytest.raises(ValueError, match="Do not combine"):
        profiles.apply_system_profile(
            argparse.Namespace(database="explicit", profile="choice")
        )
    with pytest.raises(ValueError, match="not both"):
        profiles.apply_system_profile(
            argparse.Namespace(database="explicit", core_endpoint="endpoint")
        )
    args = argparse.Namespace(
        profile="choice", materialization_root="", log_directory="keep"
    )
    assert profiles.apply_system_profile(args) is profile
    assert args.database == values["database"] and args.core_endpoint is None
    assert (
        args.database_metadata == values["database_metadata"]
        and args.database_metadata is not values["database_metadata"]
    )
    assert args.database_metadata["nested"] is values["database_metadata"]["nested"]
    assert (
        args.materialization_root == values["materialization_root"]
        and args.log_directory == "keep"
    )
    assert not hasattr(args, "system_root") and not hasattr(args, "store_root")
    assert args.resolved_system_manifest == str(profile.path)


def test_redaction_is_limited_and_preserves_unrecognized_nested_values() -> None:
    """
    Mask recognized key substrings and URL passwords without claiming arbitrary nested/query/invalid-URL secrets are removed.

    Example:
        >>> test_redaction_is_limited_and_preserves_unrecognized_nested_values()


    :return: None after exact masks, retained username/query data, malformed-URL fallback, and shallow alias checks.
    """
    extra = {"password": "nested-example"}
    original = {
        "monkey": "benign substring",
        "PASSWORD": "example",
        "database": "postgres://reader:example@DB:5432/catalogue?token=query-example#fragment",
        "database_metadata": {"token": "example", "extra": extra},
        "extra": extra,
    }
    result = profiles.redacted_manifest(original)
    assert result["monkey"] == result["PASSWORD"] == "<redacted>"
    assert (
        result["database"]
        == "postgres://reader:<redacted>@db:5432/catalogue?token=query-example#fragment"
    )
    assert result["database_metadata"]["token"] == "<redacted>"
    assert result["extra"] is result["database_metadata"]["extra"] is extra
    assert original["PASSWORD"] == "example"
    malformed = "https://reader:example@example.invalid:bad/books"
    assert profiles._redact_url(malformed) == malformed
    assert profiles._redact_url("//reader:example@example.invalid/books").startswith(
        "//reader:example@"
    )
    assert (
        profiles._redact_url("https://reader:example@[::1]/books")
        == "https://reader:<redacted>@::1/books"
    )


def test_presentation_fallback_boundaries_and_stored_reader_identity() -> None:
    """
    Preserve broad initial option fallback, visible bound errors, narrow-width dots, and uncached byte-reader identity.

    Example:
        >>> test_presentation_fallback_boundaries_and_stored_reader_identity()


    :return: None after initial-conversion failure, inconsistent widths, and repeated reader-call assertions.
    """

    class Unprintable:
        """
        Simulate a value whose initial string conversion raises a non-ValueError exception.

        Example:
            >>> value = Unprintable()  # doctest: +SKIP
        """

        def __str__(self) -> str:
            """
            Always raise a simulated formatting failure for parser-boundary tests.

            Example:
                >>> str(value)  # doctest: +SKIP


            :return: No value; RuntimeError is raised on every conversion.
            :raises RuntimeError: Always, to exercise the parser's broad initial catch.
            """
            raise RuntimeError("string conversion failed")

    assert presentation.coerce_int(Unprintable(), default=4) == 4
    with pytest.raises(ValueError):
        presentation.coerce_int("bad", default="also bad")
    with pytest.raises(ValueError):
        presentation.coerce_int("2", default=4, minimum="bad")
    assert presentation.short_text("", width=-1) == "..."
    assert presentation.escape("&amp;") == "&amp;amp;"
    reader = Mock()
    payload = b"same payload"
    reader.acquisition_read.return_value = ({"ignored": True}, payload)
    stored = CoreStoredFile(reader, "file", 7)
    assert stored.read_bytes() is payload and stored.read_bytes() is payload
    assert reader.acquisition_read.call_count == 2
    reader.acquisition_read.assert_called_with("file", 7)
