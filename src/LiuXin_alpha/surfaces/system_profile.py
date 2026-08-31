"""Resolve a LiuXin system manifest into transport-safe surface arguments.

The manifest is deliberately a small deployment description rather than an
application-preference store.  It tells a surface how to reach Core (directly
or through its HTTP endpoint) and where operator-owned local artifacts live.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


SYSTEM_MANIFEST_NAME = "liuxin-system.json"
SYSTEM_MANIFEST_FORMAT = "liuxin.system"
SYSTEM_MANIFEST_VERSION = 1
MAX_SYSTEM_MANIFEST_BYTES = 1024 * 1024
ACTIVE_CONNECTION_FORMAT = "liuxin.active-connection"
ACTIVE_CONNECTION_VERSION = 1
ACTIVE_CONNECTION_NAME = "active-connection.json"
MAX_ACTIVE_CONNECTION_BYTES = 64 * 1024
PROFILE_POINTER_FORMAT = "liuxin.profile-pointer"
PROFILE_POINTER_VERSION = 1


@dataclass(frozen=True, slots=True)
class ResolvedSystemProfile:
    """One validated manifest and its filesystem identity."""

    path: Path
    values: dict[str, Any]
    source: str

    @property
    def system_root(self) -> Path:
        raw = self.values.get("system_root")
        if raw not in (None, ""):
            return _manifest_path_value(self.path, raw)
        return self.path.parent


def default_named_profile_path(name: str) -> Path:
    """Return the XDG path used by a named ``--profile`` selector."""

    token = str(name).strip()
    if not token or token in {".", ".."} or any(
        separator in token for separator in ("/", "\\")
    ):
        raise ValueError("A named LiuXin profile must be a simple name.")
    config_home = os.environ.get("XDG_CONFIG_HOME")
    root = (
        Path(config_home).expanduser()
        if config_home
        else Path.home() / ".config"
    )
    return root / "liuxin" / "profiles" / (token + ".json")


def named_profiles_directory() -> Path:
    """Return the XDG directory containing named deployment selectors."""

    return default_named_profile_path("profile").parent


def iter_named_profile_paths() -> tuple[Path, ...]:
    """List named profile JSON files without interpreting their contents."""

    root = named_profiles_directory()
    if not root.is_dir():
        return ()
    return tuple(
        sorted(
            (
                path.absolute()
                for path in root.iterdir()
                if path.is_file()
                and not path.is_symlink()
                and path.suffix.casefold() == ".json"
            ),
            key=lambda path: path.name,
        )
    )


def active_connection_path() -> Path:
    """Return the per-user persisted connection-selection path."""

    config_home = os.environ.get("XDG_CONFIG_HOME")
    root = Path(config_home).expanduser() if config_home else Path.home() / ".config"
    return (root / "liuxin" / ACTIVE_CONNECTION_NAME).resolve(strict=False)


def persisted_manifest_path() -> Path | None:
    """Read and validate the persisted manifest pointer, if one exists."""

    path = active_connection_path()
    try:
        with path.open("rb") as stream:
            content = stream.read(MAX_ACTIVE_CONNECTION_BYTES + 1)
    except FileNotFoundError:
        return None
    if len(content) > MAX_ACTIVE_CONNECTION_BYTES:
        raise ValueError(
            "Persisted LiuXin connection exceeds the 64 KiB safety limit: {!s}."
            .format(path)
        )
    try:
        raw = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            "Invalid persisted LiuXin connection {!s}: {}; run `liuxin disconnect` "
            "and connect again.".format(path, error)
        ) from error
    if not isinstance(raw, Mapping):
        raise ValueError("Persisted LiuXin connection must contain a JSON object.")
    if raw.get("format") != ACTIVE_CONNECTION_FORMAT:
        raise ValueError("Unsupported persisted LiuXin connection format.")
    version = raw.get("version")
    if type(version) is not int or version != ACTIVE_CONNECTION_VERSION:
        raise ValueError("Unsupported persisted LiuXin connection version.")
    manifest = raw.get("manifest")
    if not isinstance(manifest, str) or not manifest.strip():
        raise ValueError("Persisted LiuXin connection has no manifest path.")
    selected = Path(manifest).expanduser()
    if not selected.is_absolute():
        raise ValueError("Persisted LiuXin connection manifest path must be absolute.")
    return selected.resolve(strict=False)


def persist_manifest_path(manifest: str | os.PathLike[str]) -> Path:
    """Atomically persist one credential-free manifest pointer, mode 0600."""

    selected = Path(manifest).expanduser().resolve(strict=False)
    if not selected.is_file():
        raise FileNotFoundError(
            "Cannot persist a connection to a missing manifest: {!s}.".format(
                selected
            )
        )
    path = active_connection_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = json.dumps(
        {
            "format": ACTIVE_CONNECTION_FORMAT,
            "version": ACTIVE_CONNECTION_VERSION,
            "manifest": str(selected),
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8") + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".active-connection-",
        suffix=".tmp",
        dir=str(path.parent),
    )
    staged = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(staged, 0o600)
        os.replace(staged, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            staged.unlink()
        except FileNotFoundError:
            pass
    return path


def clear_persisted_connection() -> bool:
    """Remove the per-user persisted selector, leaving every system intact."""

    path = active_connection_path()
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    _fsync_directory(path.parent)
    return True


def selected_manifest_path(
    *,
    system_root: str | os.PathLike[str] | None = None,
    profile: str | os.PathLike[str] | None = None,
    use_environment: bool = True,
    use_persisted: bool = True,
) -> tuple[Path | None, str | None]:
    """Resolve explicit, environment, or persisted selectors in precedence order."""

    root_value = None if system_root in (None, "") else os.fspath(system_root)
    profile_value = None if profile in (None, "") else os.fspath(profile)
    source: str | None = None
    if root_value and profile_value:
        raise ValueError("Use --system-root or --profile, not both.")
    if not root_value and not profile_value and use_environment:
        root_value = os.environ.get("LIUXIN_SYSTEM_ROOT") or None
        profile_value = os.environ.get("LIUXIN_PROFILE") or None
        if root_value and profile_value:
            raise ValueError(
                "LIUXIN_SYSTEM_ROOT and LIUXIN_PROFILE are mutually exclusive."
            )
        if root_value:
            source = "LIUXIN_SYSTEM_ROOT"
        elif profile_value:
            source = "LIUXIN_PROFILE"
    elif root_value:
        source = "system-root"
    elif profile_value:
        source = "profile"

    if not root_value and not profile_value and use_persisted:
        selected = persisted_manifest_path()
        if selected is not None:
            return selected, "active-connection"

    if root_value:
        return (
            Path(root_value).expanduser().resolve(strict=False)
            / SYSTEM_MANIFEST_NAME,
            source,
        )
    if profile_value:
        candidate = Path(profile_value).expanduser()
        if candidate.is_absolute() or candidate.parent != Path("."):
            selected = candidate
        elif candidate.suffix.lower() == ".json":
            selected = candidate
        else:
            selected = default_named_profile_path(profile_value)
        if selected.is_dir():
            selected = selected / SYSTEM_MANIFEST_NAME
        return selected.resolve(strict=False), source
    return None, None


def load_system_profile(
    *,
    system_root: str | os.PathLike[str] | None = None,
    profile: str | os.PathLike[str] | None = None,
    use_environment: bool = True,
    use_persisted: bool = True,
    required: bool = False,
    _seen: frozenset[Path] = frozenset(),
) -> ResolvedSystemProfile | None:
    """Load and validate the selected LiuXin deployment manifest."""

    path, source = selected_manifest_path(
        system_root=system_root,
        profile=profile,
        use_environment=use_environment,
        use_persisted=use_persisted,
    )
    if path is None:
        if required:
            raise ValueError(
                "Select a LiuXin system with --system-root, --profile, "
                "LIUXIN_SYSTEM_ROOT, LIUXIN_PROFILE, or `liuxin connect`."
            )
        return None
    path = path.resolve(strict=False)
    if path in _seen:
        raise ValueError(
            "LiuXin profile pointers contain a cycle at {!s}.".format(path)
        )
    try:
        with path.open("rb") as stream:
            content = stream.read(MAX_SYSTEM_MANIFEST_BYTES + 1)
    except FileNotFoundError as error:
        connection_hint = (
            " The persisted connection target is stale; run `liuxin disconnect` "
            "and `liuxin connect` again."
            if source == "active-connection"
            else ""
        )
        raise FileNotFoundError(
            "LiuXin system manifest does not exist: {!s}; run `liuxin init` "
            "or select another system.{}".format(path, connection_hint)
        ) from error
    if len(content) > MAX_SYSTEM_MANIFEST_BYTES:
        raise ValueError("LiuXin system manifest exceeds the 1 MiB limit.")
    try:
        raw = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            "Invalid UTF-8 JSON in LiuXin system manifest {!s}: {}".format(
                path, error
            )
        ) from error
    if not isinstance(raw, Mapping):
        raise ValueError("LiuXin system manifest must contain a JSON object.")
    values = {str(key): value for key, value in raw.items()}
    if values.get("format") == PROFILE_POINTER_FORMAT:
        version = values.get("version")
        if type(version) is not int or version != PROFILE_POINTER_VERSION:
            raise ValueError(
                "Unsupported LiuXin named-profile pointer version in {!s}."
                .format(path)
            )
        manifest = values.get("manifest")
        if not isinstance(manifest, str) or not manifest.strip():
            raise ValueError(
                "LiuXin named-profile pointer has no manifest path."
            )
        target = Path(manifest).expanduser()
        if not target.is_absolute():
            raise ValueError(
                "LiuXin named-profile pointer manifest path must be absolute."
            )
        resolved = load_system_profile(
            profile=str(target),
            use_environment=False,
            use_persisted=False,
            required=True,
            _seen=_seen | {path},
        )
        assert resolved is not None
        return ResolvedSystemProfile(
            path=resolved.path,
            values=resolved.values,
            source=source or "profile",
        )
    if values.get("format") != SYSTEM_MANIFEST_FORMAT:
        raise ValueError(
            "Unsupported LiuXin system manifest format in {!s}.".format(path)
        )
    try:
        version_value = values.get("version", 0)
        if not isinstance(version_value, (str, int, float)):
            raise TypeError
        version = int(version_value)
    except (TypeError, ValueError) as error:
        raise ValueError("LiuXin system manifest version must be an integer.") from error
    if version != SYSTEM_MANIFEST_VERSION:
        raise ValueError(
            "Unsupported LiuXin system manifest version {} in {!s}.".format(
                version, path
            )
        )
    database = values.get("database")
    endpoint = values.get("core_endpoint")
    if database not in (None, "") and endpoint not in (None, ""):
        raise ValueError(
            "LiuXin system manifest must select either database or core_endpoint."
        )
    if database in (None, "") and endpoint in (None, ""):
        raise ValueError(
            "LiuXin system manifest must define database or core_endpoint."
        )
    db_type = str(values.get("db_type") or "SQLite").strip()
    if not db_type:
        raise ValueError("LiuXin system manifest db_type must not be empty.")
    values["db_type"] = db_type
    database_metadata = values.get("database_metadata")
    if database_metadata is not None and not isinstance(database_metadata, Mapping):
        raise ValueError("LiuXin system manifest database_metadata must be an object.")
    if database not in (None, "") and db_type.casefold() in {"sqlite", "apsw"}:
        values["database"] = str(_manifest_path_value(path, database))
    for name in (
        "system_root",
        "store_root",
        "materialization_root",
        "log_directory",
    ):
        if values.get(name) not in (None, ""):
            values[name] = str(_manifest_path_value(path, values[name]))
    return ResolvedSystemProfile(path=path, values=values, source=source or "explicit")


def apply_system_profile(args: argparse.Namespace) -> ResolvedSystemProfile | None:
    """Apply a selected manifest to an argparse namespace in place.

    Explicit ``--database``/``--core-endpoint`` values are authoritative and
    suppress environment-selected profiles.  Combining an explicit transport
    with an explicit profile selector is rejected to avoid surprising targets.
    """

    database = getattr(args, "database", None)
    endpoint = getattr(args, "core_endpoint", None)
    system_root = getattr(args, "system_root", None)
    profile_name = getattr(args, "profile", None)
    explicit_selector = bool(system_root or profile_name)
    if database and endpoint:
        raise ValueError("Use --database or --core-endpoint, not both.")
    already_resolved = bool(getattr(args, "resolved_system_manifest", None))
    if (database or endpoint) and explicit_selector and not already_resolved:
        raise ValueError(
            "Do not combine --database/--core-endpoint with --system-root/--profile."
        )
    if database or endpoint:
        return None
    resolved = load_system_profile(
        system_root=system_root,
        profile=profile_name,
        use_environment=True,
        required=False,
    )
    if resolved is None:
        raise ValueError(
            "Select exactly one of --database, --core-endpoint, --system-root, "
            "or --profile, set LIUXIN_SYSTEM_ROOT/LIUXIN_PROFILE, or run "
            "`liuxin connect`."
        )
    values = resolved.values
    args.database = values.get("database") or None
    args.core_endpoint = values.get("core_endpoint") or None
    args.db_type = str(values.get("db_type") or getattr(args, "db_type", "SQLite"))
    setattr(
        args,
        "database_metadata",
        dict(values.get("database_metadata") or {}),
    )
    for name in ("materialization_root", "log_directory"):
        if hasattr(args, name) and getattr(args, name, None) in (None, ""):
            setattr(args, name, values.get(name) or None)
    setattr(args, "resolved_system_manifest", str(resolved.path))
    return resolved


def redacted_manifest(values: Mapping[str, Any]) -> dict[str, Any]:
    """Return a display-safe manifest without embedded URL credentials."""

    result = {str(key): value for key, value in values.items()}
    for key in tuple(result):
        token = key.casefold()
        if any(marker in token for marker in ("password", "secret", "token", "key")):
            result[key] = "<redacted>"
    for key in ("database", "core_endpoint"):
        value = result.get(key)
        if isinstance(value, str):
            result[key] = _redact_url(value)
    metadata = result.get("database_metadata")
    if isinstance(metadata, Mapping):
        result["database_metadata"] = redacted_manifest(metadata)
    return result


def _manifest_path_value(manifest: Path, value: Any) -> Path:
    path = Path(os.fspath(value)).expanduser()
    if not path.is_absolute():
        path = manifest.parent / path
    return path.resolve(strict=False)


def _redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    if not parsed.scheme or parsed.password is None:
        return value
    hostname = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError:
        return value
    if port is not None:
        hostname += ":{}".format(port)
    username = "" if parsed.username is None else parsed.username + ":<redacted>@"
    return urlunsplit(
        (parsed.scheme, username + hostname, parsed.path, parsed.query, parsed.fragment)
    )


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "MAX_SYSTEM_MANIFEST_BYTES",
    "PROFILE_POINTER_FORMAT",
    "PROFILE_POINTER_VERSION",
    "ResolvedSystemProfile",
    "SYSTEM_MANIFEST_FORMAT",
    "SYSTEM_MANIFEST_NAME",
    "SYSTEM_MANIFEST_VERSION",
    "apply_system_profile",
    "default_named_profile_path",
    "iter_named_profile_paths",
    "load_system_profile",
    "named_profiles_directory",
    "redacted_manifest",
    "selected_manifest_path",
]
