"""
Select deployment manifests, resolve local paths, and manage a per-user connection pointer for surfaces.

Explicit selectors take precedence over environment selectors and the persisted
connection. Bounded UTF-8 JSON reads validate manifest/pointer structure; they do
not probe Core, open a database, or guarantee referenced paths exist. Named-profile
pointers can chain to manifests with cycle detection. Deployment values are not
application preferences, and path resolution is not confinement to a system root.

Persistence writes only the selected manifest path and pointer metadata, not the
manifest contents. Redaction is a limited display helper, not a comprehensive
secret scrubber or safe-URL validator. Selection helpers can consult the user's
environment and config directory unless explicitly disabled by their arguments.
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
    """
    Hold a selected manifest's path, normalized values, and selection-source label.

    Loading supplies validated values, but this dataclass constructor performs no
    validation itself. Frozen slotted fields do not freeze or copy the values dict;
    later mutations to it affect properties such as system_root. The path identifies
    the final manifest, while source can identify an outer named-profile pointer.

    Example:
        >>> profile = ResolvedSystemProfile(Path("deployment/liuxin-system.json"), {}, "explicit")
        >>> profile.system_root == Path("deployment")
        True
    """

    path: Path
    values: dict[str, Any]
    source: str

    @property
    def system_root(self) -> Path:
        """
        Resolve a nonempty system_root value relative to the manifest, or use its parent directory unchanged.

        Resolving an explicit value expands the user directory and permits paths
        outside the manifest directory; neither result is checked for existence.

        Example:
            >>> ResolvedSystemProfile(Path("deployment/manifest.json"), {}, "explicit").system_root == Path("deployment")
            True


        :return: Resolved configured root, or the retained manifest path's parent when the value is None/empty.
        """
        raw = self.values.get("system_root")
        if raw not in (None, ""):
            return _manifest_path_value(self.path, raw)
        return self.path.parent


def default_named_profile_path(name: str) -> Path:
    """
    Build the per-user profiles/<name>.json path after validating a simple stripped name.

    Empty names, dot/dot-dot, and either slash separator are rejected. Other
    characters are not sanitized, and .json is appended even when already present.
    A truthy XDG_CONFIG_HOME is expanded; otherwise use ~/.config. A relative
    XDG_CONFIG_HOME remains relative here, and nothing is created or read.

    Example:
        >>> default_named_profile_path(" research ").name
        'research.json'


    :param name: Name converted to text and stripped before simple-name validation.
    :return: Unresolved profile JSON path beneath the selected config root's liuxin/profiles directory.
    :raises ValueError: If the normalized name is empty, dot/dot-dot, or contains a path separator.
    """

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
    """
    Return the parent directory of a conventional named-profile path without creating it.

    Example:
        >>> named_profiles_directory().name
        'profiles'


    :return: The selected config root's liuxin/profiles path, possibly relative under a relative XDG_CONFIG_HOME.
    """

    return default_named_profile_path("profile").parent


def iter_named_profile_paths() -> tuple[Path, ...]:
    """
    List regular non-symlink JSON files directly within the named-profile directory, sorted by filename.

    Suffix matching is case-insensitive; sorting is case-sensitive. Returned paths
    are absolute without resolving their parent symlinks. Contents are not loaded
    or validated. A root not recognized as a directory yields an empty tuple;
    other directory-iteration failures are not caught.

    Example:
        >>> paths = iter_named_profile_paths()  # doctest: +SKIP


    :return: Sorted tuple of qualifying direct children, possibly empty; no recursive traversal occurs.
    """

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
    """
    Resolve the per-user liuxin/active-connection.json location without requiring it to exist.

    Use expanded XDG_CONFIG_HOME when truthy, otherwise ~/.config. Unlike named
    profile path construction, this makes relative roots absolute and resolves
    existing symlinks; it does not enforce confinement or create directories.

    Example:
        >>> active_connection_path().is_absolute()
        True


    :return: Resolved path used for reading, replacing, or clearing the persisted selector.
    """

    config_home = os.environ.get("XDG_CONFIG_HOME")
    root = Path(config_home).expanduser() if config_home else Path.home() / ".config"
    return (root / "liuxin" / ACTIVE_CONNECTION_NAME).resolve(strict=False)


def persisted_manifest_path() -> Path | None:
    """
    Read the active-connection pointer with a 64-KiB bound and return its absolute target path.

    Missing pointer files return None. Other read failures propagate. UTF-8 JSON
    must be an object with the exact format and an actual int version equal to
    one; booleans and numeric strings are rejected. The manifest field must be
    nonblank text and absolute after user expansion, but it is not stripped before
    path construction. The target is resolved without opening or validating it.

    Example:
        >>> selected = persisted_manifest_path()  # doctest: +SKIP


    :return: Resolved manifest target even if stale, or None only when the pointer file is absent.
    :raises ValueError: If the pointer is oversized, malformed, unsupported, or lacks an absolute target.
    """

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
    """
    Atomically replace the active selector with a mode-0600 JSON pointer to an existing manifest file.

    The target is expanded/resolved and checked with is_file, not loaded as a
    manifest. Only its path, format, and version are persisted; manifest contents
    and embedded credentials are not copied. The pointer's parent is created with
    requested mode 0700 when needed; existing directory permissions are unchanged.

    A same-directory temporary file is written, flushed, fsynced, chmodded, and
    replaced into place before directory sync. Failure before replacement leaves
    the existing pointer intact; directory-sync failure after replacement can
    propagate after the new pointer is already installed. Temporary cleanup is
    attempted in finally, without rolling back an installed pointer.

    Example:
        >>> pointer = persist_manifest_path(manifest_path)  # doctest: +SKIP


    :param manifest: Existing file path, expanded and resolved without validating its contents.
    :return: Active-connection pointer path after replacement and successful or unavailable directory sync.
    :raises FileNotFoundError: If the resolved target is not recognized as an existing file.
    """

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
    """
    Unlink the resolved active-connection selector without deleting its referenced manifest or database.

    Missing selectors return false. Other unlink errors propagate. Directory sync
    follows a successful unlink, so a sync failure can propagate after removal;
    no reconstruction or rollback is attempted.

    Example:
        >>> removed = clear_persisted_connection()  # doctest: +SKIP


    :return: True after unlink and successful or unavailable directory sync, or false when already absent.
    """

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
    """
    Choose explicit, environment, then persisted connection selectors without loading the selected manifest.

    Nonempty explicit root/profile choices are mutually exclusive and suppress
    environment lookup. Otherwise enabled LIUXIN_SYSTEM_ROOT/LIUXIN_PROFILE values
    are considered, also exclusively, before an enabled persisted pointer. A root
    always has liuxin-system.json appended; its existence is not checked.

    A profile that is absolute, has a non-dot parent, or has a .json suffix is
    treated as a path. Other tokens become named-profile paths; even an existing
    simple relative directory name takes this named-profile route. Only after that
    choice does an existing selected directory receive the manifest filename.
    Expansion/resolution does not confine the path or validate a manifest's contents.

    Example:
        >>> selected_manifest_path(use_environment=False, use_persisted=False)
        (None, None)


    :param system_root: Optional explicit root directory; None/empty string is absent.
    :param profile: Optional explicit path or simple named selector; exclusive with a supplied root.
    :param use_environment: Whether environment selectors may supply a choice when explicit selectors are absent.
    :param use_persisted: Whether the active-connection pointer may supply a final fallback.
    :return: Resolved manifest path and source label, or (None, None) when no enabled selector supplies a path.
    :raises ValueError: If a considered selector pair conflicts or named/persisted selector validation fails.
    """

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
    """
    Load a bounded UTF-8 JSON manifest or follow absolute named-profile pointers to its final manifest.

    Each read is limited to 1 MiB. Resolved-path cycles are rejected; there is no
    separate pointer-depth limit. Pointer versions require actual int one. System
    manifest versions instead accept str/int/float values converted with int,
    including True and fractional floats truncating to one; this is not strict
    integer validation. Unknown manifest fields are retained.

    A system manifest must declare exactly one database or endpoint using
    None/empty-string tests. Endpoint syntax and reachability are not checked.
    Driver type is stripped, with falsey values defaulting to SQLite; non-None
    database_metadata must be a mapping. SQLite/APSW database paths and nonempty
    root/store/materialization/log paths resolve relative to the final manifest,
    permitting missing targets and traversal outside its directory. Other database
    selectors remain unchanged. The outer selection source survives pointer chains.

    Example:
        >>> load_system_profile(use_environment=False, use_persisted=False) is None
        True


    :param system_root: Explicit root selector forwarded to selection, exclusive with profile.
    :param profile: Explicit named/path selector, which may identify a pointer or a system manifest.
    :param use_environment: Allow environment selection only when explicit selectors are absent.
    :param use_persisted: Allow the active-connection pointer as the last selection fallback.
    :param required: Raise for no selection when true; a selected missing file always raises.
    :param _seen: Resolved paths already followed in this pointer chain, used internally for cycle detection.
    :return: Final manifest path and normalized mutable values with the outer source label, or None for optional no-selection.
    :raises ValueError: For required no-selection, cycles, oversized/invalid JSON, unsupported formats/versions, or invalid required fields.
    :raises FileNotFoundError: If a selected manifest or pointer target does not exist.
    """

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
    """
    Apply selected deployment values to parsed arguments in place, unless an explicit transport is already present.

    Truthy database/endpoint values suppress profile loading, including environment
    and persisted fallback. Both transports are rejected. Combining a transport
    with a truthy explicit profile/root is rejected unless the namespace already
    has a truthy resolved_system_manifest marker; the marker itself is not verified.

    A loaded profile replaces database, endpoint, driver type, and a shallow copy
    of connection metadata. Materialization/log paths are filled only if those
    attributes already exist and are None/empty; system/store roots and other
    manifest fields are not copied. The final manifest path is recorded. Errors
    propagate without rollback of namespace assignments already made.

    Example:
        >>> args = argparse.Namespace(core_endpoint="http://127.0.0.1:8080")
        >>> apply_system_profile(args) is None
        True


    :param args: Parsed namespace to inspect and, when loading is needed, mutate with resolved connection values.
    :return: Loaded profile, or None when an existing truthy transport bypasses profile loading.
    :raises ValueError: If selectors conflict or no profile/transport can be selected.
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
    """
    Copy manifest fields while masking selected secret-like keys and recognized URL passwords.

    Keys are stringified. Any case-insensitive password/secret/token/key substring
    masks that whole value, including benign names containing those substrings.
    Only exact database/core_endpoint string fields receive URL-password redaction;
    only a database_metadata mapping is recursively processed. Other nested values
    remain shared and untouched. Usernames, query strings, fragments, arbitrary
    nested secrets, and malformed credential URLs can remain visible: this is not
    a comprehensive sanitizer or a guarantee that the result is safe to publish.

    Example:
        >>> redacted_manifest({"password": "example", "database": "postgres://user:example@db/catalogue"})
        {'password': '<redacted>', 'database': 'postgres://user:<redacted>@db/catalogue'}


    :param values: Manifest-like mapping copied shallowly except for the explicitly recursive metadata field.
    :return: New string-key dict with the limited masks applied, leaving the input mapping unchanged.
    """

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
    """
    Expand a path-like value and resolve relative paths against a manifest's parent directory.

    Existing symlinks and parent traversals are resolved without requiring the
    destination to exist or enforcing confinement beneath the manifest directory.

    Example:
        >>> _manifest_path_value(Path("deployment/manifest.json"), "../books").is_absolute()
        True


    :param manifest: Manifest path whose parent anchors relative values.
    :param value: Path-like value accepted by os.fspath and Path; invalid types propagate errors.
    :return: Expanded, resolved absolute path, possibly outside the deployment root.
    """
    path = Path(os.fspath(value)).expanduser()
    if not path.is_absolute():
        path = manifest.parent / path
    return path.resolve(strict=False)


def _redact_url(value: str) -> str:
    """
    Replace a parsed URL's password with a literal marker when a scheme and valid authority can be inspected.

    Parsing failures, absent schemes/passwords, and invalid ports return the
    original string, potentially with credentials still present. Rebuilding keeps
    username, path, query, and fragment and uses the parsed hostname/port; it is
    not lossless URL normalization and does not restore brackets around IPv6 hosts.
    Other secret-bearing components are not scrubbed.

    Example:
        >>> _redact_url("https://reader:example@example.invalid:8443/books?q=1")
        'https://reader:<redacted>@example.invalid:8443/books?q=1'


    :param value: URL-like string to inspect without network access.
    :return: Rebuilt password-masked URL when supported, otherwise the original string unchanged.
    """
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
    """
    Attempt read-only directory opening and fsync its descriptor, ignoring only an opening OSError.

    Once opened, the descriptor is closed in finally. Sync and close failures
    propagate; this helper is not a blanket best-effort durability guarantee.

    Example:
        >>> _fsync_directory(directory)  # doctest: +SKIP


    :param path: Directory path converted to text for os.open with O_RDONLY.
    :return: None after sync/close succeed or when opening raises OSError.
    """
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
