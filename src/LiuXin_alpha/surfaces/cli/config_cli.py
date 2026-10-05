"""
Inspect deployment manifests and manage named/default manifest selectors.

Selection and manifest loading belong to system_profile: explicit selectors take
precedence over environment and persisted defaults. Validation checks syntax and
local accessibility, not database integrity or server readiness. Named profiles
and the default connection store pointers rather than copying a deployment.
Publication errors do not undo already written/removed selectors. Manifest
redaction is deliberately limited; exception messages and health receipts are
not comprehensively sanitized by these handlers.
"""

from __future__ import annotations

import argparse
import os
import stat

from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from LiuXin_alpha.surfaces.cli.common import (
    add_json_output,
    emit_bytes,
    emit_json,
    json_bytes,
    open_cli_core,
)
from LiuXin_alpha.surfaces.system_profile import (
    PROFILE_POINTER_FORMAT,
    PROFILE_POINTER_VERSION,
    SYSTEM_MANIFEST_NAME,
    active_connection_path,
    clear_persisted_connection,
    default_named_profile_path,
    iter_named_profile_paths,
    load_system_profile,
    named_profiles_directory,
    persist_manifest_path,
    persisted_manifest_path,
    redacted_manifest,
    selected_manifest_path,
)


def add_profile_selector(parser: argparse.ArgumentParser) -> None:
    """
    Declare optional, mutually exclusive system-root and profile selectors.

    Absence is allowed so later selection can use environment/persisted defaults.
    Construction does not load or validate a profile.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> add_profile_selector(parser)
        >>> parser.parse_args(["--profile", "research"]).profile
        'research'


    :param parser: Mutable command parser receiving the selector group.
    :return: None; selector arguments are added in place.
    """
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--system-root",
        help="Directory containing liuxin-system.json.",
    )
    group.add_argument(
        "--profile",
        help="Named profile, manifest path, or manifest directory.",
    )


def _selected(args: argparse.Namespace):
    """
    Require and load the effective system profile using normal selector precedence.

    Forward explicit roots/profiles, permit environment selection, and retain the
    loader's default persisted fallback. Selection, read, and format errors escape.

    Example:
        >>> resolved = _selected(parsed_config_show_args)  # doctest: +SKIP


    :param args: Namespace optionally containing system_root and profile selectors.
    :return: Loaded ResolvedSystemProfile; required selection does not return None.
    """
    return load_system_profile(
        system_root=getattr(args, "system_root", None),
        profile=getattr(args, "profile", None),
        use_environment=True,
        required=True,
    )


def cmd_config_path(args: argparse.Namespace) -> int:
    """
    Publish the selected manifest path and its current file-existence status.

    Selection does not load manifest contents or follow a named pointer to its
    final target. Existence is checked separately for output and exit status,
    so a concurrent filesystem change can make those observations differ.

    Example:
        >>> cmd_config_path(parsed_config_path_args)  # doctest: +SKIP


    :param args: Optional system_root/profile selectors and JSON output controls.
    :return: Zero if the path is a file at the final check, otherwise one.
    :raises ValueError: No selector supplies a manifest, or selection conflicts.
    """
    path, source = selected_manifest_path(
        system_root=getattr(args, "system_root", None),
        profile=getattr(args, "profile", None),
        use_environment=True,
    )
    if path is None:
        raise ValueError(
            "No LiuXin system selected; use --system-root/--profile or set "
            "LIUXIN_SYSTEM_ROOT/LIUXIN_PROFILE, or run `liuxin connect`."
        )
    emit_json({"path": str(path), "source": source, "exists": path.is_file()}, args)
    return 0 if path.is_file() else 1


def cmd_config_show(args: argparse.Namespace) -> int:
    """
    Publish the loaded manifest, normalized root, and selection provenance.

    Apply system_profile's limited manifest redaction, not the recursive diagnostic
    sanitizer. Unknown nested secrets, path details, or URL components may remain;
    this is not a guarantee that arbitrary manifests are safe to share.

    Example:
        >>> cmd_config_show(parsed_config_show_args)  # doctest: +SKIP


    :param args: Optional root/profile selection and JSON output controls.
    :return: Zero after publishing the resolved manifest projection.
    """
    resolved = _selected(args)
    assert resolved is not None
    emit_json(
        {
            "path": str(resolved.path),
            "source": resolved.source,
            "system_root": str(resolved.system_root),
            "manifest": redacted_manifest(resolved.values),
        },
        args,
    )
    return 0


def _check(
    checks: list[dict[str, Any]],
    name: str,
    ok: bool,
    message: str,
    *,
    severity: str = "error",
) -> None:
    """
    Append one validation observation with a truthiness-coerced success flag.

    Names, messages, and severity tokens are stored without validation/redaction.

    Example:
        >>> checks = []
        >>> _check(checks, "permissions", False, "Prefer 0600", severity="warning")
        >>> checks[0]["severity"]
        'warning'


    :param checks: Mutable report list receiving the new observation.
    :param name: Machine-readable check identifier.
    :param ok: Value coerced to the recorded boolean status.
    :param message: Human-readable check detail, retained verbatim.
    :param severity: Severity token; aggregate failure recognizes exact 'error'.
    :return: None; checks is extended by one dictionary.
    """
    checks.append(
        {
            "name": name,
            "ok": bool(ok),
            "severity": severity,
            "message": message,
        }
    )


def validate_profile(args: argparse.Namespace) -> dict[str, Any]:
    """
    Check manifest selection, target syntax, permissions, and local path access.

    Selection/loading Exceptions produce a failed manifest report with path None
    and raw exception text. Later errors are not all converted to observations:
    malformed URL parsing or other unexpected validation failures can propagate.
    Manifest stat failures and group/other permission bits are warnings, not blockers.

    Endpoint targets require an HTTP(S) scheme and nonempty authority only; they
    are not contacted. SQLite/APSW targets require an existing file and readable/
    traversable parent, not a schema or integrity check. Other databases require
    only a truthy target. Configured Store/materialization/log directories must
    be readable, writable, and traversable; absent optional directories are info.
    Access checks are observations, not reservations against later changes.

    Manifest values receive limited redaction; raw errors and all arbitrary nested
    values are not guaranteed secret-free. No Core session is opened.

    Example:
        >>> report = validate_profile(parsed_config_validate_args)  # doctest: +SKIP


    :param args: Optional system_root/profile selectors with normal fallback policy.
    :return: Report with ok/path/checks, plus source/redacted manifest after loading;
        ok means no failed error-severity check, not complete deployment readiness.
    """

    checks: list[dict[str, Any]] = []
    try:
        resolved = _selected(args)
    except Exception as error:
        _check(checks, "manifest", False, str(error))
        return {"ok": False, "path": None, "checks": checks}
    assert resolved is not None
    _check(checks, "manifest", True, "Manifest format and version are supported.")
    try:
        mode = stat.S_IMODE(resolved.path.stat().st_mode)
    except OSError as error:
        _check(checks, "manifest_permissions", False, str(error), severity="warning")
    else:
        private = mode & 0o077 == 0
        _check(
            checks,
            "manifest_permissions",
            private,
            "Manifest mode is {:04o}{}".format(
                mode,
                "; prefer 0600" if not private else ".",
            ),
            severity="warning",
        )

    values = resolved.values
    endpoint = values.get("core_endpoint")
    database = values.get("database")
    db_type = str(values.get("db_type") or "SQLite").casefold()
    if endpoint:
        parsed = urlsplit(str(endpoint))
        valid = parsed.scheme in {"http", "https"} and bool(parsed.netloc)
        _check(
            checks,
            "core_endpoint",
            valid,
            "Core endpoint syntax is valid." if valid else "Core endpoint must be an HTTP(S) URL.",
        )
    elif database and db_type in {"sqlite", "apsw"}:
        path = Path(str(database))
        _check(
            checks,
            "database",
            path.is_file(),
            "Catalogue exists: {!s}.".format(path)
            if path.is_file()
            else "Catalogue does not exist: {!s}.".format(path),
        )
        _check(
            checks,
            "database_parent",
            path.parent.is_dir() and os.access(path.parent, os.R_OK | os.X_OK),
            "Catalogue parent is accessible: {!s}.".format(path.parent),
        )
    else:
        _check(
            checks,
            "database_target",
            bool(database),
            "Database target is configured for {}.".format(values.get("db_type")),
        )

    for name in ("store_root", "materialization_root", "log_directory"):
        raw = values.get(name)
        if raw in (None, ""):
            _check(
                checks,
                name,
                True,
                "{} is not configured.".format(name),
                severity="info",
            )
            continue
        path = Path(str(raw))
        accessible = path.is_dir() and os.access(path, os.R_OK | os.W_OK | os.X_OK)
        _check(
            checks,
            name,
            accessible,
            "Directory is accessible: {!s}.".format(path)
            if accessible
            else "Directory is missing or not readable/writable: {!s}.".format(path),
        )
    ok = not any(
        not item["ok"] and item["severity"] == "error" for item in checks
    )
    return {
        "ok": ok,
        "path": str(resolved.path),
        "source": resolved.source,
        "manifest": redacted_manifest(values),
        "checks": checks,
    }


def cmd_config_validate(args: argparse.Namespace) -> int:
    """
    Publish the profile validation report and use its aggregate status as exit code.

    Unexpected validation or output errors propagate rather than becoming codes.

    Example:
        >>> cmd_config_validate(parsed_config_validate_args)  # doctest: +SKIP


    :param args: Optional root/profile selectors and JSON publication controls.
    :return: Zero for an ok report, otherwise one, after publication.
    """
    result = validate_profile(args)
    emit_json(result, args)
    return 0 if result["ok"] else 1


def cmd_config_profiles_list(args: argparse.Namespace) -> int:
    """
    Inspect discovered named selectors, retaining per-profile load failures.

    Load each discovered path without environment/persisted fallback. Successful
    loading marks valid but does not check target accessibility or Core health.
    Individual errors retain unredacted text; discovery/output errors propagate.
    The list follows the profile-directory helper's filename order.

    Example:
        >>> cmd_config_profiles_list(parsed_profiles_list_args)  # doctest: +SKIP


    :param args: JSON output controls; profile-directory selection is environment-based.
    :return: Zero after publishing directory/profiles/count, even for invalid entries.
    """
    profiles: list[dict[str, Any]] = []
    for path in iter_named_profile_paths():
        value: dict[str, Any] = {
            "name": path.stem,
            "path": str(path),
            "valid": False,
        }
        try:
            resolved = load_system_profile(
                profile=str(path),
                use_environment=False,
                use_persisted=False,
                required=True,
            )
        except Exception as error:
            value["error"] = str(error) or type(error).__name__
        else:
            assert resolved is not None
            value.update(
                {
                    "valid": True,
                    "manifest": str(resolved.path),
                    "system_root": str(resolved.system_root),
                    "db_type": str(resolved.values.get("db_type") or "SQLite"),
                    "transport": (
                        "core_endpoint"
                        if resolved.values.get("core_endpoint")
                        else "database"
                    ),
                }
            )
        profiles.append(value)
    emit_json(
        {
            "directory": str(named_profiles_directory()),
            "profiles": profiles,
            "count": len(profiles),
        },
        args,
    )
    return 0


def cmd_config_profiles_add(args: argparse.Namespace) -> int:
    """
    Persist a named mode-0600 pointer to a structurally loaded manifest.

    Expand the target and append the manifest filename for an existing directory,
    then load without environment/persisted fallback. A simple non-path token can
    still resolve as a named profile via the loader. New profile directories request
    mode 0700 without tightening existing parents. The pointer contains the final
    resolved manifest path, not database credentials or a copy of its contents.

    Shared byte publication honors the replace flag. Final report failure leaves
    the selector in place; neither target readiness nor Core health is checked.

    Example:
        >>> cmd_config_profiles_add(parsed_profiles_add_args)  # doctest: +SKIP


    :param args: name, target, replace collision policy, and JSON output controls.
    :return: Zero after writing the pointer and publishing its creation receipt.
    """
    candidate = Path(args.target).expanduser()
    if candidate.is_dir():
        candidate = candidate / SYSTEM_MANIFEST_NAME
    resolved = load_system_profile(
        profile=str(candidate),
        use_environment=False,
        use_persisted=False,
        required=True,
    )
    assert resolved is not None
    destination = default_named_profile_path(args.name)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    emit_bytes(
        json_bytes(
            {
                "format": PROFILE_POINTER_FORMAT,
                "version": PROFILE_POINTER_VERSION,
                "manifest": str(resolved.path.resolve(strict=False)),
            }
        ),
        output=destination,
        replace=bool(args.replace),
        mode=0o600,
    )
    emit_json(
        {
            "created": True,
            "name": args.name,
            "profile": str(destination),
            "manifest": str(resolved.path),
            "system_root": str(resolved.system_root),
        },
        args,
    )
    return 0


def cmd_config_profiles_remove(args: argparse.Namespace) -> int:
    """
    Unlink a confirmed named selector without changing its referenced deployment.

    Missing selectors produce removed=False; other unlink/output errors propagate.
    A later report failure does not restore a successfully removed pointer.

    Example:
        >>> cmd_config_profiles_remove(argparse.Namespace(yes=False))
        Traceback (most recent call last):
        ...
        ValueError: Named profile removal requires --yes.


    :param args: Simple profile name, yes confirmation, and JSON output controls.
    :return: Zero if the pointer was removed, one if it was absent.
    :raises ValueError: Confirmation is absent or the simple profile name is invalid.
    """
    if not args.yes:
        raise ValueError("Named profile removal requires --yes.")
    path = default_named_profile_path(args.name)
    try:
        path.unlink()
    except FileNotFoundError:
        removed = False
    else:
        removed = True
    emit_json(
        {
            "removed": removed,
            "name": args.name,
            "profile": str(path),
            "systems_modified": False,
        },
        args,
    )
    return 0 if removed else 1


def cmd_connect(args: argparse.Namespace) -> int:
    """
    Inspect the default connection or persist a newly selected manifest pointer.

    Exact positional 'status', or no explicit selector, takes the inspection path.
    Status conflicts with option selectors; other positional roots cannot be mixed
    with them either. Status reports both persisted and environment-effective paths,
    but its exit code reflects only the persisted manifest's file existence, not
    effective selection validity, manifest contents, or Core health.

    For a new choice, load with environment/persisted fallback disabled. Unless
    no_health_check is true, query health in a storage-enabled session; a returned
    failure flag is not interpreted. Only after session exit persist the pointer.
    A truthy selector environment variable reports effective_now=False even if it
    selects the same manifest. Output failure leaves the new pointer installed.
    The target is partially redacted, but the health receipt is published unchanged.

    Example:
        >>> cmd_connect(parsed_connect_status_args)  # doctest: +SKIP


    :param args: Optional connection_system_root/system_root/profile, no_health_check
        for new choices, and JSON output controls.
    :return: For status, zero if the persisted manifest is a file, else one;
        for a new selection, zero after persistence and output.
    :raises ValueError: Explicit selection forms conflict or profile loading fails.
    """

    positional_root = getattr(args, "connection_system_root", None)
    option_root = getattr(args, "system_root", None)
    profile = getattr(args, "profile", None)
    status_requested = positional_root == "status"
    if status_requested and (option_root or profile):
        raise ValueError(
            "`liuxin connect status` cannot be combined with a system/profile "
            "selector."
        )
    if positional_root and (option_root or profile):
        raise ValueError(
            "Provide the system root positionally, with --system-root, or with "
            "--profile; do not combine them."
        )
    system_root = (None if status_requested else positional_root) or option_root
    if status_requested or (not system_root and not profile):
        persisted = persisted_manifest_path()
        effective, source = selected_manifest_path(
            use_environment=True,
            use_persisted=True,
        )
        persisted_exists = bool(persisted is not None and persisted.is_file())
        emit_json(
            {
                "connected": persisted_exists,
                "persisted": persisted is not None,
                "connection_file": str(active_connection_path()),
                "persisted_manifest": (
                    None if persisted is None else str(persisted)
                ),
                "persisted_manifest_exists": persisted_exists,
                "effective_manifest": None if effective is None else str(effective),
                "effective_source": source,
            },
            args,
        )
        return 0 if persisted_exists else 1
    resolved = load_system_profile(
        system_root=system_root,
        profile=profile,
        use_environment=False,
        use_persisted=False,
        required=True,
    )
    assert resolved is not None
    health: Any = None
    if not bool(args.no_health_check):
        probe_args = argparse.Namespace(
            database=None,
            core_endpoint=None,
            db_type="SQLite",
            system_root=None,
            profile=str(resolved.path),
        )
        with open_cli_core(probe_args, enable_storage_manager=True) as core:
            health = core.query("health", {})
    connection_file = persist_manifest_path(resolved.path)
    environment_override = bool(
        os.environ.get("LIUXIN_SYSTEM_ROOT") or os.environ.get("LIUXIN_PROFILE")
    )
    emit_json(
        {
            "connected": True,
            "connection_file": str(connection_file),
            "manifest": str(resolved.path),
            "system_root": str(resolved.system_root),
            "target": redacted_manifest(resolved.values),
            "health": health,
            "effective_now": not environment_override,
            "warning": (
                "LIUXIN_SYSTEM_ROOT or LIUXIN_PROFILE currently overrides the "
                "persisted connection in this environment."
                if environment_override
                else None
            ),
        },
        args,
    )
    return 0


def cmd_disconnect(args: argparse.Namespace) -> int:
    """
    Remove only the persisted connection pointer and report remaining env selection.

    The manifest/database and environment remain untouched. Missing pointers are
    not errors, and output failure does not undo a completed removal.

    Example:
        >>> cmd_disconnect(parsed_disconnect_args)  # doctest: +SKIP


    :param args: JSON publication controls; pointer location is selected by its helper.
    :return: Zero after output, whether or not a pointer previously existed.
    """

    connection_file = active_connection_path()
    disconnected = clear_persisted_connection()
    environment_override = bool(
        os.environ.get("LIUXIN_SYSTEM_ROOT") or os.environ.get("LIUXIN_PROFILE")
    )
    emit_json(
        {
            "disconnected": disconnected,
            "connection_file": str(connection_file),
            "systems_modified": False,
            "environment_selection_remains": environment_override,
        },
        args,
    )
    return 0


def build_connection_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register connect status/selection and disconnect without touching persisted state.

    Positional-selector conflicts are handler checks, not argparse exclusivity.
    A new connection checks health by default; disconnect has no selector argument.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> build_connection_parsers(parser.add_subparsers())
        >>> parser.parse_args(["connect", "status"]).connection_system_root
        'status'


    :param subparsers: Root CLI collection receiving connect and disconnect leaves.
    :return: None; arguments and handlers are registered in place.
    """
    connect = subparsers.add_parser(
        "connect",
        help="Persist or inspect the default LiuXin system for later commands.",
    )
    connect.add_argument(
        "connection_system_root",
        nargs="?",
        metavar="SYSTEM_ROOT|status",
        help=(
            "System root containing liuxin-system.json, or `status` to inspect "
            "persisted/effective selection; omission remains a status alias."
        ),
    )
    add_profile_selector(connect)
    connect.add_argument(
        "--no-health-check",
        action="store_true",
        help="Persist a structurally valid profile without opening Core.",
    )
    add_json_output(connect)
    connect.set_defaults(handler=cmd_connect)

    disconnect = subparsers.add_parser(
        "disconnect",
        help="Clear the persisted selection without changing any database.",
    )
    add_json_output(disconnect)
    disconnect.set_defaults(handler=cmd_disconnect)


def build_config_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register manifest inspection and nested named-profile management commands.

    Path/show/validate accept optional selection; profile list/add/remove use the
    per-user profile directory. Replacement/removal flags are passed to handlers,
    with no loading, directory creation, or persistence during construction.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> build_config_parser(parser.add_subparsers())
        >>> args = parser.parse_args(["config", "profiles", "add", "books", "/srv/books"])
        >>> args.name, args.replace
        ('books', False)


    :param subparsers: Root CLI collection receiving the required config action tree.
    :return: None; inspection and named-selector leaves are added in place.
    """
    parser = subparsers.add_parser(
        "config",
        help="Inspect or validate a LiuXin system manifest/profile.",
    )
    commands = parser.add_subparsers(dest="config_command", required=True)
    for name, handler, help_text in (
        ("path", cmd_config_path, "Show which manifest is selected."),
        ("show", cmd_config_show, "Show the selected manifest with secrets redacted."),
        ("validate", cmd_config_validate, "Validate paths and deployment configuration."),
    ):
        command = commands.add_parser(name, help=help_text)
        add_profile_selector(command)
        add_json_output(command)
        command.set_defaults(handler=handler)

    profiles = commands.add_parser(
        "profiles", help="List or manage named deployment selectors."
    )
    profile_commands = profiles.add_subparsers(
        dest="config_profiles_command", required=True
    )
    profile_list = profile_commands.add_parser("list")
    add_json_output(profile_list)
    profile_list.set_defaults(handler=cmd_config_profiles_list)
    profile_add = profile_commands.add_parser("add")
    profile_add.add_argument("name", help="Simple name used by --profile.")
    profile_add.add_argument(
        "target", help="Existing system root, manifest, or named-profile pointer."
    )
    profile_add.add_argument(
        "--replace", action="store_true", help="Replace an existing named selector."
    )
    add_json_output(profile_add)
    profile_add.set_defaults(handler=cmd_config_profiles_add)
    profile_remove = profile_commands.add_parser("remove")
    profile_remove.add_argument("name")
    profile_remove.add_argument("--yes", action="store_true")
    add_json_output(profile_remove)
    profile_remove.set_defaults(handler=cmd_config_profiles_remove)


__all__ = [
    "add_profile_selector",
    "build_config_parser",
    "build_connection_parsers",
    "cmd_connect",
    "cmd_config_path",
    "cmd_config_show",
    "cmd_config_validate",
    "cmd_config_profiles_add",
    "cmd_config_profiles_list",
    "cmd_config_profiles_remove",
    "cmd_disconnect",
    "validate_profile",
]
