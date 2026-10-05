"""
Collect deployment readiness, operator status, and redacted support bundles.

Quick checks query Core; full checks also refresh storage discovery and invoke
Store probe commands. Successful calls count as successful checks even when a
receipt reports an unhealthy condition. Report collection returns raw values;
only the CLI publication handlers apply heuristic credential redaction, which
is not a guarantee that arbitrary secrets or personal data have been removed.
"""

from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import sys

from pathlib import Path
from typing import Any

from LiuXin_alpha.constants import __version__
from LiuXin_alpha.surfaces.cli.common import (
    add_connection_arguments,
    add_json_output,
    emit_json,
    open_cli_core,
)
from LiuXin_alpha.surfaces.cli.config_cli import validate_profile


_OPTIONAL_EXECUTABLES = (
    ("unsquashfs", "Read SquashFS archives."),
    ("mksquashfs", "Build SquashFS backup packs."),
    ("7z", "Read 7z and additional archive variants."),
    ("unrar", "Read compressed RAR members when no Python backend can."),
    ("rclone", "Register and read rclone-backed sources."),
    ("wget", "Register wget-backed HTML sources."),
)

_URL_CREDENTIALS = re.compile(
    r"\b([a-z][a-z0-9+.-]*://)([^:/@\s]+):([^@\s]+)@",
    flags=re.IGNORECASE,
)
_AUTHORIZATION_VALUE = re.compile(
    r"\b(authorization\s*[:=]\s*(?:bearer|basic)\s+)[^\s,;]+",
    flags=re.IGNORECASE,
)
_SECRET_ASSIGNMENT = re.compile(
    r"\b(password|passwd|secret|token|api[_-]?key|access[_-]?key)"
    r"(\s*[:=]\s*)[^\s,;}\]]+",
    flags=re.IGNORECASE,
)
_SECRET_KEYS = {
    "api_key",
    "access_key",
    "authorization",
    "credential",
    "credentials",
    "password",
    "passwd",
    "private_key",
    "secret",
    "token",
}


def _redact_diagnostic_value(value: Any, *, key: str | None = None) -> Any:
    """
    Copy supported containers while masking known credential keys and patterns.

    Key matching strips/casefolds and replaces hyphens with underscores; known
    names and selected credential suffixes hide the entire associated value.
    Dict keys become strings, tuples become lists, and other non-string objects
    remain unchanged. URL passwords are masked but usernames remain. String
    substitutions cover common authorization and assignment forms, not arbitrary
    secret encodings. Cycles are not detected and stringified keys may collide.

    Example:
        >>> _redact_diagnostic_value({"api-key": "hidden", "items": (1, 2)})
        {'api-key': '<redacted>', 'items': [1, 2]}
        >>> _redact_diagnostic_value("https://reader:password@example.test/x")
        'https://reader:<redacted>@example.test/x'


    :param value: Diagnostic value to recursively inspect without mutating it.
    :param key: Optional containing field name used to mask a whole value first.
    :return: Redacted container/string projection, or the original other value.
    """

    if key is not None:
        token = key.strip().casefold().replace("-", "_")
        if token in _SECRET_KEYS or token.endswith(
            ("_password", "_secret", "_token", "_credential")
        ):
            return "<redacted>"
    if isinstance(value, dict):
        return {
            str(item_key): _redact_diagnostic_value(item, key=str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact_diagnostic_value(item) for item in value]
    if isinstance(value, str):
        rendered = _URL_CREDENTIALS.sub(r"\1\2:<redacted>@", value)
        rendered = _AUTHORIZATION_VALUE.sub(r"\1<redacted>", rendered)
        return _SECRET_ASSIGNMENT.sub(r"\1\2<redacted>", rendered)
    return value


def _record(
    checks: list[dict[str, Any]],
    name: str,
    ok: bool,
    message: str,
    *,
    severity: str = "error",
    details: Any = None,
) -> None:
    """
    Append one readiness observation, retaining optional details by reference.

    Only ok is coerced; severity and message are not validated or redacted.

    Example:
        >>> checks = []
        >>> _record(checks, "tools", True, "Inventory complete", severity="info")
        >>> checks[0]["ok"], "details" in checks[0]
        (True, False)


    :param checks: Mutable observation list receiving the new dictionary.
    :param name: Stable check identifier used by report consumers.
    :param ok: Value whose truthiness becomes the recorded success boolean.
    :param message: Human-readable description, retained as supplied.
    :param severity: Severity token; aggregate failure recognizes exact 'error'.
    :param details: Additional report data, omitted only when None.
    :return: None; checks gains one observation.
    """
    value: dict[str, Any] = {
        "name": name,
        "ok": bool(ok),
        "severity": severity,
        "message": message,
    }
    if details is not None:
        value["details"] = details
    checks.append(value)


def _configuration_check(args: argparse.Namespace, checks: list[dict[str, Any]]) -> None:
    """
    Record selection readiness before attempting to open Core.

    With neither explicit database nor endpoint, delegate to profile validation.
    A database takes precedence over an endpoint; SQLite/APSW requires an existing
    file, whereas other drivers and endpoints are merely recorded as configured.
    This is not connectivity, schema, credential, or database integrity validation.
    Profile/path errors propagate rather than becoming failure observations.

    Example:
        >>> checks = []
        >>> _configuration_check(argparse.Namespace(core_endpoint="http://localhost:9"), checks)
        >>> checks[0]["ok"]
        True


    :param args: Explicit connection selectors or system-root/profile settings.
    :param checks: Mutable list receiving the configuration observation.
    :return: None; a configuration result is appended unless validation raises.
    """
    database = getattr(args, "database", None)
    endpoint = getattr(args, "core_endpoint", None)
    if not database and not endpoint:
        report = validate_profile(args)
        _record(
            checks,
            "configuration",
            bool(report.get("ok")),
            "System manifest is valid."
            if report.get("ok")
            else "System manifest validation failed.",
            details=report,
        )
        return
    if database:
        db_type = str(getattr(args, "db_type", "SQLite")).casefold()
        if db_type in {"sqlite", "apsw"}:
            path = Path(str(database)).expanduser().resolve(strict=False)
            _record(
                checks,
                "configuration",
                path.is_file(),
                "Local catalogue exists: {!s}.".format(path)
                if path.is_file()
                else "Local catalogue does not exist: {!s}.".format(path),
            )
        else:
            _record(checks, "configuration", True, "Database target is configured.")
    elif endpoint:
        _record(checks, "configuration", True, "Remote Core endpoint is configured.")
    else:
        _record(
            checks,
            "configuration",
            False,
            "No database, Core endpoint, system root, or profile is selected.",
        )


def collect_doctor_report(args: argparse.Namespace) -> dict[str, Any]:
    """
    Aggregate configuration and Core observations into an unredacted report.

    A configuration error prevents opening Core. Full mode inventories optional
    executables by PATH lookup only, refreshes store discovery, and commands a
    probe for each dict-shaped Store. Missing tools are informational. Health,
    database, and storage query exceptions are errors; other query exceptions
    are warnings. Returned payload flags and failed-job counts are not interpreted
    as failed checks. Empty Store probe lists therefore count as successful.

    Continue after individual probe Exceptions. Session or processing Exceptions
    within the Core block become a core_open error, retaining earlier observations;
    this label can describe an exit/shape failure, not just failure to connect.
    Configuration/tool-inventory errors occur outside that catch and propagate.
    Full mode can refresh state and contact backends; it is not a read-only promise.

    Example:
        >>> report = collect_doctor_report(parsed_doctor_args)  # doctest: +SKIP
        >>> report["mode"]  # doctest: +SKIP
        'quick'


    :param args: Connection/profile settings and optional full-mode boolean.
    :return: Raw ok, mode, checks, and sections; ok means no failed error-severity
        observation, not that every receipt declares the deployment healthy.
    """

    checks: list[dict[str, Any]] = []
    sections: dict[str, Any] = {}
    _configuration_check(args, checks)
    if bool(getattr(args, "full", False)):
        dependencies: list[dict[str, Any]] = []
        for executable, purpose in _OPTIONAL_EXECUTABLES:
            found = shutil.which(executable)
            dependencies.append(
                {
                    "executable": executable,
                    "available": found is not None,
                    "path": found,
                    "purpose": purpose,
                }
            )
        sections["executables"] = dependencies
        _record(
            checks,
            "optional_executables",
            True,
            "Optional executable availability was inventoried.",
            severity="info",
            details=dependencies,
        )

    if any(
        not item["ok"] and item["severity"] == "error" for item in checks
    ):
        return {
            "ok": False,
            "mode": "full" if bool(getattr(args, "full", False)) else "quick",
            "checks": checks,
            "sections": sections,
        }

    try:
        with open_cli_core(args, enable_storage_manager=True) as core:
            probes = (
                ("core_health", "query", "health", {}),
                ("database", "query", "database.info", {}),
                (
                    "storage",
                    "query",
                    "storage.stores.list",
                    {"refresh": bool(getattr(args, "full", False))},
                ),
                ("storage_status", "query", "storage.status", {}),
                ("capabilities", "query", "capabilities.list", {}),
                (
                    "failed_jobs",
                    "query",
                    "jobs.list",
                    {"states": ["failed", "aborted", "timed_out"], "limit": 20},
                ),
            )
            for section, mode, operation, payload in probes:
                try:
                    method = core.query if mode == "query" else core.command
                    sections[section] = method(operation, payload)
                except Exception as error:
                    required = section in {"core_health", "database", "storage"}
                    _record(
                        checks,
                        section,
                        False,
                        "{} failed: {}".format(operation, str(error) or type(error).__name__),
                        severity="error" if required else "warning",
                    )
                else:
                    _record(checks, section, True, "{} succeeded.".format(operation))

            stores = sections.get("storage", {}).get("stores", [])
            if bool(getattr(args, "full", False)) and isinstance(stores, list):
                store_probes: list[dict[str, Any]] = []
                for store in stores:
                    if not isinstance(store, dict):
                        continue
                    reference = store.get("store_uuid") or store.get("store_name")
                    try:
                        result = core.command("storage.store.probe", {"store": reference})
                    except Exception as error:
                        store_probes.append(
                            {"store": reference, "ok": False, "error": str(error)}
                        )
                    else:
                        store_probes.append({"store": reference, "ok": True, "result": result})
                sections["store_probes"] = store_probes
                _record(
                    checks,
                    "store_probes",
                    all(item["ok"] for item in store_probes),
                    "Every configured Store probe completed."
                    if all(item["ok"] for item in store_probes)
                    else "One or more configured Stores could not be probed.",
                )
    except Exception as error:
        _record(
            checks,
            "core_open",
            False,
            "Could not open Core: {}".format(str(error) or type(error).__name__),
        )

    ok = not any(
        not item["ok"] and item["severity"] == "error" for item in checks
    )
    return {
        "ok": ok,
        "mode": "full" if bool(getattr(args, "full", False)) else "quick",
        "checks": checks,
        "sections": sections,
    }


def cmd_doctor(args: argparse.Namespace) -> int:
    """
    Publish a credential-filtered readiness report and project its check status.

    Collection and output exceptions propagate; redaction is heuristic and full
    collection may refresh/probe Stores as described by collect_doctor_report.

    Example:
        >>> cmd_doctor(parsed_doctor_args)  # doctest: +SKIP


    :param args: Connection selectors, full-mode choice, and JSON output options.
    :return: Zero for a report with ok true, otherwise one, after publication.
    """
    report = collect_doctor_report(args)
    emit_json(_redact_diagnostic_value(report), args)
    return 0 if report["ok"] else 1


def cmd_status(args: argparse.Namespace) -> int:
    """
    Publish a compact, redacted dashboard derived from readiness observations.

    Include Core presence, database facts, Store/issue counts, failed-job total,
    and messages for every failed check, including warnings. Missing or unexpected
    component shapes generally yield None counts; Core availability is receipt
    truthiness, not an independent liveness test. Full mode also embeds the raw
    report before redaction and inherits its refresh/probe effects. Overall status
    comes from check severity, not the projected storage healthy flag.

    Example:
        >>> cmd_status(parsed_status_args)  # doctest: +SKIP


    :param args: Connection selectors, full-mode choice, and JSON output controls.
    :return: Zero for a successful readiness report, otherwise one, after output.
    """

    report = collect_doctor_report(args)
    sections = report.get("sections", {})
    core = sections.get("core_health", {})
    database = sections.get("database", {})
    stores = sections.get("storage", {})
    storage_status = sections.get("storage_status", {})
    jobs = sections.get("failed_jobs", {})
    store_values = stores.get("stores", []) if isinstance(stores, dict) else []
    issue_values = (
        storage_status.get("status", {}).get("issues", [])
        if isinstance(storage_status, dict)
        and isinstance(storage_status.get("status"), dict)
        else []
    )
    problems = [
        str(check.get("message") or check.get("name") or "unknown issue")
        for check in report.get("checks", [])
        if isinstance(check, dict) and not bool(check.get("ok"))
    ]
    result: dict[str, Any] = {
        "ok": bool(report.get("ok")),
        "mode": report.get("mode", "quick"),
        "core": {
            "available": bool(core),
            "shutdown": core.get("shutdown") if isinstance(core, dict) else None,
        },
        "database": {
            "type": database.get("type") if isinstance(database, dict) else None,
            "exists": database.get("exists") if isinstance(database, dict) else None,
        },
        "storage": {
            "healthy": (
                storage_status.get("healthy")
                if isinstance(storage_status, dict)
                else None
            ),
            "stores": len(store_values) if isinstance(store_values, list) else None,
            "issues": len(issue_values) if isinstance(issue_values, list) else None,
        },
        "jobs": {
            "failed": jobs.get("total") if isinstance(jobs, dict) else None,
        },
        "problems": problems,
    }
    if bool(getattr(args, "full", False)):
        result["details"] = report
    emit_json(_redact_diagnostic_value(result), args)
    return 0 if bool(report.get("ok")) else 1


def cmd_diagnostics_collect(args: argparse.Namespace) -> int:
    """
    Publish a versioned support bundle with readiness and bounded job-log reads.

    Unless no_job_logs is true, open a second, storage-disabled session and inspect
    only the first five failed-job list entries. Non-dicts and entries without a
    truthy job_id/id are skipped, not replaced from later entries. Each read asks
    for at most 16,384 bytes at offset zero: despite the output key's 'tails' name,
    these are initial chunks, not end-of-log reads. Query/session Exceptions become
    log error records and do not change the readiness-based exit status.

    Include interpreter/platform facts and only booleans for selector environment
    variables, then heuristically redact the complete bundle. Paths, usernames,
    unknown secret patterns, and arbitrary application data may remain; review
    the bundle before sharing it. Collection/serialization/output failures propagate.

    Example:
        >>> cmd_diagnostics_collect(parsed_support_bundle_args)  # doctest: +SKIP


    :param args: Connection and output controls plus full and no_job_logs flags.
    :return: Zero if the doctor report is ok, otherwise one, even if log reads fail.
    """
    report = collect_doctor_report(args)
    failed_job_logs: list[dict[str, Any]] = []
    jobs = report.get("sections", {}).get("failed_jobs", {})
    job_values = jobs.get("jobs", []) if isinstance(jobs, dict) else []
    if not bool(args.no_job_logs) and isinstance(job_values, list):
        try:
            with open_cli_core(args, enable_storage_manager=False) as core:
                for job in job_values[:5]:
                    if not isinstance(job, dict):
                        continue
                    job_id = job.get("job_id") or job.get("id")
                    if not job_id:
                        continue
                    try:
                        log = core.query(
                            "jobs.log.read",
                            {"job_id": str(job_id), "offset": 0, "max_bytes": 16384},
                        )
                    except Exception as error:
                        failed_job_logs.append(
                            {"job_id": str(job_id), "error": str(error)}
                        )
                    else:
                        failed_job_logs.append({"job_id": str(job_id), "log": log})
        except Exception as error:
            failed_job_logs.append({"error": str(error), "available": False})
    bundle = {
        "format": "liuxin.diagnostics",
        "version": 1,
        "liuxin_version": __version__,
        "python": {
            "version": sys.version,
            "executable": sys.executable,
            "platform": platform.platform(),
        },
        "selection_environment": {
            "LIUXIN_SYSTEM_ROOT": bool(os.environ.get("LIUXIN_SYSTEM_ROOT")),
            "LIUXIN_PROFILE": bool(os.environ.get("LIUXIN_PROFILE")),
        },
        "doctor": report,
        "failed_job_log_tails": failed_job_logs,
        "redaction": (
            "Known credential fields, URL passwords, authorization values, and "
            "credential assignments are redacted; environment selector values are omitted."
        ),
    }
    emit_json(_redact_diagnostic_value(bundle), args)
    return 0 if report["ok"] else 1


def build_diagnostics_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register doctor, status, and diagnostics collect with shared output controls.

    Full mode is opt-in on each leaf; support collection includes job logs unless
    explicitly disabled. Construction only declares arguments and handlers.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_diagnostics_parsers(root.add_subparsers())
        >>> args = root.parse_args(["diagnostics", "collect", "--no-job-logs"])
        >>> args.no_job_logs, args.full
        (True, False)


    :param subparsers: Root CLI collection receiving all three command families.
    :return: None; parser declarations are added without collecting diagnostics.
    """
    doctor = subparsers.add_parser(
        "doctor",
        help="Check whether the selected LiuXin system is ready to operate.",
    )
    add_connection_arguments(doctor)
    doctor.add_argument(
        "--full",
        action="store_true",
        help="Refresh and probe every Store and inventory optional executables.",
    )
    add_json_output(doctor)
    doctor.set_defaults(handler=cmd_doctor)

    status = subparsers.add_parser(
        "status",
        help="Show the selected system's concise operational status.",
    )
    add_connection_arguments(status)
    status.add_argument(
        "--full",
        action="store_true",
        help="Refresh and probe every Store and inventory optional executables.",
    )
    add_json_output(status)
    status.set_defaults(handler=cmd_status)

    diagnostics = subparsers.add_parser(
        "diagnostics",
        help="Collect a redacted operational support bundle.",
    )
    commands = diagnostics.add_subparsers(dest="diagnostics_command", required=True)
    collect = commands.add_parser("collect")
    add_connection_arguments(collect)
    collect.add_argument("--full", action="store_true")
    collect.add_argument("--no-job-logs", action="store_true")
    add_json_output(collect)
    collect.set_defaults(handler=cmd_diagnostics_collect)


__all__ = [
    "build_diagnostics_parsers",
    "cmd_diagnostics_collect",
    "cmd_doctor",
    "cmd_status",
    "collect_doctor_report",
]
