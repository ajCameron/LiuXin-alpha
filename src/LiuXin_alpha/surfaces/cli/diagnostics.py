"""Read-only readiness checks and redacted diagnostic collection."""

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
    """Remove common credentials while retaining useful diagnostic shape."""

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
    """Probe deployment readiness without aborting after the first failure."""

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
    Execute the `doctor` CLI command.


    :param args:
    :return:
    """
    report = collect_doctor_report(args)
    emit_json(_redact_diagnostic_value(report), args)
    return 0 if report["ok"] else 1


def cmd_status(args: argparse.Namespace) -> int:
    """Project the diagnostic probes into a concise operator dashboard."""

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
    Execute the `diagnostics collect` CLI command.


    :param args:
    :return:
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
    Build the `diagnostics` command-line parser family.


    :param subparsers:
    :return:
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
