"""
Register a typed Store, refresh composition, and optionally probe/select its default.

These are sequential Core operations, not one transaction. Saving can survive a
later refresh/probe/default/output failure. Only probe Exceptions are converted
to a structured failed observation here; their messages are not locally redacted.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.surfaces.cli.common import emit_json, open_cli_core
from LiuXin_alpha.surfaces.cli.storage_commands.store_options import (
    _descriptor_for_kind,
    _store_add_payload,
)


def _refresh_failure_count(value: object) -> int:
    """
    Coerce an optional refresh receipt's failure count, treating unsupported shapes as zero.

    Prefer report when present and failed_configurations when present; even invalid
    present values prevent fallback to their alternatives. Convert via int(str(raw
    or 0)), so ordinary integer strings work while True or fractional text falls
    back to zero. Negative counts are preserved, not clamped.

    Example:
        >>> _refresh_failure_count({"report": {"failed_stores": "2"}})
        2
        >>> _refresh_failure_count({"failed_configurations": "invalid", "failed_stores": 4})
        0


    :param value: Direct or report-wrapped mapping-shaped refresh receipt, or any other value.
    :return: Converted count, otherwise zero for unsupported shape or TypeError/ValueError.
    """
    if not isinstance(value, Mapping):
        return 0
    report = value.get("report", value)
    if not isinstance(report, Mapping):
        return 0
    raw = report.get("failed_configurations", report.get("failed_stores", 0))
    try:
        return int(str(raw or 0))
    except (TypeError, ValueError):
        return 0


def cmd_storage_store_add(args: argparse.Namespace) -> int:
    """
    Save a descriptor-backed Store, refresh it, and optionally probe and select it as default.

    Request noninternal providers, retaining only mappings from a list response,
    then build/save the Store and always refresh with startup/clear-existing true.
    With check enabled, probe online Stores only; ordinary probe exceptions become
    raw error receipts, and availability is taken from status.available. Offline
    Stores skip the probe and leave probe_ok true.

    Default selection requires the default flag and either no check or a successful
    probe; refresh failure counts do not prevent this selection. Overall ok requires
    zero coerced refresh failures and probe_ok, but failed saved/default receipt
    flags are not independently interpreted. Publish after Core cleanup. Earlier
    effects are not rolled back, including when the final report cannot be written.

    Example:
        >>> cmd_storage_store_add(parsed_store_add_args)  # doctest: +SKIP


    :param args: Backend kind, typed Store declaration choices, check/default,
        include_offline/strict refresh policy, and connection/output controls.
    :return: Zero when ok or check is disabled; one only for a failed checked outcome.
    """
    check = bool(getattr(args, "check", False))
    with open_cli_core(args, enable_storage_manager=True) as core:
        provider_result = core.query(
            "storage.backends.list",
            {"include_internal": False},
        )
        provider_values = (
            provider_result.get("backends", [])
            if isinstance(provider_result, Mapping)
            else []
        )
        providers = (
            [value for value in provider_values if isinstance(value, Mapping)]
            if isinstance(provider_values, list)
            else []
        )
        descriptor = _descriptor_for_kind(str(args.kind), providers)
        store = _store_add_payload(args, descriptor)
        saved = core.command("storage.store.save", {"store": store})
        refreshed = core.command(
            "storage.refresh",
            {
                "startup_on_add": True,
                "include_offline": bool(args.include_offline),
                "clear_existing": True,
                "strict": bool(args.strict),
            },
        )
        probe: dict[str, Any] | None = None
        probe_ok = True
        if check and store["store_online_status"] == "online":
            try:
                probe_result = core.command(
                    "storage.store.probe",
                    {"store": store["store_name"]},
                )
            except Exception as error:
                probe_ok = False
                probe = {
                    "ok": False,
                    "error": {
                        "type": type(error).__name__,
                        "message": str(error) or type(error).__name__,
                    },
                }
            else:
                status = (
                    probe_result.get("status", {})
                    if isinstance(probe_result, Mapping)
                    else {}
                )
                probe_ok = bool(
                    isinstance(status, Mapping) and status.get("available", False)
                )
                probe = {"ok": probe_ok, "result": probe_result}
        selected = None
        if bool(args.default) and (not check or probe_ok):
            selected = core.command(
                "storage.default.set",
                {"store": store["store_name"]},
            )
    ok = _refresh_failure_count(refreshed) == 0 and probe_ok
    emit_json(
        {
            "ok": ok,
            "backend": {
                "kind": descriptor.get("kind"),
                "label": descriptor.get("label"),
                "location_type": descriptor.get("location_type"),
                "read_only_default": bool(descriptor.get("read_only_default", False)),
            },
            "saved": saved,
            "refresh": refreshed,
            "probe": probe,
            "default": selected,
            "store": store,
            "next": [
                "liuxin",
                "storage",
                "status",
            ],
        },
        args,
    )
    return 0 if ok or not check else 1
