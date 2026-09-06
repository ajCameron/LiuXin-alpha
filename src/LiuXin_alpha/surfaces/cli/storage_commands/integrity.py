"""Storage CLI integrity ownership."""

from __future__ import annotations

import argparse
from typing import Any

from LiuXin_alpha.surfaces.cli.common import emit_json, open_cli_core
from LiuXin_alpha.surfaces.cli.storage_commands.core_access import (
    _storage_command,
    _storage_query,
)


def cmd_storage_status(args: argparse.Namespace) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query("storage.status", {"refresh_stores": bool(args.refresh)})
    emit_json(result, args)
    return 0 if bool(result.get("healthy", False)) else 1


def cmd_storage_audit(args: argparse.Namespace) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(
            "storage.audit",
            {
                "limit": int(args.limit),
                "offset": int(args.offset),
                "calculate_digests": not bool(args.no_digests),
            },
        )
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_reconcile(args: argparse.Namespace) -> int:
    payload = {"refresh_stores": bool(args.refresh)}
    if args.reconcile_action == "plan":
        return _storage_query(args, "storage.reconcile.plan", payload)
    if not args.yes:
        raise ValueError(
            "Storage reconciliation requires --yes; run `reconcile plan` first."
        )
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(
            "storage.reconcile.apply",
            {
                "max_actions": int(args.max_actions),
                "include_offline": bool(args.include_offline),
            },
        )
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_repair(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"max_assets": int(args.max_assets)}
    if args.asset_id is not None:
        payload["asset_id"] = int(args.asset_id)
    if args.repair_action == "plan":
        return _storage_query(args, "storage.repair.plan", payload)
    if not args.yes:
        raise ValueError(
            "Storage repair requires --yes; run `storage repair plan` first."
        )
    payload.update(
        {
            "max_actions": int(args.max_actions),
            "max_transfer_bytes": int(
                float(args.max_transfer_gib) * 1024 * 1024 * 1024
            ),
        }
    )
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command("storage.repair.apply", payload)
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_recovery_list(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "limit": int(args.limit),
        "offset": int(args.offset),
    }
    if args.state:
        payload["state"] = args.state
    return _storage_query(args, "storage.recovery.list", payload)


def cmd_storage_recovery_action(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError(
            "Storage ingest recovery requires --yes after reviewing "
            "`storage recovery list`."
        )
    payload: dict[str, Any] = {}
    if getattr(args, "operation_id", None):
        payload["operation_id"] = args.operation_id
    operation = (
        "storage.recovery.retry-ingest"
        if args.recovery_action == "retry-ingest"
        else "storage.recovery.recover-pending"
    )
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(operation, payload)
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_policy(args: argparse.Namespace) -> int:
    payload = {"asset_id": int(args.asset_id)}
    return _storage_query(args, "storage.policy." + args.policy_action, payload)


def cmd_storage_policy_violations(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.policy.violations",
        {"limit": int(args.limit), "offset": int(args.offset)},
    )


def cmd_storage_policy_set(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.replication_policy_id is not None:
        payload["replication_policy_id"] = int(args.replication_policy_id)
    if args.backup_policy_id is not None:
        payload["backup_policy_id"] = int(args.backup_policy_id)
    return _storage_command(args, "storage.asset.policies.set", payload)
