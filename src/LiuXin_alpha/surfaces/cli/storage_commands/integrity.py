"""
Expose storage health, reconciliation, repair, recovery, and asset-policy commands.

Plans and ordinary policy/list requests use shared JSON helpers and return zero
without interpreting receipt flags. Health reads use healthy; audit and apply/
recovery commands use ok. Receipt output follows Core session exit and precedes
status interpretation. Confirmation gates selected mutations, but no output
preflight, cross-operation rollback, or proof of a reviewed prior plan is added.
"""

from __future__ import annotations

import argparse
from typing import Any

from LiuXin_alpha.surfaces.cli.common import emit_json, open_cli_core
from LiuXin_alpha.surfaces.cli.storage_commands.core_access import (
    _storage_command,
    _storage_query,
)


def cmd_storage_status(args: argparse.Namespace) -> int:
    """
    Query overall storage health with optional Store refresh and publish the receipt.

    Example:
        >>> cmd_storage_status(parsed_storage_status_args)  # doctest: +SKIP


    :param args: refresh flag plus Core connection and JSON publication settings.
    :return: Zero for truthy receipt.healthy, otherwise one, after output succeeds.
    """
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query("storage.status", {"refresh_stores": bool(args.refresh)})
    emit_json(result, args)
    return 0 if bool(result.get("healthy", False)) else 1


def cmd_storage_audit(args: argparse.Namespace) -> int:
    """
    Run Core's paged storage audit with optional digest calculation.

    Audit is a command, not a guaranteed read-only query. Pagination is converted
    to integers without local range checks, and no --yes gate is imposed here.

    Example:
        >>> cmd_storage_audit(parsed_storage_audit_args)  # doctest: +SKIP


    :param args: limit/offset, no_digests, and connection/JSON output controls.
    :return: Zero for truthy receipt.ok, otherwise one, after publishing the audit receipt.
    """
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
    """
    Plan reconciliation or require confirmation before applying bounded actions.

    Only exact reconcile_action='plan' selects a query; direct callers otherwise
    enter the apply branch. refresh affects the plan payload only. Apply sends
    max_actions/include_offline and does not bind execution to an earlier plan.

    Example:
        >>> cmd_storage_reconcile(parsed_reconcile_args)  # doctest: +SKIP


    :param args: reconcile_action, refresh, yes, max_actions, include_offline,
        and shared connection/output settings.
    :return: Zero after plan publication; apply returns zero for truthy receipt.ok, else one.
    :raises ValueError: Applying without a truthy yes confirmation.
    """
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
    """
    Preview asset repair or apply it with confirmation and action/transfer ceilings.

    Include asset_id whenever not None, including zero. Convert GiB to truncated
    bytes only for apply; no local positive-range checks or prior-plan token is used.
    Exact repair_action='plan' selects preview; other direct values select apply.

    Example:
        >>> cmd_storage_repair(parsed_repair_args)  # doctest: +SKIP


    :param args: repair_action, optional asset_id, max_assets/max_actions/max_transfer_gib,
        yes confirmation, and shared Core/output settings.
    :return: Zero after plan publication; apply returns zero for truthy receipt.ok, else one.
    :raises ValueError: An apply request lacks confirmation or numeric conversion fails.
    """
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
    """
    List recovery records with integer pagination and an optional unchanged state filter.

    Example:
        >>> cmd_storage_recovery_list(parsed_recovery_list_args)  # doctest: +SKIP


    :param args: limit/offset, optional truthy state, and Core/JSON output controls.
    :return: Zero after publishing storage.recovery.list without inspecting issue counts.
    """
    payload: dict[str, Any] = {
        "limit": int(args.limit),
        "offset": int(args.offset),
    }
    if args.state:
        payload["state"] = args.state
    return _storage_query(args, "storage.recovery.list", payload)


def cmd_storage_recovery_action(args: argparse.Namespace) -> int:
    """
    Require confirmation before retrying ingest or recovering pending operations.

    Exact retry-ingest selects that command; every other direct action value
    selects recover-pending. Forward operation_id only when truthy. The handler
    does not verify that the operator previously listed or inspected recovery state.

    Example:
        >>> cmd_storage_recovery_action(parsed_recovery_action_args)  # doctest: +SKIP


    :param args: recovery_action, optional operation_id, yes, and Core/output controls.
    :return: Zero for truthy receipt.ok, otherwise one, after result publication.
    :raises ValueError: The yes confirmation flag is falsey.
    """
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
    """
    Query an asset policy view selected by the parser's policy_action suffix.

    The operation name is concatenated directly; this handler relies on the
    parser for allowed actions rather than enforcing its own suffix whitelist.

    Example:
        >>> cmd_storage_policy(parsed_policy_args)  # doctest: +SKIP


    :param args: asset_id, policy_action suffix, and Core/JSON output controls.
    :return: Zero after publishing storage.policy.<action>, regardless of receipt flags.
    """
    payload = {"asset_id": int(args.asset_id)}
    return _storage_query(args, "storage.policy." + args.policy_action, payload)


def cmd_storage_policy_violations(args: argparse.Namespace) -> int:
    """
    Publish a page of policy violations without making their presence an exit failure.

    Example:
        >>> cmd_storage_policy_violations(parsed_policy_violations_args)  # doctest: +SKIP


    :param args: Integer-convertible limit/offset and shared Core/output settings.
    :return: Zero after publishing storage.policy.violations, including nonempty results.
    """
    return _storage_query(
        args,
        "storage.policy.violations",
        {"limit": int(args.limit), "offset": int(args.offset)},
    )


def cmd_storage_policy_set(args: argparse.Namespace) -> int:
    """
    Submit supplied replication/backup policy IDs for an asset without a confirmation gate.

    Include each policy only when not None, retaining zero after integer conversion.
    Both may be omitted; the handler does not require an effective change.

    Example:
        >>> cmd_storage_policy_set(parsed_policy_set_args)  # doctest: +SKIP


    :param args: asset_id, optional replication_policy_id/backup_policy_id, and Core/output controls.
    :return: Zero after publishing storage.asset.policies.set without interpreting its flags.
    """
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.replication_policy_id is not None:
        payload["replication_policy_id"] = int(args.replication_policy_id)
    if args.backup_policy_id is not None:
        payload["backup_policy_id"] = int(args.backup_policy_id)
    return _storage_command(args, "storage.asset.policies.set", payload)
