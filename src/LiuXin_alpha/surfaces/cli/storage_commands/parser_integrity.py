"""Storage CLI parser integrity ownership."""

from __future__ import annotations

import argparse

from LiuXin_alpha.surfaces.cli.storage_commands.administration import (
    cmd_storage_asset_show,
    cmd_storage_asset_verify,
    cmd_storage_replica_verify,
)
from LiuXin_alpha.surfaces.cli.storage_commands.core_access import _core_json
from LiuXin_alpha.surfaces.cli.storage_commands.integrity import (
    cmd_storage_audit,
    cmd_storage_policy,
    cmd_storage_policy_set,
    cmd_storage_policy_violations,
    cmd_storage_reconcile,
    cmd_storage_recovery_action,
    cmd_storage_recovery_list,
    cmd_storage_repair,
    cmd_storage_status,
)
from LiuXin_alpha.surfaces.cli.storage_commands.resources import (
    cmd_storage_resource_delete,
    cmd_storage_resource_get,
    cmd_storage_resource_list,
    cmd_storage_resource_write,
    cmd_storage_resources_describe,
)


def _add_asset_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    asset = commands.add_parser("asset", help="Inspect a digital asset graph.")
    asset_commands = asset.add_subparsers(dest="asset_action", required=True)
    asset_show = asset_commands.add_parser("show", aliases=["get"])
    _core_json(asset_show)
    asset_show.add_argument("asset_id", type=int)
    asset_show.set_defaults(handler=cmd_storage_asset_show)

    asset_verify = asset_commands.add_parser("verify")
    _core_json(asset_verify)
    asset_verify.add_argument("asset_id", type=int)
    asset_verify.add_argument("--replica-id", action="append", type=int)
    asset_verify.add_argument("--all-replicas", action="store_true")
    asset_verify.set_defaults(handler=cmd_storage_asset_verify)


def _add_replica_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    replica = commands.add_parser("replica", help="Verify one concrete Replica.")
    replica_commands = replica.add_subparsers(dest="replica_action", required=True)
    replica_verify = replica_commands.add_parser("verify")
    _core_json(replica_verify)
    replica_verify.add_argument("replica_id", type=int)
    replica_verify.add_argument("--no-digests", action="store_true")
    replica_verify.set_defaults(handler=cmd_storage_replica_verify)


def _add_status_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    status = commands.add_parser(
        "status",
        help=(
            "Overview configured folder Stores, capacity, Replicas, and "
            "actionable health."
        ),
    )
    _core_json(status)
    status.add_argument(
        "--refresh",
        action="store_true",
        help="Probe Store backends instead of using their cached status.",
    )
    status.set_defaults(handler=cmd_storage_status)


def _add_audit_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    audit = commands.add_parser("audit", help="Verify a bounded page of Replicas.")
    _core_json(audit)
    audit.add_argument("--limit", type=int, default=100)
    audit.add_argument("--offset", type=int, default=0)
    audit.add_argument("--no-digests", action="store_true")
    audit.set_defaults(handler=cmd_storage_audit)


def _add_reconcile_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    reconcile = commands.add_parser(
        "reconcile", help="Plan or apply bounded non-destructive storage repair."
    )
    reconcile_commands = reconcile.add_subparsers(
        dest="reconcile_action", required=True
    )
    reconcile_plan = reconcile_commands.add_parser("plan")
    _core_json(reconcile_plan)
    reconcile_plan.add_argument("--refresh", action="store_true")
    reconcile_plan.set_defaults(handler=cmd_storage_reconcile)
    reconcile_apply = reconcile_commands.add_parser("apply")
    _core_json(reconcile_apply)
    reconcile_apply.add_argument("--yes", action="store_true")
    reconcile_apply.add_argument("--max-actions", type=int, default=100)
    reconcile_apply.add_argument("--include-offline", action="store_true")
    reconcile_apply.set_defaults(refresh=False, handler=cmd_storage_reconcile)


def _add_repair_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    repair = commands.add_parser(
        "repair",
        help="Plan or apply bounded, non-deleting Replica and policy repair.",
    )
    repair_commands = repair.add_subparsers(dest="repair_action", required=True)
    repair_plan = repair_commands.add_parser("plan")
    _core_json(repair_plan)
    repair_plan.add_argument("--asset-id", type=int)
    repair_plan.add_argument("--max-assets", type=int, default=100)
    repair_plan.set_defaults(handler=cmd_storage_repair)
    repair_apply = repair_commands.add_parser("apply")
    _core_json(repair_apply)
    repair_apply.add_argument("--asset-id", type=int)
    repair_apply.add_argument("--max-assets", type=int, default=100)
    repair_apply.add_argument("--max-actions", type=int, default=100)
    repair_apply.add_argument("--max-transfer-gib", type=float, default=100.0)
    repair_apply.add_argument("--yes", action="store_true")
    repair_apply.set_defaults(handler=cmd_storage_repair)


def _add_recovery_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    recovery = commands.add_parser(
        "recovery", help="Inspect and act on durable ingest-journal recovery."
    )
    recovery_commands = recovery.add_subparsers(dest="recovery_action", required=True)
    recovery_list = recovery_commands.add_parser("list")
    _core_json(recovery_list)
    recovery_list.add_argument("--state")
    recovery_list.add_argument("--limit", type=int, default=100)
    recovery_list.add_argument("--offset", type=int, default=0)
    recovery_list.set_defaults(handler=cmd_storage_recovery_list)
    recover_pending = recovery_commands.add_parser("recover-pending")
    _core_json(recover_pending)
    recover_pending.add_argument("operation_id", nargs="?")
    recover_pending.add_argument("--yes", action="store_true")
    recover_pending.set_defaults(handler=cmd_storage_recovery_action)
    retry_ingest = recovery_commands.add_parser("retry-ingest")
    _core_json(retry_ingest)
    retry_ingest.add_argument("operation_id")
    retry_ingest.add_argument("--yes", action="store_true")
    retry_ingest.set_defaults(handler=cmd_storage_recovery_action)


def _add_policies_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    policies = commands.add_parser(
        "policies", help="Assess and configure asset placement policies."
    )
    policy_commands = policies.add_subparsers(dest="policy_action", required=True)
    for action in ("assess", "plan"):
        command = policy_commands.add_parser(action)
        _core_json(command)
        command.add_argument("asset_id", type=int)
        command.set_defaults(handler=cmd_storage_policy)
    violations = policy_commands.add_parser("violations")
    _core_json(violations)
    violations.add_argument("--limit", type=int, default=100)
    violations.add_argument("--offset", type=int, default=0)
    violations.set_defaults(handler=cmd_storage_policy_violations)
    policy_set = policy_commands.add_parser("set")
    _core_json(policy_set)
    policy_set.add_argument("asset_id", type=int)
    policy_set.add_argument("--replication-policy-id", type=int)
    policy_set.add_argument("--backup-policy-id", type=int)
    policy_set.set_defaults(handler=cmd_storage_policy_set)


def _add_resources_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    resources = commands.add_parser(
        "resources", help="Inspect or edit stable storage graph resources."
    )
    resource_commands = resources.add_subparsers(dest="resource_action", required=True)
    describe = resource_commands.add_parser("describe")
    _core_json(describe)
    describe.set_defaults(handler=cmd_storage_resources_describe)
    resource_list = resource_commands.add_parser("list")
    _core_json(resource_list)
    resource_list.add_argument("resource")
    resource_list.add_argument("--where-file")
    resource_list.add_argument("--limit", type=int, default=100)
    resource_list.add_argument("--offset", type=int, default=0)
    resource_list.set_defaults(handler=cmd_storage_resource_list)
    resource_get = resource_commands.add_parser("get")
    _core_json(resource_get)
    resource_get.add_argument("resource")
    resource_get.add_argument("resource_id", type=int)
    resource_get.set_defaults(handler=cmd_storage_resource_get)
    for action in ("create", "update"):
        command = resource_commands.add_parser(action)
        _core_json(command)
        command.add_argument("resource")
        if action == "update":
            command.add_argument("resource_id", type=int)
        command.add_argument("values_file")
        command.set_defaults(handler=cmd_storage_resource_write)
    resource_delete = resource_commands.add_parser("delete")
    _core_json(resource_delete)
    resource_delete.add_argument("resource")
    resource_delete.add_argument("resource_id", type=int)
    resource_delete.add_argument("--yes", action="store_true")
    resource_delete.set_defaults(handler=cmd_storage_resource_delete)
