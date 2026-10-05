"""
Plan and apply non-deleting verification/replication repair through Core's storage manager.

Manager policy chooses source/destination eligibility. Adapters retain surplus
removal and exact recreation as deferred actions, limit attempted actions, and
budget estimated sizes of successfully returned copy calls rather than actual
I/O. Before/after replanning is observational, not an enclosing transaction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import _optional_int, _payload, plain

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def _storage_repair_plan_payload(
    manager: Any,
    *,
    asset_id: int | None,
    max_assets: int,
) -> dict[str, Any]:
    """
    Plan selected Assets in numeric-ID order, preserving manager action order and deferring deletion/recreation.

    All-Asset enumeration is materialized/sorted before slicing; a requested Asset
    uses direct lookup. Failures in an Asset's replication/backup/policy calls add
    a warning and skip its actions, but other attribute/projection errors propagate.
    Verification IDs are deduplicated across the whole selected scope; destinations
    and deferred removals are not deduplicated across replication and backup plans.

    complete counts selected versus available Assets, including failed planning,
    not successful coverage. blocked means any warning or deferred exact recreation;
    surplus removal alone does not block. Transfer estimates sum Asset sizes for
    copy actions only, with no capacity reservation or executor invocation here.

    Example:
        >>> plan = _storage_repair_plan_payload(manager, asset_id=None, max_assets=100)  # doctest: +SKIP


    :param manager: Storage manager providing Asset records, replication/backup plans, and effective policies.
    :param asset_id: Optional single Asset selector; None enumerates the complete current Asset registry.
    :param max_assets: Raw slice bound for selected Assets; public handlers validate/cap it before calling.
    :return: Actions, deferred actions, warnings, scope counts, estimated transfer bytes, and non-deleting plan flags.
    """

    from LiuXin_alpha.storage import api as storage_api

    if asset_id is None:
        all_assets = sorted(
            manager.iter_digital_asset_records(),
            key=lambda record: int(record.digital_asset_id),
        )
    else:
        all_assets = [manager.get_digital_asset_record(asset_id)]
    selected = all_assets[:max_assets]
    actions: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen_verifications: set[int] = set()
    for asset in selected:
        current_asset_id = int(asset.digital_asset_id)
        try:
            replication = manager.plan_replication(asset.digital_asset_id)
            backup = manager.plan_backup(asset.digital_asset_id)
            policies = manager.resolve_effective_policies(asset.digital_asset_id)
        except Exception as error:
            warnings.append(
                f"Asset {current_asset_id} could not be planned: {str(error) or type(error).__name__}"
            )
            continue
        for replica_id in (
            *replication.replica_ids_to_verify,
            *backup.replica_ids_to_verify,
        ):
            numeric_replica_id = int(replica_id)
            if numeric_replica_id in seen_verifications:
                continue
            seen_verifications.add(numeric_replica_id)
            actions.append(
                {
                    "action": "verify_replica",
                    "digital_asset_id": current_asset_id,
                    "replica_id": numeric_replica_id,
                    "estimated_bytes": int(asset.size_bytes),
                }
            )
        for destination in replication.destination_store_refs:
            actions.append(
                {
                    "action": "replicate_digital_asset",
                    "digital_asset_id": current_asset_id,
                    "destination_store_ref": str(destination),
                    "mode": policies.replication.mode.value,
                    "estimated_bytes": int(asset.size_bytes),
                }
            )
        for destination in backup.destination_store_refs:
            actions.append(
                {
                    "action": "replicate_digital_asset",
                    "digital_asset_id": current_asset_id,
                    "destination_store_ref": str(destination),
                    "mode": policies.backup.mode.value,
                    "estimated_bytes": int(asset.size_bytes),
                }
            )
        for replica_id in (
            *replication.replica_ids_to_remove,
            *backup.replica_ids_to_remove,
        ):
            deferred.append(
                {
                    "action": "remove_surplus_replica",
                    "digital_asset_id": current_asset_id,
                    "replica_id": int(replica_id),
                    "reason": "repair is intentionally non-deleting",
                }
            )
        if replication.exact_recreation_derivation_id is not None:
            deferred.append(
                {
                    "action": "recreate_exact_derivation",
                    "digital_asset_id": current_asset_id,
                    "digital_asset_derivation_id": int(
                        replication.exact_recreation_derivation_id
                    ),
                    "reason": "no executor is selected by storage repair",
                }
            )
        warnings.extend(
            f"Asset {current_asset_id}: {message}"
            for message in (*replication.warnings, *backup.warnings)
        )
    blocked = any(
        action["action"] == "recreate_exact_derivation" for action in deferred
    ) or bool(warnings)
    return {
        "asset_id": asset_id,
        "assets_scanned": len(selected),
        "assets_available": len(all_assets),
        "complete": len(selected) == len(all_assets),
        "max_assets": max_assets,
        "actions": actions,
        "action_count": len(actions),
        "estimated_transfer_bytes": sum(
            int(action["estimated_bytes"])
            for action in actions
            if action["action"] == "replicate_digital_asset"
        ),
        "deferred_actions": deferred,
        "warnings": warnings,
        "blocked": blocked,
        "deletes_bytes": False,
        "supported_modes": [mode.value for mode in storage_api.ReplicaMode],
    }


def storage_repair_plan(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    """
    Build a fresh non-deleting repair plan for one Asset or a capped prefix of the Asset registry.

    Optional asset_id must be positive. max_assets defaults to 100, must be positive,
    and is capped at 10,000; explicit None reaches an assertion rather than selecting
    the default. Manager planning may inspect backend state but no action is applied.

    Example:
        >>> plan = storage_repair_plan(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose storage manager supplies the repair-planning APIs.
    :param query: Query with optional asset_id and max_assets.
    :return: Fresh plan with selected-scope completeness, actions, deferrals, estimates, and warnings.
    """
    payload = _payload(query)
    asset_id = _optional_int(payload, "asset_id", minimum=1)
    max_assets = _optional_int(payload, "max_assets", default=100, minimum=1)
    assert max_assets is not None
    return _storage_repair_plan_payload(
        runtime.library.storage,
        asset_id=asset_id,
        max_assets=min(max_assets, 10_000),
    )


def storage_repair_apply(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Plan, attempt bounded verification/copy actions, and replan the same selected scope without removing replicas.

    Positive max_assets/max_actions default to 100 and cap at 10,000. The positive
    transfer budget defaults to 100 GiB. Action limits count attempts, including
    failures; transferred_bytes charges estimated sizes only after copy calls return,
    not actual bytes or partial failed transfers. An over-budget copy stops the
    sequence rather than skipping ahead. Plan blocked does not suppress its actions.

    Verification must report healthy; normally returned copy calls count as success
    with verify=True delegated to the manager. Call failures become receipts, but
    successful-result projection or replanning can still fail after changes. Final
    ok requires no failed/truncated attempts and no remaining actions/blockers in the
    replanned subset; it does not require that every available Asset was scanned.

    Example:
        >>> receipt = storage_repair_apply(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing repair planning, replica verification, and Asset replication.
    :param command: Command with optional asset_id, max_assets, max_actions, and max_transfer_bytes; no stored plan or confirmation is consumed.
    :return: Before/after plans, attempt receipts/counts, estimated successful transfer bytes, budgets, truncation, and selected-scope ok.
    """
    from LiuXin_alpha.storage import api as storage_api

    payload = _payload(command)
    asset_id = _optional_int(payload, "asset_id", minimum=1)
    max_assets = _optional_int(payload, "max_assets", default=100, minimum=1)
    max_actions = _optional_int(payload, "max_actions", default=100, minimum=1)
    max_transfer_bytes = _optional_int(
        payload,
        "max_transfer_bytes",
        default=100 * 1024 * 1024 * 1024,
        minimum=1,
    )
    assert (
        max_assets is not None
        and max_actions is not None
        and max_transfer_bytes is not None
    )
    max_assets = min(max_assets, 10_000)
    max_actions = min(max_actions, 10_000)
    manager = runtime.library.storage
    plan = _storage_repair_plan_payload(
        manager,
        asset_id=asset_id,
        max_assets=max_assets,
    )
    receipts: list[dict[str, Any]] = []
    transferred = 0
    truncated = False
    for action in plan["actions"]:
        if len(receipts) >= max_actions:
            truncated = True
            break
        action_name = str(action["action"])
        estimated = int(action.get("estimated_bytes", 0))
        if (
            action_name == "replicate_digital_asset"
            and transferred + estimated > max_transfer_bytes
        ):
            truncated = True
            break
        receipt = dict(action)
        try:
            if action_name == "verify_replica":
                result = manager.verify_replica(
                    storage_api.ReplicaID(int(action["replica_id"]))
                )
            elif action_name == "replicate_digital_asset":
                result = manager.replicate_digital_asset(
                    storage_api.DigitalAssetID(int(action["digital_asset_id"])),
                    destination_store_ref=UUID(str(action["destination_store_ref"])),
                    mode=storage_api.ReplicaMode(str(action["mode"])),
                    verify=True,
                )
                transferred += estimated
            else:
                raise CoreDispatchError(
                    f"Unknown storage repair action: {action_name}."
                )
        except Exception as error:
            receipt.update(
                {
                    "ok": False,
                    "error": str(error) or type(error).__name__,
                }
            )
        else:
            action_ok = not (
                action_name == "verify_replica"
                and not bool(getattr(result, "healthy", False))
            )
            receipt.update({"ok": action_ok, "result": plain(result)})
            if not action_ok:
                receipt["error"] = "Replica verification did not pass."
        receipts.append(receipt)
    after = _storage_repair_plan_payload(
        manager,
        asset_id=asset_id,
        max_assets=max_assets,
    )
    failed = [receipt for receipt in receipts if not receipt["ok"]]
    ok = (
        not failed
        and not truncated
        and int(after["action_count"]) == 0
        and not bool(after["blocked"])
    )
    return {
        "ok": ok,
        "before": plan,
        "after": after,
        "actions": receipts,
        "actions_applied": len(receipts),
        "actions_failed": len(failed),
        "actions_truncated": truncated,
        "transferred_bytes": transferred,
        "max_actions": max_actions,
        "max_transfer_bytes": max_transfer_bytes,
        "deletes_bytes": False,
    }
