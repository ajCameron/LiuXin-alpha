"""Freeze and order the plugin declarations used by a maintenance runtime."""

from __future__ import annotations

import dataclasses
import inspect
import json
import math
from collections.abc import Iterable

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.errors import MaintenanceFailure
from LiuXin_alpha.databases.maintenance.plugins import MaintenancePluginUpgrade
from LiuXin_alpha.databases.maintenance.schema import TABLES


@dataclasses.dataclass(frozen=True, slots=True)
class PluginDeclaration:
    """Persist one implementation's identity, subscriptions and execution policy."""

    name: str
    version: str
    mode: MaintenanceMode
    tables: tuple[str, ...]
    dependencies: tuple[str, ...]
    priority: int
    retry_limit: int
    retry_backoff_s: float

    @classmethod
    def from_plugin(cls, plugin: object) -> PluginDeclaration:
        """Validate configuration before it can acquire persistent obligations."""
        name = getattr(plugin, "name", None)
        version = getattr(plugin, "version", "1")
        if (
            not isinstance(name, str)
            or not name.strip()
            or "\x00" in name
            or name == "maintenance-plugin"
        ):
            raise ValueError("Maintenance plugins require a stable nonempty name")
        if not isinstance(version, str) or not version.strip() or "\x00" in version:
            raise ValueError("Maintenance plugins require a nonempty version")
        tables = _names(getattr(plugin, "tables", ()), "table")
        if any(table.casefold() in TABLES for table in tables):
            raise ValueError("Plugins cannot subscribe to maintenance-owned tables")
        dependencies = _names(getattr(plugin, "dependencies", ()), "dependency")
        if type(getattr(plugin, "enabled_by_default", True)) is not bool:
            raise ValueError("enabled_by_default must be a boolean")
        for hook in ("wants_event", "coalesce_key", "handle_events"):
            require_sync_callable(getattr(plugin, hook, None), hook)
        policies = getattr(plugin, "upgrades", ())
        if not isinstance(policies, (tuple, list)):
            raise ValueError("Plugin upgrades must be a sequence of policies")
        versions = set()
        for policy in policies:
            if (
                not isinstance(policy, MaintenancePluginUpgrade)
                or not isinstance(policy.from_version, str)
                or not policy.from_version.strip()
                or "\x00" in policy.from_version
                or policy.from_version in versions
                or type(policy.replay_pending) is not bool
            ):
                raise ValueError("Invalid or duplicate maintenance upgrade policy")
            versions.add(policy.from_version)
            if policy.migrate is not None:
                require_sync_callable(policy.migrate, "upgrade migration")
        priority = getattr(plugin, "priority", 0)
        retry_limit = getattr(plugin, "retry_limit", 5)
        backoff = getattr(plugin, "retry_backoff_s", 1.0)
        if (
            type(priority) is not int
            or not -(2**31) <= priority < 2**31
            or type(retry_limit) is not int
            or not 0 <= retry_limit < 2**31 - 1
        ):
            raise ValueError(
                "Priority and retry limit must fit backend integers; retries need exhaustion headroom"
            )
        if isinstance(backoff, bool) or not isinstance(backoff, (int, float)):
            raise ValueError("Retry backoff must be a finite nonnegative number")
        try:
            backoff = float(backoff)
        except OverflowError as error:
            raise ValueError(
                "Retry backoff must be a finite nonnegative number"
            ) from error
        if not math.isfinite(backoff) or backoff < 0 or backoff > 3.402823466e38:
            raise ValueError("Retry backoff must be a finite nonnegative number")
        return cls(
            name,
            version,
            MaintenanceMode(getattr(plugin, "mode", MaintenanceMode.LAZY)),
            tables,
            dependencies,
            priority,
            retry_limit,
            float(backoff),
        )

    def to_json(self) -> str:
        """Produce the stable declaration compared when implementations reattach."""
        return json.dumps(
            dataclasses.asdict(self), sort_keys=True, separators=(",", ":")
        )

    @classmethod
    def from_json(cls, value: str) -> PluginDeclaration:
        """Restore a declaration captured by this schema version."""
        data = json.loads(value)
        data["mode"] = MaintenanceMode(data["mode"])
        data["tables"] = tuple(data["tables"])
        data["dependencies"] = tuple(data["dependencies"])
        return cls(**data)


def _names(values: Iterable[str], kind: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise ValueError(f"Plugin {kind}s must be a collection, not a string")
    names = tuple(values)
    if any(
        not isinstance(name, str) or not name.strip() or "\x00" in name
        for name in names
    ):
        raise ValueError(f"Plugin {kind}s must be nonempty names")
    if len(set(names)) != len(names):
        raise ValueError(f"Duplicate plugin {kind}")
    return names


def require_sync_callable(callback: object, name: str) -> None:
    """Reject missing and deferred hooks before they can own persistent work."""
    if not callable(callback) or any(
        predicate(candidate)
        for candidate in (callback, callback.__call__)
        for predicate in (
            inspect.iscoroutinefunction,
            inspect.isgeneratorfunction,
            inspect.isasyncgenfunction,
        )
    ):
        raise ValueError(f"Maintenance {name} must be a synchronous callable")


def reject_deferred_result(result: object, name: str) -> None:
    """Catch synchronous wrappers that return unexecuted asynchronous/generator work."""
    if (
        inspect.isawaitable(result)
        or inspect.isgenerator(result)
        or inspect.isasyncgen(result)
    ):
        # Native suspended objects have not executed their bodies. Closing these
        # avoids unawaited-coroutine warnings without executing deferred effects.
        if inspect.iscoroutine(result) or inspect.isgenerator(result):
            result.close()
        raise MaintenanceFailure(f"Maintenance {name} returned deferred work")


def order_declarations(
    declarations: Iterable[PluginDeclaration],
) -> tuple[PluginDeclaration, ...]:
    """Topologically order explicit dependencies, with priority as a ready-work tie break."""
    declarations = tuple(declarations)
    pending = {item.name: item for item in declarations}
    if len(pending) != len(declarations):
        raise ValueError("Duplicate maintenance plugin name")
    for item in declarations:
        for dependency in item.dependencies:
            if dependency not in pending:
                raise ValueError(
                    f"Missing maintenance dependency {dependency!r} for {item.name!r}"
                )
            if (
                item.mode is MaintenanceMode.PROMPT
                and pending[dependency].mode is MaintenanceMode.LAZY
            ):
                raise ValueError("Prompt maintenance cannot depend on lazy completion")
    ordered = []
    completed: set[str] = set()
    while pending:
        ready = [
            item for item in pending.values() if set(item.dependencies) <= completed
        ]
        if not ready:
            raise ValueError("Maintenance dependency cycle")
        item = min(ready, key=lambda value: (-value.priority, value.name))
        ordered.append(item)
        completed.add(item.name)
        del pending[item.name]
    return tuple(ordered)
