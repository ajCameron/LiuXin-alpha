"""Small internal plugin primitives for database maintenance work."""

from __future__ import annotations

import abc
import dataclasses

from typing import TYPE_CHECKING, Iterable

from LiuXin_alpha.databases.maintenance.events import MaintenanceEvent

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI


@dataclasses.dataclass(slots=True)
class MaintenancePluginContext:
    db: "DatabaseAPI"
    logger: object | None = None


@dataclasses.dataclass(slots=True)
class MaintenancePluginResult:
    handled: int = 0
    deferred: int = 0
    errors: int = 0


class MaintenancePluginBase(abc.ABC):
    """Convenient base class for internal maintenance plugins."""

    name = "maintenance-plugin"
    priority = 0
    enabled_by_default = True

    def startup(self, context: MaintenancePluginContext) -> None:
        return None

    def shutdown(self, context: MaintenancePluginContext) -> None:
        return None

    def wants_event(self, event: MaintenanceEvent) -> bool:
        return True

    def coalesce_key(self, event: MaintenanceEvent) -> object | None:
        return None

    @abc.abstractmethod
    def handle_events(
        self,
        context: MaintenancePluginContext,
        events: Iterable[MaintenanceEvent],
    ) -> MaintenancePluginResult:
        raise NotImplementedError
