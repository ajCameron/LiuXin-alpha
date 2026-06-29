"""
Small internal plugin primitives for database maintenance work.

Plugins - intended to maintain the database can be registered here for the runner.
"""

from __future__ import annotations

import abc
import dataclasses

from typing import TYPE_CHECKING, Iterable

from LiuXin_alpha.databases.maintenance.events import MaintenanceEvent

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI


@dataclasses.dataclass(slots=True)
class MaintenancePluginContext:
    """
    Context the plugin should operate in - gathered here in one place.

    Currently, contains
     - the database the plugin is expected to operate on
     - the logger the plugin is expected to use to record it's operation
    """
    db: "DatabaseAPI"
    logger: object | None = None


@dataclasses.dataclass(slots=True)
class MaintenancePluginResult:
    """
    The result of running the plugin.

    Intended to provide telemetry.
    """
    handled: int = 0
    deferred: int = 0
    errors: int = 0


class MaintenancePluginBase(abc.ABC):
    """
    Convenient base class for internal maintenance plugins.
    """

    name = "maintenance-plugin"
    priority = 0
    enabled_by_default = True

    def startup(self, context: MaintenancePluginContext) -> None:
        """
        Called when it's time to start up the plugin.

        :param context:
        :return:
        """
        return None

    def shutdown(self, context: MaintenancePluginContext) -> None:
        """
        Called when it's time to stop the plugin.

        :param context:
        :return:
        """
        return None

    def wants_event(self, event: MaintenanceEvent) -> bool:
        """
        Events this plugin should respond to.

        :param event:
        :return:
        """
        return True

    def coalesce_key(self, event: MaintenanceEvent) -> object | None:
        """
        Not... sure.

        :param event:
        :return:
        """
        return None

    @abc.abstractmethod
    def handle_events(
        self,
        context: MaintenancePluginContext,
        events: Iterable[MaintenanceEvent],
    ) -> MaintenancePluginResult:
        """
        Run the plugin against the given events.

        Preforms a run with the given context against the given events.
        :param context:
        :param events:
        :return:
        """
        raise NotImplementedError
