"""Own Work revision receipts and an eventually current expression-count projection.

The prompt receipt is the required invalidation record for this optional read
model. A cached summary is current only when its revision matches that receipt.
Metadata validation and normalized identities remain with their existing writers.
"""

from __future__ import annotations

import json

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginBase,
    MaintenancePluginResult,
)
from LiuXin_alpha.databases.maintenance.projection_sql import (
    count_work_expressions,
    projection_revision,
    projections_for_key,
    put_projection,
    work_exists,
)
from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime


def _work_ids(events):
    revisions = {}
    for event in events:
        identities = (
            {event.row_id}
            | {
                values["work_id"]
                for values in (event.before, event.after)
                if values and "work_id" in values
            }
            if event.table == "works"
            else {
                values["expression_work_link_work_id"]
                for values in (event.before, event.after)
                if values and values.get("expression_work_link_work_id") is not None
            }
        )
        for work_id in identities:
            revisions[work_id] = max(revisions.get(work_id, 0), event.change_id)
    return revisions


class WorkRevisionPlugin(MaintenancePluginBase):
    """Publish a required revision/tombstone before a subscribed write succeeds."""

    name = "catalog.work-revision"
    mode = MaintenanceMode.PROMPT
    tables = ("works", "expression_work_links")

    def handle_events(self, context, events):
        """Invalidate every old/new Work endpoint in this captured batch."""
        for work_id, revision in _work_ids(events).items():
            exists = work_exists(context.connection, work_id)
            put_projection(
                context.connection,
                self.name,
                str(work_id),
                json.dumps({"deleted": not exists}, sort_keys=True),
                revision,
            )
        return MaintenancePluginResult(handled=len(events))


class WorkSummaryPlugin(MaintenancePluginBase):
    """Rebuild expression counts from current physical links after commit."""

    name = "catalog.work-summary"
    tables = WorkRevisionPlugin.tables
    dependencies = (WorkRevisionPlugin.name,)

    def handle_events(self, context, events):
        """Store current counts or a deletion tombstone with the observed revision."""
        for work_id in _work_ids(events):
            receipt = projection_revision(
                context.connection, WorkRevisionPlugin.name, str(work_id)
            )
            if receipt is None:
                raise RuntimeError("Work summary requires its prompt revision receipt")
            exists = work_exists(context.connection, work_id)
            count = count_work_expressions(context.connection, work_id) if exists else 0
            put_projection(
                context.connection,
                self.name,
                str(work_id),
                json.dumps(
                    {"deleted": not exists, "expression_count": count}, sort_keys=True
                ),
                receipt,
            )
        return MaintenancePluginResult(handled=len(events))


def catalog_maintenance(database) -> MaintenanceRuntime:
    """Attach domain implementations without migrating, starting threads or writing."""
    existing = vars(database.driver).get("_maintenance_runtime")
    if existing is not None:
        return existing
    return MaintenanceRuntime(database, (WorkRevisionPlugin(), WorkSummaryPlugin()))


def rebuild_work_projections(database, *, limit=128, restart=False) -> dict:
    """Schedule one resumable page of Work receipts and expression-count summaries.

    Existing Works and deleted projection identities are covered without changing
    bibliographic rows. Prompt receipts commit with the checkpoint; the normal
    jobs worker owns lazy completion. Repeat until scheduling_complete, then drain
    or run the worker and inspect maintenance status. Restart begins a new scan.
    """
    from LiuXin_alpha.databases.maintenance.rebuild import (
        RebuildDefinition,
        rebuild_page,
    )

    return rebuild_page(
        catalog_maintenance(database),
        RebuildDefinition(
            name="catalog.work-projections",
            version="1",
            source_table="works",
            plugins=(WorkRevisionPlugin.name, WorkSummaryPlugin.name),
        ),
        limit=limit,
        restart=restart,
    )


def work_summary(database, work_id: int) -> dict:
    """Read a cached count with explicit freshness; never start repair from a query."""
    runtime = catalog_maintenance(database)
    connection = database.driver.get_connection()
    try:
        from LiuXin_alpha.databases.maintenance.schema import migration_status

        if not migration_status(connection, runtime.schema)["installed"]:
            return {"work_id": work_id, "current": False, "summary": None}
        values = dict(
            (row[0], (row[1], row[2]))
            for row in projections_for_key(
                connection, str(work_id), schema=runtime.schema
            )
        )
        receipt = values.get(WorkRevisionPlugin.name)
        summary = values.get(WorkSummaryPlugin.name)
        return {
            "work_id": work_id,
            "current": bool(receipt and summary and receipt[1] == summary[1]),
            "source_revision": receipt[1] if receipt else None,
            "summary_revision": summary[1] if summary else None,
            "summary": json.loads(summary[0]) if summary else None,
        }
    finally:
        connection.close()
