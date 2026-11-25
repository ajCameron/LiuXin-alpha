"""Preview or explicitly apply the native PostgreSQL Catalog repair.

Set LIUXIN_POSTGRES_URL to the connection URL. Preview writes JSON to stdout;
save it to a file and pass --apply-plan to apply that exact reviewed shape.
No upgrade is performed merely by opening a catalogue.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from LiuXin_alpha.databases.database import Database  # noqa: E402
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.catalog_upgrade import (  # noqa: E402
    CatalogSchemaUpgradePlan,
    apply_catalog_schema_upgrade,
    plan_catalog_schema_upgrade,
)


def main(argv: list[str] | None = None) -> int:
    """Open an existing catalogue and preview unless a saved plan was supplied."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--apply-plan", type=Path)
    args = parser.parse_args(argv)
    url = os.environ.get("LIUXIN_POSTGRES_URL", "").strip()
    if not url:
        parser.error("Set LIUXIN_POSTGRES_URL to the existing PostgreSQL database")
    with Database(
        metadata={"postgres_url": url, "schema": args.schema},
        db_type="PostgreSQL",
        create=False,
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as database:
        if args.apply_plan is None:
            plan = plan_catalog_schema_upgrade(database)
        else:
            value = json.loads(args.apply_plan.read_text(encoding="utf-8"))
            plan = CatalogSchemaUpgradePlan(
                schema=value["schema"],
                fingerprint=value["fingerprint"],
                statements=tuple(value["statements"]),
                blockers=tuple(value["blockers"]),
            )
            apply_catalog_schema_upgrade(database, plan)
        print(json.dumps(asdict(plan), indent=2))
        return int(bool(plan.blockers))


if __name__ == "__main__":
    raise SystemExit(main())
