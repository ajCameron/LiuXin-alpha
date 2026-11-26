# PostgreSQL Catalog repair

The explicit [upgrade service](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/catalog_upgrade.py)
repairs existing native revision-1 catalogues created before the metadata cache
ergonomics fixes. It adds the nullable `series_parent_position` column and changes
the generated typed many-to-many identity and priority constraints to include
the relationship type. An Agent can then hold author and editor credits for the
same Work. Fresh catalogues already have this shape.

This is a targeted PostgreSQL repair. It does not import legacy custom-column
formats, adopt unversioned catalogues, or run automatically during admission.

## Preview and apply

Set `LIUXIN_POSTGRES_URL` in the environment. Use the explicitly selected schema:

```sh
PYTHONPATH=src .venv/bin/python scripts/upgrade_postgres_catalog.py \
  --schema my_catalog > /tmp/catalog-upgrade.json

# Review statements and blockers in the saved JSON before applying.
PYTHONPATH=src .venv/bin/python scripts/upgrade_postgres_catalog.py \
  --schema my_catalog --apply-plan /tmp/catalog-upgrade.json
```

Preview uses a read transaction and includes a fingerprint of the catalogue UUID, database,
namespace and relevant schema definitions. A nonempty `blockers` list prevents
application. Unexpected constraints, extra unique indexes, incompatible Series
columns and foreign keys depending on the old identity require a separate
review of that customization. The repair never drops objects with `CASCADE`.

Application owns one serialized outer transaction and locks the affected tables
in a stable order before reinspection. It derives its DDL again and rejects a
stale or edited plan. All DDL succeeds together or rolls back together. Rows,
IDs, existing role values, priorities, foreign keys and triggers are preserved;
existing Series rows receive a null sibling position. No explicit sibling order
is invented. Re-previewing an upgraded schema produces an empty successful plan.
The driver's primary connection must be idle; pending caller-owned work is
rejected before lock acquisition and is never implicitly committed or discarded.

Run during a maintenance window: exclusive table locks wait for active users and
block new access during the repair. Stop application cache users first and
reopen or reload their caches afterwards. The service refreshes its own Database
schema discovery after commit. A `CommittedTransactionError` from that refresh
reports committed DDL; inspect a fresh plan rather than treating it as rollback.

Live regressions use only disposable PostgreSQL schemas in
[test_catalog_upgrade_postgres_live.py](../../tests/databases/test_catalog_upgrade_postgres_live.py).
The ordinary test suite skips them without `LIUXIN_TEST_POSTGRES_URL`.
