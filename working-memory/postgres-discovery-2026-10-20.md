# PostgreSQL discovery checkpoint, 2026-10-20

Focused prerequisite for A08, on `postgres-link-discovery`, based on A07
`f733f76b` / PR #135. The full A01–A16 programme remains open; no PR is merged.

A08's live tests reproduced DB06 in ordinary schema-based writers, not only
caches: PostgreSQL clips a generated link-column name while the wrapper expected
the full spelling. The PostgreSQL driver now asks the server to resolve the name
as `pg_catalog.name`; the wrapper still requires an actual discovered heading.
No stored names or schema objects are changed.

Full-cache validation then exposed a swallowed SQLite `PRAGMA` error on the
PostgreSQL shared connection. Both schema-backed and NumPy caches now use the
existing backend unique-index metadata owner. They fall back to observed rows
only when the host lacks that capability; backend failures propagate.

The initial discovery regression reached the public Catalog write but its next
read failed because the cache probe had left the connection aborted. After that
correction, **81 tests passed in 147.55s**, including two live PostgreSQL cases
covering full schema discovery, both caches, public schema writes, missing names,
Unicode byte clipping and DDL collision rollback. Existing PostgreSQL, schema
and cache regressions were included. Logs:
`/tmp/liuxin-postgres-discovery-{live,verified}.log`.

The final explicit `pg_catalog.name` qualifier, real SQLite/APSW unique-index
controls and documentation links passed **21 checks in 18.61s**. The full quality
runner passed: 192 formatted files, 244 protected import modules, 1,801 layer
modules, 25 import budgets, lint/complexity, both type checkers and both 38-example
negative contracts; mypy checked 199 files. Selections overlap and are not an
aggregate count. Publication is next.

The isolated checkout is `/tmp/liuxin-postgres-discovery`; its
explicit file manifest is `/tmp/liuxin-postgres-discovery-files.json`.

Main remains `catalog-rating-validation` with uncommitted A08 work and the six
unrelated staged storage files. Preserve those files using
`/tmp/liuxin-docs-pr-preserved-storage.json`; only commit the prerequisite manifest.
After publishing this prerequisite, advance A08 onto it and finish the rating
selection. The retained Library importer still requires SQLite's compatibility
views, which PostgreSQL does not provide.

Parent CI is green: #134 at `13d6e5aa` passed **7,476 tests, 269 skipped,
17 xfailed**; #135 at `f733f76b` passed **7,487 tests, 269 skipped, 17 xfailed**.
Both PR bodies have those exact final CI results.
