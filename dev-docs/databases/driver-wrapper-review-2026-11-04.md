# Driver-wrapper follow-up review and typing

Reviewed **2026-11-04** against the working tree on `maintenance-plugin-modes`.
This follows the [first boundary repair](next_tasks.md#db08-correct-driver-wrapper-boundaries-and-keep-its-api-declarative).
It covers `databases.driver_wrapper`, its API, the driver hooks it calls and the
custom-column management paths. It is not a full database-module signoff.

Delivery update: the [combined maintenance/wrapper PR checkpoint](review-upgrade-delivery-2026-11-04.md)
supersedes the uncommitted status below; the dated validation remains historical.

## Reproduced findings and changes

| Priority | Finding | Result |
| --- | --- | --- |
| High | Renaming a WEMI custom column defaulted its attachment metadata to `books`, without moving its value/link storage. | Omitted fields now retain their stored values. An explicit different attachment raises before writing; reattachment requires a storage migration. |
| High | Custom-column metadata edits and deferred cleanup could commit unrelated pending primary writes; script-based cleanup could publish a partial removal. | Edits, deletion marking and cleanup use an owned transaction or a borrowed savepoint, joining the active macro connection when present. Cleanup selects and removes definitions/storage atomically, executes individual DDL statements and restricts definition deletion to the requested IDs. |
| Medium | View-heading inspection opened an unclosed auxiliary connection and missed pending view DDL visible to table discovery. | Heading inspection uses the primary connection when available, quotes identifiers and closes cursors on every path. A temporary connection is closed when the helper owns it. |
| Medium | Link-name cache invalidation looked for obsolete `_get_schema_version`, allowing cached misses to outlive new links. | It calls `direct_get_schema_version`; probe failures propagate instead of silently weakening cache validity. |
| Medium | Table specs looked for obsolete `_get_declared_types_for_table`, losing available type declarations and affinities. | They call `direct_get_declared_types_for_table`. SQLite type discovery sees pending DDL on the primary handle, cleans up owned resources, and caches only successful reads. |

The initial invariant probes failed in all fourteen SQLite/APSW cases; those
cases passed after the repairs. Permanent regressions also cover rejected
reattachment, active macro transactions, deletion marking, a late failed DROP,
selection-limited cleanup, quoted names, failed schema probes, temporary handle
cleanup and schema inspection after rollback.

## Typing contract

The entire wrapper package and its declaration-only API now participate in both
configured type checkers. The wrapper implementation has a shared abstract base
for cross-mixin state and methods; the public API still contains no executable
behavior. Accessing the driver after detachment raises an explicit error.

Annotations describe actual row dictionaries, identifier results, collections,
and the existing `False`/`None` absence conventions. SQL cells remain `Any`, while
their row and binding containers are typed. Portable `QueryConnection` and
`QueryCursor` protocols describe the SQL execution and lifetime subset; valid
contract examples verify SQLite and PostgreSQL adapters against it.

Accurate missing-result types exposed unchecked empty rows and absent parents;
these now raise explicit input errors before indexing. Raw `get(..., all=False)`
uses `fetchone()` and returns a complete row or `None`, including with the
PostgreSQL cursor adapter. Both raw fetch modes close their cursor.

The permanent negative contracts now contain 44 rejected examples, including
six wrapper cases: an invalid row ID, editability flag, display mapping and sort
flag, plus indexing an unchecked missing table/view row. Scope is the wrapper
and its API, not a claim that all database/backend implementations are typed.

## Validation

The final boundary/API/tooling selection passed **119 tests**. The broader
caller selection passed **137**, with three
platform/backend skips and four pre-existing expected failures. It covers row
writes, transaction ownership, custom-column facades and cache refresh, folder
trees, driver lifecycle and Catalog's row writer.

The live PostgreSQL wrapper, row-write and transaction selection passed **25
tests**, including the raw-query result contracts. The updated view-heading
doctests passed all nine examples. The test selections overlap; their counts are
not a unique-test total. Tests keep configuration, caches, scratch and telemetry
under `/tmp` and discard per-test scratch. No full-repository run is claimed.

Logs are `/tmp/liuxin-wrapper-signoff.log`,
`/tmp/liuxin-wrapper-integration.log` and `/tmp/liuxin-wrapper-typing-pg.log`.
All 105 local links in the five edited guides resolve, including new files
awaiting their eventual commit. Protected preference hashes remain unchanged.

The complete scoped quality runner passed: 290-file formatting, lint, complexity,
annotation coverage, import/layer/footprint checks, basedpyright and mypy. Both
contract probes accept valid calls and reject all 44 invalid examples. The final
API signature check passed six tests. The quality log is
`/tmp/liuxin-wrapper-quality-complete.log`. The private PostgreSQL server was
stopped and its cluster removed; its log is retained under `/tmp`.

Regression owners:

- [Wrapper boundaries](../../tests/databases/test_driver_wrapper_boundaries.py).
- [Earlier wrapper repairs](../../tests/databases/test_driver_wrapper_review.py).
- [Live PostgreSQL](../../tests/databases/test_driver_wrapper_review_postgres_live.py).
- [Static contracts](../../tests/typing/internal_contracts.py), checked by both
  tools in [the quality runner](../../scripts/run_type_checks.sh).

## Remaining work

The subsequent [feature implementation](driver-wrapper-features-2026-11-04.md)
supersedes three findings below; the earlier validation counts above belong to
the boundary/typing pass.

1. **Physical schema facts implemented.** Native column, primary-key/default,
   index and foreign-key facts now replace the nullable/conventional-ID guesses.
   Views retain unknown nullability; check constraints and arbitrary SQL are not
   a complete validation/migration model. Cached specs still need the documented
   refresh boundary after schema changes.
2. **Retire or implement remaining custom-column placeholders through callers.**
   `_get_custom_column_row` remains a no-op; `update_custom_column` still raises
   `NotImplementedError`. Their types now state this honestly. Compatibility DDL
   remains SQLite-specific, and PostgreSQL unsupported creation must continue
   rolling back its definition. Do not promise reattachment by metadata edit.
3. **View-row cleanup implemented; other SQL helpers remain.** The view reader
   borrows the explicit/active/primary connection and closes owned resources on
   query/conversion errors. Its compatibility contract still requires `id`.
   Link DDL, arbitrary scripts and some custom-column value helpers retain
   explicit commit points. Not every wrapper method joins a caller's transaction.
4. **Hierarchy cycle detection implemented.** Parent chains and descendant walks
   reject repeated IDs with `InputIntegrityError`. Traversal order is unspecified
   and a walk can yield earlier rows before encountering the malformed edge.

See [transaction ownership](transactions.md) and
[connection lifetime](<02 - Connection ownership and stale connections.md>) for
the supported boundaries. The monitored real SquashFS import still awaits
physical server access; these synthetic checks do not replace it.
