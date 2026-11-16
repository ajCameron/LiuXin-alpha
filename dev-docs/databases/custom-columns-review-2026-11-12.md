# Custom-column service correctness review

Source links now point to the current implementation after removal of native-package
compatibility shims; labels retain the original filenames for these historical findings.

Reviewed **2026-11-12** at `a88a5230e819c8de40518228b34e70584283f4cb` on
`main`. That commit contains the preceding database-facade repairs and notes.
This review covers `databases.custom_columns`, its manager, and the storage,
macro, field-metadata and cache boundaries used by its public methods.

**Original verdict: not ready for correctness signoff.** Six P2 findings reproduce on
both `SQLite` and `SQLite_apsw`. The cached-write rollback boundary is much
stronger than the definition/trigger lifecycle and generalized-table support.
The original pass changed review documentation only. The subsequent authorized
repair implements all six findings; see the repair follow-up below and the
[current service contract](custom-columns.md).

## CC01 — Generated cleanup triggers reference a nonexistent owner column

Location: [custom_columns.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/facade.py),
`refresh_db_custom_columns_metadata`, lines 448–455.

The loader passes `plural_singular_mapper(in_table)` as the complete search
column to `get_foreign_key_replacement_trigger`. For a Works column this emits:

```sql
DELETE FROM works_custom_column_1_link WHERE work=OLD.work_id;
```

The generated storage column is `works_custom_column_1_link_book`, not `work`.
After constructing a facade for an existing column, deleting a Work on that
same connection raises `no such column: work`, even if no custom values are
attached. The parent remains present. The same mismatch exists for the
unnormalized storage naming and for legacy Books attachment names.

Use the actual generated owner-column name for the target storage table.
Regression coverage must execute a parent DELETE, not merely inspect whether
the TEMP trigger was created.

## CC02 — Owner deletion can leave custom links orphaned

Location: [custom_columns.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/facade.py),
constructor lines 313–335; and
[cc_crud_columns_mixin.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/adapter.py),
line 61.

There are two gaps in the cleanup lifecycle. Creation refreshes trigger SQL
snippets but never installs them. More generally, the constructor installs
cleanup only as a TEMP trigger on `db.driver.conn`; an ordinary
`db.macros.delete_rows_by_id` uses its transaction connection and does not see
that trigger.

The reproduction creates a Work, creates a normalized custom column, inserts
a value and a valid link, then deletes the Work through the portable macro.
An independent reader sees the Work gone and its link still present. This
happens both immediately after column creation and after reconstructing the
facade to install the trigger. Fixing CC01 alone therefore does not restore
referential integrity.

Cleanup must cover the connections actually used for supported owner deletes,
and the installed definition must follow column creation/deletion. Choose a
coherent schema or connection lifecycle solution and test stored links after
parent deletion on each supported path.

## CC03 — Constructing the service commits unrelated caller work

Location: [custom_columns.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/facade.py),
constructor lines 318–321.

When definitions exist, initialization enters the borrowed connection context
and invokes `driver_wrapper.execute` to drop/create triggers. Those execution
paths complete the primary connection's current transaction.

Reproduction: create a column; `BEGIN` on `db.driver.conn`; insert an unrelated
row; construct `CustomColumns(db, table="works")`; call `rollback()`. The row
is already visible from an independent connection and survives rollback. The
constructor has committed work it does not own. The same entry point is used
by lazy manager loading.

Use transaction-preserving trigger installation, or reject an active caller
transaction before any initialization effects. Verify this with independent
durability reads on both SQLite drivers.

## CC04 — Definition changes leave FieldMetadata stale

Location: [cc_crud_columns_mixin.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/adapter.py),
lines 61 and 93; and
[custom_columns.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/facade.py),
lines 457–461.

Creation and metadata edits refresh the label/number maps, but custom-field
registration only runs in the constructor. A newly created `review` column
is absent from `field_metadata`. After loading that column and renaming it to
`renamed`, the definition maps show the new name while FieldMetadata still
contains `#review` and lacks `#renamed`. Editability and display descriptors
can similarly retain old values.

This is observable within the service: `custom_dirty_books_referencing`
looks up the new field key in FieldMetadata and raises `KeyError`, blocking
the rename/delete-value paths that depend on it. Manager refresh calls the
same incomplete refresh method.

Reconcile field descriptors and aliases alongside the definition maps,
including removal of old keys and handling of Series companion fields.

## CC05 — Generalized facades still require the legacy Books projection

Location: [cc_set_methods_mixin.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/values.py),
lines 482–485; and
[custom_columns.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/facade.py),
lines 739–758 and 919–920.

`CustomColumns` accepts Works and other FRBR tables and maps its default Books
attachment to Manifestations. Nevertheless, the cached writers unconditionally
read back from `meta2`. A newly generated FRBR database has no such relation.
With a valid Work, a freshly created text column, a supplied cache and a correct
field map, `set_custom(owner, "New", num=column)` raises
`no such table: meta2`. The transaction correctly restores SQL and cache, but
the requested write cannot succeed.

The service's own projection builder hardcodes `books.book_id`, and owner
dirtying uses a macro deriving `books_<custom table>_link`. Supplying a cache
does not resolve those table assumptions. Existing transaction tests build an
artificial Books-shaped link table and `meta2`, so they do not exercise this
normal FRBR service configuration.

Use attachment-aware reads and owner resolution for the generalized service,
with an end-to-end create/set/read/delete contract on generated FRBR storage.
Any retained legacy projection dependency should belong to an explicitly
separate compatibility contract.

## CC06 — Enumeration rename does not persist its new allowed values

Location: [custom_columns.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/facade.py),
lines 570–573.

`rename_custom_item` renames the stored value and changes
`data["display"]["enum_values"]` in memory, but does not update
`custom_columns.custom_column_display`.

Using the existing legacy SQL/cache fixture with the real enum adapter, rename
`old` to `new`: SQL values and the in-memory allowed list contain `new`, while
an independent read of the definition still allows only `old`. Reloading that
persisted display definition makes `set_custom(..., "new")` raise
`InvalidUpdate`. A successful operation has left its stored values outside its
stored domain.

Persist the revised choices in the same transaction as the value rename and
retain rollback restoration of the shared metadata. Cover rename, merge into
an existing choice, reload, and late failure.

## Validation and limits

- **215 existing focused tests passed**, covering service contracts, cached
  value transactions, generated custom storage, definition refresh safety,
  fixture construction and facade bookkeeping.
- **34 additional existing compatibility/cache tests passed; 1 module skipped**.
  The skipped bootstrap module requires explicit legacy CalibreCache opt-in.
- **16 review probes passed** across SQLite and APSW. Fourteen assert the six
  observed defects (including owner deletion with and without reconstruction);
  two confirm that clearing multiple text with `None` works correctly. These
  are reproduction probes, not passing regressions for repaired behavior.
- **16 documentation/navigation checks passed**, using a disposable Git index
  and object directory to include the new review without staging it in the
  checkout. The real Git index remains empty.
- The initial probe harness used a misspelled macro method and an incorrect
  hypothesis about clearing multiple text. Both were corrected. A second run
  established that macro deletes bypass the TEMP trigger; the final run checks
  direct-connection failure and macro orphaning separately.
- No PostgreSQL run or repository-wide CI was performed for this review. The
  reviewed compatibility DDL is SQLite-specific. No source fixes were made.

Local evidence is in `.tmp/custom-columns-review-2026-11-12/`:
`baseline.log`, `cache-baseline.log`, `probes.log`, and
`test_review_probes.py`. The probes use disposable generated databases and the
existing legacy cache fixture; production catalogues and the data submodule
were not changed. The logs and reproduction script are ignored local evidence.

The two ordinary test commands selected:

```text
tests/databases/test_custom_column_service_contracts.py
tests/databases/test_custom_value_transactions.py
tests/databases/database/database_contract/test_db_custom_columns_calibre_style.py
tests/databases/database/database_contract/test_custom_column_refresh_safety.py
tests/databases/database/database_contract/test_custom_column_fixture.py
tests/databases/database/database_contract/test_custom_column_facade_writes.py
tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py

tests/databases/test_legacy_custom_value_compatibility.py
tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py
tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py
tests/databases/database_calibre_emultation/test_calibre_emulation_d1_custom_columns_introspection.py
tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py
```

Both used `.venv/bin/python -m pytest -q -n 2` with separate disposable
`--basetemp` directories. The final probe command used `-q -rA -n 2` against
the ignored reproduction file.


## Repair follow-up on 2026-11-12

The user authorized repairs for all six findings and explicitly chose direct
read/write support without a Calibre results cache, retaining cache integration.
All six now have implementations and real-engine regressions on both SQLite
driver names. The [service contract](custom-columns.md) records the resulting
ownership and migration rules.

| Finding | Implemented behavior |
| --- | --- |
| CC01 | Cleanup uses the generated storage owner reference and attachment primary key. |
| CC02 | Each column owns persistent schema cleanup, installed during creation, reconciled on load/refresh and removed during purge. Macro and independent-connection deletes are covered. |
| CC03 | Loading/refresh use owned transactions or borrowed savepoints, preserving unrelated pending primary and macro work. |
| CC04 | Field registrations are prepared before SQL commit, then published into the shared FieldMetadata object. Labels, search aliases, Series companions and result slots follow the stable column ID. |
| CC05 | Value reads and cache refresh query attachment-specific storage. Direct persistent-ID use requires neither a cache nor `meta2`; dirtying and compatibility projections use the actual attachment. |
| CC06 | Enum rename/merge persists allowed choices with SQL values/links. A failed write restores SQL, cache and the shared choices list. |

Integration exposed two adjacent defects: native boolean inputs reached a
shadowed `datetime` reference, and the generated link-retarget integrity trigger
watched a nonexistent `value` column. Native nullable boolean adaptation and
newly generated link UPDATE guards are repaired and tested. Existing stored
UPDATE trigger bodies are not rewritten by the owner-cleanup migration.

Validation:

- Broad custom-column, driver-wrapper, driver-rebinding, cache, Catalog and
  adapter integration: **585 passed, one explicit legacy module skip**.
- Final lifecycle/transaction/definition contracts after the creation guard:
  **121 passed**, including **106 new mandatory database cases**. Each new
  case passed setup, call and teardown. The manifest retains all 1,622 earlier
  requirements, for **1,728 total**; the entire manifest was not rerun.
- The full local quality script passed, including both type checkers, valid/
  invalid static contracts, lint, formatting and architecture/complexity checks.
- **79 documentation/tooling checks passed** using a disposable Git index and
  object store; the real index remains empty. The final cache-access diagnostic
  rerun passed all 11 service contracts.
- No PostgreSQL custom-storage run or hosted CI is claimed. The compatibility
  DDL remains SQLite-specific. Python 3.12 reports existing datetime
  adapter/converter deprecations during the runtime suite.

Evidence lives in `.tmp/custom-columns-repairs-2026-11-12/`: `comprehensive.log`
and JSON, `final-contracts.log` and JSON, and quality logs. Earlier failed runs
retain harness corrections and the boolean-adapter discovery; use final logs
for the result. The new regression source is
[test_custom_column_repairs.py](../../tests/databases/test_custom_column_repairs.py).

The fixes and notes are local and uncommitted. No production database or data
submodule was changed. Older columns gain cleanup when their attachment loads;
existing orphan rows require a separate repair. Deferred deletion and existing
multiple-Series/composite compatibility limits are retained as documented.
