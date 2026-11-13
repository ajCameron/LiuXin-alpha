# Custom-column native and compatibility follow-up review

Source links now point to the current implementation after removal of native-package
compatibility shims; labels retain the original filenames for these historical findings.

Reviewed **2026-11-12** on `main` at
`a88a5230e819c8de40518228b34e70584283f4cb`, including the uncommitted native
implementation and the six earlier repairs. This pass covers native storage,
queries, migrations, retirement, audit, transaction publication, Catalog
composites, and the legacy facade/wrapper/Core paths that reach that storage.
See the [native contract](native-custom-fields.md),
[legacy contract](custom-columns.md), and [earlier review](custom-columns-review-2026-11-12.md).

**All six findings are repaired and locally validated.** The
user authorized fixes after this review. The original findings below describe
the reviewed snapshot (one P1 and five P2), reproduced on both SQLite drivers.
The original 296 passing tests and 18 defect probes did not cover repaired
behavior. See the repair section below for the subsequent implementation.

## CC07 — P1: Native retirement can become an automatic legacy purge

Location: [native_service.py](../../src/LiuXin_alpha/databases/custom_columns/native_service.py),
`retire_field`, lines 386–397.

`retire_field` resolves version-one definitions and sets
`custom_column_mark_for_delete` without requiring a native storage upgrade.
For version one, that flag means deferred physical deletion to the legacy
loader. Calling `CustomColumns(db, table="works")` subsequently deletes the
definition, value table and memberships. Native retirement therefore performs
a destructive purge indirectly, without a purge plan or explicit application.

Reproduction: create and populate a legacy Work text field; retire it through
`CustomFieldService`; verify that the retired definition and values still
exist; construct the legacy facade. Independent queries then show both the
definition and its value table are gone. Version-two retirement is separately
covered by the existing passing preservation test.

Require an explicit upgrade before retiring a version-one field, or introduce
a retirement state that the legacy purge cannot interpret as deletion. Do not
silently upgrade as part of retirement. Cover both preserved values and the
absence of physical deletion after every supported loader/refresh path.

## CC08 — P2: Wrapper and existing Core edits bypass native definition validation

Location: [driver_wrapper_custom_columns_mixin.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/adapter.py),
`set_custom_column_metadata`, lines 360–368; exposed through
[schema.py](../../src/LiuXin_alpha/core/program_services/schema.py), lines 353–359.

The wrapper still sends every definition edit directly to the legacy metadata
macro, including storage-version-two fields. The existing `custom-fields.update`
Core endpoint falls back to that wrapper on the real Database facade. These
paths skip native migration validation and revision updates.

Reproduction: create a native enumeration permitting `Ready`, and store that
value. Native migration preview correctly reports an issue when asked to replace
the choices with only `Done`, and native application refuses it. The wrapper
and public Core command both accept the same edit; Core returns `ok=True` and
`updated=True`. The field revision remains unchanged, while reading its stored
value now raises `CustomFieldError: 'Ready' is not an allowed enumeration choice`.

Dispatch native definitions through the native validation/migration owner at
the wrapper boundary as well as the higher-level `CustomColumns` facade. The
existing Core endpoint must not provide a route around these invariants. Retain
the version-one contract and test invalid enumeration edits through both routes.

## CC09 — P2: Upgraded facade metadata edits publish before the outer commit

Location: [cc_crud_columns_mixin.py](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/adapter.py),
`set_custom_column_metadata`, lines 129–139.

The version-two branch executes before the legacy transaction-ownership check.
It joins an active macro transaction, applies the migration in a savepoint,
refreshes the facade maps and immediately notifies listeners. Rolling back the
outer transaction restores SQL but cannot restore these published projections.

Reproduction: within `db.macros.transaction()`, rename native field `review` to
`changed` through the legacy facade, then raise an exception. After rollback,
the database still calls the field `review`, the facade only recognizes
`changed`, and its notification was already delivered.

Apply the documented legacy ownership refusal before selecting the storage
version, or fully defer facade publication and notification to the owning
commit. The current behavior satisfies neither contract. Include outer rollback
and successful outer completion in the regression coverage.

## CC10 — P2: Incompatible legacy edits commit before being rejected

Location: [compatibility value adapter](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/values.py) (originally `native_compatibility.py`, removed when v1 support was retired),
bulk-add and rename dispatch at lines 227–247; post-commit projection at lines
128–154.

The compatibility adapter rejects delimiter-bearing existing text-set values,
but validates incoming values only in the ordinary set/bulk-set branch.
`rename_custom_item(..., "a|b")` and
`set_custom_bulk_multiple(..., add=["a|b"])` reach the native writer without
the corresponding check. Publication discovers the incompatible representation
after SQL has committed.

Both reproductions raise `ExceptionGroup: Committed transaction publication
failed`, while an independent native read confirms `a|b` was stored. Subsequent
legacy reads raise `CompatibilityError`. This occurs without an optional cache,
so it is not limited to a failing cache callback.

Validate the prospective compatibility representation on every mutation path
before committing. A legacy operation that cannot represent its own result
must leave the field, inventory and memberships unchanged. Keep delimiter-bearing
values supported through the native API.

## CC11 — P2: Upgraded bulk removal loses case-insensitive identity matching

Location: [compatibility value adapter](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/values.py) (originally `native_compatibility.py`, removed when v1 support was retired),
lines 230–236.

The bulk text-set adapter compares each stored spelling directly against the
raw removal strings. It neither normalizes removals like the legacy worker nor
compares native identity keys. A successful call to remove `READY` therefore
leaves the stored `Ready` membership untouched.

The reproduction includes an untouched version-one control: the same facade
operation removes `Ready` there, while the native field retains it. An upgrade
thus changes an established public operation into a silent no-op for ordinary
case variants.

Normalize removal inputs and compare the field's canonical identities, keeping
the documented precedence for additions. Cover case variants and the native
Unicode normalization policy, not only exact spelling.

## CC12 — P2: Cleanup audit can report an unsafe native schema as healthy

Location: [native_migrations.py](../../src/LiuXin_alpha/databases/custom_columns/native_migrations.py),
`audit`, lines 239–255. Compare the objects emitted by
[native_repository.py](../../src/LiuXin_alpha/databases/custom_columns/native_repository.py),
`install_cleanup`, lines 337–370.

SQLite auditing checks only the owner-delete trigger, and accepts its SQL when
it contains two quoted column names. It ignores the vocabulary-delete trigger
and insertion/update reference guards. Dropping those triggers yields no issue.
Replacing the owner trigger with an ineffective `DELETE ... AND 0` also passes
the two-substring test.

Both damage variants return an empty audit and an empty default repair-effects
preview. A subsequent supported independent-connection deletion with
`foreign_keys=OFF` leaves an orphan membership. The later audit detects the
orphan, but the preflight inspection failed to report the broken protection.

Validate every required native cleanup/reference trigger and its effective
definition. Missing or stale protections should appear in the preview before
destructive operations expose the fault. The existing explicit repair can
reinstall these objects; the discovery and effects reporting are incomplete.

## Original review verification and scope limits

Persistent evidence is in `.tmp/custom-columns-followup-review-2026-11-12/`:

- `existing.{log,json}`: **296 passed**, covering native SQLite/APSW contracts,
  earlier repairs, value transactions, service contracts and legacy compatibility.
- `test_review_probes.py` and `probes-verified.{log,json}`: **18 passed**, with
  setup/call/teardown passing on both SQLite drivers. Passing assertions here
  demonstrate defects; they are not repaired-behavior tests.
- `documentation.{log,json}`: **16 passed**, using a disposable Git index and
  object store to check the new documents without staging them.
- `*.done.json`: exact commands, completion times and exit codes.
- `baseline-fingerprints.json` and `baseline-state.json`: the reviewed source
  snapshot, real Git index hash and pre-existing data-submodule status.

The final probes include version-one removal and native migration-refusal
controls, plus actual Core dispatch. An earlier Core probe used an empty
preference store and failed during unrelated presentation initialization;
`probes-final.log` preserves that harness failure. The verified run uses the
normal isolated test preferences and the command returns success.

No fresh PostgreSQL, hosted CI or complete regression-ratchet run was performed.
Earlier quality and regression evidence remains in the
[implementation handoff](../../working-memory/native-custom-fields-2026-11-12.md).
The implementation and test sources remain unchanged by this review.
`git diff --check` passes; the real Git index hash and dirty data-submodule
status match the captured baseline. All review checks have finished.

## Authorized repairs

The follow-up repair preserves version-one behavior while enforcing native
invariants at public compatibility boundaries:

| Finding | Implemented behavior | Regression evidence |
| --- | --- | --- |
| CC07 | Retirement requires an explicit storage upgrade before writing the legacy deletion flag. | Real legacy fields, old schemas without native metadata columns, Core dispatch, reload/refresh preservation, explicit upgrade/retire/purge. |
| CC08 | Wrapper and existing Core metadata edits use native migration validation and revision updates. | Invalid enum/dependency edits leave data unchanged; valid edits advance revisions; wrapper commit/rollback on managed transactions. |
| CC09 | Facade metadata edits and native retirement refuse borrowed transactions before publishing. Dispatch checks the current storage version after upgrades. | Primary/macro transactions with both rollback and commit; unchanged maps and notifications; retained-facade upgrade case. |
| CC10 | Every incoming compatibility text-set addition or rename checks delimiters before mutation. | Set, append, bulk-set, bulk-add and used/unused value renames, with and without a cache; unchanged SQL fingerprints; native/scalar positive controls. |
| CC11 | Bulk removals clean input and use NFC/case-folded identities; additions take precedence. | ASCII case, decomposed accents, sharp-S folding, whitespace/comma cleanup and retained entry IDs. |
| CC12 | Audit compares all generated cleanup/reference triggers against the canonical definitions shared with installation. | Missing and ineffective owner/value cleanup and insertion/update guards; explicit repair; independent foreign-keys-off deletes and invalid writes. |

See [the SQLite/APSW regressions](../../tests/databases/test_custom_column_followup_repairs.py)
and [the PostgreSQL regressions](../../tests/databases/test_custom_column_followup_postgres_live.py).
All 128 added cases are mandatory; the manifest preserves all 1,862 prior cases
and now requires 1,990. Trigger comparison intentionally reports even equivalent
manual SQL rewrites for canonical reconciliation. Audit never repairs implicitly.

Repair evidence is in `.tmp/custom-columns-followup-fixes-2026-11-12/`.
Final validation on the preserved source snapshot:

- Complete database regression gate: **2,101 passed**, including **1,990/1,990
  mandatory cases**, zero skips/failures, and ten existing deprecation warnings.
- Full quality gate: formatting, lint, architecture, both type checkers and
  internal valid/invalid type contracts passed.
- Core and runner integration: **93 passed**. Documentation/navigation:
  **16 passed**, using a disposable Git index/object store.
- All **128 new cases** pass in the complete gate on SQLite, SQLite_apsw and
  the applicable PostgreSQL paths. All 1,862 prior requirements are retained.

The first broad run was interrupted after Unicode driver checks exposed an
ASCII-encoded temporary PostgreSQL database. All 80 affected checks passed
against the corrected UTF-8 database before the entire gate was restarted and
passed. Both attempts remain in the evidence; only the completed UTF-8 run is
the full-gate signoff. `ratchet.json`, `ratchet.log` and the archived telemetry
record its result. No production catalogue was migrated, and no commit, push
or PR was requested. The original Git index and dirty data submodule are
preserved.

Return to the [database index](README.md).
