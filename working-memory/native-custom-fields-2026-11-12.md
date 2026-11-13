# Native custom-field implementation — 2026-11-12

The user approved the full native custom-column plan and requested implementation.
The earlier six repairs remain included and uncommitted. Preserve the existing
dirty `LiuXin_alpha_data` submodule and the real Git index. No commit, push, PR,
or production catalogue upgrade is authorized by this implementation request.

## Scope implemented

Native typed identities and ordered scalar/set/list values for every stored
datatype; per-entry Series positions; portable SQLite/SQLite_apsw/PostgreSQL
storage; shared vocabulary edits; atomic mixed patches and entry editing;
managed macro commit/savepoint publication; scoped metadata and categories;
batched reads, inventory, predicates, pagination and usage; explicit previews,
upgrades, field/attachment conversions, retirement, purge, audits and repairs;
Calibre template evaluation in Catalog with explicit relationship paths;
lossless legacy adapters and completed wrapper hooks; Core native endpoints.

Read [the native contract](../dev-docs/databases/native-custom-fields.md) and
[the legacy contract](../dev-docs/databases/custom-columns.md). The new modules
are `databases/custom_columns/native_*`, `databases/transaction_participants.py`,
`catalog/custom_fields.py`, and the native Core service/provider modules.

## Verification completed

Evidence is in `.tmp/custom-fields-native-2026-11-12/`. The saved results are
newer than this note's original in-progress handoff:

- The complete regression run passed **1,971 tests**, with two cleanup
  connection-lifetime failures subsequently repaired. Its historical failure
  remains visible in `ratchet-full.log`.
- The final targeted gate passed **145/145** cases through setup, call and
  teardown: all **134 native cases** across SQLite, APSW and PostgreSQL, plus
  all eleven cleanup/service contracts. See `native-gate-final.{log,json}`.
- `verified-regressions.json` records **1,862/1,862 required cases** verified
  across the full run and corrective reruns, with no unresolved failures.
  This is combined evidence, not a single green full-suite run. The manifest
  retains the preceding 1,728 requirements (including all 1,622 at HEAD).
- `quality-verified.log` records the completed full quality runner: formatting,
  annotations, architecture, import budgets, lint and complexity passed;
  basedpyright reports zero errors; mypy covers 476 source files. Static
  contracts reject 95/93 invalid examples respectively.
- Core runtime, composite and PostgreSQL audit follow-ups also passed; their
  logs are retained. Earlier broad-run failures and tooling/environment errors
  are historical and do not describe the final source.

## Windows crash recovery

On restart, all **63** saved SHA-256 fingerprints matched the recovered files,
covering every changed source, test, configuration and documentation file;
the dirty data submodule is intentionally separate. The manifest's baseline
requirements and all 134 native cases were checked against the saved final
gate. No implementation repair was needed.

Fresh recovery checks use `PYTHONPATH="$PWD/src" .venv/bin/python -m pytest`
so subprocesses import this checkout rather than another virtualenv's editable
installation. Results and commands are retained under the evidence directory's
`recovery/` folder, with JSON reports and completion markers:

- Native SQLite/APSW, cleanup/service, Core service contracts and the earlier
  failing subprocess case: **127 passed** (`contracts.*`).
- Regression-runner, quality-runner, formatting-runner, docstring migration and
  public documentation checks: **84 passed** (`tooling.*`).
- Developer documentation navigation and links: **16 passed** (`links.*`),
  using a disposable Git index and object store to include new files without
  staging them. Total fresh recovery checks: **227 passed**.

`git diff --check` passes. Only this handoff and its index changed during
recovery; the original Git index hash and dirty data-submodule status are
unchanged. `recovery/reconciliation.json` records the recovery comparison.

The disposable PostgreSQL cluster and original full-run scratch directory under
`/tmp` are absent after restart. PostgreSQL was not restarted or rerun during
recovery; its passing evidence above is from before the crash and matches the
recovered files. Preserve the persistent evidence folder. Hosted CI and a new
single green full-suite run have not been performed.

The working-memory index now points to this completed native implementation.
Changes remain uncommitted on `main` at `a88a5230`; preserve the real index and
dirty data submodule. Publication or production upgrades need a separate user
request.
