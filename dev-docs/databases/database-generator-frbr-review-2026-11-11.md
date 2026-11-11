# FRBR database generator review — 2026-11-11

Reviewed at `5304f34f55d7fb73d9921d72a2248818e4aa22eb`, the published driver
checkpoint. This is a bounded first review of `SQL/database_generator_frbr`, its
packaged schema resources, shared link SQL emitter and relevant Catalog callers.
The original checkout is older; the review uses the published branch's source.
The initial review made no implementation changes. The user subsequently
requested repairs and architectural improvements; the original reproductions
below describe the baseline, followed by the repair record.

**Initial assessment: not ready for correctness signoff.** Five defects were
reproduced. Two affect identifier integrity in ordinary generated databases;
three affect requested schema options or path constraints. All five now have
local repairs; validation and publication status are recorded below.

## FRBR01 — P1: paired BREAK parsing silently omits shipped schema objects

Owners: [table and trigger loaders](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/database_generator.py)
(`create_main_tables`, lines 1190–1230; `create_main_triggers`, lines 1262–1303),
and [identifier guard resource](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/trigger_sql/frbr_metadata_core_attributes/entity_identifiers_triggers.sql).

When any `-- BREAK` marker is present, the loaders execute only chunks ending at
every second marker. They never check or execute the final unmatched chunk.
The identifier guard file contains two complete CREATE TRIGGER statements and
only one trailing marker, so neither trigger is installed. Other resource files
also lose a trailing declaration. The build nevertheless returns successfully.

A declaration-to-sqlite_master audit of the packaged table/trigger resources
found four missing objects among 257 declared objects:

- `trg_entity_identifiers_validate_entity_ref`
- `trg_entity_identifiers_validate_entity_ref_upd`
- `trg_genres_parent_no_cycles_after_ins`
- `idx_expressions_language_id`

On the generated database, inserting a UUID for nonexistent work 999 succeeds,
an empty identifier value succeeds, and two primary identifiers for the same
work/scheme succeed. Foreign keys are enabled. As a control, executing the
bundled identifier script directly in the disposable database installs both
guards: all three invalid writes then fail with their intended diagnostics,
while a valid identifier succeeds.

Repair the loader/resource contract so nonempty SQL cannot disappear silently.
Prefer one validated statement-loading path over two copies of marker pairing;
preserve complete trigger bodies. Add resource-inventory and behavior checks,
including malformed/trailing marker cases. Existing placeholder-row workflows
must be considered when enabling the missing guards; simply switching them on
without integration coverage is insufficient.

## FRBR02 — P1: deleting an owner can transfer its identifiers to a new entity

Owners: [polymorphic identifier schema](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_attributes/entity_identifiers.sql),
[identifier guards](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/trigger_sql/frbr_metadata_core_attributes/entity_identifiers_triggers.sql),
and [repository deletion](../../src/LiuXin_alpha/catalog/repositories/base.py)
(`delete`, lines 690–715).

The polymorphic `(entity_type, entity_id)` reference has no foreign key and no
owner-deletion handling. The declared guards validate writes to identifiers,
not the lifetime of the referenced entity. Repository deletion delegates to
ordinary row deletion without cleaning these references.

Reproduced through public `Database` and `Catalog` APIs on **SQLite and
SQLite_apsw**, each using a fresh file:

1. Create a work, receiving ID 1, and attach a valid UUID.
2. Delete it with `catalog.works.delete(1)`; the identifier still exists.
3. Create an unrelated work; it receives ID 1.
4. An independent connection sees the old UUID joined to the unrelated work.

`PRAGMA foreign_key_check` remains empty because it cannot inspect this
polymorphic relationship. A separate native control installs the missing guards
from FRBR01 first and reproduces the same delete/reuse result: this is an
independent defect. Ordinary INTEGER PRIMARY KEY values may be reused after
deletion, as described in [SQLite's row-ID allocation documentation](https://www.sqlite.org/autoinc.html).

Choose an explicit delete policy (cascade, restrict, or deliberate detachment)
and enforce it at the structural boundary. Cover all five supported owner types,
key changes, transaction rollback, and independent committed readers. Preventing
automatic ID reuse alone would leave orphaned identifiers and would not complete
the referential-integrity repair. The observed public-API reproduction covers
Work deletion; the other entity types need follow-up regression coverage.

## FRBR03 — P2: valid intralink column selections produce invalid DDL

Owners: [intralink parser and creation](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/database_generator.py)
and [shared emitter](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/utility_mixins.py)
(lines 1322–1351).

The parser accepts `requested_cols = ["sequence_number"]` and
`requested_cols = ["is_required"]`. The emitter adds each as a standard integer
column, then adds it again as a bespoke TEXT column because neither suffix is in
the reserved-name set. Creating the self-link raises `sqlite3.OperationalError:
duplicate column name`. Requesting both fails too; `requested_cols = "all"`
works because it bypasses bespoke-column emission.

Reproduced through TOML parsing followed by `create_intralink_table`, not just a
string inspection. These selections are accepted configuration options; the
shipped intralink specification does not currently request them. Use one optional
column definition source and test each supported subset by executing its DDL.

## FRBR04 — P2: disabling redundant links creates them with weaker defaults

Owner: [interlink parser](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/database_generator.py)
(lines 1713–1722), followed by `apply_interlink_constraints_from_spec`.

The parser adds a pair to the returned set before checking an existing FK edge
and `allow_redundant_links = false`. The `continue` skips only its metadata.
The later builder still creates the table, using default many-to-many cardinality
and priority instead of the requested direction, columns and type enumeration.

A minimal schema with expressions referencing works demonstrates this using a
requested one-to-many typed link. With redundancy disabled, the pair remains in
the returned set, metadata is empty, and `expression_work_links` is created as
many-to-many without its type table. With redundancy enabled, the declared
one-to-many settings and `expression_work_links__types` are retained.

Filter excluded pairs before admitting them to the build plan. Add an end-to-end
assertion that a disallowed redundant table is absent, plus the allowed control.
This is a configurable-path defect; the ordinary shipped spec defaults to
allowing redundant links.

## FRBR05 — P2: folder path guards check two backslashes instead of one

Owner: [folder normalization triggers](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/trigger_sql/storage/2-folders_triggers.sql)
(lines 65–69 and 98–102).

Both INSERT and UPDATE guards use a SQL string containing two backslash
characters. SQLite does not treat a backslash as a C-style string escape; see
[literal syntax](https://www.sqlite.org/lang_expr.html#literal_values_constants).
The one-character prefix comparison therefore never detects a single leading
backslash, and INSTR only rejects doubled backslashes.

The generated database accepts `a\b` and `\root` on both insert and update,
despite the resource's explicit forward-slash/relative-path contract. Insert also
accepts `C:\root`. Controls accept `a/b` and reject `../b`, `a//b`, and a doubled
backslash. This review establishes a schema-validation bypass, not a demonstrated
filesystem escape; storage resolution has separate checks.

Use an unambiguous single-backslash SQL literal or character expression and
exercise both write paths with positive and negative cases.

## Validation and limits

- Focused existing suite: **63 passed, one skipped**, 147.98 seconds. The skip is
  the conformance test requiring a shipped `requested_cols = "all"` entry;
  none currently exists. This review's explicit all-columns control executed.
- Fresh complete schema: foreign keys enabled, `foreign_key_check=[]`,
  `integrity_check=ok`; all **22 views** prepared successfully.
- Default-insert probes against **207 generated tables** found no operational
  missing-object/column errors. Expected semantic constraint errors were allowed;
  this is not complete positive/negative DML coverage.
- **257** packaged table/trigger object declarations compared with sqlite_master;
  the four omissions in FRBR01 were confirmed individually.
- Catalog Work/UUID deletion/reuse reproduced on both registered SQLite drivers
  with independent committed readers. Additional native controls distinguish
  missing guards from absent owner-lifecycle enforcement.
- No PostgreSQL server or full repository suite was run for this review.
  PostgreSQL's separate schema translation is outside this signoff.
- Documentation navigation and public-boundary checks: **22 passed**, 58.80 s.
- Production source and tests are unchanged; disposable databases are removed.

The focused suite command was:

```bash
python -m pytest -q tests/databases/test_frbr_*.py \
  tests/storage/backup/test_frbr_index_ownership.py -n 0
```

Execution used external preferences/configuration, temporary database files and
telemetry. Local evidence is retained under
`/mnt/c/dev/LiuXin-alpha/.tmp/frbr-generator-review-2026-11-11/`: baseline.log,
source-hashes.json, probe.py/probe-results.json, catalog-probe.py/
catalog-probe-results.json, and resource-probe.py/resource-probe-results.json.
These ignored artifacts are local evidence, not portable documentation links.

## Maintainability assessment

Useful foundations include canonical vocabulary substitution, deterministic
resource discovery, per-instance link settings, independent schema conformance
tests, and safe higher-level publication. The connection-level builder's staged
commits are documented; [ordinary creation](creation-and-replacement.md) builds
privately before publishing. Non-atomic low-level construction alone is not a new
finding here.

The 2,695-line generator combines resource discovery, SQL loading, TOML parsing,
normalization, validation, DDL assembly, metadata seeding and execution. The
duplicated loaders and column rules are already producing correctness gaps.
Several docstrings accurately describe broken or overridden options; recording
those limitations does not make them reliable configuration contracts.

Fix the five findings first. Then separate a validated build plan from SQL
emission and execution, share column definitions, and add generated-schema
invariant tests. Explicitly resolve the accepted-but-overridden nullable option
against the blank-row workflow; its compatibility rationale makes it a deliberate
design debt rather than a newly discovered behavior in this review.

## Repair pass — 2026-11-11

FRBR01–FRBR05 are repaired locally. The [build contract](database-generator-frbr-build-contract.md)
records the resulting behavior, responsibilities and compatibility limits.

1. A single SQL resource loader replaces paired BREAK parsing. It snapshots and
   validates complete statements before executing a stage, preserves full trigger
   bodies, and reports incomplete tails and execution failures with source context.
2. Identifier guards permit blank placeholders and unassigned candidates (both
   owner fields NULL), while validating every populated reference. Owner
   deletion cascades for all five entity types; referenced-key changes and
   replacement of identified owners are rejected, including through rowid aliases,
   UPDATE OR REPLACE collisions and with recursive triggers disabled. Catalog's complete replacement demotes existing primary
   identifiers before promotion within the same transaction. Rollback and namespace
   isolation are covered.
3. Both link emitters and parser column vocabularies share one definition catalog;
   every optional subset executes without duplicate or mistyped columns.
4. Redundant-link filtering admits each pair together with its metadata. A
   disallowed redundant link produces neither its table nor its type table.
5. Both folder guards use char(92), rejecting a single backslash on INSERT/UPDATE.

SQL resources and relationship DDL are immutable plans. Both relationship
configurations compile before link execution. Vocabulary rendering is separate;
aggregate resources share the loader. Explicit allow_placeholders settings retain
Catalog compatibility by default and permit literal nullable enforcement in
standalone strict builds. The existing public builder signatures remain intact.

Final validation:

- Final ownership/generator/Catalog suite: **160 passed, zero skipped**, 380.63 s.
  All **103 new mandatory cases** pass setup, call and teardown. The manifest now
  requires **1,430 cases**, preserving every previous case and invariant.
- Broader generator, Catalog, storage, link, creation and API run: **1,233 passed,
  58 skipped**, with two HTTP cases blocked by sandbox socket permissions. Both
  passed an isolated loopback-enabled retry (**2 passed**, 58.66 s). The initial
  run took 1,612.52 s. Its skips concern unavailable PostgreSQL, backend-specific
  concurrency/locking assumptions and the shipped-spec all-columns condition;
  the new explicit all-columns and all-subsets checks executed.
- Combining that run, the HTTP retry and final guard-focused suite gives **1,275
  distinct passing cases**. This is reconciled coverage across runs, not one
  all-green invocation or a rerun of the entire 1,430-case PostgreSQL-aware gate.
- Full required quality gates pass: formatting, lint, annotations, import cycles,
  layer direction, import footprint, both type checkers and internal contracts.
  Mypy checked 462 source files; basedpyright reported zero errors/warnings.
- Documentation/public-boundary/CI-contract checks: 79 initially passed; the
  remaining link check required staging the new guide. All 16 link checks then
  passed. PostgreSQL offline schema rendering produced 3,019 statements and
  preserved the vocabulary compatibility import; no live PostgreSQL run.
- An additional direct type check of Catalog's identifier repository reports an
  existing optional-string diagnostic in `find()`, outside the required quality
  scope. That method's AST is identical to the baseline. It is recorded as
  unrelated typing debt, not hidden by changing the checking rules.
- Final source hashes match the tested snapshot. The original checkout's protected
  source/preferences/architecture notes and recovery index are unchanged.

Enabling the bundled guards initially exposed supported unassigned candidates;
the guard now permits that explicit state. Same-scheme replacement then exposed
transient duplicate primaries; transactional demotion repairs that workflow.
A native rowid probe also demonstrated that UPDATE OF a named primary key misses
its aliases. The final update guards check actual OLD/NEW keys, including an
identified replacement destination. The final focused suite covers these repairs.

Evidence is retained locally under
`/mnt/c/dev/LiuXin-alpha/.tmp/frbr-generator-fixes-2026-11-11/`, especially
validation-summary.json, verified.log, http.log, key.log, quality-verified.log,
postgres-render.log, docs.log/doclinks.log and final-source-hashes.json. Earlier
red/interrupted runs are diagnostic evidence, not final signoff.

Changes are staged and uncommitted on `database-generator-frbr-review-2026-11-11`.
No generator push, PR update, database migration or merge was performed. The
published driver checkpoint and PR #156 are unchanged.

## Second review and repairs — FRBR06–FRBR07

A fresh pass over the staged repairs found an existing primary-flag domain gap
(P2) and stale public nullability documentation (P3). Both are now repaired.

FRBR06: the generated identifier column accepted 2, -1 and text such as "true".
The uniqueness guards compare the flag to 1, whereas Catalog reads use truthiness.
Public create calls on both SQLite drivers could therefore store one of those
flags beside a normal primary, then fail on primary_values_for_wemi(). The schema
now constrains the stored flag to NULL/0/1. Repository create and update accept
None, booleans and integer 0/1, convert booleans to integers, and reject other
types or out-of-range integers before persistence. This also protects Catalog
writes against older schemas; existing invalid data is not migrated.

FRBR07: the public interlink API now documents the actual nullable_fks behavior.
The generator API explains the allow_placeholders/nullable policy and its stage
examples reflect resource snapshotting. The API changes contain only docstrings.

Validation for these repairs:

- All 14 added regression cases failed on the prior implementation, then passed.
  They exercise direct SQL INSERT/UPDATE and both registered SQLite Catalog
  drivers, including physical column names and aliases, valid flags, rejected
  writes preserving existing rows, and an independent committed reader.
- The final focused generator/Catalog run passed 174 cases with zero skips;
  identifier import/write/hydration/merge checks passed another 10 cases.
  Documentation and regression-runner checks passed 48 cases: 232 passing tests
  across the final runs, with no failures or skips.
- All 117 FRBR cases are retained in the mandatory manifest. Its total is 1,444;
  the previous 1,430 cases and their invariant assignments are preserved.
- Offline PostgreSQL rendering produced 3,019 statements and retains the new
  primary-flag constraint. This does not establish live PostgreSQL behavior.
- Full required quality gates pass: formatting, annotations, import/layer
  boundaries and budgets, lint, both type checkers and internal contract checks.
  Mypy checked 462 source files; basedpyright reported zero errors or warnings.
- An extra lint run over the two documentation-only API files reports 20
  pre-existing import/typing-style diagnostics outside the required lint scope.
  Baseline comparisons prove identical diagnostics and unchanged non-docstring
  ASTs; the Catalog implementation and regression file pass direct Ruff checks.

The second-review evidence is in
`/mnt/c/dev/LiuXin-alpha/.tmp/frbr-generator-second-review-2026-11-11/`.
Repair evidence is in
`/mnt/c/dev/LiuXin-alpha/.tmp/frbr-generator-signoff-2026-11-11/`.
Existing-database migration, pure TOML parsing and live PostgreSQL lifecycle
validation remain separately scoped follow-ups.

Local correctness signoff is appropriate for the new-database SQLite generator
tranche. Publication was subsequently authorized from
`database-generator-frbr-review-2026-11-11` to `database-review`, after driver
PR #156 merged. The earlier staged/uncommitted entries record historical status.
