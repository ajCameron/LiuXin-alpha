# Calibre generator relocation — 2026-11-11

The user authorized moving the Calibre generator into `utils/calibre_compat`
and fast-forwarding after FRBR PR #157 merged. The persistent review clone is
`/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`.

Fast-forwarded from `2823cd22` to the merged `github/database-review` at
`fcf9166c0236c94c06350c8bf830d6a8246869b5`, then created branch
`calibre-generator-relocation-2026-11-11`. The user subsequently requested commit
and PR publication targeting `database-review`; they will fast-forward their
own checkout after merging. The clone's `origin` is the original local checkout;
`github` is the publishing remote. Publication receipts are stored under
`/mnt/c/dev/LiuXin-alpha/.tmp/calibre-generator-publication-2026-11-11`.

An earlier checkout-switch request was superseded before any stash or branch
change. The original `maintenance-plugin-modes` checkout is unchanged. A backup
of its index, staged/unstaged patches and 212 changed files was retained under
`/mnt/c/dev/LiuXin-alpha/.tmp/calibre-checkout-switch-2026-11-11`.

## Result

- Moved the three implementation modules from
  `databases/database_driver_plugins/SQL/calibre_database_generator` into
  `utils/calibre_compat/database_generator`. Schema and library-builder ASTs
  match the merged implementations. The obsolete relocation TODO was removed.
- Migrated repository imports, fixture/script consumers and SQLite/APSW's
  explicit builder registrations. Ordinary SQLite/APSW creation still produces
  FRBR catalogues. The reader's version lookup now stays inside compatibility.
- Retained both type-checker scopes and added a packaged `py.typed` marker for
  the new owner. SQL assets remain under `resources/calibre`.
- Updated compatibility navigation and current docstring-review inventories.
  The old import package is removed; external callers must use
  `LiuXin_alpha.utils.calibre_compat.database_generator`.
- Added real-schema checks for the four SQLite builder names/aliases and
  extended the installed-wheel verifier to load the relocated modules and
  create a Calibre schema from installed resources.

## Validation

Evidence directory:
`/mnt/c/dev/LiuXin-alpha/.tmp/calibre-generator-move-2026-11-11`.

- Focused Calibre/compatibility, schema-builder, registry, FRBR creation,
  packaging, quality-runner and documentation tests: **145 passed, 16 skipped**.
  Skips were missing Hypothesis, external compatibility libraries and the
  separate fixture-data checkout. See `tests.log` and `telemetry/`.
- Full `scripts/run_type_checks.sh`: passed formatting, lint, annotation,
  dependency-direction/cycle/import-budget checks and both type checkers.
  Mypy still checks 462 source files; both internal type-contract checks pass.
- Built a clean wheel offline, inspected all 405 required runtime assets
  (including 317 Calibre resources), and installed it into temporary storage.
  The cold generator import loads no database driver plugins; schema creation,
  library creation and book population succeed from the installed package.
  The verifier's origin/resource/schema program passes with existing third-party
  dependencies; all LiuXin/past origins are required to resolve inside the
  installation. This was an installed Calibre probe, not the full conversion CI
  workflow. See `wheel-verification.json`, `wheel-probe.py` and associated logs.
- No old generator imports remain in current Python/configuration callers.
  Original protected source/preferences/notes and the WSL-recovery index match
  their recorded hashes.

## Existing fixture-tooling findings for the next review

The extra fixture exercise exposed two pre-existing issues. They are unchanged
by this relocation and are not repaired here:

1. `scripts/generate_calibre_fixture_libraries.py` renames the author directory
   for `03_filesystem_drift`, then attempts to read MOBI/delete EPUB through
   stale paths. On the case-sensitive temporary filesystem, generation raises
   `FileNotFoundError` at the duplicate-MOBI step.
2. The script's expected-snapshot format has fallen behind the reader tests:
   it emits only the book count and omits fields/counts expected by the complete
   snapshot and scan-report checks. Five other fixtures generate successfully,
   but their selected checks produce **13 failed, 2 passed**.

Both were reproduced in an isolated archive of merged commit `fcf9166c`, using
its original generator imports and independently generated fixtures. Baseline
and relocated runs have the same 13 failing test node IDs. See `fixtures.log`,
`fixture-tests.log`, `baseline-drift.log`, `baseline-tests.log` and
`baseline-results.json`. Fixture payloads came from the packaged English
quick-start EPUB; no external data checkout was modified.

All checks have finished. Temporary build/install/baseline/fixture directories
are disposable; durable logs and the built wheel are retained in the evidence
directory. Continue review in the persistent clone, preserving the original
checkout and WSL recovery data.
