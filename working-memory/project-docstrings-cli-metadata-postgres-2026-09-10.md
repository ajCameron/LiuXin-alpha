# CLI metadata and PostgreSQL docstrings — 2026-09-10

Continuation of the [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [CLI selection and ingest history](project-docstrings-cli-selection-ingest-2026-09-10.md).
The intervening PR-status turn made no documentation progress. Revalidated the
worktree, reread the style guide, completed the two previously source-reviewed CLI
owners, then fully read and documented both regression modules and nested helpers.

Branch remains `codex/project-docstrings`, based on `edf6bf05`. All work remains
local, uncommitted, and unpushed; PR #115 and the data-submodule publication are
unchanged. No runtime fix, test assertion change, or ownership-limit relaxation
accompanies this batch.

## Reviewed contracts

- [metadata.py](../src/LiuXin_alpha/surfaces/cli/metadata.py): CLI-host files travel
  as bounded bytes, never daemon-local paths. Metadata has separate publication,
  Core/stdout-redirection, and polling helpers; do not assume every common CLI
  helper's policy applies here. JSON sorts keys, escapes ASCII/surrogates, and adds
  a newline, but does not disable nonstandard NaN serialization.
- File outputs require an existing parent, stage beside their destination, and
  publish by replacement or race-safe no-clobber hard link. Dangling symlinks are
  occupied. Production failure cleans staging; a later sync/cleanup/output failure
  can happen after publication. Stdout is buffered first but can fail during final
  copying. Preflight does not reserve a name or prove future writability.
- Core stdout redirection spans acquisition, the handler body, and cleanup.
  Dump publication is inside that context, so the current '-' dump goes to stderr;
  most other handlers emit after exit. This existing behavior is documented, not
  silently changed. File publication can precede a later session-cleanup failure.
- ID control files are BOM-aware and limited to 16 MiB. JSON list elements use
  int(str(value)); direct deduplication uses int(value), so coercion differs.
  First-seen nonnegative IDs are retained. Full selection pages rows using the first
  reported total, then hydrates IDs without a database snapshot or aggregate cap.
  Missing/zero total can stop after one nonempty page.
- Write values unwrap single-item dumps or metadata/liuxin mappings, ignore unsupported
  fields, normalize aliases, then apply convenience overrides. Clearing requires
  replacement. Receipts need only mapping shape; CLI success does not inspect every
  nested ok/changed flag. OPF bytes are wire-decoded, not locally XML-validated.
- File reads compare descriptor size/mtime around a bounded read, not content hashes
  or later path identity. Opening precedes the regular-file check. Tiny positive MiB
  limits can truncate to zero; numeric checks do not independently reject all nonfinite
  values. Format inference uses explicit text/suffix, not magic-byte validation.
- Rewriting re-inspects returned bytes, but verified=True means a mapping-shaped
  inspection response, not requested-value equality or a checked ok flag. In-place
  mode rejects symlinks and checks source identity/size/mtime once before backup and
  replacement; a race window remains. Backup, artifact, and report are independent
  publications with no shared rollback. Only permission bits are preserved; managed
  Replica bookkeeping is not updated by this unmanaged file operation.
- Online jobs validate local/source/job timing controls, submit through Core, then
  detach or poll within the session. IDs are stringified without stripping. Exact
  terminal states exclude aborted; terminal detection precedes deadline checking.
  Poll sleeps have a 0.01-second floor; timeout does not cancel. jobs.result uses
  timeout_s=0.0 and requires execution.ok, not a nested result.ok flag.
- Cover output has no local format/size validation or cross-output path-collision
  check. Missing/nonmapping cover data produces no artifact; zero status does not
  imply a found cover. Published bytes survive later JSON failure. Binary and JSON
  may both target '-'. Parser declarations retain all existing grammar/defaults.
- [postgres.py](../src/LiuXin_alpha/surfaces/cli/postgres.py): SQL-printing commands
  do not connect. Init/grant commands delegate real database mutation without a
  CLI-wide rollback. Check prints the checker-provided result before environment
  publication; no additional CLI sanitizer is applied. Export eligibility follows
  the first exact named configured check, even if overall readiness fails.
- Environment files retain raw URL credentials. Separate-password flags do not
  remove embedded secrets. Check resolves explicit/environment passwords, whereas
  write-env passes only the explicit password or empty string. Opting into a password
  export without an explicit value writes an empty assignment, not the environment
  password. The writer directly writes/replaces then chmods to 0600; it does not use
  an atomic staged publication.
- Init closes the raw connection in finally after schema creation. Optional readiness
  checks gate later manifest publication. Manifest creation makes directories before
  target validation and replaces rather than merges content. URL userinfo passwords
  and selected secret-like query keys are removed, but fragments and unrecognized
  query values remain. Publication/printing failure does not undo preceding effects.
- [test_cli_metadata.py](../tests/surfaces/test_cli_metadata.py): document all fake
  methods, context lifecycle, fixture routing, and cases. Most ebook/job results are
  fixed receipts, including arbitrary bytes labelled EPUB/JPEG. Several atomic-named
  cases assert final files, not crash-time visibility. One injected race covers a
  specific boundary. The final real SQLite/EPUB round trip proves persisted tags,
  OPF export, byte rewriting, and the subsequently inspected title.
- [test_cli_postgres.py](../tests/surfaces/test_cli_postgres.py): distinguish recording
  connection/grant helpers and already-redacted diagnostic fixtures from actual
  local file/manifest/SQL rendering. Missing-config and empty-password cases have
  ambient service/password assumptions. No live PostgreSQL server is exercised.

Batch: **4 modules, 3 classes, 130 functions = 137 AST declarations**. Cumulative:
**246 modules, 272 classes, 2,834 functions = 3,352 reviewed AST declarations**, plus
the previously documented native C and narrowly verified runtime doc metadata.

## Verification

- Strict audit and normalizer --check pass for all four files and all **246 reviewed
  Python files**, including the data-submodule generator. No structural findings.
- All four executable ASTs match HEAD after stripping only genuine leading
  docstrings. No new runtime-doc exception is needed; the three existing exceptions
  remain confined to the earlier local proxy, vacuum adapter, and initializer Args.
- No new Ruff findings. Existing baselines: metadata I001 plus 18 UP032; postgres
  I001 plus four UP032; metadata tests I001, three UP032, one UP037; postgres tests
  one I001. No unrelated lint repair or test assertion change was made.
- Full `bash scripts/run_type_checks.sh`: **passed**. The existing gate still covers
  159 formatted files, 456 annotation-covered modules, 221 protected dependencies,
  188 strict-mypy files, and 37 rejected invalid examples per checker.
- Four-file doctest selection plus metadata/PostgreSQL/operator-hardening/dependency/
  operational regressions: **155 passed, 93 explicitly skipped integration examples**,
  134.37 seconds. This includes the real local SQLite catalogue/EPUB round trip.
- Adjacent migration/public-doc/link/ownership selection: **63 passed, 5 known
  ownership failures**, 52.77 seconds. The failures remain the Core service/facade
  and browser/windowed component/root docstring-sensitive physical-line guards.
  Their implementations and limits were not changed.
- All **298 parser paths** and root-help/bash/zsh/fish fingerprints match the
  CLI-foundation baseline. Calibre used its known temporary-config fallback.
- Temporary isolated checks passed aborted-output cleanup, no-clobber races, dangling
  symlink ownership, retained publication after sync failure, permission preservation,
  dump-to-stderr behavior, weak re-inspection verification, artifact retention after
  report failure, terminal-before-deadline polling, nonterminal aborted handling,
  no cancellation, sleep floor, duplicate identifiers, and tiny transfer limits.
- Isolated PostgreSQL checks passed failed-readiness export, first-check selection,
  limited manifest redaction, private mode, retained directory effects on missing
  configuration, and no write-env environment-password fallback. One initial scratch
  assertion expected the opted-in empty password variable to be omitted; source
  inspection showed an explicit empty assignment. Corrected the scratch expectation,
  clarified the docstring, and reran the complete PostgreSQL checks successfully.
- Final PostgreSQL source/test doctest and regression rerun after the clarification:
  **28 passed, 28 explicitly skipped examples**, 14.91 seconds. This overlaps the
  larger selection above; totals are not additive. The final 246-file audit and
  normalizer pass, as do all four batch executable-AST comparisons.
- All **249 local link destinations across twenty-two working-memory files** resolve.
  Root and data-submodule diffs are whitespace-clean; staging is empty.

No whole-project pytest, installed-wheel, live PostgreSQL, remote CI, or live HTTP
listener result is claimed. All verification processes have reached terminal states.

## Inventory and next work

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs: **1,100 modules, 2,016 classes, 22,335 functions =
25,451 declarations**. Additional overlapping findings: 4,174 delimiter-layout,
10,417 missing-example, 3,022 parameter-field, 3,961 return-field, 4,073 empty-param,
5,505 empty-return, and 28 missing-summary. Reports:
`/tmp/liuxin-docstring-current-2026-09-10.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`, and
`/tmp/liuxin-docstring-cli-metadata-postgres-2026-09-10.json`.

Next: `surfaces/cli/storage.py`, all 23 `storage_commands` modules, and their
complete regression helpers. `test_cli_storage.py` remains undocumented; the
operator-hardening module remains only partially source-read despite repeated
execution. Then continue through the rest of source/tests/scripts/examples/data
submodule scope. Preserve the configuration-exec caution in the main ledger and
the full project objective. The goal remains active.
