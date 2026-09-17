# Repository shim cleanup — complete, 2026-09-16

The user requested removal of the remaining shims after the storage-root cleanup.
The repository-wide import/alias cleanup is complete. The unrelated documentation
campaign remains paused after D095; D096 starts only on a new request.

[Plan and audit decisions](../dev-docs/shim-removal-plan.md).
[Final evidence](test-results/shim-removal-2026-09-15/final-observations.json).

61 forwarding/stub modules were deleted, 212 live Python files were modified,
and no module-level __getattr__ export hooks remain. Source, tests, examples,
packaging and current developer guides use implementation owners. This includes
storage aliases, cache/metadata/utility/SQL forwarders, terminal and CLI facades,
Calibre namespace injection, unused crawl/SQL/PDF wrappers and two GUI stubs.

The installed liuxin command now targets surfaces.cli.app:main. Its editable
installation was refreshed without dependency changes or downloads. Installed
CLI and terminal help passed. HTML2ZIP retains its existing headless failure,
now raised directly; this cleanup does not implement GUI conversion.

Private ingest types now have their real storage_manager.mixins._types module.
The journal codec retains existing version-1 type tags explicitly. Existing
database reload, ingest, encoding/decoding and new-pickle checks passed. Backend
kind strings and other persisted-data contracts remain valid.

Verification is recorded per group and selections overlap: S2 194 passed,
S3 69 passed, S4 38 passed, S5 327 passed/14 skipped, corrected S6/S7 288 passed,
and 32 final import/cache/browser checks passed. S1's corrected focused group
passed 32 tests after 244 other cases passed in its larger selection. All 7,117
repository tests collected successfully; the entire suite was not executed.
The configured quality runner passed: formatting, annotation coverage, import
direction/cycles, complexity, lint, basedpyright and mypy, including 37 rejected
negative examples per checker. The final lint comparison found no added findings.
All 130 affected reviewed Python files passed the docstring audit; 19 developer
link tests passed. Whitespace is clean. Two final newline-only corrections have
identical ASTs including docstrings. Failed attempts remain in the evidence folder.

The live documentation inventory is 804/2,671 reviewed files, 1,867 remaining,
with 30,566 remaining declarations. D remains 95/232 verified units; its current
source scope is 2,330/5,685 declarations and 118/374 complete files. No documentation
unit was promoted by this maintenance work. All surviving declarations retain
their exact assignment within the four-file/forty-declaration caps.

For future documentation work, use each affected file's maintenance_baseline_source
and maintenance_baseline_sha256 (documentation-rebase.after.json), and the current
unit scopes. Original baseline hashes, prior source snapshots and historical
documentation-only proofs are preserved. Do not replay old batch helper scripts
with hard-coded 2,732 files / 5,719 D declarations / 388 D files. Author scripts
saved as .py.txt in the evidence folder have already been applied and must not
be reapplied. The reconciliation report archives removed records and old scopes.

Functional adapters, optional-dependency fallbacks, domain error/type vocabulary,
current public API contracts and bundled compatibility libraries remain for the
reasons listed in the plan. This was not a removal of all compatibility behavior.

No commit or publication was made. Preserve all unrelated preexisting changes.
