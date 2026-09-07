# CLI dependency direction — 2026-09-07

## Scope and ownership

Stage 6 repairs the deferred CLI package/app/completion/SquashFS cycle, following
the [stage-5 formatting tranche](incremental-formatting-2026-09-07.md). Stages
5–6 are recorded together in the commit containing this note on
`codex/package-calibre-resources`. This checkpoint has not been pushed; the
last pushed checkpoint is stages 1–4, `859ab804`.

- `surfaces/cli/parsers.py` owns the complete command grammar, preserving
  registration order, aliases, options, and defaults. `app.py` retains public
  `build_parser()` and execution/selector/shortcut/error policy.
- `parser_types.py` is a standard-library-only strict leaf. Its public
  capability protocol describes the `add_parser` operation completion needs;
  `CompletionRegistrar` checks callers and the real registration function.
- `completion.py` requests the grammar with an explicit registrar, without
  importing the application. Standalone completion remains supported; there
  is no mutable registry or parser object hidden in command namespaces.
- SquashFS parser declarations and execution move to `squashfs_parsers.py` and
  `squashfs_commands.py`. `squashfs.py` keeps explicit public/private aliases
  and a lazy `main()` delegate to the complete application, not a reduced CLI.
- Core job submission, polling, receipts, publication failures, and provenance
  output retain their behavior. Private TypedDicts describe the existing
  provenance wire shape without converting missing metadata.
- Docstrings now describe the new owners and dispatch/receipt policies.
  Dependency replacement in tests targets consuming owners, not old imported
  globals in compatibility modules; no dynamic monkeypatch forwarding is added.

Canonical guide: [CLI composition](../dev-docs/cli-composition.md).

## Enforcement

- The combined dependency graph covers all 47 CLI modules, including nested
  packages and import-time, deferred, and type-only imports: 152 protected
  modules overall, with no cycles.
- CLI implementations cannot import CLI/package, app, or SquashFS entry-point
  facades. Three explicit entry wrappers may delegate to the application.
  Parser composition cannot import completion back, and the contract leaf
  cannot import another LiuXin module. Acyclic backward edges also fail.
- Eight reviewed CLI sources enter typing, lint, complexity-10, and formatting;
  only the new contract leaf enters strict basedpyright. Current strict mypy
  scope is 155 files and formatter scope is 118 files, not whole-CLI coverage.
- Positive registration examples pass both checkers; two new negative examples
  bring each checker's rejected-example total to 27. Exact-line matching and
  existing checks/suppressions policy are unchanged.
- CI includes the new CLI contract suite and expanded scanner tests, including
  every import context and a check that nested CLI modules remain in scope.

## Verification

- Full `bash scripts/run_type_checks.sh`: passed. Formatting (118 files),
  annotations (456 modules), dependency direction (152 modules), Ruff,
  complexity, basedpyright, strict mypy (155 files), and all 27 negative
  examples for each checker are green.
- Focused CLI dependency/compatibility and scanner tests: **93 passed**.
- Broader CLI, SquashFS, PostgreSQL-CLI, Core direct/RPC acceptance, boundaries,
  documentation, workflow ownership, formatter/runner, and type-verifier
  contracts: **258 passed**. SquashFS tools were available for real archive
  tests. Two existing Python multiprocessing/fork deprecation warnings were
  reported by SquashFS tests; no failures or skips.
- Read-only before/after snapshots with fixed help width: **298 parser paths**
  retain identical help and action metadata, all three shell completion scripts
  are byte-identical, and all nine exported call shapes are preserved.
- `git diff --check` is clean. Test totals above overlap and are not additive.

Database-backed tests selected SQLite; local-socket Core/RPC execution was
explicitly permitted. PostgreSQL CLI coverage is not live PostgreSQL sign-off.
No full-project test or remote-CI claim is made.

Stage 6 is complete for this bounded CLI repair and committed together with
stage 5. Pushing this checkpoint remains a separate action.

## Remaining programme

The separate terminal text-browser/windowed-UI deferred cycle is addressed by
the subsequent [stage-7 terminal repair](terminal-dependency-direction-2026-09-07.md).
Lower-level compatibility recovery policies and further package-by-package
formatter expansion remain separate. Stage 6's verification above records the
bounded CLI checkpoint, not the later terminal work.
