# Rename the unified plugin package to plugins

Date: 2026-10-19. Branch: `rename-plugins-package`, based on `a5f77cfd`.

The user selected `plugins` and requested the full refactor, documentation and a
PR. The preceding package merger is in `configuration-ownership`; the remote
`main` checkpoint `54789496` does not include it. Target the rename PR at
`configuration-ownership` to keep the review focused.

## Change

- Move `src/LiuXin_alpha/plugin_system` to `src/LiuXin_alpha/plugins`, with no
  forwarding package or import alias. Update production consumers, ZIP plugin
  examples, documentation commands and class/module identity assertions.
- Move `tests/plugin_system` to `tests/plugins` and `dev-docs/plugin_system` to
  `dev-docs/plugins`. The test-stream alias, formatting/type scopes, import budgets
  and layer rules follow the new names. Existing debt entries change namespace
  without increasing counts or cycle limits.
- Update the developer indexes, guides 02/04, composition/migration contract,
  cache guide and active architecture checks. Dated findings retain their earlier
  names with explicit follow-up notes and current navigation links.
- Reject both retired namespaces in source and wheels. Installed-wheel smoke
  imports the new application/contracts owners, checks their origins and passive
  startup, and verifies both old namespaces are absent.
- Preserve persisted plugin settings, including the `customize` component name.
  External plugins must replace `LiuXin_alpha.plugin_system` with
  `LiuXin_alpha.plugins`; relative module names and runtime behavior are unchanged.

The changes are made in the main working checkout. The six unrelated storage-file
edits remain byte-for-byte intact and must not be staged. Include the prior guide
02 refinement and workspace handoff in this documentation update. The original
workspace recovery stash described in that handoff remains available.

## Validation

- 28 preliminary package-structure, namespace-retirement and documentation checks
  pass from the main checkout; these overlap the broader run.
- All 22 documentation checks pass again after the final packaging-guide update.
- All 328 focused plugin/runtime, conversion, metadata, Core and tooling tests
  pass against the clean rename commit, with durable telemetry and immediate
  test-scratch cleanup. No full application suite was run.
- Complete quality gates pass: 176 formatting targets, 463 annotated format
  modules, 243 protected modules, 1,792 whole-package modules, the same 15 reviewed
  backward edges and 28 dynamic imports, 25 cold-import budgets, lint/complexity,
  basedpyright and mypy, including both sets of 37 rejected invalid call examples.
- A fresh 11 MiB wheel passes the supported installed-wheel gate: correct origins
  for `plugins.application` and `plugins.contracts`, passive startup, neither old
  namespace importable, CLI database creation/reopening and Unicode HTML-to-EPUB
  conversion. The wheel has 1,784 Python files and 403 runtime assets.
- Telemetry sampled a peak process-tree RSS of 269,832,192 bytes, minimum available
  memory of 6,847,332,352 bytes and no OOM kills.

The initial concurrent regression/quality attempt caused the test artifact guard
to remove the linter's new `.ruff_cache` directory: 328 test assertions passed,
with one teardown error and an interrupted lint run. The clean results above
come from sequential runs with quality caches outside the checkout. Keep these
lanes sequential when using the repository-root artifact guard.

The additional legacy `scripts/verify_wheel.py` command still requires two assets
not included by the current package-data contract: `epub/cfi/epubcfi.ebnf` and
`unihandecode/pykakasi/kakasidict.utf8` under `file_formats`. The same failure was
reproduced with its pre-rename implementation and the previous merger's wheel.
This is a separate verifier/packaging-contract follow-up; the supported
`verify_wheel_install.py` gate passes. Both checkers reject retired plugin packages.

Local-only evidence: `working-memory/test-results/plugins-package-rename-20261019/`.
The verification worktree excludes the unrelated storage edits; the main checkout
keeps the renamed package throughout.
