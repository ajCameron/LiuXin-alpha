# Consolidate customize into plugin_system

Date: 2026-10-19. Branch: `plugin-composition-ownership`, continuing PR #125.

The user explicitly requested merging both packages under `plugin_system` and
removing `customize`. This supersedes the namespace/compatibility decision in the
[earlier startup follow-up](plugin-composition-ownership-2026-10-19.md). It is a
package consolidation, not an instruction to merge GitHub PRs into their bases.

## Result

- The `src/LiuXin_alpha/customize` directory is removed. Implementations move to
  `plugin_system`, with the former package root in `plugin_system.contracts` and
  the application owner in `plugin_system.application`.
- Eight obsolete forwarding modules are deleted, including the UI, conversion,
  profile and ZIP facades and four built-in provider aliases. There is no namespace
  alias or import hook preserving `customize`. Generic base/file contracts and
  conversion contracts keep their canonical leaves.
- Application families, archives, built-ins and retained cache/driver/trigger/API
  modules all move. Repository production callers, ZIP plugin examples, test
  sentinels and docstring test commands use the new owners. Tests move from
  `tests/customize` to `tests/plugin_system`.
- The layer checker now enforces boundaries inside the merged package: standalone
  formats cannot import application/provider subtrees, and neutral plugin modules
  cannot import those subtrees or domain implementations. Unclassified new plugin
  modules default to the neutral rules. The whole package cannot import Core,
  surfaces, ingest, jobs or storage. Cold budgets also reject forbidden module
  prefixes, so a same-package import cannot hide beneath a numerical ceiling.
- Source tests reject retired namespace imports. Wheel inspection rejects stale
  `customize` files; installed runtime checks verify application/contracts origins,
  passive startup and absence of the retired namespace.
- Guides 02/04, the format index, composition contract and architecture review
  statuses describe the unified owner. The composition guide has the explicit
  migration table for external plugins. Earlier dated reviews retain their
  observations with a superseding follow-up note.

The static baseline changes only relocate three existing unresolved dynamic
imports from the old root to `plugin_system.contracts` and adjust their line
numbers. No new backward-edge allowance or larger cycle ceiling was introduced.

## Validation

- 319 regression tests pass with telemetry and immediate scratch cleanup across
  plugin/runtime, conversion, metadata, Core and architecture/documentation tooling.
- 35 follow-up tests pass for final namespace, cache identity, wheel-checker and
  documentation changes; this overlaps the broader run.
- All quality gates pass: 176 formatting targets, 463 annotated format modules,
  243 protected modules, 1,792 whole-package modules, unchanged 15 reviewed backward
  edges / 28 unresolved dynamic imports, 25 cold-import budgets, lint/complexity,
  basedpyright and mypy with both sets of 37 negative internal-call examples.
- A newly built 11 MiB wheel contains no customize package. The standard installed
  wheel verifier installs into a temporary target, verifies module origins and
  passive startup, creates/reopens a catalogue through the CLI, and performs the
  installed conversion smoke successfully.
- Retained CalibreCache class identity is checked against relocated BaseCache
  without creating a legacy database. The old schema-dependent cache test module
  remains opt-in and was skipped in the initial focused run.

Local-only evidence: `working-memory/test-results/plugin-package-merge-20261019/`.
The source wheel is retained temporarily in `/tmp/liuxin-plugin-merge-wheel/`.
No application-wide full suite was run locally for this package change.

## Compatibility and remaining work

External ZIP plugins importing `customize` must update and be repackaged. This is
intentional namespace retirement, not an accidental compatibility regression.
Unsupported device providers, retained cache/folder-driver/API remnants, structured
provider availability reporting and process-wide registry/conversion state still
need their separate follow-ups. Jobs/ingest and independent domain registries are
outside this merger.

The isolated review worktree remains `/tmp/liuxin-test-vm-telemetry`. The main
checkout's staging and unrelated edits are untouched. Keep the intentional
`LiuXin_alpha_data` symlink out of every commit.
