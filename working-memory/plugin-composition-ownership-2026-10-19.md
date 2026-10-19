# Explicit plugin composition ownership

Date: 2026-10-19. Branch: `plugin-composition-ownership`, based on documentation
commit `7d854316` on `architecture-review-followup`.

## Publication and scope

The documentation branch is published as [PR #124](https://github.com/ajCameron/LiuXin-alpha/pull/124),
based on `configuration-ownership` (#123, itself based on #122). The plugin fix
follows that branch so the updated ownership guides and paths remain consistent.
The main working checkout's staging and unrelated edits are untouched; work stays
in `/tmp/liuxin-test-vm-telemetry`. Its `LiuXin_alpha_data` symlink is intentional
and must never be staged or reset.

## Implemented ownership

- `plugin_system.loading` owns common loading, initialization failure handling,
  stream restoration and stable descending priority. Installed providers precede
  bundled providers on ties. Composition callers supply provider classes, installed
  paths and accepted families; the loader imports no concrete domain providers.
- `file_formats.plugin_defaults` owns the file-provider catalogue. The application
  catalogue extends it with application-only families rather than duplicating
  readers, writers, converters, hooks and profiles. The stream import adapter now
  belongs to `file_formats.plugin_runtime` too.
- `customize.application` owns the process application registry, lookup and
  installed-plugin operations. Import is passive. Explicit startup refreshes it;
  application lookups initialize on first use for compatibility.
- `customize.ui` contains compatibility exports without registry state. Historical
  provider-list/name attributes resolve on explicit access. Production callers now
  import either the application owner or the file runtime; a static rule rejects
  new imports of the old UI module in every import context.
- Host and standalone startup share a reentrant lock. Publication retains exact
  plugin instances, interrupted refresh restores the preceding binding, and failed
  first startup is retryable. Reentrant file lookups see an empty fleet; application
  lookups see the previous host snapshot. Concurrent initial lookups start once.
- A bundled-name conflict skips the installed entry without deleting archives,
  changing settings or recursively invoking removal/startup.

The top-level package split is retained and documented in guides 02/04 and the
plugin composition contract. The September review marks priority/startup fixed
while retaining unsupported-device and broader registry/compatibility work.

## Validation

- 178 regression tests pass across application plugins, standalone runtime,
  conversion, metadata writers, Core service contracts and architecture/CI tooling.
  This run enabled durable telemetry and immediate test scratch cleanup.
- 26 focused tests pass for the added startup failure cases and metadata source
  cache callers; 81 follow-up tests pass for final compatibility/stream-adapter
  changes, source identify/cover/worker consumers and documentation boundaries.
  These runs overlap; the counts are not a unique total.
- The complete quality runner passes: 176 formatted files, all 463 format modules
  annotated, 243 protected modules without forbidden cycles, a whole-package graph
  of 1,800 modules with unchanged 15 reviewed backward edges / 28 dynamic imports,
  26 cold-import budgets, lint/complexity, basedpyright and mypy with both sets of
  37 negative internal-contract examples.
- Fresh-process tests explicitly use this checkout's absolute source path. The
  shared editable environment otherwise resolves the older main checkout when
  pytest's cwd-isolation fixture changes directory; the initial test run exposed
  that issue and the subprocess tests now prevent it.

Local-only evidence: `working-memory/test-results/plugin-composition-20261019/`.
No full application suite was required for this focused change. Documentation PR
CI has already passed architecture/typing, wheel, confidence and format lanes;
its full-suite status must be checked on GitHub rather than inferred from these runs.

## Limits and next work

The inherited `customize` name, cache/folder-driver remnants and deleted-device
provider references remain. Optional provider failures still lack a structured
availability report. The lock protects registry startup/publication, not full
concurrent conversions or arbitrary side effects performed by plugin initializers.
The application registry remains process-wide. Database, storage, cache and terminal
registries have distinct domain responsibilities; do not merge them just because
they are called registries. Jobs/ingest composition remains separate pending work.
