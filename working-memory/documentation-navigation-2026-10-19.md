# Documentation navigation and path migration

Date: 2026-10-19
Branch: `architecture-review-followup`, following `6cfacdb2`.

## Completed change

The review branch now carries the working checkout's 80 documentation moves,
reconciled with the newer review/configuration content. Configuration ownership
also moves into `dev-docs/core/`, giving 81 moves in total. The annotated September
review, target decisions, latest top-level guide and the user's documentation
edits are retained. The docstring inventory's bytes are unchanged.

The [developer index](../dev-docs/README.md) now provides task routes and links to
subsystem indexes. Twelve new folder indexes separate current contracts, design
background and dated records. All 126 developer Markdown documents are reachable;
389 local link occurrences with file/directory paths resolve in versioned content.
The nine format dossiers are actual navigable links.

Incoming links, relative depths, package-root signposts, root/example READMEs,
historical handoff references and deployment runbooks use the new locations.
The source changes are docstrings only, verified by comparing ASTs with docstrings
removed for all eight affected Python source files.

The 86 links to ignored local test outputs are now labelled local evidence paths.
Their recorded results and tracked handoff links remain. Documentation checks
cover every developer Markdown file, reject missing/untracked local targets, and
require guide reachability. Regression cases cover locally present untracked
evidence and directory indexes that omit a guide. External websites and every
renderer-specific heading fragment are outside this gate.

Deployment metadata, generated bundle instructions and fixture paths now use the
moved runbooks. A new check against the real checkout exposed two additional stale
prerequisites: the retired CLI storage facade and surface profile module. The
packager now requires `surfaces/cli/storage_commands/__init__.py` and
`core/settings.py`. The synthetic bundle test still verifies the resulting archive.

Two small ownership corrections accompany the navigation: the Core workflow guide
names LibraryServices as the composition owner, and the old metadata DB source
guide is explicitly historical with a pointer to Catalog-owned hydration.

## Validation

- 54 documentation, public-boundary, deployment-package and CI-workflow contract
  tests pass. The first run's real-checkout packaging failure is retained in the log.
- The old-path scan is clear in source, scripts, tests, root/example guides,
  developer Markdown and tracked handoff prose. Generated JSON inventories retain
  their historical contents.
- Main-checkout documentation hashes match the pre-migration snapshot. Its index,
  source changes and data checkout were not modified by this integration work.
- The complete `scripts/run_type_checks.sh` runner passes: formatting, annotation,
  lint/complexity, import/layer boundaries, basedpyright and mypy, including both
  sets of positive/negative internal-call examples.
- An export of 183 staged Git blobs (developer docs and their referenced targets)
  has zero missing/untracked local targets and all 126 developer guides reachable.
  The export contains no ignored local test evidence.

No application-wide test suite was needed for this documentation/tooling change.

Local-only evidence: `working-memory/test-results/documentation-navigation-20261019/`.
The preparation snapshots and explicit rename map are also retained locally under
`/tmp/documentation-migration-20261019/`; that directory is not portable evidence.

## Working checkout and next work

The integrated result is in the isolated review worktree. The older main working
checkout still has the user's staged moves and unrelated source edits; it was not
reset or switched while reconciling the documentation. Review/merge the branch as
one consistent tree rather than copying newer source-dependent guides into that
older checkout piecemeal.

Next content work is the focused ownership-contract pass: reconcile
`04 - Seperation of Concerns.md`, remaining Catalog migration/status prose and the
older numbered guides against the implemented owners. Preserve the distinction
between accepted policy, completed implementation and remaining migrations.
