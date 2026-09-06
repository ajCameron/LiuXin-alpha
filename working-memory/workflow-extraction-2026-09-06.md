# Core and storage CLI workflow extraction — 2026-09-06

## Scope and ownership

Stage 2 of the readability/maintainability work extracts real workflows from
`core/program_api.py` and `surfaces/cli/storage.py`. It is included in the
[stages 1–4 checkpoint](maintainability-checkpoint-2026-09-07.md) on
`codex/package-calibre-resources`.

- Core's facade is 212 lines, down from 4,343. Its 23-file `program_services`
  package owns operations by responsibility. Eight small instance delegates
  preserve historical unbound/bound signatures; other stateless operations
  have explicit aliases. Provider declarations and registration order remain
  separate and unchanged.
- The CLI facade is 173 lines, down from 3,137. Its 23-file `storage_commands`
  package separates Core administration, options/guided setup, command-family
  parsers, and ingest configuration, readiness, paths/locks, signals, execution,
  and report publication. Tests patch consuming owners instead of facade
  globals; no dynamic compatibility proxy was introduced.
- Evacuation has immutable typed plan/entry/limit values, independent planning
  and execution, and shared policy eligibility/separation/capacity helpers.
  Runtime envelopes and wire receipts stay at the adapter. Execution rechecks
  verified replacement capacity against current topology before removing
  sources; failed placement and unsafe capacity retain source claims/bytes.
- Status separates durable inventory from per-Store rendering and aggregate
  health. Metadata writer input normalization, Store edit normalization,
  durable Store resolution, and wire projection also have named stages.
- Both implementation trees enter typing, Ruff, and complexity-10 checking.
  All seven former Core complexity violations are gone. The four newly
  written evacuation/policy modules are strict basedpyright targets; moved
  dynamic adapters retain their prior standard mode. Strict mypy covers 145
  source files. Both checkers reject all 22 negative contract examples,
  including invalid handler overrides and incorrect evacuation plans/limits.
- CI now protects workflow ownership and focused evacuation/compatibility
  contracts. Growth ceilings are 450 lines per owner, 160 per function, and
  250 per facade; implementation may not import back through a facade. The
  largest current Core owner is 418 lines and function 155; CLI is 339/132.

Canonical guide: [Core workflows](../dev-docs/core-program-workflows.md);
gate policy: [maintainability quality gates](../dev-docs/maintainability-quality-gates.md).

## Verification at stage-2 completion

- `bash scripts/run_type_checks.sh`: passed, including zero-error production
  checks, strict mypy in 145 files, and all 22 negative examples in each checker.
- Combined storage API, Core program/application API, cross-surface acceptance,
  CLI storage/operator/initializer, documentation and architecture checks:
  **160 passed**. RPC cases ran with explicitly permitted local sockets.
- Additional CLI operational families, read-only help, deployment and wheel
  verifier unit tests: **22 passed**. An old fixture still patched the facade's
  imported session opener; it was updated to the extracted consuming owners.
- Evacuation safety tests: **11 passed**, covering action/byte limits, exact
  limits, blocked placement, failed/unverified copies, topology changes,
  source-byte retention, and plan response shape.
- A direct comparison against pre-extraction commit `9d86fedb` preserved all
  **81 public Core class and instance signatures** and **66 CLI parser paths**,
  including help, option ordering, defaults, required flags, and argument
  shapes. This was a one-off read-only introspection check, not a deployment
  operation. The committed facade/parser contracts protect the extracted
  ownership; the temporary comparison script is not a required project tool.
- `git diff --check` and lint of both complete extracted trees: clean.
- Final post-compatibility Core program/application and cross-surface acceptance:
  **22 passed**, with permitted local sockets.
- Combined new evacuation/facade contracts, workflow ownership, diagnostic
  verifier, quality-runner, public documentation and surface-boundary tests:
  **40 passed**.
- Formatter check of both facades and both complete implementation trees:
  **48 files already formatted**.

Stage 2 is complete. The configured database backend is SQLite; no full-project
or PostgreSQL-suite claim is made.

## Remaining programme

Dependency-direction repair and unexpected read-model error visibility were
completed in stages 3 and 4. Incremental formatter enforcement remains next;
see the [current checkpoint](maintainability-checkpoint-2026-09-07.md). Legacy
dynamic subsystem adapters are not represented as fully strict merely because
their implementation moved.
