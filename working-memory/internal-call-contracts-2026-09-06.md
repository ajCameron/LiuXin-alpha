# Internal call contracts — 2026-09-06

## Scope

Completed item 1 from the readability/maintainability review: replace storage's
catch-all helper typing and Core's dynamic endpoint contracts with explicit,
checked interfaces. Work is on `codex/package-calibre-resources`.

## Implementation

- `_StorageManagerState` no longer supplies `__getattr__ -> Any` to analysis.
  Three private protocols declare 39 cross-component helpers, with concrete
  argument and result types. Abstract declarations also prevent construction
  when a required support component is omitted. Existing implementations,
  manager ownership, and persisted ingest wire names are preserved.
- Six Core provider families have named handler protocols. The aggregate
  contract checks the real `CoreProgramAPI` at installation. Every provider
  takes typed inputs directly; object-to-protocol casts are removed.
- Registration methods distinguish command and query handlers, spell out
  required parameters and accepted keyword arguments, and retain endpoint
  registration order and descriptions. Mapping-shaped handler results remain
  explicit; the generic schema-column result stays opaque.
- Command and query envelopes enter the explicit checking targets so mypy's
  skipped imports cannot collapse both to `Any`. Their typed empty-dictionary
  factories preserve existing runtime behaviour.
- The normal quality runner invokes static positive/negative examples for
  each selected checker. All 19 invalid examples must produce their expected
  source-line diagnostic; valid calls must remain clean. The probe never runs
  application code. Its verifier rejects unrelated or missing errors.

Canonical guidance: [maintainability quality gates](../dev-docs/maintainability-quality-gates.md).

## Verification at stage-1 completion

- `bash scripts/run_type_checks.sh`: passed, including zero basedpyright
  errors, strict mypy in 99 files, lint/complexity/annotation/import checks,
  and rejection of all 19 invalid examples by each checker.
- Storage API, Core program/application API, cross-surface acceptance, and
  new quality-helper tests: 104 tests verified. The first run passed 96;
  eight RPC cases were denied local socket creation by the sandbox. All eight
  passed when rerun with the required loopback socket access.
- Updated composition tests (including both missing-support cases),
  diagnostic-verifier tests, quality-runner tests, public documentation,
  and surface-boundary enforcement: 22 passed.
- `git diff --check`: clean.

The relevant behaviour command is:

```bash
.venv/bin/python -m pytest -q tests/storage/api \
  tests/core/test_core_program_api.py tests/core/test_core_application_api.py \
  tests/surfaces/test_core_surface_acceptance.py \
  tests/scripts/test_internal_type_contracts.py tests/scripts/test_run_type_checks.py
```

RPC cases require permission to bind local sockets. The selected runs used the
configured SQLite test backend; no whole-project test-suite claim is made.

## Subsequent stages

Workflow extraction, dependency-direction repair, and read-model failure
visibility are now complete in the same checkpoint. The final typing scope
has grown to 147 files and 25 negative examples; the counts above preserve the
stage-1 verification record. Incremental formatting enforcement remains next.
See the [current checkpoint](maintainability-checkpoint-2026-09-07.md).
