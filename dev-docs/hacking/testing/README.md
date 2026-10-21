# Testing and diagnostics

Source reviewed **2026-10-20**, working tree based on `fc5de7af`.

- [Test streams](../test-streams.md): focused commands and full-suite execution.
- [Scratch cleanup and telemetry](<../../07 - Test Databases.md#scratch-cleanup-and-telemetry>):
  bounded diagnostic runs, retained evidence and fixture lifetimes.
- [CI ownership](continuous-integration.md): automated lanes and required environments.
- [Test databases](<../../07 - Test Databases.md>) and [data artifacts](data-artifacts.md):
  fixture provenance, isolation and storage policy.
- [Malformed-input fuzzing](malformed-input-fuzzing.md): corpus and failure boundaries.
- [Live storage CI](../../storage_and_backup/live_storage_ci.md): service-dependent checks.

Run the checks appropriate to the change. Preserve diagnostic records separately
from disposable scratch, and distinguish a focused passing selection from a full
suite result. Current behaviour belongs in the guides; dated run evidence belongs
in [working memory](../../../working-memory/index.md).

Return to [contributing](../README.md) or the [developer index](../../README.md).
