# Testing and diagnostics

- [Test streams](../test-streams.md): focused commands, full-suite execution,
  resource telemetry and scratch cleanup.
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
