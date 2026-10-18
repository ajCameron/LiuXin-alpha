# Application surfaces

Surfaces parse external input and present application results. Named Core operations
provide domain behaviour; presentation does not replace lower-layer validation or
capability enforcement.

- [Operational CLI](operational-cli.md) and [metadata CLI](../metadata/metadata-cli.md):
  operator commands and their limits.
- [CLI composition](cli-composition.md) and [terminal composition](terminal-composition.md):
  implementation owners.
- [Read-only startup](../read-only-surface-appliance-startup.md) and
  [read-model failures](../read-model-failures.md): startup and error contracts.
- [Tkinter architecture](tkinter-gui-architecture.md) and
  [implementation plan](tkinter-gui-implementation-plan.md): design and staged work;
  a planned feature is not necessarily implemented.

Use the [Core API](../core/core-api.md) when adding an operation and
[test streams](../hacking/test-streams.md) to choose checks.
Return to the [developer index](../README.md).
