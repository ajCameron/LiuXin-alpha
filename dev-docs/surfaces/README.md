# What is a surface?

Reviewed **2026-10-20**. Entry-point composition and named Core operations were checked. See [next tasks](next_tasks.md); older stage counts remain dated evidence.

A means of interacting with the program.

LiuXin is intended to be flexible and modular on a very basic level.
(In some cases, this has and will end up more aspirational than otherwise. But we're working on it).
As such, as far as possible, we're making no assumptions about _how_ you might want to use the program.
Instead of a single, fixed interface we present a family of interfaces - all of which talk to a single Core.

Feel free to add more!

# Application surfaces

Surfaces parse external input and present application results.

Named Core operations  provide domain behaviour; presentation does not replace lower-layer validation or
capability enforcement.

- [Operational CLI](operational-cli.md) and [metadata CLI](../metadata/metadata-cli.md):
  operator commands and their limits.
- [CLI composition](cli/cli-composition.md) and [terminal composition](terminal/terminal-composition.md):
  implementation owners.
- [Read-only startup](read-only-surface-appliance-startup.md) and
  [read-model failures](read-model-failures.md): startup and error contracts.
- [Tkinter architecture](tkinter/tkinter-gui-architecture.md) and
  [implementation plan](tkinter/tkinter-gui-implementation-plan.md): design and staged work;
  a planned feature is not necessarily implemented.

Use the [Core API](../core/core-api.md) when adding an operation and [test streams](../hacking/test-streams.md) to choose checks.

Return to the [developer index](../README.md).
