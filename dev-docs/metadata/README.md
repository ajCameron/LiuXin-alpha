# Metadata

Reviewed **2026-10-20**. Current source and ownership were checked. [Next tasks](next_tasks.md) distinguishes implemented behaviour from the remaining contracts.

Metadata supplies values, pure rules, codecs and file metadata providers. Catalog
owns persistence-aware hydration and bibliographic mutations; application workflows
coordinate acquisition and file operations.

- [Container contracts](../metadata_container/README.md): passive values and their boundaries.
- [Local sources](metadata-local-sources.md) and [web sources](metadata-web-sources.md):
  provider and acquisition guidance.
- [Projection views](metadata-projection-views.md): display-neutral read shapes.
- [Dirtied queue](metadata_dirtied_queue.md): persistence-related update tracking.
- [Writer coverage](metadata-writer-coverage-contract.md): file metadata write behaviour.
- [Metadata CLI](metadata-cli.md): operator entry points.

[WEMI container hardening](metadata-wemi-container-hardening.md) records the
hardening work and its scope. Check its status against the current container
contracts before treating a planned change as complete.

For validation and current command owners, see [test streams](../hacking/test-streams.md).
Return to the [developer index](../README.md).
