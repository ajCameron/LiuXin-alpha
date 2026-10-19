# Core

Core owns application composition, configuration, lifecycle and the command/query
boundary. LibraryServices supplies collaborators; domain rules stay with their
Catalog, metadata and storage owners.

- [Core API](core-api.md): named operations, clients, read sources and failure contracts.
- [Workflow ownership](core-program-workflows.md): operation providers and service owners.
- [Configuration ownership](configuration-ownership.md): profile selection, fixed
  paths, preference precedence and worker propagation.
- [Lifecycle review](../architectural_reviews/lifecycle-opening-path-review-2026-10-09.md):
  the opening-path migration and remaining compatibility wrappers.

For presentation and startup procedures, use the [surface guides](../surfaces/README.md).
For lifecycle regression tests, start in `tests/core/test_library_services_lifecycle.py`;
choose other checks through [test streams](../hacking/test-streams.md).

Return to the [developer index](../README.md).
