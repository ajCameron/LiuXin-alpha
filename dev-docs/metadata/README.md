# Metadata

Reviewed **2026-10-20**. Current source and ownership were checked. [Next tasks](next_tasks.md) distinguishes implemented behaviour from the remaining contracts.

Metadata supplies values, pure rules, codecs and file metadata providers. Catalog
owns persistence-aware hydration and bibliographic mutations; application workflows
coordinate acquisition and file operations.

Field conversion ownership was updated **2026-10-29**:
[`utils.calibre_compat.metadata.field_adaptors`](../../src/LiuXin_alpha/utils/calibre_compat/metadata/field_adaptors.py)
owns legacy bibliographic and custom-field adapters. It delegates general conversion
to `utils.adaptors`, language normalization to `metadata.languages`, and identifier
cleaning to `metadata.identifiers`. Both field-factory entry points use one
implementation; existing whitespace, missing-value and failure behavior is retained.
Use this owner instead of the removed `databases.adaptors` module or the former
`utils.adaptors.cc_adapt_*` imports. These legacy adapters do not constitute complete
Catalog write validation.

- [Container contracts](../metadata_container/README.md): passive values and their boundaries.
- [Local sources](metadata-local-sources.md) and [web sources](metadata-web-sources.md):
  provider and acquisition guidance.
- [Projection views](metadata-projection-views.md): display-neutral read shapes.
- [Dirtied queue](metadata_dirtied_queue.md): persistence-related update tracking.
- [Writer coverage](metadata-writer-coverage-contract.md): file metadata write behaviour.
- [Language codes](languages.md): the language table, and which code form each layer uses.
- [Author names](author-names.md): author strings and name lists, and how each format keeps a name whole.
- [Subjects and tags](subjects.md): subject headings kept apart from tags, from readers to writers.
- [Keywords](keywords.md): how a keywords string is split into tags, matched against the library's known tags.
- [Metadata CLI](metadata-cli.md): operator entry points.

[WEMI container hardening](metadata-wemi-container-hardening.md) records the
hardening work and its scope. Check its status against the current container
contracts before treating a planned change as complete.

For validation and current command owners, see [test streams](../hacking/test-streams.md).
Return to the [developer index](../README.md).
