# Metadata Projection Views

This document describes the intended read-only projection layer for metadata
containers. It is a convenience layer over the WEMI relation graph, not a
replacement for relation links.

## Problem

WEMI metadata bundles expose graph-shaped relation data:

```python
metadata.get_relation_links("tags")
metadata.get_related("tags")
metadata.tags
```

Those surfaces are correct for provenance, link ids, primary flags, priorities,
and database round trips. They are less pleasant for common read paths that only
need the represented values, such as tag names, title strings, language codes,
or identifier values.

The API needs a convenience surface without flattening the graph or hiding link
metadata from callers that need it.

## Design

Each metadata bundle should expose two read-only projection namespaces:

```python
metadata.values
metadata.text
```

`values` returns structured, typed, immutable projections:

```python
metadata.values.tags        # tuple[str, ...]
metadata.values.labels      # tuple[str, ...]
metadata.values.genres      # tuple[str, ...]
metadata.values.subjects    # tuple[str, ...]
metadata.values.titles      # tuple[str, ...]
metadata.values.identifiers # mapping-style projection
```

`text` returns display/export strings:

```python
metadata.text.title
metadata.text.tags
metadata.text.labels
metadata.text.genres
```

The relation graph remains the authoritative surface:

```python
metadata.get_relation_links("tags")  # full link metadata
metadata.get_related("tags")         # raw targets
metadata.tags                        # existing raw-target convenience property
```

## Rules

- Projection views are read-only for the first implementation pass.
- Projection views must not mutate metadata.
- Projection views must not create, delete, reorder, or mark relation links.
- Eager metadata projections must only read already-loaded state.
- Projection views are lossy. They do not preserve link ids, provenance,
  priority, primary flags, source, policy, or other link metadata.
- Use relation-link APIs for fidelity and write-back.
- `values` should prefer tuples and mapping-like read-only structures, not
  mutable lists.
- `text` is for display/search/export text. It is not a parsing contract.

## Naming

Use `values` for structured projections. Avoid `metadata.list.tags`; `list` is
a Python built-in and describes representation rather than meaning.

Use `text` for string projections. `metadata.text.tags` is acceptable because it
is explicitly lossy display text.

## Target Extraction

Projection code should accept the target shapes already produced by metadata
containers and hydrators:

- plain strings and scalar targets
- mapping targets such as `{"tag": "Space Opera"}`
- identity/container targets with `to_mapping()`
- row-like targets with `row_dict`, `table`, and `row_id`

Extraction should use structured target data where possible rather than ad hoc
string parsing. For example, tag projection should prefer fields such as `tag`,
`label_text`, `genre`, `subject`, `title`, or their WEMI-specific row fields
depending on the relation being projected.

## Stack-Level Projection

Bundle-level projections should come first. A later stack-level projection on
`LiuXinWEMIMetadata` may combine W/E/M/I bundles into item-centered views:

```python
liuxin_wemi.values.tags
liuxin_wemi.text.title
```

That stack-level view should be explicit about precedence and should use the
primary-link projection helpers where a single preferred WEMI traversal is
needed.

Stack-level projections on `LiuXinWEMIMetadata` combine legacy/LiuXin fields
with the W/E/M/I bundle views. The precedence is:

1. already-loaded legacy/LiuXin fields
2. item bundle projections
3. manifestation bundle projections
4. expression bundle projections
5. work bundle projections

`liuxin_wemi.text.title` follows the existing `display_title` policy.
`liuxin_wemi.values.titles` follows the existing title convenience sequence and
then includes WEMI title-relation values.

## Lazy Metadata

Projection views must not silently return partial data. If a
`LazyLiuXinWEMIMetadata` projection depends on unloaded lazy legacy fields or
unloaded WEMI relation loaders, reading the projection raises
`UnloadedMetadataProjectionError`.

Call `load()` before reading projections that may have pending lazy data:

```python
metadata.load("tags")
metadata.values.tags

metadata.load()  # load all pending lazy legacy fields and relation loaders
metadata.text.tags
```

The eager `LiuXinWEMIMetadata.load()` method is a no-op that returns `self`, so
callers may use the same load-then-read pattern for eager and lazy metadata.

## Non-Goals

- No write-back through `values` or `text` in the first implementation.
- No attempt to expose every possible relation on day one.
- No replacement of relation links or graph APIs.
- No guarantee that text projections round-trip back into structured values.

## Initial Scope

Start with the high-use read projections:

- tags
- labels
- genres
- subjects
- titles
- identifiers
- languages
- ratings
- common agent names

Add tests that projection access leaves relation links unchanged.
