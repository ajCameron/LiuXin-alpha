# Metadata Projection Views - 2026-05-15

Branch: `metadata-relation-key-contract`

## Context

The WEMI metadata API now treats structural graph relations as many-to-many and
uses primary relation links as a preferred traversal projection. The next
planned slice is read-only convenience projections for callers that want the
values represented by relation targets without dealing with link objects,
database rows, or provenance metadata.

## Decision

Add embedded, read-only projection view objects rather than growing the core
metadata bundle API with many convenience methods.

Preferred surface:

```python
md.values.tags      # tuple[str, ...]
md.values.labels    # tuple[str, ...]
md.values.titles    # tuple[str, ...]
md.values.identifiers

md.text.tags        # display/export string
md.text.title       # preferred display title string
```

The existing graph surfaces remain authoritative:

```python
md.get_relation_links("tags")  # link ids, provenance, primary flags
md.get_related("tags")         # raw relation targets
md.tags                        # existing relation-target convenience surface
```

## Rules

- `values` and `text` are projections only.
- Projection views are read-only in this pass.
- They must not create, remove, or mutate relation links.
- They must not preserve provenance, link ids, priority, primary flags, or
  other link metadata.
- Callers that need fidelity or write-back should keep using relation links,
  `set_related`, `add_relation_link`, and `write_to_database`.
- `values` should return immutable structured values, usually tuples.
- `text` should return display/export strings and should not be treated as a
  parsing contract.

## Naming

Use `values` for structured projections. Avoid `list` as an attribute because
it shadows a Python built-in and frames the API around container type rather
than meaning.

Use `text` for lossy string projection. `md.text.tags` is useful for display,
search, logs, and export surfaces, but code that needs data should prefer
`md.values.tags`.

## Implementation Shape

- Add small reusable projection view classes under the metadata container layer.
- Expose `values` and `text` properties on WEMI metadata bundles first.
- Optionally expose stack-level `values` and `text` on `LiuXinWEMIMetadata`
  after bundle-level behavior is pinned.
- Start with high-value relation families:
  tags, labels, genres, subjects, titles, identifiers, languages, ratings, and
  common agent names.
- Use existing relation targets and primary helpers; do not change write paths.

## Validation To Add

- API/source hygiene tests should continue to reject broad `Any` in
  `metadata/api`.
- Unit tests for mapping targets, identity-like targets, and row-like targets.
- Tests that projection views dedupe/order deterministically.
- Tests that `values` and `text` are read-only and do not mutate relation links.
- Tests that raw relation links remain unchanged after projection access.

