# File-format containment — 2026-10-13

## Baseline

The largest runtime SCC contains 280 modules, including 153 `file_formats`, 59
`metadata`, 31 `databases`, 13 `customize`, and smaller cache/storage/utility
groups. The combined SCC is 311 modules. The largest direct fan-out is
`customize.builtins.conversion`, which eagerly imports the built-in conversion
fleet; simulation shows that removing its outgoing fan-out would reduce the
runtime SCC to roughly 190 modules, but dynamic loading must not be used merely
to disguise those dependencies.

## First implemented seam

- `customize.builtins` is now an inert namespace.
- The historical all-plugin composition moved to the explicit
  `customize.builtins.registry` module.
- The generic `customize` root defers its database export-helper import to the
  Catalog-plugin method that uses it.
- GUI and Catalog-plugin callers that need the complete fleet import the
  registry explicitly.
- Cold imports fell from 74 to 31 modules for `customize`, and from 743 to 32
  modules for `customize.builtins`; checked budgets prevent regression.

The static SCC remained 280/311 after this seam because the explicit composition
root still honestly imports its providers.

## Second implemented seam

- OPF parse/version/language/manifest/pretty-print helpers moved from generic
  `metadata.utils` to `file_formats.opf.utils` with their focused tests.
- `calibreMetaInformation` moved from `metadata.utils` to the record owner,
  `metadata.book.base`; all source and test callers now use that owner.
- No compatibility re-export remains in `metadata.utils`, because that would
  recreate the graph edge. This follows the agreed break-and-fix policy while
  retaining behavior.
- A cold-import budget now holds `metadata.utils` below 30 project modules and
  prevents it from entering file formats or higher application domains.
- The measured domain SCC falls from 17 to 9, runtime SCC from 280 to 220, and
  combined SCC from 311 to 220.

The resulting 220-module runtime and combined component contains 152
`file_formats`, 50 `metadata`, 12 `customize`, four `utils`, one `library`, and
one `surfaces` module. Databases, Catalog, caches, storage, and Core are absent.

Verification on 2026-10-13:

- `tests/file_formats` plus `tests/metadata`: 2,387 passed, 18 environment- or
  live-service-dependent skips;
- focused architecture and ownership set: 148 passed;
- developer-documentation boundaries and links: 52 passed;
- full type/quality gate: basedpyright and mypy clean, 455 file-format modules
  fully annotated, all format/lint/complexity checks clean, and all import
  ratchets passing.

The next seam is conversion-contract ownership: format plugins currently depend
on `customize.conversion`, while the registry depends on the concrete format
plugins. Preserve plugin identities and behaviour while moving the reusable
contract below composition. Format-to-UI dependencies are a subsequent slice.
