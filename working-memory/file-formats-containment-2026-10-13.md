# File-format containment — 2026-10-13

Current checkpoint: **third seam verified on 2026-10-14** on
`file-format-contract-containment`. The next slice is format-to-UI/customization
containment; preserve behavior and keep the remaining dependencies visible.

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

At the second checkpoint, the next seam was conversion-contract ownership:
format plugins depended
on `customize.conversion`, while the registry depends on the concrete format
plugins. Preserve plugin identities and behaviour while moving the reusable
contract below composition. Format-to-UI dependencies are a subsequent slice.

## Third implemented seam

- The generic `Plugin` lifecycle and `PluginPreferences` now belong to the
  passive `plugin_system.base` module.
- Conversion options, reporters, and input/output plugin bases now belong to
  `file_formats.conversion.plugin`.
- All built-in converters and internal consumers import the new owners directly.
- `customize` keeps compatibility exports with identical class objects, so
  existing plugin subclass, `isinstance`, ZIP-loading, and import contracts do
  not change.
- Cold-import budgets prevent either reusable contract from entering
  `customize` or higher application domains.

The largest runtime and combined SCCs are now 218 modules, down from 220. The
domain SCC remains 9. This is a deliberately narrow reduction: other
format-to-customization/UI paths still connect the component and are the next
containment target.

Verification on 2026-10-14:

- The complete `tests/file_formats`, `tests/metadata`, and `tests/customize`
  suites, architecture/documentation checks, and Core program-service contracts
  passed together: **2,572 passed, 18 environment-dependent or opt-in skips**.
- The full `bash scripts/run_type_checks.sh` gate passed: basedpyright and mypy
  clean, both type-contract checks passed, 456 file-format modules fully
  annotated, formatting/lint/complexity checks clean, 243 protected modules
  acyclic, and all 16 cold-import budgets passing.
- The whole-package layer check passed across 1,797 modules with 16 reviewed
  backward edges and 39 unresolved dynamic imports; domain/runtime/combined
  component sizes remain 9/218/218.
- Focused compatibility coverage includes exact plugin/preference/conversion
  class identities, conversion options, ZIP resources and context cleanup, and
  an external ZIP module importing the historical conversion contract.
- All 43 changed production consumers contain only the intended import-owner
  replacement. Scoped Ruff and `git diff --check` passed.
- Package-root documentation now states and links the canonical ownership
  boundary for both `plugin_system` and `file_formats`; the documentation check
  protects these roots. The working-memory index points to this active lane.

Local logs and command/exit records are retained under
`working-memory/test-results/file-format-contract-2026-10-14/` as
`regressions.log`, `regressions.done.json`, `quality.log`, and
`quality.done.json`. The separate `LiuXin_alpha_data` submodule changes are
excluded from this checkpoint.
