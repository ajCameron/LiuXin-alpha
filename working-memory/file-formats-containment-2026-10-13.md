# File-format containment — 2026-10-13

Current checkpoint: **containment lane verified on 2026-10-15**
on `file-format-contract-containment`. Standalone file-format composition now owns
its runtime; remaining metadata/application cycles stay explicitly tracked.

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


## Fourth implemented seam — 2026-10-15

- File-hook and metadata stream contracts moved to `plugin_system.file_types`;
  shared persisted settings and ZIP mechanics moved below application composition.
- Conversion provider assembly and device profiles now live under `file_formats`;
  metadata providers live under `metadata.file_sources`. Compatibility exports
  retain object identities at the old paths.
- `file_formats.plugin_runtime` supports native lazy initialization and explicit
  binding of application-owned instances. External ZIP plugins, provider order,
  customization, enable/disable rules, profiles, hooks and metadata flags survive.
- Native composition preserves optional-provider handling and restores process
  output streams after faulty external plugins. Rebinding clears stale writers.
- Headless rendering and HTML comments no longer depend on surfaces/library.
  Pixel serialization now returns real PNG bytes; its shared image utility avoids
  importing a PDF backend. Existing Qt availability/bootstrap stubs remain.
- The LRF input registry typo is fixed and tested against duplicate LIT entries.
- The static direction guard now rejects file-format-to-application imports and
  plugin-system-to-domain imports in every import context. No new backward-edge
  allowance was added: existing debt remains 16 identities, and the 39 dynamic
  imports are retained with reviewed owner/line relocations.
- Largest runtime and combined SCCs fall from 218 to **217**; domain SCC remains
  9. An intermediate serializer dependency grew the component; extracting the
  shared serializer removed that accidental PDF path before baselining.
- Five additional cold-import budgets protect stream contracts, shared settings,
  profiles, runtime and rendering. Runtime import is 36 project modules with no
  higher application or metadata domain; rendering import is 15.

The current containment lane is distinct from the wider FF review. Unresolved
handlers, HTML2ZIP's conversion stub, Qt bootstrap, metadata/format cycles,
conversion-loss reporting and concurrency still need separate work. Full native
conversion can load Catalog via the existing metadata record dependencies, though
it does not load customize, surfaces, library or Core.

Validation artifacts are in
`working-memory/test-results/file-format-containment-2026-10-15/` with per-command
logs and JSON exit records. The first native conversion check exposed a test
scratch-directory leak; the test now supplies `LIUXIN_BASE_DIR` under `tmp_path`.
The ten new runtime regressions passed, including real Unicode HTML-to-EPUB,
external ZIP loading and both host/native startup orders.

The full quality gate passed: basedpyright and mypy clean, both sets of 37 negative
type-contract examples rejected, formatting/lint/complexity clean, 463 annotated
file-format modules, 243 protected modules acyclic, 1,810 whole-package modules
checked and all 21 cold-import budgets passing. Scoped lint/format checks also
passed for the new runtime, composition, stream/settings contracts and serializer.

An additional image-utility run yielded 25 passed and five failures in
`tests/utils/image_tools/test_img_pillow_fallback.py`. All five import the older
Qt-only `utils.image_tools.img` module and fail because PyQt5 is absent. They were
reproduced unchanged against an archived source/test snapshot of `f8723b96` (five
failures), so they are pre-existing; no tests were weakened or newly skipped.
See `image-compat.*` and `image-baseline.*` artifacts. The new native runtime and
rendering checks passed in that same additional run.

The broad regression batch passed: **2,597 passed, 18 environment-dependent or
opt-in skips**, with existing deprecation warnings. This covers file formats,
metadata, customization, architecture and documentation boundaries, Core service
contracts, and metadata renderers. The containment lane is complete; the wider
format defects listed above remain separate follow-up work. The data submodule's
unrelated local state is excluded from the commit.
