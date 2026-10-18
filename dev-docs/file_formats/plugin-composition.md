# File-format plugin composition

Decision dates: **2026-10-13–2026-10-15**

Status: **standalone file-format containment verified; metadata cycles remain**

Conversion and metadata I/O can select and execute file plugins without importing
application composition. The owners are explicit:

| Owner | Responsibility |
|---|---|
| `plugin_system.base` | Generic plugin lifecycle and resources |
| `plugin_system.file_types` | File hooks and metadata stream contracts |
| `plugin_system.configuration` | Shared persisted customization and enable/disable settings |
| `plugin_system.zipplugin` | Generic external ZIP loading |
| `file_formats.conversion.plugin` | Conversion contracts and options |
| `file_formats.conversion.profiles` | Input/output device profiles |
| `file_formats.conversion.builtins` | Bundled conversion providers |
| `file_formats.import_plugins` | Bundled file-import hooks |
| `metadata.file_sources.builtin_readers` / `builtin_writers` | Bundled metadata I/O providers |
| `file_formats.plugin_defaults` | Explicit standalone file-plugin composition |
| `file_formats.plugin_runtime` | Selection, file hooks, metadata dispatch and host binding |
| `file_formats.rendering` | Headless rendering hooks |
| `file_formats.html.comments` | Plain-text and HTML comment formatting |

The runtime loads its fleet on first use, retaining the historical provider order.
Configured external ZIP plugins precede bundled providers and receive the same
customization settings. Optional or failing providers do not prevent other
providers from loading. Initialization restores stdout/stderr after external
plugin code. LRF registration now selects LRFInput instead of duplicating LITInput.

An application host still imports `customize.builtins.registry` to compose the
complete fleet. `customize.ui.initialize_plugins()` binds its instances into the
file runtime. Both paths then share plugin objects, persisted settings and
metadata context flags. Rebinding rebuilds hook and metadata indexes, including
clearing removed writers. Startup works with either the native runtime or the
application host imported first.

Historical `customize` contracts, provider modules, profiles and UI functions
remain compatibility exports of the same objects. `library.comments` and
`surfaces.gui2` similarly export their new owners. New production format callers
import the owning modules directly.

Headless rendering retains the existing Qt availability/bootstrap behavior. Image
serialization now preserves pixels instead of returning empty bytes; the shared
serializer lives below both rendering and image backends, without importing PDF
processing merely to serialize an image.

## Enforced boundaries

The static layer check rejects every direct file-format import into application
packages, including deferred and type-only imports. Generic plugin mechanics
also cannot import format or metadata implementations. Existing metadata-to-
Catalog/rendering debt stays visible in the whole-package graph. The largest
runtime and combined components fall from 218 to 217 modules; the 16 existing
backward-edge allowances and 39 unresolved dynamic imports remain explicit.

Fresh-process budgets cover the passive boundaries:

| Cold import | Project-module ceiling |
|---|---:|
| `plugin_system.base`, `plugin_system.file_types`, `plugin_system.configuration` | 30 each |
| `file_formats.conversion.plugin`, `file_formats.conversion.profiles` | 30 each |
| `file_formats.plugin_runtime` | 36 |
| `file_formats.rendering` | 15 |
| `customize`, `customize.builtins` | 35 each |

These imports cannot enter application domains. The native HTML-to-EPUB regression
also exercises the real fleet, profiles, metadata and output generation in a fresh
process, checks Unicode content, and rejects loading `customize`, surfaces,
library or Core. Full conversion can still load Catalog through the existing
metadata record/field-metadata dependency; this is not a package-wide isolation
claim.

The broader architecture review remains separate: unresolved handlers, the
HTML2ZIP conversion stub, remaining Qt bootstrap work, metadata/format cycles,
conversion-loss reporting and concurrent conversion state have not all been
resolved by this containment work. See the
[architecture review](../architectural_reviews/architecture-review-2026-09-26.md).

Verified on 2026-10-15: 2,597 regression tests passed, 18 skipped, and the full
repository type/quality gate passed. Five additional legacy image tests still
fail on missing PyQt5; those failures also reproduce on the preceding commit.
