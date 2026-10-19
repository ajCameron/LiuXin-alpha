# File-format plugin composition

Decision dates: **2026-10-13–2026-10-15**

Updated: **2026-10-19**, explicit application ownership and shared startup policy.

Status: **standalone containment and application startup boundaries implemented;
metadata cycles and unsupported application providers remain**

Conversion and metadata I/O can select and execute file plugins without importing
application composition. The owners are explicit:

| Owner | Responsibility |
|---|---|
| `plugin_system.base` | Generic plugin lifecycle and resources |
| `plugin_system.file_types` | File hooks and metadata stream contracts |
| `plugin_system.configuration` | Shared persisted customization and enable/disable settings |
| `plugin_system.zipplugin` | Generic external ZIP loading |
| `plugin_system.loading` | Shared initialization, priority and failure rules for explicitly supplied providers |
| `file_formats.conversion.plugin` | Conversion contracts and options |
| `file_formats.conversion.profiles` | Input/output device profiles |
| `file_formats.conversion.builtins` | Bundled conversion providers |
| `file_formats.import_plugins` | Bundled file-import hooks |
| `metadata.file_sources.builtin_readers` / `builtin_writers` | Bundled metadata I/O providers |
| `file_formats.plugin_defaults` | Explicit standalone file-plugin composition |
| `file_formats.plugin_runtime` | Selection, file hooks, metadata dispatch and host binding |
| `file_formats.rendering` | Headless rendering hooks |
| `file_formats.html.comments` | Plain-text and HTML comment formatting |
| `customize.application` | Application registry lifecycle, lookups and installed-plugin management |
| `customize.ui` | Compatibility exports of the application and file APIs; no registry state |

The runtime loads its fleet on first use. Both hosts use the shared loader's stable
descending priority order; external ZIP plugins precede bundled providers at equal
priority, and registration order breaks remaining ties. This corrects the former
discarded application priority sort. Both paths use the same customization settings.
Ordinary provider failures do not prevent other providers from loading; interrupts
propagate. Loading and initialization restore stdout/stderr after each provider.
LRF registration selects LRFInput instead of duplicating LITInput.

An application host calls `customize.application.initialize_plugins()` explicitly;
application lookups also initialize on first use for compatibility. Importing the
API or `customize.ui` does not discover or initialize providers. The application
provider catalogue extends `file_formats.plugin_defaults.builtin_plugins()` with
application-only families instead of independently assembling the file providers.

The application binds its exact instances into the file runtime. Both paths then
share plugin objects, persisted settings and metadata context flags. A shared
startup lock prevents concurrent first lookups from creating duplicate registries.
Reentrant format lookups during startup see an empty fleet; reentrant application
lookups see the previous host snapshot, or an empty snapshot on first startup.
An aborted refresh restores the preceding binding and leaves startup retryable.
Rebinding rebuilds hook and metadata indexes, including clearing removed writers.
These guarantees cover registry publication, not concurrent conversions or reversal
of arbitrary plugin initialization side effects.

A bundled name shadows a conflicting installed entry without mutating settings or
deleting its archive. Startup no longer invokes removal recursively. Explicit
installation/removal remains an application operation.

Historical `customize` contracts, provider modules, profiles and UI functions
remain compatibility exports of the same objects. `library.comments` and
`surfaces.gui2` similarly export their new owners. New production format callers
import the owning modules directly. Production imports of `customize.ui` are
rejected by the static layer gate, including deferred and type-only imports.

The top-level split is intentional: neutral mechanics in `plugin_system`, file
policy in `file_formats`, and application composition in `customize.application`.
The inherited `customize` package name remains for compatibility; unrelated cache
and folder-driver remnants inside it still need their own retirement decisions.
Database, storage and terminal plugin registries remain separate domain owners.

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
| `plugin_system.base`, `plugin_system.file_types`, `plugin_system.loading` | 30 each |
| `plugin_system.configuration` | 31 |
| `file_formats.conversion.plugin`, `file_formats.conversion.profiles` | 31 each |
| `file_formats.plugin_runtime` | 37 |
| `file_formats.rendering` | 16 |
| `customize`, `customize.builtins` | 35 each |
| `customize.application` | 44 |
| `customize.ui` | 45 |

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
