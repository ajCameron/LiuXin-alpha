# Plugin composition and import migration

Decision dates: **2026-10-13–2026-10-15**

Updated: **2026-10-19**, unified `plugin_system` package, explicit application
ownership and shared startup policy. Supersedes the earlier package split.

Status: **standalone containment and application startup boundaries implemented;
metadata cycles and unsupported application providers remain**

This is the shared contract for application and standalone file-plugin hosts.
Conversion and metadata I/O can select and execute file plugins without importing
application composition. The [package index](README.md) routes changes to their
owners:

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
| `plugin_system.application` | Application registry lifecycle, lookups and installed-plugin management |
| `plugin_system.contracts` | Application plugin families formerly defined in the customization root |
| `plugin_system.builtins` | Application catalogue and bundled application families |

## Startup and provider policy

The runtime loads its fleet on first use. Both hosts use the shared loader's stable
descending priority order; external ZIP plugins precede bundled providers at equal
priority, and registration order breaks remaining ties. This corrects the former
discarded application priority sort. Both paths use the same customization settings.
Ordinary provider failures do not prevent other providers from loading; interrupts
propagate. Loading and initialization restore stdout/stderr after each provider.
LRF registration selects LRFInput instead of duplicating LITInput.

An application host calls `plugin_system.application.initialize_plugins()` explicitly;
application lookups also initialize on first use for compatibility. Importing the
API does not discover or initialize providers. The application
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

Plugin mechanics and application composition now share `plugin_system`.
Module-level dependency rules keep neutral leaves below application assembly and
prevent standalone formats from importing application families. Package and
application API imports are passive. The old `customize` namespace and forwarding
modules are removed rather than aliased. Retained cache/folder-driver modules move
with their callers but still need their own retirement decisions. Database,
storage and terminal plugin registries remain separate domain owners.

## Import migration

External ZIP plugins must update old imports and be repackaged. There is no
`customize` compatibility namespace in the source tree or installed wheel.
Module paths below are relative to `LiuXin_alpha`; import from the full owner,
for example `from LiuXin_alpha.plugin_system.base import Plugin`. Importing
`Plugin` from the passive `plugin_system` package root is not supported.

| Former import | Current owner |
| --- | --- |
| `customize.Plugin`, `PluginPreferences` | `plugin_system.base` |
| `customize.FileTypePlugin`, `MetadataReaderPlugin`, `MetadataWriterPlugin` | `plugin_system.file_types` |
| Application families such as `customize.CatalogPlugin`, `Archive`, `StoreBase` | `plugin_system.contracts` |
| Application startup/lookup/management from `customize.ui` | `plugin_system.application` |
| File dispatch from `customize.ui` | `file_formats.plugin_runtime` |
| `customize.conversion`, `customize.profiles` | `file_formats.conversion.plugin`, `file_formats.conversion.profiles` |
| `customize.zipplugin` | `plugin_system.zipplugin` |
| `customize.builtins.conversion`, `customize.builtins.on_import` | `file_formats.conversion.builtins`, `file_formats.import_plugins` |
| `customize.builtins.metadata_readers`, `customize.builtins.metadata_writers` | `metadata.file_sources.builtin_readers`, `metadata.file_sources.builtin_writers` |
| Other retained `customize` modules (`api`, `archives`, `builtins`, `cache`, driver/trigger modules) | Same relative modules under `plugin_system` |

Repository callers, test streams and ZIP-plugin examples use these owners.
`library.comments` and `surfaces.gui2` retain their separate compatibility exports.

## Related format boundaries

Headless rendering retains the existing Qt availability/bootstrap behavior. Image
serialization now preserves pixels instead of returning empty bytes; the shared
serializer lives below both rendering and image backends, without importing PDF
processing merely to serialize an image.

## Enforced boundaries

The static layer check rejects every direct file-format import into application
packages or application modules within `plugin_system`, including deferred and
type-only imports. Generic plugin mechanics
also cannot import format or metadata implementations. Existing metadata-to-
Catalog/rendering debt stays visible in the whole-package graph. At the current
checkpoint the largest runtime and combined components contain 217 modules;
15 reviewed backward edges and 28 unresolved dynamic imports remain explicit.
The package merger adds no backward-edge allowance or larger cycle ceiling.

Fresh-process budgets cover the passive boundaries:

| Cold import | Project-module ceiling |
|---|---:|
| `plugin_system` | 2 |
| `plugin_system.base`, `plugin_system.file_types`, `plugin_system.loading` | 30 each |
| `plugin_system.configuration` | 31 |
| `file_formats.conversion.plugin`, `file_formats.conversion.profiles` | 31 each |
| `file_formats.plugin_runtime` | 37 |
| `file_formats.rendering` | 16 |
| `plugin_system.contracts` | 35 |
| `plugin_system.builtins` | 3 |
| `plugin_system.application` | 44 |

Neutral imports cannot enter application domains or application plugin modules,
even when those modules share the `plugin_system` prefix. The native HTML-to-EPUB regression
also exercises the real fleet, profiles, metadata and output generation in a fresh
process, checks Unicode content, and rejects loading application plugin assembly, surfaces,
library or Core. Full conversion can still load Catalog through the existing
metadata record/field-metadata dependency; this is not a package-wide isolation
claim.

The broader architecture review remains separate: unresolved handlers, the
HTML2ZIP conversion stub, remaining Qt bootstrap work, metadata/format cycles,
conversion-loss reporting and concurrent conversion state have not all been
resolved by this containment work. See the
[architecture review](../architectural_reviews/architecture-review-2026-09-26.md).

## Verification checkpoints

The 2026-10-19 package merger (`78a01b61`) passed 319 focused regression tests,
35 overlapping follow-up tests, the full type/quality gate and actual installed-wheel
verification. These checks cover startup, ordering, failures, namespace retirement,
internal import direction and the installed conversion path. They are not a new
full-suite result. Commands and limits are recorded in the
[merger handoff](../../working-memory/plugin-package-merge-2026-10-19.md).

Earlier containment checkpoint, 2026-10-15: 2,597 regression tests passed, 18 skipped, and the full
repository type/quality gate passed. Five additional legacy image tests still
fail on missing PyQt5; those failures also reproduce on the preceding commit.
