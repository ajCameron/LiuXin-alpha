# File-format plugin composition

Decision date: **2026-10-13**

Status: **provider and conversion-contract containment implemented; wider graph remains legacy**

Built-in conversion plugins are implementations under
`LiuXin_alpha.file_formats.conversion.plugins`. The `customize.builtins` package
is only a namespace and must remain safe to import without discovering every
plugin or assembling application services.

The complete historical plugin fleet is composed explicitly by
`LiuXin_alpha.customize.builtins.registry`. Current GUI/plugin-management code
imports that registry because it genuinely needs the fleet. Code that needs one
provider imports that provider module directly.

This distinction is behavioural, not cosmetic:

| Cold import | Before | After |
|---|---:|---:|
| `LiuXin_alpha.customize` | 74 project modules | 31 project modules |
| `LiuXin_alpha.customize.builtins` | 743 project modules | 32 project modules |

The import-footprint ratchet enforces both 35-module ceilings and rejects
crossing into Catalog, caches, Core, databases, file formats, ingest, jobs,
library, metadata, storage, or surfaces from those namespace imports.

Importing `customize.builtins.registry` remains intentionally expensive. It
preserves the existing optional-provider handling, plugin order, and conversion
class identities. This tranche does not claim to break the static conversion SCC:
the explicit registry still depends on its providers, as a composition root should.

Reusable lifecycle and conversion contracts no longer belong to the composition
package:

- `LiuXin_alpha.plugin_system.base` owns the format-neutral `Plugin` lifecycle;
- `LiuXin_alpha.file_formats.conversion.plugin` owns `ConversionOption`,
  `OptionRecommendation`, `DummyReporter`, `InputFormatPlugin`, and
  `OutputFormatPlugin`;
- concrete format providers and internal callers import those owners directly;
- `LiuXin_alpha.customize` and `customize.conversion` retain exact-identity
  compatibility exports for external plugins and historical callers.

The fresh-process ratchet holds the generic and conversion contract imports to
30 project modules and rejects entry into `customize` or any application domain.
This second seam reduced the largest runtime and combined SCCs from 220 to 218.
The remaining component is real debt, chiefly format-to-UI/customization paths;
those dependencies must not be hidden behind dynamic imports merely to improve
the graph measurement.
