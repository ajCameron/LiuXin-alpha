# File-format plugin composition

Decision date: **2026-10-13**

Status: **first containment seam implemented; wider conversion graph remains legacy**

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

The next containment seam should separate conversion plugin contracts from the
customization implementation, then address format-to-UI dependencies. Neither
dependency should be hidden behind an untracked dynamic import merely to improve
the graph measurement.
