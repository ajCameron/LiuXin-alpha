# Import-footprint ratchet

Decision date: **2026-10-12**

Status: **enforced in tests and the type/quality gate**

Importing a reusable contract or leaf policy must not assemble unrelated parts of
the application. Static import graphs do not fully demonstrate this property because
Python executes every parent package initializer while resolving a submodule.

Run the runtime check with:

```bash
.venv/bin/python scripts/check_import_footprint.py
```

The checker starts a separate interpreter for every boundary. Each probe records the
newly loaded `LiuXin_alpha` modules and enforces two independent rules:

1. a numerical module-count ceiling;
2. forbidden top-level domains that must remain absent even below that ceiling.

The repository gate covers the project root, passive plugin/metadata/catalog/cache/
storage roots, reusable plugin contracts, the generic customization namespaces,
`metadata.identifiers`, the persistence-free `metadata.facade`, and the
catalog/cache/storage API surfaces. The authoritative budgets live in
`scripts/check_import_footprint.py`; they are deliberately small ceilings, not
performance targets to consume.

The 2026-10-12 isolation pass changed the representative cold imports as follows:

| Import | Before | After |
|---|---:|---:|
| `LiuXin_alpha.metadata.identifiers` | 371 | 20 |
| `LiuXin_alpha.metadata.utils` | pulled OPF/OEB implementation code | 28 |
| `LiuXin_alpha.metadata.facade` | eager concrete container graph | 21 |
| `LiuXin_alpha.catalog.api` | 371 | 63 |
| `LiuXin_alpha.caches.api` | 100 | 72 |
| `LiuXin_alpha.metadata` | assembled facade | 2 |
| `LiuXin_alpha.catalog` | assembled concrete Catalog | 2 |
| `LiuXin_alpha.caches` | assembled facade and all bundled plugins | 2 |
| `LiuXin_alpha.customize` | 74 | 31 |
| `LiuXin_alpha.customize.builtins` | 743 | 32 |
| `LiuXin_alpha.plugin_system` | new passive boundary | 2 |
| `LiuXin_alpha.plugin_system.base` | extracted from `customize` | 27 |
| `LiuXin_alpha.file_formats.conversion.plugin` | extracted from `customize.conversion` | 29 |

The remaining modules in `catalog.api` and `caches.api` are chiefly their own
contracts plus database contracts. Persistence-owning concrete workflows
intentionally cost more and are imported explicitly:
`catalog.retrieval.metadata`, `catalog.catalog`, and `caches.cache`. The pure
`metadata.facade` lazily imports OPF/container machinery only when a conversion
is requested.

As of **2026-10-13**, `customize.builtins` is also a namespace rather than a
composition trigger. Consumers that truly need the complete built-in plugin fleet
import `customize.builtins.registry`; provider submodules can be imported without
executing the registry. The generic `customize` root defers its database export
helper until the Catalog-plugin operation that uses it.

The same date also adds a 30-module budget for `metadata.utils`. Its cold import
must not enter file formats, caches, Catalog, Core, databases, Library, storage,
or surfaces. OPF-specific helpers now live under `file_formats.opf`, and the
Calibre-record factory lives beside the record class in `metadata.book.base`.

The conversion-contract containment pass adds 30-module ceilings for the
format-neutral plugin lifecycle and the file-format conversion contract. The
generic contract may not import file formats or `customize`; the conversion
contract may import its own format domain but may not enter `customize`,
databases, metadata, Catalog, caches, Core, Library, storage, or surfaces.

When a legitimate API expansion needs more modules, first confirm that it does not
cross a forbidden domain. Raise a count only with the code change and record the
reason here; never weaken a domain prohibition to accommodate an implementation
import.
