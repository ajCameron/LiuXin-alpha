# Layer-direction ratchet

Decision date: **2026-10-11**

Status: **enforced whole-package baseline; existing debt remains explicit**

The project uses two complementary dependency checks:

- `scripts/check_modern_import_cycles.py` retains a zero-cycle, zero-debt
  protected scope for modern APIs and surfaces.
- `scripts/check_layer_direction.py` scans every Python module under
  `LiuXin_alpha`, measures the legacy graph, and prevents existing debt from
  growing.

The whole-package scanner uses Python source-encoding declarations, classifies
import-time, deferred, and type-only edges, recognizes literal calls through
`importlib.import_module` and `__import__`, and reports non-literal dynamic
imports separately. Deferred and type-only spelling does not exempt an
ownership edge.

## Named direction rules

The ratchet encodes the ownership direction documented in
[Separation of concerns](<04 - Seperation of Concerns.md>):

- metadata cannot depend on database, cache, Catalog, storage, Core, Library,
  or surface implementations;
- databases cannot depend upward on cache, Catalog, storage, Library, Core, or
  surfaces;
- storage cannot depend upward on cache, metadata, Catalog, Library, Core, or
  surfaces;
- Catalog cannot depend on storage, Library, Core, or surfaces;
- Library cannot depend on Core or surfaces; and
- Core cannot depend on surfaces.

Cache's long-term rank is intentionally absent until its role is decided.

## Exact baseline

`scripts/layer_direction_baseline.json` records:

- every existing backward edge by source, target, and execution context;
- every unresolved dynamic import with its source line and expression; and
- the largest domain, whole-runtime, and whole-combined strongly connected
  component sizes; and
- zero-sized runtime and combined Catalog/cache cross-package components.

A new edge or unresolved dynamic import fails. A removed allowance or reduced
component size also fails until the baseline is deliberately tightened; this
ensures an improvement is recorded immediately and cannot later regress under
an older ceiling. Diagnostics print the actual source path, line, context, and
target rather than presenting sorted component members as a traversal path.

To inspect candidate baseline data without modifying the repository:

```bash
python scripts/check_layer_direction.py --print-baseline
```

Baseline changes require architectural review. The command never writes the
baseline itself.

## Current measurement

On **2026-10-12**, moving database/cache metadata hydration from `metadata` to
Catalog retrieval reduced the largest domain SCC from 48 modules to 17. On
**2026-10-13**, moving OPF operations out of generic metadata utilities and the
Calibre-record factory beside its record reduced it again to 9. The
reviewed backward-edge inventory fell from 45 to 16; the whole-runtime SCC fell
from 290 to 220 and the whole-combined SCC from 322 to 220. The remaining domain
component is legacy metadata/file-format code plus one Calibre renderer; it no
longer includes cache or database modules. The former four-module
`caches.cache`/`catalog.catalog`/`catalog.retrieval` cycle is also zero in both
runtime and combined graphs. Catalog retrieval composes
`caches.read_cache.ReadCache`, while the writable `caches.cache.Cache` alone adds
the dependency back to Catalog.
