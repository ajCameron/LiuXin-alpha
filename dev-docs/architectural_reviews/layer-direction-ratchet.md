# Layer-direction ratchet

Decision date: **2026-10-11**. Source and checks reviewed **2026-10-20**,
working tree based on `fc5de7af`.

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
[Separation of concerns](<../04 - Seperation of Concerns.md>):

- metadata cannot depend on database, cache, Catalog, storage, Core, Library,
  or surface implementations;
- databases cannot depend upward on cache, Catalog, storage, Library, Core, or
  surfaces;
- storage cannot depend upward on cache, metadata, Catalog, Library, Core, or
  surfaces;
- Catalog cannot depend on storage, Library, Core, or surfaces;
- Library cannot depend on Core or surfaces; and
- Core cannot depend on surfaces.

The plugin/format rules, updated on **2026-10-19**, also apply within the merged
`plugins` package:

- File formats cannot import application plugin modules, caches, Catalog, Core,
  databases, ingest, jobs, Library, storage or surfaces. Imports of the retired
  `customize` namespace remain forbidden.
- Neutral plugin modules cannot import concrete domain implementations or the
  application subtrees `application`, `contracts`, `archives`, `builtins`,
  `cache` and `folder_store_driver`. New plugin modules default to the neutral
  rules unless explicitly reviewed as application modules.
- The whole plugin package, including application assembly, cannot import Core,
  ingest, jobs, storage or surfaces.

The authoritative host-prefix list is `PLUGIN_HOST_PREFIXES` in
[the layer checker](../../scripts/check_layer_direction.py); the cold-import
checker reuses it. Combining the packages did not relax those boundaries. See
the [composition contract](../plugins/plugin-composition.md).

A broad source-package rule for `caches` is intentionally absent from
`FORBIDDEN_TARGETS`. This does not leave its semantic ownership undecided:
Catalog owns persistent metadata reads/writes, `ReadCache` supplies reusable read
state, and writable `Cache` delegates mutations to Catalog. The
[cache contract](../catalog/catalog-cache-boundary.md) governs those roles; the
checker still needs a reviewed rule for the remaining mixed cache dependencies.

## Exact baseline

`scripts/layer_direction_baseline.json` records:

- every existing backward edge by source, target, and execution context;
- every unresolved dynamic import with its source line and expression;
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
.venv/bin/python scripts/check_layer_direction.py --print-baseline
```

Baseline changes require architectural review. The command never writes the
baseline itself and does **not** check agreement with the existing baseline.
Run the normal check to enforce it:

```bash
.venv/bin/python scripts/check_layer_direction.py
```

## Current measurement

The normal check passed on **2026-10-20**: **1,792 source modules, 15 reviewed
backward edges and 28 unresolved dynamic imports**. Candidate measurements agree
with the checked-in baseline: largest runtime/combined components **217/217**,
domain component **9**, Catalog/cache cross-package components **0/0**. The
protected modern check separately passes for **243 modules**.

A strongly connected component (SCC) is a set of mutually reachable modules. The
domain measurement uses the induced graph of caches, Catalog, databases, metadata
and surfaces; whole-runtime includes import-time and deferred edges, while
whole-combined adds type-only edges. The Catalog/cache measurement detects SCCs
containing both packages. These scopes are not interchangeable: a zero result
there does not mean the entire graph is acyclic or that the packages have no edges.

On October 19 the package merger relocated three dynamic-import entries without
adding an allowance or increasing a cycle ceiling. These counts remain unchanged
at this source checkpoint. Recognized literal imports do not resolve arbitrary
dynamic expressions, and parent-package execution needs the complementary
[cold-import check](import-footprint-ratchet.md).

## Measurement history

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

Later on **2026-10-13**, extracting the generic plugin lifecycle and conversion
plugin contracts from `customize` reduced both whole-package SCC measurements
again, from 220 to 218. The domain SCC remained 9; format-to-UI and customization
dependencies were still represented in that checkpoint's graph. Later containment
and the unified plugin package supersede those import paths.
