# Why `cache` should be a top-level module

As the rewrite has progressed, the cache has stopped looking like a thin database helper and started looking like its own architectural **couche [layer]**. It now has its own **objets [objects]**, **règles [rules]**, **état [state]**, and update **logique [logic]**: field objects, views, projections, ordering, filtering, invalidation, and refresh behaviour. That is a different concern from raw persistence. For that reason, cache should be treated as a first-class module rather than continuing to live half-inside `databases` and half-inside `library`.

## Responsibility split

The cleanest boundary is:

- `databases` owns storage reality
- `cache` owns semantic in-memory behaviour
- `library` owns higher-level user-facing behaviour and compatibility shims

Put another way: `databases` should answer “what is stored, and how do we read or write it?” The `cache` layer should answer “how is that data exposed as fields, views, projections, and ordered id sets?” The `library` layer should answer “how do callers use that machinery in a convenient and domain-friendly way?” This keeps the architectural **frontière [boundary]** **claire [clear]** and stops low-level persistence code from accreting view state and field semantics.

## What belongs in `databases`

`databases` should contain the raw storage-facing **réalité [reality]** of the system: drivers, schema introspection, rows, table APIs, link-table APIs, and low-level update objects. It is the home of persistence concerns such as SQL structure, link cardinality, row creation, and backend-specific behaviour. These APIs should be as close to storage truth as possible, even when wrapped in object-oriented interfaces.

This means `databases` should not own higher-level browsing or projection behaviour. A database row is not a cache view row. A database table is not a cache field. A SQL view is not a `CacheView`. Keeping these distinctions **nettes [clean]** matters, because otherwise the storage layer becomes responsible for semantics that are really consequences of in-memory modelling.

## What belongs in `cache`

The `cache` module should own the semantic in-memory **machinerie [machinery]** that sits above raw storage. This includes field objects, field registries, cache views, projections, filtered/ordered id sets, search restrictions, refresh/invalidation behaviour, and other structures that present persisted data in a stable and useful form.

A cache view, for example, is not a database view. It is a stateful **projection [projection]** over cached data: an ordered set of ids plus field access and filter/sort/search behaviour. That makes it a cache concern, not a database concern. Likewise, fields in cache are semantic accessors over data, not just lightly disguised link-table wrappers. Treating `cache` as its own module gives these concepts a proper **maison [home]**.

## What belongs in `library`

`library` should sit above `cache` and provide domain-facing **sucre [sugar]**, compatibility helpers, and broader orchestration. That is the right place for things that are convenient, legacy-shaped, or caller-oriented, but which are not fundamental to storage or cache semantics themselves.

This also provides a place to preserve compatibility with older APIs without polluting lower layers. For example, book-centric helper methods, higher-level presentation logic, and domain-specific convenience wrappers belong here rather than in `cache` or `databases`.

## Dependency direction

The intended dependency direction should be strict:

`databases -> nothing above it`  
`cache -> databases`  
`library -> cache` (and `databases` only where genuinely necessary)

This rule matters more than the module names themselves. A top-level `cache` module is only an improvement if its **frontière [boundary]** remains sharp. Database code should not reach upward into view state or field presentation logic. Cache code should not import arbitrary library-level formatting or compatibility behaviour. If that discipline is kept, the resulting structure remains **propre [clean]** and scalable.

## Naming notes

`CacheView` is a useful base name because it distinguishes this concept from a database view immediately. That distinction will become more important as more specialised view types are introduced later. A likely family might include `CacheView` as the common base, with more specific implementations layered on top for filtered, materialised, mutable, or search-result views.

The same naming principle applies elsewhere: names in `databases` should describe storage-facing concepts, names in `cache` should describe semantic in-memory concepts, and names in `library` should describe caller-facing concepts. Keeping those names **honnêtes [honest]** will help keep the boundaries honest too.

## Rough module layout

A sensible rough layout would be:

- `databases/`
  - drivers
  - schema specs
  - rows
  - storage tables
  - link tables
- `cache/`
  - fields/
  - views/
  - projections/
  - registries/
  - invalidation/
- `library/`
  - compatibility helpers
  - domain-facing APIs
  - higher-level orchestration

This is not meant as dogma. It is simply the current best architectural reading of the rewrite: storage concerns in `databases`, semantic in-memory concerns in `cache`, and user-facing/domain-facing concerns in `library`.

## Design rule

When deciding where a new piece of code belongs, use this test:

- If it primarily describes persisted structure or backend behaviour, it belongs in `databases`.
- If it primarily describes semantic in-memory access or projection, it belongs in `cache`.
- If it primarily exists to make caller usage easier or more domain-friendly, it belongs in `library`.

That rule should keep the architecture **sobre [sober]** and prevent the cache from becoming either a second database layer or a second library layer.