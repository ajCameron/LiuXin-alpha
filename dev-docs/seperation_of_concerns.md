# Separation of concerns: `surfaces`, `library`, `cache`, and `database`

## Note on terminology

This project uses `surfaces` as the name for the top-level external-facing layer of the program.

That includes things like CLI entrypoints, import/export paths, RSS output, APIs, automation hooks, and other ways the system is exposed to or driven by the outside world. Some of these are human-facing, some are machine-facing, but they are all part of the same outer *couche* [layer] of interaction.

`surfaces` does **not** mean interfaces in the usual Python sense of protocols, ABCs, or internal contracts. Those are separate concerns and should not be confused with the external interaction layer.

The purpose of this name is to make the dependency direction *clair* [clear]: `surfaces` sits at the top of the stack and depends on the layers beneath it.

---

## Intended module responsibilities

### `database`

`database` owns canonical persisted truth.

It is responsible for storage, retrieval, transactions, relational integrity, schema-aware operations, and enforcement of the rules that make the stored data valid. If something is part of the durable record of the library, it belongs here.

The database layer should not contain presentation logic, human-readable formatting, or broader workflow policy unless that policy is required to preserve integrity. It stores what is true, *rien de plus* [nothing more].

### `cache`

`cache` owns derived, rebuildable, non-authoritative state.

Its job is performance: speeding up reads, memoizing expensive lookups, precomputing useful derived structures, and materializing views that can be recreated from canonical data. The cache exists to make the system faster, not to become a second source of truth.

A good rule is simple: if deleting the cache would destroy essential library state, then the cache is holding the wrong thing. Cache contents should be disposable, *toujours* [always], and recoverable.

### `library`

`library` owns system-level orchestration and policy.

This is the operational core of the program. It coordinates the database, cache, stores, plugins, and internal services into one coherent library model. If an action spans multiple subsystems or represents a meaningful operation on the library as a whole, it probably belongs here.

The library layer should not be a dumping ground for UI code or transport-specific formatting. Its concern is what the system does and how its parts work together, *ensemble* [together].

### `surfaces`

`surfaces` owns interaction with the outside world.

This includes anything that presents, accepts, translates, imports, exports, publishes, or otherwise mediates between internal library operations and external consumers. That might mean a CLI, a UI, an RSS feed, an HTTP API, a sync endpoint, an importer, or some future machine-facing interface.

The `surfaces` layer is responsible for:
- input handling
- output formatting
- transport and representation concerns
- adapting external requests into library operations
- adapting internal results into external forms

It is not responsible for canonical storage or for owning the underlying library model. It is the outer *surface* [surface] of the system, not its core.

---

## Dependency direction

The intended dependency direction is:

`database` -> `cache` -> `library` -> `surfaces`

In practice:

- `database` provides canonical persistence
- `cache` derives fast-access state from canonical persistence
- `library` coordinates the system as a whole
- `surfaces` exposes that system externally

Put another way:

- `database` says **what is true**
- `cache` says **what is fast**
- `library` says **what the system does**
- `surfaces` says **how the outside world interacts with it**

This dependency direction should be kept *strict* [strict]. Once lower layers start depending on higher ones, the architecture begins to blur and responsibilities stop being trustworthy.

---

## What should generally be avoided

### `database` should not:
- format values for display
- contain CLI/UI/API-specific logic
- depend on `library` or `surfaces`

### `cache` should not:
- own canonical state
- silently become required for correctness
- contain transport or presentation concerns

### `library` should not:
- become a UI or API layer
- duplicate low-level database mechanics unnecessarily
- embed representation-specific formatting

### `surfaces` should not:
- become the real home of business logic
- bypass the library layer casually for writes
- define canonical truth

Short version: outer layers may translate and present; inner layers must remain clean and *stable* [stable].

---

## Practical placement test

When deciding where code belongs, ask:

1. Is this canonical persisted truth or integrity logic?  
   Put it in `database`.

2. Is this derived, disposable, and performance-oriented?  
   Put it in `cache`.

3. Is this orchestration or policy across subsystems?  
   Put it in `library`.

4. Is this about ingress, egress, presentation, translation, or external interaction?  
   Put it in `surfaces`.

If the honest answer is “two of these,” split the object or function rather than letting one module absorb both concerns.

---

## Suggested structure inside `surfaces`

`surfaces` is expected to contain subpackages for different kinds of external interaction. For example:

- `surfaces.user_interfaces`
- `surfaces.api`
- `surfaces.rss`
- `surfaces.import_export`
- `surfaces.automation`

The exact structure can evolve, but the important point is that these are all subdomains of the same outer layer: ways the program is exposed to and interacted with from the outside.

This keeps the top-level naming *propre* [clean] while still allowing fine-grained organisation underneath.

---

## Example: metadata and formatting

A useful sanity check is field or metadata handling.

A field definition that describes canonical semantics — type, nullability, multiplicity, link behaviour, storage meaning — belongs in the system core.

A formatter or adapter that decides how that field is shown in a CLI, serialized into RSS, exposed via an API, or joined for display belongs in `surfaces`.

If these are mixed together, storage semantics and external representation become tangled. That leads to brittle code, confused ownership, and hard-to-reason-about dependencies.

---

## Design intent

The architecture should be read from the inside out:

- the `database` preserves truth
- the `cache` accelerates access
- the `library` coordinates behaviour
- the `surfaces` layer exposes the system externally

That is the separation of concerns this project is trying to preserve.