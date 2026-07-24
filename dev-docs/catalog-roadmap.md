# Catalog roadmap

Status: active, 2026-07-24.

Catalog is fit for its core semantic-persistence purpose. The remaining work is
an ordered hardening and consolidation programme rather than completion of an
unfinished facade.

## Work list

1. **Final matching policy — complete, 2026-07-22.** The arbitrary
   average-field scorer has been replaced by the evidence-based policy in
   `catalog-matching-policy.md`. Matching distinguishes a unique match, no
   match, ambiguity, and contradictory evidence; exact identifiers and scoped
   WEMI identity have explicit semantics; Tags, Labels, Genres, Subjects,
   Series, Languages, Ratings, Comments, Synopses, Notes, Annotations, and raw
   Item identifiers now have explicit exact-default policies; automatic
   creation stops on ambiguity or conflict, and approximate value policy is
   opt-in.
2. **Legacy mutation migration — complete, 2026-07-23.** Production callers
   now use repositories, coordinated mutations, or normalized writers. Both
   the direct-import and indirect-facade allowlists are empty. The legacy
   implementations remain frozen in place for later, measured pure-SQL fast
   paths, with characterization coverage and a guard against new production
   callers. The completed contract, caller map, and evidence live in
   `catalog-legacy-mutation-migration.md`.
3. **Modern Catalog/cache boundary — adopted, 2026-07-24.** With an attached
   modern storage cache, application and library reads and writes should prefer
   the cache facade. Cache writes delegate to Catalog for authoritative
   semantic persistence and reconcile only after success; Catalog never calls
   upward into cache state. The complete contract is in
   `catalog-cache-boundary.md`.
4. **Calibre compatibility ownership.** Move Calibre-cache search and the
   remaining compatibility cache implementations under `utils`. Consolidate
   duplicated field-metadata implementations behind one mapping contract.
   Compatibility relocation must preserve the modern
   `cache -> Catalog -> database` direction.
5. **Database startup and test performance.** Profile full FRBR generation,
   aggregate-view construction, and schema introspection. Retain correctness
   while making catalog development and example startup predictable.
6. **Graph retrieval policy.** Keep deterministic WEMI-path bundles as the
   default, then decide whether catalog needs an explicit full-graph query or
   caller-supplied path selection policy.
7. **Repository-wide static boundary.** Preserve catalog's isolated strict
   type-clean surface while reducing the legacy import graph that prevents the
   configured whole-project checks from being meaningful gates.

## Working rule

Only one item should be considered active at a time. Each item needs a written
contract, focused behavioral tests, real-database coverage where persistence
matters, and an updated status here before the next item starts.
