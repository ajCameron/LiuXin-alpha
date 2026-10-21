# Preferred WEMI path selection

Implemented for review, **2026-10-20**, architecture A07.

Catalog owns preferred-path policy in
[`selection.py`](../../src/LiuXin_alpha/catalog/retrieval/selection.py).
`retrieval.bundles.for_work`, `for_expression`, `for_manifestation` and `for_item`
apply it to paths through the requested record. Repository lists and graph
membership are separate contracts.

## Ranking

Choose a complete Work–Expression–Manifestation path when one exists. Otherwise
retain the path with the most available WEM levels, keeping missing levels as
`None`. Within that group, compare:

1. Work–Expression primary flag descending, then priority ascending.
2. Expression–Manifestation primary flag descending, then priority ascending.
3. Expression ID, Manifestation ID and Work ID ascending.

Null flags and priorities mean zero. The Expression ID tie breaker comes **after**
both edges: a preferred edition can settle a tie between translations. Conflicting
primary flags still produce a deterministic result. Selection never repairs flags
or promotes siblings after unlinking.

Items do not rank bibliographic paths. Select the lowest Item ID on the chosen
Manifestation, or `None` if it has no Items. `for_item` always retains its requested
Item. A preferred translation without an edition cannot hide another complete
path; a preferred edition without an Item can still win.

For a fixed Work with any complete ray, this matches the existing
[`wemi_primary_rays_v`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/aggregate_sql/wemi_views.sql)
ordering. That SQL view contains complete WEM paths only, so it has no row when
Catalog returns a partial bundle. The SQL ranking is unchanged; existing views
need no migration for this change.

## Scope and membership

Each method fixes its requested record. A path through a specified Manifestation
need not be its Work's globally preferred edition. For a volume containing a novel
and an introduction, the default chooses one path; hierarchy, graph and
`wemi_rays_v` reads retain both Works. Ranking ancestors of a shared edition is a
deterministic projection, not a claim about its principal content or page order.

Portable repository lists retain their existing descending-priority order. They
return all related rows, not an already-selected default. Bundles collect metadata
only from their chosen path. Candidate and attachment reads are not an atomic
snapshot, and selection may inspect multiple branches. This change adds no
pagination or query-cost guarantee.

## Metadata relation selection is a different question

Passive metadata containers expose `primary_relation_link` and
`primary_expression_id` for one relation bucket. Those accessors prefer a primary
flag, then lower priority/index and input order; absent order values sort last.
Hydration's compatibility metadata spine uses those local choices and source
hints. It does not claim to select SQL's best complete ray.

Use `retrieval.bundles` for preferred complete-path projections. Use metadata
hydration for relation-rich containers and compatibility rendering. Both retain
the other related Works, but a primary Expression in a hydrated relation bucket
can differ from a bundle's Expression when its Work edge ranks differently. The
regression below explicitly demonstrates that distinction instead of silently
calling both results the same default.

## Evidence

[`test_wemi_selection.py`](../../tests/catalog/retrieval/test_wemi_selection.py)
compares actual generated SQL views and Catalog calls through SQLite and APSW.
It covers translations, competing primary flags, nulls, negative priorities, ties,
missing relationships/Items, shared Expressions, introductions, bilingual editions,
omnibuses and the public primary-change/unlink operations. It also checks retained
hydrated Work relationships and the separate relation-local selector.

The [worked WEMI examples](../databases/wemi-link-examples.md) show the resulting
paths. Core's category/tag/label semantics are unchanged.
