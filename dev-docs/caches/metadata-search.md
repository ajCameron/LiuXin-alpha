# Native cache predicates and Calibre metadata search

Implemented **2026-11-17**. This is the metadata milestone of the Calibre-plus
programme. Content full-text search, note search and annotation search are separate
milestones. This work does not sign off every cache backend or every Calibre UI
integration.

## Ownership and execution

- **Catalog** owns WEMI traversal and bibliographic field meaning. Its existing
  database search remains the default. `catalog.search.compile(text, level=...)`
  returns a physical `CacheQuery`; `find(..., read_cache=cache)` explicitly selects
  cache execution and checks that both objects use the same database.
- **Databases** owns physical predicate vocabulary and SQL execution. Low-level
  efficient SQL belongs here when useful. It does not interpret Calibre books.
- **Caches** owns candidate sets, Boolean evaluation, sorting, paging, bounded
  derived results and lifecycle recovery. It does not evaluate arbitrary SQL.
- **utils.calibre_compat.caches.search.adapter** owns Calibre syntax, typed matching,
  saved definitions and template permission. It compiles Boolean expressions to
  the native evaluator with explicit leaf providers. Native cache modules do not
  import the compatibility adapter.

```python
from LiuXin_alpha.caches.api import CacheQuery
from LiuXin_alpha.databases.db_types.query_types import BooleanMatch, ComparisonMatch

query = CacheQuery(
    "works",
    where=BooleanMatch("and", (
        ComparisonMatch("work_original_year", "gte", 2000),
        ComparisonMatch("work_title", "prefix", "A"),
    )),
    projection=("work_title",),
    limit=20,
)
page = cache.query(query)
# page.total_count is before paging; page.records carry explicit table/row IDs.

plan = catalog.search.compile('work.author:Herbert and title:Dune', level="item")
page = cache.query(plan)
rows = catalog.search.find('title:Dune', level="work", read_cache=cache)
```

`CacheQuery.where` accepts `BooleanMatch`, `TextMatch`, `ValueMatch`,
`IdentifierMatch`, `LinkedMatch`, `ForeignMatch`, `ComparisonMatch`, `CountMatch`
and `ProvidedMatch`. Boolean branches narrow candidate sets. Validation and
reference-integrity checks precede pruning, including branches that cannot match.
Count means collection cardinality (null is zero; a present scalar is one).
`ProvidedMatch` is an explicit execution-only hook; providers must validate their
location and cannot return IDs outside their input candidate set. They are never
implicitly serialized or retained in the native result cache.

SQL supports the established physical vocabulary plus equality, inequality,
ordering, membership, literal substring/prefix and null comparisons. Regex,
collection counts and execution providers currently require cache execution.
Python/SQL coercion is not promised for operands of incompatible physical types.
Native text normalization remains independent of Calibre collation.

## Freshness and bounded work

Snapshot queries retain at most **100 validated query specifications** and **50
matching-ID sets** by default. Limits are configurable, including zero. Sorting,
paging and row materialization happen after membership lookup. Keys snapshot
supported operands; unsupported operands bypass caching. Results carry the facade
generation. Invalidation evicts text indexes and membership results before stale
storage can be read.

After a successful bounded ID refresh, proven row-local queries can repair their
membership. The total changed-ID/query pairs must be **50 or fewer**. Linked,
foreign, provider and indexed-text queries are evicted. Whole-table, schema,
field and link changes also take the conservative eviction path. The repair
budget bounds predicate work; a backend may still reload a whole table. This
optimization currently applies to explicit ID invalidations, not every Catalog
write receipt.

Live backends retain no query membership between requests. `query_view()` lets a
live backend capture table state once per composed query; `database_backed` uses
an ephemeral schema-backed view. The next query captures fresh data. Held live
child objects retain their existing live behavior. Capture does **not** establish
a database transaction: the caller owns isolation when concurrent writes require
an atomic snapshot. This costs memory proportional to the captured tables.

CR02/CR03 are repaired: a failed full/partial refresh leaves the facade DIRTY,
evicts indexes and requires full recovery; an invalidation request validates all
dependencies before mutation. If a backend invalidator itself fails, the facade
already records the dirty dependencies and refuses fresh reads until recovery.
Failed clear/close operations follow the same rule, including recovery from
uninitialized storage. See the [stress-test report](metadata-search-testing-2026-11-17.md)
for null-membership, snapshot-isolation, alias and sorting regressions added before
optimization.

## Explicit Calibre projection

```python
from LiuXin_alpha.metadata.calibre_conversion import CalibreBookRef
from LiuXin_alpha.utils.calibre_compat.caches.search.adapter import CalibreSearchAdapter

view = CalibreSearchAdapter.from_catalog(
    catalog,
    (CalibreBookRef(expression_id=13), CalibreBookRef(expression_id=14)),
)
refs = view.search('tags:fiction and rating:>=3', restriction='languages:eng')
ordered_refs = view.multisort([('title', True)], references=refs)
```

An adapter is an explicit **copied snapshot**. Reconstruct it after Catalog writes
or projection changes. It never labels separate live database reads with an
unrelated cache generation. A caller needing atomic construction must own a
transaction around `from_catalog`. A supplied `records` mapping is also copied;
keys must be `CalibreBookRef`, never an ambiguous bare Work ID.

`catalog.calibre.project(ref)` is the shared display/search projection. Search
uses the selected Expression, selected Work and selected Manifestation. It does
not borrow metadata from sibling Works in an omnibus. Native WEMI search retains
its broader ancestor/descendant semantics. Projection policy and identity helper
fields do not participate in Calibre `all` searches.

| Search value | Projection meaning |
| --- | --- |
| title, sort | Displayed title and its title sort; existing override/fallback policy. |
| author_sort, authors | Selected Work sort; author-role credits on the selected W/E/M context. |
| publisher | Publisher-role credits on that context. |
| comments, rating | Existing explicitly selected Calibre conversion policy. Ratings remain on the stored 0–10 scale; search expressions use stars. |
| tags, genre, subjects, series | Ordered attachments on selected context (and Items for tags/genre/subjects), deduplicated by entity ID. Series uses the first selected attachment and its index. |
| identifiers, uuid | Selected W/E/M identifiers; later, more specific owners override duplicate schemes. |
| languages | Selected Expression language, represented by its canonical code. |
| timestamp, last_modified, pubdate | Expression timestamps and selected Manifestation publication date. |
| formats, size, cover | Linked files on the selected context and its Items; uppercase suffixes, maximum file size, explicit cover links. |
| path | First nonempty Item source path in repository order. |
| native custom fields | Canonical `#attachment.label` values/descriptors for selected owners. Short aliases require unambiguous ownership. Composites remain unevaluated until authorized. |

If several Items supply the same custom-field key, the first Item in repository
order supplies that scalar projection. Native item-level queries remain available
when callers need every copy. Application values such as `ondevice`, `marked` and
`in_tag_browser` require explicit records or `providers={field: {ref: value}}`.
Missing explicit fields raise `UnsupportedCacheQueryError`; no silent database
fallback or fabricated device state occurs.

## Definitions and templates

`SearchDefinitions(preferences)` stores query text, virtual libraries, grouped
search terms and user categories under `calibre_compat.metadata_search.v1`.
Definition changes invalidate parse/result caches. Saved-search and virtual-library
references expand before evaluation with cycle detection. Restrictions intersect
the query and any explicitly supplied reference subset.

**DANGER: templates are arbitrary code with the process's access to files,
credentials and network. They are not sandboxed.** Template queries and composite
fields are disabled by default, including when reached through saved searches,
groups, restrictions or sorting. An impossible Boolean branch cannot bypass this
check. Review all referenced code before issuing a capability:

```python
from LiuXin_alpha.utils.calibre_compat.caches.search.adapter import TrustedTemplateExecution

permission = TrustedTemplateExecution(
    trusted_local=True,
    acknowledge_arbitrary_code_execution=True,
)
refs = view.search('template:"{title}#@#:t:Dune"', template_execution=permission)
```

The capability emits a prominent warning and is consumed once. Authorization
covers nested metadata searches during that call and is cleared on success or
failure. It is never persisted with definitions. `remote=True` rejects enabling
execution; a remote surface must set that argument itself, never accept a client's
claim to be a trusted local caller. Composite sorting takes the same capability.

Current pinned Calibre template grammar, including general-program and Python
modes, is retained. `template_functions={name: function_object}` explicitly injects
reviewed formatter-compatible functions (`name`, `arg_count`, `object_type`,
`eval_`). No process-global user-function registry is imported. Pure metadata
functions such as `book_count`/`book_values` use the supplied snapshot. Functions
that need a GUI, device, note store or application service require an explicit
injected implementation; they are not a claim of whole-application parity.
Template/composite and date-dependent searches retain no result sets. Each call
uses one request clock; tests can supply `now=`.

## Parity ledger and next milestones

Reference: `dev-docs/calibre-master.zip`, revision
`5f53fce641f180cfaf0e976e703302d596e4fb2a`. This is a pinned reference, not an assertion
about whatever Calibre publishes next. The copied matcher/parser/formatter retain
GPL notices and dated modification records; see the packaged
[provenance manifest](../../src/LiuXin_alpha/utils/calibre_compat/caches/search/provenance.json).

| Capability | Metadata milestone status |
| --- | --- |
| AND/OR/NOT, nesting, phrases, escapes, default all | Pinned parser and native candidate-set execution. |
| Field aliases, exact/hierarchical/regex/accent text | Pinned matching branches; existing LiuXin ICU bridge. Full locale parity with Calibre's native ICU is **not verified**; punctuation-insensitive matching has a Unicode-category fallback. |
| Numbers, sizes, ratings, bool/tristate, dates, relative dates | Pinned typed matchers; explicit request clock. |
| Multi-values and counts, identifier key/value queries | Shared-value groups and typed iteration bridge. |
| Custom numeric/text/boolean/enumeration/date/series fields | Explicit descriptors, including native values through Catalog projection. |
| Saved searches, restrictions, virtual libraries, grouped fields, user categories | Explicit definitions, recursive expansion and invalidation. |
| Templates and composite fields | Current pinned evaluator; mandatory one-use local permission. Application-only functions need providers. |
| Result sorting | Stable multiple keys, title/author sorts, series indices, numeric/date composite sort, missing values last. Not a claim of every GUI sort preference. |
| Content FTS | **SEARCH02:** format extraction, index lifecycle and query API. |
| Notes | **SEARCH03:** note-store provider and search/index lifecycle. |
| Annotations | **SEARCH04:** annotation provider and search/index lifecycle. |
| Remaining application parity | **SEARCH05:** device/UI providers, locale differential corpus, and measured large-library tuning. |

Regression entry points: `tests/databases/caches/test_predicate_queries.py`,
`test_calibre_search_adapter.py`, `tests/catalog/test_cache_search_execution.py`
and `test_calibre_conversion_policy.py`. The existing cache suite covers held
objects and write reconciliation. The working-memory entry records commands,
results and environment limits for this implementation.

## Local performance sample

`python scripts/benchmark_metadata_search.py --records 10000 --repeats 20`
(Python 3.12.3, WSL/Linux, 2026-11-17) measured 0.633 s construction,
0.301 s cold search and 0.00309 s median warm search for 2,666 matches.
Peak traced allocation during construction/search was 14,936,956 bytes, excluding
imports and the already-created input records. Repairing one native predicate for
ten changed rows took 0.000964 s. Tracing was enabled for the adapter timings.

This is a synthetic copied-metadata workload, not a database-capture benchmark,
whole-process memory measurement or latency guarantee. The script checks result
identity and repaired membership, and prints machine-readable telemetry for
repeatable comparisons.
