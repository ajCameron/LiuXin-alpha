# WEMI search

Implemented **2026-10-29**. `catalog.search.find()` searches Works, Expressions,
Manifestations and Items using their relationships and attached metadata. It
replaces the unfinished `databases.metadata_sql.database_search.DatabaseSearch`
prototype, which had no working search callers and is now removed.

## Entry point and results

```python
from LiuXin_alpha.catalog import Catalog

catalog = Catalog(db)
works = catalog.search.find('title:"Frankenstein" and author:"Mary Shelley"')
copies = catalog.search.find('work.title:"Frankenstein"', level="item")
editions = catalog.search.find('translator:"Smith"', level="manifestation")
```

The default result level is `work`. The other accepted levels are `expression`,
`manifestation` and `item`. Results are tuples of copied row dictionaries using
physical column names, ordered by ascending ID at the requested level. Changing
a returned dictionary does not persist a change. Search does not construct
legacy title IDs or collapse multiple WEMI entities into a preferred ray.

`limit=100` and `offset=0` are the defaults. Pagination applies to matching results,
not the initial candidate rows. `limit=None` returns all matches and `limit=0`
returns none without database access. An empty query lists the requested level.
Malformed expressions and invalid options raise before backend reads. Database
errors and missing linked rows propagate rather than becoming empty results.

## Query language

The shared query parser supports `and`, `or`, `not`, parentheses, implicit AND and
quoted phrases. Terms normally use substring matching. Prefix the term with `=`
for whole-value equality, for example `work.title:"=Frankenstein"`.

Both modes apply Unicode NFKC, case folding and whitespace collapse. Punctuation
and accents remain significant. SQL wildcards and quotes are ordinary characters;
query text is never interpolated into SQL. Regex, numeric comparison, saved-search
expansion and Calibre user categories are not part of this interface. A colon
prefix that is not a supported location is treated as literal text by the shared
parser. Empty located terms such as `title:` are rejected.

| Location | Values searched |
|---|---|
| `all`, or an unqualified term | Curated WEMI display/descriptive fields and the metadata below |
| `work`, `expression`, `manifestation`, `item` | The corresponding level's descriptive fields and attached metadata |
| `title` | Work titles, Expression title overrides/subtitles, Manifestation subtitles |
| `agent` | Credited Agent canonical names, sort names and aliases, across all roles |
| `author`, `translator`, `publisher` | Agent values restricted to `aut`, `trl`, `pbl` credits respectively |
| `tag`, `label`, `genre`, `subject`, `series` | Related vocabulary values and their configured descriptive fields |
| `language` | Linked languages and Work/Expression language references, using names and codes |
| `identifier` | Curated identifier schemes/values and Item-observed schemes/values |
| `note`, `comment`, `synopsis` | Attached prose values |

Plural field aliases such as `titles`, `authors`, `tags` and `identifiers` are
accepted; `creators` aliases `agent`. Plural WEMI names are also accepted.
Qualify a field with a level to constrain its ownership: `work.tag:fiction`,
`expression.language:French`, or `item.identifier:9780306406157`.

Unqualified fields inspect the result entity and every reachable ancestor and
descendant. Boolean terms apply independently to that result: two Expression
terms may match different Expressions of the same Work. There is no syntax for
requiring two terms to match the same child or the same relationship row.

## A novel with a separate introduction

Suppose one Manifestation embodies an Expression of a novel and an Expression
of its introduction, each belonging to a separate Work with a different author.

```python
# Finds the edition through either embodied Work, including a non-preferred one.
editions = catalog.search.find('work.title:"Introduction"', level="manifestation")

# Finds Items whose edition embodies the introduction.
copies = catalog.search.find('work.title:"Introduction"', level="item")

# Finds the introduction Work, without attributing its author to the novel Work.
introductions = catalog.search.find('author:"Introduction Author"')
```

Traversal always moves toward ancestors or toward descendants from the result
entity. It never descends from the novel to the shared edition and then ascends
into the introduction Work. Work-only searches therefore do not acquire sibling
Works' authors or tags. A Manifestation or Item can legitimately match both Works
through its ancestor paths. Orphan Works and incomplete WEMI paths remain
searchable, including with negative queries.

## Ownership and implementation limits

`catalog/search/database_search.py` owns semantic locations, matching and traversal.
The facade composes this service using Catalog repositories and hierarchy reads.
Portable database macros own physical row/link reads and SQL execution. Constructing
the search service does not run a sample query, open a connection or write data.

SQLite and the APSW driver now execute predicates, monotonic relationship paths
and pagination in SQL. Catalog builds physical predicates in `physical_plan.py`;
the database compiler validates identifiers and binds every value. Shared Python
functions preserve NFKC/casefold, whitespace and identifier normalization rules.
Exact Work title and identifier lookups use versioned expression indexes. General
substring queries still scan: this is not a full-text engine.

Each graph search checks its traversed physical paths for dangling endpoints in
one preflight query, then selects matching rows. These checks cover the whole
requested path, so corrupt links can raise even where an earlier Python Boolean
short circuit or page limit once hid them. They do not read destination payloads
into Python. Scalar lookups need only their result query.

PostgreSQL and older adapters retain the semantic Python fallback; adapters with
cursor paging keep candidate payload reads bounded. PostgreSQL does not install
SQLite comparison functions or expression indexes. Providing an equivalent
indexed PostgreSQL implementation remains follow-up work.

Calls observe current data and borrow any active macro transaction. Multiple
reads do not establish a consistent snapshot; callers requiring one must supply
an appropriate transaction scope. Existing databases install the indexes via
`db.migrate_query_indexes()` or the Core migration command; opening or searching
does not trigger a migration. See
[SQL efficiency implementation](../databases/sql-efficiency-2026-10-31.md).

The existing `catalog.search.Search` class remains the Calibre-compatible cache
search engine. It has a different host and result model. Its parser grammar is
shared with WEMI search; this change does not redirect existing cache searches.

Regression coverage lives in `tests/catalog/test_wemi_database_search.py`, with
real SQLite and APSW checks for shared editions, alternate paths, partial records,
role filters, attached metadata, literal query text, pagination and read failures.
