# Read-model failure boundaries

The shared surface read model must distinguish absent catalogue data from a
failed read. An empty list, `None`, or zero is a valid result only when the
operation successfully establishes that result. It must not be a substitute
for an unavailable Core, a broken query, or a programming error.

## Outcomes

| Condition | Surface behavior |
| --- | --- |
| Schema says an optional table is absent | Empty collection, missing row, or zero count; do not query that table. |
| Successful lookup returns no record or relationships | Normal missing/empty result; API detail routes return their usual 404. |
| Invalid numeric URL identifier | Preserve the route's existing 400/404 or empty-result contract; parsing is separate from querying. |
| An optional row column raises `KeyError` | `presentation.row_value` returns `None`. Other row-access failures propagate. |
| An optimized query explicitly reports `complete=False` | Use the existing materialized read/sort/filter fallback through Core. |
| No preferred sortable column is available | Use the existing materialized ordering path. |
| File/image resolution explicitly reports unreadable or unavailable | Normal unavailable target; disabled downloads and malformed IDs remain normal exclusions. |
| Optional home-page count reports Core code `read_query_unavailable` | Show “count unavailable,” not zero; do not retry or switch read sources. |
| Query, schema, transport, iteration, or presentation failure | Propagate the exception; do not retry as a different read or publish a partial/empty result. |

Fallback is selected by a successful result or an explicit capability outcome,
never by treating arbitrary exceptions as absence. A failure in the fallback
itself also propagates. This does not introduce an implicit database fallback:
the Core read-source configuration remains authoritative.

## Ownership

`surfaces/read_model/api.py` owns catalogue projections and query selection.
It does not catch broad exceptions. Numeric metadata conversions handle only
`TypeError`, `ValueError`, and `OverflowError` around the conversion itself;
backend calls must remain outside those handlers.

The same rule applies to callers that could otherwise erase a read-model
failure: image discovery, author-route selection, API category detail,
OPDS related-data collection, home-page counts, and related-table discovery.
File/image capability queries must distinguish an explicit unavailable result
from a failed acquisition-resolution query. Actual byte-serving fallback
policy is separate and unchanged by this tranche.

`surfaces/presentation.py` retains missing-column compatibility for both Core
mapping rows and legacy database rows. A broken `__getitem__` implementation,
I/O failure, or wrong row type is not missing metadata.

No new exception hierarchy is introduced. Direct calls retain the original
exception for unexpected failures; existing Core handler and HTTP proxy
exceptions retain their error codes/details and available cause chains. This
policy concerns the shared read model and its application adapters, not every
inherited database/cache adapter's internal recovery policy.

Core translates only `UnknownCacheTableError` and `UnsupportedCacheQueryError`
from its structured cache query into the stable `read_query_unavailable` code,
with `table` and `reason` details. A full schema can include views that the
selected cache does not serve. The home page recognizes that code for its
optional counts; all other errors are re-raised. Required reads still fail,
including this known capability limit, rather than pretending to be empty.
Unknown fields, dirty caches, and arbitrary query errors are not capability
fallbacks. The translation does not consult the database, regardless of the
legacy metadata-hydration fallback setting.

## HTTP and diagnostics

Application `handle_request` and WSGI calls allow unexpected failures to escape
before publishing a successful response. The hosting WSGI server owns the
generic HTTP 500 response and error logging. The supported stdlib WSGI handler
keeps traceback detail in its error stream instead of exposing it in the public
response. Custom deployment middleware must preserve that separation.

API category detail checks missing records explicitly. Catching an entire
payload-construction operation and reporting every exception as “not found”
would conceal both failed reads and broken renderers.

## Count-only Core queries

Making home-page count failures visible exposed an existing Core bug:
`rows.query(limit=0)` ordered rows by coercing identifiers to integers, even
though it would return no rows. Text migration-ledger keys are valid data.
Count-only database queries now apply predicates/text/relation filtering but
skip ordering and row projection. Their count and pagination envelope remain
unchanged. This is not a change to the integer-identity contract of addressable
catalogue rows.

## Verification and ratchets

CI runs:

- `tests/surfaces/test_read_model_failure_contracts.py`: exception propagation,
  lazy-iteration failure, normal absence, incomplete-result fallback, row/value
  access, malformed Core payloads, and no catch-all handlers in read-model/image
  backends;
- `tests/surfaces/test_surface_read_errors.py`: API 400/404 distinctions,
  application-adapter propagation, and stdlib WSGI 500/logging boundaries;
- `tests/surfaces/test_read_model_transport_errors.py`: real Core direct/HTTP
  error-code/detail preservation and successful missing-record behavior;
- `tests/core/test_core_application_api.py`: named Core behavior, including
  count-only queries with text identifiers, filters, sorting requests, and
  offsets.

New standalone contract tests are Ruff targets in `scripts/run_type_checks.sh`.
Existing shared-helper/image tests and real-database read-model, API, web,
acquisition, and OPDS suites protect successful behavior. The dependency and
strict-leaf typing gates from stage 3 remain unchanged.
