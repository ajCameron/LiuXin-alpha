# Rating values at the Catalog boundary

Implemented for PR review, **2026-10-20**, architecture A08.

[`metadata.ratings.normalise_rating_pair`](../../src/LiuXin_alpha/metadata/ratings.py)
owns the pure value rule. `RatingRepository` invokes it through the existing
repository preparation path, including partial updates and value-based reuse.
No new persistence owner is introduced.

## Values and partial updates

The stored `rating` is a finite, nonnegative number or `None` for unrated.
`rating_out_of` is an optional positive whole number fitting a signed 64-bit
integer. A supplied value cannot exceed a supplied scale. A missing scale means
the upper bound is unknown; it does not silently mean five or ten.

Numeric text and Decimal inputs are accepted. Booleans, empty strings, negative
values and non-finite values are rejected before persistence. A fractional scale
is rejected rather than rounded. Values retain fractions; changing the scale
does not rescale the score.
Scores use floating-point storage. Bounds also check the original numeric text
or Decimal input, so rounding cannot admit a negative or over-scale value.

```python
rating_id = catalog.ratings.create(
    {"value": "4.25", "out_of": 5, "source": "reader"}
)
catalog.ratings.update(rating_id, {"out_of": 10})  # Still 4.25.
catalog.ratings.update(rating_id, {"value": None})  # Unrated; scale stays 10.
```

Updates validate the effective pair using stored values for omitted fields, but
write only supplied columns. An empty update remains a no-op. Ordinary reads and
unrelated source/display edits tolerate historical values without repairing them.
Changing either half of an invalid historical pair requires a valid resulting
pair. No backfill or automatic collision repair is included.

`rating_for_calibre_tag_viewer` remains a separately supplied compatibility display
field. This slice neither derives it nor changes its interpretation. Passive
metadata containers still retain values without performing writes or validation.

## Coverage matrix

| Public route | Preparation owner and behavior |
| --- | --- |
| `ratings.create` / `update` | Shared repository preparation invokes the pure pair rule. |
| `ratings.match_or_create` | Validates supplied values before matching and checks a reused row against its stored scale. Ordinary matching remains a tolerant read. |
| `create_writer`, `write`, `write_one` on Rating columns | Delegate to repository batch preparation; all rows are prepared before the first update. |
| Schema value links such as `write_one("works", "rating", work_id, value)` | New destinations use repository creation. Existing matches use repository reference preparation, so historical invalid values cannot bypass validation. |
| Catalog Row-shaped create/persist adapters | Delegate to the same repository; raw database Row operations are outside this guarantee. |
| `catalog.ensure.rating` / `apply.rating` | Resolve by value through the repository, preserve fractions, and share the existing macro transaction and relationship owner. |
| Core entity create/update and field-write commands | Delegate to Catalog; transport permission is an independent prerequisite. |
| Retained Library metadata-container import | Rating entries use the repository; all bibliographic metadata writes share one transaction. |
| Modern `mutations.writer.write_container` / `write_values` | Ratings are not a supported write-back field. Explicit requests remain rejected; this slice does not claim new field support. |
| Raw SQL, driver and macro writes | Outside semantic validation; useful for historical fixtures and explicit repair. |

The compatibility `ensure.rating` no longer interprets a score as
`int(score) + 1` in the ID space. It resolves a unique match or creates a Rating;
ambiguous matches follow normal Catalog matching policy. `apply.rating` no longer
truncates fractions or accepts bool as an integer score. Explicit Rating Rows are
validated using their stored pair before attachment.

This deliberately changes the old ID-based conversion: `ensure.rating(8)` now
resolves the score **8**, whereas seed row 9 contained **4**. Callers needing a
scale conversion must perform it explicitly and supply `out_of` through the
repository; neither a helper name nor a rating-source label implies rescaling.

Reference preparation distinguishes reusing an immutable seeded Language from
editing it. Language references remain valid even though creating or updating
Language rows is refused.

## Transaction boundaries

Repository batches validate every effective pair before writing. A later backend
failure rolls back earlier changes in that batch. Compatibility rating resolution
and attachment share a transaction, so a failed link cannot leave a new score row.

The retained Library importer previously could create a WEMI stack and then fail
on a later metadata field. Its metadata phase now shares one macro transaction.
Its title/book projection reads use that same active connection so they can see
the newly written stack before commit.
Optional cover/file publication runs after that unit succeeds. External publication
failure can leave written metadata; a database rollback is not filesystem rollback.
A borrowed outer transaction keeps its own commit responsibility.

The separate database bootstrap repair still owns its fixed legacy seed IDs
1–11. Its inserts/updates now use one macro transaction, which advances PostgreSQL
identity allocation after explicit seed inserts and rolls back a late repair
failure. This is an explicit bootstrap repair, not a Catalog read-side repair.

## Remaining aggregate link operations

The following direct macro operations were inspected as part of A08. They are
domain aggregates, not additional unguarded Rating value writers:

| Operation | Owner and decision |
| --- | --- |
| WEMI stack creation, primary-link changes and unlinking | `MetadataWriter` owns adjacent-level validation, primary demotion and transactional replacement. Keep the aggregate operation; do not replace it with repeated independent link writes. |
| Entity merge link transfer | `MetadataWriter` plans identity/type collisions and transfers links in the merge transaction. Keep the aggregate owner; further merge-policy work needs its own cases. |
| Agent credit replacement | `AgentRepository` validates the role and endpoint identities before typed bulk replacement. Keep that public aggregate. |
| Comment/Synopsis replacement | Their repositories coordinate row creation and link replacement in one transaction. Their remaining text validators are separate coverage work. |
| Container relation updates | `CatalogContainerWriter` already uses `CatalogRelationships` and repository preparation; do not introduce another writer. |
| Retained cover/file link registration | Library's optional publication phase still has raw storage link calls. A10/A11 own storage mutation/lifecycle migration; metadata transaction success is not a publication receipt. |

## Verification

[`test_rating_values.py`](../../tests/metadata/test_rating_values.py) checks the
pure rule. [`test_rating_validation.py`](../../tests/catalog/test_rating_validation.py)
checks repository, schema, Row/Core/HTTP and compatibility routes, historical
reads, partial updates, immutable reference linking and late failures across
SQLite, APSW and configured live PostgreSQL. Retained Library container import
is covered on SQLite/APSW: PostgreSQL does not provide the required `titles`/`books`
compatibility views, so its case is explicitly skipped. The prerequisite
[discovery correction](../databases/postgres-identifier-discovery.md) addresses
the earlier full-schema blocker without adding those views. PostgreSQL
also lacks SQLite's Language seed dataset; that reference-link test supplies its
own English row without treating it as a supported Catalog creation operation.

See the [handoff](../../working-memory/rating-validation-2026-10-20.md) for current
results and limitations. The wider [write contract](catalog-write-validation-contract.md)
still has uncovered field families; this slice does not imply universal validation.
