# Core field routing follow-up

User requested all metadata-cache follow-ups, with the Core repair in a separate
PR. Branch `core-cache-write-routing` is based on `cache-review` at `510c0a82`;
worktree `/tmp/liuxin-core-cache-write-routing-20261122`. Target `cache-review`
while foundation PR #213 remains open. Keep independent of ergonomics PR #214.

Core field commands now select `CoreServices.field_writer`: native caches use
the existing shared `CatalogCache`, uncached calls use the configured Catalog,
and external injected writer services retain their contract. Selected-writer
failures propagate without a second persistence attempt or reconciliation.

The neighbouring container route had two integration defects exposed by real
SQLite/APSW tests: vocabulary creation preferred display paths over native
names, and it opened an ordinary transaction before calling repositories that
require a serialized hierarchy write. Genre/Subject/Series creation now supplies
native names, and normalized vocabulary selections reserve the writer boundary
before repository entry. The hydrator test double now models current hierarchy
columns, paging and the transaction keyword contract.

Validation evidence: `/tmp/liuxin-core-routing-validation.json` maps 115 distinct
passing cases to JUnit reports. It combines 22 routing/receipt/injection cases,
two existing administrative-write cases, 18 application/RPC cases, 41 metadata
command/hydration cases, and 32 Catalog round-trip/documentation cases. The two
native application round trips include the new vocabulary checks on SQLite and
APSW. Loopback tests required sandbox escalation. Earlier fixture failures and
socket denials were corrected and superseded; they are not counted as passes.

Final quality runner `/tmp/liuxin-core-routing-quality-final.log` passed format,
lint, architecture, import budgets, basedpyright and mypy (563 source files;
122/118 invalid typing examples rejected). This is targeted evidence, not a
whole-repository run. Python 3.14.10, explicit `PYTHONPATH=src`, and
`LIUXIN_PLUGIN_CACHE_PATH` in `/tmp` were used.

Original checkout, dirty data submodule, Python 3.12 environment and six stashes
remain untouched. Cache lifecycle, benchmark and PostgreSQL upgrade work remains
in the separate ergonomics worktree.
