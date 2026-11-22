# Core field routing follow-up

User requested all metadata-cache follow-ups, with the Core repair in a separate
PR. Branch `core-cache-write-routing` is based on `cache-review` at `510c0a82`;
an isolated worktree. Target `cache-review`
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

The original local validation audit maps 115 distinct passing cases to JUnit reports. It combines 22 routing/receipt/injection cases,
two existing administrative-write cases, 18 application/RPC cases, 41 metadata
command/hydration cases, and 32 Catalog round-trip/documentation cases. The two
native application round trips include the new vocabulary checks on SQLite and
APSW. Loopback tests required sandbox escalation. Earlier fixture failures and
socket denials were corrected and superseded; they are not counted as passes.

The original final quality runner passed format,
lint, architecture, import budgets, basedpyright and mypy (563 source files;
122/118 invalid typing examples rejected). This is targeted evidence, not a
whole-repository run. Python 3.14.10, explicit `PYTHONPATH=src`, and
`LIUXIN_PLUGIN_CACHE_PATH` in `/tmp` were used.

Original checkout, dirty data submodule, Python 3.12 environment and six stashes
remain untouched. Cache lifecycle, benchmark and PostgreSQL upgrade work remains
in the separate ergonomics worktree.

The first remote full suite finished with six failures, 10,778 passes, 2,306 skips
and eleven xfails. These exposed neighboring hierarchy integrations: Library import
and Core tree nesting needed serialized outer units; old tests cleared semantic
Series names, borrowed ordinary transactions for deletion, or supplied sibling
positions beyond the existing population. The implementation now reserves the
writer before import/tree reads. Tests retain physical clearing coverage while
asserting semantic blank-name rejection, and wizard fixtures contain the siblings
required by their requested insertion positions. Wizard prompts name zero-based
sibling insertion and blank-to-append explicitly.

All fourteen targeted cases pass on SQLite and APSW. The broader Catalog/Core/
library run passes all 63 cases after rerunning six loopback checks with socket
access; all fourteen wizard cases pass. The complete quality runner also passes.
The combined audit originally recorded 192 distinct targeted passing cases.
The second remote full suite passed 10,783 cases and found one stale tree-handler
test double: its transaction factory rejected the new `serialized` argument.
The double now accepts that contract, and the existing test explicitly checks the
serialized reservation for both rejected and valid nesting. All 22 semantic
contract cases pass, bringing the targeted audit to 214 distinct passing cases.
The second Core database regression gate passed 5,611 cases with 125 permitted
skips. Fresh full CI is still required after the test-only correction.
The cache soak is independent and must not be stopped for these separate-worktree
changes.
