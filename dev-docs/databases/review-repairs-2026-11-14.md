# Database review repair contracts — 2026-11-14

Implemented and locally validated on top of `7780b7eb`, including the earlier
DR01–DR03 repairs. The local commit containing this note records the combined
checkpoint; no push or production catalogue migration is included. The
[independent review](../architectural_reviews/databases-review-2026-11-14.md)
retains the original findings.

## Explicit migrations

Persisted normalized-identity declarations remain authoritative. Writes deriving
keys with `title_search_term` or `tag_search_term`, and ensure operations using
those identity profiles, now refuse the lossy policy. Read/audit access remains
available. Review `audit_normalized_identities(target_profiles=...)` before
calling `migrate_normalized_identities(target_profiles=...)` with an explicit
mapping of `(table, display_column)` to a `ColumnNormalizationProfile`.
Stop other catalogue writers for a profile migration. Profile changes own an
outer transaction, check collisions before effects, and
update keys and declarations together. Existing values discarded by earlier
false matches cannot be reconstructed. Ordinary backfill without a profile
change retains its nested transaction contract.

Preference keys are unique in newly generated schemas. Existing duplicate keys
make `DBPrefs` loading fail instead of selecting a row by enumeration order.
`databases.preferences.audit_preference_keys(db)` reports candidate row IDs;
`migrate_preference_keys(db, keep={key: retained_id})` requires a complete,
explicit resolution of duplicates before installing uniqueness. Null keys
require explicit repair first. Neither ID order nor timestamps choose a winner.
Reload existing preference caches after migration. Writes use transactional
upserts; assignment and deletion publish cached changes after the owning commit.
`disable_setting` applies to both operations. Binary values use tagged JSON.

SQLite expression indexes remain part of the native format. They require LiuXin's
registered functions for writes, VACUUM and integrity checks. Ordinary tools can
read physical tables but cannot maintain these indexes without the functions.
Connections now compare a persisted function-policy/Unicode version before normal
access. Unversioned or mismatched indexes require an explicit offline
`databases.query_indexes.rebuild_sqlite_search_indexes(path)` after retaining a
backup. It rekeys known, unchanged index definitions transactionally and records
the current semantics; it refuses unknown or altered function-index definitions.
Changing normalization code still requires a deliberate policy-version bump.

## Writes, references and values

Owned SQLite macro write units acquire `BEGIN IMMEDIATE` before reads. The native
busy timeout bounds lock waiting; contention can still fail. Read transactions
remain deferred. Borrowed transactions retain their caller's snapshot and may
fail promotion. Arbitrary context-manager bodies are never replayed: retry only
an explicitly replayable outer application operation, after rollback. Journal
mode remains deployment policy; this change does not silently enable WAL.

Orphan pruning now owns a serialized outer transaction and inventories physical
inbound foreign keys as well as supplied links. PostgreSQL locks the target and
referencing tables; SQLite holds its writer reservation. Composite references
are checked together. External-schema references cause refusal. Polymorphic
references conservatively protect matching IDs regardless of the discriminator,
which can retain extra candidates but avoids guessing semantic ownership.

Maintained `executescript` rejects transaction controls, PRAGMA, VACUUM, ATTACH and
DETACH before executing any statement. Connection commands need their own explicit
boundary. Owned `transaction_scope` refuses an already active connection before
issuing BEGIN, including on PostgreSQL.

Absent SQLite type declarations preserve native values, including numeric-looking
view text. Unfamiliar nonempty declarations still use SQLite's affinity rules.
Reading a draft Row does not add keys to its write payload; database defaults
remain available. Identified single-row updates require an existing row, including
ID-only updates. A Row deliberately loaded as missing retains its no-write sentinel
behavior. Explicit insertion with an ID remains available through the insertion
API, and `from_idless_row_dict(table=...)` honors its table hint.

Identity metadata derives comparison bindings from the single identity registry.
Normalized text keys with historical `_phash` names are classified as normalized
keys. SQLite container bindings now encode JSON locally; schema-aware reads decode
PYSET/PYLIST/PYDICT, and raw SQL callers use `decode_sqlite_value` explicitly.
Opening LiuXin connections no longer changes global sqlite3 adapters, converters
or callback-traceback settings. PYSET is a JSON string array, preserving empty and
quoted strings; historical non-JSON PYSET payloads require explicit conversion.

## Publication and smaller contracts

Preference backups serialize before touching the old file, then flush and replace
a sibling temporary file. Errors propagate. SQLite creation and replacement fsync
the destination directory on POSIX; Windows lacks that directory-descriptor API.
A directory-flush error can occur after publication, so inspect the target before
retrying. Dump/restore retains a verified sibling backup named
`<database>.before-rebuild-<unique-id>.sqlite` before replacing the original.

PostgreSQL index audits check direction, column collation and default operator
class. Runtime grants explicitly include TEMPORARY. A storage bootstrap exception
produces `completed=False` with no invented configuration counts, and restores an
existing manager's startup setting. Failed dirty-record persistence requeues as
before and raises; it no longer reports the same success value as an empty queue.

The broad suggestion to add comparison columns to every policy-aware lookup is
not adopted. Names use NFC/trim/casefold, while existing search indexes use NFKC
and whitespace normalization. Substituting those indexes as equality definitions
would change identity semantics. Retain the correct fallback and add targeted
indexes when measured workloads justify their maintenance cost. Automatic
dirty-queue scheduling remains part of the wider CR07 lifecycle work.

## Finding dispositions

DBR01–DBR13 are resolved at the boundaries below, including the explicit policy
decision for DBR04. DBR14 is partial: the concrete contract fixes are complete;
broad comparison-column additions and automatic dirty-queue scheduling remain
the stated follow-ups. Platform and migration limits above remain part of the
contract. These are local validation results, ready for independent re-review.

| Finding | Implemented boundary or policy decision |
| --- | --- |
| DBR01 | Refuse lossy identity writes/ensure; explicitly audit and migrate selected profiles, refusing collisions before effects. |
| DBR02 | Inventory physical inbound references and serialize pruning; verify protection against a concurrent PostgreSQL reference insertion. |
| DBR03 | Refuse connection commands in maintained scripts before any statement runs. |
| DBR04 | Retain expression indexes, require matching persisted semantics, and provide an explicit offline rebuild. Generic-tool restrictions remain intentional. |
| DBR05 | Preserve values for absent SQLite declarations, including expression views and numeric-looking text. |
| DBR06 | Reserve owned SQLite write transactions before reads; verify contention with independent processes. Borrowed snapshot conflicts remain explicit errors. |
| DBR07 | Unique preference keys, explicit conflict resolution, transactional upsert and publication after commit. |
| DBR08 | Serialize backups first, then flush and atomically publish; failed serialization preserves the prior backup. |
| DBR09 | Draft reads leave the insertion payload unchanged, preserving database defaults. |
| DBR10 | Identified updates fail when the target is absent, including ID-only updates on SQLite and PostgreSQL. |
| DBR11 | Derive built-in bindings from one registry and classify persisted identity keys before hash heuristics. |
| DBR12 | Local container codecs without global registration; lossless JSON PYSET and explicit raw-SQL decoding. |
| DBR13 | POSIX directory flush and verified, retained dump/restore backup; Windows directory durability remains a platform limitation. |
| DBR14 | Owned-BEGIN guard, PostgreSQL index semantics/TEMPORARY grant, honest bootstrap results, Row table hints, distinct dirty-persistence failures and preference bytes/deletion. Broad indexing and automatic dirty scheduling remain the stated follow-ups. |

The regression ratchet retains all 2,259 prior requirements and adds 55 cases.
The combined commit also includes the earlier 53 DR01–DR03 cases, for 108 added
requirements relative to `7780b7eb`. The full database gate passed **2,425 tests**,
including **2,314/2,314 mandatory cases**, with no skips, failures or xfails.
Two existing logging deprecation warnings remain. All 78 frozen implementation,
test and configuration inputs matched after the run. The additional storage
docstring correction has an unchanged runtime AST and passed its checks.

The full quality runner passed formatting, annotation coverage, lint, architecture,
import budgets and both type checkers. The tooling, storage and documentation
selection passed **96 tests**. Focused results overlap these selections and must
not be summed as a unique total. The private PostgreSQL server is stopped.

Gate command, with `LIUXIN_TEST_POSTGRES_URL` set to the disposable test server:

```bash
PYTHONPATH=src:. .venv/bin/python scripts/run_database_regression_ratchet.py \
  --baseline-ref 7780b7eb62020d8b1cf6891b3bb95a47d9f7fcfb \
  --artifacts /tmp/liuxin-database-review-repairs-final-2026-11-14
```

The external artifact directory contains `ratchet.json` and telemetry. Local
supporting evidence is under `.tmp/database-review-fixes-2026-11-14/`; the tracked
results above remain the useful record when those ignored artifacts are absent.

## Independent re-review follow-up — 2026-11-14

RR03–RR06 are implemented in the worktree above `5d5c0c88`. The earlier
2,425-test result covered selected files, not all of `tests/databases`; it missed
the two new regressions identified by RR04. Implementation and local verification
are recorded below; these changes remain uncommitted.

### Catalogue admission (RR06; supersedes RR01 and RR02)

The generated base schema now has an explicit `schema_revision` singleton. Both
SQLite driver names and PostgreSQL validate it on every normal connection before
language repair, facade bootstrap or maintenance. Missing, malformed, older and
future revisions are rejected with a regeneration instruction. Opening never
adds a marker or guesses compatibility from module version strings, Git history
or SQLite's physical-schema counter.

Revision 1 starts with the `5d5c0c88` base schema plus this marker. **All existing
unversioned pre-release libraries must be regenerated**, including libraries
generated by `5d5c0c88`: their structural similarity is not proof of compatibility.
Do not stamp an existing file manually. Generators record the marker after their
schema build; SQLite builds in a private empty candidate, and PostgreSQL owns a
separate connection and checks schema emptiness under its creation lock.
Private SQLite restore and backup targets must also start empty; their populated
format is validated before publication, alongside the existing integrity checks.

Bump the revision when changing the generated base format and explicitly decide
whether a future revision migrates or refuses its predecessor. Native custom-field
DDL, catalogue data edits and profile migrations do not change the base revision.

### Unicode upgrade procedure (RR03)

Retain the policy/Unicode stamp and refuse mismatches, including read-only use.
The guard depends on the recorded Unicode and policy versions, not merely the
interpreter's minor-version label. Writer-maintained columns would still need
pinned semantics or explicit rekeying; they are not an alternative to that policy.
Errors now include both recorded and current semantics and the recovery command.
The error quotes the actual file path and interpreter for POSIX shells or Windows
PowerShell; the example below uses the checkout's POSIX environment.

1. Stop all users of the catalogue and retain a backup.
2. Use the LiuXin environment/interpreter that will subsequently open the library.
3. Run the explicit command, substituting the catalogue's actual path:

   ```bash
   .venv/bin/python -m LiuXin_alpha.databases.rebuild_search_indexes \
     --database-path /path/to/library.sqlite
   ```

4. Reopen only after the command succeeds. Do not alternate writers with incompatible
   policy stamps without repeating the controlled upgrade procedure.

The command validates the base revision first. It transactionally rebuilds known,
unchanged SQLite function-index definitions and their stamp. It does not upgrade
an unsupported catalogue format or rewrite unrelated identity policies.

### Conservative pruning (RR05)

Pruning refuses a target while any of the six known undeclared references listed
in the independent review still exists without a matching physical foreign key.
The refusal applies to both single and bulk pruning inside the serialized unit;
failure rolls the unit back. It applies even to an empty referring table because
the reference's lifecycle contract remains undefined. Declared references and
polymorphic references retain their existing protections.

This deliberately chooses the review's fail-closed alternative. Annotation
ownership and backup provenance need explicit deletion policies before constraints
are added. A future constraint removes the corresponding refusal through physical
inspection; inventing a cascade or deleting historical backup records is not part
of this repair. The error identifies the reference requiring that decision.

### Coverage and retained repair tools (RR04)

`search_index_metadata` and `schema_revision` are infrastructure tables, with no
invented metadata owner. The driver API now agrees with native value coercion.
Owner-batching fixtures create their real owners, and custom-field fixtures use
the native repository to populate keys, ordinals and memberships consistently.
Driver-only SQL grammar, codec and progress-hook fixtures install the actual revision
marker DDL; they remain minimal unit fixtures, not catalogue builders. Admission,
creation and facade integration tests use complete generated native schemas.
The rebinding failure fixture removes a required helper table from a generated
catalogue, so it still tests facade recovery after format admission; an empty
unversioned file is now rejected earlier by design.

The regression runner collects **all of `tests/databases`** and retains its strict
mandatory-case manifest: required cases must pass setup, execution and teardown,
with no skip or xfail. Ordinary suite failures outside the manifest also fail the
gate. Existing intentional skips outside that mandatory set remain visible.

Retain `target_profiles` migration: it has valid uses on current-schema libraries,
verified by a regression changing a current profile. Retain the lossy-profile
guard and explicit preference-key repair as conservative repair tools; neither
admits or upgrades unversioned libraries. Preference repair still requires an
explicit duplicate-resolution choice and cache reload afterward.

The complete validation command uses a disposable UTF-8 PostgreSQL database via
`LIUXIN_TEST_POSTGRES_URL`, with APSW installed:

```bash
PYTHONPATH=src:. .venv/bin/python scripts/run_database_regression_ratchet.py \
  --baseline-ref 5d5c0c88fb8f9e9e4a3ff14ef15c79b4b27806ba \
  --artifacts /tmp/liuxin-database-rereview-final-2026-11-14
```

### Local verification completed — 2026-11-15

The complete `tests/databases` run used Python 3.12.3, SQLite 3.45.1, APSW and
PostgreSQL 16.15. It finished with **5,000 passed, 90 skipped, two failed and eight
setup errors**, plus 26 existing marker/deprecation warnings, in 5,559 seconds.
Every one of the **2,401 mandatory cases passed setup, execution and teardown**;
none was missing, skipped or xfailed. All 2,314 prior requirements were retained
and 87 were added. The non-mandatory skips are visible in the complete run log.

The ten unsuccessful cases all belonged to `test_sqlite_progress.py`: its empty
`:memory:` fixtures were rejected by the new admission boundary. The fixture now
uses explicit revision metadata on a minimal SQL database. Its assertions still
cover default-off hooks, cancellation, existing/future handles and isolation
between drivers. The entire corrected module passed **10/10** cases separately,
including setup and teardown, and passed lint and formatting. No production code
changed after the complete run began. The full suite was not repeated after this
isolated fixture edit: its raw exit status remains 1, and a clean single-run full
gate is not claimed. The earlier unnamed order-dependent error was not reproduced.

The complete run used a Linux-filesystem snapshot with shared dependency/data
directories. All 33 frozen implementation, test and configuration inputs match
the worktree byte-for-byte. The progress fixture is the sole additional changed
input; its separate execution and formatting AST check account for it. Supporting
fingerprints and the verification audit are under
`.tmp/database-rereview-fixes-2026-11-14/`. The complete result is in the external
artifact directory above; the focused result is in
`/tmp/liuxin-rereview-progress-fixture-2026-11-15/`. Earlier interrupted telemetry
is archived separately and is not passing evidence.

The full quality runner passed formatting, annotations, lint, architecture/import
checks and both type checkers. The tooling/documentation selection passed 120
tests with one unrelated existing failure: the review index links to the user's
untracked `file-formats-top-level-review-2026-11-14.md`. That draft was preserved.
The actual July benchmark catalogue was copied and rejected by both SQLite driver
names without changing its bytes. Recovery was executed on Linux; PowerShell
quoting was source-checked, not exercised on Windows. The private PostgreSQL test
server was stopped and its shutdown verified. No new commit or publication is
claimed.
