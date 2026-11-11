# Sixth driver review fixes — 2026-11-10

User authorized fixing both DDP23 and DDP24. Implementation is complete;
final validation is running. Continue in /tmp/liuxin-databases-typing on
database-driver-plugins-review-2026-11-05. Published PR #156 head remains
bab2b682cfa642fd3ecd431c1c395fbac0ecad00, base database-review.
No new commit, push or merge.

DDP23:
- Shared SQL/databasedriver/sqlite_dump.py owns storage-value serialization
  for both SQLite names' sql_dump() and both rebuilds. APSW still restores
  through its native shell; its old dump formatter is no longer used there.
- Reads TEXT as hex bytes, decodes the database encoding and renders TEXT
  expressions. Uses absent, nonoverlapping markers with replace()/char(0)
  for NULs, with bounded expression depth even for thousands of NULs.
- SQL quote() still formats non-TEXT storage classes; infinities get usable
  SQL numeric spellings. BLOB stays BLOB, no declared-type conversions.
- Per-cursor tuple rows preserve the connection's row factory. Standard
  str/bytes text factories are supported; arbitrary transforming factories
  raise before yielding an export. Invalid encoded TEXT raises.
- Preserve generated columns, sequence high-water marks, indexes/views/
  triggers and shadow-table data; reload virtual schema with writable_schema
  RESET. Native FTS5 rows and MATCH results are tested after restore.
- All old transaction and WAL publication protections remain.

DDP24:
- PG lexer matches ASCII plus non-ASCII identifier/tag classes and exact
  ASCII SQL whitespace from PostgreSQL 16 scan.l. Numeric digits are ASCII.
- Unicode names no longer shift JSON bindings; Unicode dollar bodies remain
  opaque under adaptation and statement splitting.
- Keyword recognition folds ASCII only. New native comparisons reproduced
  four failures where Python uppercasing turned ſelect/ıN into SELECT/IN;
  the final correction and 14 additional cases cover these names and tags.
- Regressions compare native expression results, committed values through
  independent readers and installed-maintenance script success/rollback.

Manifest: 808 -> 959 (151 added; 60 SQLite values, 80 PG adaptation,
11 maintenance completion). No existing ID removed or reassigned.
New serializer and test file are in format/lint gates and database typing.

Completed checks:
- SQLite expanded selection: 76 passed.
- PostgreSQL adaptation/completion selection: 403 passed.
- Live-configured integration: 116 passed, 10 backend-specific skips.
- 121 doctest examples pass.
- 58 documentation/runner checks pass.
- First complete mandatory runs: 943/943 and 945/945 required cases passed
  every phase, failures=[], exit0. The latter passed 976 selected tests.
  The final 959-case run includes the Unicode keyword correction.
- First complete quality pass succeeded (458 mypy sources, basedpyright0,
  95/93 invalid-call probes); rerunning after final export-factory guard.

Initial issues resolved: APSW formatter exceeded expression depth for 10,000
NULs (shared serializer fixes it); two import-block lint errors corrected.
An initial integration run lacked PG URL (65pass/61skip); configured rerun
above ran all relevant PG cases. Full local repository/confidence suites not
rerun; exact-head CI is still required after eventual publication.

Evidence prefix: /tmp/liuxin-driver-sixth-fixes-.
Final pending handles: ratchet-complete session 81659; quality-delivery 96100.
Server is currently RUNNING, owned cluster receipt pg.json. After tests finish,
run /tmp/liuxin-driver-sixth-fixes-stop-pg.py with escalation; it preserves the
server log and removes only this owned temporary cluster. Then update this
handoff, review validation and both memory indexes, stage final docs and check
diff. No subagents/skills used. Preserve original checkout source/preferences/data.
