# Complete project docstrings in bounded modules

Status: **M157–M176 complete**, 2026-10-04. All **599 declarations across twenty-two
files** are verified and every file is complete. All 306 focused regressions,
599 command-example references, static and full quality checks passed with no
skips. **M177 is next** on a new request; all 34 declarations across its four
metadata test files are unchanged.
See the [batch checkpoint](../working-memory/project-docstrings-m157-m176-2026-10-04.md).
D remains archived complete; M is the active bounded campaign.

## Baseline and navigation

The baseline contains **2,730 Python files**, of which **662 are source-reviewed**:
869 classes and 7,146 functions, or 8,677 declarations including modules. There
are **2,068 remaining files** and **33,572 remaining declarations to review**.
P00's whole-project audit reported 23,003 missing/blank docstrings; existing docs
also need review. These measures are different and must not be added together.

Since that baseline, G01 and the completed C batches reached 722 complete files.
After the [shim cleanup](shim-removal-plan.md) and subsequent continuations,
**D is complete: 232/232 units, 5,685/5,685 declarations, 374/374 files**.
The active M track has **176/274 verified units**, **4,508/7,198 declarations**,
and **204/344 complete files** reviewed.
Current manifest coverage is **1,264/2,671 file review records**,
**19,421 declarations in complete files**, plus **37 verified partial catalog
declarations**. Remaining: **1,407 files and 22,703 declarations**.
Discovery found no new or missing paths; 39 unrelated review-hash differences
are recorded, with no drift delta from the prior checkpoint. Historical
maintenance entries, D archive, milestones and unrelated records are preserved.

The historical D095 checkpoint remains 95/232 units; source removals changed the
live inventory rather than completing new documentation work. Removed scopes are
archived in the [reconciliation evidence](../working-memory/test-results/shim-removal-2026-09-15/documentation-reconciliation.json).
Affected files have new functional baselines in documentation-rebase.after.json;
their original baseline hashes and historical documentation-only proofs are
preserved. Do not replay old batch helpers with fixed file/declaration totals.
The latest twenty-unit batch is complete, with M177 next only on request.

- [Exact work inventory](project-docstrings-work-units.json): stable module IDs,
  source hashes, exact file/declaration selections, prerequisites, and checkpoints.
- [Reviewed manifest](../working-memory/project-docstrings-reviewed-files.txt):
  files whose entire documentation pass is complete.
- [Established rules and exceptions](../working-memory/project-docstrings-2026-09-08.md):
  historical evidence, native-C coverage, runtime-doc exceptions, and known guards.
- [Quality gates](maintainability-quality-gates.md) and
  [test streams](test-streams.md): existing verification owners.

The inventory includes all unreviewed files, including those with no structural
findings. Scope includes private/nested/async definitions, tests, scripts,
examples, inherited code, preferences, and tracked submodule Python. Retain the
completed native-C checkpoint separately. Ignored artifacts and anonymous lambdas
are not named Python declarations to document.

## Completed twenty-unit web-source batch — M157–M176, 2026-10-04

All 599 declarations across twenty-two files are verified, with every file
complete. The [saved observations](../working-memory/test-results/docstrings-m157-m176-2026-10-04/observations.json)
record 306 passing non-live regressions, 599 valid command examples, and clean
static and full quality checks. Docs cover shared source/browser/cache contracts,
metadata and cover orchestration, retry/backoff helpers, CLI and worker adapters,
and the Big Book Search, Douban, Edelweiss, Google, Internet Archive, ISBNDB,
KDL, Library of Congress, LibraryThing, Open Library, OverDrive, OZON, Wikidata
and xISBN integrations. M177 is next; its four metadata test files remain
unchanged. Historical records are preserved.

## Completed ten-unit PDB/database-source/ISFDB/Amazon batch — M147–M156, 2026-10-04

All 195 declarations across seventeen files are verified, with every file
complete. The [saved observations](../working-memory/test-results/docstrings-m147-m156-2026-10-04/observations.json)
record 148 passing regressions, 195 valid command examples, and clean static and
full quality checks. Docs cover PDB dispatch and subreaders, database-backed
metadata hydrator/read-source/WEMI contracts, explicit local/web source discovery,
local ISFDB queries and WEMI projection, and Amazon regional identifiers, parsing,
retry, caching and cover behavior. M157 is next; its selected
`web_sources/base.py` declarations remain unchanged. Historical records are
preserved; M157–M176 subsequently completed above.

## Completed ten-unit metadata-source batch — M137–M146, 2026-10-03

All 290 declarations across sixteen files are verified, with every file complete.
The [saved observations](../working-memory/test-results/docstrings-m137-m146-2026-10-03/observations.json)
record 201 passing regressions, three environment-dependent skips, 290 valid
command examples, and clean static and full quality checks. Docs cover ODT, OPF,
PDF, Plucker/PML/RAR/Rocket eBook, reader-registry mutation, RTF/SNB/Topaz,
TXT/TXTZ, worker jobs and ZIP dispatch, including parser bounds, cover selection,
optional tools, fallback policy, registry revisions, merge precedence and
path/stream ownership. M147 is next; its four PDB files remain unchanged.
That was the boundary at this checkpoint; M147–M156 subsequently completed
above. Historical records are preserved.

## Completed ten-unit metadata-source batch — M127–M136, 2026-09-29

All 280 declarations across twelve files are verified, with every file complete.
The [saved observations](../working-memory/test-results/docstrings-m127-m136-2026-09-29/observations.json)
record 354 passing regressions, one optional LRX corpus skip, 280 valid command
examples, and clean static and full quality checks. Docs cover comic/DOCX,
EPUB/OCF, EXTZ, FB2, filename and HTML inference, IMP, LIT, LRF, LRX and MOBI,
including binary bounds, strict/fallback parsing, optional dependencies, cover
selection and path/stream ownership. M137 is next; `odt.py` remains unchanged.
That was the boundary at this checkpoint; M137–M146 subsequently completed
above. Historical records are preserved.

## Completed ten-unit item/work/manifestation/file-source batch — M117–M126, 2026-09-29

All 268 new declarations across nine files are verified, and every file is
complete. The prior 40 item-metadata declarations are promoted unchanged. The
[saved observations](../working-memory/test-results/docstrings-m117-m126-2026-09-28/observations.json)
record 183 passing regressions, 915 passing runnable statements and clean static
and full quality checks. Docs cover complete item, manifestation and work API
contracts, registry-backed metadata-reader dispatch, archive extraction and
ComicBookInfo decoding. Initial example and runtime wording failures were
corrected before final checks. M127 is next; both queued files remain unchanged.
Historical records are preserved.

## Completed ten-unit agent/expression/item API batch — M107–M116, 2026-09-28

All 263 new declarations across seven files are verified: six files are complete
and item metadata has a 40-declaration partial slice. The
[saved observations](../working-memory/test-results/docstrings-m107-m116-2026-09-28/observations.json)
record 103 passing regressions, 1,022 passing runnable statements and clean static
and full quality checks. Docs cover agent profile sidecars, expression/item identity
contracts, relation keys/cardinalities, primary WEMI traversal, projections,
mapping and writer boundaries. Initial example setup and runtime doc wording
failures were corrected and archived before final checks. M117 is next; its 27
declarations remain unchanged. Historical records are preserved.

## Completed ten-unit title/work/shared-API batch — M097–M106, 2026-09-27

All 256 new declarations across twelve files are verified, with every file complete.
The previous 24 title declarations are promoted unchanged. The
[saved observations](../working-memory/test-results/docstrings-m097-m106-2026-09-27/observations.json)
record 195 passing regressions, 728 passing runnable statements and clean static
and full quality checks. Docs cover title preference, work identity/hydration,
shared relation and projection policy, target-id extraction and agent identity.
Three initial example failures were corrected and archived; final hashes match
verification. Runtime checks cover 144 title accessors and all 18 command examples
point to executed owning regressions. M107 is next; its whole profile API file is
unchanged. Historical records are preserved.

## Completed ten-unit resource/series/subject/title batch — M087–M096, 2026-09-27

All 268 new declarations across four files are verified: resources, series and
subjects are complete, while titles have 24/95 declarations reviewed. The
[saved observations](../working-memory/test-results/docstrings-m087-m096-2026-09-27/observations.json)
record 96 passing regressions, 748 passing runnable statements and clean static
and full quality checks. Docs cover display fallbacks, numbering and authority
checks, shared ownership, explicit validation and payload fields. Runtime checks
cover 204 generated accessors. The 71 remaining title declarations were unchanged
at that checkpoint; M097–M106 subsequently completed above. Historical records
are preserved.

## Completed ten-unit manifestation/note/projection/rating batch — M077–M086, 2026-09-27

All 275 new declarations across four files are verified, with every file complete.
The [saved observations](../working-memory/test-results/docstrings-m077-m086-2026-09-27/observations.json)
record 200 passing regressions, 884 passing runnable statements and clean static
and full quality checks. Docs cover manifestation graph resolution, note/rating
validation and ownership, projection precedence, immutable results and lazy guards.
Three example expectations were corrected for existing legacy storage behavior;
initial evidence is archived and final checks were rerun. Runtime checks cover
192 generated note and rating accessors. The resources file was unchanged at that
checkpoint; M087–M096 subsequently completed above. Historical records are preserved.

## Completed ten-unit identifier/item/label/language/manifestation batch — M067–M076, 2026-09-26

All 284 new declarations across eight files are verified, with every file complete,
including the prior 86 identifier declarations. The
[saved observations](../working-memory/test-results/docstrings-m067-m076-2026-09-26/observations.json)
record 184 passing regressions, 720 passing runnable statements and clean static
and full quality checks. Docs cover generated accessors, identity guards, bundle
ownership, hydration fallbacks and collection validation. Runtime checks cover
372 generated identifier, label and language accessors. The entire M077 file was
unchanged at that checkpoint; M077–M086 subsequently completed above. Historical
records are preserved.

## Completed ten-unit date/expression/genre/identifier batch — M057–M066, 2026-09-26

All 267 new declarations across six files are verified: five complete files,
including the prior 24 date records, and 86 partial identifier declarations. The
[saved observations](../working-memory/test-results/docstrings-m057-m066-2026-09-26/observations.json)
record 184 passing regressions, 709 passing runnable statements and clean static
and full quality checks. Docs cover collection ownership and validation, expression
identity/graph distinctions, hydration fallbacks, primary selection and scheme
eligibility. Runtime checks cover 120 generated date accessors. The seven M067
installer/closure declarations were unchanged at that checkpoint; M067–M076
subsequently completed above. Historical records are preserved.

## Completed ten-unit agent/date batch — M047–M056, 2026-09-26

All 252 new declarations across six files are verified: five complete files and
24 partial date-container declarations. The
[saved observations](../working-memory/test-results/docstrings-m047-m056-2026-09-26/observations.json)
record 97 passing regressions, 625 passing runnable statements and clean static
and full quality checks. Agent credits, identity, participation and profile docs
cover validation, ownership, mapping precedence and generated accessors. Runtime
checks cover 120 generated credit accessors. All 54 M057–M058 source segments were
unchanged at that checkpoint; M057–M066 subsequently completed above. Original
baselines and historical records are preserved.

## Completed ten-unit WEMI writer/row batch — M037–M046, 2026-09-26

All 211 new declarations across 23 files are verified, with all files complete,
including the prior 40-declaration WEMI slice. The
[saved observations](../working-memory/test-results/docstrings-m037-m046-2026-09-26/observations.json)
record 162 passing regressions, 296 passing runnable statements and clean static
and full quality checks. Conversion, serialization, hydration, writer and concrete
row/tree docs describe the implemented copy, persistence and validation behavior.
A separate M029 API correction documents KeyError for unknown id names and adds no
coverage. M047 was unchanged at that checkpoint; M047–M056 subsequently completed
above. Historical baselines and the D archive are preserved.

## Completed ten-unit WEMI/lazy metadata batch — M027–M036, 2026-09-26

All 265 new declarations across 11 files are verified. Ten files are complete,
including the prior 40-declaration title API slice; the concrete WEMI file remains
partial at 40/92 declarations. The
[saved observations](../working-memory/test-results/docstrings-m027-m036-2026-09-26/observations.json)
record 207 passing regressions, 181 passing runnable statements and clean static
and full quality checks. Docs cover live/copy semantics, lazy loading and failure
states, title precedence, tree validation and retained database ownership.
All 52 M037/M038 source segments were unchanged at that checkpoint; M037–M046
subsequently completed above. Baselines, unrelated records
and the completed D archive are preserved.

## Completed ten-unit metadata container/API batch — M017–M026, 2026-09-26

All 219 selected declarations across 21 files are verified: 20 complete files and
40 declarations in the partial title metadata API. The
[saved observations](../working-memory/test-results/docstrings-m017-m026-2026-09-26/observations.json)
record 79 passing regressions, 78 passing runnable statements, and clean static
and full quality checks. Docs distinguish protocol intent from actual legacy
copy/update/cleanup behavior. All 17 M027 declarations were unchanged at that checkpoint; M027–M036
subsequently completed above. Original
baselines, unrelated file/unit records, and the completed D archive are preserved.

## Completed ten-unit metadata batch — M007–M016, 2026-09-25

All 132 new declarations across 21 files are verified. All files are complete,
including the prior 40-declaration utility slice. The
[saved observations](../working-memory/test-results/docstrings-m007-m016-2026-09-25/observations.json)
record 152 passing regressions, 98 passing runnable statements, and clean static
and full quality checks. Standalone genre tables are validated through their own
examples; the active classifier uses separate maps. All three M017 files were
unchanged at that checkpoint; M017–M026 subsequently completed above. Inventory reconciliation preserves previous baselines and the D archive.

## Completed ten-unit database/metadata batch — D229–D232 and M001–M006, 2026-09-25

All 275 selected declarations across 23 files are verified: 22 complete files
promoted, plus 40 declarations in partial metadata/utils.py. The
[saved observations](../working-memory/test-results/docstrings-d229-m006-2026-09-25/observations.json)
record 215 passing regressions, two existing view skips, and 140 passing runnable
example statements. Source/complete-file checks, full quality, and track-boundary
discovery passed. All seven M007 declarations are unchanged.
The [D completion record](../working-memory/test-results/docstrings-d229-m006-2026-09-25/D-completion.json)
confirms every D unit/file is reviewed and all 374 current source hashes match.
The complete D campaign was archived at that checkpoint. M007–M016 subsequently completed above.

## Completed ten-unit PostgreSQL/SQLite/driver-contract batch — D219–D228, 2026-09-25

All 249 new declarations across 21 files are verified and all files promoted,
including 77 previously reviewed PostgreSQL declarations. The
[saved observations](../working-memory/test-results/docstrings-d219-d228-2026-09-25/observations.json)
record 182 passing regressions, three existing skips and 40 passing runnable
example statements. Full-file documentation checks, full quality checks and
source discovery passed. Executable code, comments, gates and all four D229
files were unchanged at that checkpoint. D229–D232/M001–M006 subsequently completed above.

## Completed ten-unit Unicode/Calibre/PostgreSQL batch — D209–D218, 2026-09-25

All 212 selected declarations across 25 files are verified: 24 complete files
promoted and 77 declarations reviewed in the partial PostgreSQL backend file.
The [saved observations](../working-memory/test-results/docstrings-d209-d218-2026-09-25/observations.json)
record 209 passing regressions, two optional skips and 39 passing runnable example
statements. Selected/complete-file checks, full quality checks and discovery
passed. Executable code, comments, gates and all twenty D219 declarations are unchanged.
D219–D228 subsequently completed the PostgreSQL file and the batch recorded above.

## Completed ten-unit Database-contract batch — D199–D208, 2026-09-25

All 270 declarations across 12 files are verified and all files promoted. The
[saved observations](../working-memory/test-results/docstrings-d199-d208-2026-09-25/observations.json)
record 402 passing regressions, 40 skips, 12 expected failures,
0 non-strict unexpected passes, and 50 passing runnable example statements.
Complete-file checks, configured quality checks and discovery passed. Executable
code, comments and existing skip/xfail markers are unchanged.
D209–D218 subsequently completed the batch recorded above.

## Completed ten-unit cache/Database-contract batch — D189–D198, 2026-09-25

All 290 declarations across 24 files are verified and all files promoted. The
[saved observations](../working-memory/test-results/docstrings-d189-d198-2026-09-25/observations.json)
record 331 passing regressions, 12 skips and 119 passing runnable
example statements, including 92 isolated helper statements from six gated legacy
integration modules. Complete-file checks, configured quality checks and discovery
passed. Executable code, comments and integration gates are unchanged.
D199–D208 subsequently completed the Database-contract batch recorded above.

## Completed ten-unit database/API/cache-test batch — D179–D188, 2026-09-25

All 271 new declarations across 16 files are verified and all files promoted,
including the locking file whose 35 earlier declarations remain unchanged. The
[saved observations](../working-memory/test-results/docstrings-d179-d188-2026-09-24/observations.json)
record 309 passing regressions, two environment skips and 139 passing runnable
example statements. Complete-file checks, full configured quality checks and
source discovery passed. Executable code and comments are unchanged.
D189–D198 subsequently completed the cache and Database-contract batch recorded above.

## Completed ten-unit database-test batch — D169–D178, 2026-09-24

All 256 selected declarations across 25 files are verified: 24 complete files
promoted and 35 declarations reviewed in the partial locking-test file. The
[saved observations](../working-memory/test-results/docstrings-d169-d178-2026-09-24/observations.json)
record 141 passing regressions, one conditional skip and 180 passing runnable
example statements. Selected/complete-file checks, full configured quality gates
and source discovery passed. Executable code, comments and D179 docs are unchanged.
D179–D188 subsequently completed the locking file and the batch recorded above.

## Completed twenty-unit maintenance, metadata and test batch — D149–D168, 2026-09-24

All 477 declarations across 45 files are reviewed and promoted. The
[saved observations](../working-memory/test-results/docstrings-d149-d168-2026-09-24/observations.json)
record 311 passing regressions, 2 skips, 2 expected failures, 339 passing runnable
example statements and 211 explicitly skipped integration statements. Complete-file
audit/normalizer checks, full configured quality gates and source discovery passed.
Executable code and comments are unchanged. D169–D178 subsequently completed
the database-test batch recorded above.

## Completed ten-unit wrapper batch — D139–D148, 2026-09-24

All 241 declarations across 13 driver-wrapper and API files are reviewed and
promoted. The [saved observations](../working-memory/test-results/docstrings-d139-d148-2026-09-24/observations.json)
record 638 passing regressions, 2 skips, 12 expected failures, 103 passing runnable examples and 199
explicitly skipped integration statements. Complete-file audit/normalizer checks,
full configured quality checks and source discovery passed. Executable code and
comments are unchanged. D149–D168 subsequently completed the maintenance/metadata/test batch recorded above.

## Completed ten-unit driver batch — D129–D138, 2026-09-24

All 208 declarations across 19 APSW backend and driver API files are reviewed and
promoted. The [saved observations](../working-memory/test-results/docstrings-d129-d138-2026-09-24/observations.json)
record 185 passing regressions, 4 skips, 109 passing runnable examples and 157
explicitly skipped integration statements. Complete-file audit/normalizer checks,
full configured quality checks and source discovery passed. Executable code and
comments are unchanged. D139–D148 subsequently completed the wrapper/API batch recorded above.

## Completed SQLite concrete driver — D128, 2026-09-24

All 15 declarations in SQLite/databasedriver/__init__.py are reviewed and the file
is promoted. The [saved observations](../working-memory/test-results/docstrings-d128-2026-09-24/observations.json)
record seven passing regressions, 44 passing runnable example statements, six
explicitly skipped integration examples, complete-file audit/normalizer checks and
full configured quality checks. Executable code and comments are unchanged.
D129–D138 subsequently completed the APSW/API batch recorded above.

## Completed SQLite plugin initializer — D127, 2026-09-22

Both declarations in SQLite/__init__.py are reviewed and the file is promoted.
The [saved observations](../working-memory/test-results/docstrings-d127-2026-09-22/observations.json)
record one passing import regression, nine passing runnable example statements,
complete-file audit/normalizer checks and full configured quality checks.
Executable code and comments are unchanged. D128 subsequently completed the
SQLite concrete driver, as recorded above.

## Completed custom-column management macros — D126, 2026-09-22

All 11 declarations in SQL/macros/cc_macros_mixin/cc_management_macros.py are
reviewed and the file is promoted. The
[saved observations](../working-memory/test-results/docstrings-d126-2026-09-22/observations.json)
record 15 passing regression checks, 64 passing runnable example statements,
complete-file audit/normalizer checks and full configured quality checks.
Executable code and comments are unchanged. D127 subsequently completed the
SQLite plugin initializer, as recorded above.

## Completed custom-column macro facade — D125, 2026-09-22

All 39 declarations in SQL/macros/cc_macros_mixin/__init__.py and
cc_ensure_values_mixin.py are reviewed and both files are promoted. The
[saved observations](../working-memory/test-results/docstrings-d125-2026-09-22/observations.json)
record two passing macro API contract checks, 81 passing runnable example statements,
complete-file audit/normalizer checks and full configured quality checks. Runtime
examples use explicit hosts and in-memory SQLite; no full legacy application run is
claimed. Executable code and comments are unchanged. D126 subsequently completed
the management macro file, as recorded above.

## Completed legacy temporary-table macros — D124, 2026-09-21

All five declarations in SQL/macros/temp_tables_macros_mixin.py are reviewed and
the file is promoted. The
[saved observations](../working-memory/test-results/docstrings-d124-2026-09-21/observations.json)
record one passing SQLite regression, 37 passing runnable example statements,
complete-file audit/normalizer checks and full configured quality checks.
Executable code and comments are unchanged. D125 subsequently completed the
custom-column macro facade and ensure-value mixin, as recorded above.

## Completed portable SQL macro file — D123, 2026-09-21

The remaining 36 declarations in SQL/macros/portable_macros_mixin.py are verified;
all 76 declarations across D122 and D123 are reviewed and the file is promoted.
The [saved observations](../working-memory/test-results/docstrings-d123-2026-09-21/observations.json)
record 21 passing regressions, one expected pg_temp harness skip, 92 passing
runnable example statements (45 from D123), complete-file audit/normalizer checks
and full configured quality checks. Executable code, comments and D122 docstrings
are unchanged. D124 subsequently completed the legacy temporary-table macro file,
as recorded above.

## Completed portable SQL macro selection — D122, 2026-09-21

The first 40 declarations in SQL/macros/portable_macros_mixin.py are verified.
The file was partial at this checkpoint; D123 subsequently completed the remaining
36 declarations and promoted it as recorded above. The
[saved observations](../working-memory/test-results/docstrings-d122-2026-09-21/observations.json)
record seven passing focused regressions, 47 passing runnable example statements,
selected audit/normalizer checks and full configured quality checks. Executable
code, comments and the then-unselected D123 docstrings were unchanged.

## Completed SQL macro continuation — D121, 2026-09-20

All 32 declarations in SQL/macros/__init__.py and hash_tables_macros_mixin.py
are reviewed and both files are promoted. The
[saved observations](../working-memory/test-results/docstrings-d121-2026-09-20/observations.json)
record three passing focused regressions, 28 passing runnable example statements,
complete-file audit/normalizer checks and full configured quality checks.
Executable code and comments are unchanged. D122 subsequently completed the first
portable macro selection, as recorded above.

## Completed SQL view continuation — D120, 2026-09-20

All four declarations in SQL/databasedriver/view_mixin.py are reviewed and the
file is promoted. The
[saved observations](../working-memory/test-results/docstrings-d120-2026-09-20/observations.json)
record one passing SQLite view round-trip regression, nine passing runnable
example statements, complete-file audit/normalizer checks and full configured
quality checks. Executable code and comments are unchanged. D121 subsequently
completed the SQL macros package and hash-table mixin, as recorded above.

## Completed SQL value-casting continuation — D119, 2026-09-20

Four remaining methods in SQL/databasedriver/value_casting_mixin.py are verified;
the file is now promoted with all 44 declarations reviewed. The
[saved observations](../working-memory/test-results/docstrings-d119-2026-09-20/observations.json)
record two passing SQLite regressions, 36 passing runnable examples (26 new),
complete-file audit/normalizer checks and full configured quality checks.
Executable code, comments and the D118 docstrings are unchanged. D120 subsequently
completed the SQL view helpers, as recorded above.

## Completed SQL column-policy continuation — D118, 2026-09-20

Forty declarations in SQL/databasedriver/value_casting_mixin.py are verified.
The [saved observations](../working-memory/test-results/docstrings-d118-2026-09-20/observations.json)
retain exact scope, source hashes and commands. Twelve SQLite regressions, ten
runnable examples and the configured quality gates passed. Executable AST and
comments are unchanged; no Ruff findings were added.

The file remained partial at this checkpoint; D119 subsequently completed and
promoted it as recorded above. This resume followed the one-module contract.

## Completed SQL utility continuation — D116–D117, 2026-09-20

Both selections in SQL/databasedriver/utils.py are verified: 56 declarations,
including nested aggregate callbacks. [Saved observations](../working-memory/test-results/docstrings-d116-d117-2026-09-19/campaign.json)
retain exact scope and the original baseline.

Verification: two focused SQLite regressions and 85 runnable examples passed;
complete-file documentation checks and configured quality gates passed.
One initial example was corrected to document existing byte-input restrictions
in the collation helper. Executable ASTs and comments are unchanged.
D118 was the next queued unit at that checkpoint and is now recorded above.
This utility batch did not resume earlier broad authorizations.

## Completed SQL continuation — D106–D115, 2026-09-17

Shared SQL support, Calibre/FRBR builders and common driver mixins are complete.
Each exact selection has [saved observations](../working-memory/test-results/docstrings-d106-d115-2026-09-17/campaign.json).
The FRBR generator was promoted after both assigned units passed.

Verification: 139 focused regressions passed, three skipped, 35 runnable examples
passed, full configured quality gates passed, and all 257 declarations are clean.
Executable ASTs and comments are unchanged; no Ruff findings were added.
D116 was the next queued unit at that checkpoint; the utility continuation is
recorded above. That request covered exactly ten units.

## Previous ten-unit continuation — D096–D105, 2026-09-17

D096–D105 cover the driver registry/macros and PostgreSQL backend. Each exact
selection has its own saved observations in the
[batch evidence](../working-memory/test-results/docstrings-ten-2026-09-16/campaign.json).
The batch began on 2026-09-16 and completed on 2026-09-17. Connection and native
driver files were promoted only after all their assigned units passed.

Verification: 35 focused regressions, 78 runnable examples, four isolated macro
result modes, full configured quality gates, and complete-file docstring checks.
Executable ASTs and comments are unchanged; no Ruff findings were added.
D106 was the next queued unit at that checkpoint; the subsequent SQL continuation
is recorded above. The earlier batch authorized only D096–D105.

## Authorized batch — 2026-09-13

The user authorized thirty queued modules, C03 through C032, in inventory order.
**All thirty are now verified.** Both planned milestones and individual checkpoints
are saved. This completed batch was the sole override of the one-module stop rule;
stop for token-spend review. See the
[campaign record](../working-memory/test-results/docstrings-thirty-2026-09-13/campaign.json).

## Authorized D range — 2026-09-14

The user authorized the complete D range, D001 through D232. This supersedes the
one-module stop rule for this track. Keep individual declaration selections,
partial-file promotion rules and verification checkpoints. Work in coherent test
groups, reuse unchanged evidence, and keep the full D objective active until its
requirements are verified. Exact baseline snapshots and progress are in the
[D campaign](../working-memory/test-results/docstrings-d-2026-09-14/campaign.json).
No commit or publication is requested.

## Module contract and cost controls

A normal module targets **2–4 files and no more than 40 declarations**. A package
with one remaining file can have a one-file module. Do not combine unrelated
packages to fill capacity. P00 and G01/G02 are separately scoped enabling modules.

- One explicit module request authorizes one module, followed by a checkpoint and
  stop. There is no automatic continuation, commit, or publication.
- Use this guide, the requested inventory entry, and its latest checkpoint as the
  starting context. Do not load the complete inventory or historical conversation
  into the agent context; extract just the selected module and its file records.
- Freeze exact scope and relevant tests during preflight, before editing. Existing
  regression commands in queued entries are candidates, not proven coverage.
  Confirm them by source review; narrow broad candidates to relevant selectors.
  If none exist, select caller coverage or record why static/doctest evidence is
  appropriate. Do not run unrelated tests simply to obtain a green result.
- Read selected source and only necessary dependency excerpts. Preserve reviewed
  work and reuse evidence for unchanged source hashes. Record available token
  usage; a user-supplied budget takes precedence. Do not claim an unenforced hard cap.
- Large files are split at lexical declaration boundaries. Keep complete class or
  function subtrees together where they fit; split oversized containers into their
  own docstring and separately selected members. Line ranges are context hints,
  not permission to rewrite all nested declarations within those lines.
- Declaration IDs use `kind:qualified.name#occurrence`. Occurrence distinguishes
  repeated definitions in lexical traversal order. Docstring insertion changes
  line numbers without changing these IDs. Each selected class/function ID covers
  only its own docstring, not its members. Never duplicate declaration assignments.
- A verified partial-file module does not make the file reviewed. Promote the file
  only after all assigned modules and a complete-file audit/normalizer pass. Never
  mark a partially reviewed file complete to fit a size or cost boundary.

Every module records purpose, prerequisites, exact scope, intended changes,
validation, evidence, and a resume point. States are `queued`, `in_progress`,
`partial`, `verified`, and `blocked`. Unavailable checks remain unverified.

To retrieve a single module without dumping the backlog, run from the repository
root (replace `C01` with the requested ID):

```bash
python3 - <<'PY'
import json
from pathlib import Path
inventory = json.loads(Path('dev-docs/project-docstrings-work-units.json').read_text())
unit = next(unit for unit in inventory['units'] if unit['id'] == 'C01')
print(json.dumps(unit, indent=2))
for item in unit['scope']:
    if isinstance(item, dict):
        print(item['path'], inventory['files'][item['path']])
PY
```

At module start, compare files with `current_sha256` when present, otherwise with
`baseline_sha256`. Keep baseline hashes unchanged when recording later edits;
record their new current hashes and `last_changed_by`. Expected changes
from a preceding partial-file module must be supported by its checkpoint; unrelated
changes require revalidation and refreshed scope. Save pre-edit source/AST evidence
outside tracked source. At completion, record current hashes, commands, terminal
exit codes, skips/failures, selected declaration IDs, and the exact resume point in
`working-memory/test-results/project-docstrings-<ID>-<date>-observations.json`.
Record the checkpoint location in the unit, then refresh its file records.

## Initial modules and milestones

| ID | Fixed scope | Verifiable checkpoint |
|---|---|---|
| P00 | Reconcile the paused batch; save plan, queue, and navigation | Exact inventory partition; bounded modules; completed prior results recorded; links pass |
| G01 | Shared docstring-aware ownership measurement and focused tests | Docstrings excluded; executable content still counted |
| G02 | Integrate the helper into existing ownership tests | All seven recorded guard failures resolved without raised limits |
| C01 | Agent repository and API: 2 files, 31 declarations | Agent matching, aliases, types, and credit regressions |
| C02 | Curated identifier repository and API: 2 files, 26 declarations | Normalization, reuse, owner isolation, and rollback regressions |
| C03 | Observed Item identifier repository and API: 2 files, 13 declarations | Observation storage and optional Item-scope regressions |
| C04 | Title repository and API: 2 files, 20 declarations | Preferred-title and title-update regressions |
| C05 | Bundle retrieval and API: 2 files, 16 declarations | Path selection and attachment regressions |
| C06 | Hierarchy retrieval and API: 2 files, 9 declarations | Parent/child traversal regressions |
| C07 | Graph retrieval and API: 2 files, 10 declarations | Graph contents, limits, and truncation regressions |
| C08 | Projection retrieval and API: 2 files, 10 declarations | Display-title and summary regressions |
| C09 | Retrieval composition and API exports: 2 files, 5 declarations | Import, composition, and protocol checks |

After C04, require all 13 concrete repository modules and all 15 repository API
modules reviewed, plus full Catalog regression and quality checks. After C09,
apply the same milestone checks to the complete retrieval/API trees. Subsequent
inventory IDs use the track letter plus three digits. IDs remain stable when
statuses change; append new units rather than renumbering published IDs.

G01/G02 are user-approved changes to test measurement, not production behavior.
Use a shared helper in `tests/support/docstring_ownership.py` with focused tests in
`tests/scripts/test_docstring_ownership.py`. Exclude only recognized leading
literal docstrings from body statement counts and documentation-only physical
lines from size measurements. Preserve numeric thresholds, comparison operators,
non-doc comments/blank lines, delegation requirements, and dependency checks.
Mixed code/docstring lines still count. Ordinary strings, assigned strings, bytes,
and f-strings must not acquire an exemption. Test module/class/function/async/nested
cases, single/multiline forms, same-line code, and truly oversized code. G02 must
update the affected guard descriptions as well as their measurement calls.
Guard edits do not by themselves document unreviewed test files; their remaining
documentation assignments stay queued. G01's new Python files enter the inventory.

## Whole remaining programme

Every path has one track and one or more non-overlapping declaration assignments.
The queue groups immediate packages and corresponding API packages where possible,
then uses lexical owner/file order and packs within the size limits. The fixed
C01–C09 entries take precedence. Module prerequisites express hard dependencies;
track order is a recommended progression, not authorization to continue.

| Track | Remaining files | Modules | Module families |
|---|---:|---:|---|
| C — Catalog | 28 | 20 | Repositories, retrieval, policy/writers, search, metadata tools, compatibility, remaining tests |
| T — Test infrastructure | 116 | 66 | Shared fixtures, provisioning, database helpers, support utilities, typing fixtures |
| U — Utilities | 313 | 208 | Configuration, logging/resources, text/language, images, jobs/IPC, archives, plugins, bundled libraries |
| D — Databases and caches | 246 | 127 | Contracts/schema, individual backends, portable macros, rows/links, cache models/readers/writers, tests |
| M — Metadata | 344 | 274 | APIs/constants, container families, book helpers, file/local/web sources, tests |
| F — File formats | 574 | 347 | Conversion/OEB, individual format families, readers/writers, compatibility helpers, tests |
| A — Application/configuration remainder | 133 | 128 | Library, customization, remaining surfaces/jobs, root modules, constants/resources, preferences, residual tests |
| S — Tools and script tests | 51 | 28 | Operational tools, benchmarks, fixture generators, build/package tools, verification tools |
| L — Legacy source trees | 52 | 37 | Retained setup/test trees and compatibility shims |
| **Total** | **1,857** | **1,235** | **All currently unreviewed files** |

Implementation, API, and test documentation can occupy different modules when size
requires it. Existing tests can validate multiple modules without being edited or
counted repeatedly. A source path with several partial modules remains one file in
the track total. Reviewed baseline files have no queued documentation assignment.

At each track milestone, enumerate through `audit_project_docstrings.project_paths`
again, including submodules. Add new source paths and flag changed reviewed hashes;
never silently shrink scope because discovery, parsing, or a backend failed.

## Documentation rules and acceptance

Read source before writing substantive reST prose. Include useful summaries,
examples, ordered parameter descriptions, returns, and applicable errors. Explain
validation order, state changes, resource ownership, transactions, and limitations
where they matter; do not substitute symbol-name restatements for review.

Preserve executable ASTs, signatures, API aliases, constants, comments, fixture
literals, and assertions. Separate implementation bugs from documentation work.
Retain the three narrow established runtime-doc exceptions without generalizing
them. Inspect consumers of generated documentation and configuration executed into
dictionaries: adding a module docstring can introduce observable `__doc__` data.

Each documentation module must pass the following checklist:

- [ ] Entire selected scope source-reviewed, including private/nested declarations.
- [ ] Selected declarations pass `documentation_issues` from the project auditor.
- [ ] Selected docstrings are normalizer-clean; unsafe literals are handled explicitly.
- [ ] Executable ASTs match the module's pre-edit snapshot; comments/headers are preserved.
- [ ] No new Ruff findings and no whitespace errors.
- [ ] Runtime-doc consumers checked; appropriate runnable examples and focused regressions pass.
- [ ] Terminal exits, skips, failures, hashes, and partial-file status saved in the checkpoint.
- [ ] Module status updated and execution stopped.

For complete files, use `scripts/audit_project_docstrings.py --check <files>` and
`scripts/normalize_docstrings.py --check <files>`. For partial files, resolve the
selected stable IDs through the auditor's `definitions()` traversal and check only
those definitions. Check normalizer replacements for those nodes with their actual
implicit-receiver context; do not require unrelated unfinished docstrings to pass,
or rewrite them to make the selected unit green. Complete-file checks are mandatory
before file promotion. Static compliance is not evidence of descriptive accuracy.

At subsystem milestones, run its broader regression suite, documentation contracts,
the full quality runner, and inventory reconciliation. Reuse valid unchanged evidence
instead of repeating broad checks after every small module. Required focused tests
are not replaced by unrelated passing tests. Standalone probes importing LiuXin
must use isolated runtime directories when pytest is active; avoid the previously
observed shared-directory cleanup race.

The programme is complete only when every current project-owned file is fully
reviewed, no assignments are partial/blocked/unassigned, the whole-project audit
has no required structural findings or parse failures, and applicable regression,
documentation, typing/formatting, dependency, and ownership checks pass. Preserve
separate native-C/runtime-doc evidence. Environment-dependent checks remain explicitly
unverified until run. Final manifests, portable developer links, and counts must agree.
