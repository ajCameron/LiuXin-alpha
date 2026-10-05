# Catalogue, acquisition, and OPDS docstrings — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following the [complete shared read model](project-docstrings-read-model-2026-09-09.md).
The previous goal turn made verified progress. All six source targets and both
edited existing test modules were read completely before their documentation pass.
Work remains local on `codex/project-docstrings`, based on `edf6bf05`; no commit,
push, submodule publication, or PR mutation is part of this continuation.

## Reviewed files and behavior

- [catalog/api.py](../src/LiuXin_alpha/surfaces/catalog/api.py): borrowed read/image
  collaborators and unsynchronized explicit image overrides; shared compatibility
  token policies; exact versus normalized category selectors; falsey book-token
  behavior; duplicate-preserving ID lists; first-nonempty author-table results;
  and in-place category URL rewriting. Metadata augmentation mutates the shared
  dict and adds links for unfiltered stringified IDs, while work_rows_payload
  deliberately bypasses augmentation. Author HTML routes resolve an integer ID
  but retain its original string spelling. Visible metadata filtering preserves
  input order/raw ID types rather than page rank. Tag-browser node IDs are positional
  and editability flags are presentation metadata, not mutation authorization.
- [catalog/__init__.py](../src/LiuXin_alpha/surfaces/catalog/__init__.py): backend,
  host protocol, and built-in PNG exports without construction or reads.
- [acquisition/api.py](../src/LiuXin_alpha/surfaces/acquisition/api.py): standalone
  payload coercion (not actually called by the endpoints), list-only discovery
  receipts, unvalidated work values, and exact placeholder-dimension precedence.
  Suffix dimensions are not clamped; malformed x-size requests retain their prior
  dimensions rather than proceeding to the full-cover default. Actual image bytes
  are not resized for thumbnail requests. Cover integer/read/byte-response errors
  are caught before redirect/later-cover/placeholder fallback; format errors are
  not caught. Initial discovery, redirect, and placeholder errors remain visible.
  Format record extensions are not whitespace-stripped. Request environments are
  accepted but unused. No delivery or parsing policy was repaired incidentally.
- [acquisition/__init__.py](../src/LiuXin_alpha/surfaces/acquisition/__init__.py):
  adapter/host exports and borrowed lifecycle boundary.
- [opds/api.py](../src/LiuXin_alpha/surfaces/opds/api.py): all token, grouping,
  pagination, XML, and routing helpers plus the adapter class. Documents falsey
  encoding, raw-hex reinterpretation, repeated decoding, case-sensitive O/N/I
  prefixes, ambiguous colon-containing IDs, and unvalidated unknown categories.
  Pagination appends rather than replaces offsets and is not URL-fragment aware.
  Pager links clamp independently of actual slices. Group counts count items,
  not linked works; uppercase expansion can generate SS from sharp-s and fail
  group-route selection when the initial is normalized again. XML scalars are
  escaped, trusted entry fragments are joined verbatim, timestamps are fixed at
  the epoch, and image links always advertise PNG. Size metadata lookup catches
  Exception but length values themselves are not numerically validated.
- [opds/__init__.py](../src/LiuXin_alpha/surfaces/opds/__init__.py): exports versus
  helper-module responsibility, without starting an application or server.

These six source files cover **three classes and 66 functions** in full. Tests
remain part of the project-wide documentation goal:

- [test_acquisition_api.py](../tests/surfaces/test_acquisition_api.py): document
  all immutable records, fake Core/host methods, nested captured-payload class,
  response helper, and tests. Several retained host hooks are compatibility-only;
  their presence does not prove the current adapter calls them. Three historical
  test names overstate current coverage: the redirect fixture advertises unreadable
  content and never attempts its configured failing read; the local-target case
  returns simulated Core bytes without opening a file; missing-ID filtering occurs
  in the fake discovery producer rather than the adapter. Docstrings now state
  those boundaries explicitly while preserving the test bodies.
- [test_opds_api.py](../tests/surfaces/test_opds_api.py): fully documented frozen
  configuration, asserted-selector host double, response helper, root/search feed
  checks, and decoded category acquisition route. Examples execute without network,
  database, or actual image/ebook parsing.
- New [test_compatibility_surface_documentation_contracts.py](../tests/surfaces/test_compatibility_surface_documentation_contracts.py):
  one documented catalogue helper and eleven documented tests with executable
  examples. Covers token ambiguities, URL/offset handling, empty overlarge slices,
  expanding group initials, raw/trusted XML fragments, nonnumeric length attributes,
  list-only acquisition receipts, size precedence, and genuine cover read/response
  failure fallback. Paired format cases prove those same failures propagate.
  Additional cases exercise adapter-side malformed format filtering, shared and
  overridden image identities, mutable routes/metadata, author-table selection,
  raw ID membership, and the unaugmented bulk projection. All I/O is simulated.

Batch: **9 modules, 11 classes, 125 functions = 145 declarations**. Cumulative
reviewed Python scope: **187 modules, 246 classes, 1,993 functions = 2,426 declarations**,
plus the separately documented native C and runtime-created vacuum adapter.

## Verification

Overlapping selections must not be summed into a distinct-test total.

- Initial six-source doctests plus acquisition/catalogue/OPDS/standalone-OPDS
  regressions: **58 passed, 44 explicitly skipped examples** in 67.55 seconds.
- Fully documented existing acquisition/OPDS tests and doctests: **77 passed,
  one explicitly skipped example** in 10.04 seconds. Nested examples with skipped
  context are not claimed as separate executed integration scenarios.
- New compatibility documentation-contract tests/doctests: **23 passed** in
  13.26 seconds. The sharp-s grouping limitation is confirmed by a real routing
  call over a fake provider, not inferred solely from prose or source shape.
- Strict structural audit and normalizer --check pass across all **187 reviewed
  files**, including tracked data-submodule source. The new nine-file batch is clean.
- All **eight edited existing Python files** retain identical executable ASTs to
  HEAD after genuine leading docstrings are removed. No runtime-docstring exception
  is required for this batch; no XML template or byte payload literal was changed.
- No new Ruff findings relative to HEAD. Existing out-of-scope findings remain:
  catalogue API I001/two F401/five UP045/nine UP032; catalogue initializer I001;
  acquisition API I001; OPDS API I001/29 UP032; acquisition tests UP037/UP032/UP012;
  OPDS tests I001. Acquisition/OPDS initializers and the new test module are clean.
  The new module is independently formatter-clean. No warning was waived or fixed
  as an incidental production change.
- Full `bash scripts/run_type_checks.sh`: **passed** with unchanged scopes of
  159 formatter files, 456 annotated modules, 221 protected dependencies, 188
  strict-mypy files, and 37 invalid examples rejected by each checker. Selected
  format/lint/complexity/dependency gates and production typing pass.
- The final combined selection's handle 9174 was authoritatively missing when
  the next continuation resumed. Its final output was not observed, so no pass
  is claimed. The fresh expanded rerun is recorded in the
  [standalone OPDS continuation](project-docstrings-standalone-opds-2026-09-09.md).

Collection confirms three catalogue and three standalone-OPDS integration cases
use SQLite; the standalone module also has two non-database tests. Those two
modules were read completely and run for regression, but their docstring pass is
still outstanding at this checkpoint and they are not counted in this batch's
reviewed ledger; the next continuation completes both modules. No APSW/live
PostgreSQL, displayed UI, installed-wheel, external service, or remote-CI success
is claimed. The five known physical-line ownership conflicts are not waived or
repaired by this batch; no guard logic, limits, or default quality scopes changed.

## Inventory and next work

The whole-project audit covers **2,730 modules, 4,400 classes, 35,119 functions**,
with no parse failures. Missing/blank docs remain on **1,112 modules, 2,026 classes,
23,070 functions = 26,208 declarations**. Other overlapping findings: 4,271
delimiter-layout, 10,539 missing-example, 3,054 parameter-field, 3,995 return-field,
4,140 empty-parameter, 5,577 empty-return, and 28 missing-summary findings.

Reports remain `/tmp/liuxin-docstring-current-2026-09-09.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-09.json`. The entire project goal remains
active and unfinished. Next: document the already-read `test_catalog_api.py`
(335 lines) and `test_opds_readonly.py` (261 lines), then standalone OPDS and
Calibre-web composition (`app.py`, initializer, entrypoint in each package),
followed by remaining CLI/HTTP/web and the rest of the source/scripts/examples/tests.
Do not repeat the six completed shared-backend modules. Preserve the main ledger's
configuration-exec compatibility warning, runtime doc consumers, and submodule scope.
