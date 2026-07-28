# Core API Surface

Date: 2026-03-15

Historical first slice. Superseded by
[core-api-completion-2026-07-25.md](core-api-completion-2026-07-25.md).

Built on top of:
- main repo commit `d4ffd1a` (`Fix full-suite regressions and align fixture/contracts`)
- green full-suite baseline from `/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/test-results/full-suite-2026-03-15-001736.json`

Implemented first explicit core API slice:

- added descriptor models under `src/LiuXin_alpha/core/description.py`
- added shared write/read dispatch classifier under `src/LiuXin_alpha/core/dispatch.py`
- `CoreRuntime` now registers metadata for named commands and queries
- new query: `api.describe`
- `CoreRuntime.describe_api(...)` returns:
  - named command/query surface
  - transport stability flags
  - payload field hints
  - hosted target descriptions for `library`, `database`, and `storage`
  - per-method signatures, write/read classification, and doc summaries

Transport and proxy surface:

- HTTP GET endpoint: `/api/describe`
- `RemoteLibraryProxy.describe_api(...)`
- `LocalLibraryProxy.describe_api(...)`

Validation:

- `tests/core/test_core_runtime_phase1.py`
- `tests/core/test_core_http_daemon_phase2.py`
- result: `15 passed`

Important scope note:

- this slice is descriptive, not restrictive
- generic `invoke` still exists and still allows broad target method dispatch
- the full suite has not been rerun after this slice yet; only the targeted core tests above were rerun
- next API slice should convert high-value `library` operations away from generic `invoke` into named RPCs with stable request/response shapes
