# Full Suite Green

Date: 2026-03-15

Latest passing full-suite report:
- `/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/test-results/full-suite-2026-03-15-001736.json`

Main repo checkpoint:
- commit `d4ffd1a` (`Fix full-suite regressions and align fixture/contracts`)

Summary:
- `2582 passed`
- `26 skipped`
- `22 xfailed`
- `5 xpassed`
- `0 failed`

Main clusters fixed in this pass:
- metadata/plugin loader regressions
- resource/config isolation leakage
- SQLite driver adapters, search, schema helper, and tree/hash helper regressions
- Calibre fixture snapshot/report mismatches

Still notable:
- one remaining warning in `src/LiuXin_alpha/utils/date.py` from `datetime.utcfromtimestamp`
- `5` tests are `xpass` and should have their stale `xfail` markers reviewed
- `26` skips remain, including optional dependency coverage such as `pypdf`

Important repo-state note:
- the nested data repo `LiuXin_alpha_data` has its own local modifications, including refreshed Calibre fixture snapshots and other dirty files
- the main repo commit does not capture those nested-repo changes
