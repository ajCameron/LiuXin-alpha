# Metadata cache follow-ups, 2026-11-22

The user requested all three review follow-ups, with Core delivered separately,
and explicitly included the existing-PostgreSQL schema upgrade. No merge is
authorized by this task. Preserve the original checkout, data submodule and stashes.

Core is committed at `c9fdb07d` and delivered as [PR #215](https://github.com/ajCameron/LiuXin-alpha/pull/215),
targeting `cache-review`. Its 115 distinct targeted cases and complete modern
quality gates pass. The ordinary native vocabulary writer's serialized hierarchy
unit and preferred name columns were repaired during the neighboring-write audit.

Cache lifecycle changes extend owned-transaction completion guards to schema
writers, including LIVE storage. Writer application is guarded before link/value
resolution. Borrowed raw transactions reject before persistence. The final
lock-order follow-up acquires the database mutex before retaining the cache mutex,
for reads and reloads as well as writes. Forty-two focused lifecycle/metadata
cases and a separate 31-case transaction rerun passed before that last extension.
The final expanded lifecycle suite passed: 138 cases, with six expected skips.
Forty-five transaction/reload cases and nine upgrade cases joined the manifest.
The live-capture follow-up adds sixteen cases, giving 3,139 distinct requirements.

The reload concurrency case exposed the live backend inheriting a snapshot data
refresh that repeatedly rebuilt the entire database while walking live proxies.
An explicit live override now rebuilds once. Three focused tests pass, checking
the storage/facade entry points, fresh values, preserved proxy identity and the
competing-reader transaction boundary. Obsolete runs with the old slow refresh
were stopped and restarted against the fix; they are not acceptance evidence.

The explicit PostgreSQL upgrade previews a fingerprinted plan and revalidates it
under exclusive locks before applying transactional DDL. It repairs generated
typed-link uniqueness and adds Series sibling position, preserving existing rows.
Nine live tests pass, including saved-plan CLI, rollback, stale identity and
custom-schema rejection. The final lock-list adjustment also passes its targeted
three-case rerun. The disposable PostgreSQL cluster is stopped. The upgrade never
runs implicitly during ordinary opening.
See `dev-docs/databases/postgres-catalog-upgrade.md`.

The long-test runner now has a metadata lane. The full 10,000-Tag/1,000-Work,
12-cycle baseline was interrupted at the user’s explicit request to optimize
capture first. Its uncached call passed in 3,524.95 seconds; the live case was
incomplete and later backends did not run. This is not a passing lane result. Source hashes are captured; concurrency refinements made after
launch have separate targeted coverage and must not be described as full-soak
coverage without checking the recorded source set.

The realistic workflow benchmark records picker/select/search/edit/save costs,
nested live captures and full refreshes, plus an independent-handle writer
reservation experiment. A preliminary run accidentally measured relationship
no-ops in later backends; a corrected full 1,000-Tag/100-Work/seven-repeat run has
completed. A final measurement rerun excludes readback assertion captures from
nested capture samples. Optional profiles separate attribution from ordinary
latency. Results expose expensive Row construction, captures and refreshes;
these are not a performance sign-off. The user explicitly selected capture
optimization before completing the full soak. Native live execution now uses
per-call demand-driven views: point reads avoid full table scans, queries load
needed populations and relationship reads capture only their routes. Existing
portable SQL macros retain transaction ownership. Lightweight adapters retain
the eager fallback. No live result is reused across public operations. The value
audit also retained driver declared-type conversion after portable raw reads,
including binary buffers. All sixteen focused cases pass: fourteen SQLite/APSW
and two live PostgreSQL. The broader capture run passed 150 cases with four
expected skips before that conversion refinement; affected live scenarios are
now passing in an eighteen-case rerun. The original baseline and interrupted pre-conversion artifacts are
retained. The unchanged full workload has restarted on the corrected sources.
Do not attribute older soak evidence to this source fingerprint.

All 80 documentation/tooling checks pass. The complete quality runner passes on the final
capture implementation, including both type checkers. The final 1,000-Tag/100-Work/seven-repeat benchmark is complete, and the corrected
four-mode smoke run passes in 117.36 seconds. The full soak remains active. The separate Core PR’s full-suite
CI check failed after about an hour; its job logs are being inspected, while the
database ratchet remains pending. Do not claim all remote checks passed. Final
measurements, source audit, completed regression evidence, commit and PR #214
update remain pending. See `dev-docs/caches/metadata-followups-2026-11-22.md`.
