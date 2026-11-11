# Maintenance scale evidence

Started: **2026-11-02**. Updated: **2026-11-03**. Measurements use the local WSL2 host, Python 3.12.3,
SQLite and a private PostgreSQL 16 cluster. This is synthetic Catalog metadata
traffic with real transactions, maintenance capture and the durable jobs worker.
It is not a SquashFS import, a host power-loss test or a production capacity claim.

## Workload and correctness

[benchmark_maintenance.py](../../scripts/benchmark_maintenance.py) creates
historical Works/Expressions before installation, imports captured changes,
starts a projection rebuild, closes/reopens Core and resumes the checkpoint.
Another Core imports concurrently while the real worker drains the backlog.
Every Work's count and source/projection revisions are checked independently
against physical rows. Completed journal records are then pruned in bounded pages.
Scratch databases and job ledgers live under `/tmp` and are removed afterward;
each PostgreSQL trial drops only its randomly named test schema.

## Equivalent small trials

Each trial has 100 historical, 100 captured and 20 concurrent Works, with 440 lazy
obligations. All trials completed, verified every Work and drained/pruned the journal.

| Backend | Original drain | Page publication drain | Application peak RSS after change |
| --- | ---: | ---: | ---: |
| SQLite | 132.70 s | 37.69 s | 95.3 MiB |
| PostgreSQL | 209.46 s | 67.40 s | 95.2 MiB |

The change publishes a bounded page once and replenishes it after its delivery
budget or worker idleness. Associated job runs are read in one bounded ledger
query. Each obligation still has its own durable job, claim and receipt; failure,
dependency and retry semantics are unchanged. Measured drain time improved by
about 3.5x on SQLite and 3.1x on PostgreSQL. These are single local trials, with
ordinary host activity and no confidence intervals.

Artifacts: [SQLite baseline](maintenance-sqlite-baseline-2026-11-02.json),
[SQLite changed](maintenance-sqlite-paged-2026-11-02.json),
[PostgreSQL baseline](maintenance-postgres-baseline-2026-11-02.json),
[PostgreSQL changed](maintenance-postgres-paged-2026-11-02.json).

## Larger SQLite run

The [2,200-Work run](maintenance-sqlite-soak-2026-11-02.json) used 1,000 historical,
1,000 captured and 200 concurrent Works. It completed in **699.01 seconds**, with
**519.42 seconds** in the concurrent-import/drain phase and 4,401 worker passes
(including an idle/refill pass). All 2,200 counts/revisions matched. The final
backlog was zero, and pruning removed all 4,400 journal changes and their completed
obligations. Application peak RSS was **104.1 MiB**, compared with about 95 MiB in
the small trial. This is encouraging bounded-workload evidence, not proof of
constant memory over arbitrarily large libraries.

After pruning, the catalogue occupied **7,401,472 bytes** and the completed jobs
ledger **9,326,592 bytes**. Removing journal rows makes database pages reusable;
it does not shrink files. Maintenance pruning does not prune the jobs ledger.
The subsequent `maintenance prune-jobs` command implements bounded, age-based
history retirement after checking catalogue completion and current journal identity.
It removes approved runs, events, keys and orphaned definitions, making ledger
pages reusable. It does not compact the file. These historical measurements
predate that command; deleting a ledger while obligations still refer to it is
not a cleanup strategy. See [the retention policy](../../docs/maintenance.md#pause-retry-and-reclaim-journal-space).
PostgreSQL server memory is excluded from process RSS in all reports.

## Larger PostgreSQL run

The [660-Work run](maintenance-postgres-soak-2026-11-03.json) used 300 historical,
300 captured and 60 concurrent Works. It completed in **774.45 seconds**, with
**257.13 seconds** in concurrent import/delivery and 1,320 worker passes. All 660
counts/revisions matched; the rebuild completed and pending work reached zero.
Pruning removed all 1,320 journal changes and 2,640 completed obligations, retaining
1,320 projections. Application peak RSS was **116.2 MiB**. Final catalogue relation
size was **7,905,280 bytes**, and the completed jobs ledger occupied **2,834,432
bytes**. The disposable schema was dropped after measurement.

Historical import alone took **253.31 seconds**, before maintenance installation;
captured import took **232.94 seconds**. Those 50-Work transactions had median
durations of 42.53 and 35.57 seconds respectively. Host noise and the small sample
prevent attributing that difference to maintenance. Long scalar-write transactions
remain a separate Catalog performance target. This smaller PostgreSQL soak does
not establish parity with the 2,200-Work SQLite workload or production capacity.

## Interpretation and remaining evidence

The delivery loop had measurable avoidable overhead and is substantially better
after page publication. PostgreSQL's scalar Catalog import is still slow even
before maintenance installation: the small baseline needed roughly 63 seconds
for 100 historical Works. That cost needs a separate Catalog-write investigation;
it must not be attributed entirely to maintenance.

A real collection import with its file walking, extraction and storage telemetry
remains necessary before setting production throughput expectations. Reproduce
the SQLite soak with:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/benchmark_maintenance.py \
  --backend SQLite --historical 1000 --works 1000 --concurrent 200 \
  --batch 50 --delivery-page 128 --drain-passes 128 --max-seconds 1800 \
  --output /tmp/maintenance-soak.json
```

Use a new output path for each run. PostgreSQL additionally requires
`LIUXIN_TEST_POSTGRES_URL`; the script creates its own schema. To reproduce the
PostgreSQL soak, select `--backend PostgreSQL --historical 300 --works 300
--concurrent 60`, retaining the other limits. The time budget is checked between
phases/pages and does not forcibly interrupt a running transaction.
