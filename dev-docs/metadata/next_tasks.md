# Next metadata tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

Keep pure value rules in metadata, semantic persistence in Catalog, and file output publication in the relevant workflow. Provider availability requires observable outcomes.

## MD01. Give dirtied events a durable delivery contract

**First.** The current queue drain is best-effort, has no explicit commit and can lose or duplicate events on malformed input or partial insertion. Design bounded transactional persistence and acknowledgement before relying on sidecar jobs.

Done when failure injection before insert, during a batch and after commit gives a stated retry/deduplication outcome; a sidecar is acknowledged only after verified publication.

## MD02. Expose provider misses and provider failures separately

**Next.** Keep existing compatibility results where needed, but add structured availability and diagnostics for disabled dependencies, blocked endpoints, parse failures and genuine no-match results. Use compact offline fixtures first.

Done when one local and one web source produce distinguishable outcomes, abort/retry behaviour is bounded, and no network probe runs by default in ordinary tests.

## MD03. Expand pure validation through the shared writer

**Next; CAT01.** Select missing identifier, date, resource or text rules from current Catalog coverage. Keep display spelling separate from normalized identity, and specify tolerant reads of historical values.

Done when all supported public write routes invoke the same rule and collision audits report data requiring explicit remediation instead of silently merging it.

## MD04. Verify embedded metadata publication and exchange losses

**Following.** Extend file-writer tests around preserved payloads, no-clobber publication, provider refusal and OPF/WEMI losses. Keep a readable file distinct from a semantically complete round trip.

Done when representative formats survive valid changes, retain original bytes on failed publication, and report unsupported or lost fields alongside the artifact.

## Validation and routing

Use [writer coverage](metadata-writer-coverage-contract.md), [container contracts](../metadata_container/README.md), [local-source tests](../../tests/metadata/local_sources) and offline web-source tests. The CLI runbook describes catalogue/file operations; it is not an authorization boundary for remote Core access.

Return to [metadata](README.md).
