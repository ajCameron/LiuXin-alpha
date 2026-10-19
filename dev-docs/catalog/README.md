# Catalog

Catalog owns metadata-aware persistence, identity, matching and retrieval. Start
with the contracts and current work list. Dated reviews preserve earlier findings
and verification; their old test counts are not current checkout results.

## Current contracts

- [API usage](catalog-api-usage.md): public entry points and examples.
- [Write validation](catalog-write-validation-contract.md): semantic boundary,
  stored identities and explicit exclusions.
- [Matching policy](catalog-matching-policy.md): identity decisions.
- [Cache boundary](catalog-cache-boundary.md): derived reads and reconciliation.
- [Link writers](catalog-link-writer-architecture.md): schema-driven relationships.
- [Cache backends](<../08 - Storage Cache Backends.md>) and
  [read failures](../read-model-failures.md): backend capabilities and failure visibility.

## Current work and migration status

- [Roadmap](catalog-roadmap.md): implemented ownership changes and remaining work.
- [Legacy mutation migration](catalog-legacy-mutation-migration.md): completed caller
  migration, the live Row adapters and retired Library facades.

## Historical review

- [Fitness review](catalog-fitness-review.md): July findings and remediation evidence.

Metadata values belong to [metadata](../metadata/README.md); hydration boundaries
are described with the [container guides](../metadata_container/README.md).
Use [test streams](../hacking/test-streams.md) for validation commands.

Return to the [developer index](../README.md).
