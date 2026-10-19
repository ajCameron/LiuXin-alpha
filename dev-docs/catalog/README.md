# Catalog

Catalog owns metadata-aware persistence, identity, matching and retrieval. Start
with the contracts; roadmap and migration records provide historical context.

## Current contracts

- [API usage](catalog-api-usage.md): public entry points and examples.
- [Write validation](catalog-write-validation-contract.md): semantic boundary,
  stored identities and explicit exclusions.
- [Matching policy](catalog-matching-policy.md): identity decisions.
- [Cache boundary](catalog-cache-boundary.md): derived reads and reconciliation.
- [Link writers](catalog-link-writer-architecture.md): schema-driven relationships.
- [Cache backends](<../08 - Storage Cache Backends.md>) and
  [read failures](../read-model-failures.md): backend capabilities and failure visibility.

## Reviews and migration history

- [Legacy mutation migration](catalog-legacy-mutation-migration.md).
- [Fitness review](catalog-fitness-review.md).
- [Roadmap](catalog-roadmap.md): dated planning, not proof that every old route is gone.

Metadata values belong to [metadata](../metadata/README.md); hydration boundaries
are described with the [container guides](../metadata_container/README.md).
Use [test streams](../hacking/test-streams.md) for validation commands.

Return to the [developer index](../README.md).
