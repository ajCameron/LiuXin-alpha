# Storage design patterns

This guide records recurring implementation patterns for
`LiuXin_alpha.storage`. The [storage API guide](storage_api.md) remains the
authority for subsystem boundaries; this document explains how related public
operations should be shaped.

## Declaration-backed convenience façades

Storage creation and replacement operations often need both an ergonomic call
and a complete immutable description of intent. Implement them as a pair:

1. A task-oriented convenience façade accepts ordinary caller values.
2. The façade normalizes those values and constructs the relevant
   `*Declaration`.
3. A public declaration-taking method owns validation beyond value construction,
   deduplication, policy checks, transactions, persistence, and side effects.
4. The underlying method returns a `*Record`, result, or assigned identity.

For example:

```python
asset = manager.declare_asset(
    4,
    {"sha256": "abcd"},
    name="known object",
)

# The precise form remains available.
asset = manager.declare_digital_asset(
    DigitalAssetDeclaration(
        4,
        (Digest("sha256", "abcd"),),
        DigitalAssetMetadata(name="known object"),
    )
)
```

The convenience method must delegate to the declaration-taking method rather
than reproduce its behaviour. This gives ordinary callers a readable API while
retaining a serializable, comparable form for persistence, testing, replay, and
advanced use.

### Naming

Use the verb natural to the boundary:

- manager façades describe the task, such as `declare_asset`,
  `create_composite`, and `record_derivation`;
- narrowly typed repositories should use `add` for ordinary values, with
  `add_from_declaration(declaration)` as the precise persistence operation;
- reconstruction and durable-intent boundaries should say `from_declaration`
  or `save_workflow_declaration` explicitly, paired with `create` or
  `save_workflow` conveniences.

Do not rename a declaration-taking method merely to make it private. It remains
useful for callers that already have durable intent or need exact control.

### Ownership rules

The façade may:

- accept mappings or iterables and materialize stable tuples;
- accept records as well as IDs and extract their identity;
- normalize enums and compatibility aliases;
- construct nested metadata and declaration values; and
- reject malformed convenience-only input before delegation.

The façade must not independently:

- allocate persistent IDs or revisions;
- duplicate deduplication, policy, reference, or transaction decisions;
- publish, delete, or verify bytes before calling the owning operation;
- catch failures that the underlying contract exposes; or
- return a weaker result solely for convenience unless its name documents that
  projection.

### Declarations are not prepared transactions

Constructing a declaration has no external side effect. It captures validated
intent before an operation assigns identity or changes state. A returned record
describes manager- or repository-maintained facts, but durability still follows
the documented transaction boundary.

Nested declarations do not automatically require another façade. For example,
a `BackupSourceDeclaration` retained inside workflow intent is a domain value,
not necessarily a standalone command. Add a façade when callers are otherwise
forced to repeat construction boilerplate to perform a public task.

### Current storage pairs

| Convenience façade | Declaration-taking operation |
| --- | --- |
| `StorageConvenienceAPI.declare_asset` | `DigitalAssetRegistryAPI.declare_digital_asset` |
| `StorageConvenienceAPI.create_composite` | `CompositeDigitalAssetAPI.declare_composite_digital_asset` |
| `StorageConvenienceAPI.replace_composite` | `CompositeDigitalAssetAPI.replace_composite_digital_asset` |
| `StorageConvenienceAPI.record_derivation` | `DigitalAssetDerivationRegistryAPI.record_digital_asset_derivation` |
| repository convenience APIs' `add` methods | base repository `add_from_declaration(declaration)` methods |
| `CompositeDigitalAssetRepositoryConvenienceAPI.replace` | `CompositeDigitalAssetRepositoryAPI.replace_from_declaration(..., declaration)` |
| `BackupWorkflowAPI.create` | `BackupWorkflowAPI.from_declaration` |
| `BackupWorkflowRepositoryAPI.save_workflow` | `save_workflow_declaration` |
| `BackupWorkflowRepositoryAPI.record_backup_source_presence` | `record_backup_presence` |

Tests for a façade should verify the exact declaration passed to the underlying
method. Behavioural tests belong primarily to that underlying method so there is
one authoritative path to maintain.

The repository convenience APIs extend, rather than enlarge, the
`runtime_checkable` base protocols. This preserves structural compatibility for
minimal or third-party repository implementations. The shipped repositories and
unit-of-work properties expose the extended APIs.

## Intent, records, and observations

Use suffixes consistently:

- `Declaration` is caller-supplied or durable intent before assigned identity;
- `Record` is manager-maintained public state with identity and usually a
  revision; and
- `Observation` is evidence about physical or external state at a point in
  time.

These values should not be substituted for one another merely because their
fields overlap. See the project [style guide](<../00 - Style Guide.md>) for the
complete public-value naming policy.

## Physical work and metadata transactions

Store publication and metadata persistence are separate failure domains. A
metadata rollback cannot generally undo externally published bytes. Manager
workflows own ordering, journals, recovery evidence, and cleanup; repositories
only persist the declarations and records assigned to them.
