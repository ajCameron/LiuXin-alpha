# Storage developer guide

Use this page to enter the storage subsystem by task. The durable application
manager is the normal starting point; Stores and drivers are extension
boundaries, not alternate application facades.

## Quick start

Application code normally receives `database.storage`. For a directly runnable
in-memory introduction, attach an explicit Store to the durable manager:

```python
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import MemoryStore

with StorageManager(stores=(MemoryStore(name="scratch"),)) as manager:
    asset = manager.store_bytes(
        b"book",
        name="book.epub",
        replica_mode="transient",
    )
    payload = manager.read_asset(asset, replica_mode="transient")

assert payload == b"book"
```

When a catalogue is already open, use its composed manager directly:

```python
asset = database.storage.store_file("/incoming/book.epub")
```

The historical `LiuXin_alpha.storage.store_manager` import remains compatible,
but new code should use `durable_manager`. Use
`TransientStorageManager` only when deliberately disposable catalogue state is
appropriate, usually in focused tests.

Runnable introductions live in
[`examples/storage`](../../examples/storage/README.md). Start with
`storage_manager_manual_roundtrip_example.py`, then use
`storage_manager_workflows_example.py` for replication, digest lookup, and
Composite export.

## Choose a route

| You are changing | Start here | Contract/owner |
| --- | --- | --- |
| Application storage workflows | [Storage API: starting a manager](storage_api.md#starting-a-manager) | `storage.api.storage_manager_api`; `storage.durable_manager` |
| Common ingest/read/composite calls | [Everyday convenience surface](storage_api.md#everyday-manager-convenience-surface) | focused `*_convenience.py` modules composed by `storage_manager_api/convenience_api.py` |
| Asset, Replica, Composite, or policy persistence | [Persistence SPI](storage_api.md#persistence-spi) | `storage.api.persistence_api` |
| A configured Store facade | [Store/manager responsibilities](storage_api.md#store-and-manager-responsibilities) | `storage.api.store_api`; `storage.store_container` |
| A raw physical backend | [Reusable driver core](storage_api.md#the-reusable-driver-core) | `storage.api.store_driver_api`; backend plugin module |
| Archive/container behavior | [Backend behavior](backend-behavior.md) | concrete backend plugin and its contract tests |
| Backup or other orchestration | [Storage design patterns](design-patterns.md) | `storage.api.workflow_api`; `storage.workflows` |
| Runtime composition and durable state | [Component status](storage_component_status.md) | `storage.storage_manager`; `storage.durable_manager` |
| Vocabulary and identity boundaries | [Terminology](liuxin_terminology_and_conceptual_model.md) | domain model |

## Import narrowly

Prefer the smallest contract package that owns the concept:

```python
from LiuXin_alpha.storage.api.storage_manager_api import DigitalAssetRecord
from LiuXin_alpha.storage.api.store_api import StoreAPI
from LiuXin_alpha.storage.api.store_driver_api import StorageDriverAPI
from LiuXin_alpha.storage.api.persistence_api import DigitalAssetRepositoryAPI
from LiuXin_alpha.storage.api.workflow_api import BackupWorkflowAPI
```

`LiuXin_alpha.storage.api` is a supported discovery umbrella, but importing from
the responsibility package makes dependencies and extension ownership easier to
read. Import concrete implementations from their owning modules. The
`LiuXin_alpha.storage` root deliberately exports no implementations.

## Mental model

```text
application
    -> StorageManagerAPI       identity, policy, routing, orchestration
        -> StoreAPI            one configured LiuXin Store
            -> StorageDriverAPI  one raw byte endpoint
```

The manager owns Digital Asset identity and Replica evidence. A Store translates
one stable Store configuration into file operations. A driver knows endpoint
mechanics, but never bibliography, replication policy, or database transactions.

Creation APIs consistently offer two levels: `add(...)`/`replace(...)` accept
ordinary values, while `add_from_declaration(...)` and
`replace_from_declaration(...)` preserve exact declaration-shaped control. See
[storage design patterns](design-patterns.md) for the rule and extension
guidance.

## Verification

Start with the tests nearest the changed boundary, then run the composed manager
and documentation checks. The current commands and environment-specific live
tests are listed in [test streams](../test-streams.md) and
[live storage CI](live_storage_ci.md).
