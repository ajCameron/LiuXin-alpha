"""
Organize storage contracts, implementations, and workflows into explicit modules.

Import shared contracts from LiuXin_alpha.storage.api and concrete objects from
their owning modules, such as LiuXin_alpha.storage.durable_manager.StorageManager
and LiuXin_alpha.storage.store_container.StoreContainer. Importing this package
alone does not load storage implementations or publish compatibility aliases.

Separation of concerns: storage owns bytes, assets, stores, locations, replicas,
placement, backup execution, verification, repair, and physical reconciliation.
It may persist storage state through generic database capabilities but does not
own bibliographic identity, metadata merge policy, or Catalog write validation.
Library workflows coordinate Catalog and storage without moving either domain's
rules into this package.

See ``dev-docs/04 - Seperation of Concerns.md`` and
``dev-docs/storage_and_backup/README.md``.
"""
