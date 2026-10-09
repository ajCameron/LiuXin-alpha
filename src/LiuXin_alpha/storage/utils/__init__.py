"""
Group storage utilities by the contract boundary they operate on.

Import configured-Store operations from storage.utils.store, raw-driver
operations from storage.utils.driver, and workflow helpers from
storage.utils.workflow. Dependency-free stream adapters live in
storage.utils.streams, and shared value checks live in storage.utils.validation.
Operational-report logging adapters live in storage.utils.operational. Backend
registration, configuration-row translation, schema migrations, and Store-row
bootstrap helpers have their own focused modules here. This package performs no
imports or name forwarding.
"""
