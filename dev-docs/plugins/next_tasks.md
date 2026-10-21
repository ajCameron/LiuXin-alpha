# Plugin next tasks

Reviewed **2026-10-20**, working tree based on `fc5de7af`.
Status: **proposed follow-up**. The package merger, namespace retirement, shared
priority rules and passive startup are implemented. The
[composition contract](plugin-composition.md) remains the supported behaviour.

## PL-01 — Report provider availability explicitly

**First; architecture A13.** Add a structured discovery/initialization outcome
containing provider identity, capability and failure reason. Start with unsupported
device registrations, whose removed dependency is currently tolerated during
registry assembly. Distinguish disabled, unavailable and failed providers from an
available provider that returned no result.

Done when Core/CLI discovery can explain one real unsupported family without
parsing stderr, working providers still load, and interrupts remain visible.
Keep ordinary failure isolation and stream restoration. Do not catch more errors
merely to make every registration appear usable.

## PL-02 — Specify registry and component lifetime

The shared startup lock protects registry publication and retry, not every plugin
side effect or concurrent conversion. Specify how initialized instances and
settings behave across refresh, profile changes, cancellation and teardown.
Identify which instances are safely shared and which need per-operation state.

Done when interrupted refresh preserves the published fleet, removed writers leave
no stale dispatch entries, and profile-sensitive providers cannot silently use
another deployment's paths. Extend startup-order/reentrancy tests with the chosen
lifetime contract; avoid claiming arbitrary initialization can be rolled back.

## PL-03 — Retire unsupported compatibility interfaces by caller

Inventory `plugins.cache`, `folder_store_driver`, abstract APIs and retained
application families. Decide supported, adapter, unavailable or retired status
for each real consumer. Use shared loading only where contracts match; database,
storage and terminal registries retain domain-specific capability checks.

Done when the selected legacy interface has migrated callers or an explicit
unavailable result, installed-package checks reject retired paths, and persisted
plugin configuration still reads correctly. Retain the historical `customize`
settings filename as data until a separate data migration is specified.

## PL-04 — Keep standalone formats independent

Continue removing provider/record dependency cycles through lower-level contracts,
without importing application assembly from generic mechanics. Conversion loss
receipts and process-CWD isolation belong to format/conversion owners, with plugin
lifetime changes coordinated here.

Done when a changed provider runs through both intended hosts, neutral import
budgets remain bounded, and real standalone conversion succeeds without application
startup. An expanded count budget must not conceal a forbidden dependency.

## Validation and close-out

Run affected [plugin tests](../../tests/plugins/),
[runtime regressions](../../tests/file_formats/test_plugin_runtime.py), the
layer/cold-import checks and installed-wheel coverage when packaging changes.
Keep the 2026-10-19 results dated. Record each implementation result here and in
the composition contract; these tasks do not reopen the completed rename.
