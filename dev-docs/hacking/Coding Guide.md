# Coding guide

Reviewed **2026-10-20**, working tree based on `fc5de7af`.
This guide owns implementation principles. The [style guide](<Style Guide.md>)
owns naming, formatting and documentation form; the
[ownership guide](<../04 - Seperation of Concerns.md>) owns subsystem boundaries.

## Make contracts explicit

Name an object for its responsibility and distinguish a value, service and
resource owner. State borrowed versus owned collaborators, transaction boundaries,
error behaviour and partial effects where callers depend on them. Prefer a named
capability over an implicit global, broad dynamic lookup or another facade layer.

Introduce abstractions for a real consumer and dependency boundary. Keep domain
rules with their owner; presentation code should not become a second validator or
storage policy implementation. A useful small helper is preferable to a general
framework without an established caller.

## Migrate callers with their owner

Avoid adding an old-import forwarding shim merely to postpone a repository caller
migration. Update source, tests, examples, documentation and installed-package
checks together. If an adapter must remain, identify its consumers, contract and
retirement condition explicitly.

Functional adapters, persisted wire identifiers and deliberate lazy imports are
not interchangeable with disposable forwarding modules. The
[shim-removal record](../architectural_reviews/shim-removal-plan.md) preserves that
distinction. Existing retained adapters need caller evidence before removal; this
guide does not declare all compatibility code dead.

## Make changes reviewable

Take one observable contract or caller family at a time. Review failure paths and
run the relevant [checks](testing/README.md). Keep unrelated working-tree changes
intact. Update comments and docs when behaviour changes; preserve dated evidence
instead of presenting an old test run as validation of new code.

These are engineering guidelines. A justified exception should explain the
tradeoff and remain visible to future maintainers; it should not silently weaken
an enforced ownership or data-integrity contract.
