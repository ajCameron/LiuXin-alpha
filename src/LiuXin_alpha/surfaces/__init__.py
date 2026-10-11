"""
Application front ends and shared presentation services.

Use ordinary submodule imports; importing this namespace loads no implementations.

Separation of concerns: surfaces own argument parsing, transports, presentation,
and conversion between external representations and Core commands/queries. They
may reject malformed input early for clearer feedback, but lower boundaries do
not trust that check: Catalog still guarantees semantic mutations and databases
still enforce structural integrity. New surfaces do not construct lower-layer
services or write database rows directly.

See ``dev-docs/04 - Seperation of Concerns.md``.
"""
