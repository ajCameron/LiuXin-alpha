# Project-level goals

These goals guide design and review across LiuXin. They are not a release
checklist; subsystem contracts and tests remain the authority for concrete
behaviour.

## Preserve information safely

LiuXin should make the state and location of catalogue data and digital assets
explicit. Operations that can lose, overwrite, or orphan information need clear
preconditions, observable outcomes, and recoverable failure boundaries.

## Offer coherent public workflows

Application-facing APIs should express what a caller is trying to accomplish.
Infrastructure details, persistence rows, backend quirks, and orchestration
steps should remain behind the boundary that owns them. Lower-level APIs remain
available when their additional control is useful.

## Make the code and APIs readable to humans and AI systems

Readability for both human contributors and AI-assisted development is an
explicit project goal, not an incidental style preference. A reader should be
able to identify an operation's intent, inputs, side effects, owner, and result
without reconstructing them from distant implementation details.

In practice this means:

- prefer domain names over abbreviations and context-dependent shorthand;
- provide a convenient task-oriented API over explicit lower-level value and
  persistence contracts;
- keep one authoritative implementation path instead of parallel convenience
  and expert behaviours;
- document ownership, transaction, lifecycle, and partial-failure boundaries;
- use typed immutable values when they make state transitions easier to see;
- keep modules focused enough that source navigation reveals the architecture;
- include examples and tests that demonstrate intended use rather than only
  implementation mechanics; and
- favour predictable, inspectable control flow over clever compression.

Readability does not mean hiding useful mechanics. Explicit declarations,
records, plans, reports, and configuration objects remain public where callers
need reproducibility, persistence, or precise control. Convenience façades
should construct those values and delegate to the same underlying operation.

## Keep contracts verifiable

Important behaviour should be expressible in tests and static checks. Public
types distinguish intent from persisted facts and observed physical state.
Compatibility surfaces should be deliberate, bounded, and tested.

## Evolve incrementally

LiuXin is an alpha project with substantial inherited code. Improvements should
move ownership toward the documented architecture while preserving useful data,
tests, and compatibility paths. Transitional adapters should identify their
purpose and intended removal condition.
