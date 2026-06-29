# Metadata container test surface

This note freezes the minimum regression-test surface for the metadata container cleanup pass.

## What stage 16 is meant to protect

The metadata container architecture now has enough moving parts that "it imports on my machine" is no longer a sufficient confidence signal.

The minimum ongoing test surface is:

1. package import and root export smoke tests
2. one instantiate / validate / write-payload smoke test per container family
3. API-vs-implementation parity checks for families that intentionally share the same public names
4. a core W/E/M/I symmetry test covering identity APIs, metadata APIs, implementation containers, hydrators, and metadata DB source getters

## Scope notes

These tests are intentionally light.

They are not trying to exhaustively validate every family-specific rule. Their job is to catch structural regressions while the package surface is still being normalised.

## Families currently covered directly

- agent credit containers
- agent participation snapshot
- titles
- notes
- labels
- genres
- subjects
- identifiers
- core W/E/M/I identity + metadata + hydrator + DB source symmetry

## Follow-on work

Later stages can and should add deeper behavioural tests once the public package shape is more settled.
