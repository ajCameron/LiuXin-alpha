# Metadata Container Architecture - 2026-04-23

This note records the architecture decision for the metadata container cleanup pass.

## Current decision

The metadata container system is split into three families:

1. **Core WEMI**
   - Work / Expression / Manifestation / Item identity and metadata surfaces.
   - This is the load-bearing metadata model and should live in its own package area.

2. **Additional metadata**
   - Attached metadata families such as titles, identifiers, notes, genres, subjects, labels, and similar secondary record content.
   - These are not independent first-class entities by default.
   - They should live in a separate package area from core WEMI.

3. **Read-side views / snapshots / slices**
   - Explicit query-result containers such as participation snapshots or WEMI title slices.
   - These are not identities and not editable metadata bundles.

## Default API rule

For objects with their own durable identity, use:

- `XIdentityAPI` for the thing itself
- `XMetadataAPI` for its database-backed metadata bundle

This applies to W/E/M/I and storage entities. Agent still needs an explicit final decision on whether it also gets a real `AgentMetadataAPI`.

## Important consequence

Do not keep mixing core WEMI and attached metadata families in the same conceptual bucket.

The cleanup pass should explicitly separate:

- core WEMI packages
- additional metadata packages
- read-side snapshots / slices

## Near-term follow-ups

1. Split package placement along the new core-vs-additional boundary.
2. Align naming to the `IdentityAPI` / `MetadataAPI` pattern.
3. Quarantine or delete older container API scaffolding that does not fit the new shape.
4. Review agent as a special case.
