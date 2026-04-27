# Metadata container boundaries

Date: 2026-04-25
Status: stage-9 boundary pass

This note makes the current metadata container families explicit in terms of
what *kind* of object each family is.

The point of this pass is not to move more files. It is to stop the package
surface from being semantically muddy.

## The five categories

### 1. Core WEMI identity objects

These answer: **what is the entity itself?**

They are the smallest stable surfaces for durable database-backed entities.
They should not act like query slices or generic metadata bags.

Families in this category:
- `WorkIdentityAPI` / `WorkIdentity`
- `ExpressionIdentityAPI` / `ExpressionIdentity`
- `ManifestationIdentityAPI` / `ManifestationIdentity`
- `ItemIdentityAPI` / `ItemIdentity`
- `AgentIdentityAPI` / `AgentIdentity`

### 2. Core WEMI metadata bundles

These answer: **what is the editable metadata bundle around a core WEMI
entity on the database?**

They are not generic row proxies and they are not read-side joined views.

Families in this category:
- `WorkMetadataAPI` / `WorkMetadata`
- `ExpressionMetadataAPI` / `ExpressionMetadata`
- `ManifestationMetadataAPI` / `ManifestationMetadata`
- `ItemMetadataAPI` / `ItemMetadata`

### 3. Additional metadata families

These answer: **what attached metadata records belong to a WEMI entity?**

These are value-object container families. They are editable metadata, but they
are not independent identity objects by default.

Families in this category:
- titles
- notes
- identifiers
- labels
- genres
- subjects
- agent credit containers

These families may later be promoted into independent authority entities, but
that would be a separate architectural choice.

### 4. Agent intrinsic profile

This answers: **what metadata belongs to the agent itself?**

This is deliberately *not* called `AgentMetadataAPI`, because that name would
sound too much like the WEMI metadata bundles.

Family in this category:
- `AgentProfileAPI` / `AgentProfile`

This is the home for intrinsic agent data such as aliases, identifiers, notes,
labels, and biographical or organisational descriptive fields.

### 5. Read-side views / snapshots / slices

These answer: **what joined or layered view do we want to present?**

They are query-result objects, not editable metadata bundles.

Families in this category:
- `AgentParticipationSnapshot`
- `AgentParticipationsByRole`
- `ItemWemiTitleSlice`
- future browse/report/query views

## The practical rule

When looking at a container family, ask these questions in order:

1. Is this the durable entity itself?
   - Then it is an identity object.
2. Is this the editable database-backed metadata bundle around a core WEMI
   entity?
   - Then it is a WEMI metadata bundle.
3. Is this metadata attached to a WEMI entity but not an independent entity in
   its own right?
   - Then it is an additional metadata family.
4. Is this intrinsic metadata about an agent?
   - Then it belongs in the agent profile.
5. Is this a joined or layered read model?
   - Then it is a snapshot/view/slice.

## What this stage changes

This stage mainly changes **naming clarity and documentation clarity**.
The current package layout still has core WEMI entities and additional metadata
families under the same canonical package roots. That is acceptable for the
moment, as long as the semantic categories stay explicit.

## Immediate follow-up

- Stage 10 should continue the naming-normalisation pass.
- Future refactors can physically separate core WEMI from additional metadata
  once the semantic boundaries are fully stable.
