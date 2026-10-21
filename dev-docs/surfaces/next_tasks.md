# Next surface tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

Surfaces parse and present. Named Core operations own application behaviour, and server-side enforcement must back any read-only UI promise.

## SURF01. Enforce remote capabilities in Core

**First; architecture A06.** Define which operations each session may invoke and reject unauthorized calls before handler execution. The current transport-stable marker is an encoding flag; it does not supply authorization.

Done when a restricted client cannot invoke admin/write operations by name, authenticated and unauthenticated outcomes are explicit, and read-only presentation is not the only guard.

## SURF02. Verify GUI local/remote and lifecycle parity

**Next.** Use the [GUI plan](tkinter/tkinter-gui-implementation-plan.md) for supported metadata edits, session replacement, late callbacks and client timeout behaviour.

Done when local and remote edits produce equivalent receipts/errors, closure releases owned resources, and a timed-out mutation is not falsely labelled cancelled.

## SURF03. Finish entry-point composition through the owner

**Following; architecture A11.** Migrate remaining path-opening and legacy composition wrappers through LibraryServices/SurfaceCoreSession where appropriate. Preserve the bounded PostgreSQL provisioning and renderer exceptions deliberately.

Done when the migrated entry point has one explicit resource owner on success and partial startup failure, and the exact AST exception list shrinks rather than broadens.

## SURF04. Bound read work without hiding failures

**Following; CAT04.** Measure category/listing scans behind paged transport. Preserve missing versus unavailable distinctions and make limits/completeness visible in the result.

Done when a representative large catalogue uses bounded server work, cross-surface counts agree, and query/iteration failures cannot become empty successful responses.

## Validation and routing

Use the surface dependency, read-error, CLI grammar and GUI backend/session tests. Existing startup commands are runbooks; this review did not start listeners or verify remote firewall, TLS, GitHub or authentication configuration.

Return to [surface](README.md).
