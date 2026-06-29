# Metadata container dynamic convenience property policy

Date: 2026-04-25
Status: stage-11 policy pass

This note records the current policy for runtime-installed convenience
properties on metadata container classes.

## Why this exists

Several metadata container families expose a generic explicit API and then add a
layer of convenience sugar at import time.

Examples:
- `authors`, `authors_ids`, `authors_text`
- `isbn_13`, `isbn_13_values`, `isbn_13_text`
- `main_titles`, `main_titles_text`
- `descriptions`, `descriptions_text`

These convenience properties are useful for callers and keep day-to-day code
pleasant, but they are *not* the load-bearing core API.

## Current policy

Dynamic convenience properties are allowed **for now** under the following
rules:

1. **They must be convenience sugar only.**
   - Core logic, validation, and write-payload generation must live on explicit
     methods and explicit container classes.
   - Runtime-installed properties must not be the only way to reach essential
     behavior.

2. **They must be installed at module import time only.**
   - Use a private helper such as `_install_kind_convenience_properties(...)`.
   - Do not patch classes from outside their defining module.
   - Do not mutate the surface repeatedly at runtime.

3. **They must be driven by a canonical controlled vocabulary.**
   - Roles come from role enums.
   - Kinds come from kind enums.
   - Identifier scheme sugar comes from the canonical identifier scheme lists.

4. **They must follow the naming conventions note.**
   - Default joined text properties use `*_text`.
   - Separator-taking methods use `*_to_text(sep=...)`.
   - Singular display helpers remain explicit domain helpers such as
     `display_title`.

5. **They must remain visibly optional.**
   - Docstrings should state that the dynamic properties are convenience sugar.
   - Callers should still be able to use the explicit generic API directly.

## Why we are not generating static source yet

The controlled vocabularies are still settling.

Until roles, kinds, schemes, and similar sets are stable, generating explicit
static properties would create churn and duplicated maintenance. The runtime
installer pattern is the least-bad temporary compromise.

## Planned end-state

Once the relevant vocabularies are stable enough, the preferred end-state is:

- one canonical source of truth for controlled vocabularies
- generated explicit source or generated stubs for convenience properties
- static typing support for the convenience surface
- database constraints generated from the same canonical definitions where
  appropriate

That future code-generation step should replace or supersede the dynamic
installer pattern rather than coexist indefinitely.

## Testing rule

Tests should primarily target the explicit generic API.

Convenience properties should get smoke coverage, but they should not become the
only tested surface, because they are intentionally a temporary ergonomic layer.

## Families currently using this policy

At the time of the stage-11 pass, the following families use runtime-installed
convenience properties:

- agent credit containers
- identifier containers
- title containers
- note containers
- label containers
- subject containers

Genres deliberately do not currently use this pattern.

## Practical summary

Dynamic convenience properties are currently an accepted temporary compromise.
They are allowed because they improve ergonomics while vocabularies are still
moving.

They are not the architectural ideal, and future code generation should replace
or formalise them once the controlled vocabularies are nailed down.
