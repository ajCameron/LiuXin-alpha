# Top-level architecture review

Date: 2026-09-17

Status: Review findings and recommendations; no architecture changes implemented.

This note records the supplied architectural review and its follow-up. It is
not a fresh repository-wide dependency audit. Verification findings below are
attributed to the reviews that reported them.

## Overall assessment

The essential modules are present and their broad responsibilities are sensible.
The main opportunity is to clarify ownership and dependency boundaries within
the existing structure. The review did not identify a need for additional
top-level packages.

| Module | Broad responsibility |
|---|---|
| `databases` | Persistence and database access |
| `catalog` | Bibliographic operations |
| `metadata` | Metadata models and tooling |
| `storage` | Assets and replicas |
| `file_formats` | Format handling and conversion |
| `ingest` | Acquisition workflows |
| `jobs` | Durable execution |
| `caches` | Acceleration |
| `surfaces` | External interfaces |

## Findings and recommendations

### 1. Clarify core, library and ingest ownership

The distinction between `core` runtime/API responsibilities, `library`
orchestration and `ingest` workflows needs to be explicit.

Document which layer owns application lifecycle, coordinates library operations
and runs acquisition workflows. Use concrete call paths to establish where one
responsibility ends and the next begins before considering code moves.

### 2. Reconsider storage bootstrapping inside Database

Storage bootstrapping inside `Database` deserves review because it connects
persistence initialization with storage lifecycle responsibilities.

Determine which application component should create and connect these services,
and whether database construction needs to perform that orchestration. Treat
relocation as a proposal to validate against callers and startup behavior.

### 3. Clarify metadata persistence ownership

The boundary between metadata models/tooling in `metadata` and bibliographic
persistence operations in `catalog` needs a clear contract.

Identify the owner of metadata reads and writes, including validation and
transaction boundaries. Document how metadata objects reach persistence and
which layer callers should use for mutations.

### 4. Consolidate configuration ownership

Configuration ownership appears scattered. Review where settings are defined,
loaded and applied, then establish clear owners and consistent precedence rules.

Consolidation need not mean a single configuration module; the objective is to
make each setting's source, lifetime and responsible subsystem predictable.

### 5. Give customize a clearer plugin-oriented name

Consider whether `customize` accurately communicates its plugin responsibilities.
A clearer name could make discovery and ownership easier.

Any rename should follow confirmation of the package's actual scope and an
inventory of its callers. This review does not select a replacement name or
authorize a migration.

## Follow-up priorities — 2026-09-17

The follow-up review recommends the following order of work. These are proposed
next steps; this documentation update does not implement them.

1. **Establish lifecycle ownership across core, library and ingest.** Start with
   storage bootstrapping inside `Database`. Identify which component creates,
   starts and closes the database and storage services. Add startup and shutdown
   regression coverage before changing that ownership, including cleanup after
   partial initialization failures.
2. **Make configuration precedence explicit.** Document the supported sources of
   configuration, their override order and when values take effect. Verify that
   competing settings resolve consistently with the documented rules.
3. **Strengthen typed subsystem contracts.** Define the interfaces at the
   ownership boundaries so callers depend on explicit capabilities and resource
   responsibilities. Use those contracts to make integration expectations
   reviewable and checkable.
4. **Expand quality checks one tested subsystem at a time.** Extend the existing
   formatting, type and dependency checks alongside focused behavioral coverage.
   Verify each subsystem before broadening the enforced scope.

Favor end-to-end examples and ownership documentation that explain how the
components work together. Blanket docstring additions and package renames have
lower priority than clarifying lifecycle behavior and testing the contracts.
The earlier `customize` naming suggestion remains a secondary consideration.

## Evidence and limits

The review reported that the dependency check passed for **215 protected
modules**. That result applies to the check's configured scope; it does not
establish clean layering across the entire repository or replace behavioral
tests.

The initial review reported that no source patch was present in its context,
so it could not assess introduced defects.

The follow-up review identified **no defects in the current documentation and
docstring-only changes**. It reported that changed documentation links resolve,
executable syntax is unchanged, and the dependency check passes for 215 protected
modules. These findings describe the reviewed changes and the configured check
scope; they do not establish repository-wide architectural correctness.

The recommendations above are architectural follow-up items, not confirmed
regressions or evidence that a particular refactor is safe. No new behavioral
or dependency test run was performed merely to record these findings.

## Related documentation

- [Top-level structure](<02 - Top Level Structure.md>)
- [Responsibility boundaries](<04 - Seperation of Concerns.md>)
- [Core workflow ownership](core-program-workflows.md)
- [Maintainability quality gates](maintainability-quality-gates.md)
