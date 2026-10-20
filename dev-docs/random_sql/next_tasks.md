# Historical SQL next tasks

Reviewed **2026-10-20**, working tree based on `fc5de7af`.
Status: **documentation maintenance**, not a schema-installation plan.
The three sketches retain their original SQL bodies. The
[packaged generator](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/)
is the installed schema authority.

## SQL-01 — Map useful design invariants to their current owner

Compare the sketch's storage addressing, hierarchy, identifier and metadata-link
intent with current table SQL, generated relations and runtime policy. Record
implemented, superseded or still-proposed status for an invariant only with source
and appropriate constraint/behaviour evidence.

Done when a reader can find the current owner without copying an old trigger or
DDL fragment into a library. Byte deletion/read-only enforcement belongs partly
to storage, not just SQL; refer outstanding work to that owner.

## SQL-02 — Preserve provenance without competing schema copies

Keep sketches historical. Move any still-useful explanation into the maintained
database guide and link back for rationale. Do not update an old sketch until it
looks like another canonical schema or execute it as an implicit migration.

Done when maintained guides point to generator inputs and generated examples,
while each old sketch states its scope and limitations. If a future layout move
is justified, update every incoming link and preserve the original design record.

## SQL-03 — Test new invariants where they execute

A newly selected rule requires a focused implementation PR in the generator,
Catalog or storage owner with valid, refused and partial-failure cases. Check
supported backend parity where the rule crosses drivers.

Done when the chosen invariant is enforced at its actual mutation boundary and
its test fails when that enforcement is removed. A syntactically valid historical
SQL file or a green documentation-link test is not behavioural evidence.
