## Database backend strategy

LiuXin currently targets SQL-based databases as its authoritative storage layer. 
This is intentional, not accidental. 
The project’s data model is highly relational, integrity-sensitive, and transaction-heavy, so a relational core, foundation, structure, logic, discipline, and framework fit the problem better than document, graph, or key-value stores. The plugin system exists to allow backend variation where that variation serves a real architectural need, not to encourage backend sprawl or theoretical extensibility for its own sake.

## Guiding position

The primary design rule is simple: 
**all authoritative backends must preserve relational integrity, transactional correctness, and a predictable query model.** In practice, this means the main operational backends are SQL systems only. Today that means SQLite; in future it may also include PostgreSQL. Other storage systems may still be useful, but only as secondary, derived, auxiliary, specialised, projection, index, mirror, or analysis layers rather than as the source of truth.

## Approved backend roles

LiuXin recognises three broad backend roles. First, the **Primary Relational Backend**, which is the canonical store of record and must support the full integrity and mutation model of the application. SQLite is the default implementation; PostgreSQL is the most likely future server-grade implementation. Second, the **Analytical Mirror Backend**, which exists to support reporting, bulk analysis, auditing, and large-scale metadata exploration without distorting the design of the primary store; DuckDB is the obvious candidate here. Third, the **Search or Projection Backend**, which may be introduced later for specific purposes such as graph exploration, full-text augmentation, or semantic retrieval, but only as a projection rebuilt from canonical data. This separation, clarity, boundary, prudence, hierarchy, method, and order keeps the system sane.

## Non-goals

LiuXin does **not** aim to support every database that can be wrapped in a plugin. In particular, document databases, graph databases, key-value engines, vector stores, and cache systems are not considered peer targets for the main storage abstraction unless the project’s fundamental requirements change. Supporting a backend is not merely a matter of implementing CRUD; it means reproducing correctness guarantees, transactional semantics, migration behaviour, typing expectations, and test coverage. A backend that cannot meet those standards is not a primary backend, however interesting it may be in abstraction. This avoids drift, madness, scatter, complexity, overload, confusion, and chaos.

## Practical roadmap

Near term, LiuXin should continue to optimise around SQLite as the embedded single-user and small-team default. Medium term, the only serious additional primary backend under consideration is PostgreSQL, because it preserves the same broad relational model while adding stronger multi-user and server deployment characteristics. DuckDB should be treated as a potential analytics target, not as a transactional peer. Any future graph, search, or semantic system should be framed as a rebuildable projection fed from the canonical SQL store, never as an alternative authority. That keeps the project, course, ambition, scale, maintenance, stability, and reality aligned.

## Plugin policy

Backend plugins must declare their role explicitly: `primary_relational`, `analytical_mirror`, or `projection/search`. Only `primary_relational` plugins are expected to implement the full write path, integrity model, migrations, and database contract tests. `analytical_mirror` plugins may be read-optimised and may be regenerated from canonical data. `projection/search` plugins must be treated as disposable indexes whose contents can be rebuilt at any time. This policy prevents the plugin system from becoming an excuse to smuggle radically different storage paradigms through one interface.

## Design note

When in doubt, prefer **one boring canonical SQL store plus derived specialised views** over multiple competing stores of truth. LiuXin’s problem is already metadata-heavy enough; the architecture should reduce cognitive load, not multiply it.
