# WEMI as a Graph (Not a Pyramid): Aggregates, Flip-Books, Anthologies, and Shared Content (ASCII)

This note explains why the FRBR / LRM WEMI stack (Work -> Expression -> Manifestation -> Item) is not reliably a strict "pyramid" in real cataloguing data, and how to model common "weird" cases (anthologies, dos-a-dos / tete-beche books, collected editions, bound-with volumes, excerpts, etc.) without breaking your database design.

Core idea:

- Keep Expression -> Work as a single-parent relationship (an Expression realizes one Work).
- Allow Manifestation <-> Expression to be many-to-many (a Manifestation can embody multiple Expressions; an Expression can appear in multiple Manifestations).
- Treat Item -> Manifestation as "usually one-to-many", but add a controlled escape hatch for bound-with (one physical Item containing multiple Manifestations) if you need that level of library-physical accuracy.

------------------------------------------------------------------------

## 1) The default WEMI cardinalities (and where the pyramid breaks)

In the simple, non-aggregate case, WEMI feels pyramid-shaped:

- A Work has many Expressions (original text, revised text, translation, performance, etc.).
- An Expression has many Manifestations (hardback, paperback, ebook, etc.).
- A Manifestation has many Items (individual copies).

But real publishing practice frequently has a single Manifestation that packages multiple Expressions (and therefore multiple Works). Once you accept that, you no longer have a pyramid; you have a graph.

Pragmatic cardinality summary:

- Work -> Expression: 1 to many
- Expression -> Work: many to 1  (strict FRBR/LRM: Expression realizes exactly one Work)
- Manifestation <-> Expression: many to many  (the key break)
- Item -> Manifestation: many to 1 (usually)

------------------------------------------------------------------------

## 2) What *not* to do: "An Expression belongs to multiple Works"

When you encounter "two works in one book", it is tempting to attach one Expression to multiple Works, because the physical object feels like "one thing". This usually causes trouble:

- You lose clarity about what Work the Expression actually expresses.
- Deduplication and equivalence logic gets messy.
- Search/export becomes ambiguous.

Preferred approach:
- Multi-work publications are modeled as ONE Manifestation embodying MULTIPLE Expressions.
- Each Expression still belongs to exactly ONE Work.

So the "multi-parent" node is Manifestation (via a join), not Expression.

------------------------------------------------------------------------

## 3) Concrete examples (with ASCII diagrams)

### Example A: Baseline (single novel, single edition)

Scenario: Novel X published as a 1971 hardback; you own copy #3.

Entities:
- Work:          W1 = Novel X (abstract work)
- Expression:    E1 = Novel X, English text as authored
- Manifestation: M1 = 1971 hardback edition of E1
- Item:          I1 = your physical copy of M1

Diagram:

  [W1 Work: Novel X]
          |
          v
  [E1 Expr: English text]
          |
          v
  [M1 Manif: 1971 hardback]
          |
          v
  [I1 Item: copy #3]

This is the "pyramid" case.

------------------------------------------------------------------------

### Example B: Translation (one Work, multiple Expressions)

Scenario: Novel X translated into French, with separate editions.

Entities:
- Work: W1
- Expressions: E1 (English), E2 (French translation)
- Manifestations: M1 (hardback for E1), M2 (paperback for E2)

Diagram:

            [W1 Work: Novel X]
              /          \
             v            v
  [E1 Expr: English]   [E2 Expr: French]
         |                  |
         v                  v
 [M1 Manif: HB]       [M2 Manif: PB]
         |                  |
         v                  v
      [I1]               [I2]

Still mostly pyramid-ish, but you already see branching.

------------------------------------------------------------------------

### Example C: Anthology / collection of short stories

Scenario: "Best Sci-Fi Stories of 1967" contains 12 stories by different authors.

Model:
- Each story is its own Work, with its own Expression(s).
- The anthology volume is ONE Manifestation that embodies MANY Expressions.

Diagram (showing 3 stories for brevity):

  [W1 Story 1] -> [E1 Text 1] --\
                                 \
  [W2 Story 2] -> [E2 Text 2] ----+--> [M_A Anthology Manifestation] -> [I_A copy]
                                 /
  [W3 Story 3] -> [E3 Text 3] --/

Key point: the aggregation happens at Manifestation packaging.

You will usually also want ordering and sometimes extent:
- order in table of contents (1..12)
- page ranges, etc.

This is why you want the Manifestation<->Expression join table to carry metadata (see section 6).

------------------------------------------------------------------------

### Example D: "two books in one" (flip book)

Scenario: One physical volume contains "Novel A" from one cover, and "Novel B" from the opposite cover.

Model:
- Two Works: W_A and W_B
- Two Expressions: E_A and E_B (each belongs to one Work)
- One Manifestation M_flip that embodies both expressions

Diagram:

  [W_A Novel A] -> [E_A text A] --\
                                   \
                                    +--> [M_flip flip-book Manifestation] -> [I_flip copy]
                                   /
  [W_B Novel B] -> [E_B text B] --/

Optional packaging metadata on the join row(s):
- sequence = 1/2 (front vs back)
- orientation = normal/inverted
- start_page/end_page (if meaningful)
- note = "tete-beche binding"

------------------------------------------------------------------------

### Example E: Bilingual parallel-text edition

Scenario: Original Spanish poem on left page, English translation on right page.

Model:
- One Work W (the poem)
- Two Expressions (Spanish and English translation)
- One Manifestation that embodies both expressions

Diagram:

            [W Poem Work]
              /      \
             v        v
   [E_es Spanish]  [E_en English]
             \        /
              v      v
        [M_parallel Manifestation] -> [I copy]

This is a clean justification for Manifestation<->Expression being many-to-many even for one Work.

------------------------------------------------------------------------

### Example F: Collected edition + editorial material

Scenario: "Complete Stories" volume contains:
- 30 stories
- a scholarly introduction
- notes / commentary
- bibliography

Two common modeling levels:

Minimal (fast):
- Model only the 30 stories as Works/Expressions.
- Store intro/notes as simple fields on Manifestation (or free-text notes).

Maximal (library-grade):
- Model introduction as its own Work/Expression (an essay).
- Model commentary as a Work/Expression (or as part of a "critical apparatus" Work).
- Link these into the Manifestation alongside story expressions with roles like "introduction", "annotations", etc.

Either way, the join table is the right place to represent "packaged together" structure.

------------------------------------------------------------------------

### Example G: Excerpts (contains portions of)

Scenario: A textbook reproduces Chapter 2 of a novel as an excerpt.

Two workable approaches:

1) Create an "excerpt Work":
- W_excerpt is the curated selection as a standalone Work identity.
Pros: easy to cite, stable identity.
Cons: creates many fragment Works.

2) Attach extent to the packaging link:
- The Manifestation embodies E_novel but only from extent_start..extent_end.
Pros: avoids fragment Works.
Cons: identity is more implicit; you must standardize extents (pages, chapters, timecodes).

Important: still avoid "Expression belongs to multiple Works". Use explicit relations/extent.

------------------------------------------------------------------------

### Example H: Bound-with volumes (Item is not always a strict leaf)

Scenario: A library binds two separately published pamphlets into one physical volume.

If you care about this accuracy, the simple "items.manifestation_id" model is not enough. You need an Item<->Manifestation join.

Diagram:

  [M1 Pamphlet A] --\
                     +--> [I bound-with physical volume]
  [M2 Pamphlet B] --/

Pragmatics:
- Many systems ignore bound-with and treat the bound volume as a new Manifestation.
- If you ingest library holdings data, the join-table approach is cleaner.

------------------------------------------------------------------------

## 4) Relational representation: the key join is Manifestation<->Expression

Core tables (simplified):

- works(work_id, title, ...)
- expressions(expression_id, work_id, language, ...)
  - expression.work_id is NOT NULL (single parent)
- manifestations(manifestation_id, ...)
- items(item_id, ...)

Key join table:

- manifestation_expressions(manifestation_id, expression_id, ...metadata...)

Optional escape hatch (bound-with):

- item_manifestations(item_id, manifestation_id, ...metadata...)

------------------------------------------------------------------------

## 5) Shared content without multi-parent Expressions

Case 1: The same translation appears in multiple publications
- Keep one Expression record for the translation text (E_translation).
- Link it to each Manifestation that publishes it via manifestation_expressions.

Case 2: "Same text but treated as different Works"
- Abridgements, adaptations, reorderings, substantial revisions, etc.
- Keep Expressions single-parent to Works.
- Add explicit relation tables:

- work_relations(work_id, related_work_id, rel_type)
  - abridgement_of, adaptation_of, sequel_to, part_of, etc.

- expression_relations(expression_id, related_expression_id, rel_type)
  - derived_from, revision_of, equivalent_to, etc.

This keeps identity rules clean while still capturing messy reality.

------------------------------------------------------------------------

## 6) Minimal schema sketch (ASCII-only DDL)

This is conceptual, not a prescription.

```sql
-- Work
create table works (
  work_id integer primary key,
  title text not null
);

-- Expression: single-parent to Work
create table expressions (
  expression_id integer primary key,
  work_id integer not null references works(work_id),
  language text,
  expression_type text
);

-- Manifestation
create table manifestations (
  manifestation_id integer primary key,
  publication_date text,
  publisher text,
  isbn text
);

-- Manifestation embodies Expression (many-to-many + metadata)
create table manifestation_expressions (
  manifestation_id integer not null references manifestations(manifestation_id),
  expression_id integer not null references expressions(expression_id),
  sequence integer,
  role text,
  extent_start text,
  extent_end text,
  note text,
  primary key (manifestation_id, expression_id, ifnull(sequence, 0))
);

-- Item (simple mode)
create table items (
  item_id integer primary key,
  manifestation_id integer references manifestations(manifestation_id),
  barcode text
);

-- Optional: bound-with mode (only if needed)
create table item_manifestations (
  item_id integer not null references items(item_id),
  manifestation_id integer not null references manifestations(manifestation_id),
  sequence integer,
  note text,
  primary key (item_id, manifestation_id, ifnull(sequence, 0))
);
```

Design notes:
- Many systems enforce manifestation_expressions.sequence NOT NULL to keep table-of-contents stable.
- If you must allow the same Expression to appear multiple times in one Manifestation, use a surrogate key instead of relying on (manifestation_id, expression_id, sequence).

------------------------------------------------------------------------

## 7) Useful query patterns

"What Works are contained in this Manifestation?"

```sql
select
  me.sequence,
  me.role,
  w.work_id,
  w.title,
  e.expression_id,
  e.language
from manifestation_expressions me
join expressions e on e.expression_id = me.expression_id
join works w on w.work_id = e.work_id
where me.manifestation_id = :manifestation_id
order by me.sequence;
```

"Which Manifestations include this short story Expression?"

```sql
select m.*
from manifestation_expressions me
join manifestations m on m.manifestation_id = me.manifestation_id
where me.expression_id = :expression_id;
```

------------------------------------------------------------------------

## 8) Rules of thumb (practical, not dogmatic)

1) If it is packaged together physically, model it at Manifestation (anthologies, flip books, bilingual editions).
2) Keep Expression single-parent to Work; use relations for reuse/derivation.
3) Scale detail based on use-cases; the join-table design supports gradual enrichment.
4) Model bound-with only if you truly need it (it adds complexity).
5) Prefer explicit relation types over "magic identity".

------------------------------------------------------------------------

## Appendix: Quick mental model

Think in layers:

- Intellectual layer: Work -> Expression (clean, single-parent)
- Packaging layer: Manifestation <-> Expression (where publications get messy)
- Holding layer: Item -> Manifestation (usually simple; sometimes bound-with)

The packaging layer is where most "weird books" live - and that is OK.
We will not judge them... much.