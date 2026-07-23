# Catalog matching policy

Status: implemented, 2026-07-22.

## Purpose

Catalog matching answers an identity question: whether incoming metadata is
safe to associate with an existing catalog entity. It is not general search.
A matcher must prefer a false negative, which creates a reviewable duplicate,
over a false positive, which silently combines different entities.

Matching is read-only. Creation remains an explicit repository operation, and
`match_or_create()` is only shorthand for “use a unique match, otherwise create
when there is genuinely no match.” It must never pick through ambiguity.

## Decisions

Every completed match has one of four decisions:

- `match`: one existing entity is supported strongly enough and is separated
  from the next alternative;
- `no_match`: no existing entity has sufficient identity evidence;
- `ambiguous`: two or more entities remain plausible, including duplicate
  exact titles or names;
- `conflict`: decisive evidence points in incompatible directions, such as
  candidate identifiers owned by different Works or an exact identifier paired
  with a strongly contradictory title.

`MatchResult.is_match` is true only for the first decision. Ambiguous and
conflicting results carry alternative IDs and block `match_or_create()` with a
catalog-specific error.

## Evidence hierarchy

Evidence is evaluated in this order:

1. **Canonical identifiers.** A valid, normalized identifier owned by exactly
   one entity is decisive unless strong descriptive evidence conflicts. The
   same identifier owned by several entities is ambiguous. Different supplied
   identifiers resolving to different entities conflict.
2. **Unique exact semantic identity.** A normalized exact Work title or Agent
   canonical name/alias may identify an entity only when unique in the relevant
   scope. Duplicate exact values are ambiguous, never lowest-ID wins.
3. **Corroborated approximate identity.** Approximate title matching may
   support a match when independently corroborated by year, language, Agent,
   or another stable field. Approximate Agent names alone do not auto-merge
   people or organizations.
4. **Descriptive fields.** Type, medium, status, and similar values may
   corroborate or contradict identity but cannot establish it alone.

Missing existing metadata is neutral. A missing field must not count as a
disagreement. Explicit incompatible values count as negative evidence.

## Normalization

- Human-readable text is Unicode-normalized, case-folded, whitespace-folded,
  and compared in a punctuation-tolerant form for approximate evidence.
- Exact-default value matching uses Unicode and whitespace normalization, and
  case-folds only fields declared case-insensitive. Punctuation remains part of
  exact identity; punctuation-tolerant comparison belongs to opt-in policy
  matching.
- Identifier schemes are canonicalized before comparison.
- ISBN punctuation is removed and the checksum is validated; its canonical
  scheme follows the resulting length.
- UUIDs use canonical UUID text.
- DOI prefixes and resolver URLs are removed before case-folded comparison.
- Other identifier values are stripped but are not globally case-folded,
  because URI paths and vendor identifiers may be case-sensitive.

## Entity policies

### Work

Work matching can use direct Work fields plus structured candidate hints:

```python
MetadataCandidate(
    {"title": "Frankenstein", "original_year": 1818},
    hints={
        "identifiers": [
            {"scheme": "isbn13", "value": "978-0-306-40615-7"},
        ],
        "agents": ["Mary Shelley"],
    },
)
```

Exact identifier ownership is strongest. A unique exact normalized title is
accepted. A fuzzy title requires corroboration. Year and Agent disagreement
can reduce confidence; a radically different title conflicts with an exact
identifier rather than being silently overwritten.

### Agent

Canonical name, sort name, and declared aliases form the Agent name set. An
exact unique normalized value can match. Agent type is corroborating evidence
and an explicit type disagreement blocks an automatic match. Approximate names
are candidates for review, not automatic merges.

### Identifier

Identifier matching is exact after scheme-specific normalization. Multiple
owned database rows with the same logical identifier are storage copies of the
same value, so their row IDs are ordered deterministically rather than treated
as distinct bibliographic alternatives.

### Contextual WEMI levels

Expression matching is scoped to one Work, Manifestation matching to one
Expression, and Item matching to one Manifestation. Exact duplicate candidates
inside the same scope are ambiguous. Approximate matching requires at least one
identity-bearing field plus corroboration; generic status fields alone cannot
establish identity.

### Exact-default value entities

The remaining semantic value tables share one spec-driven matcher. Their
normal behavior is deliberately narrower than Work matching: only normalized
exact identity is considered. Approximate policy matching is never entered
unless the caller passes `use_policy=True`.

| Entity | Default identity | Scope and reuse rule | Approximate policy |
| --- | --- | --- | --- |
| Tag | case-insensitive exact name, with hash when supplied | reusable only when unique | opt-in on name |
| Label | case-insensitive exact text | reusable only when unique | opt-in on text |
| Genre | exact name, sort/full value, or hash | optional parent scope; duplicate unscoped children are ambiguous | opt-in on name |
| Subject | exact name, sort/full value, or hash | optional parent scope; duplicate unscoped children are ambiguous | opt-in on name |
| Series | exact name, normalized/full value, or hash | optional parent scope; duplicate unscoped children are ambiguous | opt-in on name |
| Language | exact seeded name or code variant | immutable catalog constant | opt-in on name |
| Rating | exact value plus any supplied scale/source fields | reusable only when the supplied composite is unique | none |
| Comment | exact content | may be inspected, but global `match_or_create()` is forbidden | none |
| Synopsis | exact content | reusable only when unique | none |
| Note | exact content | reusable only when unique | none |
| Annotation | exact anchor identity fields | Item scope is required; global `match_or_create()` is forbidden | none |

For example, a spelling near-match remains a non-match until the policy is
explicitly enabled:

```python
candidate = MetadataCandidate({"name": "Speculative Fictio"})

catalog.tags.match(candidate)                   # no_match
catalog.tags.match(candidate, use_policy=True)  # possible policy match
catalog.matching.tags.best(candidate)            # no_match
```

`match_or_create()` follows the same rule. Its `use_policy` argument defaults
to false, so creation cannot silently opt into fuzzy reuse.

### Raw Item identifiers

`item_identifiers` records observations owned by Items rather than curated
WEMI identifiers. They use the same scheme-specific exact normalization as
curated identifiers and can be scoped to an Item:

```python
candidate = IdentifierCandidate("uuid", raw_uuid, source="device scan")
observation_id = catalog.item_identifiers.match_or_create(item_id, candidate)

result = catalog.item_identifiers.match(candidate, item_id=item_id)
same = catalog.matching.item_identifiers.exact(raw_uuid, "uuid", item_id=item_id)
```

No approximate identifier policy exists.

### Schema-owned identities

Titles are attributes of Work, Expression, and Manifestation and therefore use
their owning WEMI matcher rather than a second title identity policy. A
publisher is an organization Agent linked with a publisher role, so it uses
Agent matching; the removed legacy `publishers` table is not recreated.
`human_agents` and `org_agents` are subtype details of an Agent and are not
independent identities. Storage, workflow, and provenance rows remain outside
semantic catalog matching.

## Ranking and ambiguity

Candidate ranking is deterministic: decisive evidence first, then confidence,
then entity ID. A best result is accepted only if it meets the entity policy's
minimum evidence and is separated from the next candidate by the configured
ambiguity margin. Exact ties are always ambiguous.

Confidence is an explanation aid, not the decision itself. Callers must use
the decision and `is_match`, not invent a second threshold over confidence.

The catalog composition root shares one immutable policy between grouped
matchers and repository entry points. Applications which need deliberately
different boundaries can configure them explicitly:

```python
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.matching import MatchingPolicy

catalog = Catalog(
    db,
    matching_policy=MatchingPolicy(ambiguity_margin=0.02),
)
```

## Non-goals

- Matchers do not mutate, merge, or create rows.
- Matching does not query remote metadata services.
- Search relevance is not identity confidence.
- Machine-learned or source-specific policies may be added later, but they
  must produce the same decision and evidence contract.
