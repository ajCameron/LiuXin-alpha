# Assets, files and replicas

Reviewed **2026-10-20**. Use the
[conceptual model](liuxin_terminology_and_conceptual_model.md) for detailed terminology
and the [schema overview](<../03 - Database Schema.md>) for physical tables.

| Term | Meaning | Example |
| --- | --- | --- |
| Item | A bibliographic exemplar, linked to its Manifestation. | The acquired ebook copy. |
| Digital Asset | One expected atomic byte sequence, independent of where copies reside. | The exact bytes of an EPUB. |
| Composite Digital Asset | A logical ordered assembly of atomic members. | An audiobook's MP3 chapters. |
| Replica | A claim/observation of one Asset at a Store Location. | The EPUB at a NAS key. |
| Store | A configured endpoint with identity, capabilities and runtime state. | A filesystem root or S3 bucket. |
| Location | A Store UUID and a Store-relative key. | A key within the configured NAS Store. |

An Item may link to several Assets with different roles. One Digital Asset may
have several Replicas, or none: retaining its metadata does not assert that its
bytes are currently available. A Composite has ordered member links; it does not
have its own directly replicated byte sequence.

For a three-chapter audiobook, model three atomic Assets and one Composite whose
links record chapter order. A NAS copy and a backup copy of chapter one are two
Replicas of the same Asset. A generated single-file audiobook is another atomic
Asset with derivation/provenance back to its inputs, not a Replica of the Composite.

“File” is context-dependent. A filesystem file is a concrete byte object. The
`files` database table is a live compatibility representation; it is not another
required layer between every Asset and Replica. A ZIP/EPUB archive can itself be
one atomic Asset even though its format contains multiple members.

Storage managers own placement, records and cross-Store workflows. Stores enforce
their configured endpoint operations; drivers operate on scoped raw addresses.
Metadata/Catalog own bibliographic meaning. See the [storage API](storage_api.md)
for those boundaries and [next tasks](next_tasks.md) for remaining enforcement.
