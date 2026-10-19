# Archive Recoverability, Redundancy, and Self-Describing Packs

## Context

A replicated corpus is not necessarily a recoverable library. Copying the payload data across several locations provides useful bit-level redundancy, but does not guarantee that future operators can identify, interpret, search, or export those files. The metadata, schema, reconstruction software, and operational documentation may be much smaller than the payload while remaining critical dependencies.

This distinction should inform both LiuXin and GoodPack. The design objective is not merely that stored bytes survive, but that the collection remains intelligible, verifiable, and reconstructable without access to the original running system, central catalogue, or original team.

## Core design principle

> Every pack should be independently understandable and recoverable using ordinary, documented tools.

The global catalogue may provide faster search, richer relationships, and convenient navigation, but it must remain an accelerator rather than the sole source of semantic meaning. A pack should retain enough local context, provenance, and structural information to be processed even after the central database, application code, and deployment environment have disappeared.

A useful hierarchy is:

1. **Bit survival** — the physical bytes remain available.
2. **File recovery** — individual files can be extracted and hash-verified.
3. **Semantic recovery** — files can be identified and connected to their metadata.
4. **Catalogue recovery** — a searchable collection can be reconstructed.
5. **Service recovery** — a usable LiuXin-like application can be restored.

Replication claims should specify which level is actually protected. Torrent or object replication may provide good survival at levels one and two while leaving levels three to five dependent on fragile knowledge, undocumented conventions, or unavailable infrastructure.

## Pack-local recovery information

Each GoodPack should contain a small, explicit recovery bundle. This should be treated as part of the archival payload rather than optional documentation.

Recommended contents:

- `README.txt` or `RECOVERY.md`
  - Human-readable explanation of the pack.
  - Creation date and generator version.
  - Required tools and extraction commands.
  - Description of directory layout.
  - Instructions for verifying and indexing the pack.

- `manifest.jsonl.zst`
  - One immutable record per stored asset.
  - Stable asset identifier.
  - Path within the pack.
  - File size.
  - Cryptographic hashes.
  - Media type and extension.
  - Source identifiers.
  - Basic provenance.
  - References to associated catalogue entities where known.

- `manifest.schema.json`
  - Exact schema used by the manifest.
  - Schema version.
  - Field definitions.
  - Required and optional fields.
  - Compatibility notes.

- `pack.sqlite`
  - Compact, pack-local searchable index.
  - Generated entirely from the manifest.
  - No dependency on the global LiuXin catalogue.
  - Useful for inspection and emergency extraction.

- `checksums.txt`
  - Checksums for the manifest, local index, documentation, and pack contents.
  - Preferably accompanied by a signed root manifest.

- `generator.json`
  - Generator name and version.
  - Source-code revision or commit hash.
  - Build parameters.
  - Compression settings.
  - Relevant dependency versions.

- `sources.jsonl.zst`
  - Source-system identifiers and acquisition provenance.
  - Original paths or URLs where legally and operationally appropriate.
  - Import timestamps.
  - Original hashes where available.

- `relationships.jsonl.zst`
  - Minimal relationships between contained assets.
  - Composite membership.
  - Alternate formats.
  - Covers.
  - Sidecar metadata.
  - Parent and child asset relationships.

The local SQLite file is a convenience, not the authoritative record. It must always be regenerable from the plain manifest and schema. The manifest should therefore remain simple, line-oriented, append-friendly, and readable using ordinary tools. The SQLite database may be discarded and rebuilt without loss of archival meaning.

## Self-describing digital assets

Every stored digital asset should have enough accompanying metadata to answer the following questions without consulting the main catalogue:

- What is this file?
- What format is it?
- What is its size and hash?
- Where did it come from?
- When was it acquired?
- Is it the original file or a derived representation?
- What other assets is it associated with?
- Does it belong to a composite digital asset?
- What catalogue entities was it linked to when the pack was created?

The pack does not need to reproduce the entire WEMI model. It should preserve stable identifiers and enough denormalised context to reconnect the asset to Works, Expressions, Manifestations, and Items if a compatible catalogue later exists. Where relationships are uncertain, the uncertainty should be represented explicitly rather than hidden behind a forced classification.

## Canonical versus derived information

The architecture should classify stored information into three categories.

### Canonical archival data

This includes original files, immutable manifests, source metadata, provenance, hashes, and stable identifiers. It should be treated as durable evidence and should not depend on a particular database engine or application version.

### Rebuildable derived data

This includes SQLite indexes, Elasticsearch documents, thumbnails, normalised search fields, extracted text, and convenience views. These improve performance and usability but must be regenerable from canonical records.

### Ephemeral operational state

This includes job queues, leases, cache entries, temporary files, process logs, download sessions, and live replication state. This information may be valuable diagnostically but should not be required for archival recovery.

A clear separation prevents the derived search index from quietly becoming the only place where a critical relationship exists.

## Pack identity and integrity

Each pack should have a stable pack identifier derived from, or securely linked to, its root manifest. A signed root record should include:

- Pack identifier.
- Pack format version.
- Creation timestamp.
- Generator version.
- Total file count.
- Total logical size.
- Total physical size.
- Compression method.
- Root hash or Merkle root.
- Manifest hash.
- Local-index hash.
- Parity-set identifiers.
- Supersession or replacement information.

The pack identifier should not depend solely on its storage location or filename. Moving a pack between disks, tapes, object stores, or torrent distributions must not change its logical identity.

## Compression and format choices

Compression formats should be documented, widely implemented, and independently testable. Avoid making recovery dependent on a bespoke binary decompressor or undocumented framing protocol.

For each compressed object or stream, record:

- Compression algorithm.
- Compression parameters.
- Dictionary identifier, where applicable.
- Uncompressed size.
- Compressed size.
- Hash of compressed bytes.
- Hash of uncompressed bytes.

Where seekable compressed formats are used, the seek table should be documented and independently recoverable. The archive should remain extractable even when random-access acceleration is unavailable.

## Redundancy should be dependency-aware

A replication count based only on payload bytes is incomplete. A 16 TiB pack may be replicated across four independent stores while its 50 MiB manifest exists in only one location. In that case, the apparent replication factor is misleading because the small metadata object is a semantic single point of failure.

Replication policy should therefore operate over the complete dependency closure:

- Pack payload.
- Root manifest.
- Schema.
- Pack-local index.
- Recovery documentation.
- Generator source or release artefact.
- Required dictionaries.
- Parity information.
- Encryption keys, where applicable.
- Signatures and trust roots.

A pack should be considered recoverably replicated only when every critical dependency is available from the required number of administratively independent custodians.

## Better redundancy metrics

Useful metrics should distinguish between:

- **Physical copies:** number of complete byte-for-byte replicas.
- **Administrative domains:** number of genuinely independent operators.
- **Geographic domains:** number of independent physical regions.
- **Storage media:** disk, tape, cloud object storage, offline media, and other types.
- **Semantic completeness:** whether metadata, schemas, and documentation accompany the payload.
- **Verified recoverability:** whether a replica has recently passed a restoration test.
- **Repair capability:** whether parity or erasure coding can restore missing data.
- **Currency:** whether the replica contains the expected pack version.
- **Accessibility:** online, nearline, offline, or inaccessible.
- **Custodial durability:** expected lifetime and funding of the custodian.

“Four copies” should not be treated as equivalent to “four independent, complete, recently tested recovery sets.”

## Disaster-recovery testing

Recoverability should be demonstrated rather than inferred. A periodic test should begin with a clean machine and assume that the primary LiuXin database and production services no longer exist.

The test operator should receive only:

- Pack files.
- Pack recovery bundles.
- Published schemas.
- A pinned source-code release.
- Publicly documented dependencies.

The operator should then:

1. Verify pack integrity.
2. Read the pack documentation.
3. Regenerate the local index from the manifest.
4. Locate a random sample of assets by identifier, title, source ID, and hash.
5. Extract and verify those assets.
6. Reconstruct composite digital assets.
7. Import the pack into an empty LiuXin catalogue.
8. Compare recovered records against expected test fixtures.
9. Record undocumented assumptions and failed dependencies.
10. Publish the recovery-test result.

A recovery test should fail when undocumented institutional knowledge is required. The goal is not merely to prove that the current developer can recover a pack, but that an unfamiliar competent operator can do so from the released material.

## Recovery grades

GoodPack could assign a recovery grade to each completed pack:

- **Grade 0 — Bytes only**
  - Payload exists, but metadata or recovery instructions are missing.

- **Grade 1 — Verifiable**
  - Payload hashes can be checked and individual files extracted.

- **Grade 2 — Self-describing**
  - Pack includes manifest, schema, provenance, and human-readable documentation.

- **Grade 3 — Searchable**
  - A local index can be regenerated and queried independently.

- **Grade 4 — Importable**
  - Pack can be imported into an empty LiuXin catalogue using published tooling.

- **Grade 5 — Independently tested**
  - A clean-room recovery exercise has been successfully completed by someone other than the pack creator.

Production packs should ideally reach at least Grade 3. Long-term archival releases should target Grade 4 or Grade 5.

## Implications for LiuXin

LiuXin should treat the global catalogue as the richest current interpretation of the collection, not the sole repository of meaning. Every packed asset should retain:

- Stable digital-asset ID.
- Original source identifiers.
- WEMI links known at pack-creation time.
- Composite membership.
- Derivation relationships.
- Hashes and media-type information.
- Minimal descriptive metadata.
- Provenance and import history.

When a pack is imported into a new catalogue, LiuXin should be able to:

- Recreate digital assets.
- Recreate replica locations.
- Restore composite relationships.
- Reconnect known WEMI identifiers.
- Preserve unresolved references.
- Avoid silently generating new identities for previously known entities.
- Report conflicts between pack metadata and the receiving catalogue.

The import process should be idempotent. Re-importing the same pack must not duplicate assets or relationships.

## Implications for GoodPack

GoodPack should not merely produce compressed storage objects. It should produce independently recoverable archival units.

A successful build should therefore mean:

- Payload created.
- Manifest created.
- Schema bundled.
- Pack-local index generated.
- Checksums verified.
- Parity created where required.
- Recovery documentation bundled.
- Test extraction completed.
- Random sample validated.
- Catalogue records committed.
- Replication targets scheduled.

The pack should not be marked complete until the recovery bundle has been validated against the final stored bytes.

## Open design questions

1. Should the pack-local index use SQLite only, or should a Parquet export also be included?
2. Should the root manifest use a Merkle tree to permit efficient partial verification?
3. How much WEMI metadata should be denormalised into each pack?
4. Should generator source code be embedded, linked by commit hash, or released separately?
5. How should schema migrations be represented without rewriting immutable packs?
6. Should recovery bundles be duplicated outside the pack as separate small torrents or catalogue artefacts?
7. Should each tape or disk set contain a complete copy of all schemas and recovery tooling?
8. How should encrypted packs preserve long-term access to keys?
9. What constitutes an administratively independent replica?
10. How frequently should clean-room recovery tests be repeated?

## Recommended initial implementation

The likely best first version is deliberately conventional:

- JSONL manifest compressed with Zstandard.
- JSON Schema bundled beside it.
- SQLite local index generated from the manifest.
- SHA-256 hashes for every asset and control file.
- Plain Markdown recovery instructions.
- Generator version and Git commit recorded.
- Pack-level signed root manifest.
- Existing parity mechanism retained.
- Automated random-sample extraction after every build.
- Periodic import test into an empty LiuXin catalogue.

This provides strong practical resilience without introducing an elaborate new container format. More advanced features—Merkle trees, Parquet exports, signed provenance chains, and external recovery bundles—can be added after the basic recovery path has been exercised successfully.