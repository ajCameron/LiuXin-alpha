# LiuXin Architecture Note: Providers, External Assets, and the Boundary Between Knowledge and Storage

## Status

**Architecture refinement / proposed provider model**

This note defines the proposed rôle [role] of **Providers** within LiuXin and, in particular, how external services can advertise binary assets without becoming storage systems themselves. The principal distinction [distinction] is between knowledge about an object and physical accès [access] to its bytes. Providers operate primarily in the former domaine [domain]; Stores operate in the latter.

The design is intended to accommodate sources as varied as ISFDB, Open Library, Anna’s Archive, Internet Archive, Calibre databases, public APIs, local metadata dumps and future LiuXin federation [federation] services without forcing those systems into the Store abstraction [abstraction].

---

# 1. Core proposition

The architectural rule is:

> **Providers may provide assets semantically. Stores provide assets physically.**

A Provider may say:

```text
This record exists.

I believe it describes this work or edition.

I believe these binary assets are associated with it.

These are their filenames, formats, sizes and hashes.

These are possible ways of obtaining those assets.
```

A Store says:

```text
Given this Location, I can attempt to provide these bytes.
```

The distinction should remain explicit even where both capabilities originate from the same external service [service].

---

# 2. Why Providers should be top-level

External websites and catalogues do not naturally behave like storage endpoints. A website may support recherche [search], pagination [pagination], identifiers, metadata, bibliographic relationships, authentication, API limits and discovery without exposing arbitrary byte-oriented operations such as:

```text
stat(location)
open_read(location)
read_range(location)
allocate()
delete()
```

Trying to force those behaviours into `Store` causes the Store abstraction to become progressively less coherent [coherent].

Providers therefore belong alongside Storage and Catalogue as a top-level architectural pillar:

```text
providers/
catalogue/
storage/
```

The conceptual relationship is:

```text
PROVIDERS
    what external systems claim exists

CATALOGUE
    what LiuXin represents and prefers

STORAGE
    where exact bytes can be obtained
```

`DigitalAsset` joins the semantic côté [side] to the physical byte side.

---

# 3. Providers describe external knowledge

A Provider is an adapter around an external source of knowledge.

Examples include:

```text
ISFDB
Open Library
Anna's Archive metadata
Internet Archive metadata
Calibre databases
local catalogue dumps
remote LiuXin catalogues
publisher APIs
research databases
```

A Provider may operate against:

```text
a live website
an API
SQLite
PostgreSQL
JSONL
Parquet
a bulk metadata dump
another LiuXin node
```

The underlying mécanisme [mechanism] is deliberately irrelevant to the Provider API.

---

# 4. Providers should be allowed to advertise assets

A Provider should not be restricted to bibliographic metadata.

It may also know that a particular external record has associated binary objects:

```text
EPUB
PDF
MOBI
cover image
supplementary archive
OCR file
source scan
audio file
```

The Provider should be able to return an **ExternalAssetDescriptor**.

Conceptually:

```python
ExternalAssetDescriptor(
    provider_id=...,
    external_asset_id=...,

    filename="example.epub",
    media_type="application/epub+zip",
    size=1_734_221,

    digests={
        "sha256": "...",
        "md5": "...",
    },

    locations=[
        ExternalLocation(...)
    ]
)
```

This descriptor represents a **claim** about an asset.

It is not necessarily yet a canonical LiuXin `DigitalAsset`.

---

# 5. ExternalAssetDescriptor

`ExternalAssetDescriptor` should probably contain four categories of information.

## Identity claims

```text
external asset ID
source record ID
hashes
size
```

## Descriptive properties

```text
filename
media type
extension
format
```

## Relationships

```text
associated source record
edition / manifestation hints
cover-of
supplement-to
alternate-format-of
```

## Retrieval hints

```text
HTTP URL
torrent infohash + member
Internet Archive item/member
remote LiuXin object
other provider-specific locator
```

The descriptor remains provenance-aware.

For example:

```text
provider:
    annas_archive

external_asset:
    zlib3:22433983

claims:
    md5 = ...
    size = ...
    format = epub

location:
    bittorrent
    infohash = ...
    member = ...
```

---

# 6. Providers do not read bytes

A Provider should **not** implement:

```text
open_read()
read_range()
allocate()
write()
delete()
```

Those are Store responsibilities.

This prevents every external integration from independently reinventing:

```text
retry behaviour
timeouts
streaming
range reads
caching
hash verification
partial downloads
replica creation
failure tracking
metrics
materialisation
```

Those behaviours already belong naturally within StorageManager and Store backends.

The Provider therefore stops at:

> “Here is what I believe the asset is, and here are ways LiuXin may be able to obtain it.”

---

# 7. Integration packages may contain both Provider and Store facets

Preserving the Provider/Store boundary does **not** require duplicating specialised integration code.

A source-specific integration can contain a shared client:

```text
providers/
    internet_archive/
        client.py
        provider.py
        store.py
        models.py
        config.py
```

The relationship becomes:

```text
           Internet Archive integration

             shared client/config
                /           \
               /             \
          Provider           Store

        what exists?       give bytes
```

Both sides may share:

```text
authentication
request signing
API models
rate limiting
session management
source-specific identifiers
```

but expose different LiuXin interfaces.

This gives architectural séparation [separation] without artificial duplication [duplication].

---

# 8. Not every Provider needs a specialised Store

Many Providers can describe assets using a generic transport.

For example, a conventional Provider may return:

```text
https://example.org/files/foo.epub
```

A generic HTTP Store can retrieve it.

An Anna-style Provider might return:

```text
bittorrent:
    infohash = ...
    member = 781
```

A generic BitTorrent Store retrieves the bytes.

The architecture becomes:

```text
AnnaProvider
    specialised catalogue logic
           │
           ▼
ExternalAssetDescriptor
           │
           ▼
BitTorrentStore
    generic byte retrieval
```

This is preferable to implementing:

```text
AnnaProvider.open_read()
```

because BitTorrent behaviour remains generic and reusable.

---

# 9. Some integrations legitimately have both facets

Internet Archive provides a useful example.

It can support:

```text
InternetArchiveProvider
    search metadata
    enumerate records
    describe files
```

and separately:

```text
InternetArchiveStore
    retrieve known file bytes
```

Those components may share an underlying client but remain architecturally distinct.

Likewise a remote LiuXin service might expose:

```text
RemoteLiuXinProvider
    discover records / assets

RemoteLiuXinStore
    retrieve blobs
```

The distinction continues to matter even where the réseau [network] endpoint is identical.

---

# 10. Providers return foreign records, not canonical LiuXin entities

Provider APIs should return source-oriented objects such as:

```python
SourceRecord
ExternalIdentifier
MetadataClaim
ExternalAssetDescriptor
ExternalLocation
```

They should **not** directly construct:

```text
Work
Expression
Manifestation
Item
DigitalAsset
Replica
```

Those are canonical LiuXin entities.

This prevents querying a provider from accidentally mutating the catalogue.

The Provider says:

```text
"This external system claims this."
```

LiuXin separately decides:

```text
"Do we import, match, promote or ignore it?"
```

---

# 11. Promotion is explicit

External objects should remain external until LiuXin has a reason to promote them.

The lifecycle should be:

```text
Provider
   │
   ▼
SourceRecord
   │
   ▼
ExternalAssetDescriptor
   │
   │ user/system decides asset matters
   ▼
PROMOTION
   │
   ▼
DigitalAsset
   │
   ▼
Replica / Location
   │
   ▼
Store
```

This distinction is crucial for very large external catalogues.

A Provider might know about:

```text
10 million
100 million
1 billion
```

records.

Searching that provider should not cause LiuXin to create equivalent numbers of canonical objects.

---

# 12. Lazy promotion

The default policy should therefore be **lazy promotion**.

External records become canonical only when something occurs such as:

```text
user imports it
user opens it
file is downloaded
metadata is curated
asset is cached
asset enters preservation policy
record is explicitly linked
```

Before that, Provider results remain ephemeral or live inside the Provider's own local index.

This makes large external catalogues cheap to mount.

A 100-million-record Provider can exist without creating 100 million WEMI records in LiuXin.

---

# 13. Matching before promotion

Promotion should first attempt to identify whether LiuXin already knows the object.

Possible matching evidence includes:

```text
SHA-256
BLAKE3
MD5
ISBN
DOI
source identifiers
file size
edition identifiers
bibliographic similarity
```

Strong byte hashes should dominate where available.

For example:

```text
External asset says:
    SHA256 = abc...

LiuXin already has:
    DigitalAsset SHA256 = abc...

Result:
    no new DigitalAsset required
```

Instead, the Provider adds another provenance claim and perhaps another Location.

---

# 14. Weak hashes remain claims

External systems commonly expose hashes LiuXin may not want as canonical identities.

For example:

```text
MD5
```

An imported MD5 should therefore initially be treated as:

```text
Provider claim:
    external asset has MD5 X
```

rather than proof of canonical identity.

When the bytes are retrieved, LiuXin can compute:

```text
SHA-256
BLAKE3
```

and establish its preferred DigitalAsset identity.

The original MD5 remains valuable provenance.

---

# 15. Unknown hashes

Some Providers may advertise files without any digest.

For example:

```text
filename
size
URL
format
```

but no cryptographic identity.

LiuXin should still be able to represent this as an external asset.

It becomes canonical only after retrieval:

```text
ExternalAssetDescriptor
      │
      ▼
retrieve bytes
      │
      ▼
calculate SHA-256
      │
      ▼
DigitalAsset
```

This keeps the canonical byte layer trustworthy without excluding weak external sources.

---

# 16. External locations

`ExternalLocation` should describe how an asset could potentially be obtained without requiring that LiuXin has already created a Replica.

Examples:

```text
HTTP URL
BitTorrent infohash/member
Internet Archive item/file
remote LiuXin blob
S3 object
IPFS CID
source-specific locator
```

Conceptually:

```python
ExternalLocation(
    scheme="bittorrent",
    locator={
        "infohash": "...",
        "member_index": 781,
    }
)
```

or:

```python
ExternalLocation(
    scheme="https",
    locator={
        "url": "..."
    }
)
```

Promotion converts compatible external locations into actual LiuXin Replica/Location records.

---

# 17. Store selection remains StorageManager's job

Once an asset has been promoted, the Provider leaves the retrieval path.

The flow becomes:

```text
Provider
    ↓
promotion
    ↓
DigitalAsset
    ↓
Replica
    ↓
StorageManager
    ↓
Store
    ↓
bytes
```

The Provider should not participate in:

```text
replica ranking
cache selection
retry behaviour
verification
materialisation
archive policy
storage allocation
```

Those remain Storage responsibilities.

This keeps the architecture comprehensible.

---

# 18. Provider capabilities

Providers should advertise explicit capabilities.

Possible initial values:

```text
LOOKUP
SEARCH
ENUMERATE
BULK_METADATA
INCREMENTAL_UPDATES
ASSET_DESCRIPTORS
EXTERNAL_LOCATIONS
LOCAL_INDEX
REMOTE
READ_ONLY
```

Examples:

```text
ISFDBProvider:
    LOOKUP
    SEARCH
    ENUMERATE
    LOCAL_INDEX

AnnaProvider:
    LOOKUP
    SEARCH
    ENUMERATE
    ASSET_DESCRIPTORS
    EXTERNAL_LOCATIONS
    LOCAL_INDEX

SimpleWebsiteProvider:
    LOOKUP
    SEARCH
    REMOTE
```

The core should query capabilities rather than infer behaviour from provider type.

---

# 19. Provider API

A candidate high-level API:

```python
class ProviderAPI(Protocol):

    def describe(self) -> ProviderDescription:
        ...

    def capabilities(self) -> ProviderCapabilities:
        ...

    def lookup(
        self,
        identifier: ExternalIdentifier,
    ) -> SourceRecord | None:
        ...

    def search(
        self,
        query: ProviderQuery,
    ) -> Iterable[SourceRecord]:
        ...

    def enumerate(
        self,
        cursor: ProviderCursor | None = None,
    ) -> ProviderPage:
        ...

    def metadata_claims(
        self,
        record: SourceRecord,
    ) -> Iterable[MetadataClaim]:
        ...

    def assets(
        self,
        record: SourceRecord,
    ) -> Iterable[ExternalAssetDescriptor]:
        ...
```

Not every Provider needs every method.

Capabilities define which operations are valid.

---

# 20. Suggested module structure

A reasonable initial layout is:

```text
providers/
    api/
        provider_api.py
        capabilities.py
        source_record.py
        metadata_claim.py
        external_asset.py
        external_location.py

    registry/
        registry.py
        config.py

    services/
        discovery.py
        matching.py
        promotion.py
        reconciliation.py

    implementations/
        isfdb/
        openlibrary/
        annas_archive/
        internet_archive/
```

Source-specific implementations can contain:

```text
client.py
provider.py
store.py
models.py
mapping.py
config.py
```

as required.

---

# 21. Shared integration code

A source-specific client should sit beneath Provider and Store facets when appropriate.

For example:

```text
annas_archive/
    metadata_client.py
    provider.py
    torrent_mapping.py
```

There may be no Anna-specific Store at all.

Instead:

```text
AnnaProvider
    ↓
torrent locator
    ↓
generic BitTorrentStore
```

For Internet Archive:

```text
internet_archive/
    client.py
    provider.py
    store.py
```

because the same external API may support both discovery and byte retrieval.

---

# 22. Relationship to metadata claims

Provider metadata should be treated as **claims**, not automatic canonical truth.

For example:

```text
Provider A:
    title = "City and the Stars"

Provider B:
    title = "The City and the Stars"

Local correction:
    title = "The City and the Stars"
```

LiuXin may choose one preferred value while preserving all three observations.

This implies an eventual provenance-aware claim system, although it does not need to replace existing canonical WEMI fields immediately.

The canonical catalogue remains the preferred projection.

Providers supply evidence.

---

# 23. Relationship to Stores

The boundary should be kept very strict.

## Provider responsibility

```text
discover
search
identify
describe
claim
advertise
```

## Store responsibility

```text
stat
read
range-read
write
delete
allocate
verify physical availability
```

## StorageManager responsibility

```text
choose replica
materialise
cache
verify
retry
apply policy
```

## Catalogue responsibility

```text
canonical entities
preferred metadata
relationships
bibliographic identity
```

This division should remain visible in both code and terminology.

---

# 24. Provider disappearance

One architectural benefit of this model is graceful failure.

If a Provider disappears:

```text
Provider:
    OFFLINE
```

previously imported claims can remain.

Canonical catalogue records remain.

Previously promoted DigitalAssets remain.

Replica Locations may separately become unavailable.

Thus:

```text
knowledge
≠
current provider availability
≠
byte availability
```

This is particularly important for long-lived archives.

---

# 25. Store disappearance

Likewise, a Store can disappear without invalidating Provider knowledge.

For example:

```text
Anna metadata provider:
    ONLINE

BitTorrent swarm:
    OFFLINE

Result:
    record remains discoverable
    asset remains known
    byte availability = unavailable
```

Or the reverse:

```text
Provider:
    OFFLINE

Local CAS:
    ONLINE

Result:
    canonical asset remains readable
```

The two failure domains are independent.

---

# 26. Providers may be local

Provider should not mean “website.”

A Provider may be:

```text
SQLite database
PostgreSQL database
metadata dump
Parquet dataset
JSONL files
Calibre library
old LiuXin export
remote API
website
```

The abstraction is therefore better understood as:

> **External knowledge source**

rather than:

> **Internet service**

This should help keep the API durable.

---

# 27. Providers and self-advertising storage

A LiuXin pack or removable disk can also carry metadata.

There are two possible facets:

```text
ManifestProvider
    advertises what objects the media contains

FilesystemStore
    physically exposes the bytes
```

The same disk therefore participates in both systems without merging the abstractions.

Conceptually:

```text
removable disk
      │
      ├── Provider facet
      │      "I contain these blobs"
      │
      └── Store facet
             "Here are the bytes"
```

This is especially useful for disconnected or archival media.

---

# 28. Providers and remote LiuXin nodes

The same structure naturally extends to federation.

A remote node may expose:

```text
RemoteLiuXinProvider
    manifests
    metadata claims
    object discovery

RemoteLiuXinStore
    byte retrieval
```

This gives LiuXin a clean path towards distributed operation without inventing a separate federation ontology.

A remote LiuXin instance is simply a particularly capable integration exposing both knowledge and bytes.

---

# 29. Why not let Provider directly return streams?

It would appear simpler:

```python
provider.download(asset)
```

but this couples source-specific discovery code to:

```text
network IO
cache policy
integrity verification
retry handling
replica state
storage allocation
failure accounting
```

It also bypasses the existing StorageManager architecture.

The apparent simplification therefore creates architectural duplication later.

Keeping byte I/O inside Stores is the more scalable choice.

---

# 30. Why Providers should still know about assets

The opposite extreme is also undesirable.

If Providers can expose only bibliographic metadata, LiuXin loses important source-specific knowledge such as:

```text
which EPUB belongs to this record
which PDF is the scan
which torrent member contains the object
which remote filename represents the edition
which derivative is OCR
```

That knowledge naturally belongs to the Provider.

Therefore the correct boundary is not:

```text
Provider = metadata only
Store = files
```

but:

```text
Provider = knowledge about external records and assets

Store = physical access to bytes
```

This distinction is subtle but important.

---

# 31. Candidate final pipeline

The complete flow becomes:

```text
                  EXTERNAL SYSTEM
                        │
                        ▼
                     Provider
                        │
          ┌─────────────┴─────────────┐
          │                           │
     SourceRecord            ExternalAssetDescriptor
          │                           │
     MetadataClaims              ExternalLocations
          │                           │
          └──────────────┬────────────┘
                         │
                   match / promote
                         │
                         ▼
                   LiuXin Catalogue
                         │
                    DigitalAsset
                         │
                      Replica
                         │
                      Location
                         │
                       Store
                         │
                       bytes
                         │
                       CAS
```

This pipeline preserves the distinction between discovery, meaning, identity, location and retrieval.

---

# 32. Suggested implementation sequence

The safest implementation path is incremental.

### Stage 1

Create:

```text
providers/
```

with:

```text
ProviderAPI
SourceRecord
ProviderCapabilities
```

Wrap ISFDB or another existing local metadata source.

### Stage 2

Add:

```text
MetadataClaim
ExternalIdentifier
```

without changing canonical WEMI persistence.

### Stage 3

Add:

```text
ExternalAssetDescriptor
ExternalLocation
```

Use an uncomplicated source to prove asset advertisement.

### Stage 4

Implement promotion:

```text
ExternalAssetDescriptor
    ↓
DigitalAsset
    ↓
Replica
```

### Stage 5

Connect a generic HTTP or BitTorrent Store.

Prove:

```text
Provider
    ↓
ExternalAssetDescriptor
    ↓
promotion
    ↓
Store
    ↓
local cache
```

### Stage 6

Add more complex Providers.

Only after the API has survived multiple integrations should old source-specific metadata code be migrated wholesale.

---

# 33. Architectural invariants

The Provider architecture should preserve several rules.

### 1. Providers advertise; they do not dictate.

External metadata remains provenance-bearing evidence.

### 2. Providers may describe assets.

Binary knowledge is not restricted to Storage.

### 3. Providers do not provide byte streams.

Physical retrieval remains a Store operation.

### 4. Provider queries do not mutate the canonical catalogue.

Promotion is explicit.

### 5. External assets need not immediately become DigitalAssets.

Large external catalogues remain cheap.

### 6. Stores remain byte-oriented.

They do not gain bibliographic search semantics.

### 7. Shared source-specific clients are encouraged.

Architectural separation must not force duplicate implementation.

### 8. Provider and Store failure are independent.

Knowledge and byte availability are separate states.

### 9. Generic transports are preferred.

If a Provider can advertise an ordinary HTTP or BitTorrent Location, reuse the generic Store rather than creating a source-specific reader.

---

# 34. Final architectural proposition

The earlier architecture can now be refined into four distinct concerns:

```text
Provider
    what outside systems SAY exists

Catalogue
    what LiuXin BELIEVES and represents

DigitalAsset
    what exact BYTES constitute the object

Store
    where/how those bytes can be obtained
```

The critical bridge is:

```text
ExternalAssetDescriptor
```

It allows a Provider to say:

> “I know that this binary object exists, I know something about its identity, and I know one or more possible ways of obtaining it.”

without requiring the Provider to become the mechanism that actually retrieves those bytes.

That lets LiuXin accommodate highly specialised external integrations while preserving a small and coherent Storage abstraction.

The resulting model is therefore:

```text
External knowledge
        ↓
     Provider
        ↓
 records + claims + asset descriptions
        ↓
   explicit promotion
        ↓
     Catalogue
        ↓
  DigitalAsset identity
        ↓
 Replica / Location
        ↓
       Store
        ↓
       bytes
```

This appears to offer the best balance between extensibility and architectural discipline.

Providers can become as specialised as necessary.

Stores can remain simple.

The catalogue retains authority over LiuXin's internal representation.

And external assets can participate fully in discovery, retrieval and eventual preservation without forcing external websites, APIs or metadata systems into abstractions they do not naturally fit.