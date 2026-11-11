# LiuXin Architecture Note: Stores, Objects, Claims and a Distributed Archive

## Status

**Architecture direction / candidate final form**

This note describes a proposed architecture for LiuXin in which local disks, immutable packs, tape, remote machines, torrents, public archives and external collections all appear through a common **Store Provider** abstraction.

The central idea is simple: LiuXin should not primarily model *where a file is*. It should model **what an object is**, what is known about it, which stores claim to possess it, and which transports can currently provide its bytes. Storage then becomes a dynamic graph of availability, provenance, cost, reliability, identity, verification, priority and replication.

This permits an external collection such as Anna’s Archive to be mounted as a read-only store without making Anna’s Archive a special architectural case. The same mechanism should support a local disk, an LTO tape library, another LiuXin node, an Internet Archive collection, a removable disk, or a future transport not yet designed.

---

## 1. Architectural principle

The principal abstraction should be:

> **An object exists independently of the stores which currently contain it.**

A logical document may correspond to one or more binary blobs. A blob may exist in multiple physical representations. Each representation may be available through several transports.

```text
Logical Record
      │
      ▼
Content Object / Blob
      │
      ├──────────────┬──────────────┐
      ▼              ▼              ▼
Local CAS         Pack extent    Loose file
                      │
                      ▼
                    Pack
                      │
            ┌─────────┼─────────┐
            ▼         ▼         ▼
         Torrent     Tape      HTTP
```

The catalogue, identity, storage, transport, cache, provenance, replication and search layers remain separate.

---

## 2. Objects, records and blobs are different things

### Record

A **record** represents an intellectual or bibliographic object:

```text
Arthur C. Clarke
The City and the Stars
1956
English
ISBN ...
```

A record can have multiple editions, formats and files.

### Blob

A **blob** is an exact byte sequence. Its canonical identity should be content-addressed:

```text
sha256:<hash>
```

or potentially:

```text
blake3:<hash>
```

Two files with identical bytes are therefore one blob regardless of where they were obtained.

### Representation

A **representation** explains how a blob exists physically:

```text
loose local file
extent inside immutable pack
torrent member
range inside torrent member
tape file
HTTP object
remote LiuXin node
```

This prevents metadata from becoming entangled with physical storage, avoids accidental duplicates, preserves provenance, enables migration and permits multiple independent copies to coexist.

---

## 3. Stores advertise; LiuXin decides

Every storage system should implement a common **Store Provider** interface.

A provider does not dictate global truth. It advertises **claims**.

For example:

```text
Store AnnaArchive says:

"I know blob X."
"I believe blob X corresponds to Foundation by Isaac Asimov."
"I can obtain blob X from torrent Y, member Z."
"I am read-only."
"I do not currently possess the bytes locally."
```

Another provider might say:

```text
Store LocalArchive says:

"I possess blob X."
"I verified its SHA-256 yesterday."
"It exists in pack 821."
"I can deliver it immediately."
```

LiuXin combines these claims, tracks provenance, assigns trust, records conflict, selects priority, evaluates availability and chooses retrieval strategies.

No provider should silently overwrite canonical metadata merely because it supplied a file.

---

## 4. Proposed provider interface

A minimal conceptual interface might be:

```text
StoreProvider

    describe()
    capabilities()

    enumerate_objects(cursor)
    lookup(identifier)

    advertise_metadata(object_id)
    advertise_locations(object_id)

    stat(object_id)
    open(object_id)

    verify(object_id)
    health()
```

Optional capabilities:

```text
CAP_METADATA
CAP_ENUMERATE
CAP_LOCAL_BYTES
CAP_RANDOM_ACCESS
CAP_RANGE_READ
CAP_WRITE
CAP_DELETE
CAP_CACHE
CAP_SEED
CAP_TORRENT
CAP_HTTP
CAP_TAPE
CAP_OFFLINE_MEDIA
CAP_REMOTE_NODE
CAP_READ_ONLY
CAP_VERIFY_HASH
```

A tape library does not need to pretend that it can perform random reads. A torrent store does not need to pretend that it owns local files. An Anna’s Archive provider can be read-only.

---

## 5. Metadata is a claim, not truth

Instead of:

```text
book.title = "Foundation"
```

LiuXin should conceptually record:

```text
metadata_claim

    subject       = record:123
    field         = title
    value         = "Foundation"
    source        = openlibrary
    observed_at   = ...
    confidence    = ...
```

Multiple sources may agree or disagree. LiuXin can choose a preferred display value without destroying alternatives.

This preserves provenance, contradiction, trust, correction, history, attribution, import, merge and rollback.

---

## 6. Stores can advertise unknown objects

A major consequence is that attaching storage can **expand the catalogue**.

Imagine plugging in an old 18 TB disk containing:

```text
.liuxin/
    manifest.sqlite
    packs/
```

The manifest says:

```text
I contain:
    3,184,221 blobs

I know:
    2,991,441 bibliographic mappings

I can verify:
    all pack hashes
```

LiuXin reads the manifest and immediately learns about those objects.

When the disk disconnects:

```text
object remains known
location becomes unavailable
```

Nothing disappears from the catalogue. The same model applies to tape, remote nodes and external archives.

---

## 7. Availability graph

LiuXin should maintain an explicit graph of all known ways to obtain a blob.

```text
Record:
    "Foundation"

Blob:
    sha256:abcdef...

Locations:

    Local CAS
        online
        verified 2026-11-09

    Pack 000472
        disk archive-node-01
        offset 81223991
        length 2476012

    Torrent
        infohash abc...
        member 781
        pieces 93-96

    Remote LiuXin node
        salisbury-node
        online

    Tape
        LTO5-042
        file 17
        offline
```

The resolver can then answer:

```text
Do we have it?
Where?
How quickly can we get it?
How expensive is retrieval?
How recently was this copy verified?
How many independent copies exist?
Are any copies geographically independent?
Can we reseed it?
Will retrieval require an operator to mount a tape?
```

---

## 8. Resolution policy

Applications should request an object, not a storage location.

```text
liuxin get sha256:abcdef...
```

A simple policy might rank candidate sources:

```text
1. Local CAS
2. Local loose file
3. Local pack
4. LAN LiuXin peer
5. Torrent with strong availability
6. HTTP remote
7. Remote archive
8. Tape requiring mount
```

Ranking should be policy-driven and may consider:

```text
latency
bandwidth
monetary cost
power cost
availability
verification status
legal policy
trust
geographic locality
storage wear
tape mount requirement
network limits
```

---

## 9. Anna’s Archive as a read-only Store Provider

An Anna’s Archive integration should not depend on its website interface. It can import whatever metadata and torrent manifests are deliberately configured for the provider.

```text
AnnaArchiveProvider
    read_only = true

    metadata:
        imported catalogue subset

    locations:
        torrent manifests

    bytes:
        lazy torrent retrieval
```

A record might resolve:

```text
book record
    ↓
external source ID
    ↓
AAC file record
    ↓
data_folder
    ↓
torrent
    ↓
member AACID
```

The provider advertises metadata claims, blob/source identity, torrent identity, torrent member and availability. LiuXin itself remains agnostic.

---

## 10. Mounting external stores

External stores should first appear inside **LiuXin's object namespace**:

```text
Stores
    Local Archive
    Tape Library
    Remote: Salisbury
    Anna's Archive [read-only]
    Internet Archive
```

A filesystem mount can exist, but it should be a façade rather than the canonical structure.

```text
/mnt/liuxin/
    authors/
    titles/
    isbn/
    stores/
    hashes/
```

These paths are synthetic. Several paths can resolve to the same blob without duplication.

```text
/mnt/liuxin/authors/Arthur C Clarke/...
/mnt/liuxin/isbn/978.../
/mnt/liuxin/stores/annas/...
```

All may ultimately resolve to:

```text
sha256:abcdef...
```

The file hierarchy is therefore a convenience interface, not the ontology or authority of the system.

---

## 11. Hybrid physical storage

LiuXin should deliberately support both loose objects and packed objects.

### Loose objects

Best for:

```text
recently accessed files
small working sets
frequently modified metadata
cache entries
high-demand objects
```

### Immutable packs

Best for:

```text
long-term archives
millions of tiny files
cold storage
tape
SnapRAID
integrity checking
efficient filesystem use
```

A blob can exist simultaneously as:

```text
loose cached object
+
extent in immutable pack
+
torrent member
+
tape copy
```

LiuXin sees one blob and several replicas.

---

## 12. Packs should advertise their own contents

Every LiuXin pack should carry or accompany a self-describing manifest.

```text
pack:
    id
    format
    version
    hash
    size

objects:
    blob hash
    offset
    compressed length
    uncompressed length
    codec
    media type
```

The catalogue should survive independently, but the pack should remain intelligible if separated from the central database.

An orphaned disk recovered twenty years later should be able to say, in effect: **“Here is what I contain.”**

---

## 13. Transport is separate from storage

A pack should not intrinsically be “a torrent pack.”

```text
pack_id → transport bindings
```

Example:

```text
Pack 472

local:
    /archive/packs/472.pack

torrent:
    btih:abc...

http:
    https://...

remote-node:
    node:salisbury

tape:
    LTO5-042/file17
```

This allows transport to evolve independently.

---

## 14. Torrent-backed retrieval

A torrent transport provider should expose exact object availability.

For loose torrent members:

```text
blob
    ↓
torrent
    ↓
member
```

For packed objects:

```text
blob
    ↓
pack extent
    ↓
torrent member
    ↓
piece range
```

The core resolver should not contain BitTorrent-specific arithmetic. Instead:

```text
TorrentTransport.resolve(extent)
```

returns:

```text
torrent infohash
member index
piece ranges
expected object hash
```

After retrieval:

```text
torrent verification
    ↓
blob hash verification
    ↓
local CAS
```

---

## 15. Demand creates replicas

If partially retrieved torrent data can continue to seed, then usage itself creates redundancy.

```text
read demand
    ↓
local cache
    ↓
additional distributed copy
    ↓
greater availability
```

Popular objects naturally gain replicas while cold objects remain concentrated in archival nodes.

---

## 16. Local CAS

Retrieved blobs should enter a local content-addressable cache:

```text
CAS/
    sha256/
        ab/
            cd/
                abcdef...
```

Applications should read the CAS object rather than repeatedly referring back to the transport.

Evicting a cache entry removes only one location; the object remains known.

The distinction between **known**, **available**, **local**, and **cached** should be explicit throughout LiuXin.

---

## 17. Store health and confidence

A location should carry operational state:

```text
ONLINE
OFFLINE
DEGRADED
UNKNOWN
MISSING
CORRUPT
REQUIRES_OPERATOR
REMOTE
```

Each copy should track verification information:

```text
last_verified
verification_method
expected_hash
observed_hash
failure_count
```

Knowledge, presence, accessibility, integrity, availability, health, location and recoverability are different things.

---

## 18. Suggested core schema

```text
record
edition
blob
identifier

metadata_claim
source

store
store_object
location

pack
extent

transport
torrent
torrent_member

verification
availability

cache_entry
replica_policy
```

Relationships:

```text
record
  ↓
edition
  ↓
blob
  ↓
location
  ↓
store

blob
  ↓
extent
  ↓
pack
  ↓
transport
```

Metadata claims attach primarily to records and editions. Integrity attaches to blobs. Physical placement attaches to locations and extents. Availability attaches to stores and transports.

---

## 19. Import adapters

Every external ecosystem should ideally be implemented as an adapter rather than imported directly into the core schema.

```text
OpenLibraryAdapter
LibGenAdapter
AnnaArchiveAdapter
InternetArchiveAdapter
FilesystemAdapter
SquashFSAdapter
LTOAdapter
TorrentAdapter
RemoteLiuXinAdapter
```

An adapter converts foreign concepts into:

```text
records
claims
blobs
locations
capabilities
```

If Anna changes AAC naming, only `AnnaArchiveAdapter` needs changing. If SquashFS disappears from the local archive, bibliographic records remain untouched.

---

## 20. Read-only versus writable stores

Store mutability should be explicit.

```text
Anna's Archive     READ_ONLY
Internet Archive   READ_ONLY
LTO archive        APPEND_ONLY
Local CAS          READ_WRITE
Immutable packs    APPEND_ONLY
Remote peer        POLICY_DEPENDENT
```

The resolver must never assume that because a store can read an object it can accept a replica.

---

## 21. Replication policy

Once locations are first-class objects, LiuXin can make preservation decisions automatically.

Example policy:

```text
Every important blob should have:

    2 online copies
    1 offline copy
    2 physical devices
    2 geographic locations
```

The planner can then answer:

```text
Which blobs violate policy?
Which packs exist on only one disk?
Which tapes contain the sole offline copy?
Which torrent-backed blobs have no local archival copy?
Which cached files should be promoted into permanent storage?
```

At that point LiuXin becomes partly a preservation orchestrator rather than merely a catalogue.

---

## 22. Discovery protocol between LiuXin nodes

Eventually, two LiuXin instances should be able to exchange manifests rather than entire catalogues.

A node could advertise:

```text
node identity
supported protocols
pack hashes
blob hashes
metadata namespaces
availability
```

A peer does not need the bytes to discover that they exist.

Future distributed manifests might look like:

```text
manifest:
    node
    sequence
    timestamp

contains:
    pack hashes
    object hashes

metadata:
    optional claims

transports:
    optional endpoints
```

Signed manifests could allow disconnected exchange, trust, discovery, replication, federation, audit, synchronisation and provenance.

---

## 23. The filesystem becomes a view

LiuXin's fundamental model is **not a filesystem tree**.

Directories are one possible projection. The real structure is a graph:

```text
records
objects
identifiers
claims
stores
locations
packs
transports
```

FUSE can present convenient views:

```text
/by-author/
/by-title/
/by-year/
/by-isbn/
/by-store/
/by-format/
/recent/
/offline/
```

Moving a title between generated paths does not move bytes. It changes metadata.

---

## 24. Compatibility with current LiuXin

The architecture does not require throwing away the existing archive.

Existing SquashFS/ZSTD packs become:

```text
Store:
    ExistingArchive

Pack:
    squashfs image

Contents:
    indexed blobs

Transport:
    local disk
```

Later they can gain:

```text
Transport:
    torrent
```

without rewriting them.

A sidecar extent index can progressively expose individual objects. New packs can eventually use a format better optimised for range retrieval.

Migration can therefore be incremental; no “flag day” is necessary.

---

## 25. Proposed implementation stages

### Stage 1 — Store abstraction

Implement:

```text
store
location
blob
capabilities
```

Wrap the current filesystem archive as the first provider.

### Stage 2 — Self-advertising local packs

Create sidecar manifests containing:

```text
blob
hash
pack
extent
```

Attaching a disk can now populate availability automatically.

### Stage 3 — Local CAS

Add:

```text
liuxin get <blob>
```

and a content-addressed cache.

### Stage 4 — Torrent transport

Associate one existing immutable pack with a torrent:

```text
pack → torrent
```

Initially download the whole pack.

### Stage 5 — Selective torrent retrieval

Support:

```text
blob → extent → torrent pieces
```

Retrieve only necessary pieces.

### Stage 6 — External read-only provider

Implement an external corpus as:

```text
metadata claims
+
torrent locations
```

Anna’s Archive provides a useful research model for this, although a legally uncomplicated public-domain/open-data corpus would make a cleaner first test.

### Stage 7 — Remote LiuXin nodes

Allow another LiuXin instance to advertise manifests and objects.

At this point the architecture becomes genuinely distributed.

---

## 26. Proposed invariant rules

### 1. Content identity never depends on location

```text
sha256(blob)
```

remains the same whether the blob is on disk, tape or torrent.

### 2. Metadata never silently overwrites metadata

Every imported value retains provenance.

### 3. Stores advertise capabilities explicitly

Never infer writeability or random-access capability.

### 4. Physical storage details do not leak into bibliographic records

No record should need to know what pack contains its EPUB.

### 5. Transport identities are immutable

Prefer torrent infohashes over torrent filenames.

### 6. Every retrieved object is independently hash-verified

Transport-level verification is not enough.

### 7. Disconnection removes availability, not knowledge

An offline disk does not delete catalogue entries.

### 8. Derived views are disposable

Search indexes, filesystem views and caches can always be rebuilt from authoritative records and manifests.

---

## 27. Candidate final form

```text
                     ┌────────────────────┐
                     │   LiuXin Catalogue │
                     │ records / claims   │
                     └─────────┬──────────┘
                               │
                         identity graph
                               │
                     ┌─────────▼──────────┐
                     │      Blobs         │
                     │ content-addressed  │
                     └─────────┬──────────┘
                               │
                    availability / replicas
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   ┌──────▼──────┐      ┌──────▼──────┐     ┌──────▼──────┐
   │ Local Store │      │ Remote Store│     │ External RO │
   └──────┬──────┘      └──────┬──────┘     └──────┬──────┘
          │                    │                    │
     ┌────┼────┐          network/API         torrent metadata
     │    │    │                                  │
    CAS  Pack Tape                            BitTorrent
          │
       transports
          │
     disk / torrent /
     remote / tape
```

On top sit:

```text
search
filesystem views
LiuXin UI
automation
preservation planning
replication
classification
AI tools
```

None of them needs to know which storage technology supplied the bytes.

---

## 28. Final architectural proposition

The useful conceptual shift is:

### Old model

```text
A file lives somewhere.
LiuXin catalogues it.
```

### New model

```text
An object exists.

Stores make claims about it.

Stores advertise ways of obtaining it.

LiuXin reconciles those claims.

The resolver obtains bytes from the best available source.

The cache creates another replica.

Preservation policy decides which replicas should become permanent.
```

That is sufficiently general to encompass the current LiuXin archive, immutable SquashFS packs, removable disks, LTO, remote nodes, torrent swarms, public repositories, external catalogues and future storage technologies without making any of them special cases.

It also gives LiuXin something rather unusual: **storage becomes federated, declarative and self-describing**.

A newly connected store can teach LiuXin what exists.

A disconnected store can remain represented as an unavailable location.

A remote archive can behave like a read-only extension of the local collection.

A cached retrieval can become a replica.

A torrent can function as transport without becoming the filesystem.

And the archive's actual identity survives every one of those changes.

That feels like a plausible final architectural form for LiuXin.
