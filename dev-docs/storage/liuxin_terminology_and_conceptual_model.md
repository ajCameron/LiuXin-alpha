# LiuXin Terminology and Conceptual Model

## Purpose

This document defines the principal terms used within LiuXin and explains the relationships between them. 
It describes the conceptual model rather than a particular database schema, programming interface, or storage 
implementation. 
Its purpose is to ensure that terms such as *Item*, *Asset*, *Digital Asset*, *Replica*, *Store*, and *File* are used in
a cohérent, precise, and durable manner.

At the highest level, LiuXin needs to answer four different questions:

1. **What intellectual or creative thing is this?**
2. **Which edition, version, or acquired copy does it represent?**
3. **Which digital content belongs to that copy?**
4. **Where do usable copies of the actual bytes exist?**

These questions correspond broadly to the catalogue, asset, and storage layers of the system.

---

# Top level

LiuXin is a digital library, file-management, and preservation system.

At its core, it provides a means of:

- storing digital content;
- retrieving digital content;
- identifying and verifying digital content;
- transforming content from one form into another;
- transmitting content between systems;
- maintaining multiple copies across different storage systems;
- preserving the metadata and provenance needed to understand that content.

LiuXin is therefore more than a simple file browser or directory manager. 
A conventional filesystem primarily records filenames and locations. 
LiuXin additionally records the identity of content, its bibliographic context, its provenance, its relationships to 
other content, and the location and condition of every known stored copy.

The system can be understood as three principal layers:

1. **The catalogue layer**, based around WEMI.
2. **The asset layer**, which represents digital content associated with catalogue records.
3. **The storage layer**, which records where concrete copies of that content exist.

A fourth, operational layer performs actions such as ingest, verification, replication, transformation, reconciliation, 
and deletion. 
These operations act on the three conceptual layers but do not form part of the entity hierarchy themselves.

---

# Core conceptual hierarchy

The principal conceptual path through the system is:

```text
Work
└── Expression
    └── Manifestation
        └── Item
            ├── Digital Asset
            │   └── Replica(s)
            └── Composite Digital Asset
                └── Component Digital Assets
                    └── Replica(s)
```

This tree shows the normal direction of traversal through the model. 
It should not be interpreted as requiring every database relationship to be a strict one-to-many hierarchy. 
Real bibliographic data can be complex: anthologies may contain several Works, a Manifestation may embody several 
Expressions, and an Asset may be associated with more than one Item. 
The implementation may therefore use association tables and graph-like relationships while retaining this simpler 
conceptual structure.

The key distinction is:

```text
Catalogue entities describe what something means.

Assets describe which digital content exists.

Replicas describe where copies of the bytes exist.
```

---

# WEMI

Work, Expression, Manifestation, and Item—usually abbreviated to **WEMI**—form the core of LiuXin’s bibliographic 
metadata model.

WEMI separates the abstract identity of an intellectual creation from its particular language, edition, publication, 
acquisition, and stored files. 
This separation allows LiuXin to represent multiple editions and formats without treating every EPUB, PDF, audiobook, 
scan, and physical volume as an unrelated object.

---

## Work

A **Work** represents an abstract intellectual or artistic creation.

Examples include:

- a novel;
- a short story;
- an essay;
- a musical composition;
- a film;
- a research paper;
- a comic narrative;
- a software project;
- a collection conceived as a distinct intellectual work.

A Work does not have a filename, a storage location, a checksum, or a file format. 
It represents the underlying creation independently of any particular language, performance, edition, or physical embodiment.

For example, the abstract novel *Frankenstein* is a Work. 
It is not identical to a particular English text, a French translation, a Penguin paperback, a Project Gutenberg EPUB, 
or a narrated audiobook. 
Those belong to lower layers of the WEMI stack.

A Work may have:

- one or more Expressions;
- titles and alternative titles;
- creators and contributors;
- subjects and classifications;
- relationships to other Works;
- membership in series or collections;
- descriptive notes and identifiers.

---

## Expression

An **Expression** represents a particular intellectual or artistic realisation of a Work.

An Expression captures differences such as:

- language;
- translation;
- revision;
- adaptation;
- narration or performance;
- editorial text;
- abridgement;
- arrangement;
- version of the underlying content.

For example:

- the original English text of a novel;
- a French translation;
- a revised authorial text;
- an abridged reading;
- a particular audiobook narration;
- a musical performance of a composition.

An Expression is still a logical bibliographic entity. 
It does not identify a particular published edition, acquired copy, or set of bytes. 
Several Manifestations may embody the same Expression. 
For example, the same English text may appear in a hardback, paperback, EPUB, PDF, and HTML publication.

---

## Manifestation

A **Manifestation** represents a particular published, distributed, produced, or packaged embodiment of one or more 
Expressions.

A Manifestation typically captures properties such as:

- publisher or distributor;
- publication date;
- edition statement;
- ISBN or equivalent identifier;
- media type;
- release format;
- production details;
- pagination or duration;
- packaging and presentation.

Examples include:

- a particular publisher’s 2018 paperback edition;
- a specific EPUB edition sold by a retailer;
- a particular audiobook release;
- a library-produced PDF scan;
- a journal issue containing a research paper;
- a specific Blu-ray release of a film.

A Manifestation describes an edition or release as a class of objects. 
It is not the particular copy owned, downloaded, scanned, or imported by LiuXin. 
That distinction belongs to the Item.

In some cases, changing a file format may imply a new Manifestation. 
In others, a locally generated conversion may be treated merely as a derived Asset attached to the same Item. 
That bibliographic decision should remain separate from the simpler storage fact that changed bytes always produce a new
Digital Asset.

---

## Item

An **Item** represents a particular exemplar, acquired copy, or locally recognised instance of a Manifestation.

Examples include:

- a particular physical book owned by a library;
- an EPUB purchased from a retailer;
- a PDF downloaded from a publisher;
- a scanned copy produced from a particular physical volume;
- a donated audiobook release;
- an archived copy obtained from another collection.

An Item is a catalogue entity. 
It is not itself a file, byte sequence, filesystem path, or stored copy.

For physical material, an Item may correspond to a tangible object, such as one particular book on a shelf. 
For digital material, it may correspond to one acquisition or provenance-bearing copy, even where LiuXin maintains 
several storage Replicas of the same underlying bytes.

The distinction between an Item and a Replica is important:

```text
Item:
    Which bibliographic or acquired copy is this?

Replica:
    Where does one stored copy of its bytes exist?
```

Copying an EPUB from one hard drive to another does not normally create another Item. 
It creates another Replica of the same Digital Asset. 
Acquiring the same EPUB independently from two different sources may create two Items if the separate provenance is 
important, even where both Items ultimately link to the same byte-identical Digital Asset.

An Item may exist with:

- no linked Assets;
- one linked Asset;
- several linked Assets;
- a mixture of atomic and Composite Digital Assets.

An Item with no linked Assets may represent a metadata-only record, a physical object with no digitised content, a 
known but unavailable digital object, or content that has been lost.

---

# Item–Asset relationships

Items are linked to Assets through an association or relationship.

This should preferably be modelled as an explicit association rather than merely placing an `item_id` column on every 
Asset. 
An association allows the same Asset to be linked to several Items and allows the link itself to carry useful metadata.

An Item–Asset relationship may record:

- the Asset’s role;
- whether it is the primary content;
- sequence or display order;
- provenance;
- date linked;
- whether it was acquired with or generated from the Item;
- notes about the relationship.

Possible roles include:

- primary content;
- cover;
- supplementary material;
- source scan;
- OCR output;
- transcript;
- metadata file;
- sample;
- annotation;
- derived format;
- preservation copy;
- accompanying documentation.

For example, one Item might be linked to:

```text
Primary content      → book.epub
Cover                → cover.jpg
Derived text         → extracted.txt
Supplementary files  → maps.zip
Metadata             → retailer-metadata.json
```

The role belongs to the relationship. 
The same Digital Asset might serve as the cover for several related Items, or the same source scan might be linked to 
both a physical Item and a derived digital Item.

---

# Assets

An **Asset** represents digital content known to LiuXin.

The Asset layer forms the bridge between catalogue metadata and stored bytes. 
It describes what digital content exists and how that content is organised, without tying its identity to one particular
disk, path, server, pack, or storage technology.

An Asset is a logical entity. 
It is not, by itself, a particular stored copy.

There are two principal kinds of Asset:

```text
Asset
└── Digital Asset
└── Composite Digital Asset
    └── Digital Asset
```

A Digital Asset represents one atomic byte-bearing object.

A Composite Digital Asset represents a structured whole assembled from several Digital Assets.

The abstract Asset concept allows common metadata and relationships to apply to both types without incorrectly implying 
that every Asset has its own byte stream or Replica.

---

# Digital Assets

A **Digital Asset** represents one atomic digital object.

“Atomic” means that LiuXin treats the object as one unit for storage, hashing, verification, replication, retrieval, 
and transmission. 
It does not mean that the object’s internal format is structurally simple.

A Digital Asset normally identifies a specific expected byte sequence. 
Its metadata may include:

- a stable LiuXin identifier;
- expected size;
- one or more cryptographic digests;
- media type;
- file-format identification;
- original or suggested filename;
- creation or acquisition date;
- provenance;
- technical metadata;
- preservation status.

Examples of Digital Assets include:

- an EPUB file;
- a TXT file;
- a PDF file;
- a CBR or CBZ comic archive;
- an M4B audiobook;
- an individual MP3 chapter;
- a cover image;
- a ZIP archive;
- a video file;
- a metadata document;
- a database dump.

An EPUB is normally one Digital Asset even though the EPUB format is internally a ZIP archive containing HTML, images, 
stylesheets, and metadata. 
LiuXin stores, hashes, verifies, and replicates the EPUB as one object.

Similarly, a CBZ archive is normally one Digital Asset even though it contains many page images. 
Its internal multiplicity does not make it a Composite Digital Asset unless LiuXin has explicitly unpacked and modelled 
those components as independent Digital Assets.

A single M4B audiobook is one Digital Asset. 
An audiobook distributed as twenty MP3 tracks is more naturally represented as a Composite Digital Asset whose 
components are twenty separate Digital Assets.

A Digital Asset may have:

- zero Replicas;
- one Replica;
- many Replicas.

A Digital Asset with zero Replicas remains a valid metadata record. 
It may describe an object that is known but missing, temporarily unavailable, awaiting ingest, deliberately removed, or 
entirely lost.

---

## Digital Asset identity

A Digital Asset identifies the bytes that should exist, rather than the place where those bytes happen to be stored.

In the public storage-manager API, a `DigitalAsset` is an immutable domain
snapshot of that identity. It is not the database row used to persist the
identity and it is not a container for the byte stream. Repositories privately
translate database records into domain snapshots; reading a selected Replica
Location produces the actual bytes.

The expected size and digest form an important part of that identity. 
A filename alone is not sufficient: two files with the same name may contain different bytes, while files with different
names may contain identical bytes.

The following rules should normally apply:

- Copying identical bytes to another Store creates a new Replica, not a new Digital Asset.
- Moving or renaming a stored copy does not create a new Digital Asset.
- Changing the bytes creates a new Digital Asset.
- Converting from one format to another creates a new Digital Asset.
- Re-encoding audio creates a new Digital Asset.
- Recompressing an archive creates a new Digital Asset, even when the extracted contents are unchanged.
- Two byte-identical acquisitions may share one Digital Asset while remaining linked to separate Items.
- A cryptographic digest is evidence of byte identity, not bibliographic identity.

Two EPUBs can represent the same Work, Expression, Manifestation, and perhaps even Item while still being different 
Digital Assets if their bytes differ. 
Conversely, one byte-identical Digital Asset may be associated with several Items where the catalogue or provenance 
model requires it.

---

# Composite Digital Assets

A **Composite Digital Asset** represents one meaningful digital object assembled from several component Digital Assets.

Examples include:

- an audiobook composed of many MP3 tracks;
- a scanned book composed of individual page images;
- a website composed of HTML, images, scripts, and stylesheets;
- a multimedia publication containing text, audio, and video;
- a software distribution composed of several files;
- an unarchived comic composed of individual page images;
- a research dataset consisting of several related files;
- a disc image represented by separate tracks and cue sheets.

A Composite Digital Asset records the organisation of its components. The relation [relationship] between a Composite Digital Asset and each component may include:

- component Digital Asset;
- sequence number;
- logical filename;
- logical path;
- role;
- title;
- duration;
- disc or volume number;
- optionality;
- presentation order.

For example:

```text
Composite Digital Asset: Complete audiobook
├── Digital Asset - 001  front-cover.jpg       role=cover
├── Digital Asset - 002  chapter-01.mp3        role=audio
├── Digital Asset - 003  chapter-02.mp3        role=audio
├── Digital Asset - 004  chapter-03.mp3        role=audio
└── Digital Asset - 005  booklet.pdf           role=supplement
```

The Composite Digital Asset describes the ensemble. 
The component Digital Assets identify the individual byte-bearing objects. 
The component Digital Assets, rather than the Composite Digital Asset itself, have Replicas.

The component relationship should be a first-class entity or association. 
This allows the same Digital Asset to appear in more than one Composite Digital Asset and allows ordering, paths, and 
roles to be represented without altering the component Asset.

The simplest initial rule is that Composite Digital Assets contain only Digital Assets, not other Composite Digital Assets. 
Logical paths and component roles can represent substantial internal structure without introducing recursive composites, 
cycle detection, or ambiguous nested ownership. 
Nested composites can be added later if a genuine use case requires them.

---

## Packaged forms of Composite Digital Assets

A Composite Digital Asset does not directly contain bytes and therefore does not normally have Replicas.

When a Composite Digital Asset is packaged into a single file, that package becomes a separate Digital Asset.

For example:

```text
Composite Digital Asset
├── chapter-01.mp3
├── chapter-02.mp3
└── chapter-03.mp3

Derived packaged representation
└── complete-audiobook.m4b
```

The M4B file is not a Replica of the Composite Digital Asset. 
It is a new Digital Asset derived from the same underlying content.

Similarly:

- zipping a directory produces a ZIP Digital Asset;
- creating a CBZ from page images produces a CBZ Digital Asset;
- combining WAV tracks into a FLAC image produces a new Digital Asset;
- creating a PDF from page scans produces a new Digital Asset.

These packaged or transformed Assets should be connected by explicit provenance or derivation relations.

---

# Derivation and provenance

A transformation does not overwrite the identity of its source Asset. 
It produces a new Asset connected to its source through a provenance relationship.

Examples include:

```text
EPUB
└── extracted text

Page images
└── OCR text

WAV master
├── FLAC preservation copy
└── MP3 access copy

Multi-file audiobook
└── combined M4B audiobook
```

A derivation relationship may record:

- source Asset or Assets;
- resulting Asset;
- transformation type;
- tool and tool version;
- parameters;
- execution date;
- operator or automated job;
- success or validation status.

This gives LiuXin a chain of provenance rather than silently replacing one representation with another. 
It also permits a source Asset to be retained even where a more convenient derived format is normally used for access.

At the catalogue layer, a transformation may or may not result in a new Expression, Manifestation, or Item. 
That decision depends on bibliographic significance. 
At the Asset layer, however, different bytes always mean a different Digital Asset.

---

# Storage model

The storage model records where concrete copies of Digital Assets exist.

Its central concepts are:

```text
Digital Asset
└── Replica
    └──  Store
         └── Location
```

The Digital Asset records what bytes are expected.

The Replica records that one copy of those bytes is believed to exist.

The Store identifies the storage system holding that copy.

The Location identifies where inside that Store the copy can be found.

---

# Replicas

A **Replica** represents one concrete stored instance of a Digital Asset.

This is the bottom of the digital-storage metadata hierarchy. 
A Replica is the point at which LiuXin records that bytes exist—or are expected to exist—at a particular location within
a particular Store.

A Replica may refer to:

- a file on a local filesystem;
- an object in remote object storage;
- a file available over FTP or SFTP;
- a member inside a SquashFS pack;
- an object inside another archive format;
- an archived copy on tape;
- a blob in a database;
- another addressable storage object.

A Replica is not necessarily an ordinary standalone file visible in a directory. 
The Store and Location abstractions allow LiuXin to represent packed, remote, read-only, offline, or otherwise 
specialised storage.

A Replica record may include:

- Digital Asset identifier;
- Store identifier;
- Location;
- observed size;
- observed digest;
- current state;
- creation or discovery date;
- last successful verification date;
- last access date;
- error or warning information;
- storage-specific metadata.

The Replica does not define the content’s identity. 
Its associated Digital Asset does that. 
A Replica is one claim about where a copy of the content can be found.

The manager API consequently exposes a `Replica` domain snapshot containing
its stable ID, Digital Asset ID, Location, operational mode, and latest
observation. That snapshot is not a database record or a live file handle.
Record CRUD remains behind `ReplicaRepositoryAPI`, while physical reads and
writes are routed through the Replica's Location.

---

## Replica states

A Replica may be known to the catalogue without currently being usable.

Typical states or conditions include:

- **Staged** — being written but not yet published.
- **Present** — observed at the expected location.
- **Unverified** — present but not recently checked against the expected digest.
- **Verified** — checked and confirmed to contain the expected bytes.
- **Missing** — expected at the location but not found.
- **Corrupt** — present but does not match the expected bytes.
- **Unavailable** — cannot currently be checked because the Store is offline or inaccessible.
- **Deleted** — intentionally removed but retained in historical metadata.

The precise enum names may vary, but LiuXin should preserve the distinction between *missing*, *corrupt*, and 
*temporarily unavailable*. 
Treating all three as “not found” would lose important operational information.

A **healthy Replica** is normally one that is accessible and has been verified, or is otherwise trusted under the 
applicable verification policy.

A **healthy Digital Asset** normally has at least one healthy Replica. 
A Digital Asset with zero healthy Replicas may still remain in the catalogue but should be considered unavailable or at 
risk.

---

# Stores

A **Store** represents a configured storage destination or source.

Examples include:

- a local directory tree;
- a mounted filesystem;
- an FTP server;
- an SFTP server;
- an object-storage bucket;
- an immutable pack collection;
- a tape archive;
- a removable disk;
- an unmanaged external collection;
- a database-backed blob store.

A Store is a configured instance, not merely a type of technology. 
Two local filesystem roots are two Stores even if both use the same filesystem backend. 
Likewise, two FTP servers using the same driver are separate Stores.

A Store may expose properties such as:

- human-readable name;
- backend or driver type;
- root or endpoint;
- read-only or writable status;
- available capacity;
- failure domain;
- online or offline status;
- managed or unmanaged status;
- supported operations;
- replication priority;
- preservation role.

Not every Store supports every operation. 
An immutable SquashFS Store may support reading and enumeration but not replacement or deletion. 
A tape Store may be writable but not immediately accessible. 
An unmanaged Store may allow LiuXin to read and catalogue content without granting permission to rename or delete it.

---

# Storage drivers and Store backends

A **Storage Driver** is reusable code for accessing one configured byte-storage
endpoint. A **Store Backend** adapts such a driver to one LiuXin Store. The
driver may also be used by an import source or temporary workspace, so it is not
intrinsically tied to a Store database row.

For example:

```text
Filesystem backend
├── Store: /mnt/library-a
├── Store: /mnt/library-b
└── Store: /mnt/portable-drive

FTP backend
├── Store: archive-server-one
└── Store: archive-server-two
```

The driver knows how to perform raw byte operations. The Store holds LiuXin
configuration and identity for one particular endpoint.

A generic Store interface will normally provide operations equivalent to:

```text
stat
open_read
begin_write
commit / abort
delete
iter_locations
capabilities
status
```

The mandatory `StorageDriverAPI` core is smaller: address parsing, lifecycle,
`stat`, and `open_read`. Enumeration, staged writing, deletion, allocation,
hierarchical address construction, and native accelerators are independent
capability protocols. This allows immutable, single-object, and import-source
drivers to expose only operations they genuinely implement.

At the configured Store boundary, all operations address objects with
`Location`. A `DriverObjectAddress` is meaningful only below that boundary
inside a `StorageDriverAPI`; a driver-backed Store privately translates between
the Location's opaque value and its driver's address type. Driver addresses may
also be used directly by non-Store facilities such as import sources, but must
not appear in StorageManager, Store, Replica, or workflow contracts.

Driver address types are generic (`StorageDriverAPI[DriverObjectAddressT]`) so
unlike backend address forms cannot be mixed accidentally. Because a type
parameter cannot distinguish two configured instances of the same driver, each
instance also has an injected address checker. The standard scoped checker
requires both the concrete address subtype and a mandatory
`address_space_uuid`. That
UUID identifies whichever configured endpoint owns the address space: a Store,
import source, or temporary workspace. A Store factory injects the Store UUID
when constructing a Store/driver pair, and the private Store adapter verifies
the match.

Persisted driver-relative values are parsed separately from external URIs.
Endpoint and object URIs are credential-free representations and are never a
second implicit internal addressing model.

Cross-Store transfer planning is consequently Location-based. Store
configuration may declare stable host and physical-device UUIDs, allowing the
StorageManager to distinguish “same host”, “different host”, and “unknown”
without confusing missing topology metadata with physical separation. The
manager may then choose an appropriate transfer path. A
`NativeCopyStorageDriverAPI` operation remains narrower: it copies between two
object addresses already known to belong to the same driver endpoint and does
not perform Store discovery or topology policy.

Every raw operation returning object metadata must return the checked address
that was requested or selected as its destination. Driver utilities and the
configured Store adapter treat a different returned address as an integrity
failure. Driver-native copy, move, and digest acceleration is translated by the
Store adapter rather than merely advertised.

Deletion and conditional deletion are distinct capabilities. A backend may be
able to remove an object but lack an atomic compare-and-delete operation. Safe
generic moves therefore require both an advertised `conditional_delete`
capability and a source version returned by `stat()` before any destination is
published. Without those guarantees, the operation remains a copy followed by
separately coordinated deletion rather than pretending to be a race-safe move.

Inventory, when available, returns `DriverObjectEntry` values containing cheap
native hints, optional size, modified time, digest, and version. Shared
`DriverObjectHints` attach suggested filename, media type, and native metadata
to both inventory entries and `stat()` results. A raw driver may report an
unknown size when the endpoint cannot know it before streaming; a configured
Store remains stricter and requires an authoritative size. Prefix enumeration
is a separate advertised capability and must never be silently approximated.
These values are not bibliographic facts. A legacy importer that requires a
local filesystem path can use the verified, context-managed
`storage.utils.driver.materialize_object()` adapter; cross-driver byte movement
uses `storage.utils.driver.transfer_between_drivers()` and staged publication.
Backend-native source metadata is not copied automatically between drivers;
the caller must deliberately translate any portable facts into destination
metadata.
Configured-Store conveniences and workflow helpers likewise live under
`LiuXin_alpha.storage.utils`, leaving `LiuXin_alpha.storage.api` for contracts,
models, errors, and facade adapters.

---

# Locations

A **Location** is the address of a Replica within a Store.

A Location should be treated as opaque by code outside the relevant Store backend. 
It may resemble a path, but it is not guaranteed to be an operating-system filesystem path.

Examples include:

```text
books/example.epub

objects/ab/cd/abcdef123456...

pack-000183.squashfs:/objects/abcdef123456

bucket-name/key-name

tape-set-04/pack-000183
```

The Store backend is responsible for interpreting the Location. 
Generic catalogue and replication code should not concatenate strings, assume path separators, or infer directory 
semantics from it.
It is entirely possible that that concept just doesn't exist for that store.

A Store and Location together identify where a Replica is expected to exist:

```text
Replica address = Store + Location
```

The same opaque key in two different Stores does not refer to the same Replica.

In the API, a Location is an immutable serializable value containing the
Store's UUID and that Store's opaque key. The UUID is the stable Store identity;
a database row ID or human-readable Store name may be accepted by a factory,
but must be resolved before the Location is constructed or persisted. A
Location is not a live backend object, a file handle, a status cache, or a
virtual `pathlib.Path`. Operations such as `stat`, `open_read`, staged
publication, and deletion belong to the Store or StorageManager that interprets
the Location.

Where object-style ergonomics are useful, the StorageManager may return a
short-lived `BoundLocation`. That facade pairs a manager with a Location and
delegates every operation back to the manager. It must not replace the plain
Location in Replica rows, workflow persistence, messages, or durable APIs, and
it must not grow path joining, parent traversal, or cached metadata.

The manager may also expose a `LocationFactory` for catalogue-aware resolution.
For example, `manager.location_factory.from_id(digital_asset_id)` selects one
currently readable Replica Location for a Digital Asset, while
`from_replica_id(replica_id)` resolves one exact Replica. The former is a
policy-bearing lookup, not a claim that a Digital Asset has one permanent
Location: selection may change as Replica health, availability, and placement
change.

---

# Packs and containers

A storage pack is a container used to hold many Replica byte streams efficiently.

For example, LiuXin may place thousands of Digital Assets into an immutable SquashFS pack. 
Each member remains a Replica of its own Digital Asset, even though the members are physically stored inside one larger 
pack file.

Conceptually:

```text
Pack file
├── Replica of Digital Asset A
├── Replica of Digital Asset B
├── Replica of Digital Asset C
└── Replica of Digital Asset D
```

The pack itself may also need to be tracked as a storage-management object, particularly where it has its own digest, 
parity, tape copies, or lifecycle. 
It should not, however, replace the Asset and Replica model for the files contained inside it.

A packed Replica’s Location might identify both the pack and the internal member:

```text
pack-000183.squashfs:/objects/ab/cd/asset.epub
```

The details remain backend-specific.

---

# The term “File”

The term **file** is useful in ordinary prose but is dangerously overloaded in the data model.

Depending on context, “file” might mean:

- a logical byte-bearing object;
- one stored copy of that object;
- a filesystem directory entry;
- a member inside an archive;
- a storage pack;
- a user-visible document;
- a Composite Digital Asset containing several files.

LiuXin should therefore avoid using `File` as a formal entity name unless a separate and clearly defined concept is 
genuinely required.

The preferred terminology is:

| Intended meaning | Preferred term |
|---|---|
| One logical atomic byte sequence | Digital Asset |
| A structured group of digital objects | Composite Digital Asset |
| One concrete stored copy | Replica |
| A configured storage system | Store |
| The address of a copy inside a Store | Location |
| Informal user-facing description | File |

A Digital Asset can be described informally as a file, and a Replica may correspond to an actual filesystem file, but 
the two are not interchangeable.

This distinction prevents ambiguous statements such as:

> “The file has three files.”

The intended meaning can instead be expressed precisely:

> “The Composite Digital Asset has three component Digital Assets.”

or:

> “The Digital Asset has three Replicas.”

---

# Identity and equivalence rules

LiuXin distinguishes bibliographic identity, digital identity, and storage identity.

## Bibliographic identity

Bibliographic identity is expressed through Work, Expression, Manifestation, and Item.

Two objects may represent the same Work but different Expressions, Manifestations, or Items.

## Digital identity

Digital identity is expressed through Digital Assets.

Two stored objects belong to the same Digital Asset when they are intended to contain the same byte sequence. 
Size and cryptographic digest provide the principal technical evidence for this.

## Storage identity

Storage identity is expressed through Replicas.

Two Replicas may contain identical bytes while remaining distinct because they exist in different Stores or Locations.

The following examples illustrate the distinction:

| Event | Result |
|---|---|
| Copy an EPUB to another disk | New Replica of the same Digital Asset |
| Rename an EPUB | Same Digital Asset; Replica Location changes |
| Move an EPUB between Stores | Usually a new Replica followed by deletion of the old Replica |
| Edit one byte in the EPUB | New Digital Asset |
| Convert EPUB to PDF | New Digital Asset |
| Combine MP3 tracks into M4B | New Digital Asset derived from the Composite Digital Asset |
| Download identical bytes from another source | Possibly a new Item, but the same Digital Asset |
| Discover another existing copy | New Replica record for the same Digital Asset |
| Delete the final stored copy | Digital Asset remains, but has zero available Replicas |

---

# Core operations

## Ingest

**Ingest** is the process by which content enters LiuXin.

A typical ingest may:

1. identify or create the relevant WEMI entities;
2. identify or create an Item;
3. read and inspect the incoming bytes;
4. calculate size and cryptographic digests;
5. identify an existing Digital Asset or create a new one;
6. write or adopt a Replica;
7. verify the stored copy;
8. link the Asset to the Item;
9. record provenance.

Ingest should distinguish between content already safely stored and content merely observed in an external or unmanaged 
Store.

---

## Retrieval

**Retrieval** is the process of obtaining the bytes of a Digital Asset.

The storage manager should normally:

1. identify candidate Replicas;
2. exclude known missing or corrupt copies;
3. prefer an online and healthy Replica;
4. open the Replica through its Store backend;
5. optionally verify the bytes while reading;
6. return a stream or local materialisation.

The caller requests the Digital Asset. The storage layer chooses the Replica.

---

## Replication

**Replication** creates an additional Replica of an existing Digital Asset.

A normal replication process is:

```text
Select healthy source Replica
        ↓
Select suitable destination Store
        ↓
Begin staged write
        ↓
Stream bytes
        ↓
Verify expected size and digest
        ↓
Atomically publish
        ↓
Create or update Replica record
```

Replication does not create a new Digital Asset because the expected bytes remain unchanged.

---

## Verification

**Verification** checks whether a Replica contains the bytes expected by its Digital Asset.

Verification may include:

- checking presence;
- checking size;
- calculating a digest;
- comparing the digest with the expected value;
- checking container or filesystem integrity;
- updating the last-verified timestamp;
- marking the Replica healthy, missing, or corrupt.

A Store being unreachable should normally produce an *unavailable* result rather than marking all of its Replicas 
missing.

---

## Transformation

**Transformation** creates new Assets from existing Assets.

Examples include:

- EPUB to PDF;
- extracting the cover from an EPUB
- image to OCR text;
- WAV to FLAC;
- MP3 tracks to M4B;
- unpacked page images to CBZ;
- source files to a compiled executable;
- web content to a local archival package.

A transformation creates a new Digital Asset or Composite Digital Asset and records derivation from the source. 
It does not create a Replica of the original Asset.

---

## Transmission and export

**Transmission** streams or copies content outside the immediate Store system.

Examples include:

- downloading through an API;
- sending to another LiuXin instance;
- exporting to a removable disk;
- transmitting to an external archive;
- serving a file to a reader application.

Transmission may create another managed Replica, an unmanaged external copy, or no persistent copy at all. 
The result depends on whether the destination remains under LiuXin’s control.

---

## Deletion

Deletion should distinguish between deleting a Replica and deleting metadata.

Deleting one Replica does not normally delete the Digital Asset, Item, Manifestation, Expression, or Work. 
Other Replicas may remain.

Deleting the final Replica leaves the Digital Asset without an available stored copy. 
LiuXin may retain that record to preserve:

- historical knowledge;
- provenance;
- expected digests;
- loss records;
- links from Items;
- evidence needed for later recovery.

Permanent removal of the Asset metadata is a separate and more consequential operation.

---

## Reconciliation

**Reconciliation** compares LiuXin’s metadata with the actual contents of a Store.

It may discover:

- expected Replicas that are missing;
- unexpected stored objects;
- moved or renamed content;
- duplicates;
- corrupt objects;
- incomplete writes;
- orphaned temporary files;
- metadata records that no longer match reality.

Reconciliation depends on the Store backend’s ability to enumerate Locations. 
A Store should declare whether its enumeration is complete, partial, expensive, or unavailable.

---

# Worked examples

## Example 1: A single EPUB

```text
Work
└── Example Novel
    └── Expression
        └── English text
            └── Manifestation
                └── 2024 EPUB edition
                    └── Item
                        └── Copy acquired from publisher
                            └── Digital Asset
                                └── example-novel.epub
                                    ├── Replica: live filesystem
                                    ├── Replica: immutable SquashFS pack
                                    └── Replica: tape archive
```

The EPUB is one Digital Asset. 
The three stored copies are three Replicas. 
Copying the EPUB into the SquashFS pack does not create another Item or Digital Asset.

---

## Example 2: A multi-file audiobook

```text
Work
└── Example Novel
    └── Expression
        └── Unabridged English narration
            └── Manifestation
                └── 2025 MP3 audiobook release
                    └── Item
                        └── Acquired audiobook
                            └── Composite Digital Asset
                                ├── cover.jpg
                                ├── chapter-01.mp3
                                ├── chapter-02.mp3
                                ├── chapter-03.mp3
                                └── booklet.pdf
```

Each component is a Digital Asset and may have several Replicas. 
The Composite Digital Asset records their order and roles.

If LiuXin combines the MP3 files into one M4B file, the M4B is a new Digital Asset derived from the 
Composite Digital Asset:

```text
Composite Digital Asset: MP3 release
└── Derived Digital Asset: complete-audiobook.m4b
```

The M4B is not an additional Replica of any individual MP3.

---

## Example 3: A CBZ comic archive

```text
Item
└── Digital Asset
    └── comic.cbz
        ├── Replica: main store
        └── Replica: backup store
```

Although the CBZ contains many page images internally, LiuXin treats it as one Digital Asset because it is stored, 
hashed, transmitted, and verified as one atomic file.

If the CBZ is unpacked and the pages are catalogued independently, LiuXin may additionally create:

```text
Composite Digital Asset
├── page-001.jpg
├── page-002.jpg
├── page-003.jpg
└── ...
```

The archived and unpacked representations are related but distinct Assets.

---

## Example 4: A physical book with digital supplements

```text
Work
└── Example Reference Book
    └── Expression
        └── Revised English text
            └── Manifestation
                └── 2019 hardback edition
                    └── Item
                        └── Particular physical copy
                            ├── Digital Asset: front-cover photograph
                            ├── Digital Asset: back-cover photograph
                            ├── Composite Digital Asset: page scans
                            └── Digital Asset: OCR text
```

The Item is the physical book. 
The linked Assets are digital representations or derivatives associated with it. 
The Item can continue to exist even if no digital Assets have yet been created.

---

# Relationship summary

| Entity | Core question answered | Contains or identifies bytes? | Can have Replicas? |
|---|---|---:|---:|
| Work | What abstract creation is this? | No | No |
| Expression | Which realisation, language, or performance? | No | No |
| Manifestation | Which edition, release, or publication? | No | No |
| Item | Which acquired or particular exemplar? | No | No |
| Asset | What digital content is associated with the Item? | Abstract category | No |
| Digital Asset | What atomic byte sequence is expected? | Yes | Yes |
| Composite Digital Asset | How are several Digital Assets assembled? | No direct byte stream | No direct Replicas |
| Replica | Where does one stored copy exist? | Represents actual stored bytes | Not applicable |
| Store | Which storage system holds the copy? | No | Holds Replicas |
| Location | Where inside the Store is the copy addressed? | No | Identifies one Replica address |
| Store Backend | How is that kind of Store accessed? | No | Implements storage operations |

---

# Principal relationships

The normal relations can be summarised as:

```text
Work
    has Expressions

Expression
    has Manifestations

Manifestation
    has Items

Item
    is linked to Assets

Asset
    is either a Digital Asset
    or a Composite Digital Asset

Composite Digital Asset
    has ordered or structured component Digital Assets

Digital Asset
    has zero or more Replicas

Replica
    exists at one Location in one Store

Store
    is accessed through one Store Backend
```

The Item–Asset and Composite–Component relationships should normally be explicit association records because they may 
carry order, role, provenance, and other contextual metadata.

---

# Terminology rules

To keep documentation and code consistent, the following conventions should be used:

1. Capitalised terms such as **Work**, **Item**, **Digital Asset**, and **Replica** refer to formal LiuXin concepts.
2. The word **file** may be used informally but should not normally name a database entity.
3. **Copy** should be qualified where ambiguity is possible:
   - bibliographic copy → Item;
   - stored copy → Replica.
4. **Physical** should normally be reserved for tangible objects such as books, disks, and tapes.
5. **Concrete stored copy** is preferable when describing a Replica.
6. **Digital Asset** refers to one atomic expected byte sequence.
7. **Composite Digital Asset** refers to an organised group of Digital Assets.
8. **Store** refers to one configured storage endpoint.
9. **Store Backend** refers to the code supporting a type of Store.
10. **Location** is opaque outside the Store Backend.
11. Different bytes mean different Digital Assets.
12. Additional copies of the same bytes mean additional Replicas.
13. Transformation creates a new Asset; replication creates a new Replica.
14. A Digital Asset may remain valid even when it has no surviving Replicas.
15. Catalogue identity and byte identity must not be conflated.

---

# Compact definition

LiuXin’s conceptual model can be summarised as follows:

> A Work is realised through an Expression, embodied in a Manifestation, and represented by a particular Item. An Item is linked to Assets. A Digital Asset identifies one atomic digital object, while a Composite Digital Asset describes an organised collection of Digital Assets. Each Digital Asset may have zero or more Replicas. A Replica records one concrete copy at a Location within a Store.

Or, in its most compact form:

```text
WEMI describes the catalogue.

Assets describe the digital content.

Replicas describe where the bytes are.
```

The likely best schéma [schema] therefore remains:

```text
Work
└── Expression
    └── Manifestation
        └── Item
            └── Asset
                ├── Digital Asset
                │   └── Replica(s)
                └── Composite Digital Asset
                    └── Component Digital Assets
                        └── Replica(s)
```

A separate formal `File` entity should only be introduced if LiuXin later develops a specific requirement that cannot be represented by the distinction between **Digital Asset** and **Replica**.
