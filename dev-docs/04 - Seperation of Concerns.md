
What does each bit of the system do?



Stores



Store files.






## Storage, Library, and Database Responsibilities

At the top level, LiuXin separates concerns between persistent state, physical file handling, and cross-system orchestration.

### Database

The database is the source of truth for stored state and policy. It records what the system believes should exist, what currently exists, and what relationships or constraints apply.

The database is responsible for:

- bibliographic and structural metadata
- file and store records
- storage-related state such as:
  - which files exist
  - which stores hold which files
  - checksums, sizes, and observed properties
  - desired replica counts or placement constraints
  - verification / repair / migration state
- durable job-relevant state where needed for recovery and reconciliation

The database is not responsible for performing storage actions itself. It records state; other layers act on that state.

### Storage

The storage layer is responsible for making physical reality match the storage-related state recorded in the database.

In practice, storage handles:

- storing and retrieving files
- resolving which concrete store can satisfy a read
- writing bytes to a store
- checking whether files exist on stores
- verifying integrity at the file / store level
- moving or copying files between stores
- maintaining replicas
- performing repair / reconciliation / healing work
- exposing store health and capability information

Storage should be database-aware in the narrow sense that it reads and updates storage state recorded in the database. It should not become a general consumer of the full bibliographic model.

Storage should know about storage concepts such as:

- files
- stores
- file-store links
- checksums
- placement state
- replica state
- verification state
- transfer / repair state

Storage should not own higher-level library semantics such as:

- whether two manifestations are “really the same book”
- work / expression modelling
- metadata merge policy
- user-facing presentation concerns

### Library

The library layer is the high-level orchestration layer. It brings together database, storage, metadata, cache, and interface concerns into meaningful workflows.

Library is responsible for operations such as:

- ingesting a new file and creating the right records
- reconciling an import against existing library entities
- coordinating metadata and storage updates together
- exposing higher-level workflows to interfaces and automation
- deciding when a cross-cutting operation spans multiple subsystems

A useful rule of thumb is:

- database records truth
- storage enforces and reconciles physical truth
- library coordinates multi-domain intent

### Recommended Internal Split Inside Storage

To avoid turning storage into a monolith, it should usually be split into two levels:

#### Store drivers / backend plugins

These handle the mechanics of one concrete storage backend, such as:

- local filesystem
- remote HTTP
- rclone-backed remotes
- squashfs archives
- tape-like or write-once media
- other future backends

They should ideally remain as database-agnostic as possible. Their job is to expose capabilities and perform backend-specific file operations.

#### Storage manager / storage service

This is the database-aware layer inside storage. It is responsible for:

- reading storage state and policy from the database
- deciding what storage action is required
- selecting appropriate stores
- invoking concrete store drivers
- updating durable storage state after operations complete or fail

This keeps low-level storage plugins simple while still allowing storage as a whole to participate in durable, stateful workflows.

### Anti-Responsibilities

To keep boundaries clean:

#### Database should not:

- implement physical storage operations
- contain backend-specific file transfer logic
- make policy decisions by itself without an orchestrating caller

#### Storage should not:

- become the owner of bibliographic identity
- decide semantic merges of library entities
- absorb general library business logic unrelated to physical storage

#### Library should not:

- reimplement backend-specific storage mechanics
- bypass storage for normal file operations
- become the place where low-level byte movement logic accumulates

### Replication, Backup, and Dedupe

These concerns are related, but they do not all belong to exactly the same layer.

#### Replication

Replication is a storage responsibility.

Replication is about ensuring that the required number of physical copies of a file exist across the available stores, and that those copies remain valid and retrievable.

Storage owns:

- creating replicas
- verifying replicas
- healing missing or damaged replicas
- tracking replica placement and status in the database

Library may still influence replication policy at a higher level, for example by deciding that certain classes of content need more copies than others. But the execution and state reconciliation belong to storage.

#### Backup

Backup is primarily a storage responsibility.

Backup is about producing and maintaining recoverable physical copies according to defined retention or placement policy.

Storage owns:

- running backup copy / snapshot / export jobs
- verifying backup readability where possible
- tracking backup-related state in the database
- restoring files or stores from backup media where supported

Library may define policy inputs such as:

- what should be backed up
- how aggressively
- to which class of stores
- with what retention expectations

But the mechanics of backup are storage concerns.

#### Dedupe

Dedupe is a split responsibility and should be treated as two different problems.

##### Physical / content dedupe

Physical dedupe is a storage responsibility.

This includes cases where:

- two files are byte-identical
- one blob can be stored once and referenced multiple times
- hardlink / reflink / content-addressed strategies are possible
- physical space usage should be reduced without changing higher-level library semantics

This is about bytes, storage layout, and retrieval mechanics, so it belongs in storage.

##### Semantic / bibliographic dedupe

Semantic dedupe is a library responsibility.

This includes cases where:

- two imports appear to be the same manifestation
- two records appear to represent the same expression or work
- metadata from multiple sources should be merged
- duplicate-looking library entities need human or policy-driven resolution

This is about meaning, identity, and library modelling, so it belongs in library.

### Ownership Summary

- replication: storage
- backup execution: storage
- backup policy inputs: usually library and/or database configuration
- physical/content dedupe: storage
- semantic/bibliographic dedupe: library

A good shorthand is:

- storage decides how many physical copies exist, where they live, and whether identical bytes can be shared
- library decides what counts as “the same thing” at the bibliographic or domain level

## Storage internal split

Storage now has its own strict internal separation of concerns:

- `StorageManager` orchestrates many configured stores
- `StoreContainer` wraps one configured store plus optional DB-facing state
- `StorePlugin` performs raw physical-media operations
- `Location` is the concrete file/location handle

This split matters because backend code is reusable outside the full managed-storage stack, while manager code is intentionally database-aware and orchestration-heavy.

A useful smell test is:

- if the code is deciding *which* store should do something, it belongs in the manager
- if the code is holding config / status for *one* store, it belongs in the container
- if the code is performing bytes-on-media work, it belongs in the plugin
- if the code is acting on one concrete path/key within one store, it belongs on `Location`
