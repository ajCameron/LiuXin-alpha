# Backend-neutral Store ingest - 2026-08-18

## Decision

Ingest now consumes the configured `StoreAPI`, not concrete storage drivers.
There are two deliberately distinct public operations:

- `LiuXin_alpha.ingest.ingest_store(...)` enumerates a source Store and copies
  selected bytes through `StorageManagerAPI.ingest_stream(...)` into a managed
  destination.
- `LiuXin_alpha.ingest.adopt_store(...)` registers bytes already present in an
  attached Store without publishing a second copy.

Both are also available on `Library`. Inputs may be Store facades, Store UUIDs,
or Store configurations. Optional extension filters use Store-provided filename
hints; no generic code parses opaque `Location.key` values. Selected entries are
`stat`ed by default to retain rich object metadata; `inspect=False` selects a
cheaper listing-only scan.

## Storage API limitations exposed and addressed

1. Driver discovery hints were lost at the configured-Store boundary.
   `FileInfo` now carries Store-level `FileHints`, and `DriverBackedStoreAPI`
   translates `DriverObjectHints` into them.
2. Canonical, credential-free driver object URIs were hidden from Store users.
   Stores now expose `location_uri(...)`, `location_from_uri(...)`, and matching
   capability flags. Encrypted wrappers intentionally expose neither underlying
   ciphertext URIs nor native metadata unless metadata forwarding is enabled.
3. `adopt_location(...)` could not describe a newly discovered Digital Asset.
   It now accepts `DigitalAssetMetadata`; retry identity includes that metadata.
4. Partial enumeration could look complete to batch callers. `StoreIngestReport`
   preserves the source Store's `EnumerationCompleteness` value.

## Backend coverage

The same ingest path is exercised with:

- native S3 as a rich writable destination;
- writable rclone as a destination;
- encrypted Stores as both source and destination;
- filesystem Stores for copy, filtering, URI provenance, and in-place adoption.

There are no backend type checks in ingest.

## Follow-up ingest hardening completed

- Store and driver reads accept opaque version preconditions. Filesystem,
  SQLite, HTTP, S3, and encrypted Stores enforce them; FTP, SquashFS command
  reads, and generic rclone reads reject unsupported pins explicitly.
- Store inventory has optional-size entries and resumable pages. S3 exposes its
  native continuation token; ingest reports a safe resume cursor and does not
  advance past a failed page. Filesystem, SQLite, and rclone enumeration stream
  rather than materialising full inventories.
- Store concurrency capabilities are public, with conservative backend
  recommendations. Ingest supports bounded ordered parallel work and refuses
  unsafe source/destination concurrency combinations.
- HTTP signed/token query parameters and rclone inline secrets are rejected
  from durable addresses. Generic provenance also fingerprints, rather than
  persists, a location key containing a recognised secret query parameter.
- S3's `liuxin-*` JSON metadata now round-trips as structured placement hints.
- Known-size encrypted writes stream ciphertext into the inner Store's private
  session without plaintext/ciphertext temp files in the wrapper. Encrypted
  reads use two pinned header reads and one contiguous ciphertext body stream.
- `ingest_identified_stream()` safely bypasses manager spooling when a source
  supplies authoritative size plus SHA-256; the destination commit still
  verifies both. `ingest_store_object()` selects that path automatically.
- `NativeImportStoreAPI` represents verified cross-Store acceleration.
  Rclone-to-rclone ingest implements it with remote staging, size/SHA-256
  verification, and fallback to ordinary streaming where native verification
  is unavailable.

## Optional advanced ingest-source protocol

`IngestSourceStoreAPI` is a structural, optional Store protocol layered above
the mandatory `StoreAPI`. It lets plugins advertise read consistency, object
delivery, inventory/object resume, authoritative digest algorithms, and rich
metadata availability. `prepare_ingest(...)` returns the guarantees and
observations available for one object; `open_prepared_ingest(...)` binds the
subsequent stream to those observations where the backend supports it.

All driver-backed Stores receive a conservative generic implementation. FTP,
rclone, S3, and SQLite refine only their backend-specific qualities. Generic
ingest consumes the protocol without backend type checks, while Stores that do
not implement it retain the existing stat/open fallback. Prepared authoritative
digests are accepted only when the source profile advertises their algorithms,
and a range resume is allowed only when both the Store and that prepared object
provide stable reads.

## Prepared reuse and object checkpoints

`StorageManagerAPI.ingest_prepared_store_object(...)` now accepts a preparation
created during discovery. The ordinary `ingest_store_object(...)` method remains
the compatibility wrapper, while the generic Store ingest pipeline passes its
existing preparation directly. Verified rclone native import remains available
through the prepared path, and plugin preparation is no longer repeated.

Object-level resume is deliberately opt-in through
`object_staging_directory`. For a prepared object with stable-range support,
ingest retains downloaded bytes in a private partial file when acquisition or
publication fails. `StoreIngestReport.object_checkpoints` exposes validated
checkpoints; passing them back as `resume_checkpoints` reopens the same prepared
source version at the retained byte offset. The partial file's size and SHA-256
are checked before reuse, a changed source version is rejected, and successful
commit removes the partial file. Stores without stable ranges continue through
the normal direct path. This keeps destination writes commit-based: partial
publication is never presented as a resumable Store write session.
