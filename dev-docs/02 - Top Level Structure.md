

Fundamentally, there are two BIG bits of LiuXin (and a lot of smaller bits).

These are
 - metadata (handled by the databases)
 - files (handled by storage, and some file metadata in databases)

These have separate areas of concern. In particular
 - the final arbitrator of metadata is _the database_
 - the final arbitrator of files is _whatever is on disc_

These come together in the _library_.
Which can do everything. Hopefully kinda okay.

The core has access to some number of libraries, and can do stuff with them.
Exciting stuff. Like actually finding a book for you to read.

# Caches

High performance caches of various types, for various things that could/should be stored in memory.
You might, for example, want to cache a catalog.
Or, perhaps, files and covers.

# Catalog

The raw database is of limited use.
It stores objects in the form of text, but not much more.
If you want to make a database store into something actually useful, you need a way to store and retrieve more
structured data in the form of metadata.
The catalog provides that structures.
It's intended to store and retrieve metadata from the database - in various forms.

# Core

The core orchestrates and exposes all the relevant things you might ever want to do with a LiuXin system.

When you're talking to LiuXin, you're mostly talking to the core.
It has access to one or many libraries.

# Databases

Persistent data stores.
Exposes the _database_ class - which is responsible for talking to the databases.

Read-heavy storage-facing access now also has an explicit cache backend layer.
That sits between the live database and higher-level views/interfaces.
For the current backend options and their intended semantics, see
`dev-docs/08 - Storage Cache Backends.md`.

The databases module is intended for low level object storage.
Nothing more.

# Jobs

Long-running processes - of various sorts.
Eventually will include metadata completers, downloaders, that sort of thing.

# Library

Brings storage and data together.
As a rule, the library will have access to
 - one_ish_ databases (ish, because there might be backup or mirrors the library is responsible for keeping in sync)
 - many stores (through the storage class - which is responsible for load balancing and backup - that sorta thing)

# Metadata

Tools for reading, writing and manipulating metadata.

Metadata presents a number of objects to store, present and manipulate metadata.
It contains a wide range of relevant containers - which you should be able to access through the catalog.
(Now I know that the catalog exists and can do things).

# Storage

LiuXin is, at heart, two things.
 - A metadata store
 - A file store

The job of storage is to store files and metadata somewhere persistent.
Disc, TCP servers, tape archives.
It doesn't matter. The interface should be compatible.

Storage is also responsible for
 - space optimisation
 - backup
 - protecting from delete

My view is that archival software should _never delete anything_.
At least not without considerable checks.

Exposes the _storage_ class - which is responsible for managing the backend stores.

The storage class is aware of the database - it has to be to make sure that we're backing everything up properly.
