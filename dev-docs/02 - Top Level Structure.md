

Fundamentally, there are two BIG bits of LiuXin (and a lot of smaller bits).

These are
 - metadata (handled by the databases)
 - files (handled by storage, and some file metadata in databases)

These have separate areas of concern. In particular
 - the final arbitrator of metadata is _the database_
 - the final arbitrator of files is _whatever is on disc_

These come together in the _library_.
Which can do everything.

The core has access to some number of libraries, and can do stuff with them.
Exciting stuff. Like actually finding a book for you to read.

# Core

The core orchestrates and exposes all the relevant things you might ever want to do with a LiuXin system.

When you're talking to LiuXin, you're mostly talking to the core.
It has access to one or many libraries.

# Databases

Persistent data stores.
Exposes the _database_ class - which is responsible for talking to the databases.

# Jobs

Long-running processes - of various sorts.
Eventually will include metadata completers, downloaders, that sort of thing.

# library

Brings storage and data together.
As a rule, the library will have access to
 - one_ish_ databases (ish, because there might be backup or mirrors the library is responsible for keeping in sync)
 - many stores (through the storage class - which is responsible for load balancing and backup - that sorta thing)

# Metadata





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
