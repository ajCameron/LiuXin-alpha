
As with everything else in LiuXin storage aims to be modular and extensible.
The aim is to be able to handle truly vast amounts of data.
With something resembling archival quality data handling.

# Storage philosophy

Storage is composed of a series of stores, all of which is managed by the StorageManager.
Stores can have different properties, and don't even need to be online.
Cold, or tape, storage is just another form of storage.

Some stores have different properties for different purposes.
e.g. you can have a store which is just dedicated to new_books storage, or a cache store.

# Storage hierarchy

## StorageManager

Top level - actually responsible for the storage in totality.

This is the only thing that you should ever actually touch. At all.
Everything else is internals.

The storage manager can
 - CRUD stores
 - store files
 - retrieve files (either into memory or into a local cache).
 - retrieving folders

Internally, it does a lot of work with hashes and so forth to protect against bit rot and the like.

The StorageManager makes no promises at all about _how_ the files are stored internally.
Internally, the files might, or might not, be in folders (indeed, many stores will not support folders at all).
They might, or might not, be compressed.
You don't get to know. Leave that all to the StorageManager.

Folders in a storage manager are entirely a virtual concept.
There is no guarantee that they actually exist before they are rendered by the StorageManager.
(This rendering might just be a copy - but, again, you don't need to know that).

## Stores

The actual storage objects.
These are handled by the StorageManager alone.

The stores are responsible for
 - storing files
 - retrieving files

# Files

When you put a request in to a store (either by hash, or id from the files table) you get a file object back.




