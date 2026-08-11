
Inside storage, refer to a concrete stored object with an immutable `Location`:
one configured Store reference plus one opaque Store-owned key. Persist that
value on Replicas and workflows. URLs are external representations and should
be decoded at an API boundary rather than treated as a second internal address
model.

# API top level

The storage system has, very explicitly, three different parts.

## StorageBackendAPI

The Storage Backend exists to actually interact with "physical" files and objects.
It's the lowest level of the stack, intended to interact with actual storage devices.

These are split from the StoreAPi so that the basic object can be reused elsewhere.
This enables an expansion of capability - the backends can be reused to provide capabilities elsewhere in the system.

## StoreAPI

These are the actual, LiuXin aware stores.
They are characterized by a single row on the database.

## StorageManagerAPI

The storage manager is the top level of the pyramid.
The aim is that you should only interact with this part of the system.
