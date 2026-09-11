"""
Retain the legacy lazy disk-thumbnail cache with group-scoped keys and a shared approximate LRU byte budget.

Payloads are stored unchanged; this module does not decode or resize images.
Index loading reconstructs metadata from filenames, trusts declared sizes, and
may delete mismatched or over-budget entries. Cache paths/group names are not
confined or sanitized, and the order file is unpickled: use only trusted local
cache directories, never untrusted cache artifacts.

Python 3 limitations are retained here, not hidden behind stronger documentation:
as_unicode is not defined by this module, so many I/O-error logging paths raise
NameError; deferred invalidation reads bytes but partitions with text, losing those
records; and size invalidation mutates a live OrderedDict iterator and can raise
RuntimeError after partial removal. This is compatibility code, not an atomic,
cross-process-safe, or failure-transparent cache implementation.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import errno
import os
import pickle as cPickle
import sys
from collections import OrderedDict
from threading import Lock

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems, itervalues

from LiuXin_alpha.constants import cache_dir
from LiuXin_alpha.databases.utils import CacheError, Entry
from LiuXin_alpha.utils.logging import prints


class ThumbnailCache(object):

    """
    Cache opaque thumbnail bytes on disk and lazily index entries across all stored groups.

    Public operations use one per-instance non-reentrant lock, except the early
    insert size check and identity hash. Other processes/instances are not locked
    out. Count and byte accounting cover every indexed group, while lookup and
    invalidation use the selected group. Reads promote entries in memory; shutdown
    writes hashed recency order without marking this object closed. See the module
    documentation for retained error, invalidation, and trust limitations.

    Example:
        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory() as directory:
        ...     cache = ThumbnailCache(location=directory)
        ...     cache.insert(7, 1.0, b"thumbnail")
        ...     cache[7]
        (b'thumbnail', 1.0)
    """

    def __init__(
        self,
        max_size=1024,  # The maximum disk space in MB
        name="thumbnail-cache",  # The name of this cache (should be unique in location)
        thumbnail_size=(100, 100),  # The size of the thumbnails, can be changed
        location=None,  # The location for this cache, if None cache_dir() is used
        test_mode=False,  # Used for testing
        min_disk_cache=0,
    ):  # If the size is set less than or equal to this value, the cache is disabled.
        """
        Configure a cache without loading its index or creating its cache directory.

        location uses a truthy-value fallback to cache_dir(), then name is joined
        without confinement checks. max_size at/below min_disk_cache becomes zero
        before MiB-to-byte conversion; unlike set_size, no additional negative clamp
        is applied. A zero budget still permits zero-byte insertions. Thumbnail size
        is retained as supplied, and test mode replaces this instance's logger with
        fail_on_error but does not repair failures before logging is reached.

        Example:
            >>> cache = ThumbnailCache(location="test-cache-root", max_size=2)
            >>> cache.max_size, hasattr(cache, "items")
            (2097152, False)


        :param max_size: Requested total byte budget in MiB, converted with int after multiplication by 1024 squared.
        :param name: Cache directory component joined to the selected location; expected to be a trusted unique name.
        :param thumbnail_size: Stored dimension pair used for filenames and exact index compatibility comparisons.
        :param location: Parent directory, or a falsey value to use the configured cache directory.
        :param test_mode: Truthy to make direct log calls raise CacheError through fail_on_error.
        :param min_disk_cache: MiB threshold at/below which the requested budget becomes zero.
        :return: None after initializing configuration, default group, pending-size flag, and per-instance lock.
        """
        self.location = os.path.join(location or cache_dir(), name)
        if max_size <= min_disk_cache:
            max_size = 0
        self.max_size = int(max_size * (1024**2))
        self.group_id = "group"
        self.thumbnail_size = thumbnail_size
        self.size_changed = False
        self.lock = Lock()
        self.min_disk_cache = min_disk_cache
        if test_mode:
            self.log = self.fail_on_error

    def log(self, *args, **kwargs):
        """
        Send cache diagnostics through prints with stderr forced as the destination.

        Example:
            >>> cache.log("cache diagnostic")  # doctest: +SKIP


        :param args: Positional message arguments forwarded to prints unchanged.
        :param kwargs: Print options forwarded after replacing any file option with sys.stderr.
        :return: None after printing; formatting/output errors propagate.
        """
        kwargs["file"] = sys.stderr
        prints(*args, **kwargs)

    def fail_on_error(self, *args, **kwargs):
        """
        Join positional text messages and raise CacheError for test-mode logging.

        Keyword options are ignored. A non-string positional argument can raise
        TypeError during joining before the intended cache error is constructed.

        Example:
            >>> cache = ThumbnailCache(location="test-cache-root", test_mode=True)
            >>> cache.log("cache", "failed")
            Traceback (most recent call last):
            ...
            LiuXin_alpha.databases.utils.CacheError: cache failed


        :param args: String message fragments joined with one space.
        :param kwargs: Compatibility logging options, accepted but unused.
        :return: No value; message construction is followed by an exception.
        :raises CacheError: Always after successful joining of the positional strings.
        """
        msg = " ".join(args)
        raise CacheError(msg)

    def _do_delete(self, path):
        """
        Remove one cache file, attempting legacy diagnostic formatting for any EnvironmentError.

        Missing files are not exempt. In this module as shipped, the undefined
        as_unicode name causes the diagnostic path to raise NameError before log
        is called. This helper takes no lock and does not alter index accounting.

        Example:
            >>> cache._do_delete(cached_path)  # doctest: +SKIP


        :param path: Exact filesystem path passed to os.remove, without confinement or existence checks.
        :return: None after successful removal or a diagnostic path that returns normally.
        :raises NameError: On an I/O removal failure when as_unicode remains undefined.
        """
        try:
            os.remove(path)
        except EnvironmentError as err:
            self.log("Failed to delete cached thumbnail file:", as_unicode(err))

    def _load_index(self):
        """
        Scan the cache lazily, rebuild its index from filenames, and prune incompatible or over-budget entries.

        Create the cache root if needed and scan exactly group/shard/entry depth.
        Unreadable listdir calls yield no children. Filenames encode book ID,
        timestamp, declared byte size, and dimensions; malformed names are skipped
        without deletion. Payload type, actual size, and shard correctness are not
        checked. Duplicate logical keys can collapse in the OrderedDict while
        total_size still includes every accepted filename's declared size.

        Saved hash-order values determine initial ordering with discovery-order
        ties; hashes are not stable across all interpreter processes. An invalidation
        journal is removed before its lines are parsed. On Python 3, those byte lines
        fail the text partition in record and produce None rather than invalidation
        keys. Mismatched sizes and budget excess can delete files during loading.
        This helper takes no lock and provides no rollback on a partial failure.

        Example:
            >>> cache._load_index()  # doctest: +SKIP


        :return: None after replacing index/accounting state and attempting budget pruning.
        :raises NameError: If an I/O-error path evaluates the undefined as_unicode helper.
        """
        try:
            os.makedirs(self.location)
        except OSError as err:
            if err.errno != errno.EEXIST:
                self.log("Failed to make thumbnail cache dir:", as_unicode(err))
        self.total_size = 0
        self.items = OrderedDict()
        order = self._read_order()

        def listdir(*args):
            """
            List joined directory components, substituting an empty tuple for EnvironmentError.

            Example:
                >>> names = listdir(cache.location, group)  # doctest: +SKIP


            :param args: Path components joined with os.path.join before directory listing.
            :return: os.listdir name list or an empty tuple when listing raises EnvironmentError.
            """
            try:
                return os.listdir(os.path.join(*args))
            except EnvironmentError:
                return ()  # not a directory or no permission or whatever

        entries = (
            "/".join((parent, subdir, entry))
            for parent in listdir(self.location)
            for subdir in listdir(self.location, parent)
            for entry in listdir(self.location, parent, subdir)
        )

        invalidate = set()
        try:
            with open(os.path.join(self.location, "invalidate"), "rb") as f:
                raw = f.read()
        except EnvironmentError as err:
            if getattr(err, "errno", None) != errno.ENOENT:
                self.log("Failed to read thumbnail invalidate data:", as_unicode(err))
        else:
            try:
                os.remove(os.path.join(self.location, "invalidate"))
            except EnvironmentError as err:
                self.log("Failed to remove thumbnail invalidate data:", as_unicode(err))
            else:

                def record(line):
                    """
                    Parse a legacy text journal line into a group/book key, returning None for any Exception.

                    The caller currently supplies bytes from a binary read, which
                    cannot partition on the text separator and therefore returns
                    None for each line under Python 3.

                    Example:
                        >>> key = record("group 7")  # doctest: +SKIP


                    :param line: Intended group-space-integer text line; actual binary input follows the failure path.
                    :return: Group-text/integer-ID pair for valid text, otherwise None.
                    """
                    try:
                        uuid, book_id = line.partition(" ")[0::2]
                        book_id = int(book_id)
                        return uuid, book_id
                    except Exception:
                        return None

                invalidate = {record(x) for x in raw.splitlines()}
        items = []
        try:
            for entry in entries:
                try:
                    uuid, name = entry.split("/")[0::2]
                    book_id, timestamp, size, thumbnail_size = name.split("-")
                    book_id, timestamp, size = int(book_id), float(timestamp), int(size)
                    thumbnail_size = tuple(map(int, thumbnail_size.partition("x")[0::2]))
                except (ValueError, TypeError, IndexError, KeyError, AttributeError):
                    continue
                key = (uuid, book_id)
                path = os.path.join(self.location, entry)
                if self.thumbnail_size == thumbnail_size and key not in invalidate:
                    items.append((key, Entry(path, size, timestamp, thumbnail_size)))
                    self.total_size += size
                else:
                    self._do_delete(path)
        except EnvironmentError as err:
            self.log("Failed to read thumbnail cache dir:", as_unicode(err))

        self.items = OrderedDict(sorted(items, key=lambda x: order.get(hash(x[0]), 0)))
        self._apply_size()

    def _invalidate_sizes(self):
        """
        Remove indexed entries with mismatched dimensions when a size change is pending.

        Removal consumes a generator over the live OrderedDict, so deleting an
        entry can raise RuntimeError on its next advancement. Earlier removals
        persist and size_changed remains true if the loop fails. Removing the last
        remaining entry can instead exhaust the iterator normally. The flag clears
        on successful exhaustion, including no mismatches. This helper assumes an
        initialized index and lock.

        Example:
            >>> cache._invalidate_sizes()  # doctest: +SKIP


        :return: None after successful pending cleanup or when no size change is pending.
        :raises RuntimeError: If removal changes the OrderedDict during its active iteration.
        """
        if self.size_changed:
            size = self.thumbnail_size
            remove = (key for key, entry in iteritems(self.items) if size != entry.thumbnail_size)
            for key in remove:
                self._remove(key)
            self.size_changed = False

    def _remove(self, key):
        """
        Pop an indexed entry, delete its file, then subtract its declared byte size.

        Unknown keys are ignored. A deletion/logging failure occurs after index
        removal but before accounting changes; no rollback restores either state.
        This helper does not acquire the instance lock.

        Example:
            >>> cache._remove(("group", 7))  # doctest: +SKIP


        :param key: Exact group/book key used for OrderedDict lookup.
        :return: None for an absent key or after successful deletion and accounting update.
        """
        entry = self.items.pop(key, None)
        if entry is not None:
            self._do_delete(entry.path)
            self.total_size -= entry.size

    def _apply_size(self):
        """
        Evict oldest indexed entries until declared byte usage fits the budget or the index is empty.

        Eviction spans groups and removes each entry before deleting its file and
        subtracting size. Failure can leave partial eviction or stale accounting.
        The helper does not load the index, recompute actual sizes, or take a lock.

        Example:
            >>> cache._apply_size()  # doctest: +SKIP


        :return: None after budget pruning completes, including an empty index with inconsistent residual accounting.
        """
        while self.total_size > self.max_size and self.items:
            entry = self.items.popitem(last=False)[1]
            self._do_delete(entry.path)
            self.total_size -= entry.size

    def _write_order(self):
        """
        Pickle the current ordered key hashes directly to the order file if the index has been initialized.

        Writing is neither atomic nor fsynced and does not initialize the index or
        acquire a lock. Python hash randomization can limit cross-process usefulness.
        I/O failures enter the unresolved as_unicode logging path.

        Example:
            >>> cache._write_order()  # doctest: +SKIP


        :return: None after writing or when no items attribute exists.
        """
        if hasattr(self, "items"):
            try:
                with open(os.path.join(self.location, "order"), "wb") as f:
                    f.write(cPickle.dumps(tuple(map(hash, self.items)), -1))
            except EnvironmentError as err:
                self.log("Failed to save thumbnail cache order:", as_unicode(err))

    def _read_order(self):
        """
        Unpickle trusted order data into hash-to-position entries, or use an empty mapping for a missing file.

        The entire file is read without a byte cap or safe-unpickling restriction.
        Later duplicate hashes win. Other exceptions are caught for logging, but
        the current undefined as_unicode helper can turn them into NameError.

        Example:
            >>> order = cache._read_order()  # doctest: +SKIP


        :return: Hash-to-last-position dict, normally empty when the order file is absent.
        :raises NameError: On non-ENOENT error logging while as_unicode is undefined.
        """
        order = {}
        try:
            with open(os.path.join(self.location, "order"), "rb") as f:
                order = cPickle.loads(f.read())
                order = {k: i for i, k in enumerate(order)}
        except Exception as err:
            if getattr(err, "errno", None) != errno.ENOENT:
                self.log("Failed to load thumbnail cache order:", as_unicode(err))
        return order

    def shutdown(self):
        """
        Persist in-memory recency order under the instance lock without marking the cache closed.

        An untouched cache performs no write. Later operations and repeated shutdown
        calls remain possible; this does not prune pending sizes or fsync payloads.

        Example:
            >>> cache.shutdown()  # doctest: +SKIP


        :return: None after the order-write attempt completes normally.
        """
        with self.lock:
            self._write_order()

    def set_group_id(self, group_id):
        """
        Change the namespace used by later lookups/inserts/invalidations without loading or clearing the index.

        The value is retained unchanged as a key component and later stringified in
        paths. It must be trusted: no path-component validation or confinement occurs.
        Count, byte budget, and recency ordering remain shared across all groups.

        Example:
            >>> cache = ThumbnailCache(location="test-cache-root")
            >>> cache.set_group_id("library-a")
            >>> cache.group_id
            'library-a'


        :param group_id: Caller-selected group key, expected to be a stable safe string.
        :return: None after assigning the group value under the lock.
        """
        with self.lock:
            self.group_id = group_id

    def set_thumbnail_size(self, width, height):
        """
        Store a dimension tuple and mark size cleanup pending, even if the dimensions did not change.

        Values are not validated or clamped. Loaded entries are not removed until
        lookup or insertion invokes the retained size-invalidation helper.

        Example:
            >>> cache = ThumbnailCache(location="test-cache-root")
            >>> cache.set_thumbnail_size(60, 80)
            >>> cache.thumbnail_size, cache.size_changed
            ((60, 80), True)


        :param width: First dimension retained unchanged for exact comparisons and filename formatting.
        :param height: Second dimension retained unchanged for exact comparisons and filename formatting.
        :return: None after updating dimensions and setting the pending flag under the lock.
        """
        with self.lock:
            self.thumbnail_size = (width, height)
            self.size_changed = True

    def insert(self, book_id, timestamp, data):
        """
        Store opaque bytes for the selected group/book, updating recency and pruning the shared budget.

        Payloads larger than the current budget return before locking or loading.
        Otherwise lazily load the index and process pending size changes. The path
        uses group/book-mod-100/book-ID, timestamp rounded to two decimals with .00
        removed, declared payload length, and dimensions. Data is written directly,
        not resized, validated as an image, or atomically staged.

        An existing logical entry is popped and unaccounted before the write, but
        an old file at a different filename is not removed. It can reappear on reload.
        A failed write can leave partial files or a lost index entry; recovery tries
        directory creation only when the parent does not exist. Error logging and
        pending-size cleanup retain the module's documented Python 3 limitations.

        Example:
            >>> cache.insert(7, 123.456, b"thumbnail")  # doctest: +SKIP


        :param book_id: Group-local key used in modulo/integer filename formatting without range validation.
        :param timestamp: Numeric freshness marker stored as supplied in memory and rounded in the disk filename.
        :param data: Sized bytes-like payload accepted by binary file.write, stored without conversion or image checks.
        :return: None after insertion/pruning or a rejected oversized payload; failures do not roll back earlier mutations.
        """
        if self.max_size < len(data):
            return
        with self.lock:
            if not hasattr(self, "total_size"):
                self._load_index()
            self._invalidate_sizes()
            ts = ("%.2f" % timestamp).replace(".00", "")
            path = "%s%s%s%s%d-%s-%d-%dx%d" % (
                self.group_id,
                os.sep,
                book_id % 100,
                os.sep,
                book_id,
                ts,
                len(data),
                self.thumbnail_size[0],
                self.thumbnail_size[1],
            )
            path = os.path.join(self.location, path)
            key = (self.group_id, book_id)
            e = self.items.pop(key, None)
            self.total_size -= getattr(e, "size", 0)
            try:
                with open(path, "wb") as f:
                    f.write(data)
            except EnvironmentError as err:
                d = os.path.dirname(path)
                if not os.path.exists(d):
                    try:
                        os.makedirs(d)
                        with open(path, "wb") as f:
                            f.write(data)
                    except EnvironmentError as err:
                        self.log("Failed to write cached thumbnail:", path, as_unicode(err))
                        return self._apply_size()
                else:
                    self.log("Failed to write cached thumbnail:", path, as_unicode(err))
                    return self._apply_size()
            self.items[key] = Entry(path, len(data), timestamp, self.thumbnail_size)
            self.total_size += len(data)
            self._apply_size()

    def __len__(self):
        """
        Count all indexed group/book entries, loading and pruning the disk index first if needed.

        A loaded index is not refreshed from disk and pending size changes are not
        applied here, so the count need not describe readable current-size files.

        Example:
            >>> count = len(cache)  # doctest: +SKIP


        :return: Total indexed key count across all groups, not just the selected group.
        """
        with self.lock:
            try:
                return len(self.items)
            except AttributeError:
                self._load_index()
                return len(self.items)

    def __contains__(self, book_id):
        """
        Test selected-group key membership, loading/pruning the index if it is absent.

        Membership does not open the payload, promote recency, check timestamps,
        or apply a pending size change to an already loaded index.

        Example:
            >>> present = 7 in cache  # doctest: +SKIP


        :param book_id: Book key paired unchanged with the current group.
        :return: Whether that logical key is indexed, not proof that its payload file is readable.
        """
        with self.lock:
            try:
                return (self.group_id, book_id) in self.items
            except AttributeError:
                self._load_index()
                return (self.group_id, book_id) in self.items

    def __getitem__(self, book_id):
        """
        Read selected-group bytes and timestamp, promoting the indexed entry before opening its payload file.

        Load/prune lazily and apply pending size cleanup first. Missing keys return
        (None, None), not KeyError. A remaining wrong-size entry is removed. A read
        failure attempts logging and otherwise returns the missing pair while
        retaining the promoted entry and accounting; with undefined as_unicode,
        that error path instead raises NameError. Freshness is left to the caller.

        Example:
            >>> content, timestamp = cache[7]  # doctest: +SKIP


        :param book_id: Exact group-local key paired with the current group for lookup.
        :return: Full file bytes and recorded timestamp, or (None, None) for a missing/rejected entry.
        """
        with self.lock:
            if not hasattr(self, "total_size"):
                self._load_index()
            self._invalidate_sizes()
            key = (self.group_id, book_id)
            entry = self.items.pop(key, None)
            if entry is None:
                return None, None
            if entry.thumbnail_size != self.thumbnail_size:
                try:
                    os.remove(entry.path)
                except EnvironmentError as err:
                    if getattr(err, "errno", None) != errno.ENOENT:
                        self.log(
                            "Failed to remove cached thumbnail:",
                            entry.path,
                            as_unicode(err),
                        )
                self.total_size -= entry.size
                return None, None
            self.items[key] = entry
            try:
                with open(entry.path, "rb") as f:
                    data = f.read()
            except EnvironmentError as err:
                self.log("Failed to read cached thumbnail:", entry.path, as_unicode(err))
                return None, None
            return data, entry.timestamp

    def invalidate(self, book_ids):
        """
        Remove selected-group keys immediately when loaded, otherwise append a deferred invalidation journal if the root exists.

        Immediate removals can leave partial state on failure. Deferred records use
        ASCII group-space-ID lines but append without a trailing separator between
        calls. The Python 3 loader currently loses all binary journal lines during
        text parsing. A nonexistent cache root results in no work and no iteration
        of book_ids. Non-ASCII encoding failures are not caught as EnvironmentError.

        Example:
            >>> cache.invalidate([7, 8])  # doctest: +SKIP


        :param book_ids: Iterable of current-group keys, integer-formatted when writing deferred records.
        :return: None after immediate removals, deferred append, or a no-root no-op.
        """
        with self.lock:
            if hasattr(self, "total_size"):
                for book_id in book_ids:
                    self._remove((self.group_id, book_id))
            elif os.path.exists(self.location):
                try:
                    raw = "\n".join("%s %d" % (self.group_id, book_id) for book_id in book_ids)
                    with open(os.path.join(self.location, "invalidate"), "ab") as f:
                        f.write(raw.encode("ascii"))
                except EnvironmentError as err:
                    self.log("Failed to write invalidate thumbnail record:", as_unicode(err))

    def current_size(self):
        """
        Return declared indexed byte usage across all groups, loading and pruning the index if needed.

        Usage is not recomputed from file sizes and may remain inconsistent after
        duplicate-key loading or partial failures. Pending size changes are not
        processed by this method on an already loaded index.

        Example:
            >>> size_in_bytes = cache.current_size()  # doctest: +SKIP


        :return: Current declared-byte accounting value, not a fresh filesystem usage measurement.
        """
        with self.lock:
            if not hasattr(self, "total_size"):
                self._load_index()
            return self.total_size

    def empty(self):
        """
        Remove the order file and every currently indexed payload, then clear accounting if deletion completes.

        Order-file removal errors are ignored. An unloaded index is loaded/pruned
        first. Payload deletion failures can leave partial removals and unchanged
        final accounting. Orphan/unparseable files, deferred journals, and directory
        structure are not comprehensively removed; this is not recursive directory
        erasure. All indexed groups are affected, not just the selected one.

        Example:
            >>> cache.empty()  # doctest: +SKIP


        :return: None after successful indexed-payload removal and resetting the in-memory index/byte count.
        """
        with self.lock:
            try:
                os.remove(os.path.join(self.location, "order"))
            except EnvironmentError:
                pass
            if not hasattr(self, "total_size"):
                self._load_index()
            for entry in itervalues(self.items):
                self._do_delete(entry.path)
            self.total_size = 0
            self.items = OrderedDict()

    def __hash__(self):
        """
        Return object identity as a process-local hash independent of mutable cache configuration.

        Example:
            >>> cache = ThumbnailCache(location="test-cache-root")
            >>> hash(cache) == id(cache)
            True


        :return: This instance's id, without loading the index or acquiring its lock.
        """
        return id(self)

    def set_size(self, size_in_mb):
        """
        Update the shared byte budget and immediately evict oldest loaded entries when necessary.

        Requests at/below min_disk_cache become zero and a separate clamp prevents
        negative budgets, unlike construction's threshold-only handling. An unloaded
        cache stays unloaded; pending thumbnail-size invalidation is not performed.

        Example:
            >>> cache = ThumbnailCache(location="test-cache-root")
            >>> cache.set_size(2)
            >>> cache.max_size
            2097152


        :param size_in_mb: New total cache budget in MiB, converted to bytes by int after thresholding/clamping.
        :return: None after updating the budget and any successful loaded-index eviction.
        """
        if size_in_mb <= self.min_disk_cache:
            size_in_mb = 0
        size_in_mb = max(0, size_in_mb)
        with self.lock:
            self.max_size = int(size_in_mb * (1024**2))
            if hasattr(self, "total_size"):
                self._apply_size()
