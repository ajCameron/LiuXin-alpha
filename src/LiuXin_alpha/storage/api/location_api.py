
"""Path-like API for a location inside a storage plugin.

Examples:
    Construct locations through a plugin so they remain store-relative::

        location = plugin.location("authors", "book.epub")
        payload = location.read_bytes()
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import threading
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass

from os import PathLike
from typing import (
    TypeAlias,
    Any,
    Iterator,
    Self,
    AsyncIterator,
    overload,
    TextIO,
    BinaryIO,
    IO,
    cast,
    Callable,
    TypeVar,
    TYPE_CHECKING)

from LiuXin_alpha.storage.api.modes_api import OpenTextMode, OpenBinaryMode, AsyncTextFile, AsyncBinaryFile
from LiuXin_alpha.storage.single_file import SingleFileStatus

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.store_plugin_api import StorePluginAPI

T = TypeVar("T")

StrOrBytesPath: TypeAlias = str | bytes | PathLike[str] | PathLike[bytes]
FileDescriptorOrPath: TypeAlias = int | str | bytes | PathLike[str] | PathLike[bytes]


@dataclass(frozen=True, slots=True)
class LocationCapabilities:
    """Advertised behaviour surface for one Location object.

    This is deliberately more granular than a single read-only flag. Tests and
    higher layers can inspect what the location *claims* to allow, then assert
    either success or a loud PermissionError.

    Examples:
        Advertise a readable but immutable backend::

            capabilities = LocationCapabilities(
                can_open_write=False, can_open_append=False,
                can_mkdir=False, can_touch=False, can_unlink=False,
                can_rmdir=False, can_rename=False, can_replace=False,
            )
    """

    can_stat: bool = True
    can_iterdir: bool = True
    can_glob: bool = True
    can_open_read: bool = True
    can_open_write: bool = True
    can_open_append: bool = True
    can_mkdir: bool = True
    can_touch: bool = True
    can_unlink: bool = True
    can_rmdir: bool = True
    can_rename: bool = True
    can_replace: bool = True

    @property
    def supports_mutation(self) -> bool:
        """Return whether any advertised operation can mutate storage.

        Examples:
            Disable edit controls for an immutable location::

                editable = location.location_capabilities.supports_mutation
        """
        return any((
            self.can_open_write,
            self.can_open_append,
            self.can_mkdir,
            self.can_touch,
            self.can_unlink,
            self.can_rmdir,
            self.can_rename,
            self.can_replace,
        ))

    @property
    def read_only(self) -> bool:
        """Return whether no mutating operation is advertised.

        Examples:
            Check a location before presenting a delete action::

                if location.location_capabilities.read_only:
                    hide_delete_button()
        """
        return not self.supports_mutation


READ_WRITE_LOCATION_CAPABILITIES = LocationCapabilities()
READ_ONLY_LOCATION_CAPABILITIES = LocationCapabilities(
    can_open_write=False,
    can_open_append=False,
    can_mkdir=False,
    can_touch=False,
    can_unlink=False,
    can_rmdir=False,
    can_rename=False,
    can_replace=False,
)


# Todo: Want relative and absolute? Have relative.
class StoreLocationMixinAPI(ABC):
    """
    ABC mixin for a Path-like object backed by a "store" (local pack, S3, HTTP, etc.).

    An API with extra steps, so an ABC.

    Intended usage:
        class MyStorePath(StorePathMixin, pathlib.PurePosixPath): ...

    Intended to have a very similar interface to

    Examples:
        Treat a location much like a store-relative ``pathlib`` path::

            cover = location.parent / "cover.jpg"
            assert cover.name == "cover.jpg"
    """

    _tokens: list[str]

    _store: "StorePluginAPI"

    def __init__(self, *args: str, store: "StorePluginAPI") -> None:
        """
        Startup the class - including any tokens for the location.

        :param args:

        Examples:
            Concrete locations receive path tokens and their owning store::

                location = ConcreteLocation("authors", "book.epub", store=plugin)
        """
        # Tokenize like pathlib: allow users to pass 'a/b' or ('a','b') etc.
        # Store Locations are always *relative* to the store root; absolute paths are refused.
        tokens: list[str] = []
        for ar in args:
            if ar is None:
                continue
            s = str(ar).replace('\\', '/')
            if s.startswith('/'):
                raise ValueError('Location arguments must be store-relative (no leading /).')
            for seg in s.split('/'):
                if seg in ('', '.'):
                    continue
                if seg == '..':
                    raise ValueError("Location cannot contain '..' segments (store escape risk).")
                tokens.append(seg)

        self._tokens = tokens
        self._store = store

    # ---- Core backend plumbing ----

    @property
    def store(self) -> StorePluginAPI:
        """Return the owning backend handle (client/session/repository).

        Examples:
            Compare ownership before combining locations::

                same_store = first.store is second.store
        """
        return self._store

    @store.setter
    def store(self, store: StorePluginAPI) -> None:
        """
        Refuses to update the store.

        Every location is bound to a store - you cannot change this once it's set.
        :param store:
        :return:

        Examples:
            Construct a new location for another store instead of rebinding::

                other_location = other_store.location(*location.parts)
        """
        raise AttributeError("You cannot change the store.")

    @abstractmethod
    def as_store_key(self) -> str:
        """Return the canonical key used by the backend.

        Examples:
            Persist a backend-relative identifier::

                storage_key = location.as_store_key()
        """


    # ---- Path-like semantics (PurePosix-ish) ----

    def _pure(self) -> pathlib.PurePosixPath:
        """Return the internal pure, store-relative POSIX path view.

        Examples:
            Public callers normally use ``parts`` or ``as_posix`` instead::

                portable_path = location.as_posix()
        """
        return pathlib.PurePosixPath(*self._tokens)

    # ---- pathlib-esque structural fields (store-relative) ----

    @property
    def drive(self) -> str:
        """
        Always empty: store-relative Locations have no drive.

        Preserved for compatibility with pathlib.Path.

        Examples:
            Store-relative locations never expose a drive::

                assert location.drive == ""
        """
        return ""

    @property
    def root(self) -> str:
        """
        Always empty: store-relative Locations have no root.

        Preserved for compatibility with pathlib.Path

        Examples:
            Store-relative locations never expose a root::

                assert location.root == ""
        """
        return ""

    @property
    def anchor(self) -> str:
        """
        Always empty: store-relative Locations have no anchor.

        Examples:
            Store-relative locations never expose an anchor::

                assert location.anchor == ""
        """
        return ""

    def is_absolute(self) -> bool:
        """
        Is this path absolute?

        Currently, always returns False.
        (If True, then we'd need to include the store location details).
        :return:

        Examples:
            Locations cannot escape their owning store::

                assert not location.is_absolute()
        """
        return False

    def is_reserved(self) -> bool:
        """Return ``False`` because stores have no Windows-reserved names.

        Examples:
            Portable store keys do not apply the Windows reservation rules::

                assert not location.is_reserved()
        """
        # "Reserved" is a Windows filesystem notion; a store-relative Location
        # doesn't have this concept.
        return False

    @property
    def parts(self) -> tuple[str, ...]:
        """
        Path components as a tuple.

        :return:

        Examples:
            Inspect portable components::

                assert location.parts == ("authors", "book.epub")
        """
        return tuple(self._tokens)

    @property
    def name(self) -> str:
        """Return the final path component.

        Examples:
            Read the filename portion::

                assert location.name == "book.epub"
        """
        return self._pure().name

    @property
    def suffix(self) -> str:
        """Return the final filename suffix.

        Examples:
            Select a parser from the extension::

                assert location.suffix == ".epub"
        """
        return self._pure().suffix

    @property
    def suffixes(self) -> list[str]:
        """Return every suffix in the final filename.

        Examples:
            Preserve compound archive suffixes::

                assert archive.suffixes == [".tar", ".gz"]
        """
        return list(self._pure().suffixes)

    @property
    def stem(self) -> str:
        """Return the final filename without its last suffix.

        Examples:
            Derive a display label::

                assert location.stem == "book"
        """
        return self._pure().stem

    @property
    def parent(self) -> Self:
        """Return a location for the immediately enclosing virtual folder.

        Examples:
            Navigate from a file to its author folder::

                folder = location.parent
        """
        if not self._tokens:
            return self
        return self.__class__(*self._tokens[:-1], store=self._store)

    @property
    def parents(self) -> tuple[Self, ...]:
        """Return all enclosing locations up to the store root.

        Examples:
            Inspect the store-relative ancestry::

                ancestors = location.parents
        """
        out: list[Self] = []
        toks = self._tokens
        for i in range(len(toks) - 1, -1, -1):
            out.append(self.__class__(*toks[:i], store=self._store))
        return tuple(out)

    def joinpath(self, *other: StrOrBytesPath) -> Self:
        """
        Join on the given path to the current location.

        :param other:
        :return:

        Examples:
            Append one or more store-relative components::

                cover = folder.joinpath("images", "cover.jpg")
        """
        tokens: list[str] = list(self._tokens)
        for o in other:
            s = os.fspath(o).decode() if isinstance(o, (bytes, bytearray)) else str(os.fspath(o))
            s = s.replace('\\', '/')
            if s.startswith('/'):
                raise ValueError('Location.joinpath() arguments must be store-relative (no leading /).')
            for seg in s.split('/'):
                if seg in ('', '.'):
                    continue
                if seg == '..':
                    raise ValueError("Location cannot contain '..' segments (store escape risk).") 
                tokens.append(seg)
        return self.__class__(*tokens, store=self._store)

    def __truediv__(self, key: StrOrBytesPath) -> Self:
        """Join a component using ``pathlib``-style ``/`` syntax.

        Examples:
            Build a child location::

                cover = folder / "cover.jpg"
        """
        return self.joinpath(key)

    def __rtruediv__(self, key: StrOrBytesPath) -> Self:
        """Allow `'a/b' / loc` style composition (pathlib-like).

        The left-hand side must be store-relative (no leading `/`).

        Examples:
            Prefix an existing location::

                archived = "archive" / location
        """
        s = os.fspath(key).decode() if isinstance(key, (bytes, bytearray)) else str(os.fspath(key))
        s = s.replace('\\', '/')
        if s.startswith('/'):
            raise ValueError("Left-hand operand must be store-relative (no leading /).")

        toks: list[str] = []
        for seg in s.split('/'):
            if seg in ('', '.'):
                continue
            if seg == '..':
                raise ValueError("Location cannot contain '..' segments (store escape risk).")
            toks.append(seg)
        toks.extend(self._tokens)
        return self.__class__(*toks, store=self._store)

    def __bytes__(self) -> bytes:
        """Return the filesystem-encoded POSIX representation.

        Examples:
            Pass a location to an API requiring encoded path text::

                encoded = bytes(location)
        """
        # Mirror pathlib: bytes(path) is the filesystem-encoded string form.
        return os.fsencode(self.as_posix())

    def with_stem(self, stem: str) -> Self:
        """Return a sibling location with a replaced filename stem.

        Examples:
            Rename while preserving ``.epub``::

                revised = location.with_stem("book-revised")
        """
        # Python >=3.9: PurePath.with_stem exists.
        p = self._pure()
        if hasattr(p, "with_stem"):
            newp = p.with_stem(stem)  # type: ignore[attr-defined]
        else:  # pragma: no cover
            if not p.name:
                raise ValueError("Can't change the stem of a path with no name")
            newp = p.with_name(stem + p.suffix)
        return self.__class__(*newp.parts, store=self._store)

    def as_uri(self) -> str:
        """Raise because an abstract store-relative path has no stable URI.

        Examples:
            Use ``file_url`` for backend identity instead::

                backend_url = location.file_url
        """
        # We cannot define a meaningful, portable URI for an abstract store path.
        raise ValueError("Store-relative Locations do not have a stable URI.")

    def with_name(self, name: str) -> Self:
        """Return a sibling location with a new final component.

        Examples:
            Select a cover beside a book::

                cover = location.with_name("cover.jpg")
        """
        p = self._pure().with_name(name)
        return self.__class__(*p.parts, store=self._store)

    def with_suffix(self, suffix: str) -> Self:
        """Return a sibling location with a replaced suffix.

        Examples:
            Derive a sidecar path::

                sidecar = location.with_suffix(".opf")
        """
        p = self._pure().with_suffix(suffix)
        return self.__class__(*p.parts, store=self._store)

    def relative_to(self, other: "StrOrBytesPath | StoreLocationMixinAPI") -> Self:
        """Return this location relative to a same-store base.

        Examples:
            Remove a known folder prefix::

                relative = location.relative_to("authors")
        """
        if isinstance(other, StoreLocationMixinAPI):
            if other.store is not self.store:
                raise ValueError("Cannot compute relative path across different stores.")
            base = pathlib.PurePosixPath(*other._tokens)
        else:
            s = os.fspath(other).decode() if isinstance(other, (bytes, bytearray)) else str(os.fspath(other))
            s = s.replace('\\', '/')
            if s.startswith('/'):
                raise ValueError("Base must be store-relative (no leading /).")
            base = pathlib.PurePosixPath(*[seg for seg in s.split('/') if seg not in ('', '.')])
        rel = self._pure().relative_to(base)
        return self.__class__(*rel.parts, store=self._store)

    def is_relative_to(self, other: "StrOrBytesPath | StoreLocationMixinAPI") -> bool:
        """Return whether this location is below a same-store base.

        Examples:
            Filter an inventory to one virtual folder::

                inside = location.is_relative_to("authors")
        """
        try:
            self.relative_to(other)
            return True
        except Exception:
            return False

    def match(self, pattern: str) -> bool:
        """Match the store-relative path against a glob pattern.

        Examples:
            Select EPUB locations::

                is_epub = location.match("**/*.epub")
        """
        return self._pure().match(pattern)

    def as_posix(self) -> str:
        """Return a portable POSIX-style relative path.

        Examples:
            Serialize location structure independently of its backend URL::

                key_text = location.as_posix()
        """
        return self._pure().as_posix()

    def __str__(self) -> str:
        """Return the POSIX-style relative path.

        Examples:
            Display a location in a report::

                label = str(location)
        """
        return self.as_posix()

    def __repr__(self) -> str:
        """Return a concise debug representation.

        Examples:
            Include a location in structured diagnostics::

                debug_value = repr(location)
        """
        return f"{self.__class__.__name__}({self.as_posix()!r})"

    def __fspath__(self) -> str:
        """Return the canonical store key for path-protocol consumers.

        Examples:
            Let ``os.fspath`` obtain backend-compatible text::

                key = os.fspath(location)
        """
        return self.as_store_key()

    @property
    def location_capabilities(self) -> LocationCapabilities:
        """Return the advertised capability surface for this location.

        Examples:
            Check write support before opening in mutation mode::

                writable = location.location_capabilities.can_open_write
        """
        return READ_WRITE_LOCATION_CAPABILITIES

    @property
    def file_url(self) -> str:
        """Return the canonical backend URL/key for this location.

        Examples:
            Persist the identifier needed to resolve the file later::

                file_url = location.file_url
        """
        return self.as_store_key()

    @property
    def status(self) -> SingleFileStatus | None:
        """Return cached file status without performing backend I/O.

        Examples:
            Use cached metadata when a best-effort display is sufficient::

                cached_status = location.status
        """
        return getattr(self, "_file_status", None)

    def _required_status(self, *, refresh: bool = False) -> SingleFileStatus:
        status = getattr(self, "_file_status", None)
        if refresh or status is None:
            status = self.recheck_status()
        if status is None:
            raise AttributeError("Location has no available status for {!r}".format(self.file_url))
        return status

    def recheck_status(self) -> SingleFileStatus:
        """Fetch fresh status from the owning store and cache it.

        Examples:
            Refresh metadata after an external change::

                status = location.recheck_status()
        """
        getter = getattr(self.store, "stat", None)
        if not callable(getter):
            raise AttributeError("Store {!r} does not expose stat().".format(self.store))
        status = getter(self)
        setattr(self, "_file_status", status)
        return status

    @property
    def uuid(self) -> str | None:
        """Return the UUID from cached status, if status has been fetched.

        Examples:
            Read without triggering a backend request::

                cached_uuid = location.uuid
        """
        status = getattr(self, "_file_status", None)
        return None if status is None else status.uuid

    @property
    def cached_size(self) -> int | None:
        """Return the byte size from cached status, if available.

        Examples:
            Render a best-effort inventory quickly::

                size = location.cached_size
        """
        status = getattr(self, "_file_status", None)
        return None if status is None else status.size

    @property
    def cached_hash(self) -> str | None:
        """Return the content hash from cached status, if available.

        Examples:
            Compare already-fetched metadata without new I/O::

                digest = location.cached_hash
        """
        status = getattr(self, "_file_status", None)
        return None if status is None else status.hash

    @property
    def size(self) -> int:
        """Refresh status and return the current byte size.

        Examples:
            Obtain an authoritative size before transfer::

                byte_count = location.size
        """
        return self._required_status(refresh=True).size

    @property
    def hash(self) -> str:
        """Refresh status and return the current content hash.

        Examples:
            Verify a location against expected metadata::

                matches = location.hash == expected_hash
        """
        return self._required_status(refresh=True).hash

    @property
    def url(self) -> str:
        """Return the status URL when cached, otherwise ``file_url``.

        Examples:
            Use one compatibility property across old and new callers::

                persisted_url = location.url
        """
        status = getattr(self, "_file_status", None)
        if status is not None:
            return status.url
        return self.file_url

    def as_bytes(self) -> bytes:
        """Read and return the complete file as bytes.

        Examples:
            Retrieve a small binary payload::

                payload = location.as_bytes()
        """
        return self.read_bytes()

    def as_string(self) -> str:
        """Read the complete file as replacement-decoded UTF-8 text.

        Examples:
            Preview a text-like payload safely::

                preview = location.as_string()[:160]
        """
        return self.read_bytes().decode("utf-8", "replace")

    def __eq__(self, other: object) -> bool:
        """Compare class, owning store identity, and path tokens.

        Examples:
            Locations built from the same plugin and key compare equal::

                assert plugin.location("a") == plugin.location("a")
        """
        if not isinstance(other, StoreLocationMixinAPI):
            return NotImplemented
        return (self.__class__ is other.__class__) and (self.store is other.store) and (self._tokens == other._tokens)

    def __hash__(self) -> int:
        """Hash a location by store identity, class, and path tokens.

        Examples:
            Locations can be used as dictionary keys::

                status_by_location = {location: location.status}
        """
        return hash((id(self._store), tuple(self._tokens), self.__class__))

    def __lt__(self, other: object) -> bool:
        """Order locations lexically when class and owning store match.

        Examples:
            Sort a single-store inventory::

                ordered = sorted(plugin.iter_locations())
        """
        if not isinstance(other, StoreLocationMixinAPI):
            return NotImplemented
        if self.__class__ is not other.__class__ or self.store is not other.store:
            raise TypeError("Cannot order Locations from different stores or different classes.")
        return tuple(self._tokens) < tuple(other._tokens)

    # ---- Existence / type checks ----

    @abstractmethod
    def exists(self) -> bool:
        """Return whether this location exists.

        Examples:
            Check before reading an optional file::

                if location.exists():
                    payload = location.read_bytes()
        """
        ...

    @abstractmethod
    async def aexists(self) -> bool:
        """Asynchronously return whether this location exists.

        Examples:
            Check a remote location without blocking the event loop::

                if await location.aexists():
                    payload = await location.aread_bytes()
        """
        ...

    @abstractmethod
    def is_file(self) -> bool:
        """Return whether this location is a concrete file.

        Examples:
            Exclude virtual directories from an inventory::

                files = [child for child in folder.iterdir() if child.is_file()]
        """
        ...

    @abstractmethod
    async def ais_file(self) -> bool:
        """Asynchronously return whether this location is a file.

        Examples:
            Inspect a remote child::

                concrete = await child.ais_file()
        """
        ...

    @abstractmethod
    def is_dir(self) -> bool:
        """Return whether this location is a directory-like entry.

        Examples:
            Recurse only into folders::

                folders = [child for child in root.iterdir() if child.is_dir()]
        """
        ...

    @abstractmethod
    async def ais_dir(self) -> bool:
        """Asynchronously return whether this location is a directory.

        Examples:
            Inspect a remote child::

                folder_like = await child.ais_dir()
        """
        ...

    # ---- Directory traversal ----

    @abstractmethod
    def iterdir(self) -> Iterator[Self]:
        """Iterate over direct children of this location.

        Examples:
            List one virtual directory::

                children = list(folder.iterdir())
        """
        ...

    @abstractmethod
    async def aiterdir(self) -> AsyncIterator[Self]:
        """Asynchronously iterate over direct children.

        Examples:
            Stream children from a remote backend::

                children = [child async for child in folder.aiterdir()]
        """
        ...

    @abstractmethod
    def glob(self, pattern: str) -> Iterator[Self]:
        """Iterate over locations matching a relative glob pattern.

        Examples:
            Find EPUBs immediately below a folder::

                epubs = list(folder.glob("*.epub"))
        """
        ...

    @abstractmethod
    async def aglob(self, pattern: str) -> AsyncIterator[Self]:
        """Asynchronously iterate over relative glob matches.

        Examples:
            Stream EPUB matches from a remote backend::

                epubs = [item async for item in folder.aglob("*.epub")]
        """
        ...

    @abstractmethod
    def rglob(self, pattern: str) -> Iterator[Self]:
        """Recursively iterate over locations matching a glob pattern.

        Examples:
            Inventory every EPUB below the store root::

                epubs = list(root.rglob("*.epub"))
        """
        ...

    @abstractmethod
    async def arglob(self, pattern: str) -> AsyncIterator[Self]:
        """Asynchronously recurse over glob matches.

        Examples:
            Stream a remote recursive inventory::

                epubs = [item async for item in root.arglob("*.epub")]
        """
        ...

    # ---- Metadata ----

    @abstractmethod
    def stat(self) -> os.stat_result:
        """Return backend-native stat information for this location.

        Examples:
            Read the modification time where a backend supplies one::

                modified_at = location.stat().st_mtime
        """
        ...

    @abstractmethod
    async def astat(self) -> os.stat_result:
        """Asynchronously return backend-native stat information.

        Examples:
            Query a remote size without blocking::

                size = (await location.astat()).st_size
        """
        ...

    # ---- Mutations ----

    @abstractmethod
    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """Create this directory-like location.

        Examples:
            Create a nested folder on a writable backend::

                folder.mkdir(parents=True, exist_ok=True)
        """
        ...

    @abstractmethod
    async def amkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """Asynchronously create this directory-like location.

        Examples:
            Create a remote nested folder::

                await folder.amkdir(parents=True, exist_ok=True)
        """
        ...

    @abstractmethod
    def unlink(self, missing_ok: bool = False) -> None:
        """Delete this file location.

        Examples:
            Make cleanup idempotent::

                location.unlink(missing_ok=True)
        """
        ...

    @abstractmethod
    async def aunlink(self, missing_ok: bool = False) -> None:
        """Asynchronously delete this file location.

        Examples:
            Clean up a remote temporary object::

                await location.aunlink(missing_ok=True)
        """
        ...

    @abstractmethod
    def rmdir(self) -> None:
        """Remove this empty directory-like location.

        Examples:
            Remove an empty staging folder::

                staging.rmdir()
        """
        ...

    @abstractmethod
    async def armdir(self) -> None:
        """Asynchronously remove this empty directory-like location.

        Examples:
            Remove a remote staging folder::

                await staging.armdir()
        """
        ...

    @abstractmethod
    def rename(self, target: str | os.PathLike[str]) -> Self:
        """Rename this location and return the destination handle.

        Examples:
            Rename within a writable store::

                renamed = location.rename("authors/revised.epub")
        """
        ...

    @abstractmethod
    async def arename(self, target: str | os.PathLike[str]) -> Self:
        """Asynchronously rename this location.

        Examples:
            Rename a remote object::

                renamed = await location.arename("authors/revised.epub")
        """
        ...

    @abstractmethod
    def replace(self, target: str | os.PathLike[str]) -> Self:
        """Move this location over an existing destination.

        Examples:
            Atomically promote a staged file when supported::

                final = staged.replace("authors/book.epub")
        """
        ...

    @abstractmethod
    async def areplace(self, target: str | os.PathLike[str]) -> Self:
        """Asynchronously replace an existing destination.

        Examples:
            Promote a remote staged object::

                final = await staged.areplace("authors/book.epub")
        """
        ...

    @abstractmethod
    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        """Create an empty file or update its timestamp.

        Examples:
            Create a marker file on a writable backend::

                marker.touch(exist_ok=True)
        """
        ...

    @abstractmethod
    async def atouch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        """Asynchronously create or update an empty file.

        Examples:
            Create a remote marker::

                await marker.atouch(exist_ok=True)
        """
        ...

    # ---- I/O (sync) ----

    @overload
    def open(
        self,
        mode: OpenTextMode = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> TextIO: ...

    @overload
    def open(
        self,
        mode: OpenBinaryMode,
        buffering: int = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
    ) -> BinaryIO: ...

    @abstractmethod
    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO[Any]:
        """Return a synchronous file-like object.

        Examples:
            Stream text without reading the entire object::

                with location.open("r", encoding="utf-8") as file:
                    first_line = file.readline()
        """

    # ---- I/O (async) ----

    @overload
    def aopen(
        self,
        mode: OpenTextMode = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> AsyncTextFile: ...

    @overload
    def aopen(
        self,
        mode: OpenBinaryMode,
        buffering: int = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
    ) -> AsyncBinaryFile: ...

    @abstractmethod
    def aopen(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> AsyncTextFile | AsyncBinaryFile:
        """Return an async file object supporting ``async with``.

        Examples:
            Stream bytes from an async-native backend::

                async with location.aopen("rb") as file:
                    header = await file.read(16)
        """

    # ---- Convenience helpers (sync) ----

    def read_bytes(self) -> bytes:
        """Read and return the complete file as bytes.

        Examples:
            Load a small binary payload::

                payload = location.read_bytes()
        """
        with self.open("rb") as f:
            return f.read()

    def read_text(self, encoding: str | None = None, errors: str | None = None) -> str:
        """Read and return the complete file as text.

        Examples:
            Decode a UTF-8 sidecar file::

                metadata_xml = sidecar.read_text(encoding="utf-8")
        """
        with self.open("r", encoding=encoding, errors=errors) as f:
            return f.read()

    def write_bytes(self, data: bytes) -> int:
        """Replace this location's contents with bytes.

        Examples:
            Write a generated thumbnail::

                byte_count = thumbnail.write_bytes(image_bytes)
        """
        with self.open("wb") as f:
            return f.write(data)

    def write_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        """Replace this location's contents with text.

        Examples:
            Write a UTF-8 sidecar document::

                char_count = sidecar.write_text(xml, encoding="utf-8")
        """
        with self.open("w", encoding=encoding, errors=errors, newline=newline) as f:
            return f.write(data)

    # ---- Convenience helpers (async) ----

    async def aread_bytes(self) -> bytes:
        """Asynchronously read the complete file as bytes.

        Examples:
            Fetch a small remote payload::

                payload = await location.aread_bytes()
        """
        async with self.aopen("rb") as f:
            return await f.read()

    async def aread_text(self, encoding: str | None = None, errors: str | None = None) -> str:
        """Asynchronously read the complete file as text.

        Examples:
            Fetch remote UTF-8 metadata::

                text = await location.aread_text(encoding="utf-8")
        """
        async with self.aopen("r", encoding=encoding, errors=errors) as f:
            return await f.read()

    async def awrite_bytes(self, data: bytes) -> int:
        """Asynchronously replace this location's contents with bytes.

        Examples:
            Write to an async-native backend::

                byte_count = await location.awrite_bytes(payload)
        """
        async with self.aopen("wb") as f:
            return await f.write(data)

    async def awrite_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        """Asynchronously replace this location's contents with text.

        Examples:
            Write UTF-8 metadata to an async-native backend::

                char_count = await location.awrite_text(text, encoding="utf-8")
        """
        async with self.aopen("w", encoding=encoding, errors=errors, newline=newline) as f:
            return await f.write(data)


class _AsyncLoopThread:
    """
    A dedicated event loop running in a background thread.
    Used to synchronously wait on coroutines from sync code without
    nesting event loops.

    Examples:
        Location adapters share a runner internally::

            runner = _AsyncLoopThread()
            result = runner.run(async_operation())
    """
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()
        loop.run_forever()
        loop.close()

    def ensure_started(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._thread_main, name="StorePathAsyncBridge", daemon=True)
        self._thread.start()
        self._started.wait()

    def run(self, coro: "asyncio.Future[T] | asyncio.coroutines.Coroutine[Any, Any, T]") -> T:
        self.ensure_started()
        assert self._loop is not None
        fut: Future[T] = asyncio.run_coroutine_threadsafe(cast(Any, coro), self._loop)
        return fut.result()

    def stop(self) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._loop.stop)


class _SyncFileFromAsync:
    """
    A sync file-like wrapper over an async file object + its async context manager.

    Examples:
        ``AsyncNativePretendSyncLocation.open`` returns this adapter so callers
        can use a normal context manager::

            with location.open("rb") as file:
                payload = file.read()
    """
    def __init__(self, runner: _AsyncLoopThread, async_cm: Any, afile: Any) -> None:
        self._runner = runner
        self._cm = async_cm
        self._afile = afile
        self._closed = False

    def __enter__(self) -> "_SyncFileFromAsync":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        if self._closed:
            return None
        self._closed = True
        return self._runner.run(self._cm.__aexit__(exc_type, exc, tb))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._runner.run(self._cm.__aexit__(None, None, None))

    def flush(self) -> None:
        self._runner.run(self._afile.flush())

    def read(self, n: int = -1) -> Any:
        return self._runner.run(self._afile.read(n))

    def write(self, data: Any) -> int:
        return self._runner.run(self._afile.write(data))


async def _to_thread(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    # stdlib first; easy to swap to anyio.to_thread.run_sync later if you prefer.
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _aiter_from_sync_iter(iter_fn: Callable[[], Iterator[T]]) -> AsyncIterator[T]:
    """
    Stream a sync iterator into async without materializing the whole list.

    Examples:
        Location adapters use the helper behind ``aiterdir``::

            children = [child async for child in _aiter_from_sync_iter(folder.iterdir)]
    """
    loop = asyncio.get_running_loop()
    q: asyncio.Queue[object] = asyncio.Queue()
    SENTINEL = object()
    EXC = object()

    def worker() -> None:
        try:
            for item in iter_fn():
                loop.call_soon_threadsafe(q.put_nowait, item)
        except BaseException as e:  # propagate into async generator
            loop.call_soon_threadsafe(q.put_nowait, (EXC, e))
        finally:
            loop.call_soon_threadsafe(q.put_nowait, SENTINEL)

    task = asyncio.create_task(asyncio.to_thread(worker))

    try:
        while True:
            item = await q.get()
            if item is SENTINEL:
                break
            if isinstance(item, tuple) and len(item) == 2 and item[0] is EXC:
                raise cast(BaseException, item[1])
            yield cast(T, item)
    finally:
        await task


class _AsyncFileFromSync:
    """
    Async file wrapper over a sync file object using to_thread for operations.
    Implements your AsyncTextFile/AsyncBinaryFile Protocol shape.

    Examples:
        The sync-native adapter exposes this through ``aopen``::

            async with location.aopen("rb") as file:
                payload = await file.read()
    """
    def __init__(self, f: Any) -> None:
        self._f = f

    async def __aenter__(self) -> "_AsyncFileFromSync":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        await self.close()
        return None

    async def read(self, n: int = -1) -> Any:
        return await _to_thread(self._f.read, n)

    async def write(self, data: Any) -> int:
        return await _to_thread(self._f.write, data)

    async def flush(self) -> None:
        await _to_thread(self._f.flush)

    async def close(self) -> None:
        await _to_thread(self._f.close)


class _AsyncOpenFromSync:
    """
    Async context manager that opens a sync file in a thread, then wraps it.

    Examples:
        ``SyncNativePretendAsyncLocation.aopen`` constructs this adapter::

            async with location.aopen("r") as file:
                text = await file.read()
    """
    def __init__(self, opener: Callable[[], Any]) -> None:
        self._opener = opener
        self._f: Any | None = None

    async def __aenter__(self) -> _AsyncFileFromSync:
        self._f = await _to_thread(self._opener)
        return _AsyncFileFromSync(self._f)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        if self._f is not None:
            await _to_thread(self._f.close)
        return None


# =========================================================
# 1) Async-native: implement async; get sync "pretend" free
# =========================================================
class AsyncNativePretendSyncLocation(StoreLocationMixinAPI, ABC):
    """
    Implement the async methods (aexists/astat/aopen/aiterdir/...) natively.

    Sync methods are derived by running the async methods on a background loop.

    This is the cleanest “async-first but pathlib-ish” bridge that doesn’t rely on
    nested loops or fragile `asyncio.run()` calls.

    Examples:
        Implement async primitives, then use the derived sync facade::

            assert location.exists()
            payload = location.read_bytes()
    """
    _runner = _AsyncLoopThread()

    # --- you implement these natively ---
    @abstractmethod
    async def aexists(self) -> bool: ...
    @abstractmethod
    async def ais_file(self) -> bool: ...
    @abstractmethod
    async def ais_dir(self) -> bool: ...
    @abstractmethod
    async def astat(self) -> os.stat_result: ...
    @abstractmethod
    async def amkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None: ...
    @abstractmethod
    async def aunlink(self, missing_ok: bool = False) -> None: ...
    @abstractmethod
    async def armdir(self) -> None: ...
    @abstractmethod
    async def arename(self, target: str | os.PathLike[str]) -> Self: ...
    @abstractmethod
    async def areplace(self, target: str | os.PathLike[str]) -> Self: ...
    @abstractmethod
    async def atouch(self, mode: int = 0o666, exist_ok: bool = True) -> None: ...
    @abstractmethod
    async def aiterdir(self) -> AsyncIterator[Self]: ...
    @abstractmethod
    async def aglob(self, pattern: str) -> AsyncIterator[Self]: ...
    @abstractmethod
    async def arglob(self, pattern: str) -> AsyncIterator[Self]: ...
    @abstractmethod
    def aopen(self, mode: str = "r", buffering: int = -1,
              encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any: ...

    # --- derived sync facade ---
    def exists(self) -> bool:
        return self._runner.run(self.aexists())

    def is_file(self) -> bool:
        return self._runner.run(self.ais_file())

    def is_dir(self) -> bool:
        return self._runner.run(self.ais_dir())

    def stat(self) -> os.stat_result:
        return self._runner.run(self.astat())

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        self._runner.run(self.amkdir(mode=mode, parents=parents, exist_ok=exist_ok))

    def unlink(self, missing_ok: bool = False) -> None:
        self._runner.run(self.aunlink(missing_ok=missing_ok))

    def rmdir(self) -> None:
        self._runner.run(self.armdir())

    def rename(self, target: str | os.PathLike[str]) -> Self:
        return self._runner.run(self.arename(target))

    def replace(self, target: str | os.PathLike[str]) -> Self:
        return self._runner.run(self.areplace(target))

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        self._runner.run(self.atouch(mode=mode, exist_ok=exist_ok))

    def iterdir(self) -> Iterator[Self]:
        async def collect() -> list[Self]:
            out: list[Self] = []
            async for p in self.aiterdir():
                out.append(p)
            return out
        return iter(self._runner.run(collect()))

    def glob(self, pattern: str) -> Iterator[Self]:
        async def collect() -> list[Self]:
            out: list[Self] = []
            async for p in self.aglob(pattern):
                out.append(p)
            return out
        return iter(self._runner.run(collect()))

    def rglob(self, pattern: str) -> Iterator[Self]:
        async def collect() -> list[Self]:
            out: list[Self] = []
            async for p in self.arglob(pattern):
                out.append(p)
            return out
        return iter(self._runner.run(collect()))

    def open(self, mode: str = "r", buffering: int = -1,
             encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any:
        # Open immediately (pathlib-like), returning a sync wrapper.
        async_cm = self.aopen(mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
        afile = self._runner.run(async_cm.__aenter__())
        return _SyncFileFromAsync(self._runner, async_cm, afile)


# =========================================================
# 2) Sync-native: implement sync; get async "pretend" free
# =========================================================
class SyncNativePretendAsyncLocation(StoreLocationMixinAPI):
    """
    Implement the sync methods (exists/stat/open/iterdir/...) natively.

    Async methods are derived via asyncio.to_thread + streaming iterator bridge.

    Examples:
        Implement sync primitives, then use the derived async facade::

            assert await location.aexists()
            payload = await location.aread_bytes()
    """

    # --- you implement these natively ---
    @abstractmethod
    def exists(self) -> bool: ...

    @abstractmethod
    def is_file(self) -> bool: ...

    @abstractmethod
    def is_dir(self) -> bool: ...

    @abstractmethod
    def stat(self) -> os.stat_result: ...

    @abstractmethod
    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None: ...

    @abstractmethod
    def unlink(self, missing_ok: bool = False) -> None: ...

    @abstractmethod
    def rmdir(self) -> None: ...

    @abstractmethod
    def rename(self, target: str | os.PathLike[str]) -> Self: ...

    @abstractmethod
    def replace(self, target: str | os.PathLike[str]) -> Self: ...

    @abstractmethod
    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None: ...

    @abstractmethod
    def iterdir(self) -> Iterator[Self]: ...

    @abstractmethod
    def glob(self, pattern: str) -> Iterator[Self]: ...

    @abstractmethod
    def rglob(self, pattern: str) -> Iterator[Self]: ...

    @abstractmethod
    def open(self,
             mode: str = "r",
             buffering: int = -1,
             encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any: ...

    # --- derived async facade ---
    async def aexists(self) -> bool:
        return await _to_thread(self.exists)

    async def ais_file(self) -> bool:
        return await _to_thread(self.is_file)

    async def ais_dir(self) -> bool:
        return await _to_thread(self.is_dir)

    async def astat(self) -> os.stat_result:
        return await _to_thread(self.stat)

    async def amkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        await _to_thread(self.mkdir, mode, parents, exist_ok)

    async def aunlink(self, missing_ok: bool = False) -> None:
        await _to_thread(self.unlink, missing_ok)

    async def armdir(self) -> None:
        await _to_thread(self.rmdir)

    async def arename(self, target: str | os.PathLike[str]) -> Self:
        return await _to_thread(self.rename, target)

    async def areplace(self, target: str | os.PathLike[str]) -> Self:
        return await _to_thread(self.replace, target)

    async def atouch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        await _to_thread(self.touch, mode, exist_ok)

    async def aiterdir(self) -> AsyncIterator[Self]:
        async for item in _aiter_from_sync_iter(self.iterdir):
            yield item

    async def aglob(self, pattern: str) -> AsyncIterator[Self]:
        async for item in _aiter_from_sync_iter(lambda: self.glob(pattern)):
            yield item

    async def arglob(self, pattern: str) -> AsyncIterator[Self]:
        async for item in _aiter_from_sync_iter(lambda: self.rglob(pattern)):
            yield item

    def aopen(self, mode: str = "r", buffering: int = -1,
              encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any:
        # Return an async context manager that opens the sync file in a thread.
        return _AsyncOpenFromSync(
            lambda: self.open(mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
        )


class ReadOnlySyncNativePretendAsyncLocation(SyncNativePretendAsyncLocation, ABC):
    """Sync-native Location base for backends that must never mutate in place.

    Subclasses still implement their normal non-mutating filesystem/path view
    methods, but mutation entry points now advertise read-only capabilities and
    fail loudly and consistently.

    Examples:
        Read-only backends remain readable through both facades::

            payload = location.read_bytes()
            assert location.location_capabilities.read_only
    """

    @property
    def location_capabilities(self) -> LocationCapabilities:
        return READ_ONLY_LOCATION_CAPABILITIES

    def _read_only_error(self, action: str) -> PermissionError:
        return PermissionError(f"{self.__class__.__name__} is read-only; cannot {action}.")

    def _assert_read_mode(self, mode: str) -> None:
        write_flags = {"w", "a", "x", "+"}
        if any(flag in mode for flag in write_flags):
            raise self._read_only_error(f"open with mode {mode!r}")

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        raise self._read_only_error("create directories")

    def unlink(self, missing_ok: bool = False) -> None:
        raise self._read_only_error("delete files")

    def rmdir(self) -> None:
        raise self._read_only_error("remove directories")

    def rename(self, target: str | os.PathLike[str]) -> Self:
        raise self._read_only_error("rename locations")

    def replace(self, target: str | os.PathLike[str]) -> Self:
        raise self._read_only_error("replace locations")

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        raise self._read_only_error("touch files")

    def write_bytes(self, data: bytes) -> int:
        raise self._read_only_error("write bytes")

    def write_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        raise self._read_only_error("write text")
