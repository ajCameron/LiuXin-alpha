"""
Run the project's own metadata readers as providers, in both models.

The project's metadata readers, in ``metadata.file_sources``, are calibre's
readers as forked into LiuXin and partly converted. They give two sets of
providers:

- **fork** providers (calibre model) run the fork's calibre path: the reader
  plugins that still return calibre's own ``Metadata``, and the EPUB and LRF
  readers through their switches for calibre output. The result is serialised
  with the fork's ``metadata_as_dict``. Twelve readers have such a path; they
  are listed in :data:`FORK_CALIBRE_READERS`.
- **LiuXin** providers (WEMI model) run every reader plugin exactly as the
  application runs it today. Most return the half-converted
  ``CalibreLikeLiuXinBookMetaData``; a few return calibre's ``Metadata``. Either
  is placed by WEMI level with :func:`wemi_mapping.wemi_from_calibre`. For the
  half-converted object, its creators by role and its identifier lists are
  laid over calibre's fields, because its own conversion to calibre loses
  them.

Both sets also extract covers, and one read of an unchanged file answers both
the metadata and the cover.

Importing this module loads no reader. The readers are imported when
:func:`register_fork_metadata_readers` is called or a provider runs.
"""

from __future__ import annotations

import types
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from LiuXin_alpha.file_formats.conversion.edges import normalize_format_name
from LiuXin_alpha.file_formats.providers.base import (
    ProviderContext,
    provider_id_from_name,
)
from LiuXin_alpha.file_formats.providers.legacy_conversion import (
    LEGACY_LICENCE,
    LEGACY_PRIORITY,
)
from LiuXin_alpha.file_formats.providers.metadata import (
    CalibreFields,
    CalibreMetadataReader,
    CoverExtractor,
    CoverImage,
    JSONValue,
    WEMIFields,
    WEMIMetadataReader,
)
from LiuXin_alpha.file_formats.providers.wemi_mapping import wemi_from_calibre

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry

FORK_CALIBRE_READERS: Mapping[str, str] = {
    "Read EPUB metadata": "epub",
    "Read LRF metadata": "lrf",
    "Read TXT metadata": "plugin",
    "Read OPF metadata": "plugin",
    "Read MOBI metadata": "plugin",
    "Read LIT metadata": "plugin",
    "Read LRX metadata": "plugin",
    "Read RB metadata": "plugin",
    "Read SNB metadata": "plugin",
    "Read Topaz metadata": "plugin",
    "Read comic metadata": "plugin",
    "Read ODT metadata": "plugin",
}
"""Fork readers with a calibre path, and how to take it.

``plugin`` means the plugin itself returns calibre's ``Metadata``; ``epub`` and
``lrf`` mean calling the format's reader with its calibre switch on.
"""

_CACHE_SIZE = 16
_cache: OrderedDict[tuple[object, ...], tuple[object, CoverImage | None]] = (
    OrderedDict()
)


class MetadataModelError(RuntimeError):
    """
    Report a reader that returned a different kind of metadata than expected.

    Example:
        >>> issubclass(MetadataModelError, RuntimeError)
        True
    """


def _is_calibre_metadata(metadata: object) -> bool:
    """
    Say whether a reader returned calibre's own ``Metadata``.

    Example:
        >>> _is_calibre_metadata(object())
        False


    :param metadata: What the reader returned.
    :return: ``True`` for calibre's ``Metadata``.
    """
    from LiuXin_alpha.metadata.book.base import calibreMetadata

    return isinstance(metadata, calibreMetadata)


def _is_liuxin_hybrid(metadata: object) -> bool:
    """
    Say whether a reader returned the half-converted LiuXin object.

    Example:
        >>> _is_liuxin_hybrid(object())
        False


    :param metadata: What the reader returned.
    :return: ``True`` for ``CalibreLikeLiuXinBookMetaData``.
    """
    from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
        CalibreLikeLiuXinBookMetaData,
    )

    return isinstance(metadata, CalibreLikeLiuXinBookMetaData)


def serialised_calibre(
    metadata: Any, context: ProviderContext
) -> tuple[dict[str, JSONValue], CoverImage | None]:
    """
    Serialise calibre ``Metadata`` the way the upstream providers do.

    The fork's ``metadata_as_dict`` writes the fields; values with no JSON
    form become text and are named in the report; the cover is taken out.

    Example:
        >>> from LiuXin_alpha.metadata.book.base import calibreMetadata
        >>> import logging, pathlib
        >>> from LiuXin_alpha.file_formats.conversion.report import ConversionReport
        >>> context = ProviderContext(pathlib.Path.cwd(), {}, logging.getLogger("x"), ConversionReport())
        >>> values, cover = serialised_calibre(calibreMetadata("A Book", ["Ann"]), context)
        >>> values["title"], values["authors"], cover
        ('A Book', ['Ann'], None)


    :param metadata: calibre's ``Metadata``.
    :param context: Where to report values written as text.
    :return: The fields, and the cover if there is one.
    """
    import base64

    import LiuXin_alpha.metadata.book.serialize as serialize_module
    from LiuXin_alpha.file_formats.calibre_metadata_probe import jsonable

    serialize: Any = serialize_module
    raw: dict[str, Any] = dict(
        serialize.metadata_as_dict(metadata, encode_cover_data=True)
    )
    cover_entry = raw.pop("cover_data", None)
    text_fields: list[str] = []
    values: dict[str, JSONValue] = {
        str(name): jsonable(value, str(name), text_fields)
        for name, value in raw.items()
    }
    for place in text_fields:
        context.report.add_warning(
            f"the reader gave {place} a value with no JSON form; kept as text"
        )
    cover: CoverImage | None = None
    if isinstance(cover_entry, list | tuple):
        parts = cast("Sequence[object]", cover_entry)
        if len(parts) == 2 and parts[1]:
            image_format = normalize_format_name(str(parts[0] or "")) or "unknown"
            cover = CoverImage(base64.b64decode(str(parts[1])), image_format)
    return values, cover


def _hybrid_roles(metadata: Any) -> dict[str, list[str]]:
    """
    Read the half-converted object's creators by role.

    Example:
        >>> class Hybrid:
        ...     creators = {"authors": ["Ann"], "editors": [], "producers": ["P", None]}
        >>> _hybrid_roles(Hybrid())
        {'authors': ['Ann'], 'producers': ['P']}


    :param metadata: The half-converted object.
    :return: Each role with at least one name, mapped to its names.
    """
    given: object = getattr(metadata, "creators", None)
    roles: dict[str, list[str]] = {}
    if isinstance(given, Mapping):
        for role, names in cast("Mapping[object, object]", given).items():
            if isinstance(names, list | tuple):
                found = [str(name) for name in cast("Sequence[object]", names) if name]
                if found:
                    roles[str(role)] = found
    return roles


def _hybrid_identifiers(metadata: Any) -> dict[str, list[str]]:
    """
    Read the half-converted object's identifiers, dropping placeholders.

    Example:
        >>> class Hybrid:
        ...     identifiers = {"isbn": [None, "978"], "doi": [None]}
        >>> _hybrid_identifiers(Hybrid())
        {'isbn': ['978']}


    :param metadata: The half-converted object.
    :return: Each scheme with at least one value, mapped to its values.
    """
    given: object = getattr(metadata, "identifiers", None)
    found: dict[str, list[str]] = {}
    if isinstance(given, Mapping):
        for scheme, values in cast("Mapping[object, object]", given).items():
            if isinstance(values, list | tuple | set):
                kept = [
                    str(value) for value in cast("Sequence[object]", values) if value
                ]
                if kept:
                    found[str(scheme)] = kept
    return found


def _plugin(plugin_class: type[Any]) -> Any:
    """
    Make a fork reader plugin ready for a full read.

    Example:
        >>> class Plugin:
        ...     def __init__(self, path):
        ...         self.quick = True
        >>> _plugin(Plugin).quick
        False


    :param plugin_class: The plugin class.
    :return: An instance that reads covers too.
    """
    plugin = plugin_class(None)
    plugin.quick = False
    return plugin


def _cached(
    key: tuple[object, ...], read: Callable[[], tuple[object, CoverImage | None]]
) -> tuple[object, CoverImage | None]:
    """
    Reuse a recent answer for the same reader and unchanged file.

    Example:
        >>> _cached(("example",), lambda: ("fields", None))
        ('fields', None)


    :param key: The reader, file, modification time, size and format.
    :param read: Reads the file when there is no recent answer.
    :return: The fields and the cover.
    """
    found = _cache.get(key)
    if found is not None:
        _cache.move_to_end(key)
        return found
    answer = read()
    _cache[key] = answer
    while len(_cache) > _CACHE_SIZE:
        _cache.popitem(last=False)
    return answer


def _file_key(
    provider: type[Any], source: Path, source_format: str
) -> tuple[object, ...]:
    """
    Identify one reader's read of one unchanged file.

    Example:
        >>> _file_key(int, Path(__file__), "py")[1] == str(Path(__file__).resolve())
        True


    :param provider: The provider class.
    :param source: The file.
    :param source_format: Its format.
    :return: A key that changes when the file does.
    """
    status = source.stat()
    return (
        provider,
        str(source.resolve()),
        source_format,
        status.st_mtime_ns,
        status.st_size,
    )


class ForkCalibreReader(CalibreMetadataReader, CoverExtractor):
    """
    Read metadata and the cover with the fork's calibre path for a format.

    Subclasses are made by :func:`adapt_fork_calibre_reader`; ``fork_plugin``
    is the fork's reader plugin and ``calibre_path`` how to reach calibre
    output (see :data:`FORK_CALIBRE_READERS`).

    Example:
        >>> from LiuXin_alpha.metadata.file_sources.builtin_readers import (
        ...     get_metadata_reader_plugins,
        ... )
        >>> txt = next(p for p in get_metadata_reader_plugins() if "txt" in p.file_types)
        >>> provider = adapt_fork_calibre_reader(txt)
        >>> provider.provider_id, sorted(provider.calibre_read_formats)
        ('fork.read-txt-metadata', ['txt'])
    """

    fork_plugin: ClassVar[type[Any]]
    calibre_path: ClassVar[str]

    def read_calibre_metadata(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CalibreFields:
        """
        Return the fork's calibre reading of the file.

        Example:
            Called by a metadata run::

                fields = provider().read_calibre_metadata(path, "txt", context)


        :param source: The file to read. It is not changed.
        :param source_format: Which of ``calibre_read_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The metadata, in calibre's names and shapes.
        :raises MetadataModelError: If the reader returned LiuXin metadata.
        """
        fields, _ = self._read(source, source_format, context)
        return fields

    def extract_cover(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CoverImage | None:
        """
        Return the cover the fork's calibre path found.

        Example:
            Called by a metadata run::

                cover = provider().extract_cover(path, "epub", context)


        :param source: The file to read. It is not changed.
        :param source_format: Which of ``cover_source_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The cover, or ``None``.
        :raises MetadataModelError: If the reader returned LiuXin metadata.
        """
        _, cover = self._read(source, source_format, context)
        return cover

    def _read(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> tuple[CalibreFields, CoverImage | None]:
        """
        Read the file once and keep the answer for the unchanged file.

        Example:
            Both public methods go through here::

                fields, cover = self._read(source, "txt", context)


        :param source: The file to read.
        :param source_format: Its format.
        :param context: Options, log, report and work folder for this call.
        :return: The fields and the cover.
        :raises MetadataModelError: If the reader returned LiuXin metadata.
        """

        def read() -> tuple[object, CoverImage | None]:
            """
            Run the calibre path and serialise its result.

            Example:
                Called once per unchanged file::

                    fields, cover = read()


            :return: The fields and the cover.
            """
            metadata = self._run(source, source_format)
            if metadata is None:
                return CalibreFields({}), None
            if not _is_calibre_metadata(metadata):
                raise MetadataModelError(
                    f"{self.provider_id} expected calibre metadata but the reader "
                    f"returned {type(metadata).__name__}"
                )
            values, cover = serialised_calibre(metadata, context)
            return CalibreFields(values), cover

        fields, cover = _cached(_file_key(type(self), source, source_format), read)
        return cast("CalibreFields", fields), cover

    def _run(self: Self, source: Path, source_format: str) -> object:
        """
        Take the calibre path for this format.

        Example:
            Called by :meth:`_read`::

                metadata = self._run(source, "epub")


        :param source: The file to read.
        :param source_format: Its format.
        :return: What the reader returned.
        """
        with source.open("rb") as stream:
            if self.calibre_path == "epub":
                import LiuXin_alpha.metadata.file_sources.epub as epub_module

                epub: Any = epub_module
                return epub.get_metadata(
                    stream, extract_cover=True, calibre_metadata=True
                )
            if self.calibre_path == "lrf":
                import LiuXin_alpha.metadata.file_sources.lrf as lrf_module

                lrf: Any = lrf_module
                return lrf.get_metadata(stream, calibre_md=True)
            plugin = _plugin(self.fork_plugin)
            return plugin.get_metadata(stream, source_format)


class LiuXinReader(WEMIMetadataReader, CoverExtractor):
    """
    Read metadata and the cover as the application does today, placed by WEMI level.

    Subclasses are made by :func:`adapt_liuxin_reader`, one per fork reader
    plugin; ``fork_plugin`` is the plugin.

    Example:
        >>> from LiuXin_alpha.metadata.file_sources.builtin_readers import (
        ...     get_metadata_reader_plugins,
        ... )
        >>> pdf = next(p for p in get_metadata_reader_plugins() if "pdf" in p.file_types)
        >>> adapt_liuxin_reader(pdf).provider_id
        'liuxin.read-pdf-metadata'
    """

    fork_plugin: ClassVar[type[Any]]

    def read_wemi_metadata(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> WEMIFields:
        """
        Return the reader plugin's result, placed by WEMI level.

        Example:
            Called by a metadata run::

                fields = provider().read_wemi_metadata(path, "pdf", context)


        :param source: The file to read. It is not changed.
        :param source_format: Which of ``wemi_read_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The metadata, by level.
        :raises MetadataModelError: If the reader returned an unknown object.
        """
        fields, _ = self._read(source, source_format, context)
        return fields

    def extract_cover(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CoverImage | None:
        """
        Return the cover the reader plugin found.

        Example:
            Called by a metadata run::

                cover = provider().extract_cover(path, "pdf", context)


        :param source: The file to read. It is not changed.
        :param source_format: Which of ``cover_source_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The cover, or ``None``.
        :raises MetadataModelError: If the reader returned an unknown object.
        """
        _, cover = self._read(source, source_format, context)
        return cover

    def _read(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> tuple[WEMIFields, CoverImage | None]:
        """
        Read the file once and keep the answer for the unchanged file.

        Example:
            Both public methods go through here::

                fields, cover = self._read(source, "pdf", context)


        :param source: The file to read.
        :param source_format: Its format.
        :param context: Options, log, report and work folder for this call.
        :return: The fields by level and the cover.
        :raises MetadataModelError: If the reader returned an unknown object.
        """

        def read() -> tuple[object, CoverImage | None]:
            """
            Run the plugin and place its result by level.

            Example:
                Called once per unchanged file::

                    fields, cover = read()


            :return: The fields by level and the cover.
            """
            with source.open("rb") as stream:
                metadata = _plugin(self.fork_plugin).get_metadata(stream, source_format)
            if metadata is None:
                return WEMIFields({}), None
            if _is_liuxin_hybrid(metadata):
                values, cover = serialised_calibre(metadata.to_calibre(), context)
                fields = wemi_from_calibre(
                    values,
                    creators=_hybrid_roles(metadata),
                    identifiers=_hybrid_identifiers(metadata),
                )
                return fields, cover
            if _is_calibre_metadata(metadata):
                values, cover = serialised_calibre(metadata, context)
                return wemi_from_calibre(values), cover
            raise MetadataModelError(
                f"{self.provider_id} got {type(metadata).__name__} from its reader"
            )

        fields, cover = _cached(_file_key(type(self), source, source_format), read)
        return cast("WEMIFields", fields), cover


def _formats(plugin: type[Any]) -> frozenset[str]:
    """
    Normalise a plugin's formats.

    Example:
        >>> class Plugin:
        ...     file_types = {"EPUB", "kepub"}
        >>> sorted(_formats(Plugin))
        ['epub', 'kepub']


    :param plugin: The plugin class.
    :return: Its formats in normal form.
    """
    declared: object = getattr(plugin, "file_types", ())
    names = (
        normalize_format_name(str(name))
        for name in cast("Sequence[object]", list(cast("Any", declared)))
    )
    return frozenset(name for name in names if name)


def _make[P: ForkCalibreReader | LiuXinReader](
    base: type[P], name: str, attributes: Mapping[str, object]
) -> type[P]:
    """
    Build one provider class the way a ``class`` statement would.

    Example:
        >>> _make(LiuXinReader, "Made", {"priority": 2}).priority
        2


    :param base: The adapter base class.
    :param name: The class name.
    :param attributes: The class attributes.
    :return: The new class.
    """

    def fill(namespace: dict[str, object]) -> None:
        """
        Copy the attributes into the class body.

        Example:
            >>> _make(LiuXinReader, "Empty", {}).__name__
            'Empty'


        :param namespace: The class body being built.
        :return: None after the attributes are copied.
        """
        namespace.update(attributes)

    return cast("type[P]", types.new_class(name, (base,), exec_body=fill))


def adapt_fork_calibre_reader(plugin: type[Any]) -> type[ForkCalibreReader]:
    """
    Make a calibre-model provider for a fork reader with a calibre path.

    Example:
        >>> class Plugin:
        ...     name = "Read RB metadata"
        ...     version = (1, 0)
        ...     file_types = {"rb"}
        >>> adapt_fork_calibre_reader(Plugin).calibre_path
        'plugin'


    :param plugin: The fork's reader plugin class.
    :return: A new provider class; register it to use it.
    :raises KeyError: If the plugin has no calibre path.
    """
    name = str(getattr(plugin, "name", plugin.__name__))
    formats = _formats(plugin)
    return _make(
        ForkCalibreReader,
        f"{plugin.__name__}Fork",
        {
            "provider_id": provider_id_from_name(name, "fork"),
            "version": _version(plugin),
            "licence": LEGACY_LICENCE,
            "priority": LEGACY_PRIORITY,
            "calibre_read_formats": formats,
            "cover_source_formats": formats,
            "fork_plugin": plugin,
            "calibre_path": FORK_CALIBRE_READERS[name],
            "__module__": __name__,
            "__doc__": f"Run the fork's calibre path for {name!r}.",
        },
    )


def adapt_liuxin_reader(plugin: type[Any]) -> type[LiuXinReader]:
    """
    Make a WEMI-model provider for a fork reader plugin as the application runs it.

    Example:
        >>> class Plugin:
        ...     name = "Read PML metadata"
        ...     file_types = {"pml", "pmlz"}
        >>> sorted(adapt_liuxin_reader(Plugin).wemi_read_formats)
        ['pml', 'pmlz']


    :param plugin: The fork's reader plugin class.
    :return: A new provider class; register it to use it.
    """
    name = str(getattr(plugin, "name", plugin.__name__))
    formats = _formats(plugin)
    return _make(
        LiuXinReader,
        f"{plugin.__name__}LiuXin",
        {
            "provider_id": provider_id_from_name(name, "liuxin"),
            "version": _version(plugin),
            "licence": LEGACY_LICENCE,
            "priority": LEGACY_PRIORITY,
            "wemi_read_formats": formats,
            "cover_source_formats": formats,
            "fork_plugin": plugin,
            "__module__": __name__,
            "__doc__": f"Run {name!r} as the application does, placed by WEMI level.",
        },
    )


def _version(plugin: type[Any]) -> str:
    """
    Write a plugin's version as dotted text.

    Example:
        >>> class Plugin:
        ...     version = (1, 2)
        >>> _version(Plugin), _version(object)
        ('1.2', '0')


    :param plugin: The plugin class.
    :return: The version, or ``"0"`` if it gives none.
    """
    version: object = getattr(plugin, "version", None)
    if isinstance(version, tuple) and version:
        return ".".join(str(part) for part in cast("tuple[object, ...]", version))
    return "0"


def register_fork_metadata_readers(
    registry: ProviderRegistry,
) -> tuple[type[ForkCalibreReader | LiuXinReader], ...]:
    """
    Register the fork's calibre-path readers and the LiuXin readers.

    Example:
        Assemble a registry holding both sets::

            registry = ProviderRegistry()
            register_fork_metadata_readers(registry)


    :param registry: The registry to add the providers to.
    :return: The providers registered: fork providers first, then LiuXin.
    """
    from LiuXin_alpha.metadata.file_sources.builtin_readers import (
        get_metadata_reader_plugins,
    )

    plugins: Sequence[type[Any]] = list(get_metadata_reader_plugins())
    made: list[type[ForkCalibreReader | LiuXinReader]] = [
        adapt_fork_calibre_reader(plugin)
        for plugin in plugins
        if getattr(plugin, "name", None) in FORK_CALIBRE_READERS
    ]
    made.extend(adapt_liuxin_reader(plugin) for plugin in plugins)
    for provider in made:
        registry.register(provider)
    return tuple(made)
