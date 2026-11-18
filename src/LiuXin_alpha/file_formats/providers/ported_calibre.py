"""
Read metadata with the readers ported from current upstream calibre.

The ported readers live in ``metadata.upstream_calibre``: calibre 9.18's own
reader code, changed only in its imports. These providers run them in
process, as calibre's reader plugins do, and serialise the result the same
way as the other calibre-model providers.

A port runs without calibre installed and without starting a process, so it
is preferred over the provider that runs an installed calibre for the same
reader: :data:`PORTED_PRIORITY` is above :data:`upstream_calibre.UPSTREAM_PRIORITY`.
Formats not ported yet are left to that provider.

Importing this module loads no reader.
"""

from __future__ import annotations

import types
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from LiuXin_alpha.file_formats.providers.base import (
    ProviderContext,
    provider_id_from_name,
)
from LiuXin_alpha.file_formats.providers.fork_metadata import (
    remember_reading,
    reading_key,
    serialised_calibre,
)
from LiuXin_alpha.file_formats.providers.metadata import (
    CalibreFields,
    CalibreMetadataReader,
    CoverExtractor,
    CoverImage,
)
from LiuXin_alpha.file_formats.providers.upstream_calibre import (
    UPSTREAM_CALIBRE_VERSION,
)

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry

PORTED_READERS: Mapping[str, tuple[str, frozenset[str]]] = {
    "Read TXT metadata": ("txt", frozenset({"txt"})),
    "Read IMP metadata": ("imp", frozenset({"imp"})),
    "Read RB metadata": ("rb", frozenset({"rb"})),
    "Read LRX metadata": ("lrx", frozenset({"lrx"})),
    "Read SNB metadata": ("snb", frozenset({"snb"})),
    "Read RTF metadata": ("rtf", frozenset({"rtf"})),
    "Read Topaz metadata": ("topaz", frozenset({"tpz", "azw1"})),
    "Read PML metadata": ("pml", frozenset({"pml", "pmlz"})),
}
"""calibre's reader plugins that have been ported: module and formats."""

PORTED_PRIORITY = -50
"""Priority of every ported reader: above running an installed calibre."""


def ported_reader(module: str) -> Callable[[Any], Any]:
    """
    Load the ``get_metadata`` function of one ported reader module.

    Example:
        >>> ported_reader("txt").__module__
        'LiuXin_alpha.metadata.upstream_calibre.txt'


    :param module: The module's name within ``metadata.upstream_calibre``.
    :return: Its ``get_metadata``, which takes an open binary stream.
    :raises KeyError: If no reader of that name has been ported.
    """
    if module == "txt":
        from LiuXin_alpha.metadata.upstream_calibre.txt import get_metadata

        return get_metadata
    if module == "imp":
        from LiuXin_alpha.metadata.upstream_calibre.imp import (
            get_metadata as get_imp_metadata,
        )

        return get_imp_metadata
    if module == "rb":
        from LiuXin_alpha.metadata.upstream_calibre.rb import (
            get_metadata as get_rb_metadata,
        )

        return get_rb_metadata
    if module == "lrx":
        from LiuXin_alpha.metadata.upstream_calibre.lrx import (
            get_metadata as get_lrx_metadata,
        )

        return get_lrx_metadata
    if module == "snb":
        from LiuXin_alpha.metadata.upstream_calibre.snb import (
            get_metadata as get_snb_metadata,
        )

        return get_snb_metadata
    if module == "rtf":
        from LiuXin_alpha.metadata.upstream_calibre.rtf import (
            get_metadata as get_rtf_metadata,
        )

        return get_rtf_metadata
    if module == "topaz":
        from LiuXin_alpha.metadata.upstream_calibre.topaz import (
            get_metadata as get_topaz_metadata,
        )

        return get_topaz_metadata
    if module == "pml":
        from LiuXin_alpha.metadata.upstream_calibre.pml import (
            get_metadata as get_pml_metadata,
        )

        return get_pml_metadata
    raise KeyError(f"no calibre reader named {module!r} has been ported")


class PortedCalibreReader(CalibreMetadataReader, CoverExtractor):
    """
    Read metadata and the cover with a reader ported from upstream calibre.

    Subclasses are made by :func:`adapt_ported_reader`; ``ported_module``
    names the module in ``metadata.upstream_calibre``.

    Example:
        >>> provider = adapt_ported_reader("Read RB metadata")
        >>> provider.provider_id, sorted(provider.calibre_read_formats)
        ('calibre-port.read-rb-metadata', ['rb'])
    """

    ported_module: ClassVar[str]

    def read_calibre_metadata(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CalibreFields:
        """
        Return what the ported reader says about the file.

        Example:
            Called by a metadata run::

                fields = provider().read_calibre_metadata(path, "rb", context)


        :param source: The file to read. It is not changed.
        :param source_format: Which of ``calibre_read_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The metadata, in calibre's names and shapes.
        """
        fields, _ = self._read(source, source_format, context)
        return fields

    def extract_cover(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CoverImage | None:
        """
        Return the cover the ported reader found.

        Example:
            Called by a metadata run::

                cover = provider().extract_cover(path, "rb", context)


        :param source: The file to read. It is not changed.
        :param source_format: Which of ``cover_source_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The cover, or ``None``.
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

                fields, cover = self._read(source, "rb", context)


        :param source: The file to read.
        :param source_format: Its format.
        :param context: Options, log, report and work folder for this call.
        :return: The fields and the cover.
        """

        def read() -> tuple[object, CoverImage | None]:
            """
            Run the ported reader and serialise its result.

            Example:
                Called once per unchanged file::

                    fields, cover = read()


            :return: The fields and the cover.
            """
            get_metadata = ported_reader(self.ported_module)
            with source.open("rb") as stream:
                metadata = get_metadata(stream)
            if metadata is None:
                return CalibreFields({}), None
            values, cover = serialised_calibre(metadata, context)
            return CalibreFields(values), cover

        fields, cover = remember_reading(
            reading_key(type(self), source, source_format), read
        )
        return cast("CalibreFields", fields), cover


def adapt_ported_reader(plugin_name: str) -> type[PortedCalibreReader]:
    """
    Make a provider class for one ported calibre reader.

    Example:
        >>> adapt_ported_reader("Read LRX metadata").priority
        -50


    :param plugin_name: The reader plugin's name in calibre.
    :return: A new provider class; register it to use it.
    :raises KeyError: If that reader has not been ported.
    """
    module, formats = PORTED_READERS[plugin_name]
    attributes: dict[str, object] = {
        "provider_id": provider_id_from_name(plugin_name, "calibre-port"),
        "version": UPSTREAM_CALIBRE_VERSION,
        "licence": "GPL-3.0-only",
        "priority": PORTED_PRIORITY,
        "calibre_read_formats": formats,
        "cover_source_formats": formats,
        "ported_module": module,
        "__module__": __name__,
        "__doc__": f"Run calibre's {plugin_name!r} reader, ported, in process.",
    }

    def fill(namespace: dict[str, object]) -> None:
        """
        Copy the attributes into the class body.

        Example:
            >>> adapt_ported_reader("Read TXT metadata").ported_module
            'txt'


        :param namespace: The class body being built.
        :return: None after the attributes are copied.
        """
        namespace.update(attributes)

    name = f"{module.capitalize()}Ported"
    made = types.new_class(name, (PortedCalibreReader,), exec_body=fill)
    return cast("type[PortedCalibreReader]", made)


def register_ported_calibre_readers(
    registry: ProviderRegistry,
) -> tuple[type[PortedCalibreReader], ...]:
    """
    Register a provider for every ported calibre reader.

    Example:
        >>> from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry
        >>> [p.provider_id for p in register_ported_calibre_readers(ProviderRegistry())]
        ['calibre-port.read-txt-metadata', 'calibre-port.read-imp-metadata', 'calibre-port.read-rb-metadata', 'calibre-port.read-lrx-metadata', 'calibre-port.read-snb-metadata', 'calibre-port.read-rtf-metadata', 'calibre-port.read-topaz-metadata', 'calibre-port.read-pml-metadata']


    :param registry: The registry to add the providers to.
    :return: The provider classes registered.
    """
    made = tuple(adapt_ported_reader(name) for name in PORTED_READERS)
    for provider in made:
        registry.register(provider)
    return made
