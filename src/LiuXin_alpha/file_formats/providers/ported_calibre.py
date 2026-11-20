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

calibre's PDF reader runs poppler's ``pdfinfo`` and ``pdftoppm``. Its port is
available where ``pdfinfo`` is on the PATH. Below it,
:class:`PortedPdfReaderWithoutPoppler` runs the same calibre code on fields read
by LiuXin's own PDF object reader, so PDFs are read everywhere, without a cover.

calibre's ZIP, RAR and comic readers hand the books they find to the other ported
readers. RAR contents need an extraction tool that ``rarfile`` can run, so the RAR
port is available only where there is one; a CBR without one fails to read, as a
CB7 does without ``py7zr``. Archives are checked against LiuXin's archive limits
before calibre's code opens them.

Importing this module loads no reader.
"""

from __future__ import annotations

import types
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from LiuXin_alpha.file_formats.providers.base import (
    ProviderAvailability,
    ProviderContext,
    provider_id_from_name,
)
from LiuXin_alpha.file_formats.providers.fork_metadata import (
    reading_key,
    remember_reading,
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
    "Read OPF metadata": ("opf", frozenset({"opf"})),
    "Read EPUB metadata": ("epub", frozenset({"epub", "kepub"})),
    "Read HTMLZ metadata": ("extz", frozenset({"htmlz"})),
    "Read TXTZ metadata": ("extz", frozenset({"txtz"})),
    "Read HTML metadata": ("html", frozenset({"html"})),
    "Read FB2 metadata": ("fb2", frozenset({"fb2", "fbz"})),
    "Read ODT metadata": ("odt", frozenset({"odt"})),
    "Read MOBI metadata": ("mobi", frozenset({"mobi", "prc", "azw", "azw3", "azw4", "pobi"})),
    "Read PDB metadata": ("pdb", frozenset({"pdb", "updb"})),
    "Read LRF metadata": ("lrf", frozenset({"lrf"})),
    "Read DOCX metadata": ("docx", frozenset({"docx"})),
    "Read PDF metadata": ("pdf", frozenset({"pdf"})),
    "Read comic metadata": ("comic", frozenset({"cbz", "cbr", "cb7", "cbc"})),
    "Read ZIP metadata": ("zip", frozenset({"zip", "oebzip"})),
    "Read RAR metadata": ("rar", frozenset({"rar"})),
}
"""calibre's reader plugins that have been ported: module and formats."""

PORTED_PRIORITY = -50
"""Priority of every ported reader: above running an installed calibre."""

WITHOUT_POPPLER_PRIORITY = -55
"""Priority of the PDF reader that needs no poppler: below the port, which draws a cover."""

PASSES_FORMAT: frozenset[str] = frozenset({"epub", "comic"})
"""Ported modules whose ``get_metadata`` calibre's plugin calls with the format."""

ZIP_CONTAINERS: frozenset[str] = frozenset(
    {"epub", "extz", "fb2", "odt", "docx", "zip", "comic"}
)
"""Ported modules reading ZIP containers, which LiuXin checks before calibre opens them.

The FB2 reader reads plain FB2 too, and the comic reader RAR and 7-Zip comics; a
file that is not a ZIP passes unchecked."""


def ported_reader(module: str) -> Callable[..., Any]:
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
    if module == "opf":
        from LiuXin_alpha.metadata.upstream_calibre.opf import (
            get_metadata as get_opf_metadata,
        )

        def read_opf(stream: Any) -> Any:
            """
            Return the metadata, as calibre's OPF reader plugin does.

            calibre's ``opf.get_metadata`` also returns the OPF version, the
            raster cover and the first spine item; the plugin keeps the metadata.

            Example:
                Called by the provider with an open file::

                    metadata = read_opf(stream)


            :param stream: The OPF, open for reading bytes.
            :return: The metadata.
            """
            return get_opf_metadata(stream)[0]

        return read_opf
    if module == "epub":
        from LiuXin_alpha.metadata.upstream_calibre.epub import (
            get_metadata as get_epub_metadata,
        )

        return get_epub_metadata
    if module == "extz":
        from LiuXin_alpha.metadata.upstream_calibre.extz import (
            get_metadata as get_extz_metadata,
        )

        return get_extz_metadata
    if module == "html":
        from LiuXin_alpha.metadata.upstream_calibre.html import (
            get_metadata as get_html_metadata,
        )

        return get_html_metadata
    if module == "fb2":
        from LiuXin_alpha.metadata.upstream_calibre.fb2 import (
            get_metadata as get_fb2_metadata,
        )

        return get_fb2_metadata
    if module == "odt":
        from LiuXin_alpha.metadata.upstream_calibre.odt import (
            get_metadata as get_odt_metadata,
        )

        return get_odt_metadata
    if module == "mobi":
        from LiuXin_alpha.metadata.upstream_calibre.mobi import (
            get_metadata as get_mobi_metadata,
        )

        return get_mobi_metadata
    if module == "pdb":
        from LiuXin_alpha.metadata.upstream_calibre.pdb import (
            get_metadata as get_pdb_metadata,
        )

        return get_pdb_metadata
    if module == "lrf":
        from LiuXin_alpha.metadata.upstream_calibre.lrf import (
            get_metadata as get_lrf_metadata,
        )

        return get_lrf_metadata
    if module == "docx":
        from LiuXin_alpha.metadata.upstream_calibre.docx import (
            get_metadata as get_docx_metadata,
        )

        return get_docx_metadata
    if module == "pdf":
        from LiuXin_alpha.metadata.upstream_calibre.pdf import (
            get_metadata as get_pdf_metadata,
        )

        return get_pdf_metadata
    if module == "comic":
        from LiuXin_alpha.metadata.upstream_calibre.comic import (
            get_metadata as get_comic_metadata,
        )

        return get_comic_metadata
    if module == "zip":
        from LiuXin_alpha.metadata.upstream_calibre.zip import (
            get_metadata as get_zip_metadata,
        )

        return get_zip_metadata
    if module == "rar":
        from LiuXin_alpha.metadata.upstream_calibre.rar import (
            get_metadata as get_rar_metadata,
        )

        return get_rar_metadata
    raise KeyError(f"no calibre reader named {module!r} has been ported")


def check_zip_container(source: Path, source_format: str) -> None:
    """
    Refuse a ZIP container LiuXin's archive limits would refuse.

    calibre's readers read the members they need whole, and its EPUB reader
    extracts the whole book when it looks for a cover on the first page, so a
    small file that expands enormously would fill memory or disk. This applies
    the member-count, size and compression-ratio limits LiuXin's own EPUB input
    applies, before calibre's code runs. Entry names are not checked: the
    readers only read members, and Python's ``zipfile`` makes names safe when
    it extracts.

    A file whose central directory Python cannot read is left to calibre's
    salvage reader, which inflates in bounded pieces and stops at ratios beyond
    about a thousand to one.

    Example:
        >>> import os, tempfile, zipfile
        >>> path = os.path.join(tempfile.mkdtemp(), "bomb.epub")
        >>> with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        ...     zf.writestr("big", bytes(8 * 1024 * 1024))
        >>> check_zip_container(Path(path), "epub")  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        LiuXin_alpha.file_formats.archive_preflight.ArchivePreflightError: EPUB archive member has suspicious compression ratio: big (...)


    :param source: The file.
    :param source_format: Its format, for the messages.
    :return: None when the container is within the limits.
    :raises ArchivePreflightError: When it is not.
    """
    import zipfile

    from LiuXin_alpha.file_formats.archive_preflight import validate_zip_member_infos

    try:
        with zipfile.ZipFile(source) as archive:
            infos = archive.infolist()
    except (zipfile.BadZipFile, ValueError, OSError):
        return
    label = source_format.upper()
    validate_zip_member_infos(
        infos,
        container_label=f"{label} file",
        member_label=f"{label} archive",
        allow_unsafe_paths=True,
    )


def check_seven_zip_container(source: Path, source_format: str) -> None:
    """
    Refuse a 7-Zip comic LiuXin's archive limits would refuse.

    calibre reads a CB7's cover whole through ``py7zr``. Where ``py7zr`` is not
    installed, or the file is not 7-Zip, there is nothing to check: calibre's
    reader fails or reads another kind of archive. A member whose compressed size
    7-Zip does not record (in a solid block) is checked for its size alone.

    Example:
        >>> check_seven_zip_container(Path(__file__), "cbz")


    :param source: The file.
    :param source_format: Its format, for messages.
    :raises ArchivePreflightError: When it is not within the limits.
    """
    with source.open("rb") as stream:
        if not stream.read(6).startswith(b"7z\xbc\xaf"):
            return
    try:
        from py7zr import SevenZipFile  # type: ignore[import-not-found, unused-ignore]
    except ImportError:
        return
    from LiuXin_alpha.file_formats.archive_preflight import validate_zip_member_infos

    with SevenZipFile(source, "r") as archive:
        infos = [
            types.SimpleNamespace(
                filename=info.filename + ("/" if info.is_directory else ""),
                file_size=info.uncompressed or 0,
                compress_size=info.compressed or info.uncompressed or 0,
            )
            for info in archive.list()
        ]
    label = source_format.upper()
    validate_zip_member_infos(
        infos,
        container_label=f"{label} file",
        member_label=f"{label} archive",
        allow_unsafe_paths=True,
    )


def named_by_its_bytes(cover: CoverImage | None) -> CoverImage | None:
    """
    Name a cover's format by what its bytes are, where calibre's label differs.

    calibre's EPUB reader labels every cover it finds ``jpg``, whatever the
    image is, and other readers use calibre's short names. The bytes are
    checked with calibre's own ``imghdr``; a label is kept only when they are
    not recognised.

    Example:
        >>> named_by_its_bytes(CoverImage(b"\\x89PNG\\r\\n\\x1a\\n" + bytes(16), "jpg")).image_format
        'png'


    :param cover: The cover, or ``None``.
    :return: The cover, named for its bytes.
    """
    if cover is None:
        return None
    from LiuXin_alpha.metadata.upstream_calibre.imghdr import what

    found = what(None, cover.data)
    if not found or found == cover.image_format:
        return cover
    return CoverImage(cover.data, found)


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
            metadata = run_ported_reader(self.ported_module, source, source_format)
            if metadata is None:
                return CalibreFields({}), None
            values, cover = serialised_calibre(metadata, context)
            return CalibreFields(values), named_by_its_bytes(cover)

        fields, cover = remember_reading(
            reading_key(type(self), source, source_format), read
        )
        return cast("CalibreFields", fields), cover


def run_ported_reader(module: str, source: Path, source_format: str) -> Any:
    """
    Open a file and run a ported reader's ``get_metadata`` on it, as calibre's plugin calls it.

    Example:
        >>> run_ported_reader("txt", Path(__file__), "txt").title  # doctest: +SKIP
        'ported_calibre'


    :param module: The ported module.
    :param source: The file.
    :param source_format: Its format.
    :return: calibre's metadata object, or ``None``.
    """
    if module in ZIP_CONTAINERS:
        check_zip_container(source, source_format)
    if module == "comic":
        check_seven_zip_container(source, source_format)
    from LiuXin_alpha.metadata.upstream_calibre import _readers

    # calibre's archive readers find the readers for the books inside through here.
    _readers.lookup = ported_reader
    get_metadata = ported_reader(module)
    with source.open("rb") as stream:
        if module in PASSES_FORMAT:
            return get_metadata(stream, ftype=source_format)
        return get_metadata(stream)


class PortedPdfReader(PortedCalibreReader):
    """
    Run calibre's PDF reader, which needs poppler's ``pdfinfo`` (and ``pdftoppm`` for the cover).

    Example:
        >>> provider = adapt_ported_reader("Read PDF metadata")
        >>> issubclass(provider, PortedPdfReader), provider.requires
        (True, ('poppler',))
    """

    requires: ClassVar[tuple[str, ...]] = ("poppler",)

    @classmethod
    def availability(cls: type[Self]) -> ProviderAvailability:
        """
        Report whether poppler's ``pdfinfo`` is on the PATH.

        Example:
            >>> import os
            >>> saved = os.environ.get("PATH", "")
            >>> os.environ["PATH"] = "/no/such/folder"
            >>> PortedPdfReader.availability().available
            False
            >>> os.environ["PATH"] = saved


        :return: Ready if ``pdfinfo`` was found, otherwise why not.
        """
        from LiuXin_alpha.metadata.upstream_calibre._poppler import tool_directory

        if tool_directory() is None:
            return ProviderAvailability.missing(
                "poppler's pdfinfo was not found on the PATH; install poppler-utils, or "
                "read PDFs with calibre-port.read-pdf-metadata-without-poppler"
            )
        return ProviderAvailability.ready()


class PortedRarReader(PortedCalibreReader):
    """
    Run calibre's RAR reader, which needs a tool ``rarfile`` can extract RAR contents with.

    Example:
        >>> provider = adapt_ported_reader("Read RAR metadata")
        >>> issubclass(provider, PortedRarReader), provider.requires
        (True, ('unrar',))
    """

    requires: ClassVar[tuple[str, ...]] = ("unrar",)

    @classmethod
    def availability(cls: type[Self]) -> ProviderAvailability:
        """
        Report whether ``rarfile`` has a tool to extract RAR contents with.

        Example:
            >>> PortedRarReader.availability().available in (True, False)
            True


        :return: Ready if it has, otherwise why not.
        """
        from LiuXin_alpha.metadata.upstream_calibre._unrar import available

        ready, reason = available()
        if not ready:
            return ProviderAvailability.missing(
                f"{reason}; install unrar, unar, 7-Zip or bsdtar to read RAR archives"
            )
        return ProviderAvailability.ready()


class PortedPdfReaderWithoutPoppler(CalibreMetadataReader):
    """
    Run calibre's PDF reader on fields read by LiuXin's own object reader, so no poppler is needed.

    calibre's ``get_metadata`` turns what ``pdfinfo`` prints into metadata. Here
    the same fields come from :mod:`LiuXin_alpha.file_formats.pdf.objects`, and
    calibre's code does the rest. Drawing a cover needs poppler, so there is none.

    Example:
        >>> PortedPdfReaderWithoutPoppler.provider_id, PortedPdfReaderWithoutPoppler.priority
        ('calibre-port.read-pdf-metadata-without-poppler', -55)
    """

    provider_id: ClassVar[str] = "calibre-port.read-pdf-metadata-without-poppler"
    version: ClassVar[str] = UPSTREAM_CALIBRE_VERSION
    licence: ClassVar[str] = "GPL-3.0-only"
    priority: ClassVar[int] = WITHOUT_POPPLER_PRIORITY
    calibre_read_formats: ClassVar[frozenset[str]] = frozenset({"pdf"})

    def read_calibre_metadata(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CalibreFields:
        """
        Return what calibre's PDF reader says about the file, without poppler.

        Example:
            Called by a metadata run::

                fields = provider().read_calibre_metadata(path, "pdf", context)


        :param source: The file to read. It is not changed.
        :param source_format: ``pdf``.
        :param context: Options, log, report and work folder for this call.
        :return: The metadata, in calibre's names and shapes.
        """

        def read() -> tuple[object, CoverImage | None]:
            """
            Run the reader with LiuXin's fields in place of poppler's.

            Example:
                Called once per unchanged file::

                    fields, _ = read()


            :return: The fields, and no cover.
            """
            from LiuXin_alpha.metadata.upstream_calibre._poppler import without_poppler

            with without_poppler():
                metadata = run_ported_reader("pdf", source, source_format)
            values, _ = serialised_calibre(metadata, context)
            return CalibreFields(values), None

        fields, _ = remember_reading(reading_key(type(self), source, source_format), read)
        return cast("CalibreFields", fields)


def adapt_ported_reader(plugin_name: str) -> type[PortedCalibreReader]:
    """
    Make a provider class for one ported calibre reader.

    Example:
        >>> adapt_ported_reader("Read LRX metadata").priority
        -50


    :param plugin_name: The reader plugin's name in calibre. The PDF reader's
        class derives from :class:`PortedPdfReader`, which needs poppler, and the
        RAR reader's from :class:`PortedRarReader`, which needs an extraction tool.
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

    name = "".join(word.capitalize() for word in plugin_name.split()[1:-1]) + "Ported"
    base = {"pdf": PortedPdfReader, "rar": PortedRarReader}.get(module, PortedCalibreReader)
    made = types.new_class(name, (base,), exec_body=fill)
    return cast("type[PortedCalibreReader]", made)


def register_ported_calibre_readers(
    registry: ProviderRegistry,
) -> tuple[type[PortedCalibreReader] | type[PortedPdfReaderWithoutPoppler], ...]:
    """
    Register a provider for every ported calibre reader.

    Example:
        >>> from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry
        >>> [p.provider_id for p in register_ported_calibre_readers(ProviderRegistry())]
        ['calibre-port.read-txt-metadata', 'calibre-port.read-imp-metadata', 'calibre-port.read-rb-metadata', 'calibre-port.read-lrx-metadata', 'calibre-port.read-snb-metadata', 'calibre-port.read-rtf-metadata', 'calibre-port.read-topaz-metadata', 'calibre-port.read-pml-metadata', 'calibre-port.read-opf-metadata', 'calibre-port.read-epub-metadata', 'calibre-port.read-htmlz-metadata', 'calibre-port.read-txtz-metadata', 'calibre-port.read-html-metadata', 'calibre-port.read-fb2-metadata', 'calibre-port.read-odt-metadata', 'calibre-port.read-mobi-metadata', 'calibre-port.read-pdb-metadata', 'calibre-port.read-lrf-metadata', 'calibre-port.read-docx-metadata', 'calibre-port.read-pdf-metadata', 'calibre-port.read-comic-metadata', 'calibre-port.read-zip-metadata', 'calibre-port.read-rar-metadata', 'calibre-port.read-pdf-metadata-without-poppler']


    :param registry: The registry to add the providers to.
    :return: The provider classes registered, ending with the PDF reader that
        needs no poppler.
    """
    made: tuple[type[PortedCalibreReader] | type[PortedPdfReaderWithoutPoppler], ...] = (
        *(adapt_ported_reader(name) for name in PORTED_READERS),
        PortedPdfReaderWithoutPoppler,
    )
    for provider in made:
        registry.register(provider)
    return made
