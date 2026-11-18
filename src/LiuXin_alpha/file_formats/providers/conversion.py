"""
Conversion capabilities: importers, exporters, transforms and converters.

A conversion is a route through providers. An importer reads a source file
into an intermediate book, transforms change that book, and an exporter
writes it out in a target format. A converter does a whole conversion from
one file to another without an intermediate. There are two intermediates, the
OEB book and the HTML book, and each has its own importer, exporter and
transform class so that a provider's base class says exactly what it produces
or consumes.

One provider class may offer several of these capabilities. For that reason
every capability uses its own attribute and method names: a class that both
imports EPUB into OEB and exports OEB to EPUB declares ``oeb_source_formats``
and ``oeb_target_formats`` side by side, and the two never collide.

Nothing here runs a conversion. The classes are the contracts a runner will
call. The OEB and HTML book types are named for type checking only and are
not imported when this module is.
"""

from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Self, cast

from LiuXin_alpha.file_formats.conversion.edges import (
    ConversionEdge,
    direct_edge,
    external_tool_edge,
)
from LiuXin_alpha.file_formats.providers.base import (
    PROVIDER_ID_PATTERN,
    Provider,
    ProviderContext,
    ProviderDefinitionError,
    declared,
    declared_formats,
    is_format_name,
)
from LiuXin_alpha.file_formats.providers.stages import HTMLStage, OEBStage

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.html.book import HTMLBook
    from LiuXin_alpha.file_formats.oeb.base import OEBBook


class Importer(Provider):
    """
    Group the providers that read a source file into an intermediate book.

    This class exists so that importers can be asked for as a family. A
    provider never subclasses it directly; it subclasses the class for the
    intermediate it produces, :class:`OEBImporter` or :class:`HTMLImporter`.

    Example:
        >>> issubclass(OEBImporter, Importer), issubclass(HTMLImporter, Importer)
        (True, True)
    """


class Exporter(Provider):
    """
    Group the providers that write an intermediate book out in a format.

    This class exists so that exporters can be asked for as a family. A
    provider never subclasses it directly; it subclasses the class for the
    intermediate it consumes, :class:`OEBExporter` or :class:`HTMLExporter`.

    Example:
        >>> issubclass(OEBExporter, Exporter), issubclass(HTMLExporter, Exporter)
        (True, True)
    """


class Transform(Provider):
    """
    Group the providers that change an intermediate book into itself.

    This class exists so that transforms can be asked for as a family. A
    provider never subclasses it directly; it subclasses the class for the
    intermediate it works on, :class:`OEBTransform` or :class:`HTMLTransform`.

    Example:
        >>> issubclass(OEBTransform, Transform), issubclass(HTMLTransform, Transform)
        (True, True)
    """


class OEBImporter(Importer):
    """
    Read a source file into an OEB book.

    A subclass sets ``oeb_source_formats`` to the formats it can read and
    implements :meth:`import_oeb`.

    Example:
        >>> class TextImporter(OEBImporter):
        ...     provider_id = "txt.oeb-importer.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     oeb_source_formats = frozenset({"txt"})
        ...     def import_oeb(self, source, source_format, context):
        ...         raise NotImplementedError
        >>> TextImporter.validate_declaration()
        >>> sorted(TextImporter.oeb_source_formats)
        ['txt']
    """

    oeb_source_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this importer reads.

        Example:
            >>> class Bad(OEBImporter):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     oeb_source_formats = frozenset({".EPUB"})
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.oeb_source_formats must hold lower-case format names with no leading dot, got '.EPUB'


        :return: ``None`` when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "oeb_source_formats")

    @abstractmethod
    def import_oeb(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> OEBBook:
        """
        Read one source file and return it as an OEB book.

        Example:
            A runner calls this once per conversion::

                book = importer.import_oeb(Path("novel.txt"), "txt", context)


        :param source: The file to read. It must not be changed.
        :param source_format: Which of ``oeb_source_formats`` the file is in.
        :param context: Options, log, report and scratch folder for this call.
        :return: The book, with any files it needs under ``context.work_dir``.
        """


class HTMLImporter(Importer):
    """
    Read a source file into an HTML book.

    A subclass sets ``html_source_formats`` to the formats it can read and
    implements :meth:`import_html`.

    Example:
        >>> class PdfImporter(HTMLImporter):
        ...     provider_id = "pdf.html-importer.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     html_source_formats = frozenset({"pdf"})
        ...     def import_html(self, source, source_format, context):
        ...         raise NotImplementedError
        >>> PdfImporter.validate_declaration()
        >>> sorted(PdfImporter.html_source_formats)
        ['pdf']
    """

    html_source_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this importer reads.

        Example:
            >>> class Bad(HTMLImporter):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.html_source_formats must be set


        :return: ``None`` when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "html_source_formats")

    @abstractmethod
    def import_html(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> HTMLBook:
        """
        Read one source file and return it as an HTML book.

        Example:
            A runner calls this once per conversion::

                book = importer.import_html(Path("paper.pdf"), "pdf", context)


        :param source: The file to read. It must not be changed.
        :param source_format: Which of ``html_source_formats`` the file is in.
        :param context: Options, log, report and scratch folder for this call.
        :return: The book, whose root folder is under ``context.work_dir``.
        """


class OEBExporter(Exporter):
    """
    Write an OEB book out in a target format.

    A subclass sets ``oeb_target_formats`` to the formats it can write and
    implements :meth:`export_oeb`. If the book must be shaped for this output
    first, the subclass names the transforms that do it in
    ``required_oeb_transforms``, by provider id and in the order to run, so
    that the runner applies them and the exporter does not call them itself.

    Example:
        >>> class TextExporter(OEBExporter):
        ...     provider_id = "txt.oeb-exporter.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     oeb_target_formats = frozenset({"txt"})
        ...     def export_oeb(self, book, target, target_format, context):
        ...         raise NotImplementedError
        >>> TextExporter.validate_declaration()
        >>> TextExporter.required_oeb_transforms
        ()
    """

    oeb_target_formats: ClassVar[frozenset[str]]
    required_oeb_transforms: ClassVar[tuple[str, ...]] = ()

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record, the target formats and the required transforms.

        Example:
            >>> class Bad(OEBExporter):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     oeb_target_formats = frozenset({"txt"})
            ...     required_oeb_transforms = ("Split Pages",)
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.required_oeb_transforms must be a tuple of provider ids, got ('Split Pages',)


        :return: ``None`` when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "oeb_target_formats")
        declared_provider_ids(cls, "required_oeb_transforms")

    @abstractmethod
    def export_oeb(
        self: Self,
        book: OEBBook,
        target: Path,
        target_format: str,
        context: ProviderContext,
    ) -> None:
        """
        Write an OEB book to one target file.

        Example:
            A runner calls this once per conversion::

                exporter.export_oeb(book, Path("novel.txt"), "txt", context)


        :param book: The book to write, after every transform has run.
        :param target: Where to write. The exporter creates this file.
        :param target_format: Which of ``oeb_target_formats`` to write.
        :param context: Options, log, report and scratch folder for this call.
        :return: ``None``.
        """


class HTMLExporter(Exporter):
    """
    Write an HTML book out in a target format.

    A subclass sets ``html_target_formats`` to the formats it can write and
    implements :meth:`export_html`. ``required_html_transforms`` names, by
    provider id and in the order to run, the transforms the runner must apply
    before this exporter is called.

    Example:
        >>> class PdfExporter(HTMLExporter):
        ...     provider_id = "pdf.html-exporter.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     html_target_formats = frozenset({"pdf"})
        ...     def export_html(self, book, target, target_format, context):
        ...         raise NotImplementedError
        >>> PdfExporter.validate_declaration()
        >>> PdfExporter.required_html_transforms
        ()
    """

    html_target_formats: ClassVar[frozenset[str]]
    required_html_transforms: ClassVar[tuple[str, ...]] = ()

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record, the target formats and the required transforms.

        Example:
            >>> class Bad(HTMLExporter):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     html_target_formats = frozenset()
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.html_target_formats must be a non-empty frozenset of format names, got frozenset()


        :return: ``None`` when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "html_target_formats")
        declared_provider_ids(cls, "required_html_transforms")

    @abstractmethod
    def export_html(
        self: Self,
        book: HTMLBook,
        target: Path,
        target_format: str,
        context: ProviderContext,
    ) -> None:
        """
        Write an HTML book to one target file.

        Example:
            A runner calls this once per conversion::

                exporter.export_html(book, Path("paper.pdf"), "pdf", context)


        :param book: The book to write. Its folder must not be changed.
        :param target: Where to write. The exporter creates this file.
        :param target_format: Which of ``html_target_formats`` to write.
        :param context: Options, log, report and scratch folder for this call.
        :return: ``None``.
        """


class OEBTransform(Transform):
    """
    Change an OEB book.

    A subclass sets ``oeb_stage`` to the stage it runs in and implements
    :meth:`transform_oeb`. Stages run in the order :class:`OEBStage` defines
    them; within a stage, the transform with the higher ``priority`` runs
    first.

    Example:
        >>> class Tidy(OEBTransform):
        ...     provider_id = "oeb.tidy.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     oeb_stage = OEBStage.NORMALISE
        ...     def transform_oeb(self, book, context):
        ...         return book
        >>> Tidy.validate_declaration()
        >>> Tidy().applies_to_oeb(object(), object())
        True
    """

    oeb_stage: ClassVar[OEBStage]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the stage this transform runs in.

        Example:
            >>> class Bad(OEBTransform):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     oeb_stage = "structure"
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.oeb_stage must be a member of OEBStage, got 'structure'


        :return: ``None`` when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        stage = declared(cls, "oeb_stage")
        if not isinstance(stage, OEBStage):
            raise ProviderDefinitionError(
                f"{cls.__name__}.oeb_stage must be a member of OEBStage, got {stage!r}"
            )

    def applies_to_oeb(self: Self, book: OEBBook, context: ProviderContext) -> bool:
        """
        Decide whether this transform has anything to do for this call.

        The default says yes. A transform that an option switches on, or that
        only matters for some books, overrides this so that the runner can
        skip it and record that it was skipped.

        Example:
            A runner asks before calling the transform::

                if transform.applies_to_oeb(book, context):
                    book = transform.transform_oeb(book, context)


        :param book: The book as it stands when this transform's turn comes.
        :param context: Options, log, report and scratch folder for this call.
        :return: ``True`` if :meth:`transform_oeb` should be called.
        """
        return True

    @abstractmethod
    def transform_oeb(self: Self, book: OEBBook, context: ProviderContext) -> OEBBook:
        """
        Change an OEB book and return the book to carry on with.

        The transform may alter the book it is given and return it, or build
        and return another. The caller uses only what is returned.

        Example:
            A runner threads the book through each transform in turn::

                book = transform.transform_oeb(book, context)


        :param book: The book as it stands when this transform's turn comes.
        :param context: Options, log, report and scratch folder for this call.
        :return: The book to pass to the next step.
        """


class HTMLTransform(Transform):
    """
    Change an HTML book.

    A subclass sets ``html_stage`` to the stage it runs in and implements
    :meth:`transform_html`. Stages run in the order :class:`HTMLStage` defines
    them; within a stage, the transform with the higher ``priority`` runs
    first.

    Example:
        >>> class Reencode(HTMLTransform):
        ...     provider_id = "html.reencode.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     html_stage = HTMLStage.NORMALISE
        ...     def transform_html(self, book, context):
        ...         return book
        >>> Reencode.validate_declaration()
        >>> Reencode().applies_to_html(object(), object())
        True
    """

    html_stage: ClassVar[HTMLStage]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the stage this transform runs in.

        Example:
            >>> class Bad(HTMLTransform):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     html_stage = OEBStage.NORMALISE
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.html_stage must be a member of HTMLStage, got <OEBStage.NORMALISE: 'normalise'>


        :return: ``None`` when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        stage = declared(cls, "html_stage")
        if not isinstance(stage, HTMLStage):
            raise ProviderDefinitionError(
                f"{cls.__name__}.html_stage must be a member of HTMLStage, "
                f"got {stage!r}"
            )

    def applies_to_html(self: Self, book: HTMLBook, context: ProviderContext) -> bool:
        """
        Decide whether this transform has anything to do for this call.

        The default says yes. A transform that an option switches on, or that
        only matters for some books, overrides this so that the runner can
        skip it and record that it was skipped.

        Example:
            A runner asks before calling the transform::

                if transform.applies_to_html(book, context):
                    book = transform.transform_html(book, context)


        :param book: The book as it stands when this transform's turn comes.
        :param context: Options, log, report and scratch folder for this call.
        :return: ``True`` if :meth:`transform_html` should be called.
        """
        return True

    @abstractmethod
    def transform_html(
        self: Self, book: HTMLBook, context: ProviderContext
    ) -> HTMLBook:
        """
        Produce a changed HTML book without touching the one given.

        An HTML book describes a folder and never changes it. A transform that
        alters anything writes a new folder under ``context.work_dir`` and
        returns a book that describes it. A transform with nothing to change
        returns the book it was given.

        Example:
            A runner threads the book through each transform in turn::

                book = transform.transform_html(book, context)


        :param book: The book as it stands when this transform's turn comes.
        :param context: Options, log, report and scratch folder for this call.
        :return: The book to pass to the next step.
        """


class Converter(Provider):
    """
    Convert a file in one format straight to a file in another.

    A subclass sets ``conversions`` to the exact ``(source, target)`` format
    pairs it handles and implements :meth:`convert`. Pairs are listed one by
    one because a converter rarely handles every combination of the formats
    it knows: a compressor may turn PDF into PDF and EPUB into EPUB without
    turning either into the other. A pair whose two formats are the same is
    allowed, and is how a same-format route such as compression is declared.

    A converter that drives a program outside Python names it in
    ``external_tool``. ``lossless`` says whether the result keeps everything
    the source held; leave it ``None`` when that is not known.

    Example:
        >>> class Compress(Converter):
        ...     provider_id = "pdf.compress.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     conversions = frozenset({("pdf", "pdf")})
        ...     lossless = True
        ...     def convert(self, source, source_format, target, target_format, context):
        ...         raise NotImplementedError
        >>> Compress.validate_declaration()
        >>> [(edge.name, edge.kind.value) for edge in Compress.edges()]
        [('pdf.compress.example:pdf->pdf', 'direct')]
    """

    conversions: ClassVar[frozenset[tuple[str, str]]]
    external_tool: ClassVar[str | None] = None
    lossless: ClassVar[bool | None] = None

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the conversions this converter offers.

        Example:
            >>> class Bad(Converter):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     conversions = frozenset({("pdf", "HTML")})
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.conversions must hold (source, target) pairs of lower-case format names, got ('pdf', 'HTML')


        :return: ``None`` when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_conversions(cls)
        tool = declared(cls, "external_tool")
        if tool is not None and not (isinstance(tool, str) and tool.strip()):
            raise ProviderDefinitionError(
                f"{cls.__name__}.external_tool must be None or a non-empty "
                f"string, got {tool!r}"
            )
        lossless = declared(cls, "lossless")
        if lossless is not None and not isinstance(lossless, bool):
            raise ProviderDefinitionError(
                f"{cls.__name__}.lossless must be None, True or False, got {lossless!r}"
            )

    @classmethod
    def edges(cls: type[Self]) -> tuple[ConversionEdge, ...]:
        """
        Describe each conversion this converter offers as a conversion edge.

        The edges are in the form the existing edge registry stores, so a
        converter can be listed beside the routes that go through OEB. Each is
        named after the provider and the pair, and is an external edge when
        ``external_tool`` is set and a direct edge otherwise. Edge priority is
        left at the default for its kind: an edge says that a step exists,
        and choosing between the providers able to perform it is done by
        provider priority.

        Example:
            >>> class Tool(Converter):
            ...     provider_id = "pdf.pdftohtml.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     conversions = frozenset({("pdf", "html")})
            ...     external_tool = "pdftohtml"
            ...     def convert(self, source, source_format, target, target_format, context):
            ...         raise NotImplementedError
            >>> edge, = Tool.edges()
            >>> edge.kind.value, edge.external_tool, edge.source_format
            ('external', 'pdftohtml', 'pdf')


        :return: One edge per declared pair, ordered by source then target.
        :raises ProviderDefinitionError: If the declaration is malformed.
        """
        cls.validate_declaration()
        provider_id = cls.provider_id
        found: list[ConversionEdge] = []
        for source, target in sorted(cls.conversions):
            name = f"{provider_id}:{source}->{target}"
            if cls.external_tool is None:
                found.append(
                    direct_edge(source, target, name=name, lossless=cls.lossless)
                )
            else:
                found.append(
                    external_tool_edge(
                        source,
                        target,
                        external_tool=cls.external_tool,
                        name=name,
                        lossless=cls.lossless,
                    )
                )
        return tuple(found)

    @abstractmethod
    def convert(
        self: Self,
        source: Path,
        source_format: str,
        target: Path,
        target_format: str,
        context: ProviderContext,
    ) -> None:
        """
        Convert one source file into one target file.

        Example:
            A runner calls this once for a step of a route::

                converter.convert(
                    Path("in.pdf"), "pdf", Path("out.pdf"), "pdf", context
                )


        :param source: The file to read. It must not be changed.
        :param source_format: The format of ``source``.
        :param target: Where to write. The converter creates this file, and
            it is never the same path as ``source``.
        :param target_format: The format to write; with ``source_format`` it
            is one of ``conversions``.
        :param context: Options, log, report and scratch folder for this call.
        :return: ``None``.
        """


def declared_provider_ids(provider: type[Provider], name: str) -> tuple[str, ...]:
    """
    Read a record attribute that must be a tuple of provider ids.

    Example:
        >>> class Demo(Provider):
        ...     required_oeb_transforms = ("oeb.split.builtin",)
        >>> declared_provider_ids(Demo, "required_oeb_transforms")
        ('oeb.split.builtin',)


    :param provider: The provider class being checked.
    :param name: The attribute to read.
    :return: The declared ids, in the order given.
    :raises ProviderDefinitionError: If it is unset, not a tuple, or holds
        something that is not a provider id.
    """
    value = declared(provider, name)
    if isinstance(value, tuple):
        items = cast("tuple[object, ...]", value)
        ids = tuple(
            item
            for item in items
            if isinstance(item, str) and PROVIDER_ID_PATTERN.fullmatch(item)
        )
        if len(ids) == len(items):
            return ids
    raise ProviderDefinitionError(
        f"{provider.__name__}.{name} must be a tuple of provider ids, got {value!r}"
    )


def declared_conversions(provider: type[Provider]) -> frozenset[tuple[str, str]]:
    """
    Read the ``conversions`` a converter class declares.

    Example:
        >>> class Demo(Provider):
        ...     conversions = frozenset({("pdf", "html"), ("pdf", "pdf")})
        >>> sorted(declared_conversions(Demo))
        [('pdf', 'html'), ('pdf', 'pdf')]


    :param provider: The provider class being checked.
    :return: The declared ``(source, target)`` pairs.
    :raises ProviderDefinitionError: If it is unset, not a frozen set, empty,
        or holds something that is not a pair of format names.
    """
    value = declared(provider, "conversions")
    if not isinstance(value, frozenset) or not value:
        raise ProviderDefinitionError(
            f"{provider.__name__}.conversions must be a non-empty frozenset of "
            f"(source, target) pairs, got {value!r}"
        )
    pairs: set[tuple[str, str]] = set()
    for item in cast("frozenset[object]", value):
        pair = _format_pair(item)
        if pair is None:
            raise ProviderDefinitionError(
                f"{provider.__name__}.conversions must hold (source, target) "
                f"pairs of lower-case format names, got {item!r}"
            )
        pairs.add(pair)
    return frozenset(pairs)


def _format_pair(value: object) -> tuple[str, str] | None:
    """
    Read a value as a pair of format names, if it is one.

    Example:
        >>> _format_pair(("pdf", "html")), _format_pair(("pdf",)), _format_pair("ab")
        (('pdf', 'html'), None, None)


    :param value: The candidate, of any type.
    :return: The pair, or ``None`` if the value is not two format names.
    """
    if not isinstance(value, tuple):
        return None
    items = cast("tuple[object, ...]", value)
    if len(items) != 2:
        return None
    source, target = items
    if not isinstance(source, str) or not isinstance(target, str):
        return None
    if not is_format_name(source) or not is_format_name(target):
        return None
    return source, target
