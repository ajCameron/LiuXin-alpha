"""
Check the capability base classes that file-format providers subclass.

Nothing here converts a file. The tests cover what a declaration must
contain, what makes one malformed, and the rule that lets one class offer
several capabilities. Variants that differ in one attribute are built with
:func:`tests.support.format_providers.declare`.
"""

from __future__ import annotations

import dataclasses
import doctest
import importlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from LiuXin_alpha.file_formats.conversion.edges import ConversionEdgeKind
from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers.base import (
    Provider,
    ProviderAvailability,
    ProviderContext,
    ProviderDefinitionError,
    validate_provider_class,
)
from LiuXin_alpha.file_formats.providers.conversion import (
    Converter,
    Exporter,
    HTMLExporter,
    HTMLImporter,
    HTMLTransform,
    Importer,
    OEBExporter,
    OEBImporter,
    OEBTransform,
    Transform,
)
from LiuXin_alpha.file_formats.providers.metadata import (
    CoverExtractor,
    MetadataReader,
    MetadataWriter,
)
from LiuXin_alpha.file_formats.providers.stages import (
    HTMLStage,
    OEBStage,
    stage_position,
)
from tests.support.format_providers import Recorded, declare

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.html.book import HTMLBook
    from LiuXin_alpha.file_formats.oeb.base import OEBBook

CAPABILITY_BASES: tuple[type[Provider], ...] = (
    OEBImporter,
    HTMLImporter,
    OEBExporter,
    HTMLExporter,
    OEBTransform,
    HTMLTransform,
    Converter,
    MetadataReader,
    MetadataWriter,
    CoverExtractor,
)


class EpubRoundTrip(Recorded, OEBImporter, OEBExporter):
    """
    Read and write EPUB through OEB, as one provider offering two capabilities.

    Example:
        >>> sorted(EpubRoundTrip.oeb_source_formats)
        ['epub']
    """

    provider_id = "epub.round-trip.test"
    oeb_source_formats = frozenset({"epub"})
    oeb_target_formats = frozenset({"epub"})

    def import_oeb(
        self, source: Path, source_format: str, context: ProviderContext
    ) -> OEBBook:
        """
        Stand in for a real import; never called by these tests.

        Example:
            >>> "import_oeb" in vars(EpubRoundTrip)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param context: Unused.
        :return: Never returns.
        """
        raise NotImplementedError

    def export_oeb(
        self, book: OEBBook, target: Path, target_format: str, context: ProviderContext
    ) -> None:
        """
        Stand in for a real export; never called by these tests.

        Example:
            >>> "export_oeb" in vars(EpubRoundTrip)
            True


        :param book: Unused.
        :param target: Unused.
        :param target_format: Unused.
        :param context: Unused.
        :return: Never returns.
        """
        raise NotImplementedError


class Compressor(Recorded, Converter):
    """
    Convert PDF to PDF and EPUB to EPUB, as a same-format converter would.

    Example:
        >>> sorted(Compressor.conversions)
        [('epub', 'epub'), ('pdf', 'pdf')]
    """

    provider_id = "compress.test"
    conversions = frozenset({("pdf", "pdf"), ("epub", "epub")})
    lossless = True

    def convert(
        self,
        source: Path,
        source_format: str,
        target: Path,
        target_format: str,
        context: ProviderContext,
    ) -> None:
        """
        Stand in for a real conversion; never called by these tests.

        Example:
            >>> "convert" in vars(Compressor)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param target: Unused.
        :param target_format: Unused.
        :param context: Unused.
        :return: Never returns.
        """
        raise NotImplementedError


class OEBTidy(Recorded, OEBTransform):
    """
    Pass an OEB book through unchanged in the normalise stage.

    Example:
        >>> OEBTidy.oeb_stage.value
        'normalise'
    """

    provider_id = "oeb.tidy.test"
    oeb_stage = OEBStage.NORMALISE

    def transform_oeb(self, book: OEBBook, context: ProviderContext) -> OEBBook:
        """
        Return the book unchanged.

        Example:
            >>> "transform_oeb" in vars(OEBTidy)
            True


        :param book: The book.
        :param context: Unused.
        :return: The same book.
        """
        return book


class HTMLTidy(Recorded, HTMLTransform):
    """
    Pass an HTML book through unchanged in the normalise stage.

    Example:
        >>> HTMLTidy.html_stage.value
        'normalise'
    """

    provider_id = "html.tidy.test"
    html_stage = HTMLStage.NORMALISE

    def transform_html(self, book: HTMLBook, context: ProviderContext) -> HTMLBook:
        """
        Return the book unchanged.

        Example:
            >>> "transform_html" in vars(HTMLTidy)
            True


        :param book: The book.
        :param context: Unused.
        :return: The same book.
        """
        return book


def own_names(capability: type[Provider]) -> set[str]:
    """
    List the attributes and methods a capability class adds for itself.

    The shared record and ``validate_declaration`` are excluded, since every
    capability is meant to share those.

    Example:
        >>> sorted(own_names(OEBImporter))
        ['import_oeb', 'oeb_source_formats']


    :param capability: A capability base class.
    :return: The names it defines or annotates, other than shared ones.
    """
    names = set(vars(capability)) | set(vars(capability).get("__annotations__", {}))
    return {
        name
        for name in names
        if not name.startswith("_") and name != "validate_declaration"
    }


def test_capabilities_never_share_a_member_name() -> None:
    """
    Keep each capability's members distinct so one class can inherit several.

    A class offering two capabilities inherits both sets of members. If two
    capabilities used the same name with different signatures, one would hide
    the other and the type checkers would reject the class.

    Example:
        >>> test_capabilities_never_share_a_member_name()


    :return: None after every pair of capabilities is found disjoint.
    """
    seen: dict[str, str] = {}
    for capability in CAPABILITY_BASES:
        for name in own_names(capability):
            assert name not in seen, f"{name} is on {seen[name]} and {capability}"
            seen[name] = capability.__name__


def test_abstract_parents_group_their_families() -> None:
    """
    File each per-intermediate class under its family parent.

    Example:
        >>> test_abstract_parents_group_their_families()


    :return: None after each family membership is confirmed.
    """
    assert issubclass(OEBImporter, Importer) and issubclass(HTMLImporter, Importer)
    assert issubclass(OEBExporter, Exporter) and issubclass(HTMLExporter, Exporter)
    assert issubclass(OEBTransform, Transform)
    assert issubclass(HTMLTransform, Transform)
    assert not issubclass(Converter, (Importer, Exporter, Transform))


def test_a_provider_may_offer_several_capabilities() -> None:
    """
    Check every capability of a class that offers more than one.

    Example:
        >>> test_a_provider_may_offer_several_capabilities()


    :return: None after a valid and an invalid two-capability class are checked.
    """
    assert validate_provider_class(EpubRoundTrip) is EpubRoundTrip

    half_declared = declare(
        "HalfDeclared", EpubRoundTrip, oeb_target_formats=frozenset({"EPUB"})
    )
    with pytest.raises(ProviderDefinitionError, match="oeb_target_formats"):
        validate_provider_class(half_declared)


@pytest.mark.parametrize(
    ("provider", "message"),
    [
        (
            declare("NoId", Provider, version="1", licence="GPL-3.0-only"),
            "provider_id must be set",
        ),
        (
            declare("SpacedId", Recorded, provider_id="pdf reader"),
            "provider_id must be a lower-case dotted name",
        ),
        (
            declare("UpperId", Recorded, provider_id="PDF.reader"),
            "provider_id must be a lower-case dotted name",
        ),
        (
            declare("BlankVersion", Recorded, version="  "),
            "version must be a non-empty string",
        ),
        (
            declare("NoLicence", Provider, provider_id="no.licence", version="1"),
            "licence must be set",
        ),
        (
            declare("BoolPriority", Recorded, priority=True),
            "priority must be an integer",
        ),
        (
            declare("ListRequires", Recorded, requires=["pdftohtml"]),
            "requires must be a tuple",
        ),
    ],
)
def test_shared_record_is_checked(provider: type[Provider], message: str) -> None:
    """
    Reject each way the shared record can be malformed.

    Example:
        >>> test_shared_record_is_checked(Recorded, "")  # doctest: +SKIP


    :param provider: A provider class with one malformed record attribute.
    :param message: Part of the expected error message.
    :return: None after the class is refused with that message.
    """
    with pytest.raises(ProviderDefinitionError, match=message):
        provider.validate_declaration()


def test_record_defaults() -> None:
    """
    Give a provider zero priority, no requirements and a ready availability.

    Example:
        >>> test_record_defaults()


    :return: None after the defaults are read from a minimal provider.
    """
    assert Recorded.priority == 0
    assert Recorded.requires == ()
    assert Recorded.availability() == ProviderAvailability.ready()


@pytest.mark.parametrize(
    "value",
    [
        frozenset(),
        {"pdf"},
        frozenset({".pdf"}),
        frozenset({"PDF"}),
        frozenset({" pdf"}),
        frozenset({""}),
        frozenset({1}),
    ],
)
def test_importers_declare_normalised_source_formats(value: object) -> None:
    """
    Refuse an importer whose source formats are empty or not normalised.

    Example:
        >>> test_importers_declare_normalised_source_formats(frozenset())


    :param value: A malformed ``html_source_formats`` value.
    :return: None after a well-formed importer passes and the variant is refused.
    """
    declare(
        "Good", Recorded, HTMLImporter, html_source_formats=frozenset({"pdf", "docx"})
    ).validate_declaration()

    bad = declare("Bad", Recorded, HTMLImporter, html_source_formats=value)
    with pytest.raises(ProviderDefinitionError, match="html_source_formats"):
        bad.validate_declaration()


@pytest.mark.parametrize("value", [["oeb.split"], ("Split Pages",), ("",), "oeb.split"])
def test_exporters_name_their_required_transforms_by_id(value: object) -> None:
    """
    Accept provider ids for required transforms and refuse anything else.

    Example:
        >>> test_exporters_name_their_required_transforms_by_id(("",))


    :param value: A malformed ``required_oeb_transforms`` value.
    :return: None after a well-formed exporter passes and the variant is refused.
    """
    formats = frozenset({"mobi"})
    declare(
        "Good",
        Recorded,
        OEBExporter,
        oeb_target_formats=formats,
        required_oeb_transforms=("oeb.mobi-split.builtin", "oeb.rasterise_svg"),
    ).validate_declaration()

    bad = declare(
        "Bad",
        Recorded,
        OEBExporter,
        oeb_target_formats=formats,
        required_oeb_transforms=value,
    )
    with pytest.raises(ProviderDefinitionError, match="required_oeb_transforms"):
        bad.validate_declaration()


def test_transforms_declare_a_stage_of_their_own_intermediate() -> None:
    """
    Require an OEB stage for OEB transforms and an HTML stage for HTML ones.

    Example:
        >>> test_transforms_declare_a_stage_of_their_own_intermediate()


    :return: None after matching, mismatched and missing stages are checked.
    """
    declare("Style", OEBTidy, oeb_stage=OEBStage.STYLE).validate_declaration()
    HTMLTidy.validate_declaration()

    cases = [
        (
            declare("OEBWrong", OEBTidy, oeb_stage=HTMLStage.NORMALISE),
            "member of OEBStage",
        ),
        (declare("HTMLText", HTMLTidy, html_stage="normalise"), "member of HTMLStage"),
        (declare("NoStage", Recorded, OEBTransform), "oeb_stage must be set"),
    ]
    for provider, message in cases:
        with pytest.raises(ProviderDefinitionError, match=message):
            provider.validate_declaration()


def test_transforms_apply_unless_they_say_otherwise() -> None:
    """
    Default ``applies_to_oeb`` and ``applies_to_html`` to true.

    Example:
        >>> test_transforms_apply_unless_they_say_otherwise()


    :return: None after both defaults are read from concrete transforms.
    """
    assert OEBTidy().applies_to_oeb(object(), object()) is True
    assert HTMLTidy().applies_to_html(object(), object()) is True


@pytest.mark.parametrize(
    "value",
    [
        frozenset(),
        {("pdf", "pdf")},
        frozenset({("pdf",)}),
        frozenset({("pdf", "html", "txt")}),
        frozenset({("pdf", ".html")}),
        frozenset({"pdf"}),
        frozenset({("pdf", None)}),
    ],
)
def test_converters_declare_explicit_pairs(value: object) -> None:
    """
    Refuse anything that is not a non-empty set of format-name pairs.

    Example:
        >>> test_converters_declare_explicit_pairs(frozenset())


    :param value: A malformed ``conversions`` value.
    :return: None after the same-format converter passes and the variant is refused.
    """
    Compressor.validate_declaration()
    bad = declare("Bad", Compressor, conversions=value)
    with pytest.raises(ProviderDefinitionError, match="conversions"):
        bad.validate_declaration()


def test_converter_tool_and_lossless_are_checked() -> None:
    """
    Refuse a blank external tool name and a non-boolean ``lossless``.

    Example:
        >>> test_converter_tool_and_lossless_are_checked()


    :return: None after both attributes are checked.
    """
    with pytest.raises(ProviderDefinitionError, match="external_tool"):
        declare("BlankTool", Compressor, external_tool=" ").validate_declaration()
    with pytest.raises(ProviderDefinitionError, match="lossless"):
        declare("WordyLossless", Compressor, lossless="yes").validate_declaration()


def test_converter_edges_use_the_existing_edge_model() -> None:
    """
    Describe each declared pair as a direct or external conversion edge.

    Example:
        >>> test_converter_edges_use_the_existing_edge_model()


    :return: None after the edges of an in-process and a tool converter are read.
    """
    edges = Compressor.edges()
    assert [edge.name for edge in edges] == [
        "compress.test:epub->epub",
        "compress.test:pdf->pdf",
    ]
    assert {edge.kind for edge in edges} == {ConversionEdgeKind.DIRECT}
    assert all(edge.lossless is True for edge in edges)

    ghostscript = declare(
        "Ghostscript",
        Compressor,
        provider_id="pdf.ghostscript.test",
        conversions=frozenset({("pdf", "pdf")}),
        external_tool="gs",
        lossless=None,
    )
    assert issubclass(ghostscript, Converter)
    (edge,) = ghostscript.edges()
    assert edge.kind is ConversionEdgeKind.EXTERNAL
    assert edge.external_tool == "gs"
    assert edge.lossless is None
    assert edge.supports("PDF", ".pdf")


def test_availability_must_explain_an_absence() -> None:
    """
    Require a reason exactly when a provider is unavailable.

    Example:
        >>> test_availability_must_explain_an_absence()


    :return: None after each combination of the two fields is checked.
    """
    assert ProviderAvailability.missing("gs is not installed").reason
    with pytest.raises(ValueError, match="must give a reason"):
        ProviderAvailability(False)
    with pytest.raises(ValueError, match="must give a reason"):
        ProviderAvailability(False, "  ")
    with pytest.raises(ValueError, match="no reason to report"):
        ProviderAvailability(True, "spare")


def test_context_is_immutable(tmp_path: Path) -> None:
    """
    Keep a call's context fixed once it is handed to a provider.

    Example:
        >>> test_context_is_immutable(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder used as the work folder.
    :return: None after an assignment to the context is refused.
    """
    context = ProviderContext(
        work_dir=tmp_path,
        options={},
        log=logging.getLogger("test"),
        report=ConversionReport(),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        context.work_dir = Path("/")


def test_stages_run_in_definition_order() -> None:
    """
    Fix the stage lists and their order.

    The order is part of the design: changing it changes every conversion,
    so this test makes such a change deliberate.

    Example:
        >>> test_stages_run_in_definition_order()


    :return: None after both stage lists and positions are compared.
    """
    assert [stage.value for stage in OEBStage] == [
        "normalise",
        "structure",
        "content",
        "style",
        "resources",
        "package",
    ]
    assert [stage.value for stage in HTMLStage] == ["normalise"]
    assert [stage_position(stage) for stage in OEBStage] == list(range(6))
    assert stage_position(HTMLStage.NORMALISE) == 0


@pytest.mark.parametrize(
    "module_name",
    [
        "LiuXin_alpha.file_formats.providers.base",
        "LiuXin_alpha.file_formats.providers.stages",
        "LiuXin_alpha.file_formats.providers.conversion",
        "LiuXin_alpha.file_formats.providers.metadata",
        "LiuXin_alpha.file_formats.providers.registry",
    ],
)
def test_examples_in_the_provider_modules_run(module_name: str) -> None:
    """
    Run the docstring examples of each provider module as doctests.

    The examples are the first thing a plugin author reads, so they are kept
    true by running them.

    Example:
        >>> test_examples_in_the_provider_modules_run(
        ...     "LiuXin_alpha.file_formats.providers.stages"
        ... )


    :param module_name: The dotted name of a provider module.
    :return: None after every example in the module passes.
    """
    module = importlib.import_module(module_name)
    result = doctest.testmod(module, verbose=False, report=False)
    assert result.attempted > 0
    assert result.failed == 0
