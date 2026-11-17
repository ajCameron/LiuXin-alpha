"""
Check the registry that files provider classes by capability.

The providers are small classes declared in this module. The tests cover what
the registry accepts, how it orders what it finds, and that importing the
provider package stays passive.
"""

from __future__ import annotations

import subprocess
import sys
import types
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from LiuXin_alpha.file_formats.conversion.edges import (
    ConversionEdgeRegistry,
    legacy_oeb_edge,
)
from LiuXin_alpha.file_formats.providers.base import (
    Provider,
    ProviderContext,
    ProviderDefinitionError,
)
from LiuXin_alpha.file_formats.providers.conversion import (
    Converter,
    HTMLExporter,
    HTMLImporter,
    HTMLTransform,
    Importer,
    OEBExporter,
    OEBImporter,
    OEBTransform,
)
from LiuXin_alpha.file_formats.providers.metadata import (
    CoverExtractor,
    MetadataReader,
    MetadataWriter,
)
from LiuXin_alpha.file_formats.providers.registry import (
    CAPABILITIES,
    ProviderRegistry,
    capabilities_of,
)
from LiuXin_alpha.file_formats.providers.stages import HTMLStage, OEBStage
from tests.support.format_providers import Recorded, declare

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.html.book import HTMLBook
    from LiuXin_alpha.file_formats.oeb.base import OEBBook

SOURCE_ROOT = Path(__file__).resolve().parents[3] / "src"


class Importing(Recorded, OEBImporter):
    """
    Import EPUB into OEB; subclasses change the id, formats and priority.

    Example:
        >>> sorted(Importing.oeb_source_formats)
        ['epub']
    """

    provider_id = "epub.import.test"
    oeb_source_formats = frozenset({"epub"})

    def import_oeb(
        self, source: Path, source_format: str, context: ProviderContext
    ) -> OEBBook:
        """
        Stand in for a real import; never called by these tests.

        Example:
            >>> "import_oeb" in vars(Importing)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param context: Unused.
        :return: Never returns.
        """
        raise NotImplementedError


class Exporting(Recorded, OEBExporter):
    """
    Export OEB to EPUB; subclasses change the id, formats and priority.

    Example:
        >>> sorted(Exporting.oeb_target_formats)
        ['epub']
    """

    provider_id = "epub.export.test"
    oeb_target_formats = frozenset({"epub"})

    def export_oeb(
        self, book: OEBBook, target: Path, target_format: str, context: ProviderContext
    ) -> None:
        """
        Stand in for a real export; never called by these tests.

        Example:
            >>> "export_oeb" in vars(Exporting)
            True


        :param book: Unused.
        :param target: Unused.
        :param target_format: Unused.
        :param context: Unused.
        :return: Never returns.
        """
        raise NotImplementedError


class Converting(Recorded, Converter):
    """
    Convert PDF to PDF; subclasses change the id, pairs and priority.

    Example:
        >>> sorted(Converting.conversions)
        [('pdf', 'pdf')]
    """

    provider_id = "pdf.compress.test"
    conversions = frozenset({("pdf", "pdf")})

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
            >>> "convert" in vars(Converting)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param target: Unused.
        :param target_format: Unused.
        :param context: Unused.
        :return: Never returns.
        """
        raise NotImplementedError


class OEBTransforming(Recorded, OEBTransform):
    """
    Pass an OEB book through unchanged; subclasses set the stage and priority.

    Example:
        >>> OEBTransforming.oeb_stage.value
        'normalise'
    """

    provider_id = "oeb.transform.test"
    oeb_stage = OEBStage.NORMALISE

    def transform_oeb(self, book: OEBBook, context: ProviderContext) -> OEBBook:
        """
        Return the book unchanged.

        Example:
            >>> "transform_oeb" in vars(OEBTransforming)
            True


        :param book: The book.
        :param context: Unused.
        :return: The same book.
        """
        return book


class HTMLRoundTrip(Recorded, HTMLImporter, HTMLExporter, HTMLTransform):
    """
    Offer three HTML capabilities from one class.

    Example:
        >>> [base.__name__ for base in capabilities_of(HTMLRoundTrip)]
        ['HTMLImporter', 'HTMLExporter', 'HTMLTransform']
    """

    provider_id = "html.round-trip.test"
    html_source_formats = frozenset({"htmlz"})
    html_target_formats = frozenset({"htmlz", "pdf"})
    html_stage = HTMLStage.NORMALISE

    def import_html(
        self, source: Path, source_format: str, context: ProviderContext
    ) -> HTMLBook:
        """
        Stand in for a real import; never called by these tests.

        Example:
            >>> "import_html" in vars(HTMLRoundTrip)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param context: Unused.
        :return: Never returns.
        """
        raise NotImplementedError

    def export_html(
        self, book: HTMLBook, target: Path, target_format: str, context: ProviderContext
    ) -> None:
        """
        Stand in for a real export; never called by these tests.

        Example:
            >>> "export_html" in vars(HTMLRoundTrip)
            True


        :param book: Unused.
        :param target: Unused.
        :param target_format: Unused.
        :param context: Unused.
        :return: Never returns.
        """
        raise NotImplementedError

    def transform_html(self, book: HTMLBook, context: ProviderContext) -> HTMLBook:
        """
        Return the book unchanged.

        Example:
            >>> "transform_html" in vars(HTMLRoundTrip)
            True


        :param book: The book.
        :param context: Unused.
        :return: The same book.
        """
        return book


def ids(found: tuple[type[Provider], ...]) -> list[str]:
    """
    List the ids of provider classes, in the order given.

    Example:
        >>> ids((Importing, Exporting))
        ['epub.import.test', 'epub.export.test']


    :param found: Provider classes.
    :return: Their ``provider_id`` values.
    """
    return [provider.provider_id for provider in found]


def test_capability_list_covers_every_concrete_base() -> None:
    """
    List each per-intermediate class and the converter as capabilities.

    Example:
        >>> test_capability_list_covers_every_concrete_base()


    :return: None after the capability tuple is compared.
    """
    assert CAPABILITIES == (
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
    assert capabilities_of(HTMLRoundTrip) == (
        HTMLImporter,
        HTMLExporter,
        HTMLTransform,
    )
    assert capabilities_of(Recorded) == ()


def test_registration_rejects_what_cannot_be_a_provider() -> None:
    """
    Refuse non-providers, unfinished classes and classes with no capability.

    Example:
        >>> test_registration_rejects_what_cannot_be_a_provider()


    :return: None after each kind of unusable class is refused.
    """
    registry = ProviderRegistry()
    unfinished = declare(
        "Unfinished",
        Recorded,
        OEBImporter,
        OEBExporter,
        oeb_source_formats=frozenset({"epub"}),
        oeb_target_formats=frozenset({"epub"}),
    )

    with pytest.raises(ProviderDefinitionError, match="is not a Provider subclass"):
        registry.register(object)
    with pytest.raises(
        ProviderDefinitionError, match="does not implement: export_oeb, import_oeb"
    ):
        registry.register(unfinished)
    with pytest.raises(ProviderDefinitionError, match="offers no capability"):
        registry.register(Recorded)
    with pytest.raises(ProviderDefinitionError, match="offers no capability"):
        registry.register(declare("FamilyOnly", Recorded, Importer))
    assert registry.providers() == ()


def test_registration_rejects_a_malformed_record() -> None:
    """
    Check the declaration when a class is registered, not when it is defined.

    Example:
        >>> test_registration_rejects_a_malformed_record()


    :return: None after a malformed class is defined freely and then refused.
    """
    dotted = declare("Dotted", Importing, oeb_source_formats=frozenset({".epub"}))
    with pytest.raises(ProviderDefinitionError, match="oeb_source_formats"):
        ProviderRegistry().register(dotted)


def test_provider_ids_are_unique_per_registry() -> None:
    """
    Accept a class twice but refuse a second class with the same id.

    Example:
        >>> test_provider_ids_are_unique_per_registry()


    :return: None after a repeat registration and a clash are checked.
    """
    registry = ProviderRegistry()
    registry.register(Importing)
    registry.register(Importing)
    with pytest.raises(ProviderDefinitionError, match="already registered"):
        registry.register(declare("Impostor", Importing))
    assert registry.providers() == (Importing,)
    assert registry.provider("epub.import.test") is Importing
    with pytest.raises(LookupError, match="no provider is registered"):
        registry.provider("missing.test")


def test_registries_are_independent() -> None:
    """
    Keep what one registry holds out of every other.

    Example:
        >>> test_registries_are_independent()


    :return: None after a registration is found in one registry only.
    """
    first = ProviderRegistry()
    first.register(Importing)
    assert ProviderRegistry().providers() == ()


def test_queries_ignore_case_and_leading_dot() -> None:
    """
    Normalise the format a caller asks for, as the edge registry does.

    Example:
        >>> test_queries_ignore_case_and_leading_dot()


    :return: None after differently written format names find the same providers.
    """
    registry = ProviderRegistry()
    registry.register(Importing)
    registry.register(Converting)
    assert ids(registry.oeb_importers(".EPUB")) == ["epub.import.test"]
    assert ids(registry.oeb_importers(" epub ")) == ["epub.import.test"]
    assert ids(registry.converters("PDF", ".pdf")) == ["pdf.compress.test"]


def test_queries_put_higher_priority_first_and_keep_ties_in_order() -> None:
    """
    Order providers by descending priority, then by registration order.

    This matches the order the plugin loader already uses.

    Example:
        >>> test_queries_put_higher_priority_first_and_keep_ties_in_order()


    :return: None after mixed priorities are registered and queried.
    """
    registry = ProviderRegistry()
    for name, priority in [
        ("fallback", -5),
        ("plain", 0),
        ("preferred", 10),
        ("also-plain", 0),
    ]:
        registry.register(
            declare(
                name.title(),
                Importing,
                provider_id=f"epub.import.{name}",
                priority=priority,
            )
        )
    assert ids(registry.oeb_importers("epub")) == [
        "epub.import.preferred",
        "epub.import.plain",
        "epub.import.also-plain",
        "epub.import.fallback",
    ]


def test_each_capability_is_queried_separately() -> None:
    """
    File a provider under each capability it offers and no other.

    Example:
        >>> test_each_capability_is_queried_separately()


    :return: None after every query is run against a mixed registry.
    """
    registry = ProviderRegistry()
    for provider in (Importing, Exporting, Converting, OEBTransforming, HTMLRoundTrip):
        registry.register(provider)

    assert ids(registry.oeb_importers("epub")) == ["epub.import.test"]
    assert ids(registry.oeb_exporters("epub")) == ["epub.export.test"]
    assert ids(registry.html_importers("htmlz")) == ["html.round-trip.test"]
    assert ids(registry.html_exporters("pdf")) == ["html.round-trip.test"]
    assert ids(registry.html_transforms()) == ["html.round-trip.test"]
    assert ids(registry.oeb_transforms()) == ["oeb.transform.test"]
    assert ids(registry.converters("pdf", "pdf")) == ["pdf.compress.test"]

    assert registry.oeb_importers("htmlz") == ()
    assert registry.html_importers("epub") == ()
    assert registry.oeb_exporters("pdf") == ()
    assert registry.html_exporters("epub") == ()


def test_converters_match_the_exact_pair() -> None:
    """
    Find a converter only for a pair it declares, including same-format pairs.

    Example:
        >>> test_converters_match_the_exact_pair()


    :return: None after declared, reversed and partial pairs are queried.
    """
    registry = ProviderRegistry()
    registry.register(Converting)
    registry.register(
        declare(
            "PdfToHtml",
            Converting,
            provider_id="pdf.to-html.test",
            conversions=frozenset({("pdf", "html")}),
            external_tool="pdftohtml",
        )
    )
    assert ids(registry.converters("pdf", "html")) == ["pdf.to-html.test"]
    assert ids(registry.converters("pdf", "pdf")) == ["pdf.compress.test"]
    assert registry.converters("html", "pdf") == ()
    assert registry.converters("epub", "epub") == ()


def test_transforms_run_by_stage_then_priority() -> None:
    """
    Order transforms by stage first and by priority within a stage.

    Example:
        >>> test_transforms_run_by_stage_then_priority()


    :return: None after transforms from three stages are ordered and filtered.
    """
    registry = ProviderRegistry()
    for name, stage, priority in [
        ("package", OEBStage.PACKAGE, 100),
        ("style-low", OEBStage.STYLE, 0),
        ("style-high", OEBStage.STYLE, 5),
        ("normalise", OEBStage.NORMALISE, 0),
    ]:
        registry.register(
            declare(
                name.title(),
                OEBTransforming,
                provider_id=f"oeb.{name}.test",
                oeb_stage=stage,
                priority=priority,
            )
        )

    assert ids(registry.oeb_transforms()) == [
        "oeb.normalise.test",
        "oeb.style-high.test",
        "oeb.style-low.test",
        "oeb.package.test",
    ]
    assert ids(registry.oeb_transforms(OEBStage.STYLE)) == [
        "oeb.style-high.test",
        "oeb.style-low.test",
    ]
    assert registry.oeb_transforms(OEBStage.CONTENT) == ()


def test_register_module_takes_finished_providers_in_definition_order() -> None:
    """
    Register a module's own finished providers and skip everything else.

    Example:
        >>> test_register_module_takes_finished_providers_in_definition_order()


    :return: None after a synthetic module is registered.
    """
    module = types.ModuleType("example_format_providers")
    own = {"__module__": module.__name__}
    first = declare("First", Converting, provider_id="pdf.compress.first", **own)
    helper = declare(
        "Helper", Recorded, OEBImporter, oeb_source_formats=frozenset({"epub"}), **own
    )
    second = declare("Second", Importing, provider_id="epub.import.second", **own)
    vars(module).update(
        First=first,
        Helper=helper,
        Imported=Importing,
        Base=Provider,
        Second=second,
        value=3,
    )

    registry = ProviderRegistry()
    assert registry.register_module(module) == (first, second)
    assert registry.providers() == (first, second)


def test_register_module_refuses_a_broken_provider() -> None:
    """
    Raise rather than skip a finished provider that cannot be registered.

    Example:
        >>> test_register_module_refuses_a_broken_provider()


    :return: None after a module holding a malformed provider is refused.
    """
    module = types.ModuleType("broken_format_providers")
    broken = declare(
        "Broken", Converting, provider_id="Broken", __module__=module.__name__
    )
    vars(module).update(Broken=broken)

    with pytest.raises(ProviderDefinitionError, match="provider_id"):
        ProviderRegistry().register_module(module)


def test_converter_edges_feed_the_existing_edge_registry() -> None:
    """
    Let the existing edge registry list converters beside OEB routes.

    Example:
        >>> test_converter_edges_feed_the_existing_edge_registry()


    :return: None after converter edges and a legacy edge share one registry.
    """
    registry = ProviderRegistry()
    registry.register(Importing)
    registry.register(Converting)

    edges = ConversionEdgeRegistry()
    edges.register(legacy_oeb_edge("pdf", "pdf"))
    edges.extend(registry.converter_edges())

    names = [edge.name for edge in edges.edges_for(source_format="pdf")]
    assert names == ["pdf.compress.test:pdf->pdf", "legacy-oeb:pdf->pdf"]


def test_importing_the_contracts_loads_only_the_standard_library() -> None:
    """
    Import the contracts without loading any third-party package.

    A fresh interpreter imports the registry and the HTML book model and
    lists every top-level package that came with them. Which project modules
    may load is budgeted separately by ``scripts/check_import_footprint.py``.

    Example:
        >>> test_importing_the_contracts_loads_only_the_standard_library()  # doctest: +SKIP


    :return: None after the child process finds only standard-library packages.
    """
    source = f"""
import importlib
import sys
sys.path.insert(0, {str(SOURCE_ROOT)!r})
before = set(sys.modules)
importlib.import_module("LiuXin_alpha.file_formats.providers.registry")
importlib.import_module("LiuXin_alpha.file_formats.html.book")
loaded = {{name.split(".")[0] for name in set(sys.modules) - before}}
outside = sorted(loaded - set(sys.stdlib_module_names) - {{"LiuXin_alpha"}})
assert not outside, outside
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", source],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
