"""
Check that the bundled conversion plugins run as registered providers.

The adapters must reach every bundled plugin, resolve options exactly as
``Plumber`` does, and leave the process as they found it. Writing real output
needs the CSS libraries of the conversion extra; the tests that do so skip
when they are absent.
"""

from __future__ import annotations

import doctest
import json
import logging
import os
import threading
import zipfile
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.conversion.builtins import (
    get_input_plugins,
    get_output_plugins,
)
from LiuXin_alpha.file_formats.conversion.plugin import (
    OptionRecommendation,
    OutputFormatPlugin,
)
from LiuXin_alpha.file_formats.conversion.plumber import (
    Plumber,
    pipeline_option_recommendations,
)
from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import legacy_conversion
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.legacy_conversion import (
    LEGACY_CALL_LOCK,
    LEGACY_INPUT_PLUGIN_ATTRIBUTE,
    LEGACY_PRIORITY,
    UNADAPTED_INPUT_PLUGINS,
    LegacyInputAdapter,
    LegacyOutputAdapter,
    NoLegacyInputPlugin,
    adapt_output_plugin,
    legacy_option_values,
    register_bundled_conversion_plugins,
)
from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry
from LiuXin_alpha.utils.logging import default_log
from tests.support.file_format_epub import build_unicode_epub

IGNORED_PLUMBER_ATTRIBUTES = frozenset(
    {"conversion_edge", "conversion_edge_name", "no_inline_navbars"}
)
"""Option attributes ``Plumber`` derives from its own state, not from options."""


class RecordingOutput(OutputFormatPlugin):
    """
    Write down what the exporter adapter handed to an output plugin.

    It declares one option of its own and pins ``margin_left`` at the high
    level, so tests can see both reach the plugin.

    Example:
        >>> RecordingOutput.file_type
        'recording'
    """

    name = "Recording Output"
    file_type = "recording"
    options = {
        OptionRecommendation(
            name="recording_style",
            recommended_value="plain",
            level=OptionRecommendation.LOW,
            option_help="How the recording is laid out.",
        )
    }
    recommendations = {("margin_left", 7, OptionRecommendation.HIGH)}

    def convert(
        self, oeb: Any, output_path: str, input_plugin: Any, opts: Any, log: Any
    ) -> None:
        """
        Record the call as JSON at ``output_path``.

        Example:
            >>> "convert" in vars(RecordingOutput)
            True


        :param oeb: The book.
        :param output_path: Where to write the record.
        :param input_plugin: The plugin that read the book, or a stand-in.
        :param opts: The resolved option values.
        :param log: A callable logger.
        :return: None after the record is written.
        """
        log("recording", output_path)
        others = threading.Thread(target=_try_lock, args=(results := [],))
        others.start()
        others.join()
        record = {
            "spine": [item.href for item in oeb.spine] if oeb is not None else [],
            "input_plugin": getattr(input_plugin, "name", None),
            "is_image_collection": opts.is_image_collection,
            "report_id": id(opts.conversion_report),
            "recording_style": opts.recording_style,
            "margin_left": opts.margin_left,
            "pretty_print": opts.pretty_print,
            "output_format": getattr(oeb, "plumber_output_format", None),
            "lock_free_for_other_threads": results[0],
        }
        Path(output_path).write_text(json.dumps(record), encoding="utf-8")


def _try_lock(results: list[bool]) -> None:
    """
    Record whether another thread can take the legacy call lock right now.

    Example:
        >>> found = []
        >>> _try_lock(found)
        >>> found
        [True]


    :param results: A list to append the answer to.
    :return: None after the answer is appended.
    """
    acquired = LEGACY_CALL_LOCK.acquire(blocking=False)
    if acquired:
        LEGACY_CALL_LOCK.release()
    results.append(acquired)


def context(work_dir: Path, **options: object) -> ProviderContext:
    """
    Build a provider context for one call.

    Example:
        >>> context(Path.cwd(), verbose=0).options
        {'verbose': 0}


    :param work_dir: The call's work folder.
    :param options: Option values for the call.
    :return: The context, with a fresh report.
    """
    return ProviderContext(
        work_dir=work_dir,
        options=options,
        log=logging.getLogger("tests.legacy-adapters"),
        report=ConversionReport(),
    )


def bundled_registry() -> ProviderRegistry:
    """
    Build a registry holding an adapter for every bundled plugin.

    Example:
        >>> bundled_registry().oeb_importers("epub")[0].provider_id  # doctest: +SKIP
        'legacy.epub-input'


    :return: The registry.
    """
    registry = ProviderRegistry()
    register_bundled_conversion_plugins(registry)
    return registry


def test_every_bundled_plugin_is_reachable_through_the_registry() -> None:
    """
    Find an adapter for every format of every bundled plugin.

    Example:
        >>> test_every_bundled_plugin_is_reachable_through_the_registry()


    :return: None after every plugin and format is found.
    """
    registry = bundled_registry()
    for plugin in get_input_plugins():
        if plugin.name in UNADAPTED_INPUT_PLUGINS:
            assert not any(
                getattr(found, "legacy_plugin", None) is plugin
                for found in registry.providers()
            )
            continue
        for file_type in plugin.file_types:
            found = registry.oeb_importers(file_type)
            assert any(
                issubclass(item, LegacyInputAdapter) and item.legacy_plugin is plugin
                for item in found
            ), (plugin.name, file_type)
    for plugin in get_output_plugins():
        found = registry.oeb_exporters(plugin.file_type)
        assert any(
            issubclass(item, LegacyOutputAdapter) and item.legacy_plugin is plugin
            for item in found
        ), plugin.name


def test_adapters_carry_the_plugin_record_at_a_low_priority() -> None:
    """
    Give every adapter a unique id, the plugin's version and a low priority.

    Example:
        >>> test_adapters_carry_the_plugin_record_at_a_low_priority()


    :return: None after every adapter's record is checked.
    """
    adapters = bundled_registry().providers()
    ids = [adapter.provider_id for adapter in adapters]
    assert len(ids) == len(set(ids))
    assert all(provider_id.startswith("legacy.") for provider_id in ids)
    assert {adapter.priority for adapter in adapters} == {LEGACY_PRIORITY}
    assert {adapter.licence for adapter in adapters} == {"GPL-3.0-only"}
    assert "legacy.epub-input" in ids and "legacy.epub-output" in ids


def test_unadapted_plugins_say_why() -> None:
    """
    Name a reason for every bundled plugin that is not adapted.

    Example:
        >>> test_unadapted_plugins_say_why()


    :return: None after every exclusion is found to be a bundled plugin.
    """
    names = {plugin.name for plugin in get_input_plugins()}
    for name, reason in UNADAPTED_INPUT_PLUGINS.items():
        assert name in names
        assert reason.strip()


def test_pipeline_options_are_fresh_on_every_call() -> None:
    """
    Return new recommendation objects each time, in Plumber's order.

    Example:
        >>> test_pipeline_options_are_fresh_on_every_call()


    :return: None after two calls are compared.
    """
    first, second = pipeline_option_recommendations(), pipeline_option_recommendations()
    assert [rec.option.name for rec in first] == [rec.option.name for rec in second]
    assert first[0].option.name == "verbose"
    assert all(a is not b for a, b in zip(first, second, strict=True))


@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("epub", "txt"),
        ("txt", "epub"),
        ("pdf", "mobi"),
        ("html", "pdf"),
        ("cbz", "lrf"),
        ("docx", "azw3"),
    ],
)
def test_option_resolution_matches_plumber(
    tmp_path: Path, source: str, target: str
) -> None:
    """
    Resolve the same values as ``Plumber.setup_options`` for a plugin pair.

    Example:
        >>> test_option_resolution_matches_plumber(Path.cwd(), "epub", "txt")  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder for the placeholder input file.
    :param source: The input format.
    :param target: The output format.
    :return: None after every option value is compared.
    """
    (tmp_path / f"in.{source}").write_bytes(b"placeholder")
    plumber = Plumber(
        str(tmp_path / f"in.{source}"), str(tmp_path / f"out.{target}"), default_log
    )
    plumber.setup_options()
    expected = {
        name: value
        for name, value in vars(plumber.opts).items()
        if name not in IGNORED_PLUMBER_ATTRIBUTES
    }
    report = ConversionReport()
    resolved = vars(
        legacy_option_values((plumber.input_plugin, plumber.output_plugin), {}, report)
    )
    assert resolved.pop("conversion_report") is report
    assert resolved == expected


def test_overrides_follow_plumber_ui_rules(tmp_path: Path) -> None:
    """
    Apply overrides as ``Plumber`` applies interface recommendations.

    An override changes an option unless a plugin pinned it at the high
    level; an unknown name is ignored; a profile name becomes the profile.

    Example:
        >>> test_overrides_follow_plumber_ui_rules(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder for the placeholder input file.
    :return: None after both resolutions are compared.
    """
    overrides: dict[str, object] = {
        "margin_left": 30,
        "base_font_size": 14.0,
        "pretty_print": False,
        "output_profile": "kindle",
        "no_such_option": 1,
    }
    (tmp_path / "in.cbz").write_bytes(b"placeholder")
    plumber = Plumber(str(tmp_path / "in.cbz"), str(tmp_path / "out.epub"), default_log)
    plumber.merge_ui_recommendations(
        [(name, value, OptionRecommendation.HIGH) for name, value in overrides.items()]
    )
    plumber.setup_options()
    resolved = vars(
        legacy_option_values(
            (plumber.input_plugin, plumber.output_plugin),
            overrides,
            ConversionReport(),
        )
    )
    assert resolved["margin_left"] == 0  # pinned by Comic Input
    assert resolved["pretty_print"] is True  # pinned by EPUB Output
    assert resolved["base_font_size"] == 14.0
    assert resolved["output_profile"].short_name == "kindle"
    assert "no_such_option" not in resolved
    for name in overrides:
        if hasattr(plumber.opts, name):
            assert resolved[name] == getattr(plumber.opts, name), name


def test_epub_importer_reads_a_book_and_leaves_the_process_alone(
    tmp_path: Path,
) -> None:
    """
    Read an EPUB without changing the working directory or other work files.

    Example:
        >>> test_epub_importer_reads_a_book_and_leaves_the_process_alone(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder used as the work folder.
    :return: None after the book and the process state are checked.
    """
    fixture = build_unicode_epub(tmp_path / "source.epub")
    sentinel = tmp_path / "keep-me.txt"
    sentinel.write_text("kept", encoding="utf-8")
    before = os.getcwd()

    importer = bundled_registry().oeb_importers(".EPUB")[0]
    assert importer.provider_id == "legacy.epub-input"
    book: Any = importer().import_oeb(fixture.path, "epub", context(tmp_path))

    assert os.getcwd() == before
    assert sentinel.read_text(encoding="utf-8") == "kept"
    assert [item.href for item in book.spine] == list(fixture.chapter_members)
    assert str(book.metadata.title[0]) == "EPUB Καλημέρα 世界"
    assert getattr(book, LEGACY_INPUT_PLUGIN_ATTRIBUTE).name == "EPUB Input"
    text = "".join(
        str(item.data.xpath("string(.)"))
        for item in book.spine
        if item.data is not None
    )
    for fragment in fixture.text_fragments:
        assert fragment in text


def test_exporter_hands_the_plugin_the_book_and_resolved_options(
    tmp_path: Path,
) -> None:
    """
    Pass the book, the source plugin, the call's report and options through.

    Example:
        >>> test_exporter_hands_the_plugin_the_book_and_resolved_options(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder used as the work folder.
    :return: None after the plugin's record of the call is checked.
    """
    fixture = build_unicode_epub(tmp_path / "source.epub")
    registry = bundled_registry()
    registry.register(adapt_output_plugin(RecordingOutput))
    book = registry.oeb_importers("epub")[0]().import_oeb(
        fixture.path, "epub", context(tmp_path)
    )

    call = context(tmp_path, recording_style="fancy", margin_left=99, pretty_print=True)
    before = os.getcwd()
    exporter = registry.oeb_exporters("recording")[0]
    exporter().export_oeb(book, tmp_path / "out.recording", "recording", call)

    record = json.loads((tmp_path / "out.recording").read_text(encoding="utf-8"))
    assert os.getcwd() == before
    assert record == {
        "spine": list(fixture.chapter_members),
        "input_plugin": "EPUB Input",
        "is_image_collection": False,
        "report_id": id(call.report),
        "recording_style": "fancy",
        "margin_left": 7,
        "pretty_print": True,
        "output_format": "recording",
        "lock_free_for_other_threads": False,
    }


def test_exporter_stands_in_for_a_book_with_no_input_plugin(tmp_path: Path) -> None:
    """
    Give the plugin a stand-in when the book was not read by a legacy plugin.

    Example:
        >>> test_exporter_stands_in_for_a_book_with_no_input_plugin(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder used as the work folder.
    :return: None after the plugin's record of the call is checked.
    """

    class Book:
        """
        Stand in for an OEB book with an empty spine.

        Example:
            >>> Book.spine
            ()
        """

        spine = ()

    call = context(tmp_path)
    exporter = adapt_output_plugin(RecordingOutput)
    exporter().export_oeb(Book(), tmp_path / "out.recording", "recording", call)
    record = json.loads((tmp_path / "out.recording").read_text(encoding="utf-8"))
    assert record["input_plugin"] == NoLegacyInputPlugin.name
    assert record["is_image_collection"] is False


def test_epub_round_trip_through_the_registry(tmp_path: Path) -> None:
    """
    Import an EPUB and export it again using providers the registry chose.

    This is the first proving route of the provider briefing. The output must
    be a valid EPUB holding the source's title, text and image. ``Plumber``
    converts the same file for comparison; it also runs its transforms, so
    only the text both must keep is compared.

    Example:
        >>> test_epub_round_trip_through_the_registry(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder used as the work folder.
    :return: None after both outputs are checked.
    """
    pytest.importorskip("cssutils", reason="EPUB output needs the conversion extra")
    pytest.importorskip("cssselect", reason="EPUB output needs cssselect")
    fixture = build_unicode_epub(tmp_path / "source.epub")
    registry = bundled_registry()
    importer = registry.oeb_importers("epub")[0]
    exporter = registry.oeb_exporters("epub")[0]
    work = tmp_path / "work"
    work.mkdir()
    call = context(work)
    book = importer().import_oeb(fixture.path, "epub", call)
    exporter().export_oeb(book, tmp_path / "providers.epub", "epub", call)

    plumbed = tmp_path / "plumber.epub"
    Plumber(str(fixture.path), str(plumbed), default_log).run()

    for output in (tmp_path / "providers.epub", plumbed):
        with zipfile.ZipFile(output) as archive:
            names = archive.namelist()
            assert names[0] == "mimetype"
            assert archive.read("mimetype") == b"application/epub+zip"
            opf = next(name for name in names if name.endswith(".opf"))
            assert "EPUB Καλημέρα 世界" in archive.read(opf).decode("utf-8")
            assert any(name.endswith(".png") for name in names), output
            text = "".join(
                archive.read(name).decode("utf-8")
                for name in names
                if name.endswith((".html", ".xhtml"))
            )
            for fragment in fixture.text_fragments:
                assert fragment in text, (output.name, fragment)


def test_examples_in_the_adapter_module_run() -> None:
    """
    Run the docstring examples of the adapter module as doctests.

    Example:
        >>> test_examples_in_the_adapter_module_run()  # doctest: +SKIP


    :return: None after every example passes.
    """
    result = doctest.testmod(legacy_conversion, verbose=False, report=False)
    assert result.attempted > 0
    assert result.failed == 0
