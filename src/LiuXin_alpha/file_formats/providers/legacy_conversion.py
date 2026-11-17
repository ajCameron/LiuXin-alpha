"""
Run the existing conversion plugins as OEB importers and exporters.

Every bundled ``InputFormatPlugin`` reads a format into an OEB book, and every
``OutputFormatPlugin`` writes an OEB book out. The adapters here wrap each
plugin class in a provider class, so the plugins can be registered and chosen
like any other provider while they are still the code doing the work.

An adapter does what ``Plumber.run`` does around the plugin, and nothing more:

- an importer resolves the plugin's options, runs the plugin in a fresh
  folder, turns an OPF path into an OEB book, and lets the plugin post-process
  the book;
- an exporter resolves the plugin's options and hands it the book.

The transforms ``Plumber`` runs between the two are not part of either. Until
they become transform providers, a route made of an adapted importer and an
adapted exporter writes the book as the input plugin read it.

Legacy plugins change the process working directory while they run. The
adapters hold one process-wide lock around every legacy call, so two adapted
plugins in different threads cannot change it under each other. Code outside
the adapters, ``Plumber`` included, does not take this lock
(:data:`LEGACY_CALL_LOCK`) today.

Importing this module loads no plugin and no format code. The plugins are
imported when :func:`register_bundled_conversion_plugins` is called.
"""

from __future__ import annotations

import logging
import re
import tempfile
import threading
import types
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, Self, cast

from LiuXin_alpha.file_formats.conversion.edges import normalize_format_name
from LiuXin_alpha.file_formats.providers.base import (
    ProviderContext,
    ProviderDefinitionError,
    declared,
)
from LiuXin_alpha.file_formats.providers.conversion import OEBExporter, OEBImporter
from LiuXin_alpha.utils.logging import CompatLogger

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.conversion.report import ConversionReport
    from LiuXin_alpha.file_formats.oeb.base import OEBBook
    from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry

LEGACY_PRIORITY = -100
"""Priority of every adapted plugin, so that a native provider is preferred."""

LEGACY_LICENCE = "GPL-3.0-only"
"""Licence of the bundled plugins, which derive from calibre."""

LEGACY_INPUT_PLUGIN_ATTRIBUTE = "liuxin_legacy_input_plugin"
"""Attribute on an imported book naming the plugin instance that read it."""

UNADAPTED_INPUT_PLUGINS: Mapping[str, str] = {
    "Recipe Input": (
        "downloads news from the web rather than reading a file, and depends on "
        "state that only Plumber sets"
    ),
}
"""Bundled input plugins that are not adapted, by plugin name, with the reason."""

LEGACY_CALL_LOCK = threading.RLock()
"""Held by the adapters around every legacy plugin call.

Other code that runs legacy plugins may take it too, to keep out of the
adapters' way while the working directory is changed.
"""


class LegacyPlugin(Protocol):
    """
    Describe what the adapters use of a legacy conversion plugin.

    Example:
        >>> from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin
        >>> plugin: LegacyPlugin = InputFormatPlugin(None)
        >>> sorted(rec.option.name for rec in plugin.common_options)
        ['input_encoding']
    """

    name: str
    options: Iterable[Any]
    common_options: Iterable[Any]
    recommendations: Iterable[tuple[str, Any, int]]

    def __enter__(self: Self) -> Any:
        """
        Prepare the plugin's resources for a call.

        Example:
            The adapters enter a plugin around each call::

                with plugin:
                    plugin.convert(...)


        :return: Anything; the adapters ignore it.
        """
        ...

    def __exit__(self: Self, *args: Any) -> Any:
        """
        Release what :meth:`__enter__` prepared.

        Example:
            Called by the ``with`` statement that entered the plugin::

                with plugin:
                    ...


        :param args: The exception details, if the call raised.
        :return: Anything; the adapters ignore it.
        """
        ...


class LegacyInputPlugin(LegacyPlugin, Protocol):
    """
    Describe what the importer adapter uses of an ``InputFormatPlugin``.

    Example:
        >>> from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin
        >>> plugin: LegacyInputPlugin = InputFormatPlugin(None)
        >>> plugin.output_encoding, plugin.is_image_collection
        ('utf-8', False)
    """

    output_encoding: str | None
    is_image_collection: bool

    def __call__(
        self: Self,
        stream: Any,
        options: Any,
        file_ext: str,
        log: Any,
        accelerators: Any,
        output_dir: str,
    ) -> Any:
        """
        Convert the input in ``output_dir`` and return a book or an OPF path.

        Example:
            The importer adapter calls the plugin as ``Plumber`` does::

                result = plugin(stream, options, "epub", log, {}, folder)


        :param stream: The open source file.
        :param options: The resolved option values.
        :param file_ext: The source format.
        :param log: A callable logger.
        :param accelerators: Hints from earlier steps; always empty here.
        :param output_dir: An empty folder the plugin may fill.
        :return: An OEB book, or the path of an OPF file.
        """
        ...

    def postprocess_book(self: Self, oeb: Any, opts: Any, log: Any) -> Any:
        """
        Adjust the book after it has been read.

        Example:
            The importer adapter calls it once the book exists::

                plugin.postprocess_book(book, options, log)


        :param oeb: The book.
        :param opts: The resolved option values.
        :param log: A callable logger.
        :return: Anything; the adapter ignores it.
        """
        ...


class LegacyOutputPlugin(LegacyPlugin, Protocol):
    """
    Describe what the exporter adapter uses of an ``OutputFormatPlugin``.

    Example:
        >>> from LiuXin_alpha.file_formats.conversion.plugin import OutputFormatPlugin
        >>> plugin: LegacyOutputPlugin = OutputFormatPlugin(None)
        >>> "pretty_print" in {rec.option.name for rec in plugin.common_options}
        True
    """

    def convert(
        self: Self,
        oeb: Any,
        output_path: str,
        input_plugin: Any,
        opts: Any,
        log: Any,
    ) -> Any:
        """
        Write the book to ``output_path``.

        Example:
            The exporter adapter calls it as ``Plumber`` does::

                plugin.convert(book, "/out/book.epub", source_plugin, options, log)


        :param oeb: The book.
        :param output_path: Where to write.
        :param input_plugin: The plugin that read the book, or a stand-in.
        :param opts: The resolved option values.
        :param log: A callable logger.
        :return: Anything; the adapter ignores it.
        """
        ...


class NoLegacyInputPlugin:
    """
    Stand in for the input plugin when a book was not read by one.

    Two output plugins ask the input plugin whether the book is a collection
    of images. A book that came from elsewhere is not.

    Example:
        >>> NoLegacyInputPlugin().is_image_collection
        False
        >>> NoLegacyInputPlugin().get_images()
        []
    """

    name = "No input plugin"
    is_image_collection = False

    def get_images(self: Self) -> list[str]:
        """
        Report that there are no images to collect.

        Example:
            >>> NoLegacyInputPlugin().get_images()
            []


        :return: An empty list.
        """
        return []


def legacy_log(log: logging.Logger) -> CompatLogger:
    """
    Give legacy code a logger it can call like a function.

    Legacy plugins write ``log("message")``. A :class:`CompatLogger` accepts
    that; any other logger is wrapped in one that passes its records up to it.

    Example:
        >>> import logging
        >>> plain = logging.Logger("plain")
        >>> bridge = legacy_log(plain)
        >>> bridge.parent is plain, legacy_log(bridge) is bridge
        (True, True)


    :param log: The logger from the provider context.
    :return: A callable logger whose records reach ``log``.
    """
    if isinstance(log, CompatLogger):
        return log
    bridge = CompatLogger(f"{log.name}.legacy")
    bridge.parent = log
    return bridge


def legacy_option_values(
    plugins: Sequence[LegacyPlugin],
    overrides: Mapping[str, object],
    report: ConversionReport,
) -> Any:
    """
    Resolve conversion options for legacy plugins the way ``Plumber`` does.

    The options are the pipeline options, the options every input and every
    output plugin shares, and the options of each plugin given. Each starts
    at its recommended value. Each plugin's recommendations then apply, in
    order, where they are at least as strong as the current one. Each entry
    of ``overrides`` then applies unless a plugin recommended that option at
    the high level; names that match no option are ignored, as ``Plumber``
    ignores them. Finally the two profile names become profile objects, and
    ``conversion_report`` is set so plugins record losses in ``report``.

    Given an input and an output plugin, the result holds the values
    ``Plumber.setup_options`` would hold for that pair, apart from the
    attributes ``Plumber`` derives from its own state.

    Example:
        >>> from LiuXin_alpha.file_formats.conversion.report import ConversionReport
        >>> from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin
        >>> report = ConversionReport()
        >>> values = legacy_option_values(
        ...     [InputFormatPlugin(None)], {"verbose": 2}, report
        ... )
        >>> values.verbose, values.conversion_report is report
        (2, True)


    :param plugins: The plugins whose options and recommendations apply, in
        the order ``Plumber`` applies them: input, then output.
    :param overrides: Option values chosen for this call, by option name.
    :param report: The report plugins should record warnings and losses in.
    :return: An option-values object with one attribute per option.
    """
    from LiuXin_alpha.file_formats.conversion.plugin import (
        InputFormatPlugin,
        OptionRecommendation,
        OutputFormatPlugin,
    )
    from LiuXin_alpha.file_formats.conversion.plumber import (
        OptionValues,
        pipeline_option_recommendations,
    )
    from LiuXin_alpha.file_formats.plugin_runtime import (
        input_profiles,
        output_profiles,
    )

    groups: list[Iterable[Any]] = [
        InputFormatPlugin.common_options,
        pipeline_option_recommendations(),
        OutputFormatPlugin.common_options,
    ]
    for plugin in plugins:
        groups.append(plugin.options)
    recommendations: dict[str, Any] = {}
    for group in groups:
        for recommendation in group:
            name = str(recommendation.option.name)
            if name not in recommendations:
                recommendations[name] = recommendation.clone()

    for plugin in plugins:
        for name, value, level in plugin.recommendations:
            current = recommendations.get(name)
            if current is not None and current.level <= level:
                current.recommended_value = value
                current.level = level

    high = int(OptionRecommendation.HIGH)
    for name, value in overrides.items():
        current = recommendations.get(name)
        if current is not None and current.level < high:
            current.recommended_value = value

    values: Any = OptionValues()
    for name, recommendation in recommendations.items():
        setattr(values, name, recommendation.recommended_value)
    _resolve_profile(values, "input_profile", input_profiles())
    _resolve_profile(values, "output_profile", output_profiles())
    values.conversion_report = report
    return values


def _resolve_profile(values: Any, attribute: str, profiles: Iterable[Any]) -> None:
    """
    Replace a profile's short name with the profile it names.

    An unknown name falls back to the default profile, as in ``Plumber``.

    Example:
        >>> class Profile:
        ...     def __init__(self, short_name):
        ...         self.short_name = short_name
        >>> class Values:
        ...     output_profile = "kindle"
        >>> values = Values()
        >>> _resolve_profile(values, "output_profile", [Profile("default")])
        >>> values.output_profile.short_name
        'default'


    :param values: The option values holding the short name.
    :param attribute: ``input_profile`` or ``output_profile``.
    :param profiles: The available profiles.
    :return: None after the attribute holds a profile object.
    """
    available = tuple(profiles)
    wanted = getattr(values, attribute, None)
    for profile in available:
        if profile.short_name == wanted:
            setattr(values, attribute, profile)
            return
    for profile in available:
        if profile.short_name == "default":
            setattr(values, attribute, profile)
            return


class LegacyInputAdapter(OEBImporter):
    """
    Import a format into an OEB book by running an ``InputFormatPlugin``.

    Subclasses are made by :func:`adapt_input_plugin`, one per plugin class;
    ``legacy_plugin`` names the class to run.

    The plugin runs in a new folder under the context's work folder, because
    a legacy input plugin empties the folder it is given. The book returned
    reads its content from that folder, so the folder must outlive the book.
    The plugin instance is kept on the book as
    :data:`LEGACY_INPUT_PLUGIN_ATTRIBUTE`, because some output plugins ask
    the input plugin about the book.

    Example:
        >>> from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin
        >>> class TxtInput(InputFormatPlugin):
        ...     name = "Example TXT Input"
        ...     file_types = {"txt"}
        >>> adapter = adapt_input_plugin(TxtInput)
        >>> adapter.provider_id, sorted(adapter.oeb_source_formats)
        ('legacy.example-txt-input', ['txt'])
    """

    legacy_plugin: ClassVar[type[Any]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the provider record and that a plugin class is named.

        Example:
            >>> try:
            ...     LegacyInputAdapter.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            LegacyInputAdapter.provider_id must be set


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        _declared_plugin_class(cls)

    def import_oeb(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> OEBBook:
        """
        Run the plugin on one file and return the book it read.

        Example:
            Called by a route, never directly by a plugin::

                book = adapter().import_oeb(Path("in.epub"), "epub", context)


        :param source: The file to read. It is opened read-only.
        :param source_format: Which of ``oeb_source_formats`` the file is in.
        :param context: Options, log, report and work folder for this call.
        :return: The book, reading its content from a folder under
            ``context.work_dir``.
        """
        from LiuXin_alpha.file_formats.conversion.plumber import create_oebbook

        plugin = cast(LegacyInputPlugin, self.legacy_plugin(None))
        log = legacy_log(context.log)
        options = legacy_option_values((plugin,), context.options, context.report)
        folder = tempfile.mkdtemp(prefix="import-", dir=context.work_dir)
        with LEGACY_CALL_LOCK, plugin, source.open("rb") as stream:
            result = plugin(stream, options, source_format, log, {}, folder)
            if not hasattr(result, "manifest"):
                # create_oebbook treats an empty encoding as "detect it".
                result = create_oebbook(
                    log, result, options, encoding=plugin.output_encoding or ""
                )
            plugin.postprocess_book(result, options, log)
        setattr(result, LEGACY_INPUT_PLUGIN_ATTRIBUTE, plugin)
        return cast("OEBBook", result)


class LegacyOutputAdapter(OEBExporter):
    """
    Export an OEB book to a format by running an ``OutputFormatPlugin``.

    Subclasses are made by :func:`adapt_output_plugin`, one per plugin class;
    ``legacy_plugin`` names the class to run.

    Legacy output plugins change the book they write, so a book must not be
    used again after it has been exported.

    Example:
        >>> from LiuXin_alpha.file_formats.conversion.plugin import OutputFormatPlugin
        >>> class TxtOutput(OutputFormatPlugin):
        ...     name = "Example TXT Output"
        ...     file_type = "txt"
        >>> adapter = adapt_output_plugin(TxtOutput)
        >>> adapter.provider_id, sorted(adapter.oeb_target_formats)
        ('legacy.example-txt-output', ['txt'])
    """

    legacy_plugin: ClassVar[type[Any]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the provider record and that a plugin class is named.

        Example:
            >>> try:
            ...     LegacyOutputAdapter.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            LegacyOutputAdapter.provider_id must be set


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        _declared_plugin_class(cls)

    def export_oeb(
        self: Self,
        book: OEBBook,
        target: Path,
        target_format: str,
        context: ProviderContext,
    ) -> None:
        """
        Run the plugin to write one book to one file.

        As ``Plumber`` does, this tells the book which format it is being
        written as, tells the options whether the book is a collection of
        images, and turns off inline navigation bars for MOBI output on a
        profile with MOBI indexing.

        Example:
            Called by a route, never directly by a plugin::

                adapter().export_oeb(book, Path("out.epub"), "epub", context)


        :param book: The book to write. The plugin may change it.
        :param target: Where to write. The plugin creates this file or folder.
        :param target_format: Which of ``oeb_target_formats`` to write.
        :param context: Options, log, report and work folder for this call.
        :return: None.
        """
        plugin = cast(LegacyOutputPlugin, self.legacy_plugin(None))
        log = legacy_log(context.log)
        options = legacy_option_values((plugin,), context.options, context.report)
        legacy_book: Any = book
        source_plugin: Any = getattr(legacy_book, LEGACY_INPUT_PLUGIN_ATTRIBUTE, None)
        if source_plugin is None:
            source_plugin = NoLegacyInputPlugin()
        options.is_image_collection = source_plugin.is_image_collection
        profile = getattr(options, "output_profile", None)
        options.no_inline_navbars = (
            bool(getattr(profile, "supports_mobi_indexing", False))
            and target_format == "mobi"
        )
        legacy_book.plumber_output_format = target_format
        with LEGACY_CALL_LOCK, plugin:
            plugin.convert(legacy_book, str(target), source_plugin, options, log)


def _declared_plugin_class(provider: type[Any]) -> type[Any]:
    """
    Read the plugin class an adapter declares.

    Example:
        >>> class Demo:
        ...     __name__ = "Demo"
        ...     legacy_plugin = int
        >>> _declared_plugin_class(Demo)
        <class 'int'>


    :param provider: The adapter class being checked.
    :return: The plugin class.
    :raises ProviderDefinitionError: If it is unset or not a class.
    """
    plugin = declared(provider, "legacy_plugin")
    if not isinstance(plugin, type):
        raise ProviderDefinitionError(
            f"{provider.__name__}.legacy_plugin must be a class, got {plugin!r}"
        )
    return plugin


def legacy_provider_id(plugin: type[Any], *, prefix: str = "legacy") -> str:
    """
    Make a provider id from a plugin's name.

    Example:
        >>> class Plugin:
        ...     name = "HTMLZ Input"
        >>> legacy_provider_id(Plugin)
        'legacy.htmlz-input'


    :param plugin: The plugin class.
    :param prefix: The first segment of the id.
    :return: ``prefix``, a dot, and the name in lower case with each run of
        other characters replaced by one hyphen.
    :raises ProviderDefinitionError: If the name has no letters or digits.
    """
    name = str(getattr(plugin, "name", ""))
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not slug:
        raise ProviderDefinitionError(
            f"{plugin!r} has no name to make a provider id from"
        )
    return f"{prefix}.{slug}"


def _legacy_version(plugin: type[Any]) -> str:
    """
    Write a plugin's version tuple as dotted text.

    Example:
        >>> class Plugin:
        ...     version = (1, 2, 0)
        >>> _legacy_version(Plugin)
        '1.2.0'


    :param plugin: The plugin class.
    :return: The version, or ``"0"`` if the plugin gives none.
    """
    version: object = getattr(plugin, "version", None)
    if isinstance(version, tuple):
        parts = cast("tuple[object, ...]", version)
        return ".".join(str(part) for part in parts) or "0"
    return str(version) if version else "0"


def _format_names(values: Iterable[object]) -> frozenset[str]:
    """
    Normalise a plugin's format names, dropping any that are empty.

    Example:
        >>> sorted(_format_names(["EPUB", ".kepub", ""]))
        ['epub', 'kepub']


    :param values: Format names as a plugin declares them.
    :return: The names in normal form.
    """
    names = (normalize_format_name(str(value)) for value in values)
    return frozenset(name for name in names if name)


def adapt_input_plugin(plugin: type[Any]) -> type[LegacyInputAdapter]:
    """
    Make an importer class that runs one ``InputFormatPlugin`` class.

    Example:
        >>> from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin
        >>> class PdbInput(InputFormatPlugin):
        ...     name = "Example PDB Input"
        ...     version = (2, 0)
        ...     file_types = {"PDB", "updb"}
        >>> adapter = adapt_input_plugin(PdbInput)
        >>> adapter.version, sorted(adapter.oeb_source_formats), adapter.priority
        ('2.0', ['pdb', 'updb'], -100)


    :param plugin: The plugin class.
    :return: A new importer class; register it to use it.
    """
    formats: Iterable[object] = getattr(plugin, "file_types", ())
    attributes: dict[str, object] = {
        "provider_id": legacy_provider_id(plugin),
        "version": _legacy_version(plugin),
        "licence": LEGACY_LICENCE,
        "priority": LEGACY_PRIORITY,
        "oeb_source_formats": _format_names(formats),
        "legacy_plugin": plugin,
        "__module__": __name__,
        "__doc__": f"Run {getattr(plugin, 'name', plugin.__name__)} as an importer.",
    }
    return _adapter_class(f"{plugin.__name__}Importer", LegacyInputAdapter, attributes)


def adapt_output_plugin(plugin: type[Any]) -> type[LegacyOutputAdapter]:
    """
    Make an exporter class that runs one ``OutputFormatPlugin`` class.

    Example:
        >>> from LiuXin_alpha.file_formats.conversion.plugin import OutputFormatPlugin
        >>> class RbOutput(OutputFormatPlugin):
        ...     name = "Example RB Output"
        ...     file_type = "rb"
        >>> sorted(adapt_output_plugin(RbOutput).oeb_target_formats)
        ['rb']


    :param plugin: The plugin class.
    :return: A new exporter class; register it to use it.
    """
    attributes: dict[str, object] = {
        "provider_id": legacy_provider_id(plugin),
        "version": _legacy_version(plugin),
        "licence": LEGACY_LICENCE,
        "priority": LEGACY_PRIORITY,
        "oeb_target_formats": _format_names([getattr(plugin, "file_type", "")]),
        "legacy_plugin": plugin,
        "__module__": __name__,
        "__doc__": f"Run {getattr(plugin, 'name', plugin.__name__)} as an exporter.",
    }
    return _adapter_class(f"{plugin.__name__}Exporter", LegacyOutputAdapter, attributes)


def _adapter_class[A: LegacyInputAdapter | LegacyOutputAdapter](
    name: str, base: type[A], attributes: Mapping[str, object]
) -> type[A]:
    """
    Build one adapter class the way a ``class`` statement would.

    Example:
        >>> made = _adapter_class("Made", LegacyOutputAdapter, {"priority": 3})
        >>> made.__name__, made.priority, issubclass(made, LegacyOutputAdapter)
        ('Made', 3, True)


    :param name: The class name.
    :param base: The adapter base class.
    :param attributes: The class attributes.
    :return: The new class.
    """

    def fill(namespace: dict[str, object]) -> None:
        """
        Copy the attributes into the class body.

        Example:
            >>> _adapter_class("Empty", LegacyOutputAdapter, {}).__name__
            'Empty'


        :param namespace: The class body being built.
        :return: None after the attributes are copied.
        """
        namespace.update(attributes)

    return cast("type[A]", types.new_class(name, (base,), exec_body=fill))


def register_bundled_conversion_plugins(
    registry: ProviderRegistry,
) -> tuple[type[LegacyInputAdapter | LegacyOutputAdapter], ...]:
    """
    Register an adapter for every bundled conversion plugin.

    The bundled plugins are those ``file_formats.conversion.builtins`` loaded,
    in its order. Plugins named in :data:`UNADAPTED_INPUT_PLUGINS` are left
    out. Importing the plugins happens here, not when this module is
    imported.

    Example:
        Assemble a registry holding today's conversion plugins::

            registry = ProviderRegistry()
            register_bundled_conversion_plugins(registry)
            importer = registry.oeb_importers("epub")[0]


    :param registry: The registry to add the adapters to.
    :return: The adapter classes registered, importers first.
    """
    from LiuXin_alpha.file_formats.conversion.builtins import (
        get_input_plugins,
        get_output_plugins,
    )

    adapters: list[type[LegacyInputAdapter | LegacyOutputAdapter]] = []
    input_plugins: Iterable[type[Any]] = get_input_plugins()
    output_plugins: Iterable[type[Any]] = get_output_plugins()
    for plugin in input_plugins:
        if getattr(plugin, "name", None) not in UNADAPTED_INPUT_PLUGINS:
            adapters.append(adapt_input_plugin(plugin))
    for plugin in output_plugins:
        adapters.append(adapt_output_plugin(plugin))
    for adapter in adapters:
        registry.register(adapter)
    return tuple(adapters)
