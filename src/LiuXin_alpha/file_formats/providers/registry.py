"""
A registry that files provider classes by the capabilities they offer.

The registry holds classes, not instances, and answers one kind of question:
which providers can do this piece of work, best first. It does not choose
between them according to settings, check whether they can run here, or run
anything; those belong to the layers that will be built on top of it.

There is no shared registry object. Whatever assembles the application creates
one and registers providers with it, so importing a provider module never
changes what a conversion will do.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterable
from types import ModuleType
from typing import Self

from LiuXin_alpha.file_formats.conversion.edges import (
    ConversionEdge,
    normalize_format_name,
)
from LiuXin_alpha.file_formats.providers.base import (
    Provider,
    ProviderDefinitionError,
    validate_provider_class,
)
from LiuXin_alpha.file_formats.providers.conversion import (
    Converter,
    HTMLExporter,
    HTMLImporter,
    HTMLTransform,
    OEBExporter,
    OEBImporter,
    OEBTransform,
)
from LiuXin_alpha.file_formats.providers.stages import (
    HTMLStage,
    OEBStage,
    stage_position,
)

CAPABILITIES: tuple[type[Provider], ...] = (
    OEBImporter,
    HTMLImporter,
    OEBExporter,
    HTMLExporter,
    OEBTransform,
    HTMLTransform,
    Converter,
)
"""Every capability base class a provider may offer."""


def capabilities_of(provider: type[Provider]) -> tuple[type[Provider], ...]:
    """
    List the capabilities a provider class offers.

    Example:
        >>> class Both(OEBImporter, OEBExporter):
        ...     pass
        >>> [capability.__name__ for capability in capabilities_of(Both)]
        ['OEBImporter', 'OEBExporter']


    :param provider: The provider class to inspect.
    :return: The capability base classes it subclasses, in the order of
        :data:`CAPABILITIES`.
    """
    return tuple(
        capability for capability in CAPABILITIES if issubclass(provider, capability)
    )


class ProviderRegistry:
    """
    Hold provider classes and find the ones able to do a piece of work.

    Every query returns the matching providers best first: higher
    ``priority`` before lower, and, where priorities are equal, the provider
    registered earlier before the one registered later. A caller that wants
    the default takes the first; a caller building a fallback chain takes
    them in order.

    Example:
        >>> class TextImporter(OEBImporter):
        ...     provider_id = "txt.oeb-importer.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     oeb_source_formats = frozenset({"txt"})
        ...     def import_oeb(self, source, source_format, context):
        ...         raise NotImplementedError
        >>> registry = ProviderRegistry()
        >>> registry.register(TextImporter)
        >>> [found.provider_id for found in registry.oeb_importers(".TXT")]
        ['txt.oeb-importer.example']
        >>> registry.oeb_importers("epub")
        ()
    """

    def __init__(self: Self) -> None:
        """
        Create an empty registry.

        Example:
            >>> ProviderRegistry().providers()
            ()


        :return: ``None``.
        """
        self._providers: dict[str, type[Provider]] = {}

    def register(self: Self, provider: type[Provider]) -> None:
        """
        Add a provider class after checking that it is complete.

        Registering the same class again changes nothing, so a module may be
        registered twice without harm.

        Example:
            >>> class Unfinished(OEBImporter):
            ...     provider_id = "unfinished.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     oeb_source_formats = frozenset({"txt"})
            >>> try:
            ...     ProviderRegistry().register(Unfinished)
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Unfinished does not implement: import_oeb


        :param provider: The class to add.
        :return: ``None``.
        :raises ProviderDefinitionError: If the class is incomplete, declares
            a malformed record, offers no capability, or uses the id of a
            different class that is already registered.
        """
        checked = validate_provider_class(provider)
        if not capabilities_of(checked):
            raise ProviderDefinitionError(f"{checked.__name__} offers no capability")
        provider_id = checked.provider_id
        existing = self._providers.get(provider_id)
        if existing is checked:
            return
        if existing is not None:
            raise ProviderDefinitionError(
                f"provider id {provider_id!r} is already registered by "
                f"{existing.__module__}.{existing.__qualname__}; "
                f"{checked.__module__}.{checked.__qualname__} cannot reuse it"
            )
        self._providers[provider_id] = checked

    def register_module(self: Self, module: ModuleType) -> tuple[type[Provider], ...]:
        """
        Add every finished provider class that a module defines.

        Classes are taken in the order the module defines them. A class is
        left out if it was imported from elsewhere, still has abstract
        methods, or offers no capability; these are how a module holds a
        shared helper class. Every other provider class must register
        cleanly.

        Example:
            >>> import types
            >>> module = types.ModuleType("example_providers")
            >>> class Tidy(OEBTransform):
            ...     provider_id = "oeb.tidy.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     oeb_stage = OEBStage.NORMALISE
            ...     def transform_oeb(self, book, context):
            ...         return book
            >>> Tidy.__module__ = module.__name__
            >>> module.Tidy = Tidy
            >>> module.Reused = OEBTransform
            >>> registry = ProviderRegistry()
            >>> [found.__name__ for found in registry.register_module(module)]
            ['Tidy']


        :param module: An imported module to take provider classes from.
        :return: The classes that were registered, in definition order.
        :raises ProviderDefinitionError: If a finished provider class in the
            module cannot be registered.
        """
        registered: list[type[Provider]] = []
        for value in tuple(vars(module).values()):
            if not isinstance(value, type) or not issubclass(value, Provider):
                continue
            if value.__module__ != module.__name__ or inspect.isabstract(value):
                continue
            if not capabilities_of(value):
                continue
            self.register(value)
            registered.append(value)
        return tuple(registered)

    def providers(self: Self) -> tuple[type[Provider], ...]:
        """
        List every registered provider in the order it was registered.

        Example:
            >>> ProviderRegistry().providers()
            ()


        :return: The registered classes.
        """
        return tuple(self._providers.values())

    def provider(self: Self, provider_id: str) -> type[Provider]:
        """
        Find a registered provider by its id.

        Example:
            >>> try:
            ...     ProviderRegistry().provider("pdf.compress.example")
            ... except LookupError as error:
            ...     print(error)
            no provider is registered as 'pdf.compress.example'


        :param provider_id: The id the provider declares.
        :return: The provider class.
        :raises LookupError: If no provider with that id is registered.
        """
        try:
            return self._providers[provider_id]
        except KeyError:
            raise LookupError(f"no provider is registered as {provider_id!r}") from None

    def oeb_importers(self: Self, source_format: str) -> tuple[type[OEBImporter], ...]:
        """
        Find the providers that can read a format into an OEB book.

        Example:
            >>> ProviderRegistry().oeb_importers("epub")
            ()


        :param source_format: The format to read; case and a leading dot are
            ignored.
        :return: The matching providers, best first.
        """
        wanted = normalize_format_name(source_format)
        return _best_first(
            found
            for found in self._providers.values()
            if issubclass(found, OEBImporter) and wanted in found.oeb_source_formats
        )

    def html_importers(
        self: Self, source_format: str
    ) -> tuple[type[HTMLImporter], ...]:
        """
        Find the providers that can read a format into an HTML book.

        Example:
            >>> ProviderRegistry().html_importers("pdf")
            ()


        :param source_format: The format to read; case and a leading dot are
            ignored.
        :return: The matching providers, best first.
        """
        wanted = normalize_format_name(source_format)
        return _best_first(
            found
            for found in self._providers.values()
            if issubclass(found, HTMLImporter) and wanted in found.html_source_formats
        )

    def oeb_exporters(self: Self, target_format: str) -> tuple[type[OEBExporter], ...]:
        """
        Find the providers that can write an OEB book out in a format.

        Example:
            >>> ProviderRegistry().oeb_exporters("epub")
            ()


        :param target_format: The format to write; case and a leading dot are
            ignored.
        :return: The matching providers, best first.
        """
        wanted = normalize_format_name(target_format)
        return _best_first(
            found
            for found in self._providers.values()
            if issubclass(found, OEBExporter) and wanted in found.oeb_target_formats
        )

    def html_exporters(
        self: Self, target_format: str
    ) -> tuple[type[HTMLExporter], ...]:
        """
        Find the providers that can write an HTML book out in a format.

        Example:
            >>> ProviderRegistry().html_exporters("pdf")
            ()


        :param target_format: The format to write; case and a leading dot are
            ignored.
        :return: The matching providers, best first.
        """
        wanted = normalize_format_name(target_format)
        return _best_first(
            found
            for found in self._providers.values()
            if issubclass(found, HTMLExporter) and wanted in found.html_target_formats
        )

    def converters(
        self: Self, source_format: str, target_format: str
    ) -> tuple[type[Converter], ...]:
        """
        Find the providers that convert one format straight to another.

        The two formats may be the same, which finds same-format converters
        such as compressors.

        Example:
            >>> ProviderRegistry().converters("pdf", "pdf")
            ()


        :param source_format: The format to read; case and a leading dot are
            ignored.
        :param target_format: The format to write; case and a leading dot are
            ignored.
        :return: The matching providers, best first.
        """
        wanted = (
            normalize_format_name(source_format),
            normalize_format_name(target_format),
        )
        return _best_first(
            found
            for found in self._providers.values()
            if issubclass(found, Converter) and wanted in found.conversions
        )

    def oeb_transforms(
        self: Self, stage: OEBStage | None = None
    ) -> tuple[type[OEBTransform], ...]:
        """
        List the OEB transforms in the order they run.

        Example:
            >>> ProviderRegistry().oeb_transforms(OEBStage.STYLE)
            ()


        :param stage: Keep only the transforms of this stage; ``None`` keeps
            every stage.
        :return: The transforms by stage, and best first within a stage.
        """
        found = _best_first(
            transform
            for transform in self._providers.values()
            if issubclass(transform, OEBTransform)
            and (stage is None or transform.oeb_stage is stage)
        )
        return tuple(sorted(found, key=lambda item: stage_position(item.oeb_stage)))

    def html_transforms(
        self: Self, stage: HTMLStage | None = None
    ) -> tuple[type[HTMLTransform], ...]:
        """
        List the HTML transforms in the order they run.

        Example:
            >>> ProviderRegistry().html_transforms()
            ()


        :param stage: Keep only the transforms of this stage; ``None`` keeps
            every stage.
        :return: The transforms by stage, and best first within a stage.
        """
        found = _best_first(
            transform
            for transform in self._providers.values()
            if issubclass(transform, HTMLTransform)
            and (stage is None or transform.html_stage is stage)
        )
        return tuple(sorted(found, key=lambda item: stage_position(item.html_stage)))

    def converter_edges(self: Self) -> tuple[ConversionEdge, ...]:
        """
        Describe every registered converter's conversions as edges.

        The result can be given to the existing conversion edge registry, so
        that converters are listed beside the routes that go through OEB.

        Example:
            >>> ProviderRegistry().converter_edges()
            ()


        :return: The edges of each converter, in registration order.
        """
        edges: list[ConversionEdge] = []
        for found in self._providers.values():
            if issubclass(found, Converter):
                edges.extend(found.edges())
        return tuple(edges)


def _best_first[P: Provider](found: Iterable[type[P]]) -> tuple[type[P], ...]:
    """
    Order provider classes by priority, keeping the given order for ties.

    Example:
        >>> class Low(Provider):
        ...     priority = 1
        >>> class High(Provider):
        ...     priority = 5
        >>> class AlsoLow(Provider):
        ...     priority = 1
        >>> [item.__name__ for item in _best_first([Low, High, AlsoLow])]
        ['High', 'Low', 'AlsoLow']


    :param found: Provider classes in registration order.
    :return: The same classes, higher priority first.
    """
    return tuple(sorted(found, key=lambda item: -item.priority))
