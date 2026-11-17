"""
Shared record, availability and call context for file-format providers.

Every capability base class derives from :class:`Provider`. The record a
provider declares is a set of class attributes, so it can be read without
creating an instance. Declaring a provider has no side effects; a declaration
is checked when the class is registered, by :func:`validate_provider_class`.
"""

from __future__ import annotations

import inspect
import logging
import re
from abc import ABC
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Self, cast

from LiuXin_alpha.file_formats.conversion.edges import normalize_format_name

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.conversion.report import ConversionReport

PROVIDER_ID_PATTERN = re.compile(r"[a-z0-9]+(?:[._-][a-z0-9]+)*")
"""A lower-case dotted name: letters and digits joined by ``.``, ``_`` or ``-``."""

_UNSET = object()


class ProviderDefinitionError(TypeError):
    """
    Report a provider class whose declared record is missing or malformed.

    The message names the class and the attribute at fault. It is a
    :class:`TypeError` because the mistake is in how the class was written,
    not in a value passed at run time.

    Example:
        >>> try:
        ...     raise ProviderDefinitionError("Demo.provider_id must be set")
        ... except TypeError as error:
        ...     print(error)
        Demo.provider_id must be set
    """


@dataclass(frozen=True, slots=True)
class ProviderAvailability:
    """
    Say whether a provider can run here, and if not, why.

    A provider that needs an optional package or an external tool reports its
    absence through this value, so that an unusable provider can explain
    itself and is never mistaken for one that ran and produced nothing.

    Example:
        >>> ProviderAvailability.ready().available
        True
        >>> ProviderAvailability.missing("pdftohtml is not installed").reason
        'pdftohtml is not installed'


    :param available: ``True`` when the provider can be used.
    :param reason: Why it cannot be used; required when unavailable and
        absent when available.
    """

    available: bool
    reason: str | None = None

    def __post_init__(self: Self) -> None:
        """
        Reject a reason on an available provider and silence on an unavailable one.

        Example:
            >>> ProviderAvailability(False)
            Traceback (most recent call last):
            ...
            ValueError: an unavailable provider must give a reason


        :return: ``None``.
        :raises ValueError: If the two fields contradict each other.
        """
        if self.available and self.reason is not None:
            raise ValueError("an available provider has no reason to report")
        if not self.available and not (self.reason and self.reason.strip()):
            raise ValueError("an unavailable provider must give a reason")

    @classmethod
    def ready(cls: type[Self]) -> ProviderAvailability:
        """
        Build the value for a provider that can be used.

        Example:
            >>> ProviderAvailability.ready()
            ProviderAvailability(available=True, reason=None)


        :return: An available result with no reason.
        """
        return cls(True)

    @classmethod
    def missing(cls: type[Self], reason: str) -> ProviderAvailability:
        """
        Build the value for a provider that cannot be used.

        Example:
            >>> ProviderAvailability.missing("needs the conversion extra").available
            False


        :param reason: What is absent, in words a person can act on.
        :return: An unavailable result carrying ``reason``.
        """
        return cls(False, reason)


@dataclass(frozen=True, slots=True)
class ProviderContext:
    """
    Carry what a provider needs for one call.

    A provider reads its options, logs and reports losses through this value.
    It may create files only under ``work_dir`` and at any target path it is
    given, and it must not depend on or change the process working directory:
    two calls with different work folders must not be able to disturb each
    other.

    Example:
        >>> import logging, pathlib
        >>> from LiuXin_alpha.file_formats.conversion.report import ConversionReport
        >>> context = ProviderContext(
        ...     work_dir=pathlib.Path.cwd(),
        ...     options={"max_levels": 5},
        ...     log=logging.getLogger("example"),
        ...     report=ConversionReport(),
        ... )
        >>> context.options["max_levels"]
        5


    :param work_dir: A folder owned by this call for scratch files.
    :param options: Settings for this call, by name.
    :param log: Where the provider writes messages for a person.
    :param report: Where the provider records warnings and losses.
    """

    work_dir: Path
    options: Mapping[str, object]
    log: logging.Logger
    report: ConversionReport


class Provider(ABC):
    """
    Declare the record every file-format provider carries.

    The record is class attributes, so a registry can list, order and describe
    providers without creating them. A subclass sets ``provider_id``,
    ``version`` and ``licence``, and may set ``priority`` and ``requires``.
    Capability base classes add the attributes their kind of work needs.

    A provider with a higher ``priority`` is preferred. ``requires`` names the
    optional packages or external tools the provider cannot work without; it
    is descriptive, and :meth:`availability` is what decides.

    Example:
        >>> class Demo(Provider):
        ...     provider_id = "demo.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        >>> Demo.priority, Demo.availability().available
        (0, True)
    """

    provider_id: ClassVar[str]
    version: ClassVar[str]
    licence: ClassVar[str]
    priority: ClassVar[int] = 0
    requires: ClassVar[tuple[str, ...]] = ()

    @classmethod
    def availability(cls: type[Self]) -> ProviderAvailability:
        """
        Report whether this provider can run in the current environment.

        The default says it can. A provider that depends on something optional
        overrides this to look for it and to name what is missing. The check
        should be cheap and must not raise for an ordinary absence.

        Example:
            >>> class Demo(Provider):
            ...     provider_id = "demo.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            >>> Demo.availability()
            ProviderAvailability(available=True, reason=None)


        :return: Whether the provider is usable, with a reason when it is not.
        """
        return ProviderAvailability.ready()

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the attributes this class declares for the shared record.

        Capability base classes override this, call ``super()`` first, and
        then check their own attributes. A provider offering several
        capabilities is therefore checked for every one of them.

        Example:
            >>> class Demo(Provider):
            ...     provider_id = "Demo Provider"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            >>> try:
            ...     Demo.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Demo.provider_id must be a lower-case dotted name, got 'Demo Provider'


        :return: ``None`` when the shared record is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        provider_id = declared_text(cls, "provider_id")
        if PROVIDER_ID_PATTERN.fullmatch(provider_id) is None:
            raise ProviderDefinitionError(
                f"{cls.__name__}.provider_id must be a lower-case dotted name, "
                f"got {provider_id!r}"
            )
        declared_text(cls, "version")
        declared_text(cls, "licence")
        priority = declared(cls, "priority")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ProviderDefinitionError(
                f"{cls.__name__}.priority must be an integer, got {priority!r}"
            )
        requires = declared(cls, "requires")
        if not isinstance(requires, tuple) or not all(
            isinstance(item, str) and item.strip()
            for item in cast("tuple[object, ...]", requires)
        ):
            raise ProviderDefinitionError(
                f"{cls.__name__}.requires must be a tuple of non-empty strings, "
                f"got {requires!r}"
            )


def declared(provider: type[Provider], name: str) -> object:
    """
    Read a record attribute from a provider class, insisting that it is set.

    Example:
        >>> class Demo(Provider):
        ...     version = "2"
        >>> declared(Demo, "version")
        '2'


    :param provider: The provider class being checked.
    :param name: The attribute to read.
    :return: The attribute's value, with no assumption about its type.
    :raises ProviderDefinitionError: If the class does not set the attribute.
    """
    value: object = getattr(provider, name, _UNSET)
    if value is _UNSET:
        raise ProviderDefinitionError(f"{provider.__name__}.{name} must be set")
    return value


def declared_text(provider: type[Provider], name: str) -> str:
    """
    Read a record attribute that must be a non-blank string.

    Example:
        >>> class Demo(Provider):
        ...     licence = "GPL-3.0-only"
        >>> declared_text(Demo, "licence")
        'GPL-3.0-only'


    :param provider: The provider class being checked.
    :param name: The attribute to read.
    :return: The attribute's value.
    :raises ProviderDefinitionError: If it is unset, not a string, or blank.
    """
    value = declared(provider, name)
    if not isinstance(value, str) or not value.strip():
        raise ProviderDefinitionError(
            f"{provider.__name__}.{name} must be a non-empty string, got {value!r}"
        )
    return value


def is_format_name(value: object) -> bool:
    """
    Say whether a value is a format name in its normal form.

    The normal form is the one :func:`normalize_format_name` produces: lower
    case with no leading dot. Declarations must already be in that form, so a
    record reads the same everywhere it is shown.

    Example:
        >>> is_format_name("epub"), is_format_name(".EPUB"), is_format_name("")
        (True, False, False)


    :param value: The candidate, of any type.
    :return: ``True`` for a non-empty string that normalising leaves unchanged.
    """
    return (
        isinstance(value, str) and bool(value) and normalize_format_name(value) == value
    )


def declared_formats(provider: type[Provider], name: str) -> frozenset[str]:
    """
    Read a record attribute that must be a non-empty frozen set of format names.

    Example:
        >>> class Demo(Provider):
        ...     oeb_source_formats = frozenset({"epub", "kepub"})
        >>> sorted(declared_formats(Demo, "oeb_source_formats"))
        ['epub', 'kepub']


    :param provider: The provider class being checked.
    :param name: The attribute to read.
    :return: The declared format names.
    :raises ProviderDefinitionError: If it is unset, not a frozen set, empty,
        or holds a name that is not in normal form.
    """
    value = declared(provider, name)
    if not isinstance(value, frozenset) or not value:
        raise ProviderDefinitionError(
            f"{provider.__name__}.{name} must be a non-empty frozenset of "
            f"format names, got {value!r}"
        )
    formats: set[str] = set()
    for item in cast("frozenset[object]", value):
        if not isinstance(item, str) or not is_format_name(item):
            raise ProviderDefinitionError(
                f"{provider.__name__}.{name} must hold lower-case format names "
                f"with no leading dot, got {item!r}"
            )
        formats.add(item)
    return frozenset(formats)


def validate_provider_class(provider: object) -> type[Provider]:
    """
    Check that an object is a complete, well-declared provider class.

    A complete provider implements every abstract method of the capabilities
    it subclasses, and its declared record passes
    :meth:`Provider.validate_declaration` for each of them. Nothing is created
    or registered. The argument may be anything, because this is the check
    made on what a plugin hands in.

    Example:
        >>> class Demo(Provider):
        ...     provider_id = "demo.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        >>> validate_provider_class(Demo) is Demo
        True
        >>> try:
        ...     validate_provider_class("demo.example")
        ... except ProviderDefinitionError as error:
        ...     print(error)
        'demo.example' is not a Provider subclass


    :param provider: The candidate.
    :return: The candidate, known to be a provider class.
    :raises ProviderDefinitionError: If it is not a provider class, still has
        abstract methods, or declares a malformed record.
    """
    if not isinstance(provider, type) or not issubclass(provider, Provider):
        raise ProviderDefinitionError(f"{provider!r} is not a Provider subclass")
    if inspect.isabstract(provider):
        missing = ", ".join(sorted(provider.__abstractmethods__))
        raise ProviderDefinitionError(
            f"{provider.__name__} does not implement: {missing}"
        )
    provider.validate_declaration()
    return provider
