"""
Build small file-format provider classes for tests.

Provider tests declare many short-lived classes that differ in one attribute.
:func:`declare` builds such a class in one expression, and :class:`Recorded`
carries a well-formed shared record for them to inherit.

Example:
    >>> from LiuXin_alpha.file_formats.providers.conversion import Converter
    >>> Broken = declare("Broken", Recorded, Converter, provider_id="Broken")
    >>> Broken.provider_id, Broken.licence
    ('Broken', 'GPL-3.0-only')
"""

from __future__ import annotations

import types
from typing import ClassVar

from LiuXin_alpha.file_formats.providers.base import Provider


class Recorded(Provider):
    """
    Carry a well-formed shared record for test providers to inherit.

    Example:
        >>> Recorded.validate_declaration()
    """

    provider_id: ClassVar[str] = "test.recorded"
    version: ClassVar[str] = "1.0"
    licence: ClassVar[str] = "GPL-3.0-only"


def declare(name: str, *bases: type[Provider], **attributes: object) -> type[Provider]:
    """
    Build a provider class from its bases and class attributes.

    The class is built the way a ``class`` statement builds it, so abstract
    methods and declarations behave exactly as they would in a plugin. Pass
    ``__module__`` to say which module the class belongs to.

    Example:
        >>> Plain = declare("Plain", Recorded, priority=3)
        >>> Plain.__name__, Plain.priority, issubclass(Plain, Recorded)
        ('Plain', 3, True)


    :param name: The class name, used in error messages.
    :param bases: The classes to inherit from, in order.
    :param attributes: The class attributes to set.
    :return: The new class.
    """

    def fill(namespace: dict[str, object]) -> None:
        """
        Copy the attributes into the class body.

        Example:
            >>> declare("Empty", Recorded).__name__
            'Empty'


        :param namespace: The class body being built.
        :return: None after the attributes are copied.
        """
        namespace.update(attributes)

    return types.new_class(name, bases, exec_body=fill)
