"""
Describe named endpoints and dynamic method targets for Core API introspection.

The frozen records describe contracts but do not enforce them. Their dictionary
methods normalize names/flags and recursively render child descriptions. Callable
defaults use a deliberately lossy display conversion, not the strict Core wire
encoder; nonfinite floats and repr-dependent text can survive that conversion.
"""

from __future__ import annotations

import dataclasses

from typing import Any, Mapping


def _normalize_jsonish(value: Any) -> Any:
    """
    Render a callable default using simple containers and repr fallbacks for unfamiliar values.

    ``dataclasses.MISSING`` becomes ``None``; tuples become lists and dictionary
    keys become strings without collision checks. Floats pass through even when
    nonfinite. Other mappings, bytes, and arbitrary objects use ``repr`` rather
    than typed wire tags. There is no cycle detection or reversible decoding.

    Example:
        >>> _normalize_jsonish({"missing": dataclasses.MISSING, "pair": (1, 2)})
        {'missing': None, 'pair': [1, 2]}
        >>> _normalize_jsonish(b"hi")
        "b'hi'"


    :param value: Default value or nested structure to display in an introspection response.
    :return: Simplified scalar/container or repr string; strict JSON finiteness is not guaranteed.
    """
    if value is dataclasses.MISSING:
        return None
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_normalize_jsonish(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _normalize_jsonish(v) for k, v in value.items()}
    return repr(value)


def _annotation_text(annotation: Any) -> str | None:
    """
    Prefer an annotation's short name, falling back to text while hiding absence sentinels.

    ``None`` and ``dataclasses.MISSING`` are absent, as is the exact rendered
    ``inspect._empty`` spelling. A truthy ``__name__`` takes precedence, which can
    discard qualification or generic arguments. The initial set-membership check
    requires a hashable annotation.

    Example:
        >>> _annotation_text(int), _annotation_text("str | None"), _annotation_text(None)
        ('int', 'str | None', None)


    :param annotation: Hashable annotation-like object or absence sentinel.
    :return: Short annotation text or ``None`` for a recognized absence value.
    :raises TypeError: If the annotation is unhashable during sentinel checking.
    """
    if annotation in {None, dataclasses.MISSING}:
        return None
    text = getattr(annotation, "__name__", None)
    if text:
        return str(text)
    rendered = str(annotation)
    return None if rendered == "<class 'inspect._empty'>" else rendered


@dataclasses.dataclass(frozen=True)
class CorePayloadFieldDescription:
    """
    Advertise a structured payload field's name, required flag, optional type label, and explanatory prose.

    These values describe endpoint expectations; this record does not validate
    payload values or interpret the type label as an executable schema.

    Example:
        >>> field = CorePayloadFieldDescription("job_id", required=True, field_type="str")
        >>> field.to_dict()["required"]
        True
    """

    name: str
    required: bool = False
    field_type: str | None = None
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """
        Copy field metadata to a dictionary, stringifying labels and truth-converting requiredness.

        A missing type stays ``None``; a falsey description becomes empty text.

        Example:
            >>> CorePayloadFieldDescription("limit").to_dict()
            {'name': 'limit', 'required': False, 'field_type': None, 'description': ''}


        :return: New four-field dictionary without validating the advertised field's runtime values.
        """

        return {
            "name": str(self.name),
            "required": bool(self.required),
            "field_type": None if self.field_type is None else str(self.field_type),
            "description": str(self.description or ""),
        }


@dataclasses.dataclass(frozen=True)
class CoreParameterDescription:
    """
    Describe a callable parameter's name, invocation kind, requiredness, default, and annotation text.

    ``kind`` is descriptive text, not an enforced enum. The default is retained
    by reference until rendered, so freezing the record does not freeze a mutable
    default. ``required`` distinguishes absence from a legitimate ``None`` default.

    Example:
        >>> parameter = CoreParameterDescription("limit", "keyword_only", False, 20, "int")
        >>> parameter.to_dict()["default"]
        20
    """

    name: str
    kind: str
    required: bool = True
    default: Any = None
    annotation: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """
        Render parameter labels and a lossy JSON-like default without invoking the described callable.

        The default converter does not reject nonfinite floats or cycles in
        advance; this method is not the strict Core transport encoder.

        Example:
            >>> CoreParameterDescription("names", "positional_or_keyword", False, ("a", "b")).to_dict()["default"]
            ['a', 'b']


        :return: New parameter dictionary with textual labels, boolean requiredness, and rendered default.
        """

        return {
            "name": str(self.name),
            "kind": str(self.kind),
            "required": bool(self.required),
            "default": _normalize_jsonish(self.default),
            "annotation": None if self.annotation is None else str(self.annotation),
        }


@dataclasses.dataclass(frozen=True)
class CoreEndpointDescription:
    """
    Advertise a named command/query endpoint with prose, ordered payload fields, tags, and a stability declaration.

    ``kind`` identifies the advertised route family. ``transport_stable`` is a
    declaration supplied by registration, not a check performed by this record.
    Neither names, duplicate fields, nor tags are validated on construction.

    Example:
        >>> endpoint = CoreEndpointDescription("jobs.list", "query", tags=("jobs",))
        >>> endpoint.to_dict()["tags"]
        ['jobs']
    """

    name: str
    kind: str
    summary: str = ""
    description: str = ""
    payload_fields: tuple[CorePayloadFieldDescription, ...] = ()
    tags: tuple[str, ...] = ()
    transport_stable: bool = True

    def to_dict(self) -> dict[str, Any]:
        """
        Render endpoint prose, ordered child fields, tags, and the truth-converted stability flag.

        Example:
            >>> CoreEndpointDescription("jobs.list", "query").to_dict()["payload_fields"]
            []


        :return: New endpoint dictionary with lists for payload fields and tags; no deduplication or routing occurs.
        """

        return {
            "name": str(self.name),
            "kind": str(self.kind),
            "summary": str(self.summary or ""),
            "description": str(self.description or ""),
            "payload_fields": [field.to_dict() for field in self.payload_fields],
            "tags": [str(tag) for tag in self.tags],
            "transport_stable": bool(self.transport_stable),
        }


@dataclasses.dataclass(frozen=True)
class CoreMethodDescription:
    """
    Describe a dynamic target method's name, write classification, prose, ordered parameters, and return annotation.

    ``write`` records the producer's classification; it does not inspect effects
    or grant permission to call the method. Annotation strings remain explanatory
    metadata rather than validators.

    Example:
        >>> method = CoreMethodDescription("get_row", write=False, return_annotation="Row | None")
        >>> method.to_dict()["return_annotation"]
        'Row | None'
    """

    name: str
    write: bool
    summary: str = ""
    description: str = ""
    parameters: tuple[CoreParameterDescription, ...] = ()
    return_annotation: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """
        Render method labels and child parameter descriptions without invoking the target method.

        Parameter defaults inherit the child renderer's lossy/nonfinite-value
        limitations. A missing return annotation remains ``None``.

        Example:
            >>> CoreMethodDescription("get_row", write=False).to_dict()["write"]
            False


        :return: New method dictionary with ordered parameter dictionaries and normalized labels/flags.
        """

        return {
            "name": str(self.name),
            "write": bool(self.write),
            "summary": str(self.summary or ""),
            "description": str(self.description or ""),
            "parameters": [param.to_dict() for param in self.parameters],
            "return_annotation": None
            if self.return_annotation is None
            else str(self.return_annotation),
        }


@dataclasses.dataclass(frozen=True)
class CoreTargetDescription:
    """
    Describe a dispatch target's canonical name, aliases, prose, and advertised methods.

    The record preserves supplied alias/method order. It neither binds aliases
    into a registry nor checks uniqueness, target existence, or callability.

    Example:
        >>> target = CoreTargetDescription("database", aliases=("db",))
        >>> target.to_dict()["aliases"]
        ['db']
    """

    name: str
    aliases: tuple[str, ...] = ()
    summary: str = ""
    description: str = ""
    methods: tuple[CoreMethodDescription, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """
        Render target labels, ordered aliases, and recursively rendered method descriptions.

        Nested parameter defaults retain their display converter's limitations;
        this is not an additional strict transport-validation pass.

        Example:
            >>> CoreTargetDescription("database").to_dict()["methods"]
            []


        :return: New target dictionary containing text aliases and child method dictionaries.
        """

        return {
            "name": str(self.name),
            "aliases": [str(alias) for alias in self.aliases],
            "summary": str(self.summary or ""),
            "description": str(self.description or ""),
            "methods": [method.to_dict() for method in self.methods],
        }


__all__ = [
    "CoreEndpointDescription",
    "CoreMethodDescription",
    "CoreParameterDescription",
    "CorePayloadFieldDescription",
    "CoreTargetDescription",
]
