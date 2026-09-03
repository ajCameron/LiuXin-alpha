"""Inspectable descriptions for the core RPC/API surface."""

from __future__ import annotations

import dataclasses

from typing import Any, Mapping


def _normalize_jsonish(value: Any) -> Any:
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
    if annotation in {None, dataclasses.MISSING}:
        return None
    text = getattr(annotation, "__name__", None)
    if text:
        return str(text)
    rendered = str(annotation)
    return None if rendered == "<class 'inspect._empty'>" else rendered


@dataclasses.dataclass(frozen=True)
class CorePayloadFieldDescription:
    """Serializable description of one structured endpoint payload field."""

    name: str
    required: bool = False
    field_type: str | None = None
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return the transport-safe mapping exposed by Core introspection."""

        return {
            "name": str(self.name),
            "required": bool(self.required),
            "field_type": None if self.field_type is None else str(self.field_type),
            "description": str(self.description or ""),
        }


@dataclasses.dataclass(frozen=True)
class CoreParameterDescription:
    """Serializable description of one callable Core method parameter."""

    name: str
    kind: str
    required: bool = True
    default: Any = None
    annotation: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return the transport-safe parameter description."""

        return {
            "name": str(self.name),
            "kind": str(self.kind),
            "required": bool(self.required),
            "default": _normalize_jsonish(self.default),
            "annotation": None if self.annotation is None else str(self.annotation),
        }


@dataclasses.dataclass(frozen=True)
class CoreEndpointDescription:
    """Serializable description of one named Core endpoint."""

    name: str
    kind: str
    summary: str = ""
    description: str = ""
    payload_fields: tuple[CorePayloadFieldDescription, ...] = ()
    tags: tuple[str, ...] = ()
    transport_stable: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Return the transport-safe endpoint description."""

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
    """Serializable description of one method on a Core target."""

    name: str
    write: bool
    summary: str = ""
    description: str = ""
    parameters: tuple[CoreParameterDescription, ...] = ()
    return_annotation: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return the transport-safe method description."""

        return {
            "name": str(self.name),
            "write": bool(self.write),
            "summary": str(self.summary or ""),
            "description": str(self.description or ""),
            "parameters": [param.to_dict() for param in self.parameters],
            "return_annotation": None if self.return_annotation is None else str(self.return_annotation),
        }


@dataclasses.dataclass(frozen=True)
class CoreTargetDescription:
    """Serializable description of one command/query dispatch target."""

    name: str
    aliases: tuple[str, ...] = ()
    summary: str = ""
    description: str = ""
    methods: tuple[CoreMethodDescription, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return the transport-safe target and its method descriptions."""

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
