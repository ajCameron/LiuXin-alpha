from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Iterable


class ConversionEdgeKind(StrEnum):
    OEB = "oeb"
    DIRECT = "direct"
    EXTERNAL = "external"


def normalize_format_name(format_name: str) -> str:
    return str(format_name).strip().lower().removeprefix(".")


@dataclass(frozen=True, slots=True)
class ConversionEdge:
    source_format: str
    target_format: str
    kind: ConversionEdgeKind
    name: str
    priority: int = 100
    intermediate_format: str | None = None
    input_plugin_name: str | None = None
    output_plugin_name: str | None = None
    external_tool: str | None = None
    external_tool_version: str | None = None
    lossless: bool | None = None
    notes: tuple[str, ...] = ()

    def supports(self, source_format: str, target_format: str) -> bool:
        source = normalize_format_name(source_format)
        target = normalize_format_name(target_format)
        return self.source_format == source and self.target_format == target

    def to_mapping(self) -> dict[str, object]:
        return {
            "name": self.name,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "kind": self.kind.value,
            "priority": self.priority,
            "intermediate_format": self.intermediate_format,
            "input_plugin_name": self.input_plugin_name,
            "output_plugin_name": self.output_plugin_name,
            "external_tool": self.external_tool,
            "external_tool_version": self.external_tool_version,
            "lossless": self.lossless,
            "notes": list(self.notes),
        }


@dataclass(slots=True)
class ConversionEdgeRegistry:
    edges: list[ConversionEdge] = field(default_factory=list)

    def register(self, edge: ConversionEdge) -> ConversionEdge:
        self.edges.append(edge)
        self.edges.sort(key=lambda item: (item.priority, item.name))
        return edge

    def extend(self, edges: Iterable[ConversionEdge]) -> None:
        for edge in edges:
            self.register(edge)

    def edges_for(
        self,
        *,
        source_format: str | None = None,
        target_format: str | None = None,
        kind: ConversionEdgeKind | str | None = None,
    ) -> list[ConversionEdge]:
        source = normalize_format_name(source_format) if source_format is not None else None
        target = normalize_format_name(target_format) if target_format is not None else None
        edge_kind = ConversionEdgeKind(kind) if kind is not None else None
        return [
            edge
            for edge in self.edges
            if (source is None or edge.source_format == source)
            and (target is None or edge.target_format == target)
            and (edge_kind is None or edge.kind == edge_kind)
        ]

    def preferred_edge(self, source_format: str, target_format: str) -> ConversionEdge | None:
        matches = self.edges_for(source_format=source_format, target_format=target_format)
        return matches[0] if matches else None

    def to_mapping(self) -> dict[str, object]:
        return {"edges": [edge.to_mapping() for edge in self.edges]}


def plugin_name(plugin: object | None) -> str | None:
    return getattr(plugin, "name", None)


def legacy_oeb_edge(
    source_format: str,
    target_format: str,
    *,
    input_plugin: object | None = None,
    output_plugin: object | None = None,
    priority: int = 100,
) -> ConversionEdge:
    source = normalize_format_name(source_format)
    target = normalize_format_name(target_format)
    return ConversionEdge(
        source_format=source,
        target_format=target,
        kind=ConversionEdgeKind.OEB,
        name="legacy-oeb:%s->%s" % (source, target),
        priority=priority,
        intermediate_format="oeb",
        input_plugin_name=plugin_name(input_plugin),
        output_plugin_name=plugin_name(output_plugin),
        lossless=None,
        notes=("Legacy input plugin -> OEB transforms -> output plugin path.",),
    )


def direct_edge(
    source_format: str,
    target_format: str,
    *,
    name: str | None = None,
    priority: int = 50,
    lossless: bool | None = None,
    notes: Iterable[str] = (),
) -> ConversionEdge:
    source = normalize_format_name(source_format)
    target = normalize_format_name(target_format)
    return ConversionEdge(
        source_format=source,
        target_format=target,
        kind=ConversionEdgeKind.DIRECT,
        name=name or "direct:%s->%s" % (source, target),
        priority=priority,
        lossless=lossless,
        notes=tuple(notes),
    )


def external_tool_edge(
    source_format: str,
    target_format: str,
    *,
    external_tool: str,
    external_tool_version: str | None = None,
    name: str | None = None,
    priority: int = 75,
    lossless: bool | None = None,
    notes: Iterable[str] = (),
) -> ConversionEdge:
    source = normalize_format_name(source_format)
    target = normalize_format_name(target_format)
    return ConversionEdge(
        source_format=source,
        target_format=target,
        kind=ConversionEdgeKind.EXTERNAL,
        name=name or "external:%s:%s->%s" % (external_tool, source, target),
        priority=priority,
        external_tool=external_tool,
        external_tool_version=external_tool_version,
        lossless=lossless,
        notes=tuple(notes),
    )


def build_legacy_oeb_edges(
    input_formats: Iterable[str],
    output_formats: Iterable[str],
) -> ConversionEdgeRegistry:
    registry = ConversionEdgeRegistry()
    for source in sorted({normalize_format_name(format_name) for format_name in input_formats}):
        for target in sorted({normalize_format_name(format_name) for format_name in output_formats}):
            registry.register(legacy_oeb_edge(source, target))
    return registry
