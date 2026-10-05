"""
Model legal input/output format edges in the conversion graph.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise edges through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
"""
from __future__ import annotations

import typing as _typing

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Iterable


class ConversionEdgeKind(StrEnum):
    """
    Provide the conversionedgekind contract for validated ebook processing.

    Example:
        Exercise ConversionEdgeKind through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
    """
    OEB = "oeb"
    DIRECT = "direct"
    EXTERNAL = "external"


def normalize_format_name(format_name: str) -> str:
    """
    Normalize format name under the format's safety and compatibility rules.

    Example:
        Exercise normalize format name through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param format_name: Value supplied for format name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return str(format_name).strip().lower().removeprefix(".")


@dataclass(frozen=True, slots=True)
class ConversionEdge:
    """
    Carry normalized conversionedge data across the conversion pipeline.

    Example:
        Exercise ConversionEdge through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
    """
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

    def supports(self: _typing.Self, source_format: str, target_format: str) -> bool:
        """
        Perform the supports operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionEdge.supports through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param source_format: Value supplied for source format under the utility contract.
        :param target_format: Value supplied for target format under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        source = normalize_format_name(source_format)
        target = normalize_format_name(target_format)
        return self.source_format == source and self.target_format == target

    def to_mapping(self: _typing.Self) -> dict[str, object]:
        """
        Perform the to mapping operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionEdge.to mapping through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Provide the conversionedgeregistry contract for validated ebook processing.

    Example:
        Exercise ConversionEdgeRegistry through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
    """
    edges: list[ConversionEdge] = field(default_factory=list)

    def register(self: _typing.Self, edge: ConversionEdge) -> ConversionEdge:
        """
        Perform the register operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionEdgeRegistry.register through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param edge: Value supplied for edge under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.edges.append(edge)
        self.edges.sort(key=lambda item: (item.priority, item.name))
        return edge

    def extend(self: _typing.Self, edges: Iterable[ConversionEdge]) -> None:
        """
        Perform the extend operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionEdgeRegistry.extend through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param edges: Value supplied for edges under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for edge in edges:
            self.register(edge)

    def edges_for(
        self: _typing.Self,
        *,
        source_format: str | None = None,
        target_format: str | None = None,
        kind: ConversionEdgeKind | str | None = None,
    ) -> list[ConversionEdge]:
        """
        Perform the edges for operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionEdgeRegistry.edges for through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param source_format: Value supplied for source format under the utility contract.
        :param target_format: Value supplied for target format under the utility contract.
        :param kind: Value supplied for kind under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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

    def preferred_edge(self: _typing.Self, source_format: str, target_format: str) -> ConversionEdge | None:
        """
        Perform the preferred edge operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionEdgeRegistry.preferred edge through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param source_format: Value supplied for source format under the utility contract.
        :param target_format: Value supplied for target format under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        matches = self.edges_for(source_format=source_format, target_format=target_format)
        return matches[0] if matches else None

    def to_mapping(self: _typing.Self) -> dict[str, object]:
        """
        Perform the to mapping operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionEdgeRegistry.to mapping through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {"edges": [edge.to_mapping() for edge in self.edges]}


def plugin_name(plugin: object | None) -> str | None:
    """
    Perform the plugin name operation under explicit file-format and conversion rules.

    Example:
        Exercise plugin name through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param plugin: Value supplied for plugin under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return getattr(plugin, "name", None)


def legacy_oeb_edge(
    source_format: str,
    target_format: str,
    *,
    input_plugin: object | None = None,
    output_plugin: object | None = None,
    priority: int = 100,
) -> ConversionEdge:
    """
    Perform the legacy oeb edge operation under explicit file-format and conversion rules.

    Example:
        Exercise legacy oeb edge through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param source_format: Value supplied for source format under the utility contract.
    :param target_format: Value supplied for target format under the utility contract.
    :param input_plugin: Value supplied for input plugin under the utility contract.
    :param output_plugin: Value supplied for output plugin under the utility contract.
    :param priority: Value supplied for priority under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the direct edge operation under explicit file-format and conversion rules.

    Example:
        Exercise direct edge through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param source_format: Value supplied for source format under the utility contract.
    :param target_format: Value supplied for target format under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :param priority: Value supplied for priority under the utility contract.
    :param lossless: Value supplied for lossless under the utility contract.
    :param notes: Value supplied for notes under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the external tool edge operation under explicit file-format and conversion rules.

    Example:
        Exercise external tool edge through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param source_format: Value supplied for source format under the utility contract.
    :param target_format: Value supplied for target format under the utility contract.
    :param external_tool: Value supplied for external tool under the utility contract.
    :param external_tool_version: Value supplied for external tool version under the
        utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :param priority: Value supplied for priority under the utility contract.
    :param lossless: Value supplied for lossless under the utility contract.
    :param notes: Value supplied for notes under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the build legacy oeb edges operation under explicit file-format and conversion rules.

    Example:
        Exercise build legacy oeb edges through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param input_formats: Value supplied for input formats under the utility contract.
    :param output_formats: Value supplied for output formats under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    registry = ConversionEdgeRegistry()
    for source in sorted({normalize_format_name(format_name) for format_name in input_formats}):
        for target in sorted({normalize_format_name(format_name) for format_name in output_formats}):
            registry.register(legacy_oeb_edge(source, target))
    return registry
