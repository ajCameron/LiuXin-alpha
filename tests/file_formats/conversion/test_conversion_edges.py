from __future__ import annotations

from types import SimpleNamespace


def test_legacy_oeb_edge_records_current_pipeline_shape() -> None:
    from LiuXin_alpha.file_formats.conversion.edges import ConversionEdgeKind, legacy_oeb_edge

    edge = legacy_oeb_edge(
        ".HTML",
        ".PMLZ",
        input_plugin=SimpleNamespace(name="HTML Input"),
        output_plugin=SimpleNamespace(name="PML Output"),
    )

    assert edge.name == "legacy-oeb:html->pmlz"
    assert edge.source_format == "html"
    assert edge.target_format == "pmlz"
    assert edge.kind is ConversionEdgeKind.OEB
    assert edge.intermediate_format == "oeb"
    assert edge.input_plugin_name == "HTML Input"
    assert edge.output_plugin_name == "PML Output"
    assert edge.supports("HTML", "pmlz")

    payload = edge.to_mapping()
    assert payload["kind"] == "oeb"
    assert payload["intermediate_format"] == "oeb"
    assert payload["notes"] == ["Legacy input plugin -> OEB transforms -> output plugin path."]


def test_registry_prefers_lower_priority_edges_deterministically() -> None:
    from LiuXin_alpha.file_formats.conversion.edges import ConversionEdgeRegistry, direct_edge, legacy_oeb_edge

    registry = ConversionEdgeRegistry()
    oeb = registry.register(legacy_oeb_edge("html", "txt"))
    direct = registry.register(direct_edge("html", "txt", lossless=False))

    assert registry.preferred_edge("HTML", ".TXT") is direct
    assert registry.edges_for(source_format="html", target_format="txt") == [direct, oeb]
    assert registry.edges_for(kind="direct") == [direct]


def test_external_tool_edges_record_tool_diagnostics() -> None:
    from LiuXin_alpha.file_formats.conversion.edges import ConversionEdgeKind, external_tool_edge

    edge = external_tool_edge(
        "md",
        "html",
        external_tool="pandoc",
        external_tool_version="3.2.1",
        notes=("optional direct markup adapter",),
    )

    assert edge.kind is ConversionEdgeKind.EXTERNAL
    assert edge.name == "external:pandoc:md->html"
    assert edge.external_tool == "pandoc"
    assert edge.external_tool_version == "3.2.1"
    assert edge.to_mapping()["notes"] == ["optional direct markup adapter"]


def test_build_legacy_oeb_edges_crosses_input_and_output_formats() -> None:
    from LiuXin_alpha.file_formats.conversion.edges import build_legacy_oeb_edges

    registry = build_legacy_oeb_edges(["HTML", ".txt"], ["PMLZ", "EPUB"])

    assert [edge.name for edge in registry.edges] == [
        "legacy-oeb:html->epub",
        "legacy-oeb:html->pmlz",
        "legacy-oeb:txt->epub",
        "legacy-oeb:txt->pmlz",
    ]
    assert registry.preferred_edge("html", "epub").intermediate_format == "oeb"
