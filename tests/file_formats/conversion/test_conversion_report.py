from __future__ import annotations

from types import SimpleNamespace


def test_conversion_report_records_loss_events_as_mappings() -> None:
    from LiuXin_alpha.file_formats.conversion.report import ConversionLossSample, ConversionReport

    report = ConversionReport(source_format="oeb", target_format="pmlz", edge_name="oeb-to-pmlz")
    event = report.add_loss_event(
        phase="pml-output",
        code="unsupported-character-replacement",
        message="PML output replaced unsupported Unicode characters with '?'.",
        count=3,
        samples=[ConversionLossSample.from_text("世")],
        details={"replacement": "?", "unique_characters": 1},
    )

    assert event.recoverable is True
    assert event.source_format == "oeb"
    assert event.target_format == "pmlz"
    assert event.edge_name == "oeb-to-pmlz"

    payload = report.to_mapping()
    assert payload["loss_event_count"] == 1
    assert payload["recoverable_loss_event_count"] == 1
    assert payload["loss_events"][0]["samples"][0] == {
        "text": "世",
        "codepoints": ["U+4E16"],
    }
    assert payload["loss_events"][0]["details"]["replacement"] == "?"


def test_ensure_conversion_report_attaches_and_preserves_existing_report() -> None:
    from LiuXin_alpha.file_formats.conversion.report import ConversionReport, ensure_conversion_report

    holder = SimpleNamespace()
    report = ensure_conversion_report(
        holder,
        source_format="oeb",
        target_format="pmlz",
        edge_name="oeb-to-pmlz",
    )

    assert holder.conversion_report is report
    assert report.source_format == "oeb"
    assert report.target_format == "pmlz"
    assert report.edge_name == "oeb-to-pmlz"

    existing = ensure_conversion_report(
        holder,
        source_format="html",
        target_format="txt",
        edge_name="html-to-txt",
    )

    assert existing is report
    assert report.source_format == "oeb"
    assert report.target_format == "pmlz"
    assert report.edge_name == "oeb-to-pmlz"


def test_ensure_conversion_report_fills_missing_context_on_existing_report() -> None:
    from LiuXin_alpha.file_formats.conversion.report import ConversionReport, ensure_conversion_report

    holder = SimpleNamespace(conversion_report=ConversionReport())

    report = ensure_conversion_report(
        holder,
        source_format="oeb",
        target_format="pmlz",
        edge_name="oeb-to-pmlz",
    )

    assert report is holder.conversion_report
    assert report.source_format == "oeb"
    assert report.target_format == "pmlz"
    assert report.edge_name == "oeb-to-pmlz"
