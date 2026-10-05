"""
Capture structured conversion events, timings, warnings and artifacts.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise report through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
"""
from __future__ import annotations

import typing as _typing

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


@dataclass(frozen=True, slots=True)
class ConversionLossSample:
    """
    Provide the conversionlosssample contract for validated ebook processing.

    Example:
        Exercise ConversionLossSample through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
    """
    text: str
    codepoints: tuple[str, ...]

    @classmethod
    def from_text(cls: type[_typing.Self], text: str) -> "ConversionLossSample":
        """
        Perform the from text operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionLossSample.from text through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return cls(
            text=text,
            codepoints=tuple("U+%04X" % ord(char) for char in text),
        )

    def to_mapping(self: _typing.Self) -> dict[str, object]:
        """
        Perform the to mapping operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionLossSample.to mapping through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "text": self.text,
            "codepoints": list(self.codepoints),
        }


@dataclass(frozen=True, slots=True)
class ConversionLossEvent:
    """
    Carry normalized conversionlossevent data across the conversion pipeline.

    Example:
        Exercise ConversionLossEvent through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
    """
    phase: str
    code: str
    message: str
    count: int = 1
    recoverable: bool = True
    source_format: str | None = None
    target_format: str | None = None
    edge_name: str | None = None
    samples: tuple[ConversionLossSample, ...] = ()
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_mapping(self: _typing.Self) -> dict[str, object]:
        """
        Perform the to mapping operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionLossEvent.to mapping through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "phase": self.phase,
            "code": self.code,
            "message": self.message,
            "count": self.count,
            "recoverable": self.recoverable,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "edge_name": self.edge_name,
            "samples": [sample.to_mapping() for sample in self.samples],
            "details": dict(self.details),
        }


@dataclass(slots=True)
class ConversionReport:
    """
    Carry normalized conversionreport data across the conversion pipeline.

    Example:
        Exercise ConversionReport through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
    """
    source_format: str | None = None
    target_format: str | None = None
    edge_name: str | None = None
    loss_events: list[ConversionLossEvent] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def apply_context_defaults(
        self: _typing.Self,
        *,
        source_format: str | None = None,
        target_format: str | None = None,
        edge_name: str | None = None,
    ) -> None:
        """
        Perform the apply context defaults operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionReport.apply context defaults through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param source_format: Value supplied for source format under the utility contract.
        :param target_format: Value supplied for target format under the utility contract.
        :param edge_name: Value supplied for edge name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.source_format is None:
            self.source_format = source_format
        if self.target_format is None:
            self.target_format = target_format
        if self.edge_name is None:
            self.edge_name = edge_name

    def add_warning(self: _typing.Self, message: str) -> None:
        """
        Perform the add warning operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionReport.add warning through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.warnings.append(message)

    def add_loss_event(
        self: _typing.Self,
        *,
        phase: str,
        code: str,
        message: str,
        count: int = 1,
        recoverable: bool = True,
        source_format: str | None = None,
        target_format: str | None = None,
        edge_name: str | None = None,
        samples: Iterable[ConversionLossSample] = (),
        details: Mapping[str, Any] | None = None,
    ) -> ConversionLossEvent:
        """
        Perform the add loss event operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionReport.add loss event through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param phase: Value supplied for phase under the utility contract.
        :param code: Value supplied for code under the utility contract.
        :param message: Value supplied for message under the utility contract.
        :param count: Value supplied for count under the utility contract.
        :param recoverable: Value supplied for recoverable under the utility contract.
        :param source_format: Value supplied for source format under the utility contract.
        :param target_format: Value supplied for target format under the utility contract.
        :param edge_name: Value supplied for edge name under the utility contract.
        :param samples: Value supplied for samples under the utility contract.
        :param details: Value supplied for details under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        event = ConversionLossEvent(
            phase=phase,
            code=code,
            message=message,
            count=count,
            recoverable=recoverable,
            source_format=source_format if source_format is not None else self.source_format,
            target_format=target_format if target_format is not None else self.target_format,
            edge_name=edge_name if edge_name is not None else self.edge_name,
            samples=tuple(samples),
            details=dict(details or {}),
        )
        self.loss_events.append(event)
        return event

    def to_mapping(self: _typing.Self) -> dict[str, object]:
        """
        Perform the to mapping operation under explicit file-format and conversion rules.

        Example:
            Exercise ConversionReport.to mapping through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "source_format": self.source_format,
            "target_format": self.target_format,
            "edge_name": self.edge_name,
            "warnings": list(self.warnings),
            "loss_event_count": len(self.loss_events),
            "recoverable_loss_event_count": sum(1 for event in self.loss_events if event.recoverable),
            "loss_events": [event.to_mapping() for event in self.loss_events],
        }


def ensure_conversion_report(
    holder: object | None,
    *,
    source_format: str | None = None,
    target_format: str | None = None,
    edge_name: str | None = None,
) -> ConversionReport:
    """
    Perform the ensure conversion report operation under explicit file-format and conversion rules.

    Example:
        Exercise ensure conversion report through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param holder: Value supplied for holder under the utility contract.
    :param source_format: Value supplied for source format under the utility contract.
    :param target_format: Value supplied for target format under the utility contract.
    :param edge_name: Value supplied for edge name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    report = getattr(holder, "conversion_report", None)
    if isinstance(report, ConversionReport):
        report.apply_context_defaults(
            source_format=source_format,
            target_format=target_format,
            edge_name=edge_name,
        )
        return report

    report = ConversionReport(
        source_format=source_format,
        target_format=target_format,
        edge_name=edge_name,
    )
    if holder is not None:
        try:
            setattr(holder, "conversion_report", report)
        except Exception:
            pass
    return report
