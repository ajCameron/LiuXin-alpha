from __future__ import annotations

import typing as _typing

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


@dataclass(frozen=True, slots=True)
class ConversionLossSample:
    text: str
    codepoints: tuple[str, ...]

    @classmethod
    def from_text(cls: type[_typing.Self], text: str) -> "ConversionLossSample":
        return cls(
            text=text,
            codepoints=tuple("U+%04X" % ord(char) for char in text),
        )

    def to_mapping(self: _typing.Self) -> dict[str, object]:
        return {
            "text": self.text,
            "codepoints": list(self.codepoints),
        }


@dataclass(frozen=True, slots=True)
class ConversionLossEvent:
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
        if self.source_format is None:
            self.source_format = source_format
        if self.target_format is None:
            self.target_format = target_format
        if self.edge_name is None:
            self.edge_name = edge_name

    def add_warning(self: _typing.Self, message: str) -> None:
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
