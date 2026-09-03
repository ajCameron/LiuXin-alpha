"""Core WEMI expression identity implementation containers.

Category: core WEMI identity object.
This module implements the expression entity itself, not the editable metadata
bundle and not a read-side query result.
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Mapping, Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import (
    ExpressionFlags,
    ExpressionIdentityAPI,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)


def _coerce_expression_flags(value: Iterable[str] | str | None) -> ExpressionFlags:
    if value is None:
        return ()
    if isinstance(value, str):
        tokens = value.split(",")
    else:
        tokens = value

    flags: list[str] = []
    seen: set[str] = set()
    for raw_token in tokens:
        token = str(raw_token).strip()
        if token and token not in seen:
            flags.append(token)
            seen.add(token)
    return tuple(flags)


def _serialize_expression_flags(value: ExpressionFlags) -> str | None:
    if not value:
        return None
    return ",".join(value)


class ExpressionIdentity(ExpressionIdentityAPI):
    """
    Represent an Expression identity and its intrinsic language, form, and extent fields.
    """
    def __init__(
        self,
        *,
        expression_id: Optional[int] = None,
        expression_work_id: Optional[int] = None,
        expression_type: Optional[str] = None,
        expression_language_id: Optional[int] = None,
        expression_label: Optional[str] = None,
        expression_title_override: Optional[str] = None,
        expression_subtitle: Optional[str] = None,
        expression_flags: Iterable[str] | str | None = None,
        expression_status: Optional[str] = None,
        expression_original_date: Optional[str] = None,
        expression_original_copyright_date: Optional[str] = None,
        expression_year: Optional[int] = None,
        expression_wordcount: Optional[int] = None,
        expression_nominal_duration_seconds: Optional[int] = None,
        expression_created_timestamp_ep_k: Optional[int] = None,
        expression_modified_timestamp_ep_k: Optional[int] = None,
        expression_scratch: Optional[str] = None,
    ) -> None:
        self._expression_id = expression_id
        self._expression_work_id = expression_work_id
        self._expression_type = expression_type
        self._expression_language_id = expression_language_id
        self._expression_label = expression_label
        self._expression_title_override = expression_title_override
        self._expression_subtitle = expression_subtitle
        self._expression_flags = _coerce_expression_flags(expression_flags)
        self._expression_status = expression_status
        self.expression_original_date = expression_original_date
        self.expression_original_copyright_date = expression_original_copyright_date
        self.expression_year = expression_year
        self.expression_wordcount = expression_wordcount
        self.expression_nominal_duration_seconds = expression_nominal_duration_seconds
        self.expression_created_timestamp_ep_k = expression_created_timestamp_ep_k
        self.expression_modified_timestamp_ep_k = expression_modified_timestamp_ep_k
        self.expression_scratch = expression_scratch

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "ExpressionIdentity":
        return cls(**{k: row.get(k) for k in [
            "expression_id", "expression_work_id", "expression_type", "expression_language_id",
            "expression_label", "expression_title_override", "expression_subtitle", "expression_flags",
            "expression_status", "expression_original_date", "expression_original_copyright_date",
            "expression_year", "expression_wordcount", "expression_nominal_duration_seconds",
            "expression_created_timestamp_ep_k", "expression_modified_timestamp_ep_k", "expression_scratch",
        ]})

    def to_mapping(self) -> dict[str, Any]:
        return {
            "expression_id": self.expression_id,
            "expression_work_id": self.expression_work_id,
            "expression_type": self.expression_type,
            "expression_language_id": self.expression_language_id,
            "expression_label": self.expression_label,
            "expression_title_override": self.expression_title_override,
            "expression_subtitle": self.expression_subtitle,
            "expression_flags": _serialize_expression_flags(self.expression_flags),
            "expression_status": self.expression_status,
            "expression_original_date": self.expression_original_date,
            "expression_original_copyright_date": self.expression_original_copyright_date,
            "expression_year": self.expression_year,
            "expression_wordcount": self.expression_wordcount,
            "expression_nominal_duration_seconds": self.expression_nominal_duration_seconds,
            "expression_created_timestamp_ep_k": self.expression_created_timestamp_ep_k,
            "expression_modified_timestamp_ep_k": self.expression_modified_timestamp_ep_k,
            "expression_scratch": self.expression_scratch,
        }

    def __str__(self) -> str:
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=("expression_id", "expression_work_id"),
            display_keys=(
                "expression_title_override",
                "expression_label",
                "expression_type",
            ),
        )

    @property
    def expression_id(self) -> Optional[int]: return self._expression_id
    @expression_id.setter
    def expression_id(self, value: Optional[int]) -> None:
        if self._expression_id is None: self._expression_id = value
        else: raise AttributeError("Expression id is already set.")
    @property
    def expression_work_id(self) -> Optional[int]: return self._expression_work_id
    @expression_work_id.setter
    def expression_work_id(self, value: Optional[int]) -> None: self._expression_work_id = value
    @property
    def expression_type(self) -> Optional[str]: return self._expression_type
    @expression_type.setter
    def expression_type(self, value: Optional[str]) -> None: self._expression_type = value
    @property
    def expression_language_id(self) -> Optional[int]: return self._expression_language_id
    @expression_language_id.setter
    def expression_language_id(self, value: Optional[int]) -> None: self._expression_language_id = value
    @property
    def expression_label(self) -> Optional[str]: return self._expression_label
    @expression_label.setter
    def expression_label(self, value: Optional[str]) -> None: self._expression_label = value
    @property
    def expression_title_override(self) -> Optional[str]: return self._expression_title_override
    @expression_title_override.setter
    def expression_title_override(self, value: Optional[str]) -> None: self._expression_title_override = value
    @property
    def expression_subtitle(self) -> Optional[str]: return self._expression_subtitle
    @expression_subtitle.setter
    def expression_subtitle(self, value: Optional[str]) -> None: self._expression_subtitle = value
    @property
    def expression_flags(self) -> ExpressionFlags: return self._expression_flags
    @expression_flags.setter
    def expression_flags(self, value: Iterable[str] | str | None) -> None:
        self._expression_flags = _coerce_expression_flags(value)
    @property
    def expression_status(self) -> Optional[str]: return self._expression_status
    @expression_status.setter
    def expression_status(self, value: Optional[str]) -> None: self._expression_status = value


__all__ = ["ExpressionIdentity"]
