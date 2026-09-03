"""Core WEMI manifestation identity implementation containers.

Category: core WEMI identity object.
This module implements the manifestation entity itself, not the editable
metadata bundle and not a read-side query result.
"""
from __future__ import annotations

from typing import Any, Mapping, Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)


class ManifestationIdentity(ManifestationIdentityAPI):
    """
    Represent a Manifestation identity and its edition, carrier, and extent fields.
    """
    def __init__(
        self,
        *,
        manifestation_id: Optional[int] = None,
        manifestation_expression_id: Optional[int] = None,
        manifestation_format_detail: Optional[str] = None,
        manifestation_carrier_type: Optional[str] = None,
        manifestation_edition_statement: Optional[str] = None,
        manifestation_pub_year: Optional[int] = None,
        manifestation_status: Optional[str] = None,
        manifestation_flags: Optional[str] = None,
        manifestation_page_count: Optional[int] = None,
        manifestation_runtime_minutes: Optional[int] = None,
        manifestation_note: Optional[str] = None,
        manifestation_created_timestamp_ep_k: Optional[int] = None,
        manifestation_modified_timestamp_ep_k: Optional[int] = None,
        manifestation_scratch: Optional[str] = None,
    ) -> None:
        self._manifestation_id = manifestation_id
        self._manifestation_expression_id = manifestation_expression_id
        self._manifestation_format_detail = manifestation_format_detail
        self._manifestation_carrier_type = manifestation_carrier_type
        self._manifestation_edition_statement = manifestation_edition_statement
        self._manifestation_pub_year = manifestation_pub_year
        self._manifestation_status = manifestation_status
        self._manifestation_flags = manifestation_flags
        self.manifestation_page_count = manifestation_page_count
        self.manifestation_runtime_minutes = manifestation_runtime_minutes
        self.manifestation_note = manifestation_note
        self.manifestation_created_timestamp_ep_k = manifestation_created_timestamp_ep_k
        self.manifestation_modified_timestamp_ep_k = manifestation_modified_timestamp_ep_k
        self.manifestation_scratch = manifestation_scratch

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "ManifestationIdentity":
        return cls(**{k: row.get(k) for k in [
            'manifestation_id', 'manifestation_expression_id', 'manifestation_format_detail',
            'manifestation_carrier_type', 'manifestation_edition_statement', 'manifestation_pub_year',
            'manifestation_status', 'manifestation_flags', 'manifestation_page_count',
            'manifestation_runtime_minutes', 'manifestation_note', 'manifestation_created_timestamp_ep_k',
            'manifestation_modified_timestamp_ep_k', 'manifestation_scratch',
        ]})

    def to_mapping(self) -> dict[str, Any]:
        return {
            'manifestation_id': self.manifestation_id,
            'manifestation_expression_id': self.manifestation_expression_id,
            'manifestation_format_detail': self.manifestation_format_detail,
            'manifestation_carrier_type': self.manifestation_carrier_type,
            'manifestation_edition_statement': self.manifestation_edition_statement,
            'manifestation_pub_year': self.manifestation_pub_year,
            'manifestation_status': self.manifestation_status,
            'manifestation_flags': self.manifestation_flags,
            'manifestation_page_count': self.manifestation_page_count,
            'manifestation_runtime_minutes': self.manifestation_runtime_minutes,
            'manifestation_note': self.manifestation_note,
            'manifestation_created_timestamp_ep_k': self.manifestation_created_timestamp_ep_k,
            'manifestation_modified_timestamp_ep_k': self.manifestation_modified_timestamp_ep_k,
            'manifestation_scratch': self.manifestation_scratch,
        }

    def __str__(self) -> str:
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=("manifestation_id", "manifestation_expression_id"),
            display_keys=(
                "manifestation_format_detail",
                "manifestation_edition_statement",
                "manifestation_pub_year",
            ),
        )

    @property
    def manifestation_id(self) -> Optional[int]: return self._manifestation_id
    @manifestation_id.setter
    def manifestation_id(self, value: Optional[int]) -> None:
        if self._manifestation_id is None: self._manifestation_id = value
        else: raise AttributeError('Manifestation id is already set.')
    @property
    def manifestation_expression_id(self) -> Optional[int]: return self._manifestation_expression_id
    @manifestation_expression_id.setter
    def manifestation_expression_id(self, value: Optional[int]) -> None: self._manifestation_expression_id = value
    @property
    def manifestation_format_detail(self) -> Optional[str]:
        """Specific format or product label, such as EPUB or A-format paperback."""
        return self._manifestation_format_detail
    @manifestation_format_detail.setter
    def manifestation_format_detail(self, value: Optional[str]) -> None:
        self._manifestation_format_detail = value
    @property
    def manifestation_carrier_type(self) -> Optional[str]: return self._manifestation_carrier_type
    @manifestation_carrier_type.setter
    def manifestation_carrier_type(self, value: Optional[str]) -> None: self._manifestation_carrier_type = value
    @property
    def manifestation_edition_statement(self) -> Optional[str]: return self._manifestation_edition_statement
    @manifestation_edition_statement.setter
    def manifestation_edition_statement(self, value: Optional[str]) -> None: self._manifestation_edition_statement = value
    @property
    def manifestation_pub_year(self) -> Optional[int]: return self._manifestation_pub_year
    @manifestation_pub_year.setter
    def manifestation_pub_year(self, value: Optional[int]) -> None: self._manifestation_pub_year = value
    @property
    def manifestation_status(self) -> Optional[str]: return self._manifestation_status
    @manifestation_status.setter
    def manifestation_status(self, value: Optional[str]) -> None: self._manifestation_status = value
    @property
    def manifestation_flags(self) -> Optional[str]: return self._manifestation_flags
    @manifestation_flags.setter
    def manifestation_flags(self, value: Optional[str]) -> None: self._manifestation_flags = value


__all__ = ["ManifestationIdentity"]
