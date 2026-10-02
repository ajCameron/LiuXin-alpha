"""
Implement intrinsic manifestation identity fields separately from the editable relation bundle.

The identity retains edition, carrier, publication and extent values and maps them
to prefixed row columns. It performs no lookup or persistence.

Example:
    >>> identity = ManifestationIdentity(manifestation_id=3)
    >>> identity.manifestation_id
    3
"""
from __future__ import annotations

from typing import Any, Mapping, Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)


class ManifestationIdentity(ManifestationIdentityAPI):
    """
    Store a manifestation identity and its edition, carrier, publication and extent metadata.

    This mirrors the manifestations row; editable relationships belong to
    ManifestationMetadata. Values, including flags, are retained as supplied. The public
    id setter accepts assignments only while the stored id is None.

    Example:
        >>> identity = ManifestationIdentity(manifestation_id=3)
        >>> identity.manifestation_id
        3
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
        """
        Store the supplied manifestation fields without coercion or validation.

        Flags remain in their supplied representation. Dates, extent values and foreign-key
        references are not parsed or resolved here.

        Example:
            >>> identity = ManifestationIdentity(manifestation_id=3)
            >>> identity.to_mapping()['manifestation_id']
            3


        :param manifestation_id: Optional manifestation row id; a non-None value locks the
            public id setter.
        :param manifestation_expression_id: Optional parent expression id; retained as
            supplied.
        :param manifestation_format_detail: Optional specific format or product label;
            retained as supplied.
        :param manifestation_carrier_type: Optional carrier type; retained as supplied.
        :param manifestation_edition_statement: Optional edition statement; retained as
            supplied.
        :param manifestation_pub_year: Optional publication year; retained as supplied.
        :param manifestation_status: Optional status text; retained as supplied.
        :param manifestation_flags: Optional flags text; retained as supplied.
        :param manifestation_page_count: Optional page count; retained as supplied.
        :param manifestation_runtime_minutes: Optional runtime in minutes; retained as
            supplied.
        :param manifestation_note: Optional note text; retained as supplied.
        :param manifestation_created_timestamp_ep_k: Optional creation timestamp in ep_k
            form; retained as supplied.
        :param manifestation_modified_timestamp_ep_k: Optional modification timestamp in
            ep_k form; retained as supplied.
        :param manifestation_scratch: Optional scratch text; retained as supplied.
        :return: None.
        """
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
        """
        Build a manifestation identity from recognized prefixed row columns.

        Missing columns become None and unknown keys are ignored. All recognized values are
        retained without coercion or normalization.

        Example:
            >>> identity = ManifestationIdentity.from_mapping({'manifestation_id': 3, 'manifestation_flags': 'draft, draft'})
            >>> identity.manifestation_flags
            'draft, draft'


        :param row: Mapping supplying optional manifestation-prefixed identity columns.
        :return: New ManifestationIdentity instance of the requested class.
        """
        return cls(**{k: row.get(k) for k in [
            'manifestation_id', 'manifestation_expression_id', 'manifestation_format_detail',
            'manifestation_carrier_type', 'manifestation_edition_statement', 'manifestation_pub_year',
            'manifestation_status', 'manifestation_flags', 'manifestation_page_count',
            'manifestation_runtime_minutes', 'manifestation_note', 'manifestation_created_timestamp_ep_k',
            'manifestation_modified_timestamp_ep_k', 'manifestation_scratch',
        ]})

    def to_mapping(self) -> dict[str, Any]:
        """
        Serialize every supported manifestation identity field to its prefixed column name.

        Stored values, including None and unnormalized flags, are retained.

        Example:
            >>> identity = ManifestationIdentity(manifestation_id=3)
            >>> identity.to_mapping()['manifestation_id']
            3


        :return: New dictionary of manifestation row fields.
        """
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
        """
        Render a compact diagnostic summary emphasizing manifestation ids and display fields.

        Example:
            >>> identity = ManifestationIdentity(manifestation_format_detail='EPUB')
            >>> 'EPUB' in str(identity)
            True


        :return: Human-readable identity summary.
        """
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
    def manifestation_id(self) -> Optional[int]:
        """
        Return the stored manifestation row id.

        Example:
            >>> identity = ManifestationIdentity(manifestation_id=3)
            >>> identity.manifestation_id
            3


        :return: Manifestation row id, or None when unset.
        """
        return self._manifestation_id
    @manifestation_id.setter
    def manifestation_id(self, value: Optional[int]) -> None:
        """
        Assign a manifestation id only while the stored id is None.

        A stored non-None id causes AttributeError even for an identical value. Assigning
        None while unset leaves the setter available.

        Example:
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_id = 3
            >>> identity.manifestation_id = 3
            Traceback (most recent call last):
            ...
            AttributeError: Manifestation id is already set.


        :param value: New manifestation row id, or None to leave or mark it unset.
        :return: None.
        """
        if self._manifestation_id is None: self._manifestation_id = value
        else: raise AttributeError('Manifestation id is already set.')
    @property
    def manifestation_expression_id(self) -> Optional[int]:
        """
        Return the stored parent expression id.

        Example:
            >>> identity = ManifestationIdentity(manifestation_expression_id=2)
            >>> identity.manifestation_expression_id
            2


        :return: Parent expression id, or None when unset.
        """
        return self._manifestation_expression_id
    @manifestation_expression_id.setter
    def manifestation_expression_id(self, value: Optional[int]) -> None:
        """
        Replace the parent expression id without coercion, validation or lookup.

        Example:
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_expression_id = 2
            >>> identity.manifestation_expression_id
            2


        :param value: New parent expression id, or None to leave or mark it unset.
        :return: None.
        """
        self._manifestation_expression_id = value
    @property
    def manifestation_format_detail(self) -> Optional[str]:
        """
        Return the specific format or product label, such as EPUB or A-format paperback.

        Example:
            >>> identity = ManifestationIdentity(manifestation_format_detail='EPUB')
            >>> identity.manifestation_format_detail
            'EPUB'


        :return: Specific format or product label, or None when unset.
        """
        return self._manifestation_format_detail
    @manifestation_format_detail.setter
    def manifestation_format_detail(self, value: Optional[str]) -> None:
        """
        Replace the specific format or product label without coercion, validation or lookup.

        Example:
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_format_detail = 'EPUB'
            >>> identity.manifestation_format_detail
            'EPUB'


        :param value: New specific format or product label, or None to leave or mark it
            unset.
        :return: None.
        """
        self._manifestation_format_detail = value
    @property
    def manifestation_carrier_type(self) -> Optional[str]:
        """
        Return the stored carrier type.

        Example:
            >>> identity = ManifestationIdentity(manifestation_carrier_type='ebook')
            >>> identity.manifestation_carrier_type
            'ebook'


        :return: Carrier type, or None when unset.
        """
        return self._manifestation_carrier_type
    @manifestation_carrier_type.setter
    def manifestation_carrier_type(self, value: Optional[str]) -> None:
        """
        Replace the carrier type without coercion, validation or lookup.

        Example:
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_carrier_type = 'ebook'
            >>> identity.manifestation_carrier_type
            'ebook'


        :param value: New carrier type, or None to leave or mark it unset.
        :return: None.
        """
        self._manifestation_carrier_type = value
    @property
    def manifestation_edition_statement(self) -> Optional[str]:
        """
        Return the stored edition statement.

        Example:
            >>> identity = ManifestationIdentity(manifestation_edition_statement='First edition')
            >>> identity.manifestation_edition_statement
            'First edition'


        :return: Edition statement, or None when unset.
        """
        return self._manifestation_edition_statement
    @manifestation_edition_statement.setter
    def manifestation_edition_statement(self, value: Optional[str]) -> None:
        """
        Replace the edition statement without coercion, validation or lookup.

        Example:
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_edition_statement = 'First edition'
            >>> identity.manifestation_edition_statement
            'First edition'


        :param value: New edition statement, or None to leave or mark it unset.
        :return: None.
        """
        self._manifestation_edition_statement = value
    @property
    def manifestation_pub_year(self) -> Optional[int]:
        """
        Return the stored publication year.

        Example:
            >>> identity = ManifestationIdentity(manifestation_pub_year=2020)
            >>> identity.manifestation_pub_year
            2020


        :return: Publication year, or None when unset.
        """
        return self._manifestation_pub_year
    @manifestation_pub_year.setter
    def manifestation_pub_year(self, value: Optional[int]) -> None:
        """
        Replace the publication year without coercion, validation or lookup.

        Example:
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_pub_year = 2020
            >>> identity.manifestation_pub_year
            2020


        :param value: New publication year, or None to leave or mark it unset.
        :return: None.
        """
        self._manifestation_pub_year = value
    @property
    def manifestation_status(self) -> Optional[str]:
        """
        Return the stored status text.

        Example:
            >>> identity = ManifestationIdentity(manifestation_status='active')
            >>> identity.manifestation_status
            'active'


        :return: Status text, or None when unset.
        """
        return self._manifestation_status
    @manifestation_status.setter
    def manifestation_status(self, value: Optional[str]) -> None:
        """
        Replace the status text without coercion, validation or lookup.

        Example:
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_status = 'active'
            >>> identity.manifestation_status
            'active'


        :param value: New status text, or None to leave or mark it unset.
        :return: None.
        """
        self._manifestation_status = value
    @property
    def manifestation_flags(self) -> Optional[str]:
        """
        Return the stored flags text.

        Example:
            >>> identity = ManifestationIdentity(manifestation_flags='draft, draft')
            >>> identity.manifestation_flags
            'draft, draft'


        :return: Flags text, or None when unset.
        """
        return self._manifestation_flags
    @manifestation_flags.setter
    def manifestation_flags(self, value: Optional[str]) -> None:
        """
        Replace the flags text without coercion, validation or lookup.

        Example:
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_flags = 'draft, draft'
            >>> identity.manifestation_flags
            'draft, draft'


        :param value: New flags text, or None to leave or mark it unset.
        :return: None.
        """
        self._manifestation_flags = value


__all__ = ["ManifestationIdentity"]
