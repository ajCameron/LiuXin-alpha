
"""
Define the intrinsic manifestation identity contract and its expression-id compatibility alias.

Editable graph relations and read-side projections live in separate APIs.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
    >>> identity = ManifestationIdentity(manifestation_id=3, manifestation_format_detail='EPUB')
    >>> identity.WEMI_LEVEL
    'manifestation'
"""

from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MutableMetadataRecord,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.identity_api import (
    WemiIdentityAPI,
)

class ManifestationIdentityPropertiesAPI(WemiIdentityAPI, metaclass=abc.ABCMeta):
    """
    Require row-level ids, format, carrier, edition, publication, status and flag fields for one manifestation.

    Concrete identities decide assignment guards, normalization and mapping ownership.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
        >>> identity = ManifestationIdentity(manifestation_id=3, manifestation_format_detail='EPUB')
        >>> identity.id
        3
    """
    WEMI_LEVEL: ClassVar[str] = "manifestation"
    SOURCE_TABLE: ClassVar[str] = "manifestations"
    ID_FIELD: ClassVar[str] = "manifestation_id"

    @property
    def id(self) -> Optional[int]:
        """
        Return manifestation_id through the compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_id=3)
            >>> identity.id
            3


        :return: Current manifestation id, or None.
        """
        return self.manifestation_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        """
        Assign manifestation_id through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.id = 3
            >>> identity.manifestation_id
            3


        :param value: New manifestation id, or None.
        :return: None.
        """
        self.manifestation_id = value

    @property
    @abc.abstractmethod
    def manifestation_id(self) -> Optional[int]:
        """
        Return manifestation_id through the canonical manifestation property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_id=3)
            >>> identity.manifestation_id
            3


        :return: Current manifestation id, or None.
        """

    @manifestation_id.setter
    @abc.abstractmethod
    def manifestation_id(self, manifestation_id: Optional[int]) -> None:
        """
        Assign manifestation_id through the canonical manifestation property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_id = 3
            >>> identity.manifestation_id
            3


        :param manifestation_id: New manifestation id, or None.
        :return: None.
        """

    @property
    def expression_id(self) -> Optional[int]:
        """
        Return manifestation_expression_id through the compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_expression_id=2)
            >>> identity.expression_id
            2


        :return: Current manifestation expression id, or None.
        """
        return self.manifestation_expression_id

    @expression_id.setter
    def expression_id(self, value: Optional[int]) -> None:
        """
        Assign manifestation_expression_id through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.expression_id = 2
            >>> identity.manifestation_expression_id
            2


        :param value: New manifestation expression id, or None.
        :return: None.
        """
        self.manifestation_expression_id = value

    @property
    @abc.abstractmethod
    def manifestation_expression_id(self) -> Optional[int]:
        """
        Return manifestation_expression_id through the canonical manifestation property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_expression_id=2)
            >>> identity.manifestation_expression_id
            2


        :return: Current manifestation expression id, or None.
        """

    @manifestation_expression_id.setter
    @abc.abstractmethod
    def manifestation_expression_id(self, manifestation_expression_id: Optional[int]) -> None:
        """
        Assign manifestation_expression_id through the canonical manifestation property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_expression_id = 2
            >>> identity.manifestation_expression_id
            2


        :param manifestation_expression_id: New manifestation expression id, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def manifestation_format_detail(self) -> Optional[str]:
        """
        Return the Specific format or product label.

        It is finer-grained than ``manifestation_carrier_type``.

        Examples include EPUB, PDF and A-format paperback. Carrier type describes broad
        families.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_format_detail='EPUB')
            >>> identity.manifestation_format_detail
            'EPUB'


        :return: Current format detail, or None.
        """

    @manifestation_format_detail.setter
    @abc.abstractmethod
    def manifestation_format_detail(self, manifestation_format_detail: Optional[str]) -> None:
        """
        Assign manifestation_format_detail through the canonical manifestation property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_format_detail = 'EPUB'
            >>> identity.manifestation_format_detail
            'EPUB'


        :param manifestation_format_detail: New manifestation format detail, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def manifestation_carrier_type(self) -> Optional[str]:
        """
        Return manifestation_carrier_type through the canonical manifestation property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_carrier_type='ebook')
            >>> identity.manifestation_carrier_type
            'ebook'


        :return: Current manifestation carrier type, or None.
        """

    @manifestation_carrier_type.setter
    @abc.abstractmethod
    def manifestation_carrier_type(self, manifestation_carrier_type: Optional[str]) -> None:
        """
        Assign manifestation_carrier_type through the canonical manifestation property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_carrier_type = 'ebook'
            >>> identity.manifestation_carrier_type
            'ebook'


        :param manifestation_carrier_type: New manifestation carrier type, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def manifestation_edition_statement(self) -> Optional[str]:
        """
        Return manifestation_edition_statement through the canonical manifestation property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_edition_statement='Second edition')
            >>> identity.manifestation_edition_statement
            'Second edition'


        :return: Current manifestation edition statement, or None.
        """

    @manifestation_edition_statement.setter
    @abc.abstractmethod
    def manifestation_edition_statement(self, manifestation_edition_statement: Optional[str]) -> None:
        """
        Assign manifestation_edition_statement through the canonical manifestation property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_edition_statement = 'Second edition'
            >>> identity.manifestation_edition_statement
            'Second edition'


        :param manifestation_edition_statement: New manifestation edition statement, or
            None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def manifestation_pub_year(self) -> Optional[int]:
        """
        Return manifestation_pub_year through the canonical manifestation property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_pub_year=2026)
            >>> identity.manifestation_pub_year
            2026


        :return: Current manifestation pub year, or None.
        """

    @manifestation_pub_year.setter
    @abc.abstractmethod
    def manifestation_pub_year(self, manifestation_pub_year: Optional[int]) -> None:
        """
        Assign manifestation_pub_year through the canonical manifestation property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_pub_year = 2026
            >>> identity.manifestation_pub_year
            2026


        :param manifestation_pub_year: New manifestation pub year, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def manifestation_status(self) -> Optional[str]:
        """
        Return manifestation_status through the canonical manifestation property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_status='published')
            >>> identity.manifestation_status
            'published'


        :return: Current manifestation status, or None.
        """

    @manifestation_status.setter
    @abc.abstractmethod
    def manifestation_status(self, manifestation_status: Optional[str]) -> None:
        """
        Assign manifestation_status through the canonical manifestation property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_status = 'published'
            >>> identity.manifestation_status
            'published'


        :param manifestation_status: New manifestation status, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def manifestation_flags(self) -> Optional[str]:
        """
        Return manifestation_flags through the canonical manifestation property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_flags='illustrated')
            >>> identity.manifestation_flags
            'illustrated'


        :return: Current manifestation flags, or None.
        """

    @manifestation_flags.setter
    @abc.abstractmethod
    def manifestation_flags(self, manifestation_flags: Optional[str]) -> None:
        """
        Assign manifestation_flags through the canonical manifestation property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity()
            >>> identity.manifestation_flags = 'illustrated'
            >>> identity.manifestation_flags
            'illustrated'


        :param manifestation_flags: New manifestation flags, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """
        Require serialization to canonical manifestation-column keys.

        Field inclusion, value normalization and copy depth belong to the concrete identity.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_id=3, manifestation_format_detail='EPUB')
            >>> identity.to_mapping()['manifestation_id']
            3


        :return: Mutable metadata record describing the manifestation.
        """

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when the concrete identity does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> identity = ManifestationIdentity(manifestation_id=3, manifestation_format_detail='EPUB')
            >>> ManifestationIdentityPropertiesAPI.__str__(identity)
            'ManifestationIdentity()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"


class ManifestationIdentityAPI(ManifestationIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """
    Mark a concrete manifestation identity that provides every row-level property.

    The marker adds no behavior and provides a stable public annotation boundary.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
        >>> identity = ManifestationIdentity(manifestation_id=3, manifestation_format_detail='EPUB')
        >>> isinstance(identity, ManifestationIdentityAPI)
        True
    """

__all__ = ["ManifestationIdentityPropertiesAPI", "ManifestationIdentityAPI"]
