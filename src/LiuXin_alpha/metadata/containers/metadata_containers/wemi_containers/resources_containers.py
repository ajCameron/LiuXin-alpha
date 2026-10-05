"""
Represent external-resource attachments and editable kind buckets for WEMI targets.

URI, label, media and access metadata are stored without fetching or opening
resources. Construction and serialization do not validate; containers retain shared
records.

Example:
    >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
    >>> record.validate()
    >>> record.target_id
    1
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import ResourceKind
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID

ResourceT = TypeVar("ResourceT", bound="ResourceBase")
KindContainerT = TypeVar("KindContainerT", bound="KindResourcesContainer")


@dataclass(slots=True, kw_only=True)
class ResourceBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a resource attachment with a URI, optional label/media/access hints and public/primary flags.

    Concrete keyword-only dataclasses add the target id and level-specific context.
    Values are retained as supplied. Validation and referenced-row resolution are
    separate operations.

    Example:
        >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
        >>> record.target_kind
        'work'
    """

    resource_kind: ResourceKind
    uri: str
    label: str | None = None
    mime_type: str | None = None
    access_note: str | None = None

    position: int | None = None
    is_primary: bool = False
    is_public: bool = True

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("label", "uri", "resource_kind")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this resource attachment.

        Example:
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this resource attachment.

        Example:
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def display_text(self) -> str:
        """
        Prefer a truthy label over the stored URI without trimming or validation.

        An empty label falls back to the URI; a whitespace-only label remains the display
        value. is_public does not affect display selection.

        Example:
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.display_text
            'urn:example:catalogue:1'
            >>> record.label = ' '
            >>> record.display_text
            ' '


        :return: Stored label or URI string.
        """
        return self.label or self.uri

    def validate(self) -> None:
        """
        Reject a whitespace-only URI or negative record position.

        The URI is checked with strip but retained unchanged. URI syntax, reachability,
        kind, MIME type and public/access hints are not checked. Failures raise ValueError.

        Example:
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.uri = ' '
            >>> record.validate()
            Traceback (most recent call last):
            ...
            ValueError: uri cannot be blank


        :return: None.
        """
        if not self.uri.strip():
            raise ValueError("uri cannot be blank")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared resource fields without target additions, validation or persistence.

        Example:
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record._common_write_payload()['resource_kind'] == ResourceKind.AUTHORITY
            True


        :return: New dictionary retaining stored values and enum members.
        """
        return {
            "resource_kind": self.resource_kind,
            "uri": self.uri,
            "label": self.label,
            "mime_type": self.mime_type,
            "access_note": self.access_note,
            "position": self.position,
            "is_primary": self.is_primary,
            "is_public": self.is_public,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require serialization of shared resource fields and concrete target context.

        Example:
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkResource(ResourceBase):
    """
    Attach a resource record to a work, including the canonical-for-work flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = WorkResource(work_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
        >>> record.target_id, record.target_kind
        (3, 'work')
    """
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this resource.

        Example:
            >>> record = WorkResource(work_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this resource as attached to a work.

        Example:
            >>> record = WorkResource(work_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared resource fields with the work id and the canonical-for-work flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = WorkResource(work_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.as_write_payload()['work_id']
            3


        :return: New dictionary of shared and work-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"work_id": self.work_id, "canonical_for_work": self.canonical_for_work})
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionResource(ResourceBase):
    """
    Attach a resource record to a expression, including the applies-to-realisation flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ExpressionResource(expression_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
        >>> record.target_id, record.target_kind
        (3, 'expression')
    """
    expression_id: ExpressionID
    applies_to_realisation: bool = True

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this resource.

        Example:
            >>> record = ExpressionResource(expression_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this resource as attached to a expression.

        Example:
            >>> record = ExpressionResource(expression_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared resource fields with the expression id and the applies-to-realisation flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ExpressionResource(expression_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"expression_id": self.expression_id, "applies_to_realisation": self.applies_to_realisation})
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationResource(ResourceBase):
    """
    Attach a resource record to a manifestation, including the edition-specific flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ManifestationResource(manifestation_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
        >>> record.target_id, record.target_kind
        (3, 'manifestation')
    """
    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this resource.

        Example:
            >>> record = ManifestationResource(manifestation_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this resource as attached to a manifestation.

        Example:
            >>> record = ManifestationResource(manifestation_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared resource fields with the manifestation id and the edition-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ManifestationResource(manifestation_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"manifestation_id": self.manifestation_id, "edition_specific": self.edition_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class ItemResource(ResourceBase):
    """
    Attach a resource record to a item, including the copy-specific flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ItemResource(item_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
        >>> record.target_id, record.target_kind
        (3, 'item')
    """
    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this resource.

        Example:
            >>> record = ItemResource(item_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this resource as attached to a item.

        Example:
            >>> record = ItemResource(item_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared resource fields with the item id and the copy-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ItemResource(item_id=3, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> record.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"item_id": self.item_id, "copy_specific": self.copy_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class KindResourcesContainer(MetadataSequenceStringMixin, Generic[ResourceT], abc.ABC):
    """
    Maintain ordered resource records for one kind and WEMI target.

    The generated constructor retains an explicitly supplied _resources list, or creates a
    fresh list by default. Records remain shared. Insertion checks shape and renumbers
    positions; full validation is explicit.

    Example:
        >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
        >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
        >>> records.add_resource(record)
        >>> records.texts()
        ('urn:example:catalogue:1',)
    """
    resource_kind: ResourceKind
    target_id: int
    _resources: list[ResourceT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]
    STRING_COUNT_LABEL = "resources"

    def __iter__(self) -> Iterator[ResourceT]:
        """
        Iterate over shared resource records in list order.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> next(iter(records)) is record
            True


        :return: Iterator over stored references.
        """
        return iter(self._resources)

    def __len__(self) -> int:
        """
        Count the stored resource records.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> len(records)
            1


        :return: Number of stored records.
        """
        return len(self._resources)

    def __getitem__(self, index: int) -> ResourceT:
        """
        Read a resource by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records[-1] is record
            True


        :param index: List index of the record to read.
        :return: Stored record object.
        """
        return self._resources[index]

    def resources(self) -> tuple[ResourceT, ...]:
        """
        Take a tuple snapshot of record order while retaining shared mutable objects.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.resources()[0] is record
            True


        :return: Tuple of stored record references.
        """
        return tuple(self._resources)

    def texts(self) -> tuple[str, ...]:
        """
        Collect resource display_text values in list order without validation.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.texts()
            ('urn:example:catalogue:1',)


        :return: Tuple of strings, retaining duplicates.
        """
        return tuple(resource.display_text for resource in self._resources)

    def to_text(self, sep: str = ", ") -> str:
        """
        Join every formatted resource using the separator.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.to_text(sep=' / ')
            'urn:example:catalogue:1'


        :param sep: Separator between resource displays; defaults to a comma and space.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.texts())

    def add_resource(self, resource: ResourceT) -> None:
        """
        Check target and resource kind, append the shared record and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record fields are checked
        only by validate.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> record.position
            0


        :param resource: Record matching this bucket target kind, id and assertion kind.
        :return: None.
        """
        self._validate_resource_shape(resource)
        self._resources.append(resource)
        self.normalize_positions()

    def replace_resource(self, index: int, resource: ResourceT) -> None:
        """
        Check shape, replace the indexed record and renumber positions.

        Shape mismatches raise ValueError; invalid indices raise IndexError.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.replace_resource(0, record)
            >>> records[0] is record and record.position == 0
            True


        :param index: List index of the record to replace.
        :param resource: Replacement record matching this bucket target and kind.
        :return: None.
        """
        self._validate_resource_shape(resource)
        self._resources[index] = resource
        self.normalize_positions()

    def remove_resource_at(self, index: int) -> ResourceT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its own fields. Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.remove_resource_at(0) is record
            True
            >>> len(records)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed record object.
        """
        removed = self._resources.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.clear()
            >>> len(records)
            0


        :return: None.
        """
        self._resources.clear()

    def move_resource(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber all positions.

        The source follows list.pop rules and the destination follows list.insert rules.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.move_resource(0, 99)
            >>> records[0] is record and record.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal; out-of-range destinations are
            clipped.
        :return: None.
        """
        resource = self._resources.pop(old_index)
        self._resources.insert(new_index, resource)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.set_primary(0)
            >>> record.is_primary
            True
            >>> records.set_primary(-1)
            >>> record.is_primary
            False


        :param index: Nonnegative index to designate, or an unmatched index to clear all
            flags.
        :return: None.
        """
        for i, resource in enumerate(self._resources):
            resource.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> record.position = 9
            >>> records.normalize_positions()
            >>> record.position
            0


        :return: None.
        """
        for index, resource in enumerate(self._resources):
            resource.position = index

    def validate(self) -> None:
        """
        Check resource shapes and values, contiguous positions and at most one primary record.

        Raise ValueError on the first failed constraint. Empty buckets are valid; values are
        not repaired.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.validate()
            >>> record.position = 2
            >>> records.validate()
            Traceback (most recent call last):
            ...
            ValueError: Resource position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0
        for expected_index, resource in enumerate(self._resources):
            self._validate_resource_shape(resource)
            resource.validate()
            if resource.position != expected_index:
                raise ValueError(f"Resource position mismatch for {self.target_kind} {self.target_id}: expected {expected_index}, got {resource.position}")
            if resource.is_primary:
                primary_count += 1
        if primary_count > 1:
            raise ValueError(f"Only one primary resource is allowed for {self.target_kind} {self.target_id} kind {self.resource_kind}")

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of per-record payload dictionaries.
        """
        return [resource.as_write_payload() for resource in self._resources]

    def _validate_resource_shape(self, resource: ResourceT) -> None:
        """
        Require matching target kind, target id and resource kind.

        Raise ValueError on the first mismatch; other record fields are not inspected.

        Example:
            >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records._validate_resource_shape(record)


        :param resource: Candidate record whose target and assertion kind must match this
            bucket.
        :return: None.
        """
        if resource.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {resource.target_kind} resource to {self.target_kind} container")
        if resource.target_id != self.target_id:
            raise ValueError(f"Resource target_id {resource.target_id} does not match container target_id {self.target_id}")
        if resource.resource_kind != self.resource_kind:
            raise ValueError(f"Resource kind {resource.resource_kind} does not match container kind {self.resource_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindResourcesContainer(KindResourcesContainer[WorkResource]):
    """
    Collect ordered resource assertions of one kind on a work.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'work' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = WorkKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=3)
        >>> records.target_kind, len(records)
        ('work', 0)
    """
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindResourcesContainer(KindResourcesContainer[ExpressionResource]):
    """
    Collect ordered resource assertions of one kind on a expression.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'expression' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ExpressionKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=3)
        >>> records.target_kind, len(records)
        ('expression', 0)
    """
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindResourcesContainer(KindResourcesContainer[ManifestationResource]):
    """
    Collect ordered resource assertions of one kind on a manifestation.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'manifestation' and can be supplied explicitly; construction does
    not validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ManifestationKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=3)
        >>> records.target_kind, len(records)
        ('manifestation', 0)
    """
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindResourcesContainer(KindResourcesContainer[ItemResource]):
    """
    Collect ordered resource assertions of one kind on a item.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'item' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ItemKindResourcesContainer(resource_kind=ResourceKind.AUTHORITY, target_id=3)
        >>> records.target_kind, len(records)
        ('item', 0)
    """
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetResourcesContainer(
    MetadataSequenceStringMixin,
    Generic[ResourceT, KindContainerT],
    abc.ABC,
):
    """
    Group editable resource buckets by kind for one WEMI target.

    Kind registration order is retained, including empty buckets. The dataclass
    constructor retains a supplied _by_kind dictionary, or creates an independent empty
    mapping.

    Example:
        >>> records = WorkResourcesContainer(work_id=1)
        >>> records.kinds()
        ()
    """
    _by_kind: dict[ResourceKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "resources"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id represented by this container.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> records.target_id
            1


        :return: Concrete target id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level represented by this container.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> records.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, resource_kind: ResourceKind) -> KindContainerT:
        """
        Require creation of an empty kind bucket using this target id.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> bucket = records._make_kind_container(ResourceKind.AUTHORITY)
            >>> bucket.target_id, records.kinds()
            (1, ())


        :param resource_kind: ResourceKind value selecting a bucket for this target.
        :return: New target-specific bucket; registration belongs to ensure_kind.
        """

    def kinds(self) -> tuple[ResourceKind, ...]:
        """
        Return registered kind keys in insertion order, including empty buckets.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> records.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def get_kind(self, resource_kind: ResourceKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> records.get_kind(ResourceKind.AUTHORITY) is None
            True


        :param resource_kind: ResourceKind value selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(resource_kind)

    def ensure_kind(self, resource_kind: ResourceKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one if absent.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> bucket = records.ensure_kind(ResourceKind.AUTHORITY)
            >>> records.ensure_kind(ResourceKind.AUTHORITY) is bucket
            True


        :param resource_kind: ResourceKind value selecting a bucket for this target.
        :return: Live bucket using this target id.
        """
        container = self._by_kind.get(resource_kind)
        if container is None:
            container = self._make_kind_container(resource_kind)
            self._by_kind[resource_kind] = container
        return container

    def add_resource(self, resource: ResourceT) -> None:
        """
        Check target id, ensure the resource kind bucket and delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later shape failure can
        leave a new empty bucket. Successful insertion renumbers positions; full validation
        remains explicit.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> records.add_resource(WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1'))
            >>> records.kind_text(ResourceKind.AUTHORITY)
            'urn:example:catalogue:1'


        :param resource: Shared record to insert into its kind bucket.
        :return: None.
        """
        if resource.target_id != self.target_id:
            raise ValueError(f"Resource target_id {resource.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(resource.resource_kind).add_resource(resource)

    def iter_all_resources(self) -> Iterator[ResourceT]:
        """
        Yield shared records in kind registration order and then bucket order.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> next(records.iter_all_resources()) is record
            True


        :return: Iterator over all stored records.
        """
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, resource_kind: ResourceKind, sep: str = ", ") -> str:
        """
        Join resource displays without creating a missing bucket.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> records.kind_text(ResourceKind.AUTHORITY), records.kinds()
            ('', ())


        :param resource_kind: ResourceKind value selecting a bucket for this target.
        :param sep: Separator between resource displays; defaults to a comma and space.
        :return: Rendered text, or an empty string for an absent or empty bucket.
        """
        container = self.get_kind(resource_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def validate(self) -> None:
        """
        Validate every registered bucket without repairing data or checking bucket ownership against this container.

        Each bucket checks its own target, kind and records. The first bucket error
        propagates.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> records.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-bucket payloads in kind registration order without validation or persistence.

        Example:
            >>> records = WorkResourcesContainer(work_id=1)
            >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
            >>> records.add_resource(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of record payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkResourcesContainer(BaseTargetResourcesContainer[WorkResource, WorkKindResourcesContainer]):
    """
    Group all resource assertions on a work by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = WorkResourcesContainer(work_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this resource container.

        Example:
            >>> records = WorkResourcesContainer(work_id=3)
            >>> records.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the resource-container target as a work.

        Example:
            >>> records = WorkResourcesContainer(work_id=3)
            >>> records.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, resource_kind: ResourceKind) -> WorkKindResourcesContainer:
        """
        Build an empty work kind bucket without registering it.

        Example:
            >>> records = WorkResourcesContainer(work_id=3)
            >>> bucket = records._make_kind_container(ResourceKind.AUTHORITY)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param resource_kind: ResourceKind value selecting a bucket for this target.
        :return: New WorkKindResourcesContainer using the stored target id.
        """
        return WorkKindResourcesContainer(resource_kind=resource_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionResourcesContainer(BaseTargetResourcesContainer[ExpressionResource, ExpressionKindResourcesContainer]):
    """
    Group all resource assertions on a expression by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ExpressionResourcesContainer(expression_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this resource container.

        Example:
            >>> records = ExpressionResourcesContainer(expression_id=3)
            >>> records.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the resource-container target as a expression.

        Example:
            >>> records = ExpressionResourcesContainer(expression_id=3)
            >>> records.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(self, resource_kind: ResourceKind) -> ExpressionKindResourcesContainer:
        """
        Build an empty expression kind bucket without registering it.

        Example:
            >>> records = ExpressionResourcesContainer(expression_id=3)
            >>> bucket = records._make_kind_container(ResourceKind.AUTHORITY)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param resource_kind: ResourceKind value selecting a bucket for this target.
        :return: New ExpressionKindResourcesContainer using the stored target id.
        """
        return ExpressionKindResourcesContainer(resource_kind=resource_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationResourcesContainer(BaseTargetResourcesContainer[ManifestationResource, ManifestationKindResourcesContainer]):
    """
    Group all resource assertions on a manifestation by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ManifestationResourcesContainer(manifestation_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this resource container.

        Example:
            >>> records = ManifestationResourcesContainer(manifestation_id=3)
            >>> records.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the resource-container target as a manifestation.

        Example:
            >>> records = ManifestationResourcesContainer(manifestation_id=3)
            >>> records.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(self, resource_kind: ResourceKind) -> ManifestationKindResourcesContainer:
        """
        Build an empty manifestation kind bucket without registering it.

        Example:
            >>> records = ManifestationResourcesContainer(manifestation_id=3)
            >>> bucket = records._make_kind_container(ResourceKind.AUTHORITY)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param resource_kind: ResourceKind value selecting a bucket for this target.
        :return: New ManifestationKindResourcesContainer using the stored target id.
        """
        return ManifestationKindResourcesContainer(resource_kind=resource_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemResourcesContainer(BaseTargetResourcesContainer[ItemResource, ItemKindResourcesContainer]):
    """
    Group all resource assertions on a item by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ItemResourcesContainer(item_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this resource container.

        Example:
            >>> records = ItemResourcesContainer(item_id=3)
            >>> records.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the resource-container target as a item.

        Example:
            >>> records = ItemResourcesContainer(item_id=3)
            >>> records.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, resource_kind: ResourceKind) -> ItemKindResourcesContainer:
        """
        Build an empty item kind bucket without registering it.

        Example:
            >>> records = ItemResourcesContainer(item_id=3)
            >>> bucket = records._make_kind_container(ResourceKind.AUTHORITY)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param resource_kind: ResourceKind value selecting a bucket for this target.
        :return: New ItemKindResourcesContainer using the stored target id.
        """
        return ItemKindResourcesContainer(resource_kind=resource_kind, target_id=self.item_id)


def _kind_property_stem(resource_kind: ResourceKind) -> str:
    """
    Look up the fixed convenience-property stem for a resource kind.

    Unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(ResourceKind.AUTHORITY)
        'authority_resources'


    :param resource_kind: ResourceKind value selecting a convenience stem.
    :return: Attribute stem for the selected kind.
    """
    stems = {
        ResourceKind.AUTHORITY: "authority_resources",
        ResourceKind.CATALOGUE: "catalogue_resources",
        ResourceKind.FULL_TEXT: "full_text_resources",
        ResourceKind.PREVIEW: "preview_resources",
        ResourceKind.DOWNLOAD: "download_resources",
        ResourceKind.COVER_IMAGE: "cover_image_resources",
        ResourceKind.MIRROR: "mirror_resources",
        ResourceKind.PUBLISHER: "publisher_resources",
        ResourceKind.PURCHASE: "purchase_resources",
    }
    return stems[resource_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetResourcesContainer]) -> None:
    """
    Install per-kind bucket, text and configurable text accessors on a resource container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing names are overwritten. The
    explicit kind methods remain canonical under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> records = WorkResourcesContainer(work_id=1)
        >>> records.authority_resources_text, records.kinds()
        ('', ())
        >>> bucket = records.authority_resources
        >>> records.get_kind(ResourceKind.AUTHORITY) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """
    for resource_kind in ResourceKind:
        stem = _kind_property_stem(resource_kind)

        def kind_container_getter(self, _kind=resource_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> records = WorkResourcesContainer(work_id=1)
                >>> bucket = records.authority_resources
                >>> records.get_kind(ResourceKind.AUTHORITY) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Resource kind captured as the default argument during installation.
            :return: Live per-kind container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=resource_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> records = WorkResourcesContainer(work_id=1)
                >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
                >>> records.add_resource(record)
                >>> records.authority_resources_text
                'urn:example:catalogue:1'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Resource kind captured as the default argument during installation.
            :return: Joined resource displays, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=resource_kind) -> str:
            """
            Render the captured kind with a caller-selected separator.

            Example:
                >>> records = WorkResourcesContainer(work_id=1)
                >>> record = WorkResource(work_id=1, resource_kind=ResourceKind.AUTHORITY, uri='urn:example:catalogue:1')
                >>> records.add_resource(record)
                >>> records.authority_resources_to_text(sep=' / ')
                'urn:example:catalogue:1'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Resource kind captured as the default argument during installation.
            :return: Joined resource displays, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkResourcesContainer)
_install_kind_convenience_properties(ExpressionResourcesContainer)
_install_kind_convenience_properties(ManifestationResourcesContainer)
_install_kind_convenience_properties(ItemResourcesContainer)


__all__ = [
    "ResourceKind",
    "ResourceBase",
    "WorkResource",
    "ExpressionResource",
    "ManifestationResource",
    "ItemResource",
    "KindResourcesContainer",
    "WorkKindResourcesContainer",
    "ExpressionKindResourcesContainer",
    "ManifestationKindResourcesContainer",
    "ItemKindResourcesContainer",
    "BaseTargetResourcesContainer",
    "WorkResourcesContainer",
    "ExpressionResourcesContainer",
    "ManifestationResourcesContainer",
    "ItemResourcesContainer",
]
