"""
Represent work identities and ordered collections of those identities.

Identity fields mirror the works table; relation collections belong to WorkMetadata.
These containers perform no database lookup or writes.

Example:
    >>> work = WorkIdentity(work_id=3, work_title='Example')
    >>> work.to_mapping()['work_title']
    'Example'
"""



from __future__ import annotations

from typing import Any, Iterator, Mapping, Optional, Iterable

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import \
    WorkIdentityAPI
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_container_string,
    compact_mapping_string,
)

from LiuXin_alpha.utils.adaptors import _boolish_to_bool, _bool_to_int_or_none


class WorkIdentity(WorkIdentityAPI):
    """
    Hold intrinsic work fields with mapping conversion and a write-once id.

    Construction retains values as supplied. The fiction flag is normalized by its
    setter and from_mapping, but not by the constructor. Related metadata lives in a
    separate bundle.

    Example:
        >>> work = WorkIdentity(work_id=3, work_title='Example')
        >>> work.work_id, work.work_title
        (3, 'Example')
    """

    def __init__(
        self,
        *,
        word_id: Optional[int] = None,
        work_id: Optional[int] = None,
        work_type: Optional[str] = None,
        work_medium: Optional[str] = None,
        work_title: Optional[str] = None,
        work_canonical_title: Optional[str] = None,
        work_sort_title: Optional[str] = None,
        work_original_language_id: Optional[int] = None,
        work_original_year: Optional[int] = None,
        work_is_fiction: Optional[bool] = None,
        work_audience: Optional[str] = None,
        work_completion_status: Optional[str] = None,
        work_discovery_note: Optional[str] = None,
        work_created_timestamp_ep_k: Optional[int] = None,
        work_modified_timestamp_ep_k: Optional[int] = None,
        work_scratch: Optional[str] = None,
    ) -> None:
        """
        Store intrinsic work fields, accepting word_id as a legacy constructor alias.

        Use word_id only when work_id is None; any supplied work_id, including zero, wins.
        Values are retained without coercion, including the fiction flag. A non-None id
        locks subsequent assignments.

        Example:
            >>> WorkIdentity(word_id=7).work_id
            7
            >>> WorkIdentity(word_id=7, work_id=0).work_id
            0


        :param word_id: Legacy id used only when work_id is None.
        :param work_id: Optional work row id retained as supplied.
        :param work_type: Optional work form or type retained as supplied.
        :param work_medium: Optional medium retained as supplied.
        :param work_title: Optional display title retained as supplied.
        :param work_canonical_title: Optional canonical title retained as supplied.
        :param work_sort_title: Optional sort title retained as supplied.
        :param work_original_language_id: Optional original-language row id retained as
            supplied.
        :param work_original_year: Optional original year retained as supplied.
        :param work_is_fiction: Optional fiction flag retained as supplied.
        :param work_audience: Optional intended audience retained as supplied.
        :param work_completion_status: Optional completion status retained as supplied.
        :param work_discovery_note: Optional discovery note retained as supplied.
        :param work_created_timestamp_ep_k: Optional creation timestamp retained as
            supplied.
        :param work_modified_timestamp_ep_k: Optional modification timestamp retained as
            supplied.
        :param work_scratch: Optional scratch text retained as supplied.
        :return: None.
        """
        if work_id is None and word_id is not None:
            work_id = word_id
        self._work_id: Optional[int] = work_id

        self._work_type: Optional[str] = work_type
        self._work_medium: Optional[str] = work_medium

        self._work_title: Optional[str] = work_title
        self._work_canonical_title: Optional[str] = work_canonical_title
        self._work_sort_title: Optional[str] = work_sort_title

        self._work_original_language_id: Optional[int] = work_original_language_id
        self._work_original_year: Optional[int] = work_original_year

        self._work_is_fiction: Optional[bool] = work_is_fiction
        self._work_audience: Optional[str] = work_audience
        self._work_completion_status: Optional[str] = work_completion_status

        self._work_discovery_note: Optional[str] = work_discovery_note

        self._work_created_timestamp_ep_k: Optional[int] = work_created_timestamp_ep_k
        self._work_modified_timestamp_ep_k: Optional[int] = work_modified_timestamp_ep_k

        self._work_scratch: Optional[str] = work_scratch

    # -------------------------
    # Construction helpers
    # -------------------------

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "WorkIdentity":
        """
        Construct a work from canonical works-table keys.

        Missing fields become None and extra keys, including word_id, are ignored. Only the
        fiction flag uses the shared bool-like conversion; other values are retained.

        Example:
            >>> work = WorkIdentity.from_mapping({'work_id': 3, 'work_is_fiction': '0'})
            >>> work.work_id, work.work_is_fiction
            (3, False)


        :param row: Mapping supporting get for canonical work fields.
        :return: New identity of the requested class.
        """
        return cls(
            work_id=row.get("work_id"),
            work_type=row.get("work_type"),
            work_medium=row.get("work_medium"),
            work_title=row.get("work_title"),
            work_canonical_title=row.get("work_canonical_title"),
            work_sort_title=row.get("work_sort_title"),
            work_original_language_id=row.get("work_original_language_id"),
            work_original_year=row.get("work_original_year"),
            work_is_fiction=_boolish_to_bool(row.get("work_is_fiction")),
            work_audience=row.get("work_audience"),
            work_completion_status=row.get("work_completion_status"),
            work_discovery_note=row.get("work_discovery_note"),
            work_created_timestamp_ep_k=row.get("work_created_timestamp_ep_k"),
            work_modified_timestamp_ep_k=row.get("work_modified_timestamp_ep_k"),
            work_scratch=row.get("work_scratch"),
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Copy all fifteen canonical fields into a database-shaped dictionary.

        The fiction flag becomes None or integer 0/1 according to truthiness. This can
        differ from the stricter setter conversion when construction retained a nonboolean
        value. Other fields are unchanged.

        Example:
            >>> WorkIdentity(work_is_fiction='0').to_dict()['work_is_fiction']
            1


        :return: New dictionary including None-valued fields.
        """
        return {
            "work_id": self.work_id,
            "work_type": self.work_type,
            "work_medium": self.work_medium,
            "work_title": self.work_title,
            "work_canonical_title": self.work_canonical_title,
            "work_sort_title": self.work_sort_title,
            "work_original_language_id": self.work_original_language_id,
            "work_original_year": self.work_original_year,
            # DB stores 0/1/NULL
            "work_is_fiction": _bool_to_int_or_none(self.work_is_fiction),
            "work_audience": self.work_audience,
            "work_completion_status": self.work_completion_status,
            "work_discovery_note": self.work_discovery_note,
            "work_created_timestamp_ep_k": self.work_created_timestamp_ep_k,
            "work_modified_timestamp_ep_k": self.work_modified_timestamp_ep_k,
            "work_scratch": self.work_scratch,
        }

    def to_mapping(self) -> dict[str, Any]:
        """
        Serialize all canonical work fields through to_dict.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> work.to_mapping()['work_id']
            3


        :return: New dictionary with the fiction flag encoded as 0, 1 or None.
        """
        return self.to_dict()

    def __str__(self) -> str:
        """
        Render a compact diagnostic summary using the id and preferred title/type fields.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> 'Example' in str(work)
            True


        :return: Human-readable work summary.
        """
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=("work_id",),
            display_keys=("work_canonical_title", "work_title", "work_type"),
        )

    # -------------------------
    # Core fields
    # -------------------------

    @property
    def work_id(self) -> Optional[int]:
        """
        Return the stored work row id without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_id=3)
            >>> work.work_id
            3


        :return: Stored value, or None.
        """
        return self._work_id

    @work_id.setter
    def work_id(self, work_id: Optional[int]) -> None:
        # IDs are usually set once; keep the previous behaviour.
        """
        Assign the work id only while its current value is None.

        Assigning None leaves the id assignable. Once non-None, even assignment of the same
        id raises AttributeError.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_id = 3
            >>> work.work_id = 3
            Traceback (most recent call last):
            ...
            AttributeError: Work id is already set.


        :param work_id: New id retained without conversion.
        :return: None.
        """
        if self._work_id is None:
            self._work_id = work_id
        else:
            raise AttributeError("Work id is already set.")

    @property
    def work_type(self) -> Optional[str]:
        """
        Return the stored work form or type without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_type='novel')
            >>> work.work_type
            'novel'


        :return: Stored value, or None.
        """
        return self._work_type

    @work_type.setter
    def work_type(self, value: Optional[str]) -> None:
        """
        Replace the work form or type without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_type = 'novel'
            >>> work.work_type
            'novel'


        :param value: New work form or type, or None.
        :return: None.
        """
        self._work_type = value

    @property
    def work_medium(self) -> Optional[str]:
        """
        Return the stored medium without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_medium='text')
            >>> work.work_medium
            'text'


        :return: Stored value, or None.
        """
        return self._work_medium

    @work_medium.setter
    def work_medium(self, value: Optional[str]) -> None:
        """
        Replace the medium without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_medium = 'text'
            >>> work.work_medium
            'text'


        :param value: New medium, or None.
        :return: None.
        """
        self._work_medium = value

    @property
    def work_title(self) -> Optional[str]:
        """
        Return the stored display title without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_title='Example')
            >>> work.work_title
            'Example'


        :return: Stored value, or None.
        """
        return self._work_title

    @work_title.setter
    def work_title(self, value: Optional[str]) -> None:
        """
        Replace the display title without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_title = 'Example'
            >>> work.work_title
            'Example'


        :param value: New display title, or None.
        :return: None.
        """
        self._work_title = value

    @property
    def work_canonical_title(self) -> Optional[str]:
        """
        Return the stored canonical title without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_canonical_title='Example')
            >>> work.work_canonical_title
            'Example'


        :return: Stored value, or None.
        """
        return self._work_canonical_title

    @work_canonical_title.setter
    def work_canonical_title(self, value: Optional[str]) -> None:
        """
        Replace the canonical title without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_canonical_title = 'Example'
            >>> work.work_canonical_title
            'Example'


        :param value: New canonical title, or None.
        :return: None.
        """
        self._work_canonical_title = value

    @property
    def work_sort_title(self) -> Optional[str]:
        """
        Return the stored sort title without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_sort_title='Example, The')
            >>> work.work_sort_title
            'Example, The'


        :return: Stored value, or None.
        """
        return self._work_sort_title

    @work_sort_title.setter
    def work_sort_title(self, value: Optional[str]) -> None:
        """
        Replace the sort title without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_sort_title = 'Example, The'
            >>> work.work_sort_title
            'Example, The'


        :param value: New sort title, or None.
        :return: None.
        """
        self._work_sort_title = value

    @property
    def work_original_language_id(self) -> Optional[int]:
        """
        Return the stored original-language row id without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_original_language_id=2)
            >>> work.work_original_language_id
            2


        :return: Stored value, or None.
        """
        return self._work_original_language_id

    @work_original_language_id.setter
    def work_original_language_id(self, value: Optional[int]) -> None:
        """
        Replace the original-language row id without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_original_language_id = 2
            >>> work.work_original_language_id
            2


        :param value: New original-language row id, or None.
        :return: None.
        """
        self._work_original_language_id = value

    @property
    def work_original_year(self) -> Optional[int]:
        """
        Return the stored original year without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_original_year=1818)
            >>> work.work_original_year
            1818


        :return: Stored value, or None.
        """
        return self._work_original_year

    @work_original_year.setter
    def work_original_year(self, value: Optional[int]) -> None:
        """
        Replace the original year without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_original_year = 1818
            >>> work.work_original_year
            1818


        :param value: New original year, or None.
        :return: None.
        """
        self._work_original_year = value

    @property
    def work_is_fiction(self) -> Optional[bool]:
        """
        Return the stored fiction flag without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_is_fiction=True)
            >>> work.work_is_fiction
            True


        :return: Stored value, or None.
        """
        return self._work_is_fiction

    @work_is_fiction.setter
    def work_is_fiction(self, value: Any) -> None:
        """
        Normalize a bool-like fiction flag with the shared adapter.

        Recognized boolean, integer and case-sensitive text spellings become bool;
        unsupported values become None.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_is_fiction = '0'
            >>> work.work_is_fiction
            False
            >>> work.work_is_fiction = 2
            >>> work.work_is_fiction is None
            True


        :param value: Bool-like value to normalize.
        :return: None.
        """
        self._work_is_fiction = _boolish_to_bool(value)

    @property
    def work_audience(self) -> Optional[str]:
        """
        Return the stored intended audience without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_audience='adult')
            >>> work.work_audience
            'adult'


        :return: Stored value, or None.
        """
        return self._work_audience

    @work_audience.setter
    def work_audience(self, value: Optional[str]) -> None:
        """
        Replace the intended audience without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_audience = 'adult'
            >>> work.work_audience
            'adult'


        :param value: New intended audience, or None.
        :return: None.
        """
        self._work_audience = value

    @property
    def work_completion_status(self) -> Optional[str]:
        """
        Return the stored completion status without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_completion_status='complete')
            >>> work.work_completion_status
            'complete'


        :return: Stored value, or None.
        """
        return self._work_completion_status

    @work_completion_status.setter
    def work_completion_status(self, value: Optional[str]) -> None:
        """
        Replace the completion status without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_completion_status = 'complete'
            >>> work.work_completion_status
            'complete'


        :param value: New completion status, or None.
        :return: None.
        """
        self._work_completion_status = value

    @property
    def work_discovery_note(self) -> Optional[str]:
        """
        Return the stored discovery note without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_discovery_note='Found in catalogue')
            >>> work.work_discovery_note
            'Found in catalogue'


        :return: Stored value, or None.
        """
        return self._work_discovery_note

    @work_discovery_note.setter
    def work_discovery_note(self, value: Optional[str]) -> None:
        """
        Replace the discovery note without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_discovery_note = 'Found in catalogue'
            >>> work.work_discovery_note
            'Found in catalogue'


        :param value: New discovery note, or None.
        :return: None.
        """
        self._work_discovery_note = value

    @property
    def work_created_timestamp_ep_k(self) -> Optional[int]:
        """
        Return the stored creation timestamp without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_created_timestamp_ep_k=100)
            >>> work.work_created_timestamp_ep_k
            100


        :return: Stored value, or None.
        """
        return self._work_created_timestamp_ep_k

    @work_created_timestamp_ep_k.setter
    def work_created_timestamp_ep_k(self, value: Optional[int]) -> None:
        """
        Replace the creation timestamp without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_created_timestamp_ep_k = 100
            >>> work.work_created_timestamp_ep_k
            100


        :param value: New creation timestamp, or None.
        :return: None.
        """
        self._work_created_timestamp_ep_k = value

    @property
    def work_modified_timestamp_ep_k(self) -> Optional[int]:
        """
        Return the stored modification timestamp without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_modified_timestamp_ep_k=200)
            >>> work.work_modified_timestamp_ep_k
            200


        :return: Stored value, or None.
        """
        return self._work_modified_timestamp_ep_k

    @work_modified_timestamp_ep_k.setter
    def work_modified_timestamp_ep_k(self, value: Optional[int]) -> None:
        """
        Replace the modification timestamp without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_modified_timestamp_ep_k = 200
            >>> work.work_modified_timestamp_ep_k
            200


        :param value: New modification timestamp, or None.
        :return: None.
        """
        self._work_modified_timestamp_ep_k = value

    @property
    def work_scratch(self) -> Optional[str]:
        """
        Return the stored scratch text without lookup or conversion.

        Example:
            >>> work = WorkIdentity(work_scratch='local note')
            >>> work.work_scratch
            'local note'


        :return: Stored value, or None.
        """
        return self._work_scratch

    @work_scratch.setter
    def work_scratch(self, value: Optional[str]) -> None:
        """
        Replace the scratch text without validation or conversion.

        Example:
            >>> work = WorkIdentity()
            >>> work.work_scratch = 'local note'
            >>> work.work_scratch
            'local note'


        :param value: New scratch text, or None.
        :return: None.
        """
        self._work_scratch = value

    # -------------------------
    # Back-compat / convenience
    # -------------------------

    @property
    def work_name(self) -> Optional[str]:
        """
        Read the legacy name alias from the same field as work_title.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> work.work_name
            'Example'


        :return: Stored display title, or None.
        """
        return self._work_title

    @work_name.setter
    def work_name(self, value: Optional[str]) -> None:
        """
        Replace work_title through the legacy name alias.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> work.work_name = 'Revised'
            >>> work.work_title
            'Revised'


        :param value: New display title retained as supplied.
        :return: None.
        """
        self._work_title = value

    def __repr__(self) -> str:
        """
        Format the id, display title and work type with repr-style values.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> repr(work)
            "WorkIdentity(work_id=3, work_title='Example', work_type=None)"


        :return: Diagnostic representation of these three fields.
        """
        return (
            f"WorkIdentity(work_id={self._work_id!r}, "
            f"work_title={self._work_title!r}, work_type={self._work_type!r})"
        )


class WorkIdentities:
    """
    Keep work identities in insertion order without type checks or deduplication.

    The collection owns its list and shares the identity objects. Lookup returns the
    first equal id, so duplicate ids remain meaningful.

    Example:
        >>> work = WorkIdentity(work_id=3, work_title='Example')
        >>> works = WorkIdentities([work])
        >>> works[0] is work
        True
    """

    def __init__(self, works: Iterable[WorkIdentity] = ()) -> None:
        """
        Materialize the supplied iterable into a new list of shared identities.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> len(works)
            1


        :param works: Iterable consumed in order; defaults to empty.
        :return: None.
        """
        self._works: list[WorkIdentity] = [wc for wc in works]

    def __iter__(self) -> Iterator[WorkIdentity]:
        """
        Iterate shared identities in current list order.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> list(works)[0] is work
            True


        :return: Iterator over the backing list.
        """
        return iter(self._works)

    def __len__(self) -> int:
        """
        Count stored entries, including duplicates.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> len(works)
            1


        :return: Number of entries.
        """
        return len(self._works)

    def __getitem__(self, idx: int) -> WorkIdentity:
        """
        Delegate indexing to the backing list.

        Negative indexes and runtime slices follow list behavior. Invalid indexes raise the
        usual list exceptions.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> works[-1] is work
            True


        :param idx: List index; slicing is also accepted at runtime.
        :return: Shared identity for an integer index, or a new list for a slice.
        """
        return self._works[idx]

    def add(self, work: WorkIdentity) -> None:
        """
        Append the supplied identity without validation or duplicate checks.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> works.add(work)
            >>> len(works)
            2


        :param work: Identity object to retain by reference.
        :return: None.
        """
        self._works.append(work)

    def extend(self, works: Iterable[WorkIdentity]) -> None:
        """
        Materialize an iterable before appending its shared identities in order.

        If iteration fails, no entries from that iterable are appended by this method.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> works.extend([work, work])
            >>> len(works)
            3


        :param works: Iterable of identity objects to append.
        :return: None.
        """
        self._works.extend(list(works))

    def get_by_id(self, work_id: int) -> Optional[WorkIdentity]:
        """
        Find the first identity whose work_id compares equal to the supplied id.

        No integer conversion or uniqueness check occurs.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> works.get_by_id(3) is work
            True
            >>> works.get_by_id('3') is None
            True


        :param work_id: Id compared directly against each stored work_id.
        :return: First matching shared identity, or None.
        """
        for w in self._works:
            if w.work_id == work_id:
                return w
        return None

    def to_dicts(self) -> list[dict[str, Any]]:
        """
        Serialize every stored identity in order through its to_dict method.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> works.to_dicts()[0]['work_title']
            'Example'


        :return: New list of dictionaries, including duplicate entries.
        """
        return [w.to_dict() for w in self._works]

    def __str__(self) -> str:
        """
        Render a compact diagnostic summary with the number of works.

        Example:
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> works = WorkIdentities([work])
            >>> 'WorkIdentities' in str(works)
            True


        :return: Human-readable collection summary.
        """
        return compact_container_string(self, count_label="works")

__all__ = ["WorkIdentity", "WorkIdentities"]
