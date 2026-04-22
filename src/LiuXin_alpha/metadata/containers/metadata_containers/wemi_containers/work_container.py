
"""
Container for information from the Works table.

Works are at the top of the FRBR tree - everything descends from them.
"""

from __future__ import annotations

from typing import Any, Iterator, Mapping, Optional, Iterable

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_metadata_container_api import \
    WorkContainerAPI

from LiuXin_alpha.utils.adaptors import _boolish_to_bool, _bool_to_int_or_none


class WorkContainer(WorkContainerAPI):
    """
    Container for a single row from the `works` table.

    This is distinct from a richer "WorkMetadata" concept. It only mirrors the
    `works` row itself, plus light convenience helpers.
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
    def from_mapping(cls, row: Mapping[str, Any]) -> "WorkContainer":
        """
        Build from a mapping (e.g. sqlite3.Row, dict).
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
        Convert to a plain dict using column names from the `works` table.
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
        return self.to_dict()

    # -------------------------
    # Core fields
    # -------------------------

    @property
    def work_id(self) -> Optional[int]:
        return self._work_id

    @work_id.setter
    def work_id(self, work_id: Optional[int]) -> None:
        # IDs are usually set once; keep the previous behaviour.
        if self._work_id is None:
            self._work_id = work_id
        else:
            raise AttributeError("Work id is already set.")

    @property
    def work_type(self) -> Optional[str]:
        return self._work_type

    @work_type.setter
    def work_type(self, value: Optional[str]) -> None:
        self._work_type = value

    @property
    def work_medium(self) -> Optional[str]:
        return self._work_medium

    @work_medium.setter
    def work_medium(self, value: Optional[str]) -> None:
        self._work_medium = value

    @property
    def work_title(self) -> Optional[str]:
        return self._work_title

    @work_title.setter
    def work_title(self, value: Optional[str]) -> None:
        self._work_title = value

    @property
    def work_canonical_title(self) -> Optional[str]:
        return self._work_canonical_title

    @work_canonical_title.setter
    def work_canonical_title(self, value: Optional[str]) -> None:
        self._work_canonical_title = value

    @property
    def work_sort_title(self) -> Optional[str]:
        return self._work_sort_title

    @work_sort_title.setter
    def work_sort_title(self, value: Optional[str]) -> None:
        self._work_sort_title = value

    @property
    def work_original_language_id(self) -> Optional[int]:
        return self._work_original_language_id

    @work_original_language_id.setter
    def work_original_language_id(self, value: Optional[int]) -> None:
        self._work_original_language_id = value

    @property
    def work_original_year(self) -> Optional[int]:
        return self._work_original_year

    @work_original_year.setter
    def work_original_year(self, value: Optional[int]) -> None:
        self._work_original_year = value

    @property
    def work_is_fiction(self) -> Optional[bool]:
        return self._work_is_fiction

    @work_is_fiction.setter
    def work_is_fiction(self, value: Any) -> None:
        self._work_is_fiction = _boolish_to_bool(value)

    @property
    def work_audience(self) -> Optional[str]:
        return self._work_audience

    @work_audience.setter
    def work_audience(self, value: Optional[str]) -> None:
        self._work_audience = value

    @property
    def work_completion_status(self) -> Optional[str]:
        return self._work_completion_status

    @work_completion_status.setter
    def work_completion_status(self, value: Optional[str]) -> None:
        self._work_completion_status = value

    @property
    def work_discovery_note(self) -> Optional[str]:
        return self._work_discovery_note

    @work_discovery_note.setter
    def work_discovery_note(self, value: Optional[str]) -> None:
        self._work_discovery_note = value

    @property
    def work_created_timestamp_ep_k(self) -> Optional[int]:
        return self._work_created_timestamp_ep_k

    @work_created_timestamp_ep_k.setter
    def work_created_timestamp_ep_k(self, value: Optional[int]) -> None:
        self._work_created_timestamp_ep_k = value

    @property
    def work_modified_timestamp_ep_k(self) -> Optional[int]:
        return self._work_modified_timestamp_ep_k

    @work_modified_timestamp_ep_k.setter
    def work_modified_timestamp_ep_k(self, value: Optional[int]) -> None:
        self._work_modified_timestamp_ep_k = value

    @property
    def work_scratch(self) -> Optional[str]:
        return self._work_scratch

    @work_scratch.setter
    def work_scratch(self, value: Optional[str]) -> None:
        self._work_scratch = value

    # -------------------------
    # Back-compat / convenience
    # -------------------------

    @property
    def work_name(self) -> Optional[str]:
        """
        Backwards-compatible alias for the old `_work_name` field.
        Prefer `work_title` going forward.
        """
        return self._work_title

    @work_name.setter
    def work_name(self, value: Optional[str]) -> None:
        self._work_title = value

    def __repr__(self) -> str:
        return (
            f"WorkContainer(work_id={self._work_id!r}, "
            f"work_title={self._work_title!r}, work_type={self._work_type!r})"
        )


class WorksContainer:
    """
    A light collection wrapper for multiple WorkContainer objects.
    """

    def __init__(self, works: Iterable[WorkContainer] = ()) -> None:
        self._works: list[WorkContainer] = [wc for wc in works]

    def __iter__(self) -> Iterator[WorkContainer]:
        return iter(self._works)

    def __len__(self) -> int:
        return len(self._works)

    def __getitem__(self, idx: int) -> WorkContainer:
        return self._works[idx]

    def add(self, work: WorkContainer) -> None:
        self._works.append(work)

    def extend(self, works: Iterable[WorkContainer]) -> None:
        self._works.extend(list(works))

    def get_by_id(self, work_id: int) -> Optional[WorkContainer]:
        for w in self._works:
            if w.work_id == work_id:
                return w
        return None

    def to_dicts(self) -> list[dict[str, Any]]:
        return [w.to_dict() for w in self._works]


