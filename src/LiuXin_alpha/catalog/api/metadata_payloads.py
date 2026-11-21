"""Typed public scalar payloads for native WEMI creation and partial updates.

Absent keys preserve existing values on update; an explicit None requests SQL
NULL, subject to Catalog and schema validation. These optional-key TypedDicts
describe the portable aliases, not physical columns or attachment groups.
Creation keeps schema defaults for omitted keys. Catalog remains the runtime
authority for required values, references, normalization and constraints.
"""

from __future__ import annotations

from typing import TypedDict


class WorkUpdate(TypedDict, total=False):
    """Change only the supplied Work attributes."""

    title: str | None
    canonical_title: str | None
    sort_title: str | None
    creator_sort: str | None
    type: str | None
    medium: str | None
    original_language_id: int | None
    original_year: int | None
    original_date: int | None
    original_copyright_date: str | None
    wikipedia_link: str | None
    is_fiction: bool | None
    audience: str | None
    completion_status: str | None
    discovery_note: str | None


class WorkCreate(WorkUpdate):
    """Create a Work with schema defaults for omitted attributes."""


class ExpressionUpdate(TypedDict, total=False):
    """Change only the supplied realization attributes."""

    type: str | None
    label: str | None
    year: int | None
    is_preferred: bool | None
    language_id: int | None
    mode: str | None
    title: str | None
    title_override: str | None
    subtitle: str | None
    wordcount: int | None
    status: str | None
    origin_note: str | None


class ExpressionCreate(ExpressionUpdate):
    """Create an Expression with schema defaults for omitted attributes."""


class ManifestationUpdate(TypedDict, total=False):
    """Change only the supplied publication attributes."""

    subtitle: str | None
    carrier_type: str | None
    format_detail: str | None
    edition_statement: str | None
    pub_year: int | None
    pub_date: str | None
    page_count: int | None
    runtime_minutes: int | None
    region_code: str | None
    status: str | None
    note: str | None


class ManifestationCreate(ManifestationUpdate):
    """Create a Manifestation with schema defaults for omitted attributes."""


class ItemUpdate(TypedDict, total=False):
    """Change only the supplied copy attributes; parent changes follow Catalog."""

    manifestation_id: int | None
    type: str | None
    location: str | None
    inventory_code: str | None
    source: str | None
    source_detail: str | None
    source_path: str | None
    source_name: str | None
    acquired_date: str | None
    acquired_price_minor: int | None
    lifecycle_status: str | None
    condition: str | None


class ItemCreate(ItemUpdate):
    """Create an Item with schema defaults for omitted attributes."""
