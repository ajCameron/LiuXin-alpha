"""Validate native metadata transport values without taking over Catalog policy."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, cast

from LiuXin_alpha.catalog.api.common import WemiLevel
from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface
from LiuXin_alpha.catalog.api.metadata_revisions import (
    MetadataEditRevision,
    MetadataRevision,
    SharedMetadataReference,
    SharedMetadataRevision,
)
from LiuXin_alpha.catalog.api.metadata_search import MetadataReference
from LiuXin_alpha.catalog.api.metadata_shared import SharedMetadataKind
from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.payloads import integer_value, invalid_payload, validate_payload

MAX_PAGE = 1000
MAX_TREE = 1000
MAX_BATCH = 100
MAX_DEPTH = 48
MAX_INPUT_VALUES = 100000
SHARED_KINDS = (
    "agents",
    "tags",
    "labels",
    "genres",
    "subjects",
    "series",
    "languages",
    "ratings",
    "identifiers",
    "item_identifiers",
    "notes",
    "comments",
    "synopses",
    "annotations",
)
TREE_KINDS = ("genres", "subjects", "series")
SUGGEST_KINDS = (
    "agents",
    "tags",
    "labels",
    "genres",
    "subjects",
    "series",
    "languages",
)
type Payload = Mapping[str, Any]
type Handler = Callable[[MetadataAPIInterface, Payload], Any]


def choice(value: object, name: str, choices: tuple[str, ...]) -> str:
    """Accept only an explicit public selector; never resolve a caller attribute."""
    if not isinstance(value, str) or value not in choices:
        invalid_payload(name, f"must be one of: {', '.join(choices)}.")
    return value


def level(payload: Payload, name: str = "level") -> WemiLevel:
    """Validate a WEMI discriminator; an omitted level means Work."""
    return cast(
        WemiLevel,
        choice(
            payload.get(name, "work"),
            name,
            ("work", "expression", "manifestation", "item"),
        ),
    )


def identity(payload: Payload, name: str = "entity_id") -> int:
    """Require a positive, non-boolean integer identity."""
    value = integer_value(payload.get(name), name)
    if value < 1:
        invalid_payload(name, "must be positive.")
    return value


def optional_identity(payload: Payload, name: str) -> int | None:
    """Read an optional positive identity, preserving explicit root scope."""
    return None if payload.get(name) is None else identity(payload, name)


def bounded(value: object, name: str, maximum: int, minimum: int = 0) -> int:
    """Reject a request exceeding its published inclusive transport budget."""
    result = integer_value(value, name)
    if not minimum <= result <= maximum:
        invalid_payload(name, f"must be between {minimum} and {maximum}.")
    return result


def sequence(value: object, name: str, maximum: int = MAX_PAGE) -> list[Any]:
    """Copy a bounded array, accepting the local tuple spelling too."""
    if not isinstance(value, (list, tuple)) or len(value) > maximum:
        invalid_payload(name, f"must be an array of at most {maximum} entries.")
    return list(value)


def identities(
    payload: Payload, name: str = "entity_ids", *, allow_zero: bool = False
) -> list[int]:
    """Validate every identity, retaining order and duplicate positions."""
    values = sequence(payload.get(name), name)
    return [
        0
        if allow_zero and type(item) is int and item == 0
        else identity({name: item}, name)
        for item in values
    ]


def mapping(value: object, name: str) -> dict[str, Any]:
    """Require a string-keyed object without interpreting its domain fields."""
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        invalid_payload(name, "must be an object with string keys.")
    return dict(value)


def page(payload: Payload, default: int = 100) -> dict[str, int]:
    """Validate a bounded page; pagination does not promise a retained snapshot."""
    offset = integer_value(payload.get("offset", 0), "offset")
    if offset < 0:
        invalid_payload("offset", "must not be negative.")
    return {
        "limit": bounded(payload.get("limit", default), "limit", MAX_PAGE),
        "offset": offset,
    }


def entities(api: MetadataAPIInterface, value: WemiLevel) -> Any:
    """Select heterogeneous native containers from a closed service map."""
    return {
        "work": api.works,
        "expression": api.expressions,
        "manifestation": api.manifestations,
        "item": api.items,
    }[value]


def shared(api: MetadataAPIInterface, kind: object, *, mutable: bool = False) -> Any:
    """Select a declared record service; language constants remain read-only."""
    selected = choice(kind, "kind", SHARED_KINDS)
    if mutable and selected == "languages":
        invalid_payload("kind", "languages are immutable constants.")
    return {
        "agents": api.agents,
        "tags": api.tags,
        "labels": api.labels,
        "genres": api.genres,
        "subjects": api.subjects,
        "series": api.series,
        "languages": api.languages,
        "ratings": api.ratings,
        "identifiers": api.identifiers,
        "item_identifiers": api.item_identifiers,
        "notes": api.notes,
        "comments": api.comments,
        "synopses": api.synopses,
        "annotations": api.annotations,
    }[selected]


def revision(value: object) -> MetadataEditRevision:
    """Decode either revision family without coercing identities or digest values."""
    raw = mapping(value, "expected_revision")
    ref = mapping(raw.get("reference"), "reference")
    if (
        set(raw) != {"reference", "digest"}
        or not isinstance(raw.get("digest"), str)
        or not raw["digest"]
    ):
        invalid_payload(
            "expected_revision", "requires reference and a nonempty digest."
        )
    if set(ref) == {"level", "entity_id"}:
        return MetadataRevision(
            MetadataReference(level(ref), identity(ref)), raw["digest"]
        )
    if set(ref) == {"kind", "entity_id"}:
        kind = cast(SharedMetadataKind, choice(ref["kind"], "kind", SHARED_KINDS))
        if kind == "languages":
            invalid_payload("kind", "languages have no edit revisions.")
        return SharedMetadataRevision(
            SharedMetadataReference(kind, identity(ref)), raw["digest"]
        )
    invalid_payload("reference", "requires an entity_id and exactly one level or kind.")


def expected(payload: Payload, *, shared_record: bool = False) -> Any:
    """Decode an optional edit token and reject the wrong reference family."""
    value = payload.get("expected_revision")
    if value is None:
        return None
    token = revision(value)
    target = SharedMetadataRevision if shared_record else MetadataRevision
    if not isinstance(token, target):
        invalid_payload("expected_revision", "belongs to the wrong record family.")
    return token


def copy_input(value: Any) -> Any:
    """Detach bounded JSON input and reject cycles, opaque objects and nonfinite values."""
    remaining = MAX_INPUT_VALUES

    def visit(item: Any, depth: int) -> Any:
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > MAX_DEPTH:
            invalid_payload("payload", "exceeds the native metadata structure budget.")
        if item is None or type(item) in (str, int, bool):
            return item
        if type(item) is float and math.isfinite(item):
            return item
        if isinstance(item, Mapping):
            return {
                key: visit(child, depth + 1)
                for key, child in mapping(item, "payload").items()
            }
        if isinstance(item, (list, tuple)):
            return [visit(child, depth + 1) for child in item]
        invalid_payload("payload", "must contain only finite JSON values.")

    return visit(value, 0)


@dataclass(frozen=True)
class Route:
    """Bind one named metadata operation and its explicit transport declaration."""

    name: str
    write: bool
    summary: str
    handler: Handler
    fields: tuple[CorePayloadFieldDescription, ...]

    def validate(self, payload: Payload) -> None:
        """Reject unknown controls and validate declared primitive fields before effects."""
        validate_payload(payload, self.fields)
        unknown = set(payload) - {field.name for field in self.fields}
        if unknown:
            invalid_payload(
                "payload", f"contains unknown fields: {', '.join(sorted(unknown))}."
            )

    def execute(self, api: MetadataAPIInterface, payload: Payload) -> Any:
        """Run a checked operation against the already bound metadata authority."""
        self.validate(payload)
        return self.handler(api, payload)


def route(
    name: str, write: bool, summary: str, handler: Handler, *declarations: str
) -> Route:
    """Build static declarations; a trailing ! marks a required payload field."""
    fields = []
    for declaration in declarations:
        name_part, kind = declaration.split(":", 1)
        fields.append(
            CorePayloadFieldDescription(
                name_part.rstrip("!"), required=name_part.endswith("!"), field_type=kind
            )
        )
    return Route(name, write, summary, handler, tuple(fields))
