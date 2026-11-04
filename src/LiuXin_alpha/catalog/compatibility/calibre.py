"""Project Expression-centred books and resolve compatibility writes through Catalog.

Reads may select a preferred path. Writes never infer a Work from a preferred
path when an Expression has several Work parents. All SQL and transaction
mechanics remain in portable database macros; this module owns field meaning.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.catalog.api.common import CatalogMutationError
from LiuXin_alpha.catalog.relationships import CatalogRelationships
from LiuXin_alpha.catalog.retrieval.selection import WemiPath
from LiuXin_alpha.databases.db_types.macro_types import LinkValue
from LiuXin_alpha.metadata.calibre_conversion import (
    DEFAULT_CALIBRE_CONVERSION_POLICY,
    CalibreBookRef,
    CalibreConversionPolicy,
)
from LiuXin_alpha.metadata.title_names import title_to_title_sort


class CalibreCompatibility:
    """Apply one named policy without changing source, SQL or web ID contracts.

    Example:
        ``catalog.calibre.write_field("title", {CalibreBookRef(13): "Translation"})``
        changes an Expression title override, leaving the Work title unchanged.
    """

    def __init__(
        self,
        db: Any,
        repositories: Any,
        *,
        policy: CalibreConversionPolicy = DEFAULT_CALIBRE_CONVERSION_POLICY,
    ) -> None:
        """Borrow the database and repositories; reject unimplemented profiles."""
        if not isinstance(policy, CalibreConversionPolicy):
            raise TypeError("policy must be a CalibreConversionPolicy")
        self.db = db
        self.repositories = repositories
        self.policy = policy
        self._relationships = CatalogRelationships(db)

    @staticmethod
    def _reference(reference: CalibreBookRef | str) -> CalibreBookRef:
        if isinstance(reference, CalibreBookRef):
            return reference
        if isinstance(reference, str):
            return CalibreBookRef.from_sql_ray(reference)
        raise TypeError(
            "ambiguous book ID: use CalibreBookRef(expression_id=...) or a SQL W:E:M ray; "
            "source Calibre IDs and web Work IDs require an explicit mapping"
        )

    def _context(self, reference: CalibreBookRef | str):
        ref = self._reference(reference)
        expression = self.repositories.expressions.require(ref.expression_id)
        works = self.repositories.expressions.list_works(ref.expression_id)
        manifestations = self.repositories.manifestations.list_for_expression(
            ref.expression_id
        )
        for selected, rows, column in (
            (ref.work_id, works, "work_id"),
            (ref.manifestation_id, manifestations, "manifestation_id"),
        ):
            if selected is not None and not any(
                row[column] == selected for row in rows
            ):
                raise CatalogMutationError(
                    f"{column}={selected} is not linked to Expression {ref.expression_id}"
                )
        return ref, expression, works, manifestations

    def _target(self, field, context) -> tuple[str, int]:
        ref, _, works, _ = context
        owner = self.policy.owner(field)
        if owner == "expression":
            return owner, ref.expression_id
        work_ids = {row["work_id"] for row in works}
        work_id = ref.work_id
        if work_id is None:
            if len(work_ids) != 1:
                raise CatalogMutationError(
                    f"{field} writes a shared Work field; select work_id explicitly "
                    "when the Expression has zero or several Work parents"
                )
            work_id = next(iter(work_ids))
        return owner, work_id

    def resolve_field_target(
        self, field: str, reference: CalibreBookRef | str
    ) -> tuple[str, int]:
        """Return the live field owner without writing or choosing ambiguous parents."""
        self.policy.owner(field)
        return self._target(field, self._context(reference))

    def project(self, reference: CalibreBookRef | str) -> dict[str, object]:
        """Read one Expression projection, retaining all parent/edition IDs.

        Explicit context is honored; otherwise reuse Catalog's preferred WEM
        path ranking for display. The selected path is returned separately from
        the input reference, so a display preference is not a write instruction.
        This bounded projection covers the six writable fields, not files,
        covers, all identifiers or a lossless external Calibre record. Reads
        are not an atomic snapshot.
        """
        ref, expression, works, manifestations = self._context(reference)
        candidates_w = [row for row in works if ref.work_id in (None, row["work_id"])]
        candidates_m = [
            row
            for row in manifestations
            if ref.manifestation_id in (None, row["manifestation_id"])
        ]
        path = min(
            (
                WemiPath(work, expression, manifestation, work, manifestation)
                for work in candidates_w or (None,)
                for manifestation in candidates_m or (None,)
            ),
            key=WemiPath.selection_key,
        )
        work = path.work or {}
        manifestation = path.manifestation or {}
        comments = self.repositories.comments.list_for_wemi(
            level="expression", entity_id=ref.expression_id
        )
        title = next(
            (
                value.strip()
                for value in (
                    expression.get("expression_title_override"),
                    expression.get("expression_subtitle"),
                    work.get("work_title"),
                    work.get("work_canonical_title"),
                )
                if isinstance(value, str) and value.strip()
            ),
            f"Untitled expression {ref.expression_id}",
        )
        ratings = () if not work else self._policy_ratings(work["work_id"])
        if len(ratings) > 1:
            raise CatalogMutationError(
                "multiple ratings exist for this policy and Work"
            )
        rating = (
            None
            if not ratings
            else self.repositories.ratings.require(ratings[0].secondary_id)
        )
        return {
            "policy": self.policy.value,
            "reference": ref,
            "expression_id": ref.expression_id,
            "work_id": work.get("work_id"),
            "manifestation_id": manifestation.get("manifestation_id"),
            "work_ids": tuple(sorted({row["work_id"] for row in works})),
            "manifestation_ids": tuple(
                sorted({row["manifestation_id"] for row in manifestations})
            ),
            "title": title,
            "title_sort": title_to_title_sort(title),
            "author_sort": work.get("work_creator_sort"),
            "language": expression.get("expression_language_id"),
            "last_modified": expression.get("expression_modified_timestamp_ep_k"),
            "rating": 0 if rating is None else rating["rating"],
            "comments": "\n\n".join(
                str(row["comment"])
                for row in comments
                if row.get("comment") is not None
            ),
        }

    def write_field(
        self, field: str, values: Mapping[CalibreBookRef | str, object]
    ) -> dict[CalibreBookRef | str, int | None]:
        """Atomically write one field after resolving and validating the whole batch.

        Identical writes through aliases coalesce. Conflicting values for the
        same canonical owner reject the entire batch before any writes. Results
        retain input keys: scalar writes return owner IDs; comments/ratings
        return associated row IDs, or None on clear. Callers must refresh every
        cached projection of affected owners, including other edition rays.
        """
        self.policy.owner(field)
        if not values:
            return {}
        with self.db.macros.transaction():
            plans = {}
            aliases = {}
            for key, value in values.items():
                prepared = self.policy.prepare(field, value)
                target = self._target(field, self._context(key))
                if field == "language" and prepared is not None:
                    self.repositories.languages.require(prepared)
                if target in plans and plans[target] != prepared:
                    raise CatalogMutationError(
                        f"conflicting {field} values for {target[0]} {target[1]}"
                    )
                plans[target] = prepared
                aliases[key] = target
            written = {
                target: self._write_one(field, target, value)
                for target, value in plans.items()
            }
            return {key: written[target] for key, target in aliases.items()}

    def _write_one(self, field, target, value):
        level, owner_id = target
        if field == "comments":
            return self.repositories.comments.replace_for_wemi(
                level=level,
                entity_id=owner_id,
                data=None if value is None else {"text": value},
            )
        if field == "rating":
            return self._replace_rating(owner_id, value)
        columns = {
            "title": "expression_title_override",
            "language": "expression_language_id",
            "last_modified": "expression_modified_timestamp_ep_k",
            "author_sort": "work_creator_sort",
        }
        repository = getattr(self.repositories, f"{level}s")
        repository.update(owner_id, {columns[field]: value})
        return owner_id

    @property
    def _rating_link_type(self) -> str:
        return f"calibre:{self.policy.value}"

    def _rating_spec(self):
        return self.db.driver_wrapper.get_link_spec("works", "ratings")

    def _policy_ratings(self, work_id):
        return tuple(
            row
            for row in self.db.macros.get_link_rows(self._rating_spec(), work_id)
            if row.link_type == self._rating_link_type
        )

    def _replace_rating(self, work_id, value):
        spec = self._rating_spec()
        links = self.db.macros.get_link_rows(spec, work_id)
        own = [row for row in links if row.link_type == self._rating_link_type]
        if value is not None and len(own) == 1:
            current = self.repositories.ratings.require(own[0].secondary_id)
            if (current["rating"], current["rating_out_of"]) == (value, 10):
                return current["rating_id"]
        id_column = self.db.driver_wrapper.get_id_column(spec.link_table)
        self.db.macros.replace_links(
            spec,
            work_id,
            tuple(
                LinkValue(
                    row.secondary_id,
                    link_type=row.link_type,
                    priority=row.priority,
                    extra={
                        key: val for key, val in row.extra.items() if key != id_column
                    },
                )
                for row in links
                if row.link_type != self._rating_link_type
            ),
        )
        if value is None:
            return None
        rating_id = self.repositories.ratings.create(
            {
                "value": value,
                "out_of": 10,
                "source": self._rating_link_type,
            }
        )
        self._relationships.link(
            "works", work_id, "ratings", rating_id, link_type=self._rating_link_type
        )
        return rating_id


__all__ = ["CalibreCompatibility"]
