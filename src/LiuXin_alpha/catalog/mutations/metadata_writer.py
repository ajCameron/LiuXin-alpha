"""
Coordinate WEMI graph writes, metadata replacement and selected merge transfers.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from typing import Any, cast

from LiuXin_alpha.databases.macro_types import LinkValue

from ..api.common import (
    CatalogMutationError,
    CreatedWemiStack,
    DatabaseHandle,
    EntityId,
    IdentifierCandidate,
    MetadataCandidate,
    RowInput,
    RowMapping,
    WemiLevel,
)
from ..repositories.base import WEMI_TABLES
from .mutation_policy import MutationPolicy


class MetadataWriter:
    """
    Coordinate WEMI creation, links and selected metadata attachment/merge operations.

    Explicit graph/replacement methods require macro transactions. Attachment
    and merge methods also support legacy handles without a callable transaction,
    where atomicity is not guaranteed. Policy checks are preliminary rather than
    a complete validation of all repository payloads.

    Example:
        Attach a title and role-bearing Agent mapping through
        ``catalog.mutations.writer.attach_metadata`` for coordinated repository writes.
    """

    def __init__(self, db: DatabaseHandle, repositories: Any, policy: MutationPolicy) -> None:
        """
        Retain database, repositories and policy without performing reads.

        Example:
            CatalogMutations injects its own policy into the writer for shared decisions.


        :param db: Borrowed database handle.
        :param repositories: Semantic repository group.
        :param policy: Policy object used by attachment, creation and merge checks.
        :return: None; stores the supplied references.
        """

        self.db = db
        self.repositories = repositories
        self.policy = policy

    def create_wemi_stack(
        self,
        *,
        work: RowInput,
        expression: RowInput,
        manifestation: RowInput,
        items: Sequence[RowInput] = (),
        origin: str | None = None,
        work_id: EntityId | None = None,
    ) -> CreatedWemiStack:
        """
        Create a linked WEMI path inside a macro transaction.

        Validate work_id, look up its current row, check payloads/items and origin
        before the transaction. Existing-row lookup is not protected by that later
        transaction. Create/update the Work, then preferred priority-zero graph
        links and Items. Item manifestation_id takes precedence over
        item_manifestation_id; the selected value must be None or the new ID, and
        output always uses the new Manifestation. Late failures roll back through
        the macro transaction. Replaced links do not delete former descendant rows.

        Example:
            Supplying work_id replaces that Work's Expression links with the new preferred Expression.


        :param work: Work creation/update payload; empty permitted only for an existing work_id.
        :param expression: Nonempty Expression mapping to create.
        :param manifestation: Nonempty Manifestation mapping to create.
        :param items: Sequence of nonempty Item mappings, shallow-copied before mutation.
        :param origin: Optional string provenance required to fit both graph-link schemas.
        :param work_id: Optional nonnegative integer, excluding bool; reuse/update or explicitly insert this ID.
        :return: CreatedWemiStack containing Work, Expression, Manifestation and Item IDs.
        :raises TypeError: work_id, items container or origin has an invalid type/value.
        :raises CatalogMutationError: Required data is empty, an Item targets another Manifestation,
            an explicit Work ID is not preserved, or link metadata cannot be represented.
        """

        if work_id is not None and (
            not isinstance(work_id, int) or isinstance(work_id, bool) or work_id < 0
        ):
            raise TypeError("work_id must be a non-negative integer or None")
        existing_work = (
            None if work_id is None else self.repositories.works.get(work_id)
        )
        payloads = {
            "expression": expression,
            "manifestation": manifestation,
        }
        for level, payload in payloads.items():
            if not self.policy.can_create(level=cast(WemiLevel, level), data=payload):
                raise CatalogMutationError(
                    f"WEMI stack requires a non-empty {level} payload"
                )
        if not self.policy.can_create(level="work", data=work) and existing_work is None:
            raise CatalogMutationError(
                "WEMI stack requires a non-empty work payload"
            )
        raw_items: object = items
        if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
            raise TypeError("items must be a sequence of mappings")
        item_payloads: list[dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, Mapping) or not item:
                raise CatalogMutationError("every WEMI stack Item must be a non-empty mapping")
            item_payloads.append(dict(item))
        if origin is not None and not isinstance(origin, str):
            raise TypeError("origin must be a string or None")
        with self.db.macros.transaction():
            if work_id is None:
                created_work_id = self.repositories.works.create(work)
            elif existing_work is None:
                payload = self.repositories.works.normalise_input(work)
                payload["work_id"] = work_id
                inserted_work_id = self.db.macros.insert_row(
                    "works",
                    payload,
                    id_column="work_id",
                )
                if inserted_work_id != work_id:
                    raise CatalogMutationError(
                        "database did not preserve the requested Work ID"
                    )
                created_work_id = work_id
            elif work:
                self.repositories.works.update(work_id, work)
                created_work_id = work_id
            else:
                created_work_id = work_id
            expression_id = self.repositories.expressions.create(expression)
            work_expression_extra = self._wemi_link_extra(
                "works",
                "expressions",
                primary=True,
                origin=origin,
            )
            if work_id is None:
                self.repositories.works._link(
                    "works",
                    created_work_id,
                    "expressions",
                    expression_id,
                    priority=0,
                    extra=work_expression_extra,
                )
            else:
                work_expression_spec = self.repositories.works._link_spec(
                    "works",
                    "expressions",
                )
                self.db.macros.replace_links(
                    work_expression_spec,
                    created_work_id,
                    (
                        LinkValue(
                            expression_id,
                            priority=0,
                            extra=work_expression_extra,
                        ),
                    ),
                )
            manifestation_id = self.repositories.manifestations.create(manifestation)
            self.repositories.expressions._link(
                "expressions",
                expression_id,
                "manifestations",
                manifestation_id,
                priority=0,
                extra=self._wemi_link_extra(
                    "expressions",
                    "manifestations",
                    primary=True,
                    origin=origin,
                ),
            )
            item_ids: list[EntityId] = []
            for item_payload in item_payloads:
                supplied_manifestation = item_payload.get(
                    "manifestation_id",
                    item_payload.get("item_manifestation_id"),
                )
                if supplied_manifestation not in (None, manifestation_id):
                    raise CatalogMutationError(
                        "WEMI stack Item cannot target a different Manifestation"
                    )
                item_payload.pop("manifestation_id", None)
                item_payload["item_manifestation_id"] = manifestation_id
                item_ids.append(self.repositories.items.create(item_payload))
        return CreatedWemiStack(
            work_id=created_work_id,
            expression_id=expression_id,
            manifestation_id=manifestation_id,
            item_ids=tuple(item_ids),
        )

    def _wemi_link_extra(
        self,
        primary_table: str,
        secondary_table: str,
        *,
        primary: bool,
        origin: str | None,
    ) -> dict[str, object]:
        """
        Resolve writable primary/origin columns from a WEMI link specification.

        Suffix matching selects from non-primary-key extra columns; the schema is
        expected to provide unambiguous _primary and _origin names.

        Example:
            An origin column is required only when origin is not None.


        :param primary_table: Parent table name.
        :param secondary_table: Child table name.
        :param primary: Primary flag converted with int().
        :param origin: Optional origin text; None omits the field.
        :return: Mapping from discovered column names to requested values.
        :raises CatalogMutationError: A required marker column is missing or the link spec is unavailable.
        """

        spec = self.repositories.works._link_spec(primary_table, secondary_table)
        writable = {
            column.name
            for column in spec.extra_link_columns
            if not column.is_primary_key
        }
        result: dict[str, object] = {}
        primary_column = next(
            (name for name in writable if name.endswith("_primary")),
            None,
        )
        origin_column = next(
            (name for name in writable if name.endswith("_origin")),
            None,
        )
        if primary_column is None:
            raise CatalogMutationError(
                f"{primary_table}-to-{secondary_table} link has no primary marker"
            )
        result[primary_column] = int(primary)
        if origin is not None:
            if origin_column is None:
                raise CatalogMutationError(
                    f"{primary_table}-to-{secondary_table} link has no origin field"
                )
            result[origin_column] = origin
        return result

    @staticmethod
    def _validate_wemi_edge(
        parent_level: WemiLevel,
        child_level: WemiLevel,
    ) -> tuple[str, str]:
        """
        Map an adjacent downward WEMI pair to its storage tables.

        Example:
            >>> MetadataWriter._validate_wemi_edge("work", "expression")
            ('works', 'expressions')


        :param parent_level: Work, Expression or Manifestation parent level.
        :param child_level: Immediate child level.
        :return: Parent/child table-name tuple.
        :raises CatalogMutationError: The level pair is not an adjacent downward edge.
        """

        pairs = {
            ("work", "expression"): ("works", "expressions"),
            ("expression", "manifestation"): (
                "expressions",
                "manifestations",
            ),
            ("manifestation", "item"): ("manifestations", "items"),
        }
        try:
            return pairs[(parent_level, child_level)]
        except KeyError as error:
            raise CatalogMutationError(
                "WEMI relationships must join adjacent parent/child levels"
            ) from error

    def link_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: EntityId,
        child_level: WemiLevel,
        child_id: EntityId,
        primary: bool | None = None,
        priority: int | None = None,
        origin: str | None = None,
    ) -> Mapping[str, object]:
        """
        Link existing adjacent entities and reconcile primary status transactionally.

        Validate level pair and optional metadata before opening a transaction;
        require both endpoints inside it. For link tables, a new primary demotes
        other primary children of this parent. For Items, primary=False is invalid;
        the foreign key may reassign ownership from another Manifestation.

        Example:
            Linking an Item changes its sole Manifestation foreign key; priority/origin are unsupported there.


        :param parent_level: Parent WEMI level.
        :param parent_id: Existing parent ID, validated by its repository inside the transaction.
        :param child_level: Immediate child WEMI level.
        :param child_id: Existing child ID, validated by its repository inside the transaction.
        :param primary: True demotes sibling links; None preserves an existing flag or selects a first primary.
        :param priority: Optional integer priority, excluding bool; negative values are accepted locally.
        :param origin: Optional string origin; None omits an explicit origin update.
        :return: Receipt with parent/child levels and IDs plus authoritative link metadata.
        :raises TypeError: Optional primary, priority or origin has an invalid type.
        :raises CatalogMutationError: Levels, Item link metadata or required marker columns are unsupported.
        """

        parent_table, child_table = self._validate_wemi_edge(
            parent_level,
            child_level,
        )
        if primary is not None and not isinstance(primary, bool):
            raise TypeError("primary must be a boolean or None")
        if priority is not None and (
            not isinstance(priority, int) or isinstance(priority, bool)
        ):
            raise TypeError("priority must be an integer or None")
        if origin is not None and not isinstance(origin, str):
            raise TypeError("origin must be a string or None")
        repository = self.repositories.works
        with self.db.macros.transaction():
            repository._require_table_row(parent_table, parent_id)
            repository._require_table_row(child_table, child_id)
            if child_level == "item":
                if primary is False:
                    raise CatalogMutationError(
                        "an Item's sole Manifestation relationship is primary"
                    )
                if priority is not None or origin is not None:
                    raise CatalogMutationError(
                        "Manifestation-to-Item ownership has no link metadata"
                    )
                self.repositories.items.update(
                    child_id,
                    {"item_manifestation_id": parent_id},
                )
                return {
                    "parent_level": parent_level,
                    "parent_id": parent_id,
                    "child_level": child_level,
                    "child_id": child_id,
                    "link": {"storage": "foreign_key", "primary": True},
                }

            spec = repository._link_spec(parent_table, child_table)
            rows = self.db.macros.get_link_rows(spec, parent_id)
            writable = {
                column.name
                for column in spec.extra_link_columns
                if not column.is_primary_key
            }
            primary_column = next(
                (name for name in writable if name.endswith("_primary")),
                None,
            )
            if primary_column is None:
                raise CatalogMutationError(
                    f"{parent_table}-to-{child_table} link has no primary marker"
                )
            existing = next(
                (row for row in rows if row.secondary_id == child_id),
                None,
            )
            if primary is None:
                if existing is not None and primary_column in existing.extra:
                    primary = bool(existing.extra[primary_column])
                else:
                    primary = not any(
                        bool(row.extra.get(primary_column)) for row in rows
                    )
            if primary:
                for row in rows:
                    if row.secondary_id == child_id:
                        continue
                    if row.extra.get(primary_column):
                        self.db.macros.upsert_link(
                            spec,
                            parent_id,
                            LinkValue(
                                row.secondary_id,
                                link_type=row.link_type,
                                priority=row.priority,
                                extra={primary_column: 0},
                            ),
                        )
            link = repository._link(
                parent_table,
                parent_id,
                child_table,
                child_id,
                priority=priority,
                extra=self._wemi_link_extra(
                    parent_table,
                    child_table,
                    primary=primary,
                    origin=origin,
                ),
            )
            return {
                "parent_level": parent_level,
                "parent_id": parent_id,
                "child_level": child_level,
                "child_id": child_id,
                "link": repository._link_metadata(link),
            }

    def unlink_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: EntityId,
        child_level: WemiLevel,
        child_id: EntityId,
    ) -> bool:
        """
        Remove an adjacent relationship while retaining both entity rows.

        Require both endpoints inside the macro transaction, even when unrelated.
        Item ownership is cleared only if its foreign key matches the parent. Other
        relations are replaced with the remaining links and writable extra fields.
        Failure to discover the link ID column is ignored.

        Example:
            Removing a primary Expression link does not promote a remaining sibling.


        :param parent_level: Parent WEMI level.
        :param parent_id: Existing parent ID, validated by its repository inside the transaction.
        :param child_level: Immediate child WEMI level.
        :param child_id: Existing child ID, validated by its repository inside the transaction.
        :return: True when the relation was removed; False when absent.
        :raises CatalogMutationError: The pair is not an adjacent downward WEMI relationship.
        """

        parent_table, child_table = self._validate_wemi_edge(
            parent_level,
            child_level,
        )
        repository = self.repositories.works
        with self.db.macros.transaction():
            repository._require_table_row(parent_table, parent_id)
            repository._require_table_row(child_table, child_id)
            if child_level == "item":
                item = self.repositories.items.require(child_id)
                if item.get("item_manifestation_id") != parent_id:
                    return False
                self.repositories.items.update(
                    child_id,
                    {"item_manifestation_id": None},
                )
                return True

            spec = repository._link_spec(parent_table, child_table)
            rows = self.db.macros.get_link_rows(spec, parent_id)
            if not any(row.secondary_id == child_id for row in rows):
                return False
            writable = {
                column.name
                for column in spec.extra_link_columns
                if not column.is_primary_key
            }
            try:
                writable.discard(
                    repository._wrapper.get_id_column(spec.link_table)
                )
            except Exception:
                pass
            desired = [
                LinkValue(
                    row.secondary_id,
                    link_type=row.link_type,
                    priority=row.priority,
                    extra={
                        key: value
                        for key, value in row.extra.items()
                        if key in writable
                    },
                )
                for row in rows
                if row.secondary_id != child_id
            ]
            self.db.macros.replace_links(spec, parent_id, desired)
            return True

    def attach_metadata(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> None:
        """
        Apply direct fields and attachment groups using an available macro transaction.

        Reserve fields, title/titles, agents, identifiers and notes. Apply direct
        fields first, then titles, Agents, identifiers and Notes; title precedes
        entries in titles. If macros.transaction is callable, all writes use it;
        otherwise the null context permits partial effects. Preflight checks shapes
        and selected existing IDs, but deeper normalization may fail after writes.

        Example:
            An explicit fields mapping overrides same-named unreserved top-level fields.


        :param level: WEMI level selecting the semantic repository.
        :param entity_id: Existing entity ID.
        :param data: Nonempty mapping of direct fields and semantic attachment groups.
        :return: None; updates the entity and selected attachments.
        :raises CatalogMutationError: Policy or an attachment shape/value is rejected.
        """

        transaction = getattr(self.db.macros, "transaction", None)
        context = transaction() if callable(transaction) else nullcontext()
        with context:
            self._attach_metadata(level=level, entity_id=entity_id, data=data)

    def _attach_metadata(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> None:

        """
        Perform ordered attachment writes without opening a transaction.

        Copy the top-level mapping. Identifier creation uses identifier_type before
        scheme, source before provenance, and validates string values late in the
        write sequence. Identifier IDs link existing records; Note/title strings
        become single-column mappings. Call attach_metadata for transaction handling.

        Example:
            An Agent mapping needs a role; agent_id reuses a Row, otherwise data or
            remaining non-role/priority fields feed match_or_create.


        :param level: WEMI level selecting the semantic repository.
        :param entity_id: Existing entity ID.
        :param data: Nonempty mapping of direct fields and semantic attachment groups.
        :return: None; mutates direct fields and attachments.
        :raises CatalogMutationError: Policy, fields mapping, group shapes or identifier values are rejected.
        """

        if not self.policy.can_update(level=level, entity_id=entity_id, data=data):
            raise CatalogMutationError(f"metadata attachment rejected for {level}:{entity_id}")
        payload = dict(data)
        reserved = {"fields", "title", "titles", "agents", "identifiers", "notes"}
        direct = {key: value for key, value in payload.items() if key not in reserved}
        fields = payload.get("fields", {})
        if not isinstance(fields, Mapping):
            raise CatalogMutationError("fields must be a mapping")
        direct.update(fields)

        title_values = self._as_sequence(payload.get("titles", ()))
        if "title" in payload:
            title_values = (payload["title"], *title_values)
        agent_values = self._as_sequence(payload.get("agents", ()))
        identifier_values = self._as_sequence(payload.get("identifiers", ()))
        note_values = self._as_sequence(payload.get("notes", ()))
        self._preflight_attachments(
            level=level,
            entity_id=entity_id,
            titles=title_values,
            agents=agent_values,
            identifiers=identifier_values,
            notes=note_values,
        )

        repository = getattr(self.repositories, f"{level}s")
        if direct:
            repository.update(entity_id, direct)
        for title in title_values:
            title_data = (
                {"title": title}
                if isinstance(title, str)
                else dict(cast(Mapping[str, Any], title))
            )
            self.repositories.titles.add_for_wemi(
                level=level,
                entity_id=entity_id,
                data=title_data,
            )
        for value in agent_values:
            record = dict(cast(Mapping[str, Any], value))
            agent_id = record.get("agent_id")
            if agent_id is None:
                agent_data = record.get("data")
                if agent_data is None:
                    agent_data = {
                        key: item
                        for key, item in record.items()
                        if key not in {"role", "priority"}
                    }
                agent_id = self.repositories.agents.match_or_create(
                    MetadataCandidate(agent_data)
                )
            self.repositories.agents.link_to_wemi(
                agent_id=agent_id,
                level=level,
                entity_id=entity_id,
                role=record["role"],
                priority=record.get("priority"),
            )
        for value in identifier_values:
            record = dict(cast(Mapping[str, Any], value))
            identifier_id = record.get("identifier_id")
            if identifier_id is None:
                identifier_type = record.get(
                    "identifier_type",
                    record.get("scheme"),
                )
                identifier_value = record.get("value")
                normalised_value = record.get("normalised_value")
                source = record.get("source", record.get("provenance"))
                if not isinstance(identifier_type, str) or not identifier_type:
                    raise CatalogMutationError(
                        "identifier scheme/type must be a non-empty string"
                    )
                if not isinstance(identifier_value, str) or not identifier_value:
                    raise CatalogMutationError(
                        "identifier value must be a non-empty string"
                    )
                if normalised_value is not None and not isinstance(
                    normalised_value,
                    str,
                ):
                    raise CatalogMutationError(
                        "normalised identifier value must be a string or None"
                    )
                if source is not None and not isinstance(source, str):
                    raise CatalogMutationError(
                        "identifier source must be a string or None"
                    )
                identifier_id = self.repositories.identifiers.match_or_create(
                    IdentifierCandidate(
                        identifier_type=identifier_type,
                        value=identifier_value,
                        normalised_value=normalised_value,
                        source=source,
                    )
                )
            self.repositories.identifiers.link_to_wemi(
                identifier_id=identifier_id,
                level=level,
                entity_id=entity_id,
                priority=record.get("priority"),
            )
        for value in note_values:
            note_data = (
                {"note": value}
                if isinstance(value, str)
                else dict(cast(Mapping[str, Any], value))
            )
            self.repositories.notes.add_for_wemi(
                level=level,
                entity_id=entity_id,
                data=note_data,
            )

    def replace_metadata(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> None:
        """
        Replace explicitly supplied groups inside a macro transaction.

        Unlike attachment, identifiers must be a scheme/value mapping (None means
        empty). Title accepts at most one string/mapping or None to clear. Replace
        title before direct fields so explicit fields win. Agents are deduplicated
        by ID within stripped roles; existing roles absent from input are cleared,
        and supplied priority values are ignored in favor of replacement order.
        Notes, comments and synopses use their repository replacement contracts.
        Policy and some validation happen before the transaction; deeper errors
        inside it roll back earlier group writes.

        Example:
            ``{"agents": [], "identifiers": {}}`` clears those groups while leaving Notes unchanged.


        :param level: WEMI level selecting the semantic repository.
        :param entity_id: Existing entity ID.
        :param data: Nonempty mapping of direct fields and semantic attachment groups.
        :return: None; omitted groups remain unchanged.
        :raises CatalogMutationError: Policy, fields or group shape is rejected.
        """

        if not self.policy.can_update(
            level=level,
            entity_id=entity_id,
            data=data,
        ):
            raise CatalogMutationError(
                f"metadata replacement rejected for {level}:{entity_id}"
            )
        payload = dict(data)
        reserved = {
            "fields",
            "title",
            "titles",
            "agents",
            "identifiers",
            "notes",
            "comments",
            "synopses",
        }
        direct = {
            key: value for key, value in payload.items() if key not in reserved
        }
        fields = payload.get("fields", {})
        if not isinstance(fields, Mapping):
            raise CatalogMutationError("fields must be a mapping")
        direct.update(fields)
        repository = getattr(self.repositories, f"{level}s")
        if direct:
            repository.normalise_input(direct)

        title_supplied = "title" in payload or "titles" in payload
        title_value: object = payload.get("title")
        if "titles" in payload:
            title_values = self._as_sequence(payload["titles"])
            if "title" in payload:
                title_values = (title_value, *title_values)
            if len(title_values) > 1:
                raise CatalogMutationError(
                    "logical WEMI title replacement accepts at most one value"
                )
            title_value = title_values[0] if title_values else None
        if title_supplied and title_value is not None and not isinstance(
            title_value,
            (str, Mapping),
        ):
            raise CatalogMutationError(
                "title replacement must be a string, mapping, or None"
            )

        agent_values = (
            self._as_sequence(payload["agents"])
            if "agents" in payload
            else ()
        )
        if "agents" in payload:
            self._preflight_attachments(
                level=level,
                entity_id=entity_id,
                titles=(),
                agents=agent_values,
                identifiers=(),
                notes=(),
            )

        identifier_values = payload.get("identifiers")
        if "identifiers" in payload:
            if identifier_values is None:
                identifier_values = {}
            if not isinstance(identifier_values, Mapping):
                raise CatalogMutationError(
                    "identifier replacement must be a scheme/value mapping"
                )

        note_values = (
            self._as_sequence(payload["notes"])
            if "notes" in payload
            else ()
        )
        synopsis_values = (
            self._as_sequence(payload["synopses"])
            if "synopses" in payload
            else ()
        )
        comment_value = payload.get("comments")
        if "comments" in payload and comment_value is not None:
            if isinstance(comment_value, str):
                comment_value = {"comment": comment_value}
            elif isinstance(comment_value, Mapping):
                comment_value = dict(comment_value)
            else:
                raise CatalogMutationError(
                    "comments replacement must be text, a mapping, or None"
                )

        with self.db.macros.transaction():
            if title_supplied:
                self.repositories.titles.replace_for_wemi(
                    level=level,
                    entity_id=entity_id,
                    data=cast(RowInput | str | None, title_value),
                )
            # Explicit direct fields are the narrowest instruction and
            # therefore win if they overlap a semantic group (for example a
            # Work canonical title supplied alongside ``title``).
            if direct:
                repository.update(entity_id, direct)
            if "agents" in payload:
                desired_by_role: dict[str, list[EntityId]] = {}
                for value in agent_values:
                    record = dict(cast(Mapping[str, Any], value))
                    role = cast(str, record["role"]).strip()
                    agent_id = record.get("agent_id")
                    if agent_id is None:
                        agent_data = record.get("data")
                        if agent_data is None:
                            agent_data = {
                                key: item
                                for key, item in record.items()
                                if key not in {"role", "priority"}
                            }
                        agent_id = self.repositories.agents.match_or_create(
                            MetadataCandidate(cast(RowMapping, agent_data))
                        )
                    role_ids = desired_by_role.setdefault(role, [])
                    if agent_id not in role_ids:
                        role_ids.append(cast(EntityId, agent_id))
                existing_roles = {
                    link.get("type")
                    for agent in self.repositories.agents.list_for_wemi(
                        level=level,
                        entity_id=entity_id,
                    )
                    if isinstance(
                        link := agent.get("_catalog_link"),
                        Mapping,
                    )
                    and isinstance(link.get("type"), str)
                }
                for role in sorted(
                    cast(set[str], existing_roles) | set(desired_by_role)
                ):
                    self.repositories.agents.replace_for_wemi(
                        level=level,
                        entity_id=entity_id,
                        role=role,
                        agent_ids=desired_by_role.get(role, ()),
                    )
            if "identifiers" in payload:
                self.repositories.identifiers.replace_for_wemi(
                    level=level,
                    entity_id=entity_id,
                    identifiers=cast(Mapping[str, str], identifier_values),
                )
            if "notes" in payload:
                self.repositories.notes.replace_for_wemi(
                    level=level,
                    entity_id=entity_id,
                    notes=cast(Sequence[str | RowInput], note_values),
                )
            if "comments" in payload:
                self.repositories.comments.replace_for_wemi(
                    level=level,
                    entity_id=entity_id,
                    data=cast(RowInput | None, comment_value),
                )
            if "synopses" in payload:
                self.repositories.synopses.replace_for_wemi(
                    level=level,
                    entity_id=entity_id,
                    synopses=cast(
                        Sequence[str | RowInput],
                        synopsis_values,
                    ),
                )

    def merge_entities(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> None:
        """
        Fill missing target fields and transfer selected relationships before deleting the source.

        Use macros.transaction when callable, otherwise a null context. Only
        supported WEMI adjacency, Agent/Note links and curated identifiers are
        explicitly transferred; this is not a complete merge of every metadata
        family. Target nonempty fields and existing link identities win. Unsupported
        link specifications are skipped. Deletion/cascades follow repository policy.

        Example:
            Merging Manifestations reassigns source Items to the target.


        :param level: WEMI level selecting the semantic repository.
        :param source_id: Existing entity to absorb and delete.
        :param target_id: Distinct existing entity to retain.
        :return: None; retains target identity and deletes the source on success.
        :raises CatalogMutationError: Policy rejects level, IDs or entity existence.
        """

        transaction = getattr(self.db.macros, "transaction", None)
        context = transaction() if callable(transaction) else nullcontext()
        with context:
            self._merge_entities(
                level=level,
                source_id=source_id,
                target_id=target_id,
            )

    def _merge_entities(
        self,
        *,
        level: WemiLevel,
        source_id: EntityId,
        target_id: EntityId,
    ) -> None:

        """
        Execute the selected-field/link merge without opening a transaction.

        Require both Rows after policy, fill nonempty source values into empty target
        columns, transfer selected links/identifiers, reassign Manifestation Items,
        then delete the source. Use merge_entities for optional transaction handling.

        Example:
            Item merges have no descendant transfer; foreign-key values are filled only
            when the target field is empty.


        :param level: WEMI level selecting the semantic repository.
        :param source_id: Existing entity to absorb and delete.
        :param target_id: Distinct existing entity to retain.
        :return: None; deletes the source after transfers.
        :raises CatalogMutationError: Policy rejects the merge.
        """

        if not self.policy.can_merge(level=level, source_id=source_id, target_id=target_id):
            raise CatalogMutationError(f"merge rejected for {level}:{source_id}->{target_id}")
        repository = getattr(self.repositories, f"{level}s")
        source = repository.require(source_id)
        target = repository.require(target_id)
        changes = self._missing_target_values(repository, source, target)
        if changes:
            repository.update(target_id, changes)

        table = WEMI_TABLES[level]
        for secondary in self._relationship_targets(level):
            self._transfer_links(table, source_id, target_id, secondary)
        self._transfer_links(table, source_id, target_id, "agents")
        self._transfer_links(table, source_id, target_id, "notes")
        self._transfer_identifiers(level, source_id, target_id)

        if level == "item":
            pass
        elif level == "manifestation":
            for item in self.repositories.items.list_for_manifestation(source_id):
                self.repositories.items.update(
                    item["item_id"],
                    {"item_manifestation_id": target_id},
                )
        repository.delete(source_id)

    @staticmethod
    def _as_sequence(value: object) -> tuple[object, ...]:
        """
        Normalize an attachment group without splitting text or mappings.

        Other iterables, including generators and sets, are rejected.

        Example:
            >>> MetadataWriter._as_sequence("note")
            ('note',)


        :param value: None, string/bytes, mapping or Sequence.
        :return: Empty tuple for None, singleton tuple for text/mapping, otherwise tuple(value).
        :raises CatalogMutationError: The value has no supported group shape.
        """

        if value is None:
            return ()
        if isinstance(value, (str, bytes, Mapping)):
            return (value,)
        if isinstance(value, Sequence):
            return tuple(value)
        raise CatalogMutationError("metadata attachment groups must be sequences")

    def _preflight_attachments(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        titles: tuple[object, ...],
        agents: tuple[object, ...],
        identifiers: tuple[object, ...],
        notes: tuple[object, ...],
    ) -> None:
        """
        Check owner existence, group shapes and explicit Agent/identifier IDs.

        This is partial validation: title/note mappings and candidate metadata are
        not fully normalized here. Identifier scheme/value type validation happens
        later. Whitespace role text is checked but not rewritten.

        Example:
            An agent_id key with value None still triggers an existing-row lookup
            and fails repository validation.


        :param level: WEMI level selecting the semantic repository.
        :param entity_id: Existing attachment owner.
        :param titles: Title values; Items reject any nonempty title group.
        :param agents: Agent mappings, each requiring a nonblank string role.
        :param identifiers: Identifier mappings with an ID key or truthy scheme/type and value.
        :param notes: Note strings or mappings.
        :return: None; performs reads without creating or linking records.
        :raises CatalogMutationError: A group shape, role or required identifier input is missing.
        """

        self.repositories.titles._require_table_row(WEMI_TABLES[level], entity_id)
        if titles and not self.repositories.titles._TITLE_COLUMNS[level]:
            raise CatalogMutationError("Items do not own title columns")
        for title in titles:
            if not isinstance(title, (str, Mapping)):
                raise CatalogMutationError("titles must be strings or mappings")
        for agent in agents:
            if not isinstance(agent, Mapping):
                raise CatalogMutationError("agents must be mappings")
            role = agent.get("role")
            if not isinstance(role, str) or not role.strip():
                raise CatalogMutationError("every Agent attachment requires a role")
            if "agent_id" in agent:
                self.repositories.agents.require(agent["agent_id"])
        for identifier in identifiers:
            if not isinstance(identifier, Mapping):
                raise CatalogMutationError("identifiers must be mappings")
            if "identifier_id" in identifier:
                self.repositories.identifiers.require(identifier["identifier_id"])
            elif not (
                identifier.get("identifier_type", identifier.get("scheme"))
                and identifier.get("value")
            ):
                raise CatalogMutationError(
                    "identifiers require identifier_id or scheme/type and value"
                )
        for note in notes:
            if not isinstance(note, (str, Mapping)):
                raise CatalogMutationError("notes must be strings or mappings")

    @staticmethod
    def _missing_target_values(
        repository: Any,
        source: RowMapping,
        target: RowMapping,
    ) -> dict[str, object]:
        """
        Select nonempty source fields to fill empty target columns.

        Exclude the identity column and names ending _timestamp_ep_k or _scratch.

        Example:
            Whitespace, zero and False count as populated; only None and empty text are treated as missing.


        :param repository: Repository exposing columns and id_column.
        :param source: Source mapping.
        :param target: Target mapping.
        :return: New changes mapping sharing source values.
        """

        return {
            column: source[column]
            for column in repository.columns
            if column != repository.id_column
            and not column.endswith(("_timestamp_ep_k", "_scratch"))
            and target.get(column) in (None, "")
            and source.get(column) not in (None, "")
        }

    @staticmethod
    def _relationship_targets(level: WemiLevel) -> tuple[str, ...]:
        """
        List WEMI link-table families transferred for a merge level.

        Example:
            Manifestation returns expressions; its Items are handled separately as foreign keys.


        :param level: WEMI level selecting the semantic repository.
        :return: Tuple of adjacent table names; Item has none.
        :raises KeyError: Level is unknown.
        """

        return {
            "work": ("expressions",),
            "expression": ("works", "manifestations"),
            "manifestation": ("expressions",),
            "item": (),
        }[level]

    def _transfer_links(
        self,
        table: str,
        source_id: EntityId,
        target_id: EntityId,
        secondary_table: str,
    ) -> None:
        """
        Move supported link identities with target-first collision precedence.

        A CatalogMutationError resolving the spec skips the family. Filter extras
        to writable columns; failure to discover the ID column is ignored. Identity
        includes link type only when the spec requires it. Clear/replace uses an
        available macro transaction, otherwise partial effects are possible.

        Example:
            An existing target link retains its priority/extras when a source link has the same identity.


        :param table: Primary entity table.
        :param source_id: Source entity ID.
        :param target_id: Target entity ID.
        :param secondary_table: Linked table to transfer.
        :return: None; clears source links and replaces target links when source has rows.
        """

        repository = self.repositories.works
        try:
            spec = repository._link_spec(table, secondary_table)
        except CatalogMutationError:
            return
        rows = self.db.macros.get_link_rows(spec, source_id)
        writable_extras = {
            column.name
            for column in spec.extra_link_columns
            if not column.is_primary_key
        }
        try:
            writable_extras.discard(
                repository._wrapper.get_id_column(spec.link_table)
            )
        except Exception:
            pass
        if not rows:
            return
        target_rows = self.db.macros.get_link_rows(spec, target_id)
        desired: dict[tuple[object, ...], LinkValue] = {}
        for row in (*target_rows, *rows):
            identity: tuple[object, ...] = (row.secondary_id,)
            if spec.type_part_of_identity:
                identity += (row.link_type,)
            desired.setdefault(
                identity,
                LinkValue(
                    row.secondary_id,
                    link_type=row.link_type,
                    priority=row.priority,
                    extra={
                        key: value
                        for key, value in row.extra.items()
                        if key in writable_extras
                    },
                ),
            )
        transaction = getattr(self.db.macros, "transaction", None)
        context = transaction() if callable(transaction) else nullcontext()
        with context:
            self.db.macros.replace_links(spec, source_id, ())
            self.db.macros.replace_links(spec, target_id, desired.values())

    def _transfer_identifiers(
        self,
        level: WemiLevel,
        source_id: EntityId,
        target_id: EntityId,
    ) -> None:
        """
        Move curated identifiers and demote source primaries conflicting with the target.

        The target-primary scheme set is captured once and is not updated as rows
        move. Multiple source primaries for a previously absent scheme can remain
        primary. No normalization, deduplication or standalone transaction occurs.

        Example:
            A primary source ISBN becomes nonprimary when the target already has a primary ISBN.


        :param level: WEMI level selecting the semantic repository.
        :param source_id: Source owner ID.
        :param target_id: Target owner ID.
        :return: None; updates source identifier Rows in place.
        """

        target_primary_schemes = {
            row.get("entity_identifier_scheme")
            for row in self.repositories.identifiers.list_for_wemi(
                level=level,
                entity_id=target_id,
            )
            if row.get("entity_identifier_is_primary")
        }
        for row in self.repositories.identifiers.list_for_wemi(
            level=level,
            entity_id=source_id,
        ):
            changes: dict[str, object] = {
                "entity_identifier_entity_id": target_id,
            }
            if (
                row.get("entity_identifier_is_primary")
                and row.get("entity_identifier_scheme") in target_primary_schemes
            ):
                changes["entity_identifier_is_primary"] = 0
            self.repositories.identifiers.update(
                row["entity_identifier_id"],
                changes,
            )
