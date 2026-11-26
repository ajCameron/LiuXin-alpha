"""Own vocabulary forests, structural validation and explicit attachment deletion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from LiuXin_alpha.catalog.api.common import CatalogMutationError, MetadataCandidate
from LiuXin_alpha.catalog.api.metadata_trees import (
    HierarchyIntegrityError,
    HierarchyInUseError,
    HierarchyLimitError,
    VocabularyTreeInput,
)
from LiuXin_alpha.catalog.hierarchy import (
    TREE_SPECS,
    TreeRow,
    VocabularyTreeSpec,
    ancestor_rows,
    node_id,
    node_limit,
    subtree_rows,
)
from LiuXin_alpha.catalog.inspection import BoundedInspection
from LiuXin_alpha.databases.db_types.repair_types import PhysicalReference
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec

from .exact import ExactEntityRepository


class DatabaseHierarchySource:
    """Borrow a repository and the caller's current database read/write unit."""

    def __init__(self, repository: HierarchicalRepository) -> None:
        self.repository = repository

    def get(self, entity_id: int) -> TreeRow | None:
        """Read one persisted node."""
        return self.repository.get(entity_id)

    def children(
        self, parent_id: int | None, *, limit: int, offset: int = 0
    ) -> tuple[TreeRow, ...]:
        """Read a bounded ordered child page using the physical database helper."""
        repo = self.repository
        return repo._macros.get_tree_children_page(
            repo.table_name,
            parent_id,
            position_column=repo.tree_spec.position,
            limit=limit,
            offset=offset,
        )


# The legacy repository base is outside mypy's checked source set.
class HierarchicalRepository(ExactEntityRepository):  # type: ignore[misc]
    """Supply the same hierarchy policy to CRUD, batches and named tree methods.

    All structural writes reserve a serialized macro transaction. Full labels
    and tree IDs remain caller-owned scalar values, never derived path caches.
    Direct physical SQL remains outside this semantic boundary.
    """

    @property
    def tree_spec(self) -> VocabularyTreeSpec:
        """Return the reviewed column declaration for this vocabulary."""
        return TREE_SPECS[self.table_name]

    def _source(self) -> DatabaseHierarchySource:
        return DatabaseHierarchySource(self)

    def _parent(self, value: object) -> int | None:
        return None if value is None else node_id(value)

    def _prepare_write(
        self, data: Mapping[str, Any], *, existing: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        payload = super()._prepare_write(data, existing=existing)
        spec = self.tree_spec
        if spec.parent in payload:
            self._parent(payload[spec.parent])
        if spec.position in payload and payload[spec.position] is not None:
            node_id(payload[spec.position])
        name = payload.get(
            self.match_spec.primary_field,
            (existing or {}).get(self.match_spec.primary_field),
        )
        if not isinstance(name, str) or not name.strip():
            raise CatalogMutationError("Hierarchy nodes require a nonblank name")
        return payload

    def _siblings(self, parent_id: int | None) -> list[TreeRow]:
        """Read siblings in bounded pages for an atomic ordering mutation."""
        result: list[TreeRow] = []
        while True:
            rows = self._source().children(parent_id, limit=1000, offset=len(result))
            result.extend(rows)
            if len(rows) < 1000:
                return result

    def _order(self, identities: Sequence[int]) -> bool:
        """Persist only changed positions, preserving every unrelated field."""
        changed = False
        for position, identity in enumerate(identities):
            if self.require(identity)[self.tree_spec.position] != position:
                self._macros.update_row(
                    self.table_name, identity, {self.tree_spec.position: position}
                )
                changed = True
        return changed

    def _ids(self, rows: Sequence[TreeRow]) -> list[int]:
        return [node_id(row[self.id_column]) for row in rows]

    def _check_parent(self, parent_id: int | None) -> None:
        if parent_id is not None:
            ancestor_rows(self._source(), self.tree_spec, parent_id)

    def ensure(self, data: Mapping[str, Any]) -> tuple[int, bool]:
        """Validate explicit parent scope even when reusing an existing node."""
        with self._macros.transaction(serialized=True):
            normal = self.normalise_input(data)
            if self.tree_spec.parent in normal:
                self._check_parent(self._parent(normal[self.tree_spec.parent]))
            return super().ensure(data)

    def _create(self, data: Mapping[str, Any], *, entity_id: int | None = None) -> int:
        with self._macros.transaction(serialized=True):
            payload = self._prepare_write(data)
            parent = self._parent(payload.get(self.tree_spec.parent))
            self._check_parent(parent)
            if self.table_name == "series":
                match = self.match(
                    MetadataCandidate({"name": payload[self.match_spec.primary_field]})
                )
                if match.is_match:
                    raise CatalogMutationError(
                        "Series names are globally unique; an existing Series cannot be implicitly moved"
                    )
            siblings = self._ids(self._siblings(parent))
            position = payload.get(self.tree_spec.position)
            position = len(siblings) if position is None else node_id(position)
            if position > len(siblings):
                raise ValueError("position exceeds the destination sibling count")
            payload[self.tree_spec.position] = position
            new_id = super()._create(payload, entity_id=entity_id)
            siblings.insert(position, new_id)
            self._order(siblings)
            return new_id

    def _update_many(self, updates: Mapping[int, Mapping[str, Any]]) -> None:
        """Validate the proposed graph before writing any part of a batch."""
        self._edit_many(updates)

    def update_changed(self, entity_id: int, data: Mapping[str, object]) -> bool:
        """Apply one validated edit and report actual scalar/ordering changes."""
        self._validate_entity_id(entity_id)
        return self._edit_many({entity_id: data})

    def _edit_many(self, updates: Mapping[int, Mapping[str, Any]]) -> bool:
        """Apply a prepared final graph and retain whether anything changed."""
        spec = self.tree_spec
        with self._macros.transaction(serialized=True):
            old = {identity: self.require(identity) for identity in updates}
            prepared = {
                identity: self._prepare_write(data, existing=old[identity])
                for identity, data in updates.items()
            }
            proposed = {
                identity: {**old[identity], **data}
                for identity, data in prepared.items()
            }
            # A rename or move must not reach a sibling's name as a raw driver error.
            self._validate_proposed(proposed.items(), old)
            depths: dict[int, int] = {}
            for identity in proposed:
                seen = {identity}
                row = proposed[identity]
                while row[spec.parent] is not None:
                    ancestor_id = node_id(row[spec.parent])
                    if ancestor_id in seen:
                        raise HierarchyIntegrityError(
                            "Update would create a hierarchy cycle"
                        )
                    if len(seen) > 10000:
                        raise HierarchyLimitError("Parent validation exceeds max_nodes")
                    seen.add(ancestor_id)
                    row = (
                        proposed[ancestor_id]
                        if ancestor_id in proposed
                        else self.require(ancestor_id)
                    )
                depths[identity] = len(seen)
            structural = {
                identity
                for identity, data in prepared.items()
                if spec.parent in data or spec.position in data
            }
            groups: dict[int | None, list[int]] = {}
            for identity in structural:
                for row in (old[identity], proposed[identity]):
                    parent = self._parent(row[spec.parent])
                    if parent not in groups:
                        groups[parent] = [
                            i
                            for i in self._ids(self._siblings(parent))
                            if i not in structural
                        ]
            for identity in prepared:
                if identity not in structural:
                    continue
                parent = self._parent(proposed[identity][spec.parent])
                siblings = groups[parent]
                supplied = prepared[identity].get(spec.position)
                if supplied is None and old[identity][spec.parent] == parent:
                    # A parent-only no-op preserves the current relative location.
                    original = self._ids(self._siblings(parent))
                    before = set(original[: original.index(identity)])
                    position = sum(i in before for i in siblings)
                else:
                    position = len(siblings) if supplied is None else node_id(supplied)
                if position > len(siblings):
                    raise ValueError("position exceeds the destination sibling count")
                siblings.insert(position, identity)
            # Parents move before children, so valid final graphs do not transiently cycle.
            changed = False
            for identity in sorted(prepared, key=depths.__getitem__):
                changes = {
                    key: value
                    for key, value in prepared[identity].items()
                    if key != spec.position and old[identity].get(key) != value
                }
                if changes:
                    self._macros.update_row(self.table_name, identity, changes)
                    changed = True
            for siblings in groups.values():
                changed |= self._order(siblings)
            return changed

    def roots(self, *, limit: int = 100, offset: int = 0) -> tuple[TreeRow, ...]:
        """Read one root page in sibling order."""
        self._validate_page(limit, offset)
        with self._macros.read_transaction():
            return self._source().children(None, limit=limit, offset=offset)

    def children(
        self, entity_id: int, *, limit: int = 100, offset: int = 0
    ) -> tuple[TreeRow, ...]:
        """Require a node and read its ordered direct children."""
        self._validate_page(limit, offset)
        with self._macros.read_transaction():
            self.require(entity_id)
            return self._source().children(entity_id, limit=limit, offset=offset)

    def ancestors(
        self, entity_id: int, *, max_nodes: int = 10000
    ) -> tuple[TreeRow, ...]:
        """Read a complete root-to-parent chain without trusting stored labels."""
        with self._macros.read_transaction():
            return ancestor_rows(
                self._source(), self.tree_spec, entity_id, max_nodes=max_nodes
            )

    def subtree(self, entity_id: int, *, max_nodes: int = 10000) -> tuple[TreeRow, ...]:
        """Read one complete root-inclusive subtree in preorder."""
        with self._macros.read_transaction():
            return subtree_rows(
                self._source(), self.tree_spec, entity_id, max_nodes=max_nodes
            )

    def ensure_path(
        self,
        names: Sequence[str],
        *,
        parent_id: int | None = None,
        max_nodes: int = 10000,
    ) -> tuple[tuple[int, ...], bool]:
        """Reuse or create a literal component path under one writer reservation."""
        node_limit(max_nodes)
        if isinstance(names, (str, bytes)) or not names:
            raise ValueError("names must be a nonempty sequence of components")
        components = tuple(names)
        if len(components) > max_nodes:
            raise HierarchyLimitError("Path exceeds max_nodes")
        if any(not isinstance(name, str) or not name.strip() for name in components):
            raise ValueError("Path components must be nonblank strings")
        with self._macros.transaction(serialized=True):
            parent = self._parent(parent_id)
            self._check_parent(parent)
            result: list[int] = []
            changed = False
            for name in components:
                parent, created = self.ensure({"name": name, "parent_id": parent})
                result.append(parent)
                changed |= created
            return tuple(result), changed

    def create_tree(
        self,
        node: VocabularyTreeInput,
        *,
        parent_id: int | None = None,
        max_nodes: int = 10000,
    ) -> tuple[int, ...]:
        """Create a fresh nested tree atomically; reject shared or recursive inputs."""
        node_limit(max_nodes)
        pending = [(node, -1)]
        plan: list[tuple[dict[str, Any], int]] = []
        seen: set[int] = set()
        while pending:
            current, parent_index = pending.pop()
            if not isinstance(current, VocabularyTreeInput):
                raise TypeError("Expected VocabularyTreeInput")
            if id(current) in seen:
                raise ValueError("Tree input contains a repeated or cyclic node")
            seen.add(id(current))
            if len(seen) > max_nodes:
                raise HierarchyLimitError("Tree input exceeds max_nodes")
            data = self.normalise_input(current.data)
            if self.tree_spec.parent in data or self.tree_spec.position in data:
                raise ValueError(
                    "Nested tree payloads cannot specify parent or position"
                )
            index = len(plan)
            plan.append((data, parent_index))
            pending.extend((child, index) for child in reversed(current.children))
        with self._macros.transaction(serialized=True):
            parent = self._parent(parent_id)
            self._check_parent(parent)
            siblings = self._ids(self._siblings(parent))
            self._order(siblings)
            positions: dict[int | None, int] = {parent: len(siblings)}
            created: list[int] = []
            for data, parent_index in plan:
                owner = parent if parent_index == -1 else created[parent_index]
                position = positions.get(owner, 0)
                payload = {
                    **data,
                    self.tree_spec.parent: owner,
                    self.tree_spec.position: position,
                }
                # The validated construction order guarantees existing acyclic parents.
                created.append(super()._create(payload))
                positions[owner] = position + 1
            return tuple(created)

    def move(
        self, entity_id: int, *, parent_id: int | None, before_id: int | None = None
    ) -> bool:
        """Move a whole branch, preserving node identities and metadata links."""
        with self._macros.transaction(serialized=True):
            row = self.require(entity_id)
            parent = self._parent(parent_id)
            self._check_parent(parent)
            destination = [
                i for i in self._ids(self._siblings(parent)) if i != entity_id
            ]
            if before_id is None:
                position = len(destination)
            else:
                node_id(before_id)
                if before_id not in destination:
                    raise ValueError("before_id must be another destination sibling")
                position = destination.index(before_id)
            original = self._ids(
                self._siblings(self._parent(row[self.tree_spec.parent]))
            )
            expected = list(destination)
            expected.insert(position, entity_id)
            if row[self.tree_spec.parent] == parent and original == expected:
                return False
            self._update_many(
                {entity_id: {"parent_id": parent, self.tree_spec.position: position}}
            )
            return True

    def reorder(self, *, parent_id: int | None, entity_ids: Sequence[int]) -> bool:
        """Reorder a complete sibling set, rejecting missing or duplicate members."""
        ids = tuple(node_id(identity) for identity in entity_ids)
        with self._macros.transaction(serialized=True):
            parent = self._parent(parent_id)
            self._check_parent(parent)
            current = self._ids(self._siblings(parent))
            if len(set(ids)) != len(ids) or set(ids) != set(current):
                raise ValueError("reorder requires every current sibling exactly once")
            if tuple(current) == ids:
                return False
            return self._order(ids)

    def _link_specs(self) -> dict[str, StorageLinkSpec]:
        specs: dict[str, StorageLinkSpec] = {}
        for other in {
            self.table_name,
            *self._wrapper.get_interlinked_tables(self.table_name),
        }:
            spec = (
                self._wrapper.get_intralink_spec(other)
                if other == self.table_name
                else self._wrapper.get_link_spec(self.table_name, other)
            )
            if isinstance(spec, StorageLinkSpec):
                specs[spec.link_table] = spec
        return specs

    def _delete_nodes(
        self,
        rows: Sequence[TreeRow],
        *,
        detach: bool,
        inventory: Sequence[PhysicalReference],
    ) -> tuple[int, ...]:
        """Inventory references before removing declared links and then nodes."""
        if type(detach) is not bool:
            raise TypeError("detach must be boolean")
        reader = BoundedInspection(self.db, 100000)
        specs = self._link_specs()
        links: dict[tuple[str, int], TreeRow] = {}
        ids = set(self._ids(rows))
        for reference in inventory:
            if reference["external_schema"] or (
                reference["kind"] == "polymorphic"
                and reference["table"] != "entity_identifiers"
            ):
                raise HierarchyInUseError(
                    "Unsupported reference declaration prevents tree deletion"
                )
            table = reference["table"]
            for row in rows:
                where = {
                    column: row[target]
                    for column, target in zip(
                        reference["columns"], reference["target_columns"], strict=True
                    )
                }
                if reference["kind"] == "polymorphic":
                    type_column = reference.get("type_column")
                    if not type_column:
                        raise HierarchyInUseError(
                            "Polymorphic reference has no type discriminator"
                        )
                    where[type_column] = (
                        "series"
                        if self.table_name == "series"
                        else self.table_name[:-1]
                    )
                for found in reader.rows(table, where):
                    if (
                        table == self.table_name
                        and reference["columns"] == [self.tree_spec.parent]
                        and node_id(found[self.id_column]) in ids
                    ):
                        continue
                    spec = specs.get(table)
                    if spec is None or not detach:
                        raise HierarchyInUseError(
                            "Hierarchy is referenced; declared attachments require detach=True"
                        )
                    identity = node_id(found[self._wrapper.get_id_column(table)])
                    links[(table, identity)] = found
        # Do not let deletion of an attachment cascade into an unreviewed owner.
        link_inventories = {
            table: self._macros.lock_repair_references(table)
            for table in sorted({table for table, _ in links})
        }
        for (table, _), row in links.items():
            for reference in link_inventories[table]:
                if reference["external_schema"] or (
                    reference["kind"] == "polymorphic"
                    and reference["table"] != "entity_identifiers"
                ):
                    raise HierarchyInUseError(
                        "Unsupported attachment reference prevents deletion"
                    )
                where = {
                    column: row[target]
                    for column, target in zip(
                        reference["columns"], reference["target_columns"], strict=True
                    )
                }
                if reference["kind"] == "polymorphic":
                    type_column = reference.get("type_column")
                    if not type_column:
                        raise HierarchyInUseError(
                            "Polymorphic reference has no type discriminator"
                        )
                    where[type_column] = table
                if reader.rows(reference["table"], where):
                    raise HierarchyInUseError(
                        "Referenced attachment cannot be detached"
                    )
        for table, identity in links:
            self._macros.delete_row(
                table, identity, id_column=self._wrapper.get_id_column(table)
            )
        result = tuple(reversed(self._ids(rows)))
        parent = self._parent(rows[0][self.tree_spec.parent])
        for identity in result:
            self._macros.delete_row(self.table_name, identity, id_column=self.id_column)
        self._order(self._ids(self._siblings(parent)))
        return result

    def delete(self, entity_id: int, *, detach: bool = False) -> None:
        """Delete a leaf only, refusing implicit promotion or cascade of children."""
        with self._macros.transaction(serialized=True):
            inventory = self._macros.lock_repair_references(self.table_name)
            row = self.require(entity_id)
            if self._source().children(entity_id, limit=1):
                raise HierarchyInUseError(
                    "Node has children; use move or delete_subtree explicitly"
                )
            self._delete_nodes((row,), detach=detach, inventory=inventory)

    def delete_subtree(
        self, entity_id: int, *, detach: bool = False, max_nodes: int = 10000
    ) -> tuple[int, ...]:
        """Delete complete bounded descendants child-first within one write unit."""
        with self._macros.transaction(serialized=True):
            inventory = self._macros.lock_repair_references(self.table_name)
            rows = subtree_rows(
                self._source(), self.tree_spec, entity_id, max_nodes=max_nodes
            )
            return self._delete_nodes(rows, detach=detach, inventory=inventory)
