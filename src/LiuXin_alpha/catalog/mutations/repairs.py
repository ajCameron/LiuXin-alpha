"""Preview and apply explicit, attributed Catalog merge and disposal decisions.

No spelling match proves identity and no unlinked row proves disposability. All
affected physical references are inventoried; unknown semantics, incomplete scans
and unresolved conflicts block apply. The database owns locks, SQL and receipts.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from LiuXin_alpha.catalog.api.common import CatalogMutationError
from LiuXin_alpha.catalog.inspection import BoundedInspection
from LiuXin_alpha.catalog.repositories.base import WEMI_TABLES
from LiuXin_alpha.databases.db_types.repair_types import repair_json_scalar

_TABLES = {
    *WEMI_TABLES.values(),
    "tags",
    "genres",
    "subjects",
    "series",
    "labels",
    "notes",
}
_DISPLAY = {
    "works": ("work_title", "work_canonical_title"),
    "expressions": (
        "expression_label",
        "expression_title_override",
        "expression_subtitle",
    ),
    "manifestations": (
        "manifestation_title",
        "manifestation_subtitle",
        "manifestation_format_detail",
    ),
    "items": ("item_inventory_code",),
    "tags": ("tag",),
    "genres": ("genre",),
    "subjects": ("subject",),
    "series": ("series",),
    "labels": ("label_text",),
    "notes": ("note",),
}


def _digest(value):
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=repair_json_scalar,
            allow_nan=False,
        ).encode()
    ).hexdigest()


def _operational(column):
    return column.endswith(
        ("_created_timestamp_ep_k", "_modified_timestamp_ep_k", "_datestamp")
    )


class CatalogRepairs:
    """Own conservative merge and selected-unused cleanup policies over repositories."""

    def __init__(self, db, repositories):
        self.db, self.repositories = db, repositories

    def _repository(self, name):
        repository = self.repositories.for_name(name)
        if repository.table_name not in _TABLES:
            raise CatalogMutationError(
                "This repository requires a separately reviewed repair policy"
            )
        return repository

    def _references(self, reader, table, entities, inventory):
        references, blockers = [], []
        singular = next(
            (level for level, name in WEMI_TABLES.items() if name == table),
            "series" if table == "series" else table.rstrip("s"),
        )
        for ref in inventory:
            if ref["external_schema"]:
                blockers.append({"kind": "external_reference", "reference": ref})
                continue
            if ref["kind"] == "polymorphic" and ref["table"] != "entity_identifiers":
                blockers.append(
                    {"kind": "unknown_polymorphic_reference", "reference": ref}
                )
                continue
            for entity in entities:
                where = dict(
                    zip(
                        ref["columns"],
                        (entity[c] for c in ref["target_columns"]),
                        strict=True,
                    )
                )
                if ref["kind"] == "polymorphic":
                    where[ref["type_column"]] = singular
                rows = reader.rows(ref["table"], where)
                references.append(
                    {
                        "reference": ref,
                        "entity_id": entity[
                            self.db.driver_wrapper.get_id_column(table)
                        ],
                        "rows": rows,
                    }
                )
        return references, blockers

    def merge(
        self,
        *,
        repository,
        source_id,
        target_id,
        resolutions=None,
        relationship_resolutions=None,
        max_rows=10000,
        dry_run=True,
        expected_state=None,
    ):
        """Inspect or atomically merge two explicitly equivalent same-table identities.

        Preview is the default. Apply requires the preview's state fingerprint and
        explicit resolution of every differing populated scalar/link payload.
        Field decisions are {take: source|target} or {value: ...}; link decisions
        use table:row_id keys with {values: {...}} and optional take. Unknown direct
        ownership, hierarchy cycles, scoped vocabulary changes and self-links are
        blockers requiring a different policy. No file operation is performed.
        """
        repo = self._repository(repository)
        if source_id == target_id:
            raise CatalogMutationError("A merge requires two distinct identities")
        if type(dry_run) is not bool:
            raise ValueError("dry_run must be boolean")
        for value in (resolutions, relationship_resolutions):
            if value is not None and not isinstance(value, Mapping):
                raise ValueError("Repair resolutions must be mappings")
        resolutions, relationship_resolutions = (
            dict(resolutions or {}),
            dict(relationship_resolutions or {}),
        )
        macros = self.db.macros
        context = (
            macros.read_transaction()
            if dry_run
            else macros.transaction(serialized=True, require_outermost=True)
        )
        with context:
            inventory = (
                macros.inspect_repair_references
                if dry_run
                else macros.lock_repair_references
            )(repo.table_name)
            reader = BoundedInspection(self.db, max_rows)
            source, target = (
                reader.require(repo.table_name, identity)
                for identity in (source_id, target_id)
            )
            refs, blockers = self._references(
                reader, repo.table_name, (source, target), inventory
            )
            snapshot = {
                "source": source,
                "target": target,
                "inventory": inventory,
                "references": refs,
            }
            changes, conflicts = self._scalars(repo, source, target, resolutions)
            prepared = repo._prepare_write(changes, existing=target) if changes else {}
            proposed = {**target, **prepared}
            identity_matches = []
            for spec in self.db.driver_wrapper.iter_normalized_identity_specs(
                table=repo.table_name
            ):
                key = proposed.get(spec.identity_column)
                if not spec.unique or key is None:
                    continue
                where = {
                    spec.identity_column: key,
                    **{c: proposed[c] for c in spec.scope_columns},
                }
                matches = reader.rows(repo.table_name, where)
                identity_matches.append({"where": where, "rows": matches})
                if any(
                    r[repo.id_column] not in {source_id, target_id} for r in matches
                ):
                    conflicts.append({"kind": "normalized_identity", "where": where})
            snapshot["identity_matches"] = identity_matches
            operations, relation_conflicts = self._relations(
                reader, repo, source_id, target_id, refs, relationship_resolutions
            )
            conflicts.extend(relation_conflicts)
            coalescence = {}
            for table in sorted(
                {op["table"] for op in operations if op["action"] == "delete"}
            ):
                incoming = [
                    r
                    for r in macros.inspect_repair_references(table)
                    if r["kind"] == "foreign_key"
                ]
                coalescence[table] = incoming
                if incoming:
                    blockers.append(
                        {
                            "kind": "referenced_relationship_identity",
                            "table": table,
                            "references": incoming,
                        }
                    )
            snapshot["coalescence_inventory"] = coalescence
            parent = next((c for c in repo.columns if c.endswith("_parent_id")), None)
            if parent and (
                source.get(parent) != target.get(parent)
                or target.get(parent) == source_id
            ):
                blockers.append({"kind": "hierarchy_scope", "column": parent})
            plan = {
                "policy": "catalog-merge-v1",
                "repository": repo.table_name,
                "source_id": source_id,
                "target_id": target_id,
                "state": _digest(snapshot),
                "snapshot": snapshot,
                "changes": changes,
                "operations": operations,
                "conflicts": conflicts,
                "blockers": blockers,
                "scanned": reader.scanned,
                "resolutions": resolutions,
                "relationship_resolutions": relationship_resolutions,
                "dry_run": dry_run,
                "merged": False,
            }
            if dry_run:
                return plan
            self._apply_check(plan, expected_state)
            # Delete only explicitly coalesced duplicate relationships. Every row
            # appears in the receipt, including data discarded by explicit choice.
            for operation in operations:
                if operation["action"] == "delete":
                    macros.delete_row(operation["table"], operation["row_id"])
            for operation in operations:
                if operation["action"] == "update":
                    macros.update_row(
                        operation["table"], operation["row_id"], operation["values"]
                    )
            # Delete first inside the transaction to free source-only unique keys.
            # All incoming references have already been moved or coalesced.
            repo.delete(source_id)
            if changes:
                repo.update(target_id, changes)
            plan["merged"] = True
            plan["receipt_id"] = macros.record_catalog_repair(plan)
            return plan

    @staticmethod
    def _apply_check(plan, expected_state):
        if not isinstance(expected_state, str) or expected_state != plan["state"]:
            raise CatalogMutationError(
                "Repair preview is missing or stale; inspect again"
            )
        if plan["blockers"] or plan["conflicts"]:
            raise CatalogMutationError(
                "Repair has unresolved conflicts or unsupported references"
            )

    def _scalars(self, repo, source, target, resolutions):
        derived = {
            spec.identity_column
            for spec in self.db.driver_wrapper.iter_normalized_identity_specs(
                table=repo.table_name
            )
        }
        excluded = {repo.id_column, *derived}
        writable = {
            c for c in repo.columns if c not in excluded and not _operational(c)
        }
        if set(resolutions) - writable:
            raise CatalogMutationError(
                "Scalar resolutions must name writable physical metadata columns"
            )
        changes, conflicts = {}, []
        for column in sorted(writable):
            old, new = target.get(column), source.get(column)
            decision = resolutions.get(column)
            if decision is not None:
                if not isinstance(decision, Mapping):
                    raise CatalogMutationError(
                        "Each scalar resolution must be a mapping"
                    )
                if set(decision) == {"value"}:
                    chosen = decision["value"]
                elif set(decision) == {"take"} and decision["take"] in {
                    "source",
                    "target",
                }:
                    chosen = new if decision["take"] == "source" else old
                else:
                    raise CatalogMutationError(
                        "A scalar resolution requires take or value"
                    )
                if chosen != old:
                    changes[column] = chosen
            elif old in (None, "") and new not in (None, ""):
                changes[column] = new
            elif old not in (None, "") and new not in (None, "") and old != new:
                conflicts.append(
                    {"kind": "scalar", "column": column, "source": new, "target": old}
                )
        return changes, conflicts

    def _relations(self, reader, repo, source_id, target_id, refs, decisions):
        operations, conflicts, tables = [], [], {}
        # Multiple FK columns may select the same physical intralink row.
        for group in refs:
            ref = group["reference"]
            if not group["rows"]:
                continue
            entry = tables.setdefault(ref["table"], {"refs": [], "rows": {}})
            if ref not in entry["refs"]:
                entry["refs"].append(ref)
            id_col = self.db.driver_wrapper.get_id_column(ref["table"])
            entry["rows"].update({row[id_col]: row for row in group["rows"]})
        used = set()
        for table, entry in sorted(tables.items()):
            columns = {c for ref in entry["refs"] for c in ref["columns"]}
            if any(
                len(r["columns"]) != 1 or r["target_columns"] != [repo.id_column]
                for r in entry["refs"]
            ):
                conflicts.append({"kind": "unsupported_reference", "table": table})
                continue
            spec = self._link_spec(repo.table_name, table)
            direct = (
                table == "items"
                and repo.table_name == "manifestations"
                and columns == {"item_manifestation_id"}
            )
            identifiers = table == "entity_identifiers"
            if spec is None and not direct and not identifiers:
                conflicts.append({"kind": "unsupported_reference", "table": table})
                continue
            id_col = self.db.driver_wrapper.get_id_column(table)
            retained = {}
            for identity, before in sorted(
                entry["rows"].items(),
                key=lambda pair: (
                    any(pair[1].get(c) == source_id for c in columns),
                    pair[0],
                ),
            ):
                moved = any(before.get(c) == source_id for c in columns)
                after = {
                    **before,
                    **{c: target_id for c in columns if before.get(c) == source_id},
                }
                key = f"{table}:{identity}"
                decision = decisions.get(key, {})
                if key in decisions:
                    used.add(key)
                if not isinstance(decision, Mapping) or set(decision) - {
                    "take",
                    "values",
                }:
                    raise CatalogMutationError(
                        "Link resolutions accept only take and values"
                    )
                values = decision.get("values", {})
                allowed = set(before) - columns - {id_col}
                if spec:
                    allowed -= {
                        spec.primary_link_col,
                        spec.secondary_link_col,
                        spec.type_link_col,
                    }
                allowed = {c for c in allowed if not _operational(c)}
                if not isinstance(values, Mapping) or set(values) - allowed:
                    raise CatalogMutationError(
                        "Link resolution cannot change identity, endpoints or timestamps"
                    )
                after.update(values)
                if spec:
                    if spec.primary_table == spec.secondary_table:
                        role = after.get(spec.type_link_col)
                        supported = {"user_marked_different"}
                        if repo.table_name == "works":
                            supported |= {"inspired_by", "fanfiction_of"}
                        if role not in supported:
                            conflicts.append(
                                {"kind": "unsupported_intralink_type", "row": key}
                            )
                        elif role == "user_marked_different":
                            left, right = (
                                after[spec.primary_link_col],
                                after[spec.secondary_link_col],
                            )
                            if left is not None and right is not None:
                                (
                                    after[spec.primary_link_col],
                                    after[spec.secondary_link_col],
                                ) = sorted((left, right))
                    link_key = (
                        after[spec.primary_link_col],
                        after[spec.secondary_link_col],
                    )
                    if spec.type_part_of_identity:
                        link_key += (after.get(spec.type_link_col),)
                    if (
                        spec.primary_table == spec.secondary_table
                        and link_key[0] == link_key[1]
                    ):
                        conflicts.append({"kind": "self_link", "row": key})
                elif identifiers:
                    link_key = (
                        after["entity_identifier_scheme"],
                        after["entity_identifier_value"],
                    )
                else:
                    link_key = (identity,)
                semantic = {
                    c: v
                    for c, v in after.items()
                    if c != id_col and not _operational(c)
                }
                duplicate = retained.get(link_key)
                if duplicate:
                    retained_id, retained_after = duplicate
                    other = {
                        c: v
                        for c, v in retained_after.items()
                        if c != id_col and not _operational(c)
                    }
                    take = decision.get("take")
                    if take not in (None, "source", "target"):
                        raise CatalogMutationError("Link take must be source or target")
                    if semantic != other and take is None:
                        conflicts.append(
                            {
                                "kind": "relationship_payload",
                                "row": key,
                                "source": after,
                                "target": retained_after,
                            }
                        )
                    elif take == "source":
                        operations.append(
                            {"action": "delete", "table": table, "row_id": retained_id}
                        )
                        operations = [
                            o
                            for o in operations
                            if not (
                                o["action"] == "update"
                                and o["table"] == table
                                and o["row_id"] == retained_id
                            )
                        ]
                        retained[link_key] = (identity, after)
                    else:
                        operations.append(
                            {"action": "delete", "table": table, "row_id": identity}
                        )
                        continue
                else:
                    if "take" in decision:
                        raise CatalogMutationError(
                            "A link take decision requires a duplicate identity"
                        )
                    retained[link_key] = (identity, after)
                if moved or values:
                    changes = {c: v for c, v in after.items() if v != before[c]}
                    if changes:
                        operations.append(
                            {
                                "action": "update",
                                "table": table,
                                "row_id": identity,
                                "values": changes,
                            }
                        )
            # Priority and primary collisions remain explicit; no ordering or
            # canonical-identifier choice is silently reassigned during a merge.
            seen = {}
            for identity, row in retained.values():
                groups = []
                if spec:
                    owner = row[spec.primary_link_col]
                    role = row.get(spec.type_link_col)
                    if (
                        spec.priority_link_col
                        and row.get(spec.priority_link_col) is not None
                    ):
                        groups.append(
                            ("priority", owner, role, row[spec.priority_link_col])
                        )
                    for column in row:
                        if column.endswith("_primary") and row[column]:
                            groups.append(("primary", column, owner, role))
                elif identifiers and row.get("entity_identifier_is_primary"):
                    groups.append(
                        ("primary_identifier", row["entity_identifier_scheme"])
                    )
                for group in groups:
                    if group in seen:
                        conflicts.append(
                            {
                                "kind": group[0],
                                "rows": [
                                    f"{table}:{seen[group]}",
                                    f"{table}:{identity}",
                                ],
                            }
                        )
                    seen[group] = identity
        if set(decisions) - used:
            raise CatalogMutationError(
                "Link resolution names an uninspected relationship"
            )
        return operations, conflicts

    def _link_spec(self, entity_table, link_table):
        for other in sorted(
            {entity_table, *self.db.driver_wrapper.get_interlinked_tables(entity_table)}
        ):
            spec = (
                self.db.driver_wrapper.get_intralink_spec(entity_table)
                if other == entity_table
                else self.db.driver_wrapper.get_link_spec(entity_table, other)
            )
            if spec is not None and spec.link_table == link_table:
                return spec
        return None

    def cleanup(
        self,
        *,
        repository,
        candidates,
        max_rows=10000,
        dry_run=True,
        expected_state=None,
    ):
        """Preview/apply explicit discard decisions for selected unused identities.

        Each candidate is {entity_id, decision: discard|preserve, reason}. A discard
        requires nonblank reason, recognizable display metadata, no inbound or
        outgoing references, and a matching preview. Preserve always wins for
        that candidate. No global scan, file deletion or recursive cleanup occurs.
        """
        repo = self._repository(repository)
        if type(dry_run) is not bool:
            raise ValueError("dry_run must be boolean")
        if not isinstance(candidates, (list, tuple)) or not 1 <= len(candidates) <= 500:
            raise ValueError("Cleanup requires between 1 and 500 explicit candidates")
        ids = set()
        for candidate in candidates:
            if not isinstance(candidate, Mapping) or set(candidate) != {
                "entity_id",
                "decision",
                "reason",
            }:
                raise ValueError(
                    "Cleanup candidates require entity_id, decision and reason"
                )
            identity = candidate["entity_id"]
            if type(identity) is not int or identity < 1 or identity in ids:
                raise ValueError(
                    "Cleanup identities must be distinct positive integers"
                )
            ids.add(identity)
            if (
                candidate["decision"] not in {"discard", "preserve"}
                or not isinstance(candidate["reason"], str)
                or not candidate["reason"].strip()
            ):
                raise ValueError(
                    "Each cleanup candidate needs a decision and nonblank reason"
                )
        macros = self.db.macros
        with (
            macros.read_transaction()
            if dry_run
            else macros.transaction(serialized=True, require_outermost=True)
        ):
            inventory = (
                macros.inspect_repair_references
                if dry_run
                else macros.lock_repair_references
            )(repo.table_name)
            reader = BoundedInspection(self.db, max_rows)
            rows = [
                reader.require(repo.table_name, candidate["entity_id"])
                for candidate in candidates
            ]
            refs, blockers = self._references(reader, repo.table_name, rows, inventory)
            conflicts, selected = [], []
            for candidate, row in zip(candidates, rows, strict=True):
                if candidate["decision"] == "preserve":
                    continue
                identity = row[repo.id_column]
                incoming = [r for r in refs if r["entity_id"] == identity and r["rows"]]
                outgoing = {
                    c: v
                    for c, v in row.items()
                    if c.endswith("_id") and c != repo.id_column and v is not None
                }
                if not any(
                    row.get(c) not in (None, "") for c in _DISPLAY[repo.table_name]
                ):
                    conflicts.append({"kind": "placeholder", "entity_id": identity})
                elif incoming or outgoing:
                    conflicts.append(
                        {
                            "kind": "referenced",
                            "entity_id": identity,
                            "outgoing": outgoing,
                        }
                    )
                else:
                    selected.append(identity)
            snapshot = {"rows": rows, "inventory": inventory, "references": refs}
            plan = {
                "policy": "catalog-unused-v1",
                "repository": repo.table_name,
                "candidates": list(candidates),
                "state": _digest(snapshot),
                "snapshot": snapshot,
                "selected": selected,
                "conflicts": conflicts,
                "blockers": blockers,
                "scanned": reader.scanned,
                "dry_run": dry_run,
                "deleted": [],
            }
            if dry_run:
                return plan
            self._apply_check(plan, expected_state)
            for identity in selected:
                repo.delete(identity)
            plan["deleted"] = selected
            plan["receipt_id"] = macros.record_catalog_repair(plan)
            return plan
