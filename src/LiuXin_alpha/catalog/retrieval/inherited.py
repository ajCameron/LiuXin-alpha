"""Read attributed tag inheritance down the complete WEMI containment graph.

No inherited value is copied into a descendant's stored metadata. Contributions
retain every relationship path, including multi-Work Manifestations. Agent and
series tagging is not inferred from authorship or membership.
"""

from __future__ import annotations

from LiuXin_alpha.catalog.inspection import BoundedInspection
from LiuXin_alpha.catalog.repositories.base import WEMI_TABLES


class InheritedMetadataRetriever:
    """Compute structured tag contributions in a bounded read-only snapshot."""

    def __init__(self, db):
        self.db = db

    def tags(self, *, level, entity_id, max_rows=10000):
        """Return distinct tags with all direct and inherited provenance paths.

        Follow Item→Manifestation→Expression→Work parents without primary-row
        selection. Inspect direct tags only where a physical link is supported
        (currently Works, Expressions and Items). Preserve link rows as provenance,
        sort tags by casefolded text then ID, and retain source/path order by level
        and physical link identity. Missing nodes or an exceeded row/visit budget
        fail the whole inspection. No worker, installation or repair is requested.
        """
        if level not in WEMI_TABLES:
            raise ValueError(
                "Inherited tags require a Work, Expression, Manifestation or Item"
            )
        reader = BoundedInspection(self.db, max_rows)
        tags = {}
        with self.db.macros.read_transaction():
            self._visit(reader, level, entity_id, (), tags)
        return {
            "level": level,
            "entity_id": entity_id,
            "policy": "wemi-parent-tags-v1",
            "scanned": reader.scanned,
            "tags": sorted(
                tags.values(),
                key=lambda tag: (str(tag["text"] or "").casefold(), tag["tag_id"]),
            ),
        }

    def _visit(self, reader, level, entity_id, path, tags):
        reader.consume()
        table = WEMI_TABLES[level]
        entity = reader.require(table, entity_id)
        path = (*path, {"level": level, "entity_id": entity_id})
        spec = self.db.driver_wrapper.get_link_spec(table, "tags")
        if spec is not None:
            for link in reader.rows(
                spec.link_table, {spec.primary_link_col: entity_id}
            ):
                tag_id = link[spec.secondary_link_col]
                if tag_id is None:
                    continue
                tag = reader.require("tags", tag_id)
                value = tags.setdefault(
                    tag_id, {"tag_id": tag_id, "text": tag["tag"], "contributions": []}
                )
                value["contributions"].append(
                    {
                        "source_level": level,
                        "source_id": entity_id,
                        "inherited": len(path) > 1,
                        "path": list(path),
                        "tag_link": {"table": spec.link_table, "row": link},
                    }
                )
        if level == "work":
            return
        if level == "item":
            parent = entity.get("item_manifestation_id")
            if parent is not None:
                path = (
                    *path[:-1],
                    {
                        **path[-1],
                        "parent_link": {
                            "table": "items",
                            "column": "item_manifestation_id",
                            "value": parent,
                        },
                    },
                )
                self._visit(reader, "manifestation", parent, path, tags)
            return
        parent_level = "work" if level == "expression" else "expression"
        spec = self.db.driver_wrapper.get_link_spec(table, WEMI_TABLES[parent_level])
        if spec is None:
            raise ValueError(f"Missing physical WEMI parent relation for {level}")
        for link in reader.rows(spec.link_table, {spec.primary_link_col: entity_id}):
            parent = link[spec.secondary_link_col]
            if parent is None:
                continue
            # Store the exact traversed edge on its child step. Different typed
            # links to the same endpoint remain distinct provenance paths.
            edge_path = (
                *path[:-1],
                {**path[-1], "parent_link": {"table": spec.link_table, "row": link}},
            )
            self._visit(reader, parent_level, parent, edge_path, tags)
