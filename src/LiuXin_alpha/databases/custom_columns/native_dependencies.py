"""Definition-level composite dependencies, including explicit relationship paths."""

from __future__ import annotations

import re

from LiuXin_alpha.databases.custom_columns.native_types import (
    CustomFieldError,
    FieldDefinition,
)


def dependencies(
    definition: FieldDefinition, definitions: list[FieldDefinition]
) -> set[int]:
    found = set(definition.display.get("dependencies", []))
    scoped = {(d.attachment, d.label): d.id for d in definitions}
    template = definition.display.get("composite_template", "")
    for label in re.findall(r"#([\w]+(?:\.[\w]+)?)", template):
        attachment, label = (
            label.split(".", 1) if "." in label else (definition.attachment, label)
        )
        if (attachment, label) not in scoped:
            raise CustomFieldError(f"Unknown composite dependency {attachment}.{label}")
        found.add(scoped[attachment, label])
    for path in definition.display.get("paths", {}).values():
        tables = path.get("tables", [])
        label = path.get("field", "")
        if label.startswith("#"):
            target = tables[-1] if tables else definition.attachment
            label = label.removeprefix("#").removeprefix(target + ".")
            if (target, label) not in scoped:
                raise CustomFieldError(f"Unknown path dependency {target}.{label}")
            found.add(scoped[target, label])
    if not found <= {d.id for d in definitions}:
        raise CustomFieldError("Composite references a missing dependency ID")
    return found


def validate_dependencies(
    definition: FieldDefinition, definitions: list[FieldDefinition]
) -> None:
    proposed = [d for d in definitions if d.id != definition.id] + [definition]
    graph = {
        d.id: dependencies(d, proposed) for d in proposed if d.datatype == "composite"
    }
    done, active = set(), set()

    def visit(node: int) -> None:
        if node in active:
            raise CustomFieldError("Composite dependency cycle")
        if node in done:
            return
        active.add(node)
        for child in graph.get(node, ()):
            visit(child)
        active.remove(node)
        done.add(node)

    for node in graph:
        visit(node)
