"""Make the old merge fixtures' target-preference decisions explicit."""


def apply_target_preferred_merge(
    catalog, *, level, source_id, target_id, relationship_resolutions=None
):
    """Resolve the fixture's documented scalar/link collisions, then apply its preview."""
    args = dict(
        level=level,
        source_id=source_id,
        target_id=target_id,
        relationship_resolutions=dict(relationship_resolutions or {}),
    )
    writer = catalog.mutations.writer
    preview = writer.merge_entities(**args)
    args["resolutions"] = {}
    for conflict in preview["conflicts"]:
        if conflict["kind"] == "scalar":
            args["resolutions"][conflict["column"]] = {"take": "target"}
        elif conflict["kind"] == "relationship_payload":
            args["relationship_resolutions"][conflict["row"]] = {"take": "target"}
        else:
            raise AssertionError(
                f"Fixture needs an explicit repair decision: {conflict}"
            )
    preview = writer.merge_entities(**args)
    assert not preview["blockers"] and not preview["conflicts"], preview
    return writer.merge_entities(**args, dry_run=False, expected_state=preview["state"])
