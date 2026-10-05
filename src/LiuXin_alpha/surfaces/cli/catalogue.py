"""
Translate catalogue, browsing, and acquisition CLI requests into Core operations.

JSON specifications are read on the CLI host before dispatch. Core owns semantic
validation, persistence, and resource resolution. Common query/command adapters
publish receipts after closing their storage-enabled session and return zero
without interpreting receipt status; a publication failure does not undo a
mutation. Confirmation policies differ: entity deletion, WEMI unlink, and
metadata merge refuse without --yes, while custom-field deletion previews locally.
Acquisition byte output uses a CLI-host destination, not a Core-host file path.
"""

from __future__ import annotations

import argparse
import json
import sys

from typing import Any

from LiuXin_alpha.surfaces.cli.common import (
    add_connection_arguments,
    add_json_output,
    decode_wire_bytes,
    emit_bytes,
    emit_json,
    load_json_file,
    load_json_object,
    open_cli_core,
)


def _query(args: argparse.Namespace, operation: str, payload: dict[str, Any]) -> int:
    """
    Query a storage-enabled Core session and publish its receipt after exit.

    The request is passed by identity; returned status/shape is not validated.
    Connection, query, cleanup, serialization, and publication errors propagate.

    Example:
        >>> _query(args, "browse.categories", {})  # doctest: +SKIP


    :param args: Connection selectors and JSON publication controls.
    :param operation: Exact registered Core query name.
    :param payload: Request dictionary forwarded without copying or normalization.
    :return: Zero after output, even if the returned receipt reports failure.
    """
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query(operation, payload)
    emit_json(result, args)
    return 0


def _command(args: argparse.Namespace, operation: str, payload: dict[str, Any]) -> int:
    """
    Execute a Core command and publish its receipt after the session exits.

    No generic confirmation or receipt-status check is added. Failures propagate;
    an accepted mutation is not rolled back if cleanup or publication later fails.

    Example:
        >>> _command(args, "catalog.entity.create", declaration)  # doctest: +SKIP


    :param args: Connection selectors and JSON publication controls.
    :param operation: Exact registered Core command name.
    :param payload: Request dictionary forwarded by identity, without validation.
    :return: Zero once output completes, without interpreting the command receipt.
    """
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(operation, payload)
    emit_json(result, args)
    return 0


def _core_json(parser: argparse.ArgumentParser) -> None:
    """
    Add shared Core selection and JSON publication flags to one command leaf.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _core_json(parser)
        >>> parser.parse_args(["--database", "db.sqlite"]).database
        'db.sqlite'


    :param parser: Mutable leaf parser receiving connection and output options.
    :return: None; no connection or output is performed during registration.
    """
    add_connection_arguments(parser)
    add_json_output(parser)


def cmd_catalog_search(args: argparse.Namespace) -> int:
    """
    Search catalogue text with integer paging and optional table restrictions.

    Copy truthy table selections to a list, retaining duplicates/order. Text and
    table names are not normalized, and numeric bounds are not clamped locally.

    Example:
        >>> cmd_catalog_search(parsed_catalog_search_args)  # doctest: +SKIP


    :param args: Common connection/output controls plus text, limit, offset, table.
    :return: Zero after publishing the search.global query receipt.
    """
    payload: dict[str, Any] = {
        "text": args.text,
        "limit": int(args.limit),
        "offset": int(args.offset),
    }
    if args.table:
        payload["tables"] = list(args.table)
    return _query(args, "search.global", payload)


def cmd_catalog_entity_list(args: argparse.Namespace) -> int:
    """
    Request one repository's entity page without locally validating its name.

    Example:
        >>> cmd_catalog_entity_list(parsed_entity_list_args)  # doctest: +SKIP


    :param args: Common controls, repository token, and integer-convertible
        limit/offset; negative values are forwarded rather than clamped.
    :return: Zero after publishing catalog.entity.list's receipt.
    """
    return _query(
        args,
        "catalog.entity.list",
        {
            "repository": args.repository,
            "limit": int(args.limit),
            "offset": int(args.offset),
        },
    )


def cmd_catalog_entity_show(args: argparse.Namespace) -> int:
    """
    Fetch one semantic repository entity by its integer-converted identifier.

    Example:
        >>> cmd_catalog_entity_show(parsed_entity_show_args)  # doctest: +SKIP


    :param args: Common controls, repository name, and entity_id; Core resolves
        existence and repository-specific semantics.
    :return: Zero after publishing catalog.entity.get's receipt.
    """
    return _query(
        args,
        "catalog.entity.get",
        {"repository": args.repository, "entity_id": int(args.entity_id)},
    )


def cmd_catalog_entity_match(args: argparse.Namespace) -> int:
    """
    Match a CLI-host candidate declaration, optionally creating on no match.

    Load candidate and any truthy hints-file selection as JSON objects before
    opening Core. Include parent_id when non-None, including zero, and source
    only when truthy. The create flag selects the mutating match-or-create
    command instead of catalog.match; there is no separate --yes requirement.

    Example:
        >>> cmd_catalog_entity_match(parsed_entity_match_args)  # doctest: +SKIP


    :param args: Common controls plus repository, candidate_file, parent_id,
        source, hints_file, and create; file selectors belong to the CLI host.
    :return: Zero after publishing the match query or match-or-create receipt.
    """
    payload: dict[str, Any] = {
        "repository": args.repository,
        "candidate": load_json_object(args.candidate_file),
    }
    if args.parent_id is not None:
        payload["parent_id"] = int(args.parent_id)
    if args.source:
        payload["source"] = args.source
    if args.hints_file:
        payload["hints"] = load_json_object(args.hints_file)
    operation = (
        "catalog.entity.match-or-create" if args.create else "catalog.match"
    )
    if args.create:
        return _command(args, operation, payload)
    return _query(args, operation, payload)


def cmd_catalog_entity_write(args: argparse.Namespace) -> int:
    """
    Create or update an entity from a CLI-host JSON object.

    Only the exact entity_action 'update' includes entity_id and selects update;
    all other direct-call values select create. The parser constrains normal CLI
    calls. No confirmation flag or local entity-field schema is enforced here.

    Example:
        >>> cmd_catalog_entity_write(parsed_entity_update_args)  # doctest: +SKIP


    :param args: Common controls, repository, data_file, entity_action, and an
        integer-convertible entity_id for update.
    :return: Zero after publishing the create/update command receipt.
    """
    payload: dict[str, Any] = {
        "repository": args.repository,
        "data": load_json_object(args.data_file),
    }
    operation = "catalog.entity.create"
    if args.entity_action == "update":
        payload["entity_id"] = int(args.entity_id)
        operation = "catalog.entity.update"
    return _command(args, operation, payload)


def cmd_catalog_entity_delete(args: argparse.Namespace) -> int:
    """
    Delete a repository entity only after explicit confirmation.

    Refusal happens before opening Core and produces no preview.

    Example:
        >>> cmd_catalog_entity_delete(argparse.Namespace(yes=False))
        Traceback (most recent call last):
        ...
        ValueError: Catalogue entity deletion requires --yes.


    :param args: Common controls, yes confirmation, repository, and entity_id.
    :return: Zero after publishing the confirmed deletion receipt.
    :raises ValueError: Confirmation is absent or entity_id cannot convert to int.
    """
    if not args.yes:
        raise ValueError("Catalogue entity deletion requires --yes.")
    return _command(
        args,
        "catalog.entity.delete",
        {"repository": args.repository, "entity_id": int(args.entity_id)},
    )


def cmd_catalog_bundle(args: argparse.Namespace) -> int:
    """
    Request a hydrated WEMI bundle for one level and entity identifier.

    Example:
        >>> cmd_catalog_bundle(parsed_catalog_bundle_args)  # doctest: +SKIP


    :param args: Common controls, unmodified WEMI level, and entity_id converted
        to int; hydration and level validation are Core responsibilities.
    :return: Zero after publishing catalog.bundle.get's receipt.
    """
    return _query(
        args,
        "catalog.bundle.get",
        {"level": args.level, "entity_id": int(args.entity_id)},
    )


def cmd_catalog_graph(args: argparse.Namespace) -> int:
    """
    Request a Work graph with separate expression, manifestation, and Item caps.

    Convert all identifiers/counts to integers without checking positive ranges
    or enforcing the requested caps locally.

    Example:
        >>> cmd_catalog_graph(parsed_catalog_graph_args)  # doctest: +SKIP


    :param args: Common controls plus work_id, max_expressions,
        max_manifestations, and max_items.
    :return: Zero after publishing catalog.graph.get's receipt.
    """
    return _query(
        args,
        "catalog.graph.get",
        {
            "work_id": int(args.work_id),
            "max_expressions": int(args.max_expressions),
            "max_manifestations": int(args.max_manifestations),
            "max_items": int(args.max_items),
        },
    )


def cmd_catalog_item_summary(args: argparse.Namespace) -> int:
    """
    Fetch Core's semantic summary for one Item, not its acquisition bytes.

    Example:
        >>> cmd_catalog_item_summary(parsed_catalog_item_args)  # doctest: +SKIP


    :param args: Common controls and an integer-convertible item_id.
    :return: Zero after publishing catalog.item.summary's receipt.
    """
    return _query(
        args, "catalog.item.summary", {"item_id": int(args.item_id)}
    )


def cmd_catalog_hierarchy(args: argparse.Namespace) -> int:
    """
    List adjacent WEMI entities in the requested parent/child direction.

    Direction and level pass through unchanged; only the CLI parser restricts
    direction choices. This adapter does not traverse the hierarchy itself.

    Example:
        >>> cmd_catalog_hierarchy(parsed_catalog_hierarchy_args)  # doctest: +SKIP


    :param args: Common controls plus level, integer-convertible entity_id,
        and direction.
    :return: Zero after publishing catalog.hierarchy.list's receipt.
    """
    return _query(
        args,
        "catalog.hierarchy.list",
        {
            "level": args.level,
            "entity_id": int(args.entity_id),
            "direction": args.direction,
        },
    )


def cmd_catalog_identifiers(args: argparse.Namespace) -> int:
    """
    Fetch complete identifier records or Core's primary-value projection.

    Example:
        >>> cmd_catalog_identifiers(parsed_identifier_list_args)  # doctest: +SKIP


    :param args: Common controls, level, entity_id, and primary_only selecting
        primary-values instead of the identifiers.list operation.
    :return: Zero after publishing the chosen identifier query receipt.
    """
    operation = (
        "catalog.identifiers.primary-values"
        if args.primary_only
        else "catalog.identifiers.list"
    )
    return _query(
        args,
        operation,
        {"level": args.level, "entity_id": int(args.entity_id)},
    )


def cmd_catalog_identifiers_set(args: argparse.Namespace) -> int:
    """
    Replace entity identifiers using a CLI-host JSON object or array.

    Validate only the top-level container before opening Core; element schemas
    and replacement semantics belong to Core. No --yes gate is imposed.

    Example:
        >>> cmd_catalog_identifiers_set(parsed_identifier_set_args)  # doctest: +SKIP


    :param args: Common controls, level, entity_id, and identifiers_file selector.
    :return: Zero after publishing catalog.identifiers.replace's receipt.
    :raises ValueError: Decoded identifiers are neither a dict nor a list.
    """
    identifiers = load_json_file(args.identifiers_file)
    if not isinstance(identifiers, (dict, list)):
        raise ValueError("Identifiers JSON must contain an object or array.")
    return _command(
        args,
        "catalog.identifiers.replace",
        {
            "level": args.level,
            "entity_id": int(args.entity_id),
            "identifiers": identifiers,
        },
    )


def cmd_catalog_agents(args: argparse.Namespace) -> int:
    """
    List agents attached to one entity, optionally restricted by a truthy role.

    Example:
        >>> cmd_catalog_agents(parsed_agent_list_args)  # doctest: +SKIP


    :param args: Common controls plus level, entity_id, and optional role token;
        only entity_id is integer-coerced.
    :return: Zero after publishing catalog.agents.list's receipt.
    """
    payload: dict[str, Any] = {
        "level": args.level,
        "entity_id": int(args.entity_id),
    }
    if args.role:
        payload["role"] = args.role
    return _query(args, "catalog.agents.list", payload)


def cmd_catalog_annotations(args: argparse.Namespace) -> int:
    """
    List an Item's annotations with optional user and annotation-kind filters.

    A non-None user_id, including zero, is integer-converted; a falsey kind is
    omitted. No local ownership or permission inference is performed.

    Example:
        >>> cmd_catalog_annotations(parsed_annotation_list_args)  # doctest: +SKIP


    :param args: Common controls, item_id, optional user_id, and kind.
    :return: Zero after publishing catalog.annotations.list's receipt.
    """
    payload: dict[str, Any] = {"item_id": int(args.item_id)}
    if args.user_id is not None:
        payload["user_id"] = int(args.user_id)
    if args.kind:
        payload["kind"] = args.kind
    return _query(args, "catalog.annotations.list", payload)


def cmd_catalog_agent_resolve(args: argparse.Namespace) -> int:
    """
    Ask Core to resolve an agent name with an optional truthy role restriction.

    Example:
        >>> cmd_catalog_agent_resolve(parsed_agent_resolve_args)  # doctest: +SKIP


    :param args: Common controls plus name and role, forwarded without trimming
        or local ambiguity resolution.
    :return: Zero after publishing catalog.agent.resolve's receipt.
    """
    payload = {"name": args.name}
    if args.role:
        payload["role"] = args.role
    return _query(args, "catalog.agent.resolve", payload)


def cmd_catalog_agent_link(args: argparse.Namespace) -> int:
    """
    Attach an agent to an entity with optional role and ordering priority.

    Convert both IDs and a non-None priority to integers; zero priority is kept.
    No confirmation or local relationship/existence validation is performed.

    Example:
        >>> cmd_catalog_agent_link(parsed_agent_link_args)  # doctest: +SKIP


    :param args: Common controls, agent_id, level, entity_id, role, and priority.
    :return: Zero after publishing catalog.agent.link's receipt.
    """
    payload: dict[str, Any] = {
        "agent_id": int(args.agent_id),
        "level": args.level,
        "entity_id": int(args.entity_id),
    }
    if args.role:
        payload["role"] = args.role
    if args.priority is not None:
        payload["priority"] = int(args.priority)
    return _command(args, "catalog.agent.link", payload)


def cmd_catalog_agent_create(args: argparse.Namespace) -> int:
    """
    Create a person or organisation agent from a CLI-host JSON object.

    Remove all 'create-' substrings from agent_action to build the command
    suffix. Normal CLI choices constrain it; direct callers are not allowlisted.
    The loaded specification is sent as the payload, not nested under another key.

    Example:
        >>> cmd_catalog_agent_create(parsed_create_person_args)  # doctest: +SKIP


    :param args: Common controls, agent_action, and spec_file JSON selector.
    :return: Zero after publishing the selected agent-creation receipt.
    """
    payload = load_json_object(args.spec_file)
    return _command(
        args,
        "catalog.agent.create-" + args.agent_action.replace("create-", ""),
        payload,
    )


def cmd_catalog_wemi_create(args: argparse.Namespace) -> int:
    """
    Submit a complete WEMI creation declaration loaded on the CLI host.

    Only JSON-object shape is checked locally; Core validates its work,
    expression, manifestation, and optional Item declarations.

    Example:
        >>> cmd_catalog_wemi_create(parsed_wemi_create_args)  # doctest: +SKIP


    :param args: Common controls and spec_file supplying the direct Core payload.
    :return: Zero after publishing catalog.wemi.create's receipt.
    """
    payload = load_json_object(args.spec_file)
    return _command(args, "catalog.wemi.create", payload)


def cmd_catalog_wemi_link(args: argparse.Namespace) -> int:
    """
    Create an explicit parent/child WEMI relationship with optional link facts.

    Preserve explicit false primary and zero priority by omitting them only for
    None. Origin is included only when truthy. Core owns level and cycle validation.

    Example:
        >>> cmd_catalog_wemi_link(parsed_wemi_link_args)  # doctest: +SKIP


    :param args: Common controls, parent_level/parent_id, child_level/child_id,
        and primary, priority, origin; IDs/priority convert to int.
    :return: Zero after publishing catalog.wemi.link's receipt.
    """
    payload: dict[str, Any] = {
        "parent_level": args.parent_level,
        "parent_id": int(args.parent_id),
        "child_level": args.child_level,
        "child_id": int(args.child_id),
    }
    if args.primary is not None:
        payload["primary"] = bool(args.primary)
    if args.priority is not None:
        payload["priority"] = int(args.priority)
    if args.origin:
        payload["origin"] = args.origin
    return _command(args, "catalog.wemi.link", payload)


def cmd_catalog_wemi_unlink(args: argparse.Namespace) -> int:
    """
    Remove an explicit WEMI edge only after confirmation, without a preview.

    Example:
        >>> cmd_catalog_wemi_unlink(argparse.Namespace(yes=False))
        Traceback (most recent call last):
        ...
        ValueError: WEMI unlinking requires --yes.


    :param args: Common controls, yes flag, and parent/child level-ID pairs.
    :return: Zero after publishing the confirmed catalog.wemi.unlink receipt.
    :raises ValueError: Confirmation is absent or an endpoint ID is not an integer.
    """
    if not args.yes:
        raise ValueError("WEMI unlinking requires --yes.")
    return _command(
        args,
        "catalog.wemi.unlink",
        {
            "parent_level": args.parent_level,
            "parent_id": int(args.parent_id),
            "child_level": args.child_level,
            "child_id": int(args.child_id),
        },
    )


def cmd_catalog_metadata_write(args: argparse.Namespace) -> int:
    """
    Attach or replace entity metadata from a CLI-host JSON object.

    The action is appended directly to 'catalog.metadata.'; parser choices,
    rather than this handler, constrain it to attach/replace. No --yes gate applies.

    Example:
        >>> cmd_catalog_metadata_write(parsed_metadata_attach_args)  # doctest: +SKIP


    :param args: Common controls, catalog_metadata_action, level, entity_id,
        and data_file; data is decoded before opening Core.
    :return: Zero after publishing the selected metadata command receipt.
    """
    return _command(
        args,
        "catalog.metadata." + args.catalog_metadata_action,
        {
            "level": args.level,
            "entity_id": int(args.entity_id),
            "data": load_json_object(args.data_file),
        },
    )


def cmd_catalog_metadata_merge(args: argparse.Namespace) -> int:
    """
    Merge source entity metadata into a target only with explicit confirmation.

    Refusal precedes dispatch and emits no preview; merge policy belongs to Core.

    Example:
        >>> cmd_catalog_metadata_merge(argparse.Namespace(yes=False))
        Traceback (most recent call last):
        ...
        ValueError: Catalogue metadata merge requires --yes.


    :param args: Common controls, yes flag, level, source_id, and target_id.
    :return: Zero after publishing catalog.metadata.merge's receipt.
    :raises ValueError: Confirmation is absent or an ID cannot convert to int.
    """
    if not args.yes:
        raise ValueError("Catalogue metadata merge requires --yes.")
    return _command(
        args,
        "catalog.metadata.merge",
        {
            "level": args.level,
            "source_id": int(args.source_id),
            "target_id": int(args.target_id),
        },
    )


def cmd_catalog_fields(args: argparse.Namespace) -> int:
    """
    Fetch a named metadata field or list definitions by kind/composite policy.

    A truthy key selects a single-field query and ignores the listing filters.

    Example:
        >>> cmd_catalog_fields(parsed_catalog_fields_args)  # doctest: +SKIP


    :param args: Common controls, optional key, kind, and no_composites flag.
    :return: Zero after publishing catalog.fields.get or catalog.fields.list.
    """
    payload: dict[str, Any] = {}
    if args.key:
        return _query(args, "catalog.fields.get", {"key": args.key})
    payload["kind"] = args.kind
    payload["include_composites"] = not args.no_composites
    return _query(args, "catalog.fields.list", payload)


def _custom_field_match(fields: Any, reference: str) -> dict[str, Any]:
    """
    Require one list entry whose stringified number or exact label matches.

    Strip only the reference, not stored labels; comparisons are case-sensitive.
    Skip non-dicts and count duplicate entries separately. Missing numbers
    stringify to 'None', and missing labels to an empty string; these can match
    unusual references. A definition matching both keys still counts only once.

    Example:
        >>> field = {"num": 7, "label": "rating"}
        >>> _custom_field_match([None, field], " 7 ") is field
        True


    :param fields: Core's fields value; only a list is accepted as its container.
    :param reference: Identifier/label to stringify and trim before matching.
    :return: The original matching dictionary, not a copy.
    :raises ValueError: The container is invalid or there is not exactly one match.
    """
    if not isinstance(fields, list):
        raise ValueError("Core returned an invalid custom-field list.")
    token = str(reference).strip()
    matches = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        if str(field.get("num")) == token or str(field.get("label", "")) == token:
            matches.append(field)
    if len(matches) != 1:
        raise ValueError(
            "Custom field {!r} matched {} definitions.".format(reference, len(matches))
        )
    return matches[0]


def cmd_catalog_custom_fields_list(args: argparse.Namespace) -> int:
    """
    Publish all Core custom-field definitions without local projection.

    Example:
        >>> cmd_catalog_custom_fields_list(parsed_custom_list_args)  # doctest: +SKIP


    :param args: Common Core connection and JSON publication controls.
    :return: Zero after publishing custom-fields.list's receipt.
    """
    return _query(args, "custom-fields.list", {})


def cmd_catalog_custom_fields_show(args: argparse.Namespace) -> int:
    """
    Select one custom-field definition from Core's complete returned list.

    Close the session before matching and emitting a one-key field envelope.
    Invalid envelopes and ambiguous/missing matches propagate as errors.

    Example:
        >>> cmd_catalog_custom_fields_show(parsed_custom_show_args)  # doctest: +SKIP


    :param args: Common controls and field, an exact label or stringified number.
    :return: Zero after publishing the uniquely matched field definition.
    :raises ValueError: The fields list is invalid or the reference is not unique.
    """
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query("custom-fields.list", {})
    field = _custom_field_match(result.get("fields"), args.field)
    emit_json({"field": field}, args)
    return 0


def cmd_catalog_custom_fields_create(args: argparse.Namespace) -> int:
    """
    Create a custom field from typed options and optional CLI-host display JSON.

    Include label/display only for truthy selectors, and make_category whenever
    non-None, preserving false. Creation uses the payload key 'editable'; update
    uses 'is_editable'. No datatype/table/display schema is validated locally.

    Example:
        >>> cmd_catalog_custom_fields_create(parsed_custom_create_args)  # doctest: +SKIP


    :param args: Common controls plus name, datatype, multiple, not_editable,
        table, label, display_file, and make_category.
    :return: Zero after publishing custom-fields.create's receipt.
    """
    payload: dict[str, Any] = {
        "name": args.name,
        "datatype": args.datatype,
        "is_multiple": bool(args.multiple),
        "editable": not bool(args.not_editable),
        "table": args.table,
    }
    if args.label:
        payload["label"] = args.label
    if args.display_file:
        payload["display"] = load_json_object(args.display_file)
    if args.make_category is not None:
        payload["make_category"] = bool(args.make_category)
    return _command(args, "custom-fields.create", payload)


def cmd_catalog_custom_fields_update(args: argparse.Namespace) -> int:
    """
    Update a numbered custom field, with typed options overriding JSON changes.

    A None changes_file starts an empty mapping; other selectors are read on the
    CLI host. Non-None name/label include empty strings, editable writes the key
    is_editable, and a truthy display_file replaces display. Reject a still-empty
    change mapping before opening Core; field/value semantics remain Core-owned.

    Example:
        >>> cmd_catalog_custom_fields_update(parsed_custom_update_args)  # doctest: +SKIP


    :param args: Common controls, num, changes_file, name, label, editable,
        and display_file; unspecified typed fields are None.
    :return: Zero after publishing custom-fields.update's receipt.
    :raises ValueError: No changes are supplied or num cannot convert to int.
    """
    changes = (
        {} if args.changes_file is None else load_json_object(args.changes_file)
    )
    for name in ("name", "label"):
        value = getattr(args, name)
        if value is not None:
            changes[name] = value
    if args.editable is not None:
        changes["is_editable"] = bool(args.editable)
    if args.display_file:
        changes["display"] = load_json_object(args.display_file)
    if not changes:
        raise ValueError("Provide a changes file or at least one typed change option.")
    return _command(
        args,
        "custom-fields.update",
        {"num": int(args.num), "changes": changes},
    )


def cmd_catalog_custom_fields_delete(args: argparse.Namespace) -> int:
    """
    Preview a custom-field deletion locally, or execute it when yes is truthy.

    A non-None number and truthy label are included; direct callers can supply
    both, although the parser makes them mutually exclusive. Preview opens no
    Core session and proves neither field existence nor deletion feasibility.

    Example:
        >>> cmd_catalog_custom_fields_delete(parsed_custom_delete_args)  # doctest: +SKIP


    :param args: Common controls, num/label selector, and yes execution flag.
    :return: Zero after emitting either the preview or the command receipt.
    :raises ValueError: Neither selector contributes a value, or num is invalid.
    """
    payload: dict[str, Any] = {}
    if args.num is not None:
        payload["num"] = int(args.num)
    if args.label:
        payload["label"] = args.label
    if not payload:
        raise ValueError("Provide --num or --label.")
    if not args.yes:
        emit_json(
            {
                "preview": True,
                "operation": "custom-fields.delete",
                **payload,
                "message": "No custom field was deleted; pass --yes to execute.",
            },
            args,
        )
        return 0
    return _command(args, "custom-fields.delete", payload)


def cmd_browse_categories(args: argparse.Namespace) -> int:
    """
    Request Core's category overview without adding local paging or filters.

    Example:
        >>> cmd_browse_categories(parsed_browse_categories_args)  # doctest: +SKIP


    :param args: Common Core connection and JSON publication controls.
    :return: Zero after publishing browse.categories' receipt.
    """
    return _query(args, "browse.categories", {})


def cmd_browse_category(args: argparse.Namespace) -> int:
    """
    List category items with explicit paging and sort direction.

    Limit/offset are integer-converted without clamping; category/sort pass
    through unchanged and descending is inverted into ascending.

    Example:
        >>> cmd_browse_category(parsed_browse_category_args)  # doctest: +SKIP


    :param args: Common controls, category, limit, offset, sort, and descending.
    :return: Zero after publishing browse.category.items' receipt.
    """
    return _query(
        args,
        "browse.category.items",
        {
            "category": args.category,
            "limit": int(args.limit),
            "offset": int(args.offset),
            "sort": args.sort,
            "ascending": not args.descending,
        },
    )


def cmd_browse_works(args: argparse.Namespace) -> int:
    """
    Browse paged Work projections with optional category and text selectors.

    Omit selectors only for None, retaining empty text and zero category_id.
    Only limit/offset are converted here; the parser normally types category_id.

    Example:
        >>> cmd_browse_works(parsed_browse_works_args)  # doctest: +SKIP


    :param args: Common controls, limit, offset, sort, descending, category,
        category_id, and text; Core interprets filtering and sort semantics.
    :return: Zero after publishing browse.works' receipt.
    """
    payload: dict[str, Any] = {
        "limit": int(args.limit),
        "offset": int(args.offset),
        "sort": args.sort,
        "ascending": not args.descending,
    }
    for name in ("category", "category_id", "text"):
        value = getattr(args, name)
        if value is not None:
            payload[name] = value
    return _query(args, "browse.works", payload)


def cmd_browse_work(args: argparse.Namespace) -> int:
    """
    Fetch one Work's browsing projection rather than its complete WEMI graph.

    Example:
        >>> cmd_browse_work(parsed_browse_work_args)  # doctest: +SKIP


    :param args: Common controls and an integer-convertible work_id.
    :return: Zero after publishing browse.work's receipt.
    """
    return _query(args, "browse.work", {"work_id": int(args.work_id)})


def cmd_acquire_query(args: argparse.Namespace) -> int:
    """
    List a Work's formats/covers or resolve a resource without reading bytes.

    Exact formats/cover actions use work_id; every other direct-call action uses
    acquisition.resolve with kind/resource_id. The parser constrains CLI actions.

    Example:
        >>> cmd_acquire_query(parsed_acquire_formats_args)  # doctest: +SKIP


    :param args: Common controls, acquire_action, and either work_id or
        kind/resource_id; the selected identifier is integer-converted.
    :return: Zero after publishing the selected acquisition query receipt.
    """
    if args.acquire_action in {"formats", "cover"}:
        operation = "acquisition." + args.acquire_action
        payload = {"work_id": int(args.work_id)}
    else:
        operation = "acquisition.resolve"
        payload = {"kind": args.kind, "id": int(args.resource_id)}
    return _query(args, operation, payload)


def cmd_acquire_get(args: argparse.Namespace) -> int:
    """
    Read a Core resource and publish decoded bytes to a CLI-host destination.

    Fetch the complete receipt and close Core before decoding content. The shared
    byte publisher buffers stdout ('-') and handles filesystem collision/replacement
    policy; this handler neither streams Core chunks nor validates the file format.
    For file output, print resource/byte-count JSON to stderr only after publication.
    A later diagnostic failure cannot retract already published bytes. Fetch,
    decoding, serialization, and output errors propagate; no rollback is added.

    Example:
        >>> cmd_acquire_get(parsed_acquire_get_args)  # doctest: +SKIP


    :param args: Connection controls, kind, resource_id, file_output, and
        replace_file_output; this leaf does not use JSON output flags.
    :return: Zero after byte publication and any file-output diagnostic.
    """
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query(
            "acquisition.read",
            {"kind": args.kind, "id": int(args.resource_id)},
        )
    content = decode_wire_bytes(result.get("content"), label="acquisition content")
    emit_bytes(content, output=args.file_output, replace=args.replace_file_output)
    if args.file_output != "-":
        print(
            json.dumps(
                {"resource": result.get("resource"), "size": len(content)},
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
    return 0


def build_catalog_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register the catalog/catalogue tree for semantic reads, writes, and browsing.

    Every leaf receives common connection/JSON options. Nested action selection
    is required; show/get aliases, paging defaults, tri-state link/editability
    flags, and custom-field selector exclusivity are declared here. Parsing does
    not validate positive ranges, repository schemas, files, or confirmation
    policy beyond providing the handlers' flags, and performs no Core operations.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_catalog_parser(root.add_subparsers())
        >>> args = root.parse_args(["catalogue", "browse", "works"])
        >>> args.limit, args.offset, args.sort
        (100, 0, 'title')


    :param subparsers: Root CLI collection receiving catalog and its alias.
    :return: None; nested parsers and command handlers are installed in place.
    """
    parser = subparsers.add_parser(
        "catalog", aliases=["catalogue"], help="Search, browse, and edit semantic catalogue entities."
    )
    commands = parser.add_subparsers(dest="catalog_command", required=True)

    search = commands.add_parser("search", help="Search catalogue text across selected tables.")
    _core_json(search)
    search.add_argument("text")
    search.add_argument("--table", action="append")
    search.add_argument("--limit", type=int, default=100)
    search.add_argument("--offset", type=int, default=0)
    search.set_defaults(handler=cmd_catalog_search)

    entity = commands.add_parser("entity", help="Read and mutate a named semantic repository.")
    entity_commands = entity.add_subparsers(dest="entity_action", required=True)
    entity_list = entity_commands.add_parser("list")
    _core_json(entity_list)
    entity_list.add_argument("repository")
    entity_list.add_argument("--limit", type=int, default=100)
    entity_list.add_argument("--offset", type=int, default=0)
    entity_list.set_defaults(handler=cmd_catalog_entity_list)
    entity_show = entity_commands.add_parser("show", aliases=["get"])
    _core_json(entity_show)
    entity_show.add_argument("repository")
    entity_show.add_argument("entity_id", type=int)
    entity_show.set_defaults(handler=cmd_catalog_entity_show)
    entity_match = entity_commands.add_parser("match")
    _core_json(entity_match)
    entity_match.add_argument("repository")
    entity_match.add_argument("candidate_file")
    entity_match.add_argument("--parent-id", type=int)
    entity_match.add_argument("--source")
    entity_match.add_argument("--hints-file")
    entity_match.add_argument("--create", action="store_true", help="Create when no existing entity matches.")
    entity_match.set_defaults(handler=cmd_catalog_entity_match)
    for action in ("create", "update"):
        command = entity_commands.add_parser(action)
        _core_json(command)
        command.add_argument("repository")
        if action == "update":
            command.add_argument("entity_id", type=int)
        command.add_argument("data_file")
        command.set_defaults(handler=cmd_catalog_entity_write)
    entity_delete = entity_commands.add_parser("delete")
    _core_json(entity_delete)
    entity_delete.add_argument("repository")
    entity_delete.add_argument("entity_id", type=int)
    entity_delete.add_argument("--yes", action="store_true")
    entity_delete.set_defaults(handler=cmd_catalog_entity_delete)

    bundle = commands.add_parser("bundle", help="Retrieve a hydrated WEMI entity bundle.")
    _core_json(bundle)
    bundle.add_argument("level")
    bundle.add_argument("entity_id", type=int)
    bundle.set_defaults(handler=cmd_catalog_bundle)
    graph = commands.add_parser("graph", help="Retrieve a bounded WEMI graph for one Work.")
    _core_json(graph)
    graph.add_argument("work_id", type=int)
    graph.add_argument("--max-expressions", type=int, default=100)
    graph.add_argument("--max-manifestations", type=int, default=500)
    graph.add_argument("--max-items", type=int, default=1000)
    graph.set_defaults(handler=cmd_catalog_graph)
    item = commands.add_parser("item", help="Retrieve one semantic Item summary.")
    _core_json(item)
    item.add_argument("item_id", type=int)
    item.set_defaults(handler=cmd_catalog_item_summary)
    hierarchy = commands.add_parser("hierarchy", help="List adjacent WEMI entities.")
    _core_json(hierarchy)
    hierarchy.add_argument("level")
    hierarchy.add_argument("entity_id", type=int)
    hierarchy.add_argument("--direction", choices=("children", "parents"), default="children")
    hierarchy.set_defaults(handler=cmd_catalog_hierarchy)

    identifiers = commands.add_parser("identifiers", help="Read or replace entity identifiers.")
    identifier_commands = identifiers.add_subparsers(dest="identifier_action", required=True)
    identifier_list = identifier_commands.add_parser("list")
    _core_json(identifier_list)
    identifier_list.add_argument("level")
    identifier_list.add_argument("entity_id", type=int)
    identifier_list.add_argument("--primary-only", action="store_true")
    identifier_list.set_defaults(handler=cmd_catalog_identifiers)
    identifier_set = identifier_commands.add_parser("set")
    _core_json(identifier_set)
    identifier_set.add_argument("level")
    identifier_set.add_argument("entity_id", type=int)
    identifier_set.add_argument("identifiers_file")
    identifier_set.set_defaults(handler=cmd_catalog_identifiers_set)

    agents = commands.add_parser("agents", help="Resolve, list, and link catalogue agents.")
    agent_commands = agents.add_subparsers(dest="agent_action", required=True)
    agent_list = agent_commands.add_parser("list")
    _core_json(agent_list)
    agent_list.add_argument("level")
    agent_list.add_argument("entity_id", type=int)
    agent_list.add_argument("--role")
    agent_list.set_defaults(handler=cmd_catalog_agents)
    resolve = agent_commands.add_parser("resolve")
    _core_json(resolve)
    resolve.add_argument("name")
    resolve.add_argument("--role")
    resolve.set_defaults(handler=cmd_catalog_agent_resolve)
    for action in ("create-person", "create-organisation"):
        create_agent = agent_commands.add_parser(
            action,
            help="Create an Agent from a complete CLI-host JSON specification.",
        )
        _core_json(create_agent)
        create_agent.add_argument("spec_file")
        create_agent.set_defaults(handler=cmd_catalog_agent_create)
    link = agent_commands.add_parser("link")
    _core_json(link)
    link.add_argument("agent_id", type=int)
    link.add_argument("level")
    link.add_argument("entity_id", type=int)
    link.add_argument("--role")
    link.add_argument("--priority", type=int)
    link.set_defaults(handler=cmd_catalog_agent_link)

    annotations = commands.add_parser(
        "annotations", help="List annotations attached to one Item."
    )
    _core_json(annotations)
    annotations.add_argument("item_id", type=int)
    annotations.add_argument("--user-id", type=int)
    annotations.add_argument("--kind")
    annotations.set_defaults(handler=cmd_catalog_annotations)

    wemi = commands.add_parser("wemi", help="Create or change explicit WEMI relationships.")
    wemi_commands = wemi.add_subparsers(dest="wemi_action", required=True)
    wemi_create = wemi_commands.add_parser("create")
    _core_json(wemi_create)
    wemi_create.add_argument("spec_file", help="JSON with work, expression, manifestation, and optional items.")
    wemi_create.set_defaults(handler=cmd_catalog_wemi_create)
    wemi_link = wemi_commands.add_parser("link")
    _core_json(wemi_link)
    wemi_link.add_argument("parent_level")
    wemi_link.add_argument("parent_id", type=int)
    wemi_link.add_argument("child_level")
    wemi_link.add_argument("child_id", type=int)
    wemi_link.add_argument("--primary", action=argparse.BooleanOptionalAction, default=None)
    wemi_link.add_argument("--priority", type=int)
    wemi_link.add_argument("--origin")
    wemi_link.set_defaults(handler=cmd_catalog_wemi_link)
    wemi_unlink = wemi_commands.add_parser("unlink")
    _core_json(wemi_unlink)
    wemi_unlink.add_argument("parent_level")
    wemi_unlink.add_argument("parent_id", type=int)
    wemi_unlink.add_argument("child_level")
    wemi_unlink.add_argument("child_id", type=int)
    wemi_unlink.add_argument("--yes", action="store_true")
    wemi_unlink.set_defaults(handler=cmd_catalog_wemi_unlink)

    catalog_metadata = commands.add_parser(
        "metadata", help="Attach, replace, or merge semantic WEMI metadata."
    )
    metadata_commands = catalog_metadata.add_subparsers(
        dest="catalog_metadata_action", required=True
    )
    for action in ("attach", "replace"):
        metadata_write = metadata_commands.add_parser(action)
        _core_json(metadata_write)
        metadata_write.add_argument("level")
        metadata_write.add_argument("entity_id", type=int)
        metadata_write.add_argument("data_file")
        metadata_write.set_defaults(handler=cmd_catalog_metadata_write)
    metadata_merge = metadata_commands.add_parser("merge")
    _core_json(metadata_merge)
    metadata_merge.add_argument("level")
    metadata_merge.add_argument("source_id", type=int)
    metadata_merge.add_argument("target_id", type=int)
    metadata_merge.add_argument("--yes", action="store_true")
    metadata_merge.set_defaults(handler=cmd_catalog_metadata_merge)

    fields = commands.add_parser("fields", help="Inspect public metadata-field definitions.")
    _core_json(fields)
    fields.add_argument("--key")
    fields.add_argument("--kind", default="all")
    fields.add_argument("--no-composites", action="store_true")
    fields.set_defaults(handler=cmd_catalog_fields)

    custom_fields = commands.add_parser(
        "custom-fields", help="Administer semantic user-defined metadata fields."
    )
    custom_commands = custom_fields.add_subparsers(
        dest="custom_fields_action", required=True
    )
    custom_list = custom_commands.add_parser("list")
    _core_json(custom_list)
    custom_list.set_defaults(handler=cmd_catalog_custom_fields_list)
    custom_show = custom_commands.add_parser("show", aliases=["get"])
    _core_json(custom_show)
    custom_show.add_argument("field", help="Numeric id or exact label.")
    custom_show.set_defaults(handler=cmd_catalog_custom_fields_show)
    custom_create = custom_commands.add_parser("create")
    _core_json(custom_create)
    custom_create.add_argument("name")
    custom_create.add_argument("--label")
    custom_create.add_argument("--datatype", default="text")
    custom_create.add_argument("--multiple", action="store_true")
    custom_create.add_argument("--not-editable", action="store_true")
    custom_create.add_argument("--table", default="books")
    custom_create.add_argument("--display-file")
    custom_create.add_argument(
        "--make-category", action=argparse.BooleanOptionalAction, default=None
    )
    custom_create.set_defaults(handler=cmd_catalog_custom_fields_create)
    custom_update = custom_commands.add_parser("update")
    _core_json(custom_update)
    custom_update.add_argument("num", type=int)
    custom_update.add_argument("changes_file", nargs="?")
    custom_update.add_argument("--name")
    custom_update.add_argument("--label")
    custom_update.add_argument(
        "--editable", action=argparse.BooleanOptionalAction, default=None
    )
    custom_update.add_argument("--display-file")
    custom_update.set_defaults(handler=cmd_catalog_custom_fields_update)
    custom_delete = custom_commands.add_parser("delete")
    _core_json(custom_delete)
    identity = custom_delete.add_mutually_exclusive_group(required=True)
    identity.add_argument("--num", type=int)
    identity.add_argument("--label")
    custom_delete.add_argument("--yes", action="store_true")
    custom_delete.set_defaults(handler=cmd_catalog_custom_fields_delete)

    browse = commands.add_parser("browse", help="Browse categories and Work projections.")
    browse_commands = browse.add_subparsers(dest="browse_action", required=True)
    categories = browse_commands.add_parser("categories")
    _core_json(categories)
    categories.set_defaults(handler=cmd_browse_categories)
    category = browse_commands.add_parser("category")
    _core_json(category)
    category.add_argument("category")
    category.add_argument("--limit", type=int, default=100)
    category.add_argument("--offset", type=int, default=0)
    category.add_argument("--sort", default="name")
    category.add_argument("--descending", action="store_true")
    category.set_defaults(handler=cmd_browse_category)
    works = browse_commands.add_parser("works")
    _core_json(works)
    works.add_argument("--category")
    works.add_argument("--category-id", type=int)
    works.add_argument("--text")
    works.add_argument("--limit", type=int, default=100)
    works.add_argument("--offset", type=int, default=0)
    works.add_argument("--sort", default="title")
    works.add_argument("--descending", action="store_true")
    works.set_defaults(handler=cmd_browse_works)
    work = browse_commands.add_parser("work")
    _core_json(work)
    work.add_argument("work_id", type=int)
    work.set_defaults(handler=cmd_browse_work)


def build_acquisition_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register acquire/acquisition discovery and binary get/read command leaves.

    Formats, cover, and resolve publish JSON; get/read instead requires a byte
    destination and an explicit replacement flag for overwriting files. Parser
    construction does not resolve resources, open Core, or touch the destination.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_acquisition_parser(root.add_subparsers())
        >>> args = root.parse_args(["acquire", "read", "file", "7", "book.epub"])
        >>> args.resource_id, args.file_output, args.replace_file_output
        (7, 'book.epub', False)


    :param subparsers: Root CLI collection receiving the acquisition family.
    :return: None; all leaves and aliases are added with their handler defaults.
    """
    parser = subparsers.add_parser(
        "acquire", aliases=["acquisition"], help="Resolve and safely retrieve catalogue resources."
    )
    commands = parser.add_subparsers(dest="acquire_action", required=True)
    formats = commands.add_parser("formats", help="List downloadable formats for one Work.")
    _core_json(formats)
    formats.add_argument("work_id", type=int)
    formats.set_defaults(handler=cmd_acquire_query)
    cover = commands.add_parser("cover", help="List cover resources for one Work.")
    _core_json(cover)
    cover.add_argument("work_id", type=int)
    cover.set_defaults(handler=cmd_acquire_query)
    resolve = commands.add_parser("resolve", help="Resolve one resource without reading bytes.")
    _core_json(resolve)
    resolve.add_argument("kind")
    resolve.add_argument("resource_id", type=int)
    resolve.set_defaults(handler=cmd_acquire_query)
    get = commands.add_parser("get", aliases=["read"], help="Read one resource to a CLI-host file.")
    add_connection_arguments(get)
    get.add_argument("kind")
    get.add_argument("resource_id", type=int)
    get.add_argument("file_output")
    get.add_argument("--replace-file-output", action="store_true")
    get.set_defaults(handler=cmd_acquire_get)


__all__ = ["build_acquisition_parser", "build_catalog_parser"]
