"""Resolve declared physical relationships for table-specific SQL readers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.errors import InputIntegrityError

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.schema_specs import StorageLinkSpec


def require_link_spec(
    database: DatabaseAPI, source: str, destination: str
) -> StorageLinkSpec:
    """Require an oriented physical link before handing it to portable SQL.

    Missing relationships are invalid requests, while declared links with no rows
    produce empty reader results. Resolution does not open a transaction or infer
    a bibliographic projection.
    """
    spec = database.driver_wrapper.get_link_spec(source, destination)
    if spec is None:
        raise InputIntegrityError(
            f"No physical relationship links {source!r} to {destination!r}"
        )
    return spec
