"""Keep table-oriented application writes inside Catalog's semantic boundary."""

from __future__ import annotations

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.library.library import Library


def test_administrative_identifier_crud_uses_catalog_validation(db) -> None:
    """Generic forms cannot insert or partially update an invalid curated pair."""
    catalog = Catalog(db)
    runtime = CoreRuntime(library=Library(database=db), catalog=catalog)
    try:
        assert (
            catalog.repositories.for_table("entity_identifiers") is catalog.identifiers
        )
        assert catalog.repositories.for_table("works") is catalog.works
        assert catalog.repositories.for_table("titles") is None
        assert catalog.repositories.for_table("stores") is None
        work_id = catalog.works.create({"title": "Administrative write parity"})
        values = {
            "entity_identifier_entity_type": "work",
            "entity_identifier_entity_id": work_id,
            "entity_identifier_scheme": "DOI",
            "entity_identifier_value": "https://doi.org/10.1000/write-parity",
        }
        result = runtime.command(
            "admin.row.create", {"table": "entity_identifiers", "values": values}
        )
        record = result["record"]
        row_id = record["row_id"]
        assert record["values"]["entity_identifier_scheme"] == "doi"
        assert (
            record["values"]["entity_identifier_value"]
            == values["entity_identifier_value"]
        )
        assert record["values"] == dict(catalog.identifiers.require(row_id))

        before = tuple(catalog.identifiers.list(limit=10000))
        with pytest.raises(CoreHandlerError):
            runtime.command(
                "admin.row.create",
                {
                    "table": "entity_identifiers",
                    "values": {
                        **values,
                        "entity_identifier_scheme": "isbn",
                        "entity_identifier_value": "invalid-checksum",
                    },
                },
            )
        with pytest.raises(CoreHandlerError):
            runtime.command(
                "admin.row.update",
                {
                    "table": "entity_identifiers",
                    "row_id": row_id,
                    "updates": {"entity_identifier_scheme": "isbn"},
                },
            )
        assert tuple(catalog.identifiers.list(limit=10000)) == before

        updated = runtime.command(
            "admin.row.update",
            {
                "table": "entity_identifiers",
                "row_id": row_id,
                "updates": {
                    "entity_identifier_value": "https://doi.org/10.1000/updated-parity"
                },
            },
        )
        assert (
            updated["record"]["values"]["entity_identifier_value"]
            == "https://doi.org/10.1000/updated-parity"
        )
        deleted = runtime.command(
            "admin.row.delete", {"table": "entity_identifiers", "row_id": row_id}
        )
        assert deleted["deleted"] == updated["record"]["values"]
        assert catalog.identifiers.get(row_id) is None
    finally:
        runtime.shutdown()
