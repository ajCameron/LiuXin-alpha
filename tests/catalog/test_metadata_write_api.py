"""
Verify metadata writer construction and public mutation contract.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata write api through its owning regression module::

        python -m pytest -q tests/catalog/test_metadata_write_api.py
"""

from __future__ import annotations

from LiuXin_alpha.catalog.api import (
    MetadataWriteReportAPI,
    MetadataWriteReportMapping,
)
from LiuXin_alpha.catalog.api import __all__ as catalog_api_all
from LiuXin_alpha.catalog.mutations import CatalogMetadataWriteReport


def test_metadata_write_report_contract_is_exported_from_api_root() -> None:
    """
    Verify the metadata write report contract is exported from Catalog's API root.

    Example:
        Exercise test metadata write report contract is exported from api root through its owning regression module::

            python -m pytest -q tests/catalog/test_metadata_write_api.py


    :return: None; the function records state or raises through its assertions.
    """
    assert "MetadataWriteReportAPI" in catalog_api_all
    assert "MetadataWriteReportMapping" in catalog_api_all


def test_concrete_wemi_write_report_satisfies_public_protocol() -> None:
    """
    Verify concrete wemi write report satisfies public protocol.

    Example:
        Exercise test concrete wemi write report satisfies public protocol through its owning regression module::

            python -m pytest -q tests/catalog/test_metadata_write_api.py


    :return: None; the function records state or raises through its assertions.
    """
    report: MetadataWriteReportAPI = CatalogMetadataWriteReport(
        item_id=7,
        target_level="work",
        target_table="works",
        target_id=11,
    )

    assert isinstance(report, MetadataWriteReportAPI)

    report.fields_checked.append("tags")
    report.skipped.append("tags: already linked")

    assert report.changed is False

    report.links_added.append(
        {
            "field": "tags",
            "source": {"table": "works", "row_id": 11},
            "target": {"table": "tags", "row_id": 5},
        }
    )
    payload: MetadataWriteReportMapping = report.to_mapping()

    assert report.changed is True
    assert set(payload) == {
        "item_id",
        "target_level",
        "target_table",
        "target_id",
        "fields_checked",
        "rows_added",
        "rows_updated",
        "rows_removed",
        "links_added",
        "links_removed",
        "skipped",
        "errors",
        "changed",
    }
    assert payload["changed"] is True
    assert payload["fields_checked"] == ["tags"]
    assert payload["skipped"] == ["tags: already linked"]
    assert payload["links_added"] == [
        {
            "field": "tags",
            "source": {"table": "works", "row_id": 11},
            "target": {"table": "tags", "row_id": 5},
        }
    ]

    payload["fields_checked"].append("labels")
    assert report.fields_checked == ["tags"]
