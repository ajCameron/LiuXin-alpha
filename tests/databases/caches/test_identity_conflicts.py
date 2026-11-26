"""
Report name collisions as a typed Catalog error instead of a raw driver error.

Renaming a Tag, Label or Series to a name another row holds, moving a Genre
beside a same-named sibling, or creating a duplicate used to surface the
driver's own IntegrityError (sqlite3 or psycopg), which Core reported only as a
generic handler error.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.catalog.api.common import CatalogIdentityConflictError
from LiuXin_alpha.core.errors import core_error_details
from tests.databases.caches.test_metadata_api import MODES, service
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("mode", MODES)
def test_renames_moves_and_creates_report_the_existing_row(metadata_db, mode):
    """
    Raise CatalogIdentityConflictError naming the row that holds the identity.

    Example:
        >>> test_renames_moves_and_creates_report_the_existing_row(db, None)  # doctest: +SKIP


    :param metadata_db: Fresh catalogue.
    :param mode: Cache backend, or None for direct database reads.
    :return: ``None`` after checking each collision and the unchanged rows.
    """
    api, cache = service(metadata_db, mode)
    try:
        alpha = api.ensure_tag("Alpha").result
        beta = api.ensure_tag("Beta").result
        first_label = api.ensure_label("First").result
        second_label = api.ensure_label("Second").result
        first_series = api.ensure_series("First Series").result
        second_series = api.ensure_series("Second Series").result
        fiction, nested = api.genres.ensure_path(["Fiction", "Fantasy"]).result
        (root,) = api.genres.ensure_path(["Fantasy"]).result
        (other,) = api.genres.ensure_path(["Other"]).result
        cases = (
            (lambda: api.tags.update(beta, {"name": " ALPHA "}), "tags", alpha),
            (
                lambda: api.labels.update(second_label, {"name": "first"}),
                "labels",
                first_label,
            ),
            (
                lambda: api.series.update(second_series, {"name": "first series"}),
                "series",
                first_series,
            ),
            (lambda: api.genres.move(root, parent_id=fiction), "genres", nested),
            (lambda: api.genres.update(other, {"name": "FICTION"}), "genres", fiction),
            (lambda: api.tags.create({"name": "beta"}), "tags", beta),
        )
        for operation, table, existing in cases:
            with pytest.raises(CatalogIdentityConflictError) as caught:
                operation()
            assert (caught.value.table, caught.value.existing_id) == (table, existing)
            assert core_error_details(caught.value) == (
                "identity_conflict",
                {"table": table, "existing_id": existing},
            )
        assert [record.fields["name"] for record in api.tags.list()] == [
            "Alpha",
            "Beta",
        ]
        assert api.genres.require(root).fields["parent_id"] is None
        assert api.tags.update(alpha, {"name": "ALPHA"}).changed
    finally:
        if cache is not None:
            cache.close()
