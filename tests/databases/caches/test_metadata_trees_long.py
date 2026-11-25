"""Opt-in deep and wide vocabulary construction with measured cached traversal."""

import os

import pytest

from LiuXin_alpha.catalog.api.metadata_trees import (
    HierarchyLimitError,
    VocabularyTreeInput,
)
from tests.databases.caches.test_metadata_api import MODES, service
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)
from tests.support.cache_long_runs import measurements as measurements
from tests.support.cache_long_runs import workload as workload

pytestmark = [
    pytest.mark.slow,
    pytest.mark.cache_long,
    pytest.mark.skipif(
        os.environ.get("LIUXIN_RUN_CACHE_LONG_TESTS") != "1",
        reason="Set LIUXIN_RUN_CACHE_LONG_TESTS=1 to run the vocabulary tree stress suite",
    ),
]


@pytest.mark.parametrize("mode", MODES)
def test_deep_and_wide_vocabulary_trees(metadata_db, mode, workload, measurements):
    """Traverse beyond Python recursion depth and reject bounded partial output."""
    api, cache = service(metadata_db, mode)
    depth, width = workload.database_records, workload.records
    chain = VocabularyTreeInput({"name": f"Depth {depth}"})
    for index in reversed(range(depth)):
        chain = VocabularyTreeInput({"name": f"Depth {index}"}, (chain,))
    wide = VocabularyTreeInput(
        {"name": "Wide"},
        tuple(VocabularyTreeInput({"name": f"Child {i}"}) for i in range(width)),
    )
    try:
        deep_ids = measurements.measure(
            "deep_create", api.genres.create_tree, chain, max_nodes=depth + 1
        ).result
        wide_ids = measurements.measure(
            "wide_create", api.genres.create_tree, wide, max_nodes=width + 1
        ).result
        assert len(deep_ids) == depth + 1 and len(wide_ids) == width + 1
        assert (
            tuple(
                row.entity_id
                for row in measurements.measure(
                    "deep_read", api.genres.subtree, deep_ids[0], max_nodes=depth + 1
                )
            )
            == deep_ids
        )
        assert (
            tuple(
                row.entity_id
                for row in measurements.measure(
                    "wide_read", api.genres.subtree, wide_ids[0], max_nodes=width + 1
                )
            )
            == wide_ids
        )
        assert len(api.genres.ancestors(deep_ids[-1], max_nodes=depth)) == depth
        with pytest.raises(HierarchyLimitError):
            api.genres.subtree(wide_ids[0], max_nodes=width)
        reverse = tuple(reversed(wide_ids[1:]))
        measurements.measure(
            "wide_reorder",
            api.genres.reorder,
            parent_id=wide_ids[0],
            entity_ids=reverse,
        )
        assert (
            tuple(
                row.entity_id for row in api.genres.children(wide_ids[0], limit=width)
            )
            == reverse
        )
    finally:
        if cache is not None:
            cache.close()
