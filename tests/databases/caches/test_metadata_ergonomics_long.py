"""Opt-in metadata picker, hydration and membership churn with measured operations."""

from dataclasses import replace

import pytest

from LiuXin_alpha.catalog.api.metadata_search import MetadataSearchRequest
from tests.databases.caches.test_metadata_api import MODES, service
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)
from tests.support.cache_long_runs import long_marks
from tests.support.cache_long_runs import measurements as measurements
from tests.support.cache_long_runs import workload as workload

pytestmark = long_marks()


@pytest.mark.parametrize("mode", MODES)
def test_metadata_picker_hydration_and_membership_churn(
    metadata_db, mode, workload, measurements
):
    """Compare composed pages with the reference search through repeated add/remove cycles."""
    api, cache = service(metadata_db, mode)

    def populate(current):
        tags = tuple(
            current.tags.create({"name": f"Picker {index:05d}"}).result
            for index in range(workload.records)
        )
        works = tuple(
            current.works.create({"title": f"Document {index:05d}"}).result
            for index in range(workload.database_records)
        )
        agent = current.agents.create_person({"name": "Long-run Writer"}).result
        return tags, works, agent

    try:
        tags, works, agent = measurements.measure(
            "populate", api.batch, populate
        ).result
        request = MetadataSearchRequest(query="work.title:Document", limit=7)
        for cycle in range(workload.cycles):
            page = measurements.measure(
                "picker", api.tags.suggest, "Picker", limit=17, offset=cycle
            )
            assert page.total_count == len(tags)
            assert (
                tuple(record.entity_id for record in page.records)
                == tags[cycle : cycle + 17]
            )
            search = measurements.measure(
                "hydrate",
                api.works.search,
                request.query,
                limit=request.limit,
                offset=cycle,
            )
            reference = api.search.query(replace(request, offset=cycle))
            assert search.references == reference.references
            assert search.total_count == len(works)
            assert len(search.records) == len(search.references)
            selected = tags[cycle : cycle + 7]

            def edits(current, selected=selected):
                for work in works:
                    current.works.set_tags(work, selected)
                    current.works.add_credit(work, agent_id=agent, role="aut")
                    current.works.add_credit(work, agent_id=agent, role="edt")
                    current.works.remove_credit(work, agent_id=agent, role="edt")

            measurements.measure("relationship_cycle", api.batch, edits)
            for work in works:
                assert {
                    member.member.entity_id for member in api.works.list_tags(work)
                } == set(selected)
                assert [
                    (credit.agent.entity_id, credit.role)
                    for credit in api.works.list_credits(work)
                ] == [(agent, "aut")]
            duplicates = measurements.measure(
                "bulk", api.tags.get_many, (tags[0], tags[-1], tags[0])
            )
            assert duplicates[0] == duplicates[2] and duplicates[0] is not duplicates[2]
    finally:
        if cache is not None:
            cache.close()
