"""
Refuse to delete linked Tags, Labels, Ratings and Agents unless detach=True.

Tree vocabularies already refused in-use deletion. Flat vocabulary and Agents
used to cascade silently: deleting an author removed their credits from every
Work, Expression, Manifestation and Item.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.catalog.api.common import CatalogInUseError, CatalogNotFoundError
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.core.errors import (
    CoreDispatchError,
    CoreHandlerError,
    core_error_details,
)
from LiuXin_alpha.library import Library
from tests.databases.caches.test_metadata_api import MODES, service, stack
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("mode", MODES)
def test_linked_values_and_agents_need_detach(metadata_db, mode):
    """
    Refuse linked deletions with their link counts, then delete with detach=True.

    Example:
        >>> test_linked_values_and_agents_need_detach(db, None)  # doctest: +SKIP


    :param metadata_db: Fresh catalogue.
    :param mode: Cache backend, or None for direct database reads.
    :return: ``None`` after checking refusals, detached deletes and plain deletes.
    """
    api, cache = service(metadata_db, mode)
    try:
        created = stack(api).result
        work = created.work_id
        tag = api.works.add_tag(work, text="Linked").result
        agent = api.agents.create_person({"name": "Credited Author"}).result
        for entities, entity_id in (
            (api.works, work),
            (api.expressions, created.expression_id),
            (api.items, created.item_ids[0]),
        ):
            entities.add_credit(entity_id, agent_id=agent, role="aut")

        with pytest.raises(CatalogInUseError) as refused:
            api.tags.delete(tag)
        assert refused.value.references == {"tag_work_links": 1}
        assert core_error_details(refused.value) == (
            "in_use",
            {"references": {"tag_work_links": 1}},
        )
        with pytest.raises(CatalogInUseError) as credited:
            api.agents.delete(agent)
        assert credited.value.references == {
            "agent_expression_links": 1,
            "agent_item_links": 1,
            "agent_work_links": 1,
        }
        assert api.tags.require(tag).fields["name"] == "Linked"
        assert len(api.works.list_credits(work)) == 1
        with pytest.raises(TypeError):
            api.tags.delete(tag, detach=1)

        assert api.agents.delete(agent, detach=True).changed
        assert api.works.list_credits(work) == ()
        assert api.tags.delete(tag, detach=True).changed
        assert api.works.list_tags(work) == ()

        unused = api.ensure_label("Unused").result
        assert api.labels.delete(unused).changed
        with pytest.raises(CatalogNotFoundError):
            api.labels.require(unused)
    finally:
        if cache is not None:
            cache.close()


def test_core_entity_delete_honours_detach(metadata_db):
    """
    Apply the same refusal to Core's catalog.entity.delete command.

    Example:
        >>> test_core_entity_delete_honours_detach(db)  # doctest: +SKIP


    :param metadata_db: Fresh catalogue.
    :return: ``None`` after a refused, a rejected and a detached Core delete.
    """
    catalog = Catalog(metadata_db)
    work = catalog.works.create({"title": "Core Work"})
    tag, _created = catalog.tags.ensure({"name": "Core Tag"})
    catalog.tags.link_to_work(entity_id=tag, work_id=work)
    runtime = CoreRuntime(library=Library(database=metadata_db))
    try:
        command = {"repository": "tags", "entity_id": tag}
        with pytest.raises(CoreHandlerError) as refused:
            runtime.command("catalog.entity.delete", command)
        assert refused.value.code == "in_use"
        assert refused.value.details == {"references": {"tag_work_links": 1}}
        with pytest.raises((CoreDispatchError, CoreHandlerError)):
            runtime.command("catalog.entity.delete", {**command, "detach": "yes"})
        deleted = runtime.command("catalog.entity.delete", {**command, "detach": True})
        assert deleted["deleted"]["tag_id"] == tag
        assert catalog.tags.get(tag) is None
        assert catalog.works.get(work) is not None
    finally:
        runtime.shutdown()
