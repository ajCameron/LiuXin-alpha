"""
Keep shared lookups and suggestions off the per-row hydration path.

Pickers used to wrap every population row in a ``Row``, whose construction runs
schema discovery; a suggestion over 2,000 tags took about two seconds on every
backend. Exact lookups also read whole populations they could narrow.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.caches.metadata.reads import MetadataReadSource
from LiuXin_alpha.databases.row import Row
from tests.databases.caches.test_metadata_api import MODES, service
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("mode", MODES)
def test_pickers_never_build_hydration_rows(metadata_db, monkeypatch, mode):
    """
    Answer lookups and suggestions from plain row values on every source.

    Example:
        >>> test_pickers_never_build_hydration_rows(db, mp, None)  # doctest: +SKIP


    :param metadata_db: Fresh catalogue.
    :param monkeypatch: Fixture used to forbid Row construction.
    :param mode: Cache backend, or None for direct database reads.
    :return: ``None`` after checking results with Row construction forbidden.
    """
    api, cache = service(metadata_db, mode)
    try:
        tag = api.ensure_tag("Space Opera").result
        api.ensure_tag("Space Western")
        agent = api.agents.create_person(
            {"name": "Ursula Le Guin", "aliases": ["U. K. Le Guin"]}
        ).result

        def forbidden(*args, **kwargs):
            """
            Fail if a picker read wraps a row for hydration.

            Example:
                Installed as ``Row.__init__`` for the duration of the checks.


            :param args: Ignored positional constructor arguments.
            :param kwargs: Ignored keyword constructor arguments.
            :return: Never returns; always raises AssertionError.
            """
            raise AssertionError("picker reads must not construct Row objects")

        monkeypatch.setattr(Row, "__init__", forbidden)
        assert [
            r.entity_id for r in api.tags.lookup({"name": " SPACE  opera "}).records
        ] == [tag]
        assert api.tags.suggest("space").total_count == 2
        assert [
            r.entity_id for r in api.agents.lookup({"name": "ursula le guin"}).records
        ] == [agent]
        assert [r.entity_id for r in api.agents.suggest("u. k.").records] == [agent]
    finally:
        if cache is not None:
            cache.close()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("normalized_search", (True, False))
def test_exact_lookup_reads_only_text_key_candidates(
    metadata_db, monkeypatch, mode, normalized_search
):
    """
    Narrow exact text lookups by text key while keeping the exact rule.

    Without normalized SQL search the read falls back to the whole population;
    the answer is the same either way.

    Example:
        >>> test_exact_lookup_reads_only_text_key_candidates(db, mp, None, True)  # doctest: +SKIP


    :param metadata_db: Fresh catalogue.
    :param monkeypatch: Fixture used to observe population reads.
    :param mode: Cache backend, or None for direct database reads.
    :param normalized_search: Whether the driver reports normalized SQL search.
    :return: ``None`` after checking which reads ran and what they found.
    """
    api, cache = service(metadata_db, mode)
    try:
        first = api.ensure_tag("Ｈｉｓｔｏｒｙ & Nature").result
        api.ensure_tag("History of Art")
        if not normalized_search:
            monkeypatch.setattr(
                type(metadata_db.macros),
                "supports_normalized_search",
                lambda self: False,
            )
        whole: list[str] = []
        original = MetadataReadSource.get_all_row_dicts

        def observed(self, table):
            """
            Record whole-population reads.

            Example:
                Installed as ``MetadataReadSource.get_all_row_dicts``.


            :param self: Read source being observed.
            :param table: Physical table whose population is read.
            :return: The original method's rows.
            """
            whole.append(table)
            return original(self, table)

        monkeypatch.setattr(MetadataReadSource, "get_all_row_dicts", observed)
        page = api.tags.lookup({"text": "history  &  NATURE"})
        assert [record.entity_id for record in page.records] == [first]
        assert page.total_count == 1
        assert whole == []
        assert api.tags.lookup({"text": "history"}).records == ()
        api.tags.suggest("history")
        assert whole == ["tags"]
    finally:
        if cache is not None:
            cache.close()


def test_interlinked_tables_are_kept_until_the_schema_version_changes(
    metadata_db, monkeypatch
):
    """
    Ask the schema version once per call and recompute only after it changes.

    Example:
        >>> test_interlinked_tables_are_kept_until_the_schema_version_changes(db, mp)  # doctest: +SKIP


    :param metadata_db: Fresh catalogue.
    :param monkeypatch: Fixture used to count and change schema versions.
    :return: ``None`` after checking cached and recomputed answers.
    """
    wrapper = metadata_db.driver_wrapper
    expected = wrapper.get_interlinked_tables("tags")
    assert "works" in expected
    versions = {"value": 1, "calls": 0}

    def version():
        """
        Count schema-version queries and report the current test version.

        Example:
            Installed as the driver's ``direct_get_schema_version``.


        :return: The version the test currently reports.
        """
        versions["calls"] += 1
        return versions["value"]

    monkeypatch.setattr(wrapper.driver, "direct_get_schema_version", version)
    wrapper.get_interlinked_tables("tags")
    before = versions["calls"]
    first = wrapper.get_interlinked_tables("tags")
    assert versions["calls"] == before + 1
    assert first == expected
    first.add("not a table")
    assert wrapper.get_interlinked_tables("tags") == expected
    computed = []
    original = type(wrapper).get_link_table_name

    def counted(self, table1, table2):
        """
        Record link-name lookups made while recomputing.

        Example:
            Installed as the wrapper's ``get_link_table_name``.


        :param self: Driver wrapper being observed.
        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :return: The original method's answer.
        """
        computed.append((table1, table2))
        return original(self, table1, table2)

    monkeypatch.setattr(type(wrapper), "get_link_table_name", counted)
    wrapper.get_interlinked_tables("tags")
    assert computed == []
    versions["value"] = 2
    assert wrapper.get_interlinked_tables("tags") == expected
    assert computed
