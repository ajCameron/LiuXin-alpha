"""
Create Agent aggregates from modern and legacy Creator/Publisher inputs.
"""

from __future__ import unicode_literals

import datetime

from typing import Any, Iterable, Optional, Sequence, Union

from LiuXin_alpha.utils.libraries.liuxin_six import string_types

from LiuXin_alpha.databases.api import RowAPI
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.metadata.constants import CREATOR_TYPES
from LiuXin_alpha.metadata.utils import author_to_author_sort
from LiuXin_alpha.utils.language_tools import best_effort_language_id
from LiuXin_alpha.utils.logging import default_log


class AgentCreatorOrgMixin:
    """
    Supply Agent aggregate creation to a host with database and peer helpers.

    Methods call shared helpers through self; a composing class can override
    coercion behavior. Add uses WEMI date coercion through its MRO. Sequential
    sidecar, link and identifier writes are not wrapped in a transaction.

    Example:
        A legacy Creator call builds a person Agent, a human sidecar and optional
        language/biography/image links.
    """

    @staticmethod
    def _coerce_iso_date(value: Optional[Union[str, datetime.date, datetime.datetime]]) -> Optional[str]:
        """
        Convert dates to ISO day strings and strip other values.

        Strings are not validated as dates. Add overrides this method through its
        MRO, using the WEMI variant which preserves string whitespace.

        Example:
            >>> AgentCreatorOrgMixin._coerce_iso_date(" 2026-01-02 ")
            '2026-01-02'


        :param value: Date/datetime, arbitrary stringifiable value, or None.
        :return: ISO date, stripped text, or None for absent/blank values.
        """

        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value.date().isoformat()
        if isinstance(value, datetime.date):
            return value.isoformat()
        text = str(value).strip()
        return text or None

    @staticmethod
    def _coerce_epoch_ms(value: Optional[Union[int, float, datetime.date, datetime.datetime, str]]) -> Optional[int]:
        """
        Coerce timestamps using the Agent mixin's permissive legacy rules.

        Integer inputs including bool pass through; floats truncate. Datetimes use
        timestamp(), and dates use local midnight. Naive values therefore depend on
        local timezone. Conversion errors can propagate. Add uses its WEMI override.

        Example:
            A negative numeric string is unsupported, whereas a negative integer is returned unchanged.


        :param value: Integer/float epoch milliseconds, date/datetime, digit string, or None.
        :return: Integer milliseconds, or None for unsupported input.
        """

        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, datetime.datetime):
            return int(value.timestamp() * 1000)
        if isinstance(value, datetime.date):
            dt = datetime.datetime(value.year, value.month, value.day)
            return int(dt.timestamp() * 1000)
        if isinstance(value, float):
            return int(value)
        if isinstance(value, string_types):
            text = value.strip()
            if text.isdigit():
                return int(text)
        return None

    @staticmethod
    def _normalize_aliases(agent_aliases: Optional[Union[str, Sequence[str]]]) -> Optional[str]:
        """
        Strip aliases and deduplicate by lowercase spelling in encounter order.

        Embedded (#BREAK#) text is not escaped or split; None entries are ignored.

        Example:
            >>> AgentCreatorOrgMixin._normalize_aliases([" A ", "a", None, "B"])
            'A(#BREAK#)B'


        :param agent_aliases: String treated as one alias, or sequence of stringifiable values; None allowed.
        :return: (#BREAK#)-joined first spellings, or None when empty.
        """

        if agent_aliases is None:
            return None

        if isinstance(agent_aliases, string_types):
            values = [agent_aliases]
        else:
            values = list(agent_aliases)

        normalized = []
        seen = set()
        for value in values:
            if value is None:
                continue
            alias = str(value).strip()
            if not alias:
                continue
            key = alias.lower()
            if key in seen:
                continue
            seen.add(key)
            normalized.append(alias)

        if not normalized:
            return None
        return "(#BREAK#)".join(normalized)

    def _has_insertable_table(self, table_name: str) -> bool:
        """
        Check table presence and reject views, treating inspection errors as absence.

        Example:
            A failed get_tables call returns False here rather than propagating its Exception.


        :param table_name: Exact table name.
        :return: True only for a listed non-view table.
        """

        try:
            if table_name not in set(self.db.get_tables()):
                return False
            return not self.db.driver_wrapper.is_view(table_name)
        except Exception:
            return False

    def _require_insertable_table(self, table_name: str, *, for_method: str) -> None:
        """
        Require a table that capability inspection considers writable.

        Example:
            A view named agents is rejected even though it appears in get_tables().


        :param table_name: Exact table name.
        :param for_method: Operation name included in the error message.
        :return: None when available.
        :raises InputIntegrityError: The table is absent, a view, or inspection failed.
        """

        if not self._has_insertable_table(table_name):
            raise InputIntegrityError(
                "Cannot run `{}`: schema does not expose insertable `{}`.".format(for_method, table_name)
            )

    def _set_row_values(self, row: RowAPI, payload: dict[str, Any]) -> None:
        """
        Assign allowed payload columns, then sync the Row once.

        Example:
            A None value for an allowed column is written rather than filtered out.


        :param row: Mutable Row exposing allowed_columns.
        :param payload: Column/value mapping; unknown columns are ignored.
        :return: None; synchronizes even an empty accepted payload.
        """

        for col, value in payload.items():
            if col in row.allowed_columns:
                row[col] = value
        row.sync()

    def _first_row_for_value(self, table: str, column: str, value: Any) -> Optional[RowAPI]:
        """
        Search one column and select the first returned Row.

        Example:
            Multiple matches are not treated as ambiguous; database order selects one.


        :param table: Table name passed to search.
        :param column: Column name passed to search.
        :param value: Exact search value passed unchanged.
        :return: First Row, or None for a false-valued result collection.
        """

        rows = self.db.search(table=table, column=column, search_term=value)
        if rows:
            return rows[0]
        return None

    def _upsert_sidecar(
        self,
        *,
        table: str,
        fk_column: str,
        fk_value: int,
        payload: dict[str, Any],
    ) -> RowAPI:
        """
        Update the first matching sidecar or insert a new payload.

        No uniqueness lock or transaction protects the lookup/insertion interval.

        Example:
            An existing row ignores unknown payload columns, while insertion passes the payload to Row creation.


        :param table: Sidecar table to search/insert.
        :param fk_column: Foreign-key column used to find an existing sidecar.
        :param fk_value: Owner ID used in lookup and as the insertion default.
        :param payload: Payload; can override the foreign-key default on insertion.
        :return: Existing updated Row or new Row.
        """

        current = self._first_row_for_value(table=table, column=fk_column, value=fk_value)
        if current is not None:
            self._set_row_values(current, payload)
            return current

        row_dict = {fk_column: fk_value}
        row_dict.update(payload)
        return Row.from_idless_row_dict(self.db, row_dict=row_dict, table=table)

    def _extract_agent_id(self, row: RowAPI) -> Optional[int]:
        """
        Try supported Agent and sidecar ID columns in priority order.

        Access exceptions are skipped; int conversion TypeError/ValueError are
        skipped too. Other conversion failures can propagate, and bool can become an ID.

        Example:
            agent_id precedes human_agent_agent_id, org_agent_agent_id and publisher_agent_id.


        :param row: Row-like object inspected without validating its table.
        :return: First value convertible with int(), or None.
        """

        for key in (
            "agent_id",
            "human_agent_agent_id",
            "org_agent_agent_id",
            "publisher_agent_id",
        ):
            try:
                value = row[key]
            except Exception:
                continue
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        return None

    def _safe_interlink(
        self,
        primary_row: RowAPI,
        secondary_row: RowAPI,
        *,
        priority: Union[int, float, str, None] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> RowAPI:
        """
        Retry with reversed endpoints after an InputIntegrityError.

        The first failed attempt may already have side effects; no rollback or
        transaction is provided. Errors from the second attempt propagate.

        Example:
            A backend DatabaseIntegrityError is not retried.


        :param primary_row: Initial primary Row.
        :param secondary_row: Initial secondary Row.
        :param priority: Priority passed unchanged to both attempts.
        :param type: Relationship type passed unchanged.
        :param col_value_pairs: Additional link columns passed unchanged.
        :return: Result of the first successful interlink attempt.
        """

        try:
            return self.db.interlink_rows(
                primary_row=primary_row,
                secondary_row=secondary_row,
                priority=priority,
                type=type,
                **col_value_pairs,
            )
        except InputIntegrityError:
            return self.db.interlink_rows(
                primary_row=secondary_row,
                secondary_row=primary_row,
                priority=priority,
                type=type,
                **col_value_pairs,
            )

    def _language_to_row(self, language: Union[RowAPI, str, int]) -> RowAPI:
        """
        Resolve a concrete Language Row, ID or text for Agent linking.

        Without Ensure, search language then language_code. Unresolved inputs use
        best_effort_language_id with strict=False before a final InputIntegrityError.

        Example:
            When Ensure is wired, text is delegated to ensure.language with its default name mode.


        :param language: Concrete Row returned directly, integer ID, or language text.
        :return: Resolved Row; a concrete Row is not checked for its table.
        :raises InputIntegrityError: No Language Row can be resolved.
        """

        if isinstance(language, Row):
            return language

        if isinstance(language, int):
            row = self.db.get_row_from_id("languages", int(language))
            if row is not None:
                return row

        if isinstance(language, string_types):
            if self.ensure is not None:
                return self.ensure.language(language)
            row = self._first_row_for_value("languages", "language", language)
            if row is not None:
                return row
            row = self._first_row_for_value("languages", "language_code", language)
            if row is not None:
                return row

        lang_id = best_effort_language_id(self.db, language, default=None, strict=False)
        if lang_id is not None:
            row = self.db.get_row_from_id("languages", int(lang_id))
            if row is not None:
                return row

        err_str = "Unable to parse language while linking to agent"
        err_str = default_log.log_variables(
            err_str,
            "ERROR",
            ("language", language),
            ("language_type", type(language)),
        )
        raise InputIntegrityError(err_str)

    def _note_to_row(self, note: Union[RowAPI, str]) -> RowAPI:
        """
        Reuse a concrete Row or insert text into notes.

        Example:
            A concrete Row is returned without checking its table; text requires an insertable table.


        :param note: Concrete Row or text; no blank-string validation.
        :return: Original Row or newly inserted text Row.
        :raises InputIntegrityError: Input is unsupported or the target table is not insertable.
        """

        if isinstance(note, Row):
            return note
        if isinstance(note, string_types):
            self._require_insertable_table("notes", for_method="agent note linking")
            return Row.from_idless_row_dict(self.db, row_dict={"note": note}, table="notes")
        err_str = "Unable to parse note while linking to agent"
        err_str = default_log.log_variables(
            err_str,
            "ERROR",
            ("note", note),
            ("note_type", type(note)),
        )
        raise InputIntegrityError(err_str)

    def _synopsis_to_row(self, synopsis: Union[RowAPI, str]) -> RowAPI:
        """
        Reuse a concrete Row or insert text into synopses.

        Example:
            A concrete Row is returned without checking its table; text requires an insertable table.


        :param synopsis: Concrete Row or text; no blank-string validation.
        :return: Original Row or newly inserted text Row.
        :raises InputIntegrityError: Input is unsupported or the target table is not insertable.
        """

        if isinstance(synopsis, Row):
            return synopsis
        if isinstance(synopsis, string_types):
            self._require_insertable_table("synopses", for_method="agent synopsis linking")
            return Row.from_idless_row_dict(self.db, row_dict={"synopsis": synopsis}, table="synopses")
        err_str = "Unable to parse synopsis while linking to agent"
        err_str = default_log.log_variables(
            err_str,
            "ERROR",
            ("synopsis", synopsis),
            ("synopsis_type", type(synopsis)),
        )
        raise InputIntegrityError(err_str)

    def _entity_identifier(
        self,
        *,
        agent_id: int,
        scheme: str,
        value: Optional[str],
        is_primary: Optional[int] = None,
        provenance: str = "add_agent_mixin",
    ) -> Optional[RowAPI]:
        """
        Insert an Agent identifier when value and table capability permit it.

        No normalization, reuse lookup or enclosing transaction is performed.

        Example:
            An unavailable entity_identifiers table silently skips the optional identifier.


        :param agent_id: Agent ID coerced with int().
        :param scheme: Scheme stored unchanged.
        :param value: False-valued input skips insertion.
        :param is_primary: Primary flag stored unchanged.
        :param provenance: Provenance string stored unchanged.
        :return: New identifier Row, or None for absent value/table capability.
        """

        if not value:
            return None
        if not self._has_insertable_table("entity_identifiers"):
            return None

        payload = {
            # We always tag these ids against the FRBR agent identity.
            "entity_identifier_entity_type": "agent",
            "entity_identifier_entity_id": int(agent_id),
            # Identifier namespace and canonical value (URL, IMDB id, etc.).
            "entity_identifier_scheme": scheme,
            "entity_identifier_value": value,
            # Priority and provenance help dedupe/trace importer decisions.
            "entity_identifier_is_primary": is_primary,
            "entity_identifier_provenance": provenance,
        }
        return Row.from_idless_row_dict(self.db, row_dict=payload, table="entity_identifiers")

    def agent(
        self,
        agent_canonical_name: str,
        *,
        agent_type: str = "person",
        agent_sort_name: Optional[str] = None,
        agent_aliases: Optional[Union[str, Sequence[str]]] = None,
        agent_note: Optional[str] = None,
        agent_created_timestamp_ep_k: Optional[Union[int, float, datetime.date, datetime.datetime, str]] = None,
        human_sidecar: Optional[dict[str, Any]] = None,
        org_sidecar: Optional[dict[str, Any]] = None,
        linked_languages: Optional[Iterable[Union[RowAPI, str, int]]] = None,
        linked_notes: Optional[Iterable[Union[RowAPI, str]]] = None,
        linked_synopses: Optional[Iterable[Union[RowAPI, str]]] = None,
        linked_images: Optional[Iterable[RowAPI]] = None,
    ) -> RowAPI:
        """
        Insert an Agent, then optional sidecars and related metadata.

        The agents table is checked before type validation. Sidecar capability checks
        and attachment resolution occur after Agent insertion; failures can leave a
        partial aggregate. Add resolves shared date helpers through WEMIAdderMixin
        before AgentCreatorOrgMixin. No encompassing transaction is opened.

        Example:
            Adding both sidecar payloads attempts both independently of the selected Agent type.


        :param agent_canonical_name: Canonical name stored unchanged; no local nonempty check.
        :param agent_type: Case/whitespace-normalized type; common person/organisation aliases are accepted.
        :param agent_sort_name: Sort name stored unchanged.
        :param agent_aliases: String or sequence; strip and deduplicate case-insensitively, joining with (#BREAK#).
        :param agent_note: Note stored unchanged.
        :param agent_created_timestamp_ep_k: Creation timestamp via the active coercion helper; also sets modification time.
        :param human_sidecar: Truthy person-sidecar payload to upsert after Agent insertion.
        :param org_sidecar: Truthy organisation-sidecar payload to upsert after Agent insertion.
        :param linked_languages: Language Rows, strings or IDs resolved and linked with native type/priority zero.
        :param linked_notes: Concrete Rows or strings inserted as Notes and linked at priority zero.
        :param linked_synopses: Concrete Rows or strings inserted as Synopses and linked at priority zero.
        :param linked_images: Only concrete Rows are linked as agent_photo; other values are ignored.
        :return: Created database Row; synchronization and schema errors propagate.
        :raises InputIntegrityError: Required tables, type or attachment inputs are unsupported.
        """
        self._require_insertable_table("agents", for_method="agent")

        normalized_type = str(agent_type).lower().strip()
        type_aliases = {
            "organisation": "organisation",
            "organization": "organisation",
            "org": "organisation",
            "company": "organisation",
            "publisher": "organisation",
            "group": "group",
            "pseudonym": "pseudonym",
            "person": "person",
            "human": "person",
            "author": "person",
            "creator": "person",
        }
        normalized_type = type_aliases.get(normalized_type, normalized_type)

        if normalized_type == "organisation":
            db_type = "organisation"
        elif normalized_type in {"person", "group", "pseudonym"}:
            db_type = normalized_type
        else:
            raise InputIntegrityError("agent_type not recognised for agents table: {!r}".format(agent_type))

        payload = {
            # Entity discriminator used by FRBR sidecar tables and link rules.
            "agent_type": db_type,
            # Canonical display and sorting values used by UI/search.
            "agent_canonical_name": agent_canonical_name,
            "agent_sort_name": agent_sort_name,
            # Aliases are stored as a normalized delimiter-joined string.
            "agent_aliases": self._normalize_aliases(agent_aliases),
            # Free-form annotation for provenance/import notes.
            "agent_note": agent_note,
        }

        created_epk = self._coerce_epoch_ms(agent_created_timestamp_ep_k)
        if created_epk is not None:
            # On create we initialize both timestamps to the same source value.
            payload["agent_created_timestamp_ep_k"] = created_epk
            payload["agent_modified_timestamp_ep_k"] = created_epk

        agent_row = Row.from_idless_row_dict(self.db, row_dict=payload, table="agents")
        agent_id = int(agent_row["agent_id"])

        if human_sidecar:
            self._require_insertable_table("human_agents", for_method="agent(human_sidecar)")
            self._upsert_sidecar(
                table="human_agents",
                fk_column="human_agent_agent_id",
                fk_value=agent_id,
                payload=human_sidecar,
            )

        if org_sidecar:
            self._require_insertable_table("org_agents", for_method="agent(org_sidecar)")
            self._upsert_sidecar(
                table="org_agents",
                fk_column="org_agent_agent_id",
                fk_value=agent_id,
                payload=org_sidecar,
            )

        for language in linked_languages or []:
            language_row = self._language_to_row(language)
            self._safe_interlink(agent_row, language_row, priority=0, type="native")

        for note in linked_notes or []:
            note_row = self._note_to_row(note)
            self._safe_interlink(agent_row, note_row, priority=0)

        for synopsis in linked_synopses or []:
            synopsis_row = self._synopsis_to_row(synopsis)
            self._safe_interlink(agent_row, synopsis_row, priority=0)

        for image_row in linked_images or []:
            if not isinstance(image_row, Row):
                continue
            self._safe_interlink(agent_row, image_row, priority=0, type="agent_photo")

        return agent_row

    def creator(
        self,
        creator,
        creator_sort=None,
        creator_short_name=None,
        creator_last_name=None,
        creator_phash=None,
        creator_legal_name=None,
        creator_birth_date=None,
        creator_death_date=None,
        creator_type="authors",
        creator_seminal_work=None,
        creator_one_person=True,
        creator_wikipedia=None,
        creator_imdb=None,
        creator_link=None,
        creator_created_datestamp=None,
        creator_datestamp=None,
        creator_language=None,
        creator_bio=None,
        creator_image=None,
    ):
        """
        Translate legacy Creator fields into a person Agent and attachments.

        Requires insertable agents and human_agents before role validation. Missing
        identifier-table capability silently omits identifiers. All aggregate writes
        follow agent() without an encompassing transaction.

        Example:
            A multiword name is split heuristically into first, middle and last names.


        :param creator: Name converted to stripped text.
        :param creator_sort: Sort name; None derives it with author_to_author_sort.
        :param creator_short_name: Preferred name and optional alias.
        :param creator_last_name: Explicit family name; None uses the final split name part.
        :param creator_phash: Truthy phonetic value retained as an alias and optional identifier.
        :param creator_legal_name: Truthy legal name retained as an alias when different from canonical text.
        :param creator_birth_date: Birth date through active ISO-date coercion.
        :param creator_death_date: Death date through active ISO-date coercion.
        :param creator_type: Normalized role checked against CREATOR_TYPES, then retained in Agent notes.
        :param creator_seminal_work: Truthy seminal-work description retained in Agent notes.
        :param creator_one_person: Flag encoded as 0/1 in notes; Agent type remains person.
        :param creator_wikipedia: Optional wikipedia_url identifier.
        :param creator_imdb: Optional imdb_id identifier.
        :param creator_link: Optional url identifier.
        :param creator_created_datestamp: Truthy creation date preferred over creator_datestamp.
        :param creator_datestamp: Fallback timestamp when creation date is false-valued.
        :param creator_language: Optional Language Row/string/ID to link.
        :param creator_bio: Text stored in biography and also linked as a Note; a Row is only linked.
        :param creator_image: Concrete image Row to link; other values are ignored.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        self._require_insertable_table("agents", for_method="creator")
        self._require_insertable_table("human_agents", for_method="creator")

        creator_type = str(creator_type).lower().strip()
        if creator_type not in CREATOR_TYPES:
            err_str = "Unable to create_creator - creator type was not recognized."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("creator_type", creator_type),
                ("CREATOR_TYPES", CREATOR_TYPES),
            )
            raise InputIntegrityError(err_str)

        creator_name = str(creator).strip()
        name_parts = [part for part in creator_name.replace(",", " ").split(" ") if part]
        if creator_last_name is not None:
            family_name = creator_last_name
        else:
            family_name = name_parts[-1] if name_parts else None

        given_name = name_parts[0] if name_parts else None
        middle_name = " ".join(name_parts[1:-1]) if len(name_parts) > 2 else None
        if middle_name == "":
            middle_name = None

        aliases = []
        if creator_short_name:
            aliases.append(creator_short_name)
        if creator_legal_name and creator_legal_name != creator_name:
            aliases.append(creator_legal_name)
        if creator_phash:
            aliases.append("phash:{}".format(creator_phash))

        note_lines = [
            # Explicit role classification from metadata input.
            "creator_type={}".format(creator_type),
            # Preserve plurality signal (single human vs collective attribution).
            "creator_one_person={}".format(int(bool(creator_one_person))),
        ]
        if creator_seminal_work:
            note_lines.append("creator_seminal_work={}".format(creator_seminal_work))

        agent_row = self.agent(
            creator_name,
            agent_type="person",
            agent_sort_name=creator_sort if creator_sort is not None else author_to_author_sort(creator_name),
            agent_aliases=aliases or None,
            agent_note="\n".join(note_lines),
            agent_created_timestamp_ep_k=creator_created_datestamp or creator_datestamp,
            human_sidecar={
                # Person-name decomposition for sort/display/export layers.
                "human_agent_given_name": given_name,
                "human_agent_middle_name": middle_name,
                "human_agent_family_name": family_name,
                "human_agent_preferred_name": creator_short_name,
                # Lifespan values are normalized to YYYY-MM-DD.
                "human_agent_birth_date": self._coerce_iso_date(creator_birth_date),
                "human_agent_death_date": self._coerce_iso_date(creator_death_date),
                # Long-form descriptive text lives both here and as a linked note.
                "human_agent_biography": creator_bio if isinstance(creator_bio, string_types) else None,
            },
            linked_languages=[creator_language] if creator_language is not None else None,
            linked_notes=[creator_bio] if creator_bio is not None else None,
            linked_images=[creator_image] if creator_image is not None and isinstance(creator_image, Row) else None,
        )

        agent_id = int(agent_row["agent_id"])
        self._entity_identifier(agent_id=agent_id, scheme="wikipedia_url", value=creator_wikipedia, is_primary=1)
        self._entity_identifier(agent_id=agent_id, scheme="imdb_id", value=creator_imdb, is_primary=0)
        self._entity_identifier(agent_id=agent_id, scheme="url", value=creator_link, is_primary=0)
        if creator_phash:
            self._entity_identifier(agent_id=agent_id, scheme="creator_phash", value=creator_phash, is_primary=0)

        return agent_row

    def organisation(
        self,
        organisation,
        organisation_sort=None,
        organisation_aliases=None,
        organisation_note=None,
        organisation_legal_name=None,
        organisation_trading_name=None,
        organisation_registration_id=None,
        organisation_jurisdiction=None,
        organisation_founded_date=None,
        organisation_dissolved_date=None,
        organisation_website=None,
        organisation_contact_email=None,
        organisation_description=None,
        organisation_parent=None,
        organisation_relation_type="imprint_of",
        organisation_relation_note=None,
        organisation_language=None,
        organisation_synopsis=None,
    ) -> RowAPI:
        """
        Insert an organisation Agent with a sidecar and optional parent relation.

        Required Agent/sidecar tables are checked before insertion. Parent ID
        validation occurs after creation when the relation table is available.
        DatabaseIntegrityError while inserting the parent relation is suppressed;
        other failures propagate after earlier writes. No enclosing transaction
        is opened.

        Example:
            If org_agent_relations is unavailable, a supplied parent is silently ignored.


        :param organisation: Name converted to stripped text.
        :param organisation_sort: Sort name; None uses the stripped name.
        :param organisation_aliases: Aliases normalized by agent().
        :param organisation_note: Agent note text.
        :param organisation_legal_name: Legal name stored in the organisation sidecar.
        :param organisation_trading_name: Trading name stored in the sidecar.
        :param organisation_registration_id: Registration ID stored unchanged.
        :param organisation_jurisdiction: Jurisdiction stored unchanged.
        :param organisation_founded_date: Foundation date through active ISO-date coercion.
        :param organisation_dissolved_date: Dissolution date through active ISO-date coercion.
        :param organisation_website: Website stored in the sidecar and optionally as a primary url identifier.
        :param organisation_contact_email: Contact email stored unchanged.
        :param organisation_description: Description text stored unchanged.
        :param organisation_parent: Concrete parent Row or integer Agent ID; evaluated only if the relation table is insertable.
        :param organisation_relation_type: Relation type, default imprint_of, stored unchanged.
        :param organisation_relation_note: Relation note stored on the edge and appended to Agent notes.
        :param organisation_language: Optional Language Row/string/ID to link.
        :param organisation_synopsis: Optional Synopsis Row/string to link.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        self._require_insertable_table("agents", for_method="organisation")
        self._require_insertable_table("org_agents", for_method="organisation")

        organisation_name = str(organisation).strip()
        org_sidecar = {
            # Legal identity values for compliance/reporting use-cases.
            "org_agent_legal_name": organisation_legal_name,
            # Market-facing labels and registry metadata.
            "org_agent_trading_name": organisation_trading_name,
            "org_agent_registration_id": organisation_registration_id,
            "org_agent_jurisdiction": organisation_jurisdiction,
            # Lifespan of the organisation itself.
            "org_agent_founded_date": self._coerce_iso_date(organisation_founded_date),
            "org_agent_dissolved_date": self._coerce_iso_date(organisation_dissolved_date),
            # Contact/public profile information.
            "org_agent_website": organisation_website,
            "org_agent_contact_email": organisation_contact_email,
            "org_agent_description": organisation_description,
        }

        org_note = organisation_note
        if organisation_relation_note:
            relation_line = "organisation_relation_note={}".format(organisation_relation_note)
            if org_note:
                org_note = "{}\n{}".format(org_note, relation_line)
            else:
                org_note = relation_line

        agent_row = self.agent(
            organisation_name,
            agent_type="organisation",
            agent_sort_name=organisation_sort if organisation_sort is not None else organisation_name,
            agent_aliases=organisation_aliases,
            agent_note=org_note,
            org_sidecar=org_sidecar,
            linked_languages=[organisation_language] if organisation_language is not None else None,
            linked_synopses=[organisation_synopsis] if organisation_synopsis is not None else None,
        )

        if organisation_parent is not None and self._has_insertable_table("org_agent_relations"):
            parent_agent_id = None
            if isinstance(organisation_parent, Row):
                parent_agent_id = self._extract_agent_id(organisation_parent)
            elif isinstance(organisation_parent, int):
                parent_agent_id = int(organisation_parent)
            if parent_agent_id is None:
                raise InputIntegrityError("Could not extract parent agent id from organisation_parent.")

            relation_payload = {
                # Directed edge: child imprint/sub-org -> parent org.
                "org_agent_relation_child_agent_id": int(agent_row["agent_id"]),
                "org_agent_relation_parent_agent_id": int(parent_agent_id),
                # Relation semantics (imprint_of, owned_by, etc.).
                "org_agent_relation_type": organisation_relation_type,
                # Optional human-readable rationale/context.
                "org_agent_relation_note": organisation_relation_note,
            }
            try:
                Row.from_idless_row_dict(self.db, row_dict=relation_payload, table="org_agent_relations")
            except DatabaseIntegrityError:
                # Duplicate edge (same child/parent/type) or equivalent constraint - safe to ignore.
                pass

        agent_id = int(agent_row["agent_id"])
        self._entity_identifier(agent_id=agent_id, scheme="url", value=organisation_website, is_primary=1)

        return agent_row

    def organization(self, *args, **kwargs) -> RowAPI:
        """
        Forward the US-spelled alias with only two keyword translations.

        If both spellings of one translated key are supplied, the US key remains
        and may fail at the organisation call.

        Example:
            Use UK-spelled organisation_aliases for additional options; organization_aliases is not translated.


        :param args: Positional arguments forwarded to organisation.
        :param kwargs: Keywords forwarded after translating organization and organization_sort when their UK keys are absent.
        :return: Result of organisation; unsupported/conflicting keywords can raise TypeError.
        """
        if "organization" in kwargs and "organisation" not in kwargs:
            kwargs["organisation"] = kwargs.pop("organization")
        if "organization_sort" in kwargs and "organisation_sort" not in kwargs:
            kwargs["organisation_sort"] = kwargs.pop("organization_sort")
        return self.organisation(*args, **kwargs)

    def publisher(
        self,
        publisher,
        publisher_sort=None,
        publisher_phash=None,
        publisher_description=None,
        publisher_wikipedia=None,
        publisher_website=None,
        publisher_parent=None,
        publishr_position=None,
        publisher_full=None,
    ):
        """
        Translate legacy Publisher values into an organisation Agent.

        Unsupported non-text/non-Row descriptions are ignored. Aggregate and later
        identifier writes are sequential, with no encompassing transaction.

        Example:
            A description Row from synopses is linked as a Synopsis; other Row tables use the Note-link path.


        :param publisher: Publisher name forwarded to organisation.
        :param publisher_sort: Optional sort name.
        :param publisher_phash: Truthy hash encoded as an alias and optional identifier.
        :param publisher_description: Text for sidecar description, or Row linked as Synopsis/Note by its table.
        :param publisher_wikipedia: Optional primary wikipedia_url identifier.
        :param publisher_website: Website forwarded to organisation for sidecar and url identifier.
        :param publisher_parent: Parent organisation Row/ID forwarded with imprint_of relation type.
        :param publishr_position: Legacy misspelled position parameter; non-None is encoded as an alias.
        :param publisher_full: Truthy full name encoded as an alias.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        self._require_insertable_table("agents", for_method="publisher")
        self._require_insertable_table("org_agents", for_method="publisher")

        description_text = None
        linked_synopsis = None
        linked_note = None
        if isinstance(publisher_description, Row):
            if publisher_description.table == "synopses":
                linked_synopsis = publisher_description
            elif publisher_description.table == "notes":
                linked_note = publisher_description
            else:
                linked_note = publisher_description
        elif isinstance(publisher_description, string_types):
            description_text = publisher_description

        alias_values = []
        if publisher_phash:
            alias_values.append("publisher_phash:{}".format(publisher_phash))
        if publishr_position is not None:
            alias_values.append("publisher_position:{}".format(publishr_position))
        if publisher_full:
            alias_values.append("publisher_full:{}".format(publisher_full))

        publisher_row = self.organisation(
            organisation=publisher,
            organisation_sort=publisher_sort,
            organisation_aliases=alias_values or None,
            organisation_description=description_text,
            organisation_parent=publisher_parent,
            organisation_relation_type="imprint_of",
            organisation_website=publisher_website,
            organisation_synopsis=linked_synopsis,
        )

        if linked_note is not None:
            self._safe_interlink(publisher_row, linked_note, priority=0)

        agent_id = int(publisher_row["agent_id"])
        self._entity_identifier(agent_id=agent_id, scheme="wikipedia_url", value=publisher_wikipedia, is_primary=1)
        self._entity_identifier(agent_id=agent_id, scheme="publisher_phash", value=publisher_phash, is_primary=0)

        return publisher_row
