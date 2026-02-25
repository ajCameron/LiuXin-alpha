from __future__ import unicode_literals

import datetime

from typing import Any, Iterable, Optional, Sequence, Union

from six import string_types

from LiuXin_alpha.databases.api import RowAPI
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.metadata.constants import CREATOR_TYPES
from LiuXin_alpha.metadata.utils import author_to_author_sort
from LiuXin_alpha.utils.language_tools import best_effort_language_id
from LiuXin_alpha.utils.logging import default_log


class AgentCreatorOrgMixin:
    """
    Add methods for authoring entities.

    In FRBR-first schemas:
     - ``agents`` stores common identity fields
     - ``human_agents`` stores person-specific sidecar fields
     - ``org_agents`` stores organisation/group-specific sidecar fields
    """

    @staticmethod
    def _coerce_iso_date(value: Optional[Union[str, datetime.date, datetime.datetime]]) -> Optional[str]:
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
        try:
            if table_name not in set(self.db.get_tables()):
                return False
            return not self.db.driver_wrapper.is_view(table_name)
        except Exception:
            return False

    def _require_insertable_table(self, table_name: str, *, for_method: str) -> None:
        if not self._has_insertable_table(table_name):
            raise InputIntegrityError(
                "Cannot run `{}`: schema does not expose insertable `{}`.".format(for_method, table_name)
            )

    def _set_row_values(self, row: RowAPI, payload: dict[str, Any]) -> None:
        for col, value in payload.items():
            if col in row.allowed_columns:
                row[col] = value
        row.sync()

    def _first_row_for_value(self, table: str, column: str, value: Any) -> Optional[RowAPI]:
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
        current = self._first_row_for_value(table=table, column=fk_column, value=fk_value)
        if current is not None:
            self._set_row_values(current, payload)
            return current

        row_dict = {fk_column: fk_value}
        row_dict.update(payload)
        return Row.from_idless_row_dict(self.db, row_dict=row_dict, table=table)

    def _extract_agent_id(self, row: RowAPI) -> Optional[int]:
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
        Create a generic agent row plus optional subtype sidecars.
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
        Create a creator as an ``agents`` row with a ``human_agents`` sidecar.
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
        Create an organisation as an ``agents`` row with an ``org_agents`` sidecar.
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
        US spelling alias for :meth:`organisation`.
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
        Add a publisher as an ``organisation``-typed agent.
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
