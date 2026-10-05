"""
Configure vocabulary/value repositories and contextual text attachments.

Ten concrete repositories bind the reviewed identity specifications to base CRUD.
Most methods are inherited; Comment/Synopsis attachment workflows and Item-scoped
Annotation listing add their own validation and transaction boundaries. Languages
are immutable at this repository layer, and Comments/Annotations reject generic
match-or-create. Constants and alias mappings are shared with their specifications.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import ClassVar

from LiuXin_alpha.databases.macro_types import LinkValue

from ..api.common import EntityId, RowInput, RowMapping, WemiLevel
from ..matching.entity_specs import (
    ANNOTATION_SPEC,
    COMMENT_SPEC,
    GENRE_SPEC,
    LABEL_SPEC,
    LANGUAGE_SPEC,
    RATING_SPEC,
    SERIES_SPEC,
    SUBJECT_SPEC,
    SYNOPSIS_SPEC,
    TAG_SPEC,
)
from .base import WEMI_TABLES
from .exact import ExactEntityRepository


class TagRepository(ExactEntityRepository):
    """
    Store reusable Tags with exact text/hash identity and optional approximate text fallback.

    TAG_SPEC configures tags/tag_id, with name/text/value aliases for tag and phash for tag_phash.
    Candidate identity includes supplied tag and tag_phash; scalar exact searches only tag. Tag text
    uses NFKC/whitespace normalization and case-folding while preserving punctuation. CRUD remains
    separate from matching, and no hash is derived by this repository.

    Example:
        >>> repository = TagRepository(None)
        >>> repository.match_spec is TAG_SPEC
        True


    :ivar match_spec: Shared TAG_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = TAG_SPEC.table_name
    id_column = TAG_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = TAG_SPEC.input_aliases
    match_spec = TAG_SPEC


class LabelRepository(ExactEntityRepository):
    """
    Store reusable operational Labels with supplied or derived normalized text.

    LABEL_SPEC configures labels/label_id. name/text/value address label_text and
    normalized/normalised address label_text_norm. Create/update derive the norm from supplied text
    only when the norm key is absent, preserving explicit None or custom values. Candidate identity
    and scalar lookup can use text and norm; text comparison is case-folded and approximate fallback uses
    the label_text policy field only when explicitly enabled. This class does not enforce
    consistency between an explicit norm and its source text.

    Example:
        >>> repository = LabelRepository(None)
        >>> repository.match_spec is LABEL_SPEC
        True


    :ivar match_spec: Shared LABEL_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = LABEL_SPEC.table_name
    id_column = LABEL_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = LABEL_SPEC.input_aliases
    match_spec = LABEL_SPEC


class GenreRepository(ExactEntityRepository):
    """
    Store hierarchical Genres with exact identity and optional parent scoping.

    GENRE_SPEC configures genres/genre_id, name/value aliases for genre, and aliases for sort,
    phash, parent_id, position, tree_id, and full. Candidate identity can include genre, genre_sort,
    genre_phash, and genre_full; scalar exact searches the three textual fields. Text comparisons
    case-fold and preserve punctuation. An omitted parent_id leaves matching global; explicit None
    filters root rows. Storage of hierarchy fields does not add tree traversal or cycle validation
    here. Approximate genre-text fallback must be explicitly requested.

    Example:
        >>> repository = GenreRepository(None)
        >>> repository.match_spec is GENRE_SPEC
        True


    :ivar match_spec: Shared GENRE_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = GENRE_SPEC.table_name
    id_column = GENRE_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = GENRE_SPEC.input_aliases
    match_spec = GENRE_SPEC


class SubjectRepository(ExactEntityRepository):
    """
    Store hierarchical Subjects with exact identity and optional parent scoping.

    SUBJECT_SPEC configures subjects/subject_id, name/value aliases for subject, and aliases for
    sort, phash, parent_id, parent_position, tree_id, and full. Candidate identity includes the
    supplied subject/sort/hash/full fields; scalar lookup searches subject, subject_sort, and
    subject_full with case-folded, punctuation-preserving equality. Omitted parent scope searches
    globally while explicit None selects root scope. Hierarchy maintenance and cycle checks are not
    implemented here; approximate subject-text fallback is opt-in.

    Example:
        >>> repository = SubjectRepository(None)
        >>> repository.match_spec is SUBJECT_SPEC
        True


    :ivar match_spec: Shared SUBJECT_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = SUBJECT_SPEC.table_name
    id_column = SUBJECT_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = SUBJECT_SPEC.input_aliases
    match_spec = SUBJECT_SPEC


class SeriesRepository(ExactEntityRepository):
    """
    Store hierarchical Series and derive missing normalized names for persistence.

    SERIES_SPEC configures series/series_id. name/value address series, and normalized/normalised
    address series_name_norm; sort, phash, over_author, parent_id, parent_position, tree_id, and
    full are also aliases. Supplied names derive a norm only when its key is absent. Candidate
    identity covers name, normalized name, hash, and full name; series_sort is not an identity
    field. Scalar lookup searches the three name forms with case-folded exact comparison. Parent
    scope is optional, and approximate fallback uses series only when enabled.

    Example:
        >>> repository = SeriesRepository(None)
        >>> repository.match_spec is SERIES_SPEC
        True


    :ivar match_spec: Shared SERIES_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = SERIES_SPEC.table_name
    id_column = SERIES_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = SERIES_SPEC.input_aliases
    match_spec = SERIES_SPEC


class LanguageRepository(ExactEntityRepository):
    """
    Read seeded Languages by names and code variants while rejecting repository mutations.

    LANGUAGE_SPEC configures languages/language_id. Scalar lookup compares language, language_code,
    ISO 639-1, bibliographic/terminological ISO 639-2, and primary BCP 47 fields with case-folded
    exact comparison. The bcp47_variants alias is a known column but is not itself a scalar or
    identity field. Candidate fields use the same configured identities, and optional approximate
    fallback uses language.

    The mutable flag blocks create, update, delete, and match_or_create before database/input
    validation, including attempted reuse of an existing Language. Use exact or resolve to read
    constants; this restriction is a repository policy, not a guarantee that lower database layers
    cannot write the table.

    Example:
        >>> repository = LanguageRepository(None)
        >>> repository.match_spec is LANGUAGE_SPEC
        True
        >>> from LiuXin_alpha.catalog.api.common import MetadataCandidate
        >>> repository.match_or_create(MetadataCandidate({"code": "en"}))
        Traceback (most recent call last):
        ...
        LiuXin_alpha.catalog.api.common.CatalogMutationError: Language rows are read-only catalog constants


    :ivar match_spec: Shared LANGUAGE_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = LANGUAGE_SPEC.table_name
    id_column = LANGUAGE_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = LANGUAGE_SPEC.input_aliases
    match_spec = LANGUAGE_SPEC


class RatingRepository(ExactEntityRepository):
    """
    Store reusable Ratings using supplied value, scale, and source identity fields.

    RATING_SPEC configures ratings/rating_id. value addresses rating, out_of addresses
    rating_out_of, source addresses rating_source, and calibre_value addresses a display column
    outside identity. Candidate matching compares all supplied identity fields; source strings
    case-fold, while scalar exact considers only the rating value. The repository adds no numeric
    range or scale validation and has no approximate policy field, so use_policy cannot add fuzzy
    fallback.

    Example:
        >>> repository = RatingRepository(None)
        >>> repository.match_spec is RATING_SPEC
        True


    :ivar match_spec: Shared RATING_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = RATING_SPEC.table_name
    id_column = RATING_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = RATING_SPEC.input_aliases
    match_spec = RATING_SPEC


class CommentRepository(ExactEntityRepository):
    """
    Create contextual Comment attachments while keeping exact inspection available.

    COMMENT_SPEC configures comments/comment_id with text/value aliases for comment. Body comparison
    preserves case and punctuation after NFKC/whitespace normalization. The spec is mutable but
    non-reusable: inherited match_or_create always rejects before lookup, while direct create and
    exact/match remain available. No approximate policy field is configured. Attachment conveniences
    create fresh rows and use macro transactions for create/link or clear/replace; they do not
    delete detached historical Comment rows themselves.

    Example:
        >>> repository = CommentRepository(None)
        >>> repository.match_spec is COMMENT_SPEC
        True


    :ivar match_spec: Shared COMMENT_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = COMMENT_SPEC.table_name
    id_column = COMMENT_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = COMMENT_SPEC.input_aliases
    match_spec = COMMENT_SPEC

    def add_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> EntityId:
        """
        Create a fresh Comment and attach it to one existing WEMI entity.

        This operation calls create directly and does not search for or reuse equal content. It does
        not clear earlier attachments itself. Require the owner first, then enter one macro
        transaction around Comment insertion and the nested link helper. The link is requested with
        priority zero; cardinality and allowed-priority rules remain with the schema and macro
        backend. Failures inside that transaction are subject to its rollback semantics, including
        failure to resolve a link after insertion.

        Example:
            >>> entity_note_id = catalog.comments.add_for_wemi(level="work", entity_id=work_id, data={"text": "First published anonymously."})  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param data: Writable Comment aliases or storage columns; text/value map to its body column.
        :return: Newly created Comment ID after linking succeeds.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        with self._macros.transaction():
            comment_id = self.create(data)
            self._link(table, entity_id, self.table_name, comment_id, priority=0)
        return comment_id

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput | None,
    ) -> EntityId | None:
        """
        Clear a WEMI entity's Comment links and optionally attach one fresh Comment.

        Validate level and owner, then resolve the link spec before entering a macro transaction.
        First replace the owner's links with an empty set. A None payload returns None from inside
        that transaction; any other payload, including an empty mapping, goes through create and may
        fail validation. For valid data, create and link a new Comment at priority zero before
        returning its ID.

        The transaction covers clearing, creation, and linking, so macro rollback can restore old
        links after a later failure. This method does not delete the old Comment rows or perform
        global matching/reuse; constraints and triggers remain backend responsibilities. Only the
        selected owner's relationship set is targeted.

        Example:
            >>> comment_id = catalog.comments.replace_for_wemi(level="work", entity_id=work_id, data={"text": "Revised comment"})  # doctest: +SKIP
            >>> catalog.comments.replace_for_wemi(level="work", entity_id=work_id, data=None)  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param data: Fresh Comment payload; exactly None clears without inserting a replacement.
        :return: New Comment ID after replacement, or None after clearing.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        spec = self._link_spec(table, self.table_name)
        with self._macros.transaction():
            self._macros.replace_links(spec, entity_id, ())
            if data is None:
                return None
            comment_id = self.create(data)
            self._link(table, entity_id, self.table_name, comment_id, priority=0)
        return comment_id

    def list_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """
        Read Comment attachments and include their traversed relationship metadata.

        Validate the WEMI level and require the owner. A missing relationship specification is an
        error, even when the owner has no attachments; this method does not provide the Note
        repository's absent-schema fallback.

        Return shallow row dictionaries with _catalog_link endpoint/type/priority/extra metadata.
        Preserve macro link order and duplicate destinations from distinct links. Portable SQL
        macros use descending priority where present, then destination ID. A dangling destination
        raises instead of being silently omitted. There is no page limit or enclosing snapshot
        transaction.

        Example:
            >>> rows = catalog.comments.list_for_wemi(level="work", entity_id=work_id)  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :return: Tuple of linked Comment row mappings; empty for no links.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._linked_rows(WEMI_TABLES[level], entity_id, self.table_name)


class SynopsisRepository(ExactEntityRepository):
    """
    Store reusable Synopses and create fresh WEMI attachments when requested.

    SYNOPSIS_SPEC configures synopses/synopsis_id and text/value aliases for synopsis. Body identity
    preserves case and punctuation after NFKC/whitespace normalization. Explicit exact
    match_or_create can reuse content; add_for_wemi and replace_for_wemi always create fresh rows.
    No approximate policy field is configured. Attachment creation/replacement uses macro
    transactions, and replacing links does not explicitly delete detached Synopsis rows or alter
    unrelated owners' links.

    Example:
        >>> repository = SynopsisRepository(None)
        >>> repository.match_spec is SYNOPSIS_SPEC
        True


    :ivar match_spec: Shared SYNOPSIS_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = SYNOPSIS_SPEC.table_name
    id_column = SYNOPSIS_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = SYNOPSIS_SPEC.input_aliases
    match_spec = SYNOPSIS_SPEC

    def add_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> EntityId:
        """
        Create a fresh Synopsis and attach it to one existing WEMI entity.

        This operation calls create directly and does not search for or reuse equal content. It does
        not clear earlier attachments itself. Require the owner first, then enter one macro
        transaction around Synopsis insertion and the nested link helper. The link is requested with
        priority zero; cardinality and allowed-priority rules remain with the schema and macro
        backend. Failures inside that transaction are subject to its rollback semantics, including
        failure to resolve a link after insertion.

        Example:
            >>> entity_note_id = catalog.synopses.add_for_wemi(level="work", entity_id=work_id, data={"text": "First published anonymously."})  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param data: Writable Synopsis aliases or storage columns; text/value map to its body column.
        :return: Newly created Synopsis ID after linking succeeds.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        with self._macros.transaction():
            synopsis_id = self.create(data)
            self._link(table, entity_id, self.table_name, synopsis_id, priority=0)
        return synopsis_id

    def list_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """
        Read Synopsis attachments and include their traversed relationship metadata.

        Validate the WEMI level and require the owner. A missing relationship specification is an
        error, even when the owner has no attachments; this method does not provide the Note
        repository's absent-schema fallback.

        Return shallow row dictionaries with _catalog_link endpoint/type/priority/extra metadata.
        Preserve macro link order and duplicate destinations from distinct links. Portable SQL
        macros use descending priority where present, then destination ID. A dangling destination
        raises instead of being silently omitted. There is no page limit or enclosing snapshot
        transaction.

        Example:
            >>> rows = catalog.synopses.list_for_wemi(level="work", entity_id=work_id)  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :return: Tuple of linked Synopsis row mappings; empty for no links.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._linked_rows(WEMI_TABLES[level], entity_id, self.table_name)

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        synopses: Sequence[str | RowInput],
    ) -> tuple[EntityId, ...]:
        """
        Create fresh synopses and replace one WEMI entity's attachment set transactionally.

        Validate level, require a non-string Sequence, and convert each string to a synopsis-column
        payload or each Mapping to a shallow dictionary. Reject other members before owner/schema
        access; individual payload keys and values are normalized later during creation. Then
        require the owner and resolve its link spec before opening the macro transaction.

        Inside the transaction, create every row in input order and replace links with LinkValue
        entries referring to those new IDs. No exact reuse or duplicate-content elimination occurs.
        Empty input still validates owner/schema and clears the selected relationship set. Old
        destination rows are not explicitly deleted, and unrelated owners' links are not targeted.
        Backend triggers/constraints remain applicable. Macro replacement assigns default priorities
        where the link spec is ordered; the returned ID tuple preserves caller order.

        The transaction joins all creations and link replacement; owner/spec preflight reads occur
        before it. There is no independent global identity/uniqueness check.

        Example:
            >>> ids = catalog.synopses.replace_for_wemi(level="work", entity_id=work_id, synopses=["First", {"text": "Second"}])  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param synopses: Sequence of Synopsis body strings or row mappings; an empty sequence clears links.
        :return: Tuple of new Synopsis IDs in input order, empty after clearing.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        :raises TypeError: If the outer value is not a non-string Sequence or a member is neither a string nor a Mapping.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(synopses, Sequence) or isinstance(
            synopses,
            (str, bytes),
        ):
            raise TypeError("synopses must be a sequence of strings or mappings")
        payloads: list[RowInput] = []
        for synopsis in synopses:
            if isinstance(synopsis, str):
                payloads.append({"synopsis": synopsis})
            elif isinstance(synopsis, Mapping):
                payloads.append(dict(synopsis))
            else:
                raise TypeError(
                    "synopses must contain only strings or mappings"
                )
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        spec = self._link_spec(table, self.table_name)
        with self._macros.transaction():
            synopsis_ids = tuple(self.create(payload) for payload in payloads)
            self._macros.replace_links(
                spec,
                entity_id,
                (LinkValue(synopsis_id) for synopsis_id in synopsis_ids),
            )
        return synopsis_ids


class AnnotationRepository(ExactEntityRepository):
    """
    Store and inspect Annotations with required Item scope for identity matching.

    ANNOTATION_SPEC configures annotations/annotation_id and public aliases for Item, user, kind,
    anchor type/start/end, source, selected/note text, device, and extra JSON. Candidate matching
    requires non-None item_id, kind, anchor_type, and anchor_start; supplied user/end/source values
    also constrain identity. Kind, anchor type, and source comparisons case-fold. Scalar exact uses
    only anchor start plus required Item scope, without the full candidate-identity check.

    Presence checks in matching do not validate Item existence; list_for_item does. Mutable direct
    CRUD is available, but inherited match_or_create rejects this non-reusable spec before lookup.
    No approximate policy field is configured.

    Example:
        >>> repository = AnnotationRepository(None)
        >>> repository.match_spec is ANNOTATION_SPEC
        True


    :ivar match_spec: Shared ANNOTATION_SPEC configuration; table, ID, and aliases are initialized from it.
    """

    table_name = ANNOTATION_SPEC.table_name
    id_column = ANNOTATION_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = ANNOTATION_SPEC.input_aliases
    match_spec = ANNOTATION_SPEC

    def list_for_item(
        self,
        item_id: EntityId,
        *,
        user_id: EntityId | None = None,
        kind: str | None = None,
    ) -> Sequence[RowMapping]:
        """
        Read one Item's Annotations with optional direct user and kind filters.

        Require the Item before validating optional filters. A supplied user_id must pass the strict
        nonnegative/non-boolean integer check, but no user row is fetched. A supplied kind must be a
        string with nonblank stripped content; strip its outer whitespace and pass it to the
        database without case-folding or matching-policy normalization. None means no constraint for
        either filter, not a request to find null-valued columns.

        Ask macros for ascending annotation_id order and shallow-copy all returned rows. No
        _catalog_link metadata, pagination, global reuse, or persistence is added. The owner read
        and annotation query are not enclosed in one snapshot.

        Example:
            >>> rows = catalog.annotations.list_for_item(item_id, user_id=11, kind="highlight")  # doctest: +SKIP


        :param item_id: Nonnegative non-boolean integer ID of the Item required first.
        :param user_id: Optional validated integer user ID used as a direct column filter, without existence checking.
        :param kind: Optional nonblank string stripped before applying a direct annotation_kind filter.
        :return: Tuple of Annotation row dictionaries in macro-provided ID order.
        :raises CatalogNotFoundError: If the Item is missing.
        :raises ValueError: If kind is supplied but is not a nonblank string, or a validated ID is negative.
        :raises TypeError: If a supplied ID is not an integer or is a boolean.
        :raises Exception: Database/schema access and row-conversion failures propagate.
        """

        self._require_table_row("items", item_id)
        where: dict[str, object] = {"annotation_item_id": item_id}
        if user_id is not None:
            self._validate_entity_id(user_id)
            where["annotation_user_id"] = user_id
        if kind is not None:
            if not isinstance(kind, str) or not kind.strip():
                raise ValueError("kind must be a non-empty string or None")
            where["annotation_kind"] = kind.strip()
        rows = self._macros.get_rows(
            self.table_name,
            where=where,
            order_by=(self.id_column,),
        )
        return tuple(self._as_mapping(row) for row in rows)


__all__ = [
    "AnnotationRepository",
    "CommentRepository",
    "GenreRepository",
    "LabelRepository",
    "LanguageRepository",
    "RatingRepository",
    "SeriesRepository",
    "SubjectRepository",
    "SynopsisRepository",
    "TagRepository",
]
