"""
Translate legacy title/book creation into writable legacy or WEMI storage.
"""

from __future__ import unicode_literals

import datetime
import os

from LiuXin_alpha.databases.hashes import generate_title_fingerprint
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.metadata.standardization import make_title_search_term
from LiuXin_alpha.metadata.utils import title_sort as generate_title_sort
from LiuXin_alpha.utils.date import isoformat_timestamp, utcnow
from LiuXin_alpha.utils.identifiers import get_unique_group_id
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log


class BookAndTitleAdderMixin:
    """
    Provide compatibility creation over schema-dependent title/book representations.

    The host supplies db plus WEMI creation/coercion helpers. WEMI title calls
    may create a Work/Expression/Manifestation path and source Items; projected
    book resolution may trigger those writes. Neither path is an atomic aggregate.

    Example:
        A title source ending in .epub can supply EPUB/ebook hints in its Manifestation.
    """

    @staticmethod
    def _split_break_joined(value):
        """
        Flatten nested list/tuple values and split the legacy separator.

        Example:
            >>> BookAndTitleAdderMixin._split_break_joined(["a(#BREAK#)b", None, " "])
            ['a', 'b', ' ']


        :param value: None, nested lists/tuples, or stringifiable scalar.
        :return: List of nonempty strings; whitespace-only values remain.
        """
        if value is None:
            return []

        if isinstance(value, (list, tuple)):
            out_vals = []
            for row_val in value:
                out_vals.extend(BookAndTitleAdderMixin._split_break_joined(row_val))
            return out_vals

        text = six_unicode(value)
        if "(#BREAK#)" in text:
            vals = text.split("(#BREAK#)")
        else:
            vals = [text]
        return [v for v in vals if v]

    @staticmethod
    def _extract_year(value):
        """
        Take a date's year or the first four leading digits of stripped text.

        Example:
            An epoch integer is interpreted from its first four digits here, not converted to a calendar date.


        :param value: Date/datetime or stringifiable value; None allowed.
        :return: Integer year-like value, or None; no calendar-range validation.
        """

        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value.year
        if isinstance(value, datetime.date):
            return value.year

        text = six_unicode(value).strip()
        if len(text) >= 4 and text[:4].isdigit():
            return int(text[:4])
        return None

    @staticmethod
    def _extract_work_id(row):
        """
        Try Work and compatibility projection ID columns in order.

        Example:
            A failed column access is ignored before trying the next ID spelling.


        :param row: Optional row-like object.
        :return: First int-convertible work_id, title_id or book_work_id; otherwise None.
        """
        if row is None:
            return None

        for key in ("work_id", "title_id", "book_work_id"):
            try:
                val = row[key]
            except Exception:
                continue

            if val is None:
                continue
            try:
                return int(val)
            except (TypeError, ValueError):
                continue

        return None

    @staticmethod
    def _guess_format_detail(title_source_name=None, title_source_path=None):
        """
        Infer a format only when all detected extensions agree.

        Example:
            A mixture of .epub and .EPUB yields EPUB; adding .pdf makes the result None.


        :param title_source_name: Source name(s), split/flattened with the legacy separator.
        :param title_source_path: Source path(s), split/flattened with the legacy separator.
        :return: Uppercase single extension, or None for none/multiple extensions.
        """

        extensions = set()
        for val in BookAndTitleAdderMixin._split_break_joined(title_source_name) + BookAndTitleAdderMixin._split_break_joined(
            title_source_path
        ):
            _, ext = os.path.splitext(six_unicode(val).strip())
            if ext:
                extensions.add(ext.lstrip(".").lower())

        if len(extensions) == 1:
            return next(iter(extensions)).upper()
        return None

    @staticmethod
    def _guess_carrier_type(format_detail):
        """
        Classify recognized ebook, audio and video extension labels.

        Example:
            >>> BookAndTitleAdderMixin._guess_carrier_type("EPUB")
            'ebook'


        :param format_detail: Format label or None; strings are lowercased but not stripped.
        :return: ebook, audiobook, video, or None when unrecognized.
        """

        if format_detail is None:
            return None
        fmt = format_detail.lower()
        if fmt in {"epub", "pdf", "mobi", "azw3", "cbz", "cbr", "djvu", "fb2", "txt", "rtf", "docx"}:
            return "ebook"
        if fmt in {"mp3", "m4b", "flac", "ogg", "aac", "wav"}:
            return "audiobook"
        if fmt in {"mp4", "mkv", "avi"}:
            return "video"
        return None

    @staticmethod
    def _best_effort_title_sort(title, explicit_sort=None):
        """
        Use an explicit sort value or generate one with a text fallback.

        Example:
            An explicit empty string is retained rather than regenerated.


        :param title: Title passed to the sort generator.
        :param explicit_sort: Any non-None override, returned unchanged.
        :return: Explicit/generated sort text, or six_unicode(title) after a generation exception.
        """

        if explicit_sort is not None:
            return explicit_sort
        try:
            return generate_title_sort(title)
        except Exception:
            return six_unicode(title)

    @staticmethod
    def _update_row(row, payload):
        """
        Apply allowed columns from a payload and sync once.

        Example:
            Accepted None values clear columns instead of being omitted.


        :param row: Mutable Row with allowed_columns.
        :param payload: Column/value mapping; unknown columns are skipped.
        :return: None; updates/synchronizes the supplied Row.
        """

        for col, val in payload.items():
            if col in row.allowed_columns:
                row[col] = val
        row.sync()

    def _legacy_title(
        self,
        title,
        title_sort=None,
        title_phash=None,
        title_creator_sort=None,
        title_pub_date=None,
        title_copyright_date=None,
        title_wikipedia=None,
        title_fiction_length_category=None,
        title_type=None,
        title_wordcount=None,
        title_source=None,
        title_source_path=None,
        title_source_name=None,
        title_created_datestamp=None,
        title_datestamp=None,
        override_title_row=None,
    ):
        """
        Populate and sync a writable legacy title Row.

        Reuse override_title_row or create an empty Row. Generate absent sort and
        phonetic values, default copyright to publication date and creation time to
        utcnow(). Assign all legacy columns and sync once. This helper itself does
        not reject a None title before assignment.

        Example:
            An override Row receives all supplied values, including None, before sync.


        :param title: Title value; None is rejected by title(), but blank text is not checked.
        :param title_sort: Explicit sort text; None uses best-effort generation.
        :param title_phash: Legacy hash; None generates it on the legacy path, ignored on WEMI.
        :param title_creator_sort: Creator-sort text mapped to Work or legacy title storage.
        :param title_pub_date: Publication date mapped to Work/Expression epoch values and publication hints.
        :param title_copyright_date: Copyright value; None falls back to publication date.
        :param title_wikipedia: Wikipedia link mapped to Work or legacy title storage.
        :param title_fiction_length_category: Fiction-length category mapped to Expression or legacy title storage.
        :param title_type: Type mapped to Work or legacy title storage.
        :param title_wordcount: Word count mapped to Expression or legacy title storage.
        :param title_source: Discovery/source value; can trigger creation of one source-only Item.
        :param title_source_path: Path value(s), recursively split on (#BREAK#) on WEMI.
        :param title_source_name: Name value(s), recursively split on (#BREAK#) on WEMI.
        :param title_created_datestamp: Creation time; legacy None uses utcnow(), WEMI uses active epoch conversion.
        :param title_datestamp: Legacy datestamp only; ignored on WEMI.
        :param override_title_row: Existing title/work-like Row to update; work_id/title_id/book_work_id are tried.
        :return: Populated legacy title Row.
        """
        if override_title_row is None:
            title_row = Row(database=self.db)
        else:
            title_row = override_title_row

        title_row["title"] = title
        title_row["title_sort"] = self._best_effort_title_sort(title, title_sort)
        title_row["title_phash"] = title_phash if title_phash is not None else make_title_search_term(title)
        title_row["title_creator_sort"] = title_creator_sort

        title_row["title_pub_date"] = title_pub_date
        if title_copyright_date is not None:
            title_row["title_copyright_date"] = title_copyright_date
        else:
            title_row["title_copyright_date"] = title_pub_date

        title_row["title_wikipedia"] = title_wikipedia
        title_row["title_fiction_length_category"] = title_fiction_length_category
        title_row["title_type"] = title_type
        title_row["title_wordcount"] = title_wordcount

        title_row["title_source"] = title_source
        title_row["title_source_path"] = title_source_path
        title_row["title_source_name"] = title_source_name
        title_row["title_created_datestamp"] = title_created_datestamp if title_created_datestamp is not None else utcnow()
        title_row["title_datestamp"] = title_datestamp

        title_row.sync()
        return title_row

    def _legacy_book(
        self,
        title_row,
        book_sort=None,
        book_flags=None,
        book_pubdate=None,
        book_copyright_date=None,
        book_uuid=None,
        book_has_cover=False,
        book_has_local_cover=None,
        book_last_modified=None,
        book_fingerprint=None,
        book_paths=None,
        book_size=None,
        book_rating=None,
        book_created_datestamp=None,
        book_datestamp=None,
    ):
        """
        Insert a writable legacy book using the title ID.

        Reject an existing book ID. Insert the ID, then sync a generated creation
        timestamp before assigning remaining fields. Final assignment overwrites
        book_created_datestamp with the caller value, including None. Derive absent
        UUID/fingerprint and date fallbacks; failures may leave an inserted or
        partially populated Row.

        Example:
            Omitting book_created_datestamp does not preserve the intermediate generated timestamp.


        :param title_row: Title Row used to resolve legacy title_id or a WEMI Work ID.
        :param book_sort: Legacy book sort; ignored when books is a view.
        :param book_flags: Legacy flags; ignored when books is a view.
        :param book_pubdate: Legacy publication date; None falls back to the title date.
        :param book_copyright_date: Legacy copyright date; falls back to supplied publication date, then title date.
        :param book_uuid: Legacy UUID; None generates a unique group ID.
        :param book_has_cover: Legacy cover flag; no asset validation.
        :param book_has_local_cover: Legacy local-cover flag.
        :param book_last_modified: Legacy modification time; None uses the creation timestamp.
        :param book_fingerprint: Legacy fingerprint; None derives relationship tokens from the title.
        :param book_paths: Legacy paths value stored unchanged.
        :param book_size: Legacy size value stored unchanged.
        :param book_rating: Legacy rating value stored unchanged.
        :param book_created_datestamp: Legacy creation value assigned at final sync, even when None.
        :param book_datestamp: Legacy datestamp stored unchanged.
        :return: Newly populated legacy book Row.
        :raises DatabaseIntegrityError: The title ID already has a book.
        """
        new_book_id = title_row["title_id"]
        clash_book_rows = self.db.driver_wrapper.search("books", "book_id", new_book_id)
        if clash_book_rows:
            err_str = (
                "Title already has a book - you cannot generate another - if you want to recreate the book "
                "first delete it. Then re-add it."
            )
            default_log.error(err_str)
            raise DatabaseIntegrityError(err_str)

        book_row_dict = {"book_id": new_book_id}
        self.db.driver_wrapper.add_row(book_row_dict)
        book_row = Row(database=self.db, row_dict=book_row_dict)

        book_creation_time = isoformat_timestamp()
        book_row["book_created_datestamp"] = book_creation_time
        book_row.sync()

        book_row["book_sort"] = book_sort
        book_row["book_flags"] = book_flags
        book_row["book_pubdate"] = book_pubdate if book_pubdate is not None else title_row["title_pub_date"]

        if book_copyright_date is not None:
            book_row["book_copyright_date"] = book_copyright_date
        elif book_pubdate is not None:
            book_row["book_copyright_date"] = book_pubdate
        else:
            book_row["book_copyright_date"] = title_row["title_pub_date"]

        book_row["book_uuid"] = book_uuid if book_uuid is not None else get_unique_group_id()
        book_row["book_has_cover"] = book_has_cover
        book_row["book_has_local_cover"] = book_has_local_cover
        book_row["book_last_modified"] = book_last_modified if book_last_modified is not None else book_creation_time
        book_row["book_fingerprint"] = (
            book_fingerprint if book_fingerprint is not None else generate_title_fingerprint(self.db, title_row)
        )
        book_row["book_paths"] = book_paths
        book_row["book_size"] = book_size
        book_row["book_rating"] = book_rating
        book_row["book_created_datestamp"] = book_created_datestamp
        book_row["book_datestamp"] = book_datestamp
        book_row.sync()

        return book_row

    def title(
        self,
        title,
        title_sort=None,
        title_phash=None,
        title_creator_sort=None,
        title_pub_date=None,
        title_copyright_date=None,
        title_wikipedia=None,
        title_fiction_length_category=None,
        title_type=None,
        title_wordcount=None,
        title_source=None,
        title_source_path=None,
        title_source_name=None,
        title_created_datestamp=None,
        title_datestamp=None,
        override_title_row=None,
    ):
        """
        Create or refresh a legacy title or its WEMI compatibility graph.

        Presence of works selects WEMI behavior. A resolved override ID can create a
        missing Work with that ID. Populate Work, Expression and Manifestation, then
        create/update source Items; old surplus Items remain. A single recognized
        extension determines format/carrier hints. Link-lookup failures are treated
        as absent links; projection lookup failures fall back to the Work. Store the
        latest WEMI rows in _last_title_wemi_bundle. No enclosing transaction prevents
        partial updates or creation on later failures.

        Example:
            An override follows the first existing Expression and Manifestation, then updates Items by position.


        :param title: Title value; None is rejected by title(), but blank text is not checked.
        :param title_sort: Explicit sort text; None uses best-effort generation.
        :param title_phash: Legacy hash; None generates it on the legacy path, ignored on WEMI.
        :param title_creator_sort: Creator-sort text mapped to Work or legacy title storage.
        :param title_pub_date: Publication date mapped to Work/Expression epoch values and publication hints.
        :param title_copyright_date: Copyright value; None falls back to publication date.
        :param title_wikipedia: Wikipedia link mapped to Work or legacy title storage.
        :param title_fiction_length_category: Fiction-length category mapped to Expression or legacy title storage.
        :param title_type: Type mapped to Work or legacy title storage.
        :param title_wordcount: Word count mapped to Expression or legacy title storage.
        :param title_source: Discovery/source value; can trigger creation of one source-only Item.
        :param title_source_path: Path value(s), recursively split on (#BREAK#) on WEMI.
        :param title_source_name: Name value(s), recursively split on (#BREAK#) on WEMI.
        :param title_created_datestamp: Creation time; legacy None uses utcnow(), WEMI uses active epoch conversion.
        :param title_datestamp: Legacy datestamp only; ignored on WEMI.
        :param override_title_row: Existing title/work-like Row to update; work_id/title_id/book_work_id are tried.
        :return: Title projection when available, otherwise the Work Row; legacy path returns its title Row.
        :raises InputIntegrityError: Title is None.
        """
        if title is None:
            err_str = "Cannot add title - title was None"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

        tables = set(self.db.get_tables())
        if "works" not in tables:
            return self._legacy_title(
                title=title,
                title_sort=title_sort,
                title_phash=title_phash,
                title_creator_sort=title_creator_sort,
                title_pub_date=title_pub_date,
                title_copyright_date=title_copyright_date,
                title_wikipedia=title_wikipedia,
                title_fiction_length_category=title_fiction_length_category,
                title_type=title_type,
                title_wordcount=title_wordcount,
                title_source=title_source,
                title_source_path=title_source_path,
                title_source_name=title_source_name,
                title_created_datestamp=title_created_datestamp,
                title_datestamp=title_datestamp,
                override_title_row=override_title_row,
            )

        work_id = self._extract_work_id(override_title_row)
        work_payload = {
            "work_title": title,
            "work_canonical_title": title,
            "work_sort_title": self._best_effort_title_sort(title, title_sort),
            "work_creator_sort": title_creator_sort,
            "work_type": title_type,
            "work_original_date": self._coerce_epoch_ms(title_pub_date),
            "work_original_year": self._extract_year(title_pub_date) or self._extract_year(title_copyright_date),
            "work_original_copyright_date": self._coerce_iso_date(
                title_copyright_date if title_copyright_date is not None else title_pub_date
            ),
            "work_wikipedia_link": title_wikipedia,
            "work_discovery_note": title_source,
        }
        created_epk = self._coerce_epoch_ms(title_created_datestamp)
        if created_epk is not None:
            work_payload["work_created_timestamp_ep_k"] = created_epk
            work_payload["work_modified_timestamp_ep_k"] = created_epk

        work_row = self.db.get_row_from_id("works", work_id) if work_id is not None else None
        if work_row is None:
            insert_payload = dict(work_payload)
            if work_id is not None:
                insert_payload["work_id"] = work_id
            work_row = Row.from_idless_row_dict(self.db, insert_payload, table="works")
        else:
            self._update_row(work_row, work_payload)

        expression_payload = {
            "expression_subtitle": None,
            "expression_title_override": None,
            "expression_type": None,
            "expression_label": None,
            "expression_year": self._extract_year(title_pub_date),
            "expression_is_preferred": 1,
            "expression_original_date": self._coerce_epoch_ms(title_pub_date),
            "expression_original_copyright_date": self._coerce_iso_date(
                title_copyright_date if title_copyright_date is not None else title_pub_date
            ),
            "expression_wordcount": title_wordcount,
            "expression_fiction_length_category": title_fiction_length_category,
        }

        expression_row = None
        if work_id is not None:
            linked_expressions = self.db.get_interlinked_rows(primary_row=work_row, secondary_table="expressions")
            if linked_expressions:
                expression_row = linked_expressions[0]

        if expression_row is None:
            expression_row = self.expression(**expression_payload)
        else:
            self._update_row(expression_row, expression_payload)

        try:
            cand_link_row = self.db.get_interlink_row(primary_row=work_row, secondary_row=expression_row)
        except Exception:
            cand_link_row = None
        if cand_link_row is None:
            self.db.interlink_rows(
                primary_row=work_row,
                secondary_row=expression_row,
                priority=0,
                primary=1,
                origin=title_source,
            )

        format_detail = self._guess_format_detail(title_source_name=title_source_name, title_source_path=title_source_path)
        manifestation_payload = {
            "manifestation_subtitle": None,
            "manifestation_carrier_type": self._guess_carrier_type(format_detail),
            "manifestation_format_detail": format_detail,
            "manifestation_pub_year": self._extract_year(title_pub_date),
            "manifestation_pub_date": self._coerce_iso_date(title_pub_date),
            "manifestation_status": None,
            "manifestation_note": None,
        }

        manifestation_row = None
        if work_id is not None:
            linked_manifestations = self.db.get_interlinked_rows(primary_row=expression_row, secondary_table="manifestations")
            if linked_manifestations:
                manifestation_row = linked_manifestations[0]

        if manifestation_row is None:
            manifestation_row = self.manifestation(**manifestation_payload)
        else:
            self._update_row(manifestation_row, manifestation_payload)

        try:
            cand_link_row = self.db.get_interlink_row(primary_row=expression_row, secondary_row=manifestation_row)
        except Exception:
            cand_link_row = None
        if cand_link_row is None:
            self.db.interlink_rows(
                primary_row=expression_row,
                secondary_row=manifestation_row,
                priority=0,
                primary=1,
                origin=title_source,
            )

        source_paths = self._split_break_joined(title_source_path)
        source_names = self._split_break_joined(title_source_name)
        item_rows = []

        existing_item_rows = self.db.search(
            table="items",
            column="item_manifestation_id",
            search_term=manifestation_row["manifestation_id"],
        )
        item_count = max(len(source_paths), len(source_names))
        if item_count == 0 and title_source is not None:
            item_count = 1

        for idx in range(item_count):
            item_source_path = source_paths[idx] if idx < len(source_paths) else None
            item_source_name = source_names[idx] if idx < len(source_names) else None
            if item_source_name is None and item_source_path:
                item_source_name = os.path.basename(item_source_path)

            item_payload = {
                "item_manifestation_id": manifestation_row["manifestation_id"],
                "item_type": "digital" if item_source_name or item_source_path else None,
                "item_source": title_source,
                "item_source_path": item_source_path,
                "item_source_name": item_source_name,
            }

            if idx < len(existing_item_rows):
                item_row = existing_item_rows[idx]
                self._update_row(item_row, item_payload)
            else:
                item_row = self.item(**item_payload)
            item_rows.append(item_row)

        self._last_title_wemi_bundle = {
            "work": work_row,
            "expression": expression_row,
            "manifestation": manifestation_row,
            "items": item_rows,
        }

        try:
            title_row = self.db.get_row_from_id("titles", work_row["work_id"])
            if title_row is not None:
                return title_row
        except Exception:
            pass
        return work_row

    def book(
        self,
        title_row,
        book_sort=None,
        book_flags=None,
        book_pubdate=None,
        book_copyright_date=None,
        book_uuid=None,
        book_has_cover=False,
        book_has_local_cover=None,
        book_last_modified=None,
        book_fingerprint=None,
        book_paths=None,
        book_size=None,
        book_rating=None,
        book_created_datestamp=None,
        book_datestamp=None,
    ):
        """
        Resolve a projected book or insert a legacy book row for a title.

        A writable/missing books entry chooses the legacy path, which rejects an
        existing title ID and writes in stages. View mode searches book_work_id;
        on a miss, a truthy title column may trigger title() graph creation, then
        a retry and an ID lookup. Resolving a projection can therefore mutate the
        WEMI graph. No enclosing transaction is opened.

        Example:
            When books is a view, optional book metadata is ignored and the existing Work projection is returned.


        :param title_row: Title Row used to resolve legacy title_id or a WEMI Work ID.
        :param book_sort: Legacy book sort; ignored when books is a view.
        :param book_flags: Legacy flags; ignored when books is a view.
        :param book_pubdate: Legacy publication date; None falls back to the title date.
        :param book_copyright_date: Legacy copyright date; falls back to supplied publication date, then title date.
        :param book_uuid: Legacy UUID; None generates a unique group ID.
        :param book_has_cover: Legacy cover flag; no asset validation.
        :param book_has_local_cover: Legacy local-cover flag.
        :param book_last_modified: Legacy modification time; None uses the creation timestamp.
        :param book_fingerprint: Legacy fingerprint; None derives relationship tokens from the title.
        :param book_paths: Legacy paths value stored unchanged.
        :param book_size: Legacy size value stored unchanged.
        :param book_rating: Legacy rating value stored unchanged.
        :param book_created_datestamp: Legacy creation value assigned at final sync, even when None.
        :param book_datestamp: Legacy datestamp stored unchanged.
        :return: First matching compatibility book Row, or newly populated legacy book Row.
        :raises InputIntegrityError: View mode cannot extract a Work ID.
        :raises DatabaseIntegrityError: Legacy book already exists or no projected book can be found.
        """
        tables = set(self.db.get_tables())
        books_is_view = "books" in tables and self.db.driver_wrapper.is_view("books")

        if not books_is_view:
            return self._legacy_book(
                title_row=title_row,
                book_sort=book_sort,
                book_flags=book_flags,
                book_pubdate=book_pubdate,
                book_copyright_date=book_copyright_date,
                book_uuid=book_uuid,
                book_has_cover=book_has_cover,
                book_has_local_cover=book_has_local_cover,
                book_last_modified=book_last_modified,
                book_fingerprint=book_fingerprint,
                book_paths=book_paths,
                book_size=book_size,
                book_rating=book_rating,
                book_created_datestamp=book_created_datestamp,
                book_datestamp=book_datestamp,
            )

        work_id = self._extract_work_id(title_row)
        if work_id is None:
            err_str = "Could not resolve work id from title row while creating a projected book"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

        # Fast path: projected book row exists already.
        cand_book_rows = self.db.search(table="books", column="book_work_id", search_term=work_id)
        if cand_book_rows:
            return cand_book_rows[0]

        # If the title row pre-dates WEMI split, create minimal WEMI nodes and retry.
        try:
            title_text = title_row["title"]
        except Exception:
            title_text = None
        if title_text:
            self.title(title=title_text, override_title_row=title_row)
            cand_book_rows = self.db.search(table="books", column="book_work_id", search_term=work_id)
            if cand_book_rows:
                return cand_book_rows[0]

        # Backstop for older compatibility assumptions.
        legacy_book_row = self.db.get_row_from_id("books", work_id)
        if legacy_book_row is not None:
            return legacy_book_row

        err_str = "Unable to project a book row from the WEMI graph"
        default_log.error(err_str)
        raise DatabaseIntegrityError(err_str)


# Backwards-compat alias for code that may still import the old mixin name.
class TitleAddMixin(BookAndTitleAdderMixin):
    """
    Retain the historical class name for the book/title compatibility mixin.

    Example:
        Existing mixin compositions can inherit TitleAddMixin to retain the same
        book and title methods.
    """
