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
    Compatibility adders for legacy ``title`` and ``book`` concepts.

    On FRBR/WEMI schemas these methods write canonical WEMI rows and return
    compatibility projections where available.
    """

    @staticmethod
    def _split_break_joined(value):
        """
        Split values that use the legacy "(#BREAK#)" separator.
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
        Try to resolve a work id from a legacy title/book/work-like row.
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
        if explicit_sort is not None:
            return explicit_sort
        try:
            return generate_title_sort(title)
        except Exception:
            return six_unicode(title)

    @staticmethod
    def _update_row(row, payload):
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
        Legacy add path for non-FRBR schemas that still expose a writable
        ``titles`` table.
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
        Legacy add path for non-FRBR schemas that still expose writable
        ``books``.
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
        Compatibility entrypoint that writes canonical WEMI rows.

        On FRBR-first schemas this creates/updates:
         - one work
         - one preferred expression
         - one manifestation
         - zero or more items (one per supplied file path/name)

        The return value stays backward-compatible: a row from ``titles`` when
        available, otherwise the underlying ``works`` row.
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
        Compatibility wrapper for FRBR-first schemas.

        On modern schema versions this resolves the projected row in ``books_v``
        for the work tied to ``title_row``.
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
    pass
