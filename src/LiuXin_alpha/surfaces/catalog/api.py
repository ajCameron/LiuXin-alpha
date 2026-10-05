"""
Adapt shared Core read-model results into Calibre-style catalogue routes, AJAX payloads, and tag-browser trees.

Compatibility token policy is shared with OPDS. Host hooks provide additional
relationship/credit projection, while image operations use a retained image
backend. Some methods mutate returned read-model dictionaries in place; others
create new wrapper payloads without deep-copying rows. Errors generally propagate
and no atomic read snapshot, authorization policy, or server lifecycle is added.
PLACEHOLDER_PNG is the module's built-in fallback image payload.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

from LiuXin_alpha.surfaces.api import CalibreCatalogHostApi
from LiuXin_alpha.surfaces.images import ImageBackend
from LiuXin_alpha.surfaces.read_model import ReadModelBackend
from LiuXin_alpha.surfaces.opds.api import decode_compat_token, encode_compat_token, normalized_category_key
from LiuXin_alpha.surfaces.acquisition_types import ResolvedFileTarget as _ResolvedFileTarget
from LiuXin_alpha.surfaces.presentation import (
    coerce_int as _coerce_int,
    escape as _escape,
    row_value as _row_value,
)


PLACEHOLDER_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc`\x00\x01"
    b"\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
)


@dataclass
class CalibreCatalogBackend:
    """
    Build Calibre-compatible catalogue projections over borrowed host, read-model, and image collaborators.

    Omitted read_model creates a new adapter; omitted images reuses that model's
    images attribute. Explicit image overrides are not written back to the read
    model or host. Construction issues no catalogue read and owns no Core lifecycle.

    Example:
        >>> CalibreCatalogBackend.category_icon_name(" AUTHORS ")
        'user_profile.png'

    :ivar host: Display/configuration and relationship/credit policies used by compatibility payloads.
    :ivar read_model: Shared catalogue reader, created from host only when None.
    :ivar images: Image collaborator, taken from read_model only when None.
    """

    host: CalibreCatalogHostApi
    read_model: Optional[ReadModelBackend] = None
    images: Optional[ImageBackend] = None

    def __post_init__(self) -> None:
        """
        Supply a missing read model and then reuse its image adapter unless an image override was provided.

        Example:
            >>> backend = CalibreCatalogBackend(host, read_model=model)  # doctest: +SKIP


        :return: None after collaborator assignment; provided objects are not validated, refreshed, or synchronized.
        """
        if self.read_model is None:
            self.read_model = ReadModelBackend(self.host)
        if self.images is None:
            self.images = self.read_model.images

    @staticmethod
    def encode_compat_token(value: object) -> str:
        """
        Delegate UTF-8 hexadecimal token encoding with the shared falsey-to-empty policy.

        Example:
            >>> CalibreCatalogBackend.encode_compat_token("雪")
            'e99baa'


        :param value: Value stringified by the OPDS compatibility encoder, with falsey values becoming empty.
        :return: Lowercase hexadecimal token or empty text.
        """
        return encode_compat_token(value)

    @staticmethod
    def decode_compat_token(raw: str) -> str:
        """
        Delegate permissive hex/UTF-8 decoding with stripped raw-text fallback.

        Example:
            >>> CalibreCatalogBackend.decode_compat_token("e9-9b-aa")
            '雪'


        :param raw: Plain or encoded token accepted by the shared OPDS decoder.
        :return: Decoded text when valid, otherwise the original stripped token.
        """
        return decode_compat_token(raw)

    @staticmethod
    def normalized_category_key(raw: object) -> str:
        """
        Delegate compatibility decoding, whitespace/case normalization, and singular category aliases.

        Example:
            >>> CalibreCatalogBackend.normalized_category_key(" Author ")
            'authors'


        :param raw: Plain or encoded category selector, not independently validated against schema.
        :return: Normalized category key, retaining unknown normalized values.
        """
        return normalized_category_key(raw)

    @staticmethod
    def category_icon_name(category: str) -> str:
        """
        Select a conventional resource filename for a known lowercase category, otherwise blank.png.

        This strips/case-normalizes but does not decode tokens or apply singular aliases.

        Example:
            >>> CalibreCatalogBackend.category_icon_name("author")
            'blank.png'


        :param category: Category-like value stringified after a falsey-to-empty fallback.
        :return: Resource basename only; no image loading or existence check occurs.
        """
        mapping = {
            "allbooks": "book.png",
            "newest": "forward.png",
            "authors": "user_profile.png",
            "tags": "tags.png",
            "series": "series.png",
            "titles": "book.png",
            "recent": "forward.png",
        }
        return mapping.get(str(category or "").strip().lower(), "blank.png")

    @staticmethod
    def category_display_name(category: str) -> str:
        """
        Use the shared read-model's fixed English category labels and title-cased unknown-name fallback.

        Example:
            >>> CalibreCatalogBackend.category_display_name("allbooks")
            'All books'


        :param category: Plain category value forwarded without compatibility-token decoding here.
        :return: Shared display label, not a localized or schema-validated name.
        """
        return ReadModelBackend.category_display_name(category)

    def author_tables(self) -> list[str]:
        """
        Delegate preferred agent-table discovery, retaining the read model's agents fallback.

        Example:
            >>> tables = backend.author_tables()  # doctest: +SKIP


        :return: Read-model table-name list unchanged, normally agents/human_agents/org_agents in preference order.
        """
        return self.read_model.author_tables()

    def split_compat_book_token(self, raw_book_id: str) -> tuple[Optional[int], str]:
        """
        Split a stripped book token at its first underscore and int-convert the leading component.

        The remainder is retained verbatim after outer stripping. Integer parsing
        catches Exception and returns None, but initial string conversion is outside
        that handler. Negative/zero parsed IDs are accepted; falsey input becomes
        empty before parsing, so numeric zero differs from text '0'.

        Example:
            >>> backend.split_compat_book_token("7_90_120")  # doctest: +SKIP
            (7, '90_120')


        :param raw_book_id: ID plus optional underscore suffix, stringified after a falsey-to-empty fallback.
        :return: Integer ID or None paired with the untouched remainder; empty input returns (None, '').
        """
        text = str(raw_book_id or "").strip()
        if not text:
            return None, ""
        base, _sep, rest = text.partition("_")
        try:
            return int(base), rest
        except Exception:
            return None, rest

    def work_rows(self, *, sorted_by: str) -> list[object]:
        """
        Delegate ordered work enumeration without copying or re-sorting the returned list.

        Example:
            >>> works = backend.work_rows(sorted_by="recent")  # doctest: +SKIP


        :param sorted_by: Ordering token forwarded unchanged; the shared model recognizes exact recent specially.
        :return: Read-model work list, with its absence/fallback/error behavior preserved.
        """
        return self.read_model.work_rows(sorted_by=sorted_by)

    def work_row_from_token(self, raw_book_id: object) -> object | None:
        """
        Parse a compatibility work token, ignore its suffix, and fetch the resulting integer work ID.

        Example:
            >>> work = backend.work_row_from_token("7_main")  # doctest: +SKIP


        :param raw_book_id: Falsey-to-empty token value passed through the shared book-token parser.
        :return: Read-model work row or None for invalid token/absent work; lookup failures propagate.
        """
        work_id, _suffix = self.split_compat_book_token(str(raw_book_id or ""))
        if work_id is None:
            return None
        return self.read_model.row_by_id("works", int(work_id))

    def work_rows_for_ids(self, raw_ids: object) -> list[object]:
        """
        Resolve comma-separated work tokens in input order, skipping missing/invalid entries without deduplicating successes.

        Example:
            >>> works = backend.work_rows_for_ids("7_main, 8, 7")  # doctest: +SKIP


        :param raw_ids: Stringified falsey-to-empty comma list; each token is stripped before lookup.
        :return: New list of resolved row objects, retaining duplicate IDs and stopping on visible lookup failures.
        """
        rows: list[object] = []
        for token in str(raw_ids or "").split(","):
            row = self.work_row_from_token(token.strip())
            if row is not None:
                rows.append(row)
        return rows

    def search_work_rows(self, query_text: object) -> list[object]:
        """
        Search normalized nonblank text through the works filter and retain only entries labelled exactly works.

        Example:
            >>> works = backend.search_work_rows(" snow ")  # doctest: +SKIP


        :param query_text: Value stringified after falsey fallback and stripped; blank text returns before searching.
        :return: Original row objects in search-entry order, excluding other tables without additional deduplication.
        """
        query = str(query_text or "").strip()
        if not query:
            return []
        return [
            entry["row"]
            for entry in self.read_model.search_entries(query, table_filter="works")
            if str(entry.get("table")) == "works"
        ]

    def work_rows_for_query_or_recent(self, query_text: object) -> list[object]:
        """
        Search nonblank normalized text or list recent works when the normalized query is empty.

        Example:
            >>> works = backend.work_rows_for_query_or_recent("")  # doctest: +SKIP


        :param query_text: Falsey-to-empty string value stripped before selecting search versus recent enumeration.
        :return: Search-selected work list or recent-ordered work list without hiding provider failures.
        """
        query = str(query_text or "").strip()
        if query:
            return self.search_work_rows(query)
        return self.work_rows(sorted_by="recent")

    def works_for_linked_entity(self, table: str, raw_row_id: str) -> list[object]:
        """
        Delegate linked-work lookup using the read model's table/identity validation and relationship policy.

        Example:
            >>> works = backend.works_for_linked_entity("tags", "7")  # doctest: +SKIP


        :param table: Entity table forwarded unchanged.
        :param raw_row_id: Entity ID text forwarded unchanged for model parsing.
        :return: Read-model linked-work list, retaining its normal absence and visible failure behavior.
        """
        return self.read_model.works_for_linked_entity(table, raw_row_id)

    def work_rows_for_category_item(self, category: str, raw_item_token: object) -> list[object]:
        """
        Select works for a normalized category and plain item ID, with first-nonempty-table behavior for authors.

        Author results are not merged across tables. Tags use the read model's
        selected tag source or tags fallback; series uses its own table. Work-list
        categories ignore the item token and choose title/recent ordering. Unlike
        category normalization, item text is not compatibility-hex decoded here.

        Example:
            >>> works = backend.work_rows_for_category_item("author", "7")  # doctest: +SKIP


        :param category: Plain/encoded selector normalized through shared category aliases.
        :param raw_item_token: Falsey-to-empty string item value passed to linked-entity lookup without token decoding.
        :return: First matching author-table result, selected entity/work list, or an empty list for unknown categories.
        """
        kind = self.normalized_category_key(category)
        item_token = str(raw_item_token or "")
        if kind == "authors":
            rows: list[object] = []
            for table in self.author_tables():
                rows = self.works_for_linked_entity(table, item_token)
                if rows:
                    break
            return rows
        if kind == "tags":
            return self.works_for_linked_entity(self.read_model.tag_category_table() or "tags", item_token)
        if kind == "series":
            return self.works_for_linked_entity("series", item_token)
        if kind in {"allbooks", "titles"}:
            return self.work_rows(sorted_by="title")
        if kind in {"newest", "recent"}:
            return self.work_rows(sorted_by="recent")
        return []

    def category_rows(self, category: str) -> list[dict[str, object]]:
        """
        Obtain normalized category entries and rewrite their URL fields in place to compatibility HTML routes.

        Works use /book, authors include table and ID, and tags/series use their
        dedicated routes. Route components are percent-quoted with safe=''. Unknown
        categories retain supplied URLs. Both the original list and entry mappings
        returned by the read model are reused, not copied.

        Example:
            >>> entries = backend.category_rows("authors")  # doctest: +SKIP


        :param category: Plain or encoded category normalized before read-model enumeration.
        :return: The read model's entry list after applicable URL mutations.
        """
        kind = self.normalized_category_key(category)
        rows = self.read_model.category_rows(kind)
        if kind in {"allbooks", "titles", "recent", "newest"}:
            for item in rows:
                item["url"] = "/book/{}".format(quote(str(item["id"]), safe=""))
            return rows
        if kind == "authors":
            for item in rows:
                item["url"] = "/author/{}/{}".format(quote(str(item["table"]), safe=""), quote(str(item["id"]), safe=""))
            return rows
        if kind == "tags":
            for item in rows:
                item["url"] = "/tag/{}".format(quote(str(item["id"]), safe=""))
            return rows
        if kind == "series":
            for item in rows:
                item["url"] = "/series/{}".format(quote(str(item["id"]), safe=""))
            return rows
        return rows

    def browse_count(self, kind: str) -> int:
        """
        Forward an exact browse-count token without category alias or compatibility-token normalization.

        Example:
            >>> count = backend.browse_count("titles")  # doctest: +SKIP


        :param kind: Exact read-model category token, passed unchanged.
        :return: Read-model count, including its zero result for unknown tokens and visible read failures.
        """
        return self.read_model.browse_count(kind)

    def category_summary_payload(self) -> list[dict[str, object]]:
        """
        Add encoded AJAX category routes and conventional icon paths to the shared navigation summary.

        Example:
            >>> summary = backend.category_summary_payload()  # doctest: +SKIP


        :return: New summary dicts in provider order with string names/categories, boolean flags, integer counts, and encoded routes.
        """
        entries = []
        for entry in self.read_model.category_summary_payload():
            category = str(entry["category"])
            encoded = self.encode_compat_token(category)
            entries.append(
                {
                    "name": str(entry["name"]),
                    "url": "/ajax/category/{}/main".format(encoded),
                    "icon": "/icon/{}".format(self.category_icon_name(category)),
                    "is_category": bool(entry["is_category"]),
                    "count": int(entry["count"]),
                    "encoded_name": encoded,
                    "category": category,
                }
            )
        return entries

    @staticmethod
    def thumbnail_text(text: str) -> str:
        """
        Delegate Unicode thumbnail-initial selection to the static shared read-model policy.

        Example:
            >>> CalibreCatalogBackend.thumbnail_text(" -- ßeta")
            'SS'


        :param text: Title-like input forwarded independently of the retained image collaborator.
        :return: Uppercase first alphanumeric character, possibly expanded, or ? for no usable character.
        """
        return ReadModelBackend.thumbnail_text(text)

    def work_subtitle(self, row) -> str:
        """
        Delegate compact credit/series/tag subtitle assembly without additional escaping or caching.

        Example:
            >>> subtitle = backend.work_subtitle(work)  # doctest: +SKIP


        :param row: Work row forwarded unchanged to the read model.
        :return: Shared plain-text subtitle, with repeated-read and failure behavior preserved.
        """
        return self.read_model.work_subtitle(row)

    def work_sort_value(self, row, *, sort_key: str) -> object:
        """
        Delegate work sort-key projection from display text, bounded relationship names, or numeric ID.

        Example:
            >>> key = backend.work_sort_value(work, sort_key="author")  # doctest: +SKIP


        :param row: Work row whose fields/relationships supply the sorting value.
        :param sort_key: Selector forwarded unchanged for read-model normalization and fallback.
        :return: Shared string or integer key without wrapper-side conversion.
        """
        return self.read_model.work_sort_value(row, sort_key=sort_key)

    def work_metadata_payload(self, row) -> dict[str, object]:
        """
        Extend shared work metadata in place with encoded author/tag/series AJAX category links.

        Additional host relationship/credit reads are independent of the metadata
        reads. IDs are stringified even when absent, and author links carry no
        table discriminator. Existing category_urls is overwritten. No atomic
        snapshot, deep copy, or empty-ID filtering is added.

        Example:
            >>> metadata = backend.work_metadata_payload(work)  # doctest: +SKIP


        :param row: Work row passed to shared metadata and host relationship/credit hooks.
        :return: The shared metadata dict after category_urls mutation; failures can occur after earlier reads.
        """
        payload = self.read_model.work_metadata_payload(row)
        related = self.host._related_rows_by_table(row)
        tag_table, tag_rows = self.read_model.work_tag_rows(related)
        category_urls = {
            "authors": [
                "/ajax/books_in/{}/{}/main".format(
                    self.encode_compat_token("authors"),
                    self.encode_compat_token(str(_row_value(entry["row"], self.host._id_column(str(entry["table"])) or ""))),
                )
                for entry in self.host._work_credit_entries(row)
            ],
            "tags": [
                "/ajax/books_in/{}/{}/main".format(
                    self.encode_compat_token("tags"),
                    self.encode_compat_token(str(_row_value(one, self.host._id_column(tag_table or "") or ""))),
                )
                for one in tag_rows
            ],
            "series": [
                "/ajax/books_in/{}/{}/main".format(
                    self.encode_compat_token("series"),
                    self.encode_compat_token(str(_row_value(one, self.host._id_column("series") or ""))),
                )
                for one in related.get("series", [])
            ],
        }
        payload["category_urls"] = category_urls
        return payload

    def work_rows_payload(self, rows: list[object]) -> list[dict[str, object]]:
        """
        Project work rows through the shared read model without this adapter's category_urls augmentation.

        Example:
            >>> payloads = backend.work_rows_payload(works)  # doctest: +SKIP


        :param rows: Work rows processed in input order without deduplication.
        :return: New list of shared metadata dicts; unlike books_metadata_payload, no extra category links are added.
        """
        return [self.read_model.work_metadata_payload(row) for row in rows]

    def ajax_setup_payload(self) -> dict[str, object]:
        """
        Describe the fixed main compatibility library and root-relative icon, OPDS, mobile, and search routes.

        Example:
            >>> setup = backend.ajax_setup_payload()  # doctest: +SKIP


        :return: Fresh setup dict using host.config.title unchanged, without querying catalogue contents.
        """
        return {
            "library_id": "main",
            "library_map": {"main": {"title": self.host.config.title}},
            "icon_path": "/icon/",
            "opds_url": "/opds",
            "mobile_url": "/mobile",
            "search_url": "/ajax/search/main",
        }

    def category_route_target(self, category: str, item_id: object) -> str:
        """
        Build an HTML category-item route, resolving ambiguous author IDs against the first table containing a row.

        Author lookup int-converts the ID but embeds its original string spelling
        in the route. Ordinary conversion failure or no matching author falls back
        to /browse/authors. Tags/series route without existence checks; every other
        normalized key becomes a browse route. Components are percent-quoted.

        Example:
            >>> target = backend.category_route_target("tags", "a/b")  # doctest: +SKIP
            '/tag/a%2Fb'


        :param category: Category selector normalized using shared token and alias rules.
        :param item_id: Raw item identity, int-converted only for author lookup and otherwise stringified for paths.
        :return: Root-relative HTML route; non-conversion provider failures remain visible.
        """
        category = self.normalized_category_key(category)
        if category == "authors":
            try:
                row_id = int(item_id)
            except (TypeError, ValueError, OverflowError):
                return "/browse/authors"
            for table in self.author_tables():
                row = self.read_model.row_by_id(table, row_id)
                if row is not None:
                    return "/author/{}/{}".format(quote(table, safe=""), quote(str(item_id), safe=""))
            return "/browse/authors"
        if category == "tags":
            return "/tag/{}".format(quote(str(item_id), safe=""))
        if category == "series":
            return "/series/{}".format(quote(str(item_id), safe=""))
        return "/browse/{}".format(quote(category, safe=""))

    def category_items_payload(self, category: str, *, num: int, offset: int, sort: str, sort_order: str) -> dict[str, object]:
        """
        Wrap a shared category page in the Calibre AJAX item shape with encoded routes, icons, and placeholder ratings.

        The shared model controls sorting/slicing. Every returned item gets zero
        average_rating, false has_children, and an HTML target that may perform
        extra author lookups. Subcategories is always empty; raw IDs are retained.

        Example:
            >>> page = backend.category_items_payload("tags", num=20, offset=0, sort="name", sort_order="asc")  # doctest: +SKIP


        :param category: Plain/encoded selector normalized before the shared page request.
        :param num: Requested slice length forwarded unchanged to the read model.
        :param offset: Requested offset forwarded unchanged, then int-converted from the returned receipt.
        :param sort: Sort selector forwarded unchanged; normalized receipt text is echoed.
        :param sort_order: Direction forwarded unchanged; receipt text is echoed.
        :return: New AJAX page with integer counts/offset, converted display fields, and freshly constructed item dicts.
        """
        kind = self.normalized_category_key(category)
        payload = self.read_model.category_items_payload(kind, num=num, offset=offset, sort=sort, sort_order=sort_order)
        visible = list(payload["items"])
        items = [
            {
                "name": str(item["label"]),
                "average_rating": 0,
                "count": int(item.get("count") or 0),
                "url": "/ajax/books_in/{}/{}/main".format(
                    self.encode_compat_token(kind),
                    self.encode_compat_token(str(item["id"])),
                ),
                "has_children": False,
                "id": item["id"],
                "item_url": self.category_route_target(kind, item["id"]),
                "icon": "/icon/{}".format(self.category_icon_name(kind)),
            }
            for item in visible
        ]
        return {
            "category_name": str(payload["category_name"]),
            "base_url": "/ajax/category/{}/main".format(self.encode_compat_token(kind)),
            "total_num": int(payload["total_num"]),
            "offset": int(payload["offset"]),
            "num": len(items),
            "sort": str(payload["sort"]),
            "sort_order": str(payload["sort_order"]),
            "subcategories": [],
            "items": items,
            "icon": "/icon/{}".format(self.category_icon_name(kind)),
            "category": kind,
        }

    def search_result_payload(
        self,
        *,
        query_text: str,
        rows: list[object],
        num: int,
        offset: int,
        sort: str,
        sort_order: str,
        base_url: str,
    ) -> dict[str, object]:
        """
        Wrap a shared sorted work-ID page with compatibility library/search metadata and a separate all-work count.

        This method does not perform text search: rows are already selected by the
        caller. Counting titles can trigger additional reads and fail independently
        of successful page construction. No visible row objects are included.

        Example:
            >>> result = backend.search_result_payload(query_text="snow", rows=works, num=20, offset=0, sort="title", sort_order="asc", base_url="/ajax/search/main")  # doctest: +SKIP


        :param query_text: Query text echoed unchanged, not executed here.
        :param rows: Candidate works passed to read_model.work_list_payload for sorting and slicing.
        :param num: Requested page length forwarded unchanged.
        :param offset: Requested offset forwarded unchanged, then read from the page receipt.
        :param sort: Work sort selector passed through to the read model.
        :param sort_order: Direction selector passed through to the read model.
        :param base_url: Caller-supplied base route echoed without validation or quoting.
        :return: New AJAX result with copied book_ids, page metadata, main library identity, all-work count, and empty vl.
        """
        payload = self.read_model.work_list_payload(rows, num=num, offset=offset, sort=sort, sort_order=sort_order)
        return {
            "total_num": int(payload["total_num"]),
            "sort_order": str(payload["sort_order"]),
            "offset": int(payload["offset"]),
            "num": int(payload["num"]),
            "sort": str(payload["sort"]),
            "base_url": base_url,
            "query": query_text,
            "library_id": "main",
            "book_ids": list(payload["book_ids"]),
            "num_books_without_search": self.browse_count("titles"),
            "vl": "",
        }

    def books_metadata_payload(self, rows: list[object]) -> dict[str, dict[str, object]]:
        """
        Build category-link-augmented metadata keyed by stringified work ID, with later duplicates replacing earlier values.

        Example:
            >>> books = backend.books_metadata_payload(works)  # doctest: +SKIP


        :param rows: Work rows processed in input order through this adapter's metadata augmentation.
        :return: New string-ID mapping; absent IDs are not filtered and duplicate keys keep their first insertion position.
        """
        payload: dict[str, dict[str, object]] = {}
        for row in rows:
            metadata = self.work_metadata_payload(row)
            payload[str(metadata["id"])] = metadata
        return payload

    def metadata_rows_for_search_result(self, rows: list[object], search_result: dict[str, object]) -> list[object]:
        """
        Filter input rows by raw ID membership in the result's book_ids set, preserving input order and duplicates.

        No numeric coercion aligns string and integer IDs, and output is not
        reordered to book_ids ranking. Unhashable IDs or malformed result shapes
        remain errors rather than empty matches.

        Example:
            >>> visible = backend.metadata_rows_for_search_result(works, result)  # doctest: +SKIP


        :param rows: Original work rows whose host-selected ID column is tested.
        :param search_result: Page mapping containing a truthy iterable book_ids value, otherwise an empty selection.
        :return: New list of original matching row objects in input order.
        """
        visible_ids = set(search_result.get("book_ids") or [])
        id_column = self.host._id_column("works") or "work_id"
        return [row for row in rows if _row_value(row, id_column) in visible_ids]

    def basic_interface_data_payload(self) -> dict[str, object]:
        """
        Describe the fixed main-library cover-list interface using host title and unvalidated default page size.

        Example:
            >>> interface = backend.basic_interface_data_payload()  # doctest: +SKIP


        :return: Fresh compatibility settings with empty session/search-net values and no catalogue or user-session reads.
        """
        return {
            "library_map": {"main": {"title": self.host.config.title}},
            "default_library_id": "main",
            "icon_path": "/icon/",
            "num_per_page": self.host.config.default_page_size,
            "default_book_list_mode": "covers",
            "search_the_net_urls": [],
            "custom_list_template": None,
            "user_session_data": {},
            "library_id": "main",
        }

    def tag_browser_payload(self) -> dict[str, object]:
        """
        Build a three-category positional tree plus an item map for authors, tags, and series.

        Category IDs are c0/c1/c2 and child IDs append current row positions, not
        durable entity identities. Category counts are item counts; child counts
        come from linked-title metadata. Edit/search flags are compatibility display
        metadata, not authorization or implemented mutation promises. Per-item
        author route lookup can issue additional reads. All categories remain in
        the root, including empty ones.

        Example:
            >>> tree = backend.tag_browser_payload()  # doctest: +SKIP


        :return: New root/children structure and ID-indexed category/item metadata dicts with encoded AJAX and HTML routes.
        """
        item_map: dict[str, dict[str, object]] = {}
        root_children: list[dict[str, object]] = []
        top_level_categories = ("authors", "tags", "series")
        for index, category in enumerate(top_level_categories):
            rows = self.category_rows(category)
            category_id = "c{}".format(index)
            item_map[category_id] = {
                "category": category,
                "name": self.category_display_name(category),
                "is_category": True,
                "count": len(rows),
                "icon": "/icon/{}".format(self.category_icon_name(category)),
                "is_editable": True,
                "is_searchable": True,
            }
            child_nodes: list[dict[str, object]] = []
            for item_index, item in enumerate(rows):
                item_id = "{}:{}".format(category_id, item_index)
                item_map[item_id] = {
                    "category": category,
                    "name": str(item["label"]),
                    "count": int(item.get("count") or 0),
                    "id": item["id"],
                    "url": "/ajax/books_in/{}/{}/main".format(
                        self.encode_compat_token(category),
                        self.encode_compat_token(str(item["id"])),
                    ),
                    "item_url": self.category_route_target(category, item["id"]),
                    "is_editable": False,
                    "is_searchable": True,
                    "icon": "/icon/{}".format(self.category_icon_name(category)),
                }
                child_nodes.append({"id": item_id, "children": []})
            root_children.append({"id": category_id, "children": child_nodes})
        return {"root": {"id": None, "children": root_children}, "item_map": item_map}

    def work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        """
        Delegate direct/WEMI-linked file discovery and integer-ID deduplication to the shared read model.

        Example:
            >>> files = backend.work_file_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Existing direct-file and expression groups forwarded unchanged.
        :return: Read-model file list without extra copying, capability filtering, or error suppression.
        """
        return self.read_model.work_file_rows(related_rows_by_table)

    def work_image_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        """
        Delegate image discovery to the retained image adapter, which may differ from the read model's adapter.

        Example:
            >>> images = backend.work_image_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Direct-image/expression groups passed unchanged to the image backend.
        :return: Backend-discovered image list unchanged.
        """
        return self.images.work_image_rows(related_rows_by_table)

    def image_download_name(self, image_row) -> str:
        """
        Delegate image filename selection without adding sanitization or revalidation.

        Example:
            >>> name = backend.image_download_name(image_row)  # doctest: +SKIP


        :param image_row: Row passed unchanged to the retained image naming policy.
        :return: Image backend's suggested name, normally with cover.bin as its final fallback.
        """
        return self.images.image_download_name(image_row)

    def image_content_type(self, image_row) -> str:
        """
        Delegate declared-or-guessed image MIME selection without inspecting content bytes.

        Example:
            >>> mime = backend.image_content_type(image_row)  # doctest: +SKIP


        :param image_row: Row passed unchanged to the retained image backend.
        :return: Image backend's MIME result without wrapper-side changes.
        """
        return self.images.image_content_type(image_row)

    def image_storage_lookup_metadata(self, image_row) -> dict[str, object]:
        """
        Delegate image metadata projection and legacy file-field aliases without another copy.

        Example:
            >>> metadata = backend.image_storage_lookup_metadata(image_row)  # doctest: +SKIP


        :param image_row: Image row projected according to the image backend's host policy.
        :return: Backend lookup metadata mapping with its shallow-sharing behavior preserved.
        """
        return self.images.image_storage_lookup_metadata(image_row)

    def resolve_storage_image(self, image_row):
        """
        Delegate readable-image resolution without fetching its bytes or hiding provider errors.

        Example:
            >>> stored = backend.resolve_storage_image(image_row)  # doctest: +SKIP


        :param image_row: Image identity forwarded unchanged to the retained resolver.
        :return: Backend's bound byte reader or None for its ordinary unavailable outcomes.
        """
        return self.images.resolve_storage_image(image_row)

    def resolve_image_target(self, image_row) -> Optional[_ResolvedFileTarget]:
        """
        Delegate redirect-target resolution without independent URL/readability validation.

        Example:
            >>> target = backend.resolve_image_target(image_row)  # doctest: +SKIP


        :param image_row: Image row forwarded unchanged for identity and fallback-name projection.
        :return: Image backend's redirect target or None.
        """
        return self.images.resolve_image_target(image_row)

    def work_image_row(self, work_row) -> Optional[object]:
        """
        Delegate first-image discovery for a work without adding cover-role ranking.

        Example:
            >>> image = backend.work_image_row(work)  # doctest: +SKIP


        :param work_row: Work context forwarded to the retained image backend.
        :return: Selected original image row or None after successful empty discovery.
        """
        return self.images.work_image_row(work_row)

    def placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        """
        Delegate SVG cover-fallback rendering with unchanged work context and dimensions.

        Example:
            >>> svg = backend.placeholder_cover_svg(work, width=120, height=180)  # doctest: +SKIP


        :param work_row: Work row used by the image backend's display-text hook.
        :param width: Requested SVG width forwarded without wrapper-side clamping.
        :param height: Requested SVG height forwarded without wrapper-side clamping.
        :return: Backend SVG byte payload unchanged, not a rasterized image.
        """
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)
