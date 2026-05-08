from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

from LiuXin_alpha.surfaces.api import CalibreCatalogHostApi
from LiuXin_alpha.surfaces.images import ImageBackend
from LiuXin_alpha.surfaces.read_model import ReadModelBackend
from LiuXin_alpha.surfaces.opds.api import decode_compat_token, encode_compat_token, normalized_category_key
from LiuXin_alpha.surfaces.web_readonly.app import _ResolvedFileTarget, _coerce_int, _escape, _row_value


PLACEHOLDER_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc`\x00\x01"
    b"\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
)


@dataclass
class CalibreCatalogBackend:
    host: CalibreCatalogHostApi
    read_model: Optional[ReadModelBackend] = None
    images: Optional[ImageBackend] = None

    def __post_init__(self) -> None:
        if self.read_model is None:
            self.read_model = ReadModelBackend(self.host)
        if self.images is None:
            self.images = self.read_model.images

    @staticmethod
    def encode_compat_token(value: object) -> str:
        return encode_compat_token(value)

    @staticmethod
    def decode_compat_token(raw: str) -> str:
        return decode_compat_token(raw)

    @staticmethod
    def normalized_category_key(raw: object) -> str:
        return normalized_category_key(raw)

    @staticmethod
    def category_icon_name(category: str) -> str:
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
        return ReadModelBackend.category_display_name(category)

    def author_tables(self) -> list[str]:
        return self.read_model.author_tables()

    def split_compat_book_token(self, raw_book_id: str) -> tuple[Optional[int], str]:
        text = str(raw_book_id or "").strip()
        if not text:
            return None, ""
        base, _sep, rest = text.partition("_")
        try:
            return int(base), rest
        except Exception:
            return None, rest

    def work_rows(self, *, sorted_by: str) -> list[object]:
        return self.read_model.work_rows(sorted_by=sorted_by)

    def works_for_linked_entity(self, table: str, raw_row_id: str) -> list[object]:
        return self.read_model.works_for_linked_entity(table, raw_row_id)

    def category_rows(self, category: str) -> list[dict[str, object]]:
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
        return self.read_model.browse_count(kind)

    def category_summary_payload(self) -> list[dict[str, object]]:
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
        return ReadModelBackend.thumbnail_text(text)

    def work_subtitle(self, row) -> str:
        return self.read_model.work_subtitle(row)

    def work_sort_value(self, row, *, sort_key: str) -> object:
        return self.read_model.work_sort_value(row, sort_key=sort_key)

    def work_metadata_payload(self, row) -> dict[str, object]:
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
        return [self.read_model.work_metadata_payload(row) for row in rows]

    def ajax_setup_payload(self) -> dict[str, object]:
        return {
            "library_id": "main",
            "library_map": {"main": {"title": self.host.config.title}},
            "icon_path": "/icon/",
            "opds_url": "/opds",
            "mobile_url": "/mobile",
            "search_url": "/ajax/search/main",
        }

    def category_route_target(self, category: str, item_id: object) -> str:
        category = self.normalized_category_key(category)
        if category == "authors":
            for table in self.author_tables():
                try:
                    row = self.host.db.get_row_from_id(table, int(item_id))
                except Exception:
                    row = None
                if row is not None:
                    return "/author/{}/{}".format(quote(table, safe=""), quote(str(item_id), safe=""))
            return "/browse/authors"
        if category == "tags":
            return "/tag/{}".format(quote(str(item_id), safe=""))
        if category == "series":
            return "/series/{}".format(quote(str(item_id), safe=""))
        return "/browse/{}".format(quote(category, safe=""))

    def category_items_payload(self, category: str, *, num: int, offset: int, sort: str, sort_order: str) -> dict[str, object]:
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
        payload: dict[str, dict[str, object]] = {}
        for row in rows:
            metadata = self.work_metadata_payload(row)
            payload[str(metadata["id"])] = metadata
        return payload

    def basic_interface_data_payload(self) -> dict[str, object]:
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
        return self.read_model.work_file_rows(related_rows_by_table)

    def work_image_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        return self.images.work_image_rows(related_rows_by_table)

    def image_download_name(self, image_row) -> str:
        return self.images.image_download_name(image_row)

    def image_content_type(self, image_row) -> str:
        return self.images.image_content_type(image_row)

    def image_storage_lookup_metadata(self, image_row) -> dict[str, object]:
        return self.images.image_storage_lookup_metadata(image_row)

    def resolve_storage_image(self, image_row):
        return self.images.resolve_storage_image(image_row)

    def resolve_image_target(self, image_row) -> Optional[_ResolvedFileTarget]:
        return self.images.resolve_image_target(image_row)

    def work_image_row(self, work_row) -> Optional[object]:
        return self.images.work_image_row(work_row)

    def placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)
