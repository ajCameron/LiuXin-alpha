"""Stable compatibility tokens and navigation helpers for OPDS feeds."""

from __future__ import annotations

import mimetypes

from dataclasses import dataclass
from urllib.parse import quote, unquote

from LiuXin_alpha.surfaces.api import OpdsHostApi, SurfaceResponseAPI
from LiuXin_alpha.surfaces.web_readonly.app import _coerce_int, _escape


def encode_compat_token(value: object) -> str:
    text = str(value or "")
    if not text:
        return ""
    return text.encode("utf-8").hex()


def decode_compat_token(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    compact = text.replace("-", "")
    if compact and len(compact) % 2 == 0 and all(char in "0123456789abcdefABCDEF" for char in compact):
        try:
            return bytes.fromhex(compact).decode("utf-8")
        except Exception:
            pass
    return text


def normalized_category_key(raw: object) -> str:
    text = decode_compat_token(str(raw or "")).strip().lower()
    aliases = {
        "author": "authors",
        "authors": "authors",
        "tag": "tags",
        "tags": "tags",
        "series": "series",
        "title": "titles",
        "titles": "titles",
        "recent": "recent",
        "newest": "newest",
        "allbooks": "allbooks",
    }
    return aliases.get(text, text)


def opds_nav_token(category: str) -> str:
    normalized = normalized_category_key(category)
    if normalized in {"recent", "newest"}:
        token = "Onewest"
    elif normalized in {"titles", "allbooks"}:
        token = "Otitle"
    else:
        token = "N{}".format(normalized)
    return encode_compat_token(token)


def decode_opds_nav_token(raw: str) -> tuple[str, str]:
    decoded = decode_compat_token(raw)
    if decoded.startswith("O"):
        tail = decoded[1:].strip().lower()
        if tail == "newest":
            return "O", "recent"
        if tail == "title":
            return "O", "titles"
    if decoded.startswith("N"):
        return "N", normalized_category_key(decoded[1:])
    return "", normalized_category_key(decoded)


def opds_category_token(category: str) -> str:
    return encode_compat_token(normalized_category_key(category))


def decode_opds_category_token(raw: str) -> str:
    decoded = decode_compat_token(raw)
    if decoded.startswith(("O", "N")):
        return decode_opds_nav_token(raw)[1]
    return normalized_category_key(decoded)


def opds_item_token(*, category: str, item_id: object) -> str:
    normalized = normalized_category_key(category)
    return encode_compat_token("I{}:{}".format(item_id, normalized))


def decode_opds_item_token(raw: str, default_category: str) -> tuple[str, str]:
    decoded = decode_compat_token(raw)
    if decoded.startswith("I"):
        payload = decoded[1:]
        if ":" in payload:
            item_id, category = payload.split(":", 1)
            return item_id, normalized_category_key(category)
        return payload, normalized_category_key(default_category)
    return decoded, normalized_category_key(default_category)


def opds_with_offset(path: str, offset: int) -> str:
    if int(offset) <= 0:
        return str(path)
    separator = "&" if "?" in str(path) else "?"
    return "{}{}offset={}".format(path, separator, int(offset))


def opds_group_label(label: object) -> str:
    text = str(label or "").strip()
    if not text:
        return "A"
    return text[0].upper()


def opds_category_groups(category_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, int] = {}
    for item in category_rows:
        key = opds_group_label(item.get("label"))
        grouped[key] = grouped.get(key, 0) + 1
    return [{"label": key, "count": grouped[key]} for key in sorted(grouped)]


def opds_pager_hrefs(*, path: str, up_href: str, offset: int, total: int, page_size: int) -> dict[str, str]:
    total = max(0, int(total))
    page_size = max(1, int(page_size))
    last_offset = max(0, ((max(0, total - 1)) // page_size) * page_size) if total else 0
    clamped_offset = min(max(0, int(offset)), last_offset)
    hrefs = {
        "self_href": opds_with_offset(path, clamped_offset),
        "up_href": str(up_href),
        "first_href": opds_with_offset(path, 0),
        "last_href": opds_with_offset(path, last_offset),
    }
    if clamped_offset > 0:
        hrefs["previous_href"] = opds_with_offset(path, max(0, clamped_offset - page_size))
    if clamped_offset + page_size < total:
        hrefs["next_href"] = opds_with_offset(path, clamped_offset + page_size)
    return hrefs


def opds_nav_entry(*, title: str, href: str, summary: str = "") -> str:
    return """
  <entry>
    <title>{title}</title>
    <id>{href}</id>
    <updated>1970-01-01T00:00:00Z</updated>
    <link href='{href}' rel='subsection'/>
    {summary}
  </entry>""".format(
        title=_escape(title),
        href=_escape(href),
        summary=("<summary>{}</summary>".format(_escape(summary)) if summary else ""),
    )


def opds_feed(
    *,
    title: str,
    feed_id: str,
    entries: list[str],
    search_href: str = "/opds/search",
    subtitle: str = "",
    self_href: str = "",
    up_href: str = "",
    first_href: str = "",
    last_href: str = "",
    next_href: str = "",
    previous_href: str = "",
) -> str:
    nav_links = []
    if self_href:
        nav_links.append("  <link rel='self' href='{}'/>".format(_escape(self_href)))
    if up_href:
        nav_links.append("  <link rel='up' href='{}'/>".format(_escape(up_href)))
    if first_href:
        nav_links.append("  <link rel='first' href='{}'/>".format(_escape(first_href)))
    if last_href:
        nav_links.append("  <link rel='last' href='{}'/>".format(_escape(last_href)))
    if next_href:
        nav_links.append("  <link rel='next' href='{}'/>".format(_escape(next_href)))
    if previous_href:
        nav_links.append("  <link rel='previous' href='{}'/>".format(_escape(previous_href)))
    return """<?xml version='1.0' encoding='utf-8'?>
<feed xmlns='http://www.w3.org/2005/Atom' xmlns:opds='http://opds-spec.org/2010/catalog'>
  <title>{title}</title>
  <author><name>LiuXin</name><uri>https://calibre-ebook.com</uri></author>
  <id>{feed_id}</id>
  <icon>/favicon.png</icon>
  <updated>1970-01-01T00:00:00Z</updated>
  {subtitle}
  <link rel='search' type='application/atom+xml' href='{search_href}'/>
  <link rel='start' href='/opds'/>
{nav_links}
{entries}
</feed>
""".format(
        title=_escape(title),
        feed_id=_escape(feed_id),
        entries="".join(entries),
        search_href=_escape(search_href),
        subtitle=("<subtitle>{}</subtitle>".format(_escape(subtitle)) if subtitle else ""),
        nav_links="\n".join(nav_links),
    )


@dataclass
class OpdsApi:
    """Render OPDS navigation, acquisition, and search feeds through a host port."""

    host: OpdsHostApi

    def page_size(self) -> int:
        return max(1, min(int(self.host.config.default_page_size), int(self.host.config.max_page_size)))

    def max_ungrouped_items(self) -> int:
        return max(0, int(getattr(self.host.config, "opds_max_ungrouped_items", 100)))

    def work_entry(self, row) -> str:
        metadata = self.host.opds_work_metadata_payload(row)
        links = [
            "<link type='image/png' href='{href}' rel='http://opds-spec.org/cover'/>".format(href=_escape(str(metadata["cover"]))),
            "<link type='image/png' href='{href}' rel='http://opds-spec.org/thumbnail'/>".format(href=_escape(str(metadata["thumbnail"]))),
            "<link type='image/png' href='{href}' rel='http://opds-spec.org/image'/>".format(href=_escape(str(metadata["cover"]))),
            "<link type='image/png' href='{href}' rel='http://opds-spec.org/image/thumbnail'/>".format(href=_escape(str(metadata["thumbnail"]))),
        ]
        for fmt in metadata["formats_detail"]:
            media_type = mimetypes.guess_type("dummy." + str(fmt["format"]).lower())[0] or "application/octet-stream"
            size_attr = ""
            try:
                size_value = metadata["format_metadata"][str(fmt["format"])]["size"]
            except Exception:
                size_value = None
            if size_value not in (None, ""):
                size_attr = " length='{}'".format(_escape(size_value))
            links.append(
                "<link type='{media_type}' href='{href}' rel='http://opds-spec.org/acquisition' title='{title}'{size_attr}/>".format(
                    media_type=_escape(media_type),
                    href=_escape(str(fmt["download_url"])),
                    title=_escape(str(fmt["format"])),
                    size_attr=size_attr,
                )
            )
        authors = "".join("<author><name>{}</name></author>".format(_escape(author)) for author in metadata["authors"]) or "<author><name>Unknown</name></author>"
        content = metadata["summary"] or ", ".join(metadata["tags"])
        return """
  <entry>
    <title>{title}</title>
    <id>urn:uuid:{uuid}</id>
    <updated>1970-01-01T00:00:00Z</updated>
    {authors}
    <link href='/book/{id}'/>
    {links}
    <summary>{summary}</summary>
  </entry>""".format(
            title=_escape(str(metadata["title"])),
            id=_escape(str(metadata["id"])),
            uuid=_escape(str(metadata["uuid"])),
            authors=authors,
            links="".join(links),
            summary=_escape(str(content)),
        )

    def serve(self, path: str, query: dict[str, list[str]]) -> SurfaceResponseAPI:
        parts = [unquote(part) for part in path.split("/") if part]
        if parts == ["opds"]:
            entries = [
                opds_nav_entry(title="Recent", href="/opds/navcatalog/{}".format(opds_nav_token("recent")), summary="Newest titles"),
                opds_nav_entry(title="Titles", href="/opds/navcatalog/{}".format(opds_nav_token("titles")), summary="Browse all titles"),
                opds_nav_entry(title="Authors", href="/opds/navcatalog/{}".format(opds_nav_token("authors")), summary="Browse authors"),
                opds_nav_entry(title="Tags", href="/opds/navcatalog/{}".format(opds_nav_token("tags")), summary="Browse tags"),
                opds_nav_entry(title="Series", href="/opds/navcatalog/{}".format(opds_nav_token("series")), summary="Browse series"),
            ]
            return self.host.opds_xml_response(
                opds_feed(
                    title=self.host.config.title,
                    feed_id="opds:root",
                    entries=entries,
                    subtitle="Books in your library",
                    self_href="/opds",
                )
            )
        if len(parts) >= 2 and parts[0] == "opds" and parts[1] == "search":
            offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
            query_text = ""
            if len(parts) >= 3:
                query_text = decode_compat_token(parts[2])
            if not query_text:
                query_text = str((query.get("query") or [""])[0] or "").strip()
            rows = self.host.opds_search_work_rows(query_text)
            page_size = self.page_size()
            href_base = "/opds/search/{}".format(quote(str(query_text), safe="")) if query_text else "/opds/search"
            hrefs = opds_pager_hrefs(path=href_base, up_href="/opds", offset=offset, total=len(rows), page_size=page_size)
            visible = rows[offset : offset + page_size]
            return self.host.opds_xml_response(
                opds_feed(
                    title="Search: {}".format(query_text or "all"),
                    feed_id="opds:search:{}".format(query_text),
                    entries=[self.work_entry(row) for row in visible],
                    search_href="/opds/search",
                    subtitle="Search results",
                    **hrefs,
                )
            )
        if len(parts) >= 3 and parts[0] == "opds" and parts[1] == "navcatalog":
            offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
            token_kind, category = decode_opds_nav_token(parts[2])
            if category in {"recent", "titles"} or token_kind == "O":
                rows = self.host.opds_work_rows(sorted_by=("recent" if category == "recent" else "title"))
                page_size = self.page_size()
                path_base = "/opds/navcatalog/{}".format(opds_nav_token(category))
                hrefs = opds_pager_hrefs(path=path_base, up_href="/opds", offset=offset, total=len(rows), page_size=page_size)
                visible = rows[offset : offset + page_size]
                return self.host.opds_xml_response(
                    opds_feed(
                        title=category.title(),
                        feed_id="opds:{}".format(category),
                        entries=[self.work_entry(row) for row in visible],
                        subtitle="Books sorted by {}".format(category.title()),
                        **hrefs,
                    )
                )
            page_size = self.page_size()
            category_rows = self.host.opds_category_rows(category)
            path_base = "/opds/navcatalog/{}".format(opds_nav_token(category))
            should_group = self.max_ungrouped_items() > 0 and len(category_rows) > self.max_ungrouped_items()
            entries = []
            if should_group:
                groups = opds_category_groups(category_rows)
                hrefs = opds_pager_hrefs(path=path_base, up_href="/opds", offset=offset, total=len(groups), page_size=page_size)
                visible_groups = groups[offset : offset + page_size]
                for item in visible_groups:
                    entries.append(
                        opds_nav_entry(
                            title=str(item["label"]),
                            href="/opds/categorygroup/{}/{}".format(
                                opds_category_token(category),
                                encode_compat_token(str(item["label"])),
                            ),
                            summary="{} items".format(item["count"]),
                        )
                    )
            else:
                hrefs = opds_pager_hrefs(path=path_base, up_href="/opds", offset=offset, total=len(category_rows), page_size=page_size)
                visible = category_rows[offset : offset + page_size]
                for item in visible:
                    entries.append(
                        opds_nav_entry(
                            title=str(item["label"]),
                            href="/opds/category/{}/{}".format(
                                opds_nav_token(category),
                                opds_item_token(category=category, item_id=item["id"]),
                            ),
                            summary="{} linked titles".format(item["count"]),
                        )
                    )
            return self.host.opds_xml_response(
                opds_feed(
                    title=self.host.opds_category_display_name(category),
                    feed_id="opds:{}".format(category),
                    entries=entries,
                    subtitle="By {}".format(self.host.opds_category_display_name(category)),
                    **hrefs,
                )
            )
        if len(parts) >= 4 and parts[0] == "opds" and parts[1] == "categorygroup":
            offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
            category = decode_opds_category_token(parts[2])
            if category in {"recent", "titles", "allbooks", "newest"}:
                return self.host.opds_text_response("404 Not Found", "Unknown OPDS route.\n", content_type="text/plain")
            group_label = decode_compat_token(parts[3]).strip() or str(parts[3]).strip()
            if not group_label:
                return self.host.opds_text_response("404 Not Found", "Unknown OPDS route.\n", content_type="text/plain")
            category_rows = [
                item
                for item in self.host.opds_category_rows(category)
                if opds_group_label(item.get("label")) == opds_group_label(group_label)
            ]
            if not category_rows:
                return self.host.opds_text_response("404 Not Found", "Unknown OPDS route.\n", content_type="text/plain")
            page_size = self.page_size()
            canonical_group_label = opds_group_label(group_label)
            path_base = "/opds/categorygroup/{}/{}".format(
                opds_category_token(category),
                encode_compat_token(canonical_group_label),
            )
            up_href = "/opds/navcatalog/{}".format(opds_nav_token(category))
            hrefs = opds_pager_hrefs(path=path_base, up_href=up_href, offset=offset, total=len(category_rows), page_size=page_size)
            visible = category_rows[offset : offset + page_size]
            entries = []
            for item in visible:
                entries.append(
                    opds_nav_entry(
                        title=str(item["label"]),
                        href="/opds/category/{}/{}".format(
                            opds_nav_token(category),
                            opds_item_token(category=category, item_id=item["id"]),
                        ),
                        summary="{} linked titles".format(item["count"]),
                    )
                )
            return self.host.opds_xml_response(
                opds_feed(
                    title="{} :: {}".format(self.host.opds_category_display_name(category), canonical_group_label),
                    feed_id="opds:categorygroup:{}:{}".format(category, canonical_group_label),
                    entries=entries,
                    subtitle="By {} :: {}".format(self.host.opds_category_display_name(category), canonical_group_label),
                    **hrefs,
                )
            )
        if len(parts) >= 4 and parts[0] == "opds" and parts[1] == "category":
            offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
            token_kind, category = decode_opds_nav_token(parts[2])
            item_token, token_category = decode_opds_item_token(parts[3], category)
            if token_category:
                category = token_category
            rows = self.host.opds_rows_for_category_item(category, item_token)
            page_size = self.page_size()
            path_base = "/opds/category/{}/{}".format(
                opds_nav_token(category if token_kind != "O" else category),
                opds_item_token(category=category, item_id=item_token),
            )
            up_href = "/opds/navcatalog/{}".format(opds_nav_token(category))
            hrefs = opds_pager_hrefs(path=path_base, up_href=up_href, offset=offset, total=len(rows), page_size=page_size)
            visible = rows[offset : offset + page_size]
            return self.host.opds_xml_response(
                opds_feed(
                    title="{} {}".format(category.title(), item_token),
                    feed_id="opds:{}:{}".format(category, item_token),
                    entries=[self.work_entry(row) for row in visible],
                    subtitle="By {} :: {}".format(category.title(), item_token),
                    **hrefs,
                )
            )
        return self.host.opds_text_response("404 Not Found", "Unknown OPDS route.\n", content_type="text/plain")
