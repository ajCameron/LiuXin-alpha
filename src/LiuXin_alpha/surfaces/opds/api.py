"""
Encode compatibility route tokens and render host-backed Atom/OPDS navigation and acquisition feeds.

Tokens use hexadecimal UTF-8 with permissive raw-text fallback, not an opaque
validated identifier scheme. XML builders escape scalar text/attributes but join
entry fragments verbatim and perform no XML validation. Feeds use fixed epoch
timestamps. Routing borrows host reads/response factories, preserves their errors,
and does not create a server or acquire download bytes itself.
"""

from __future__ import annotations

import mimetypes

from dataclasses import dataclass
from urllib.parse import quote, unquote

from LiuXin_alpha.surfaces.api import OpdsHostApi, SurfaceResponseAPI
from LiuXin_alpha.surfaces.presentation import coerce_int as _coerce_int, escape as _escape


def encode_compat_token(value: object) -> str:
    """
    Hex-encode the UTF-8 string form of a truthy value, mapping every falsey value to empty text.

    Whitespace is preserved and no URL quoting or identifier validation is applied.

    Example:
        >>> encode_compat_token("雪"), encode_compat_token(0)
        ('e99baa', '')


    :param value: Value converted through str(value or '') before encoding.
    :return: Lowercase hexadecimal UTF-8 text, or empty text for falsey input.
    """
    text = str(value or "")
    if not text:
        return ""
    return text.encode("utf-8").hex()


def decode_compat_token(raw: str) -> str:
    """
    Decode stripped hexadecimal UTF-8 when plausible, otherwise preserve stripped input text.

    Hyphens are removed only from the decoding candidate. An even, nonempty hex
    candidate is decoded; invalid UTF-8 or another decoding Exception falls back
    to the original stripped text, including hyphens. Raw text that happens to be
    valid hex is therefore interpreted as encoded data, not kept literally.

    Example:
        >>> decode_compat_token(" e9-9b-aa "), decode_compat_token("ff"), decode_compat_token("41")
        ('雪', 'ff', 'A')


    :param raw: Token-like value stringified after a falsey-to-empty fallback and stripped.
    :return: Decoded Unicode text or the original stripped spelling when decoding does not apply/succeed.
    """
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
    """
    Decode a compatibility token, strip/lowercase it, and map singular author/tag/title aliases to plural keys.

    Unknown categories remain normalized text rather than being rejected. Because
    decoding is heuristic, applying this normalizer repeatedly can decode a
    hex-looking intermediate string again.

    Example:
        >>> normalized_category_key(encode_compat_token(" Author "))
        'authors'


    :param raw: Plain or hex-encoded category selector, falsey values treated as empty.
    :return: Canonical known alias or stripped lowercase unknown category text.
    """
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
    """
    Encode an ordered-work or category-navigation token from a normalized category key.

    recent/newest map to Onewest, titles/allbooks to Otitle, and every other key
    gets an N prefix. Unknown keys are permitted rather than schema-validated.

    Example:
        >>> decode_compat_token(opds_nav_token("recent"))
        'Onewest'


    :param category: Plain or compatibility-encoded category to normalize before prefix selection.
    :return: Hexadecimal UTF-8 navigation token with the selected O or N prefix.
    """
    normalized = normalized_category_key(category)
    if normalized in {"recent", "newest"}:
        token = "Onewest"
    elif normalized in {"titles", "allbooks"}:
        token = "Otitle"
    else:
        token = "N{}".format(normalized)
    return encode_compat_token(token)


def decode_opds_nav_token(raw: str) -> tuple[str, str]:
    """
    Interpret recognized case-sensitive O/N navigation prefixes or fall back to an untyped normalized category.

    O recognizes only stripped lowercase newest/title tails and yields recent/
    titles. N accepts any normalized tail. Other O tails are normalized with their
    prefix still present. The category normalizer can perform another hex decode.

    Example:
        >>> decode_opds_nav_token(opds_nav_token("newest"))
        ('O', 'recent')


    :param raw: Encoded or raw navigation token accepted by decode_compat_token.
    :return: Prefix kind O/N/empty and the resulting category key.
    """
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
    """
    Encode a normalized category without adding the navigation prefix used by opds_nav_token.

    Example:
        >>> decode_compat_token(opds_category_token("tag"))
        'tags'


    :param category: Plain or encoded category selector normalized before encoding.
    :return: Hexadecimal UTF-8 category key, possibly empty.
    """
    return encode_compat_token(normalized_category_key(category))


def decode_opds_category_token(raw: str) -> str:
    """
    Accept plain category tokens or uppercase O/N navigation tokens and return their normalized category component.

    Example:
        >>> decode_opds_category_token(opds_nav_token("authors"))
        'authors'


    :param raw: Compatibility token decoded before choosing navigation-aware or plain normalization.
    :return: Normalized category text; unknown values are retained rather than rejected.
    """
    decoded = decode_compat_token(raw)
    if decoded.startswith(("O", "N")):
        return decode_opds_nav_token(raw)[1]
    return normalized_category_key(decoded)


def opds_item_token(*, category: str, item_id: object) -> str:
    """
    Encode an I-prefixed item ID and normalized category separated by a colon.

    Item IDs are string-formatted directly, including zero/None, without escaping
    embedded colons; a colon-containing ID is not unambiguously round-trippable.

    Example:
        >>> decode_compat_token(opds_item_token(category="tag", item_id=0))
        'I0:tags'


    :param category: Category selector normalized before joining it to the ID.
    :param item_id: Value string-formatted into the token without numeric validation.
    :return: Hexadecimal encoding of I, the item text, a colon, and the category key.
    """
    normalized = normalized_category_key(category)
    return encode_compat_token("I{}:{}".format(item_id, normalized))


def decode_opds_item_token(raw: str, default_category: str) -> tuple[str, str]:
    """
    Extract an optional uppercase I prefix and first-colon category suffix without validating item identity.

    Without a colon after I, or without I, the supplied default category is used.
    An explicit empty category suffix normalizes to empty instead of the default.

    Example:
        >>> decode_opds_item_token(opds_item_token(category="authors", item_id=7), "tags")
        ('7', 'authors')


    :param raw: Encoded or raw item token decoded through the compatibility heuristic.
    :param default_category: Category normalized only when no explicit I-token category suffix exists.
    :return: Unvalidated item text and normalized explicit/default category.
    """
    decoded = decode_compat_token(raw)
    if decoded.startswith("I"):
        payload = decoded[1:]
        if ":" in payload:
            item_id, category = payload.split(":", 1)
            return item_id, normalized_category_key(category)
        return payload, normalized_category_key(default_category)
    return decoded, normalized_category_key(default_category)


def opds_with_offset(path: str, offset: int) -> str:
    """
    Append a positive integer offset using a literal question-mark check rather than URL parsing.

    Existing offsets are not replaced, fragments are not separated, and a
    nonpositive offset returns the path unchanged, including any existing query.

    Example:
        >>> opds_with_offset("/opds?query=snow", 20)
        '/opds?query=snow&offset=20'


    :param path: URL-like value stringified without normalization or quoting.
    :param offset: Value int-converted for positivity and, when positive, the appended decimal value.
    :return: Original string path for nonpositive offsets, otherwise path plus ?offset= or &offset=.
    """
    if int(offset) <= 0:
        return str(path)
    separator = "&" if "?" in str(path) else "?"
    return "{}{}offset={}".format(path, separator, int(offset))


def opds_group_label(label: object) -> str:
    """
    Uppercase the first character of stripped stringified label text, using A for falsey or blank input.

    Punctuation/digits are retained, and Unicode uppercase expansion may produce
    several characters. This is neither an alphanumeric scan nor locale collation.

    Example:
        >>> opds_group_label(" ßeta"), opds_group_label("!title"), opds_group_label("")
        ('SS', '!', 'A')


    :param label: Display value stringified after a falsey-to-empty fallback.
    :return: Uppercase initial string, or A when no initial exists.
    """
    text = str(label or "").strip()
    if not text:
        return "A"
    return text[0].upper()


def opds_category_groups(category_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """
    Count category items by display-label initial and return groups in lexical key order.

    Counts represent input rows, not the linked-title counts stored on those rows.

    Example:
        >>> opds_category_groups([{"label": "Beta"}, {"label": "Book"}, {"label": "Alpha"}])
        [{'label': 'A', 'count': 1}, {'label': 'B', 'count': 2}]


    :param category_rows: Item mappings whose optional label field supplies the grouping initial.
    :return: New label/count dicts sorted by the exact uppercase group strings.
    """
    grouped: dict[str, int] = {}
    for item in category_rows:
        key = opds_group_label(item.get("label"))
        grouped[key] = grouped.get(key, 0) + 1
    return [{"label": key, "count": grouped[key]} for key in sorted(grouped)]


def opds_pager_hrefs(*, path: str, up_href: str, offset: int, total: int, page_size: int) -> dict[str, str]:
    """
    Build navigation links after clamping total, page size, and offset, without slicing result data.

    Total is clamped to zero, page size to one, and offset between zero and the
    last page's start. Intermediate offsets are not aligned to page boundaries.
    self/up/first/last always exist; previous/next depend on that clamped offset.
    URL assembly retains opds_with_offset's existing-query/fragment limitations.

    Example:
        >>> opds_pager_hrefs(path="/opds", up_href="/", offset=99, total=5, page_size=2)["self_href"]
        '/opds?offset=4'


    :param path: Base feed path to which positive offsets are appended.
    :param up_href: Parent navigation target stringified unchanged.
    :param offset: Requested offset int-converted and clamped for links only.
    :param total: Total result count int-converted and clamped nonnegative.
    :param page_size: Requested page length int-converted and clamped to at least one.
    :return: Named href mapping suitable for opds_feed keyword expansion.
    """
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
    """
    Render a subsection entry with escaped scalar values, a fixed epoch timestamp, and optional summary text.

    href serves as both entry ID and link target. Escaping is not URL validation
    or an XML control-character check.

    Example:
        >>> '&lt;A&gt;' in opds_nav_entry(title="<A>", href="/opds")
        True


    :param title: Display title escaped into the entry's title element.
    :param href: Target escaped into both the ID text and subsection href attribute.
    :param summary: Optional truthy summary escaped into a summary element; falsey values omit it.
    :return: XML entry fragment with leading whitespace, not a standalone feed document.
    """
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
    """
    Wrap trusted entry fragments in an Atom/OPDS feed with fixed author/icon/start metadata and optional navigation links.

    Scalar values are escaped, but entries are joined verbatim: callers must
    supply trusted already-rendered XML, not arbitrary raw user text. No XML
    parser, control-character validation, or URL validation is performed. The
    updated timestamp is the epoch rather than a live catalogue modification time.

    Example:
        >>> '<title>Snow</title>' in opds_feed(title="Snow", feed_id="opds:test", entries=[])
        True


    :param title: Escaped feed title text.
    :param feed_id: Escaped feed identifier text, not a validated URI.
    :param entries: Trusted XML entry fragments concatenated without additional escaping or separators.
    :param search_href: Escaped search link, always emitted even when empty.
    :param subtitle: Truthy text emitted as an escaped subtitle, otherwise omitted.
    :param self_href: Optional truthy current-feed link.
    :param up_href: Optional truthy parent-feed link.
    :param first_href: Optional truthy first-page link.
    :param last_href: Optional truthy last-page link.
    :param next_href: Optional truthy next-page link.
    :param previous_href: Optional truthy previous-page link.
    :return: Unicode XML document text declaring UTF-8; response encoding remains the host's responsibility.
    """
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
    """
    Render compatibility OPDS feeds through borrowed host configuration, catalogue reads, and response factories.

    The adapter retains host without opening a database or server. It builds
    acquisition links, not downloadable bytes, and forwards provider errors rather
    than converting them to empty feeds. Only explicit unrecognized/group-missing
    route cases produce its own 404 responses.

    Example:
        >>> api = OpdsApi(host)  # doctest: +SKIP

    :ivar host: Configuration, shared catalogue selectors, metadata projection, and XML/text response factories.
    """

    host: OpdsHostApi

    def page_size(self) -> int:
        """
        Clamp the configured default page size against the configured maximum and then to at least one.

        The minimum wins even when max_page_size is zero or negative. Both
        configuration values must be int-convertible; invalid values propagate.

        Example:
            >>> size = api.page_size()  # doctest: +SKIP


        :return: max(1, min(int(default_page_size), int(max_page_size))).
        """
        return max(1, min(int(self.host.config.default_page_size), int(self.host.config.max_page_size)))

    def max_ungrouped_items(self) -> int:
        """
        Read the optional grouping threshold, defaulting to 100 and clamping it nonnegative.

        A zero result disables grouping; None or malformed configured values fail
        int conversion rather than selecting the absent-attribute default.

        Example:
            >>> threshold = api.max_ungrouped_items()  # doctest: +SKIP


        :return: Nonnegative maximum ungrouped item count, with zero meaning no grouping.
        """
        return max(0, int(getattr(self.host.config, "opds_max_ungrouped_items", 100)))

    def work_entry(self, row) -> str:
        """
        Render one work's metadata as an acquisition entry with author, image, format, and summary links.

        Four legacy/current image relations advertise image/png regardless of the
        eventual cover response. Format MIME is guessed from a dummy filename;
        missing guesses use application/octet-stream. Only size metadata lookup
        catches Exception; other missing fields/read/render failures propagate.
        Nonempty sizes, including zero, are escaped into length without numeric
        validation. Empty author collections produce Unknown. A falsey summary
        falls back to comma-joined tags. Scalars are escaped, not URL-quoted or
        checked for invalid XML control characters.

        Example:
            >>> entry = api.work_entry(work)  # doctest: +SKIP


        :param row: Work context forwarded unchanged to host.opds_work_metadata_payload.
        :return: XML entry fragment with fixed epoch updated time and urn:uuid-prefixed metadata identity.
        """
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
        """
        Dispatch root, search, navigation, grouped-category, and category-item paths into host-created OPDS responses.

        Split on slashes, discard empty components, and URL-decode each component.
        Root requires exactly opds; other supported routes accept extra trailing
        components. Search prefers a decoded path token, falling back to the first
        stripped query parameter only when that token is falsey. Offsets use the
        first query value and nonnegative integer coercion. Pager links clamp an
        overlarge offset to the last page, but actual data slices retain the raw
        coerced offset and can be empty while self points to a populated page.

        Navigation renders ordered works for decoded recent/titles/O routes;
        other categories use host item rows. Positive grouping thresholds apply
        only when item count exceeds the threshold, grouping by lexical uppercase
        initials. Group routes reject work-list categories, blank groups, and
        unmatched groups. They normalize the group initial again, so an expanding
        initial such as SS from sharp-s may fail to select its original group.
        Item-token category suffixes can override route categories.
        All payload reads/rendering failures propagate; unknown routes return a
        plain-text 404. This method neither negotiates HTTP methods nor reads files.

        Example:
            >>> response = api.serve("/opds", {})  # doctest: +SKIP


        :param path: Slash-separated request path, expected without its query string.
        :param query: Parsed multi-value query mapping supplying offset and optional search query.
        :return: Host XML response for a supported feed, or host text response for an explicit not-found route/group.
        """
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
