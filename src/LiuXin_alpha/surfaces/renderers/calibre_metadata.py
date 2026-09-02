"""HTML renderers for Calibre-shaped metadata objects."""

from __future__ import annotations

import os

from binascii import hexlify
from functools import partial

from LiuXin_alpha.constants import filesystem_encoding
from LiuXin_alpha.library.comments import comments_to_html
from LiuXin_alpha.metadata import fmt_sidx
from LiuXin_alpha.metadata.web_sources.identify import urls_from_identifiers
from LiuXin_alpha.utils.calibre import force_unicode, prepare_string_for_xml
from LiuXin_alpha.utils.date import is_date_undefined
from LiuXin_alpha.utils.calibre_compat.utils.formatter import EvalFormatter
from LiuXin_alpha.utils.text.icu import sort_key
from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.localization import calibre_langcode_to_name
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

default_sort = (
    "title",
    "title_sort",
    "authors",
    "author_sort",
    "series",
    "rating",
    "pubdate",
    "tags",
    "publisher",
    "identifiers",
)


def field_sort(metadata: object, name: str):
    """
    Return a stable display ordering key for a metadata field.


    :param metadata:
    :param name:
    :return:
    """
    try:
        title = metadata.metadata_for_field(name)["name"]  # type: ignore[attr-defined]
    except Exception:
        title = "zzz"
    return {x: (i, None) for i, x in enumerate(default_sort)}.get(
        name,
        (10000, sort_key(title)),
    )


def displayable_field_keys(metadata: object):
    """
    Return Calibre metadata keys suitable for human-facing rendering.


    :param metadata:
    :return:
    """
    for key in metadata.all_field_keys():  # type: ignore[attr-defined]
        try:
            field_metadata = metadata.metadata_for_field(key)  # type: ignore[attr-defined]
        except Exception:
            continue
        if (
            field_metadata is not None
            and field_metadata["kind"] == "field"
            and field_metadata["datatype"] is not None
            and key not in ("au_map", "marked", "ondevice", "cover", "series_sort")
            and not key.endswith("_index")
        ):
            yield key


def get_field_list(metadata: object):
    """
    Build the displayable field list for a metadata object.


    :param metadata:
    :return:
    """
    for field in sorted(
        displayable_field_keys(metadata),
        key=partial(field_sort, metadata),
    ):
        yield field, True


def search_href(search_term: str, value: str) -> str:
    """
    Build a catalogue-search link for a rendered metadata value.


    :param search_term:
    :param value:
    :return:
    """
    search = '%s:"=%s"' % (search_term, value.replace('"', '\\"'))
    return prepare_string_for_xml(
        "search:" + hexlify(search.encode("utf-8")).decode("ascii"),
        True,
    )


def mi_to_html(
    metadata: object,
    field_list=None,
    default_author_link=None,
    use_roman_numbers=True,
    rating_font="Liberation Serif",
):
    """Render a Calibre-style metadata object as a detailed HTML field table."""

    if field_list is None:
        field_list = get_field_list(metadata)
    rows = []
    comment_fields = []
    isdevice = not hasattr(metadata, "id")
    row = '<td class="title">%s</td><td class="value">%s</td>'
    p = prepare_string_for_xml
    a = partial(prepare_string_for_xml, attribute=True)

    for field in (field for field, display in field_list if display):
        try:
            field_metadata = metadata.metadata_for_field(field)  # type: ignore[attr-defined]
        except Exception:
            continue
        if not field_metadata:
            continue
        if field == "sort":
            field = "title_sort"
        if field_metadata["is_custom"] and field_metadata["datatype"] in {"bool", "int", "float"}:
            isnull = metadata.get(field) is None  # type: ignore[attr-defined]
        else:
            isnull = metadata.is_null(field)  # type: ignore[attr-defined]
        if isnull:
            continue
        name = field_metadata["name"]
        if not name:
            name = field
        name += ":"
        if field_metadata["datatype"] == "comments" or field == "comments":
            val = getattr(metadata, field)
            if val:
                val = force_unicode(val)
                comment_fields.append(comments_to_html(val))
        elif field_metadata["datatype"] == "rating":
            val = getattr(metadata, field)
            if val:
                val = val / 2.0
                rows.append(
                    (
                        field,
                        '<td class="title">%s</td><td class="rating value" '
                        "style='font-family:\"%s\"'>%s</td>" % (name, rating_font, "\u2605" * int(val)),
                    )
                )
        elif field_metadata["datatype"] == "composite":
            val = getattr(metadata, field)
            if val:
                val = force_unicode(val)
                if field_metadata["display"].get("contains_html", False):
                    rows.append((field, row % (name, comments_to_html(val))))
                else:
                    if not field_metadata["is_multiple"]:
                        val = '<a href="%s" title="%s">%s</a>' % (
                            search_href(field, val),
                            _("Click to see books with {0}: {1}").format(field_metadata["name"], a(val)),
                            p(val),
                        )
                    else:
                        all_vals = [
                            v.strip()
                            for v in val.split(field_metadata["is_multiple"]["list_to_ui"])
                            if v.strip()
                        ]
                        links = [
                            '<a href="%s" title="%s">%s</a>'
                            % (
                                search_href(field, x),
                                _("Click to see books with {0}: {1}").format(field_metadata["name"], a(x)),
                                p(x),
                            )
                            for x in all_vals
                        ]
                        val = field_metadata["is_multiple"]["list_to_ui"].join(links)
                    rows.append((field, row % (name, val)))
        elif field == "path":
            path_value = getattr(metadata, "path", None)
            if path_value:
                path = force_unicode(path_value, filesystem_encoding)
                scheme = "devpath" if isdevice else "path"
                url = prepare_string_for_xml(path if isdevice else six_unicode(metadata.id), True)  # type: ignore[attr-defined]
                pathstr = _("Click to open")
                extra = ""
                if isdevice:
                    durl = url
                    if durl.startswith("mtp:::"):
                        durl = ":::".join((durl.split(":::"))[2:])
                    extra = '<br><span style="font-size:smaller">%s</span>' % (
                        prepare_string_for_xml(durl)
                    )
                link = '<a href="%s:%s" title="%s">%s</a>%s' % (
                    scheme,
                    url,
                    prepare_string_for_xml(path, True),
                    pathstr,
                    extra,
                )
                rows.append((field, row % (name, link)))
        elif field == "formats":
            if isdevice:
                continue
            path = ""
            path_value = getattr(metadata, "path", None)
            if path_value:
                head, tail = os.path.split(path_value)
                path = "/".join((os.path.basename(head), tail))
            data = (
                {
                    "fmt": fmt,
                    "path": a(path or ""),
                    "fname": a(metadata.format_files.get(fmt, "")),  # type: ignore[attr-defined]
                    "ext": fmt.lower(),
                    "id": metadata.id,  # type: ignore[attr-defined]
                }
                for fmt in metadata.formats  # type: ignore[attr-defined]
            )
            fmts = [
                '<a title="{path}/{fname}.{ext}" href="format:{id}:{fmt}">{fmt}</a>'.format(
                    **x
                )
                for x in data
            ]
            rows.append((field, row % (name, ", ".join(fmts))))
        elif field == "identifiers":
            urls = urls_from_identifiers(metadata.identifiers)  # type: ignore[attr-defined]
            links = [
                '<a href="%s" title="%s:%s">%s</a>' % (a(url), a(id_typ), a(id_val), p(namel))
                for namel, id_typ, id_val, url in urls
            ]
            links = ", ".join(links)
            if links:
                rows.append((field, row % (_("Ids") + ":", links)))
        elif field == "authors" and not isdevice:
            authors = []
            formatter = EvalFormatter()
            for aut in metadata.authors:  # type: ignore[attr-defined]
                link = ""
                if metadata.author_link_map[aut]:  # type: ignore[attr-defined]
                    link = link_title = metadata.author_link_map[aut]  # type: ignore[attr-defined]
                elif default_author_link:
                    if default_author_link == "search-calibre":
                        link = search_href("authors", aut)
                        link_title = a(_("Search the calibre library for books by %s") % aut)
                    else:
                        vals = {"author": aut.replace(" ", "+")}
                        try:
                            vals["author_sort"] = metadata.author_sort_map[aut].replace(" ", "+")  # type: ignore[attr-defined]
                        except Exception:
                            vals["author_sort"] = aut.replace(" ", "+")
                        link = link_title = a(formatter.safe_format(default_author_link, vals, "", vals))
                aut = p(aut)
                if link:
                    authors.append(
                        '<a calibre-data="authors" title="%s" href="%s">%s</a>'
                        % (link_title, link, aut)
                    )
                else:
                    authors.append(aut)
            rows.append((field, row % (name, " & ".join(authors))))
        elif field == "languages":
            if not metadata.languages:  # type: ignore[attr-defined]
                continue
            names = filter(None, map(calibre_langcode_to_name, metadata.languages))  # type: ignore[attr-defined]
            rows.append((field, row % (name, ", ".join(names))))
        else:
            val = metadata.format_field(field)[-1]  # type: ignore[attr-defined]
            if val is None:
                continue
            val = p(val)

            if field_metadata["datatype"] == "series":
                sidx = metadata.get(field + "_index")  # type: ignore[attr-defined]
                if sidx is None:
                    sidx = 1.0
                try:
                    st = field_metadata["search_terms"][0]
                except Exception:
                    st = field
                series = getattr(metadata, field)
                val = _(
                    '%(sidx)s of <a href="%(href)s" title="%(tt)s">'
                    '<span class="%(cls)s">%(series)s</span></a>'
                ) % dict(
                    sidx=fmt_sidx(sidx, use_roman=use_roman_numbers),
                    cls="series_name",
                    series=p(series),
                    href=search_href(st, series),
                    tt=p(_("Click to see books in this series")),
                )

            elif field_metadata["datatype"] == "datetime":
                aval = getattr(metadata, field)
                if is_date_undefined(aval):
                    continue

            elif field_metadata["datatype"] == "text" and field_metadata["is_multiple"]:
                try:
                    st = field_metadata["search_terms"][0]
                except Exception:
                    st = field
                all_vals = metadata.get(field)  # type: ignore[attr-defined]
                if field == "tags":
                    all_vals = sorted(all_vals, key=sort_key)
                links = [
                    '<a href="%s" title="%s">%s</a>'
                    % (
                        search_href(st, x),
                        _("Click to see books with {0}: {1}").format(field_metadata["name"], a(x)),
                        p(x),
                    )
                    for x in all_vals
                ]
                val = field_metadata["is_multiple"]["list_to_ui"].join(links)

            elif field_metadata["datatype"] == "enumeration":
                try:
                    st = field_metadata["search_terms"][0]
                except Exception:
                    st = field
                val = '<a href="%s" title="%s">%s</a>' % (
                    search_href(st, val),
                    _("Click to see books with {0}: {1}").format(field_metadata["name"], val),
                    val,
                )

            rows.append((field, row % (name, val)))

    device_collections = getattr(metadata, "device_collections", [])
    if device_collections:
        device_collections = ", ".join(sorted(device_collections, key=sort_key))
        rows.append(("device_collections", row % (_("Collections") + ":", device_collections)))

    def classname(field):
        try:
            datatype = metadata.metadata_for_field(field)["datatype"]  # type: ignore[attr-defined]
        except Exception:
            datatype = "text"
        return "datatype_%s" % datatype

    rendered_rows = [
        '<tr id="%s" class="%s">%s</tr>' % (field.replace("#", "_"), classname(field), html)
        for field, html in rows
    ]
    return '<table class="fields">%s</table>' % ("\n".join(rendered_rows)), comment_fields


def calibre_metadata_to_html(metadata: object) -> str:
    """Render a Calibre-compatible metadata object as a simple HTML table."""

    from LiuXin_alpha.metadata.ebook_metadata_tools import authors_to_string
    from LiuXin_alpha.utils.date import isoformat

    rows = [(_("Title"), six_unicode(metadata.title))]  # type: ignore[attr-defined]
    rows += [
        (
            _("Author(s)"),
            (authors_to_string(metadata.authors) if metadata.authors else _("Unknown")),  # type: ignore[attr-defined]
        )
    ]
    rows += [(_("Publisher"), six_unicode(metadata.publisher))]  # type: ignore[attr-defined]
    rows += [(_("Producer"), six_unicode(metadata.book_producer))]  # type: ignore[attr-defined]
    rows += [(_("Comments"), six_unicode(metadata.comments))]  # type: ignore[attr-defined]
    rows += [("ISBN", six_unicode(metadata.isbn))]  # type: ignore[attr-defined]
    rows += [(_("Tags"), ", ".join([six_unicode(tag) for tag in metadata.tags]))]  # type: ignore[attr-defined]
    if metadata.series:  # type: ignore[attr-defined]
        rows += [
            (
                _("Series"),
                six_unicode(metadata.series) + " #%s" % metadata.format_series_index(),  # type: ignore[attr-defined]
            )
        ]
    rows += [(_("Languages"), ", ".join(metadata.languages))]  # type: ignore[attr-defined]
    if metadata.timestamp is not None:  # type: ignore[attr-defined]
        rows += [
            (
                _("Timestamp"),
                six_unicode(isoformat(metadata.timestamp, as_utc=False, sep=" ")),  # type: ignore[attr-defined]
            )
        ]
    if metadata.pubdate is not None:  # type: ignore[attr-defined]
        rows += [
            (
                _("Published"),
                six_unicode(isoformat(metadata.pubdate, as_utc=False, sep=" ")),  # type: ignore[attr-defined]
            )
        ]
    if metadata.rights is not None:  # type: ignore[attr-defined]
        rows += [(_("Rights"), six_unicode(metadata.rights))]  # type: ignore[attr-defined]
    for key in metadata.custom_field_keys():  # type: ignore[attr-defined]
        val = metadata.get(key, None)  # type: ignore[attr-defined]
        if val:
            name, val = metadata.format_field(key)  # type: ignore[attr-defined]
            rows += [(name, val)]
    return "<table>%s</table>" % "\n".join(
        "<tr><td><b>%s</b></td><td>%s</td></tr>" % row
        for row in rows
    )


__all__ = [
    "calibre_metadata_to_html",
    "default_sort",
    "displayable_field_keys",
    "field_sort",
    "get_field_list",
    "mi_to_html",
    "search_href",
]
