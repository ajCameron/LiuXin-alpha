"""
Check how the PDF reader combines a file's Info dictionary and XMP packet under each policy.

The files are built here, with the two sources disagreeing and dated so that one or
the other is newer.

Example:
    Run these checks::

        python -m pytest -q tests/metadata/file_sources/test_pdf_merge_policies.py
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.configuration import use_preferences
from LiuXin_alpha.file_formats.pdf.objects import minimal_pdf
from LiuXin_alpha.metadata.file_sources import pdf, pdf_merge

CORPUS = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata"
OLDER, NEWER = "D:20200101000000Z", "D:20220101000000Z"


def _pdf_string(text: str) -> bytes:
    """
    Write text as a PDF string, in UTF-16 with a byte-order mark.

    Example:
        >>> _pdf_string("A")
        b'<FEFF0041>'


    :param text: The text.
    :return: The string's source.
    """
    return b"<FEFF" + text.encode("utf-16-be").hex().upper().encode("ascii") + b">"


def _info(**entries: str) -> bytes:
    """
    Write an Info dictionary.

    Example:
        >>> _info(Title="T")
        b'<< /Title <FEFF0054> >>'


    :param entries: Its entries, by name.
    :return: The dictionary's source.
    """
    body = b" ".join(
        b"/" + name.encode("ascii") + b" " + _pdf_string(value)
        for name, value in entries.items()
    )
    return b"<< " + body + b" >>"


def _packet(*, attributes: dict[str, str] | None = None, **elements: str) -> str:
    """
    Write an XMP packet from property elements, given as ``prefix_Name="<xml>"``.

    Example:
        >>> "<dc:title>T</dc:title>" in _packet(dc_title="T")
        True


    :param attributes: Properties written as attributes of ``rdf:Description``.
    :param elements: Properties written as elements; the value is the element's content.
    :return: The packet.
    """
    attrs = "".join(f' {name}="{value}"' for name, value in (attributes or {}).items())
    body = "".join(
        f"<{name.replace('_', ':', 1)}>{value}</{name.replace('_', ':', 1)}>"
        for name, value in elements.items()
    )
    return (
        '<?xpacket begin="" id="W5M0MpCehiHzreSzNTczkc9d"?>'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        '<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/"'
        ' xmlns:xmp="http://ns.adobe.com/xap/1.0/" xmlns:pdf="http://ns.adobe.com/pdf/1.3/"'
        ' xmlns:xmpMM="http://ns.adobe.com/xap/1.0/mm/"'
        f' xmlns:prism="http://prismstandard.org/namespaces/basic/2.0/"{attrs}>'
        f"{body}</rdf:Description></rdf:RDF></x:xmpmeta>"
        '<?xpacket end="w"?>'
    )


def _alt(text: str) -> str:
    """
    Write a language alternative holding one default text.

    Example:
        >>> _alt("T")
        '<rdf:Alt><rdf:li xml:lang="x-default">T</rdf:li></rdf:Alt>'


    :param text: The text.
    :return: The property's content.
    """
    return f'<rdf:Alt><rdf:li xml:lang="x-default">{text}</rdf:li></rdf:Alt>'


def _items(kind: str, values: Iterable[str]) -> str:
    """
    Write an ordered or unordered list.

    Example:
        >>> _items("Bag", ["a"])
        '<rdf:Bag><rdf:li>a</rdf:li></rdf:Bag>'


    :param kind: ``Seq`` or ``Bag``.
    :param values: The items.
    :return: The property's content.
    """
    return (
        f"<rdf:{kind}>"
        + "".join(f"<rdf:li>{value}</rdf:li>" for value in values)
        + f"</rdf:{kind}>"
    )


def _file(info: bytes, packet: str | None = None) -> bytes:
    """
    Build a PDF with an Info dictionary and, if given, an XMP packet.

    Example:
        >>> _file(_info(Title="T")).startswith(b"%PDF")
        True


    :param info: The Info dictionary's source.
    :param packet: The XMP packet.
    :return: The file's bytes.
    """
    if packet is None:
        return minimal_pdf(info)
    data = packet.encode("utf-8")
    stream = (
        b"<< /Type /Metadata /Subtype /XML /Length %d >>\nstream\n" % len(data)
        + data
        + b"\nendstream"
    )
    return minimal_pdf(
        info, extra={3: stream}, catalog=b"<< /Type /Catalog /Metadata 3 0 R >>"
    )


def _values(raw: Any) -> list[str]:
    """
    List a metadata field's values, whatever container holds them.

    Example:
        >>> _values({"a": None}), _values("b"), _values(None)
        (['a'], ['b'], [])


    :param raw: The field.
    :return: Its values as text.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return [str(value) for value in raw]


def _read(data: bytes, merge: str | None = None) -> dict[str, Any]:
    """
    Read a file and keep the fields the policies decide.

    Example:
        >>> _read(_file(_info(Title="T")))["title"]
        'T'


    :param data: The file.
    :param merge: The policy, or ``None`` for the preference.
    :return: The fields.
    """
    md = pdf.get_metadata(data, merge=merge)
    identifiers = {
        scheme: sorted(values)
        for scheme, values in md.get_identifiers().items()
        if values
    }
    return {
        "title": md.title,
        "authors": _values(md.authors),
        "tags": sorted(value.casefold() for value in _values(md.tags)),
        "comments": _values(md.comments),
        "publisher": _values(md.publisher),
        "uuid": _values(md.uuid),
        "identifiers": identifiers,
        "languages": _values(getattr(md, "languages", None)),
        "created": md.timestamp,
    }


def _disagreeing(info_date: str | None, packet_date: str | None) -> bytes:
    """
    Build a file whose two sources disagree on every field, dated as given.

    Example:
        >>> b"/ModDate" in _disagreeing(NEWER, None)
        True


    :param info_date: The Info dictionary's ``ModDate``, or ``None`` for none.
    :param packet_date: The packet's ``xmp:MetadataDate``, or ``None`` for none.
    :return: The file's bytes.
    """
    entries = {
        "Title": "Info Title",
        "Author": "Ann Info",
        "Subject": "Info subject",
        "Keywords": "info-a, info-b",
    }
    if info_date:
        entries["ModDate"] = info_date
    stamp = {"xmp:MetadataDate": packet_date} if packet_date else {}
    packet = _packet(
        attributes=stamp,
        dc_title=_alt("XMP Title"),
        dc_creator=_items("Seq", ["Bea Xmp"]),
        dc_subject=_items("Bag", ["xmp-a"]),
        dc_description=_alt("XMP description"),
        dc_publisher=_items("Bag", ["XMP Press"]),
    )
    return _file(_info(**entries), packet)


def test_newest_takes_the_packet_when_it_changed_last() -> None:
    """
    The packet is newer, so it wins every field it has.

    Example:
        >>> test_newest_takes_the_packet_when_it_changed_last()


    :return: None after the fields are checked.
    """
    got = _read(_disagreeing(OLDER, "2021-01-01T00:00:00Z"), "newest")
    assert got["title"] == "XMP Title"
    assert got["authors"] == ["Bea Xmp"]
    assert got["tags"] == ["xmp-a"]
    assert got["comments"] == ["XMP description"]
    assert got["publisher"] == ["XMP Press"]


def test_newest_takes_the_info_dictionary_when_it_changed_last() -> None:
    """
    The Info dictionary is newer, so it wins; what it lacks comes from the packet.

    Example:
        >>> test_newest_takes_the_info_dictionary_when_it_changed_last()


    :return: None after the fields are checked.
    """
    got = _read(_disagreeing(NEWER, "2021-01-01T00:00:00Z"), "newest")
    assert got["title"] == "Info Title"
    assert got["authors"] == ["Ann Info"]
    assert got["tags"] == ["info-a", "info-b"]
    # The Subject is the Info dictionary's description.
    assert got["comments"] == ["Info subject"]
    assert got["publisher"] == ["XMP Press"]


@pytest.mark.parametrize(
    ("info_date", "packet_date"), [(NEWER, None), (None, "2021-01-01"), (None, None)]
)
def test_newest_takes_the_packet_when_the_dates_cannot_decide(
    info_date: str | None, packet_date: str | None
) -> None:
    """
    Without both dates the packet wins, as PDF 2.0 makes it the main record.

    Example:
        >>> test_newest_takes_the_packet_when_the_dates_cannot_decide(None, None)


    :param info_date: The Info dictionary's date.
    :param packet_date: The packet's date.
    :return: None after the title is checked.
    """
    assert _read(_disagreeing(info_date, packet_date), "newest")["title"] == "XMP Title"


def test_the_packet_s_modify_date_counts_when_it_is_later() -> None:
    """
    The packet changed when its ``xmp:MetadataDate`` or ``xmp:ModifyDate`` says, whichever is later.

    Example:
        >>> test_the_packet_s_modify_date_counts_when_it_is_later()


    :return: None after the title is checked.
    """
    packet = _packet(
        attributes={"xmp:MetadataDate": "2020-01-01", "xmp:ModifyDate": "2022-01-01"},
        dc_title=_alt("XMP Title"),
    )
    data = _file(_info(Title="Info Title", ModDate="D:2021"), packet)
    assert _read(data, "newest")["title"] == "XMP Title"


def test_calibre_keeps_the_packet_s_other_fields_when_info_is_newer() -> None:
    """
    calibre's rule: a newer Info dictionary wins only the title, authors and tags.

    Example:
        >>> test_calibre_keeps_the_packet_s_other_fields_when_info_is_newer()


    :return: None after the fields are checked.
    """
    got = _read(_disagreeing(NEWER, "2021-01-01T00:00:00Z"), "calibre")
    assert got["title"] == "Info Title"
    assert got["authors"] == ["Ann Info"]
    assert got["tags"] == ["info subject", "info-a", "info-b"]
    assert got["comments"] == ["XMP description"]


def test_calibre_falls_back_to_the_info_tags_when_the_packet_has_none() -> None:
    """
    calibre's rule: the packet's tags win, unless it has none.

    Example:
        >>> test_calibre_falls_back_to_the_info_tags_when_the_packet_has_none()


    :return: None after the tags are checked.
    """
    data = _file(
        _info(Subject="Subject", Keywords="a, b"), _packet(dc_title=_alt("XMP Title"))
    )
    got = _read(data, "calibre")
    assert got["title"] == "XMP Title"
    assert got["tags"] == ["a", "b", "subject"]


def test_union_keeps_the_tags_of_both() -> None:
    """
    LiuXin's earlier rule: the packet's single fields win, and the tags of both are kept.

    Example:
        >>> test_union_keeps_the_tags_of_both()


    :return: None after the fields are checked.
    """
    got = _read(_disagreeing(NEWER, "2021-01-01T00:00:00Z"), "union")
    assert got["title"] == "XMP Title"
    assert got["authors"] == ["Bea Xmp"]
    assert got["tags"] == ["info subject", "info-a", "info-b", "xmp-a"]
    assert got["comments"] == ["XMP description"]


def test_an_escaped_packet_is_ignored_by_calibre_and_field_by_field_by_newest() -> None:
    """
    Nitro PDF writes escaped PDF strings into XMP; calibre drops the packet, newest drops the field.

    Example:
        >>> test_an_escaped_packet_is_ignored_by_calibre_and_field_by_field_by_newest()


    :return: None after the fields are checked.
    """
    packet = _packet(
        dc_title=_alt(r"\376\377\000T"),
        dc_description=_alt("Readable description"),
        attributes={"xmpMM:DocumentID": "uuid:0f0e0d0c-0b0a-0908-0706-050403020100"},
    )
    data = _file(_info(Title="Info Title", Subject="Info subject"), packet)
    newest, calibre = _read(data, "newest"), _read(data, "calibre")
    assert newest["title"] == calibre["title"] == "Info Title"
    assert newest["comments"] == ["Readable description"]
    assert newest["uuid"] == ["0f0e0d0c-0b0a-0908-0706-050403020100"]
    assert calibre["comments"] == []
    assert calibre["tags"] == ["info subject"]
    assert calibre["uuid"] == []


def test_attribute_properties_are_read_by_the_newer_rules_only() -> None:
    """
    Properties written as attributes are read, except by the earlier rule, which never did.

    Example:
        >>> test_attribute_properties_are_read_by_the_newer_rules_only()


    :return: None after the identifiers are checked.
    """
    packet = _packet(
        attributes={
            "xmpMM:DocumentID": "uuid:00112233-4455-6677-8899-aabbccddeeff",
            "prism:doi": "10.1234/attr",
        },
        dc_title=_alt("XMP Title"),
    )
    data = _file(_info(Title="Info Title"), packet)
    for policy in ("newest", "calibre"):
        got = _read(data, policy)
        assert got["uuid"] == ["00112233-4455-6677-8899-aabbccddeeff"]
        assert got["identifiers"]["doi"] == ["10.1234/attr"]
    union = _read(data, "union")
    assert union["uuid"] == []
    assert "doi" not in union["identifiers"]


def test_dates_languages_and_identifiers() -> None:
    """
    The newer rules give dates in ISO 8601, and read languages and identifiers from the packet.

    Example:
        >>> test_dates_languages_and_identifiers()


    :return: None after the fields are checked.
    """
    packet = _packet(
        dc_language=_items("Bag", ["en", "fr"]),
        dc_identifier="urn:isbn:9780306406157",
        prism_doi="10.5555/element",
    )
    data = _file(_info(Title="T", CreationDate="D:20190601120000+02'00'"), packet)
    got = _read(data, "newest")
    assert got["created"] == "2019-06-01T12:00:00+02:00"
    assert got["languages"] == ["en", "fr"]
    assert got["identifiers"]["doi"] == ["10.5555/element"]
    assert got["identifiers"]["isbn"] == ["9780306406157"]
    assert _read(data, "union")["created"] == "D:20190601120000+02'00'"


def test_the_preference_chooses_and_an_unknown_value_falls_back() -> None:
    """
    Without ``merge=``, the ``pdf_metadata_merge`` preference chooses; nonsense means ``newest``.

    Example:
        >>> test_the_preference_chooses_and_an_unknown_value_falls_back()


    :return: None after each preference is checked.
    """
    data = _disagreeing(NEWER, "2021-01-01T00:00:00Z")
    for setting, policy in (
        ("union", "union"),
        ("calibre", "calibre"),
        ("newest", "newest"),
        ("sideways", "newest"),
    ):
        with use_preferences({pdf_merge.MERGE_PREFERENCE: setting}):
            assert _read(data) == _read(data, policy), setting
    assert pdf_merge.merge_policy() in pdf_merge.MERGE_POLICIES


def test_the_default_preference_is_newest() -> None:
    """
    The shipped default is the improved rule.

    Example:
        >>> test_the_default_preference_is_newest()


    :return: None after the default is checked.
    """
    from LiuXin_alpha.configuration_defaults import GLOBAL_DEFAULTS

    assert (
        GLOBAL_DEFAULTS[pdf_merge.MERGE_PREFERENCE]
        == pdf_merge.DEFAULT_MERGE_POLICY
        == "newest"
    )


def test_each_fixed_rule_has_a_reader_plugin_that_is_off_by_default() -> None:
    """
    A reader plugin per rule; each is off until enabled, and then runs before the main one.

    Example:
        >>> test_each_fixed_rule_has_a_reader_plugin_that_is_off_by_default()


    :return: None after each plugin is checked.
    """
    from LiuXin_alpha.metadata.file_sources.builtin_readers import (
        get_metadata_reader_plugins,
    )
    from LiuXin_alpha.plugins.configuration import default_disabled_plugins

    readers = [
        plugin for plugin in get_metadata_reader_plugins() if "pdf" in plugin.file_types
    ]
    main, *fixed = readers
    assert main.name == "Read PDF metadata" and main.merge is None
    assert main.name not in default_disabled_plugins
    assert sorted(plugin.merge for plugin in fixed) == sorted(pdf_merge.MERGE_POLICIES)
    data = _disagreeing(NEWER, "2021-01-01T00:00:00Z")
    for plugin in fixed:
        assert plugin.name in default_disabled_plugins
        assert plugin.priority > main.priority
        md = plugin(None).get_metadata(data, "pdf")
        assert md.title == _read(data, plugin.merge)["title"]


@pytest.mark.parametrize("policy", sorted(pdf_merge.MERGE_POLICIES))
def test_every_corpus_file_reads_under_every_policy(policy: str) -> None:
    """
    No policy fails on a file the reader can open.

    Example:
        >>> test_every_corpus_file_reads_under_every_policy("newest")


    :param policy: The policy.
    :return: None after every file is read.
    """
    cases = json.loads((CORPUS / "cases.json").read_text(encoding="utf-8"))
    readable = [name for name, case in cases.items() if "error" not in case["pdfinfo"]]
    assert len(readable) == len(cases) - 1
    for name in readable:
        md = pdf.get_metadata(CORPUS / f"{name}.pdf", merge=policy)
        assert md.title, name


def test_combine_rejects_policies_it_does_not_apply() -> None:
    """
    ``union`` is the reader's own path, so :func:`pdf_merge.combine` refuses it.

    Example:
        >>> test_combine_rejects_policies_it_does_not_apply()


    :return: None after the refusal is checked.
    """
    with pytest.raises(ValueError, match="union"):
        pdf_merge.combine(pdf_merge.PdfFields(), None, "union")
