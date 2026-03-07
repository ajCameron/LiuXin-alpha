from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _first_mapping_value(raw, default=None):
    if isinstance(raw, Mapping):
        try:
            return next(iter(raw.values()))
        except StopIteration:
            return default
    return raw if raw is not None else default


def _opf2_unicode_bytes() -> bytes:
    return b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         xmlns:opf="http://www.idpf.org/2007/opf"
         unique-identifier="BookId"
         version="2.0">
  <metadata>
    <dc:title>\xe4\xb8\xbb\xe9\xa1\x8c \xf0\x9f\x99\x82 \xe2\x80\x94 \xce\x9a\xce\xb1\xce\xbb\xce\xb7\xce\xbc\xce\xad\xcf\x81\xce\xb1 \xe2\x80\x94 \xd9\x85\xd8\xb1\xd8\xad\xd8\xa8\xd8\xa7</dc:title>
    <dc:creator opf:role="aut">Ren\xc3\xa9e Fa\xc3\x9fbinder</dc:creator>
    <dc:language>ja</dc:language>
    <dc:description>Combining: cafe\xcc\x81 co\xcc\x88perate A\xcc\x8a.</dc:description>
    <dc:subject>\xe3\x82\xbf\xe3\x82\xb0;\xce\x9a\xce\xb1\xcf\x84\xce\xb7\xce\xb3\xce\xbf\xcf\x81\xce\xaf\xce\xb1</dc:subject>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
    <meta name="calibre:series" content="\xe3\x82\xb7\xe3\x83\xaa\xe3\x83\xbc\xe3\x82\xba\xce\xa9"/>
    <meta name="calibre:series_index" content="7.5"/>
    <meta name="calibre:title_sort" content="Title Sort \xce\xa9"/>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="chap1"/>
  </spine>
</package>
"""


def test_opf_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.opf as opf_md

    assert opf_md is not None


def test_opf_reader_plugin_is_available_and_preserves_stream_position() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    plugins = get_metadata_reader_plugins()
    opf_cls = next((p for p in plugins if p.__name__ == "OPFMetadataReader"), None)
    assert opf_cls is not None

    stream = io.BytesIO(_opf2_unicode_bytes())
    stream.seek(9)
    reader = opf_cls(None)
    md = reader.get_metadata(stream=stream, ftype="opf")

    assert "主題" in md.title
    assert stream.tell() == 9


def test_opf_get_metadata_default_returns_liuxin_metadata() -> None:
    from LiuXin_alpha.metadata.file_sources.opf import get_metadata

    md = get_metadata(io.BytesIO(_opf2_unicode_bytes()))
    assert "主題" in md.title
    assert _values(md.authors) == ["Renée Faßbinder"]
    assert getattr(md, "language", None) == "ja"
    assert "Combining: cafe" in (_values(getattr(md, "comments", None))[0])
    assert set(_values(getattr(md, "tags", None))) >= {"タグ", "Κατηγορία"}
    assert _values(getattr(md, "series", None)) == ["シリーズΩ"]
    assert float(_first_mapping_value(getattr(md, "series_index", None), 0.0)) == 7.5


def test_opf_get_metadata_calibre_mode() -> None:
    from LiuXin_alpha.metadata.file_sources.opf import get_metadata

    md = get_metadata(io.BytesIO(_opf2_unicode_bytes()), calibre=True)
    assert "主題" in md.title
    assert _values(md.authors) == ["Renée Faßbinder"]
    assert getattr(md, "series", None) == "シリーズΩ"
    assert float(getattr(md, "series_index", 0.0)) == 7.5


def test_opf_get_metadata_text_mode_from_string() -> None:
    from LiuXin_alpha.metadata.file_sources.opf import get_metadata

    text_payload = _opf2_unicode_bytes().decode("utf-8")
    md = get_metadata(text_payload, text=True)

    assert "主題" in md.title
    assert _values(md.authors) == ["Renée Faßbinder"]


def test_opf_get_metadata_inplace_pathlike(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.opf import get_metadata_inplace

    path = tmp_path / "unicode_fixture.opf"
    path.write_bytes(_opf2_unicode_bytes())
    md = get_metadata_inplace(path)

    assert "主題" in md.title
    assert _values(md.authors) == ["Renée Faßbinder"]


def test_opf_file_is_raw_root_can_parse_non_package_metadata_root() -> None:
    from LiuXin_alpha.metadata.file_sources.opf import get_metadata

    raw = b"""<?xml version='1.0' encoding='utf-8'?>
    <office:meta xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
                 xmlns:dc="http://purl.org/dc/elements/1.1/"
                 xmlns:meta="urn:oasis:names:tc:opendocument:xmlns:meta:1.0">
      <dc:title>Fallback Meta Title</dc:title>
      <dc:creator>Alice and Bob</dc:creator>
      <dc:language>en</dc:language>
      <dc:subject>tag-one;tag-two</dc:subject>
      <meta:keyword>tag-three</meta:keyword>
      <meta:user-defined meta:name="opf.series">Series Fallback</meta:user-defined>
      <meta:user-defined meta:name="opf.series_index">2,5</meta:user-defined>
    </office:meta>
    """
    root = etree.fromstring(raw)
    md = get_metadata(root, file_is_raw_root=True, seek_md_node=False, walk=True)

    assert md.title == "Fallback Meta Title"
    assert _values(md.authors) == ["Alice", "Bob"]
    assert set(_values(md.tags)) >= {"tag-one", "tag-two", "tag-three"}
    assert _values(md.series) == ["Series Fallback"]
    assert float(_first_mapping_value(md.series_index, 0.0)) == 2.5


def test_opf_invalid_payload_returns_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.opf import get_metadata

    md = get_metadata(io.BytesIO(b"<not-xml"))
    assert md.title == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_opf_invalid_text_mode_payload_returns_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.opf import get_metadata

    md = get_metadata("<not-xml", text=True)
    assert md.title == "Unknown"
    assert _values(md.authors) == ["Unknown"]
