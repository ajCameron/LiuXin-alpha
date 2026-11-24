"""
Check that subject headings stay apart from tags, from the readers' record to the writers.

LiuXin keeps tags and subjects in separate tables; calibre has only tags. See
``dev-docs/metadata/subjects.md``.

Example:
    Run these checks::

        python -m pytest -q tests/metadata/test_subjects_apart_from_tags.py
"""

from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from LiuXin_alpha.file_formats import plugin_runtime
from LiuXin_alpha.file_formats.providers.ported_calibre import ported_reader
from LiuXin_alpha.file_formats.providers.wemi_mapping import wemi_from_calibre
from LiuXin_alpha.metadata.api import WorkRelationLink
from LiuXin_alpha.metadata.book.base import calibreMetadata, tags_and_subjects
from LiuXin_alpha.metadata.book.json_codec import JsonCodec
from LiuXin_alpha.metadata.book.serialize import metadata_as_dict
from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from tests.support.file_format_epub import build_unicode_epub


def _record(tags: list[str], subjects: list[str]) -> Any:
    """
    Make a readers' record with the tags and subjects given.

    Example:
        >>> _record(["a"], ["S"]).subjects
        ['S']


    :param tags: The tags.
    :param subjects: The subjects.
    :return: The record.
    """
    mi = calibreMetadata("A Book", ["Ann Writer"])
    mi.tags = list(tags)
    mi.subjects = list(subjects)
    return mi


def test_the_record_holds_subjects_apart() -> None:
    """
    The readers' record has an empty ``subjects`` list, merges it like the tags, and serialises it.

    Example:
        >>> test_the_record_holds_subjects_apart()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    empty = calibreMetadata("A Book")
    assert empty.subjects == [] and empty.is_null("subjects")

    source = _record(["to-read"], ["History -- 20th century"])
    merged = _record([], ["history -- 20th century", "Sea stories"])
    merged.smart_update(source)
    assert merged.subjects == ["History -- 20th century", "Sea stories"]
    assert merged.tags == ["to-read"]
    replaced = _record([], ["Old"])
    replaced.smart_update(source, replace_metadata=True)
    assert replaced.subjects == ["History -- 20th century"]
    kept = _record([], ["Kept"])
    kept.smart_update(calibreMetadata("Other"))
    assert kept.subjects == ["Kept"]

    copied = source.deepcopy_metadata()
    copied.subjects.append("Changed")
    assert source.subjects == ["History -- 20th century"]

    assert metadata_as_dict(source)["subjects"] == ["History -- 20th century"]
    codec = JsonCodec()
    decoded = codec.raw_to_book(codec.encode_book_metadata(source), calibreMetadata, "")
    assert decoded.subjects == ["History -- 20th century"] and decoded.tags == [
        "to-read"
    ]


def test_the_calibre_compatibility_constants_stay_calibre_s() -> None:
    """
    ``subjects`` is LiuXin's field; the calibre compatibility field sets do not name it.

    Example:
        >>> test_the_calibre_compatibility_constants_stay_calibre_s()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    from LiuXin_alpha.metadata.book import LIUXIN_METADATA_FIELDS, base
    from LiuXin_alpha.utils.calibre_compat.metadata import (
        calibre_metadata_constants as calibre,
    )

    assert LIUXIN_METADATA_FIELDS == frozenset({"subjects"})
    assert "subjects" not in calibre.STANDARD_METADATA_FIELDS
    assert "subjects" not in calibre.SERIALIZABLE_FIELDS
    assert base.STANDARD_METADATA_FIELDS == calibre.STANDARD_METADATA_FIELDS | {
        "subjects"
    }
    assert "subjects" not in base.SC_COPYABLE_FIELDS


def test_tags_and_subjects_lists_each_term_once() -> None:
    """
    For a format with one field: the tags, then the subjects not among them, case ignored.

    Example:
        >>> test_tags_and_subjects_lists_each_term_once()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    assert tags_and_subjects(_record(["to-read", "History"], ["history", "Sea"])) == [
        "to-read",
        "History",
        "Sea",
    ]
    assert tags_and_subjects(SimpleNamespace(tags=["a"])) == ["a"]
    assert tags_and_subjects(_record([], [])) == []


def test_the_liuxin_record_converts_with_subjects_apart() -> None:
    """
    ``to_calibre`` keeps a LiuXin record's subjects out of its tags, and ``from_calibre`` reads them back.

    Example:
        >>> test_the_liuxin_record_converts_with_subjects_apart()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    liuxin = CalibreLikeLiuXinBookMetaData("A Book", ["Ann Writer"])
    liuxin.tags = ["to-read"]
    liuxin.subject = ["History -- 20th century"]
    calibre = liuxin.to_calibre()
    assert calibre.tags == ["to-read"]
    assert calibre.subjects == ["History -- 20th century"]

    back = CalibreLikeLiuXinBookMetaData.from_calibre(calibre)
    assert list(back.subject) == ["History -- 20th century"]
    assert list(back.tags) == ["to-read"]


def test_a_wemi_record_projects_its_work_subjects_apart() -> None:
    """
    A library record's Work subjects reach calibre's form as subjects, not tags.

    Example:
        >>> test_a_wemi_record_projects_its_work_subjects_apart()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    metadata = LiuXinWEMIMetadata("A Book")
    metadata.add_wemi_relation_link(
        "work", "tags", WorkRelationLink(target={"tag": "to-read"})
    )
    metadata.add_wemi_relation_link(
        "work", "subjects", WorkRelationLink(target={"subject": "Sea stories"})
    )
    calibre = metadata.to_calibre()
    assert "Sea stories" in calibre.subjects
    assert "Sea stories" not in calibre.tags and "to-read" in calibre.tags


def test_a_writer_with_one_field_gets_the_subjects_among_the_tags() -> None:
    """
    The runtime folds the subjects into the tags for a plain writer, and leaves them for one that writes them.

    Example:
        >>> test_a_writer_with_one_field_gets_the_subjects_among_the_tags()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    record = _record(["to-read"], ["Sea stories"])
    given = plugin_runtime._record_for_writer(SimpleNamespace(), record)
    assert given.tags == ["to-read", "Sea stories"] and given.subjects == []
    assert record.tags == ["to-read"] and record.subjects == ["Sea stories"]

    aware = SimpleNamespace(writes_subjects=True)
    assert plugin_runtime._record_for_writer(aware, record) is record

    plain = _record(["to-read"], [])
    assert plugin_runtime._record_for_writer(SimpleNamespace(), plain) is plain

    liuxin = CalibreLikeLiuXinBookMetaData("A Book", ["Ann Writer"])
    liuxin.tags = ["to-read"]
    assert plugin_runtime._record_for_writer(SimpleNamespace(), liuxin) is liuxin
    liuxin.subject = ["Sea stories"]
    folded = plugin_runtime._record_for_writer(SimpleNamespace(), liuxin)
    assert folded.tags == ["to-read", "Sea stories"] and folded.subjects == []


def test_an_epub_written_from_a_record_with_subjects_holds_them_as_dc_subject(
    tmp_path: Path,
) -> None:
    """
    EPUB has one field for both: a write keeps the subjects, which read back as tags.

    Example:
        >>> test_an_epub_written_from_a_record_with_subjects_holds_them_as_dc_subject(Path("."))  # doctest: +SKIP


    :param tmp_path: Holds the book.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "book.epub"
    build_unicode_epub(path)
    stream = io.BytesIO(path.read_bytes())
    errors: list[str] = []
    plugin_runtime.set_file_type_metadata(
        stream,
        _record(["to-read"], ["Sea stories"]),
        "epub",
        report_error=lambda _mi, _ftype, trace: errors.append(str(trace)),
    )
    assert not errors
    read = ported_reader("epub")(io.BytesIO(stream.getvalue()))
    assert {"to-read", "Sea stories"} <= set(read.tags)


def test_the_opf_builders_write_subjects_as_dc_subject() -> None:
    """
    OPF has one field for both: each builder for calibre's record writes the subjects there too.

    Example:
        >>> test_the_opf_builders_write_subjects_as_dc_subject()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    from LiuXin_alpha.file_formats.oeb.base import OEBBook
    from LiuXin_alpha.file_formats.oeb.transforms.metadata import (
        meta_info_to_oeb_metadata,
    )
    from LiuXin_alpha.file_formats.opf import opf2
    from LiuXin_alpha.utils.logging import default_log

    record = _record(["to-read"], ["Sea stories"])
    opf = opf2.calibre_metadata_to_opf(record)
    opf = opf.decode("utf-8") if isinstance(opf, bytes) else opf
    assert "to-read</dc:subject>" in opf and "Sea stories</dc:subject>" in opf

    book = OEBBook(default_log, html_preprocessor=None)
    meta_info_to_oeb_metadata(_record([], ["Sea stories"]), book.metadata, default_log)
    assert [str(x) for x in book.metadata.subject] == ["Sea stories"]


def test_the_pool_maps_tags_to_tags_and_subjects_to_subjects() -> None:
    """
    A reading puts calibre's tags at the Work's tags and a record's subjects at the Work's subjects.

    Example:
        >>> test_the_pool_maps_tags_to_tags_and_subjects_to_subjects()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    work = wemi_from_calibre(
        {"title": "A Book", "tags": ["to-read"], "subjects": ["Sea stories"]}
    ).levels["work"]
    assert work["tags"] == ("to-read",)
    assert work["subjects"] == ("Sea stories",)
    assert "unplaced" not in wemi_from_calibre(
        {"title": "A Book", "subjects": ["S"]}
    ).levels.get("manifestation", {})
