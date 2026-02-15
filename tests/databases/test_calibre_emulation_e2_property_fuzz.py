from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pytest

# Hypothesis is an optional dev dependency for LiuXin_alpha.
hypothesis = pytest.importorskip("hypothesis", reason="Install hypothesis to run E2 fuzz/property tests")
from hypothesis import HealthCheck, given, settings  # type: ignore  # noqa: E402
import hypothesis.strategies as st  # type: ignore  # noqa: E402

from LiuXin_alpha.databases.calibre_emulation import CalibreReader
from LiuXin_alpha.databases.calibre_emulation.readers import (
    _coerce_custom_item,
    _dedupe_preserve_order,
    _normalize_datetime,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


# -------------------------------------------------------------------------------------------------
# Strategies
# -------------------------------------------------------------------------------------------------


_SAFE_CHARS = list("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_.,'()[]") + [
    "é",
    "Ω",
    "字",
    "🙂",
]


def _safe_text(*, min_size: int = 1, max_size: int = 40) -> st.SearchStrategy[str]:
    # Keep it cross-platform and path-friendly (no separators, no control chars, no surrogates).
    return st.text(alphabet=_SAFE_CHARS, min_size=min_size, max_size=max_size).map(lambda s: s.strip() or "X")


def _label() -> st.SearchStrategy[str]:
    first = st.sampled_from(list("abcdefghijklmnopqrstuvwxyz"))
    rest = st.text(alphabet=list("abcdefghijklmnopqrstuvwxyz0123456789_"), min_size=0, max_size=10)
    return st.tuples(first, rest).map(lambda t: (t[0] + t[1]).lower())


@dataclass(frozen=True)
class CustomColSpec:
    label: str
    name: str
    datatype: str
    is_multiple: bool


_CUSTOM_TYPES = ("text", "bool", "int", "float", "datetime", "series", "enumeration", "rating")


def _custom_col_spec() -> st.SearchStrategy[CustomColSpec]:
    def _build(label: str, datatype: str, multi_flag: bool) -> CustomColSpec:
        is_multiple = bool(multi_flag) if datatype == "text" else False
        name = f"CC {label}"
        return CustomColSpec(label=label, name=name, datatype=datatype, is_multiple=is_multiple)

    return st.builds(
        _build,
        label=_label(),
        datatype=st.sampled_from(_CUSTOM_TYPES),
        multi_flag=st.booleans(),
    )


@dataclass(frozen=True)
class BookSpec:
    title: str
    authors: Tuple[str, ...]
    tags: Tuple[str, ...]
    languages: Tuple[str, ...]
    series: Optional[Tuple[str, float]]
    identifiers: Mapping[str, str]
    comments_html: Optional[str]
    formats: Mapping[str, bytes]
    cover: bool
    custom_values: Mapping[str, Any]


_FORMATS = ("EPUB", "PDF", "MOBI", "AZW3", "TXT")


def _format_bytes(tag: str) -> bytes:
    # Small, deterministic-ish payloads (enough to test file presence/size).
    return (f"{tag}\n" + ("x" * 128)).encode("utf-8")


def _draw_value_for_col(data: st.DataObject, col: CustomColSpec) -> Any:
    dt = col.datatype
    if dt == "text":
        if col.is_multiple:
            vals = data.draw(st.lists(_safe_text(min_size=0, max_size=20), min_size=0, max_size=6))
            # Encourage duplicates to exercise dedupe behavior.
            if vals and data.draw(st.booleans()):
                vals = vals + [vals[0]]
            return vals
        # Allow None sometimes (column absent for a given book).
        return data.draw(st.one_of(st.none(), _safe_text(min_size=0, max_size=40)))
    if dt == "bool":
        return data.draw(st.one_of(st.none(), st.booleans()))
    if dt in ("int", "rating"):
        hi = 10 if dt == "rating" else 10_000
        return data.draw(st.one_of(st.none(), st.integers(min_value=0, max_value=hi)))
    if dt == "float":
        # Use scaled ints to keep it simple and stable.
        return data.draw(
            st.one_of(
                st.none(),
                st.integers(min_value=-10_000, max_value=10_000).map(lambda i: i / 10.0),
            )
        )
    if dt == "datetime":
        # Produce a mix of ISO strings, "Z" suffix, and python datetime-ish strings.
        # Reader normalizes to ISO8601.
        choice = data.draw(st.integers(min_value=0, max_value=2))
        if choice == 0:
            return None
        if choice == 1:
            # ISO-like with Z suffix
            s = data.draw(_safe_text(min_size=10, max_size=20))
            # Force something that parses as ISO date.
            return "2024-01-02T03:04:05Z"
        # space-separated legacy-ish
        return "2024-01-02 03:04:05"
    if dt == "series":
        # Accept (name, index) or dict form; builder will store index in link.extra.
        name = data.draw(_safe_text(min_size=1, max_size=25))
        idx = data.draw(st.integers(min_value=1, max_value=20).map(float))
        as_dict = data.draw(st.booleans())
        if as_dict:
            return {"name": name, "index": idx}
        return (name, idx)
    if dt == "enumeration":
        return data.draw(st.one_of(st.none(), st.sampled_from(["red", "green", "blue", "alpha", "beta"])))
    raise AssertionError(f"Unhandled datatype: {dt!r}")


def _expected_custom_value(col: CustomColSpec, raw_value: Any) -> Any:
    """Convert a builder-input value into the CalibreReader output shape."""
    dt = col.datatype
    if raw_value is None:
        return None if not col.is_multiple else []
    if dt == "datetime":
        return _normalize_datetime(raw_value)
    if dt == "series":
        # Unpack supported shapes.
        name: Any = raw_value
        extra: Any = None
        if isinstance(raw_value, (tuple, list)) and len(raw_value) == 2:
            name, extra = raw_value[0], raw_value[1]
        elif isinstance(raw_value, dict):
            name = raw_value.get("name", raw_value.get("series", raw_value.get("value")))
            extra = raw_value.get("index", raw_value.get("series_index", raw_value.get("extra")))
        return _coerce_custom_item("series", name, extra)
    if dt == "text":
        if col.is_multiple:
            # Reader coerces each item to string and dedupes preserve-order.
            items = []
            for v in raw_value:
                items.append(_coerce_custom_item("text", v, None))
            return _dedupe_preserve_order(items)
        return _coerce_custom_item("text", raw_value, None)
    return _coerce_custom_item(dt, raw_value, None)


# -------------------------------------------------------------------------------------------------
# Property tests
# -------------------------------------------------------------------------------------------------


@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(st.data())
def test_e2_fuzz_roundtrip_reader_payloads(provision_calibre_library, data: st.DataObject) -> None:
    """Generate small random Calibre libraries and assert reader invariants.

    This is intended to catch corner cases in:
      - joins/ordering for authors/tags/languages/formats
      - custom-column decoding/normalization (D2)
      - schema guardrails that should not break basic reads
    """
    # Keep the generated libraries small and fast.
    cols = data.draw(st.lists(_custom_col_spec(), min_size=0, max_size=4))
    # Ensure unique labels (Calibre requires it).
    seen = set()
    dedup_cols: List[CustomColSpec] = []
    for c in cols:
        if c.label not in seen:
            seen.add(c.label)
            dedup_cols.append(c)
    cols = dedup_cols

    num_books = data.draw(st.integers(min_value=1, max_value=6))

    lib = provision_calibre_library(name=f"fuzz_{uuid.uuid4().hex}")
    b = CalibreLibraryBuilder(lib.root)

    # Create custom columns up-front.
    for c in cols:
        b.create_custom_column(label=c.label, name=c.name, datatype=c.datatype, is_multiple=c.is_multiple)

    expected: Dict[int, Dict[str, Any]] = {}

    for _i in range(num_books):
        title = data.draw(_safe_text(min_size=1, max_size=60))
        authors = tuple(data.draw(st.lists(_safe_text(min_size=1, max_size=30), min_size=1, max_size=3)))
        tags = tuple(data.draw(st.lists(_safe_text(min_size=1, max_size=20).map(lambda s: s.lower()), min_size=0, max_size=5)))
        languages = tuple(data.draw(st.lists(st.sampled_from(["eng", "fra", "deu", "spa"]), min_size=0, max_size=2)))
        if not languages:
            languages = ("eng",)

        series = None
        if data.draw(st.booleans()):
            series = (data.draw(_safe_text(min_size=1, max_size=25)), float(data.draw(st.integers(min_value=1, max_value=10))))

        identifiers: Dict[str, str] = {}
        if data.draw(st.booleans()):
            identifiers["isbn"] = str(data.draw(st.integers(min_value=10_000_000_000, max_value=99_999_999_999)))
        if data.draw(st.booleans()):
            identifiers["uuid"] = uuid.uuid4().hex

        comments_html = None
        if data.draw(st.booleans()):
            comments_html = f"<p>{data.draw(_safe_text(min_size=0, max_size=80))}</p>"

        fmts = data.draw(st.lists(st.sampled_from(_FORMATS), min_size=1, max_size=3, unique=True))
        formats = {f: _format_bytes(f) for f in fmts}

        cover = data.draw(st.booleans())
        cover_bytes = b"\xff\xd8\xff\xe0" + (b"\x00" * 64) if cover else None

        # Choose which custom columns to populate for this book.
        custom_values: Dict[str, Any] = {}
        for c in cols:
            if data.draw(st.booleans()):
                v = _draw_value_for_col(data, c)
                if v is not None:
                    custom_values[c.label] = v

        added = b.add_book(
            title=title,
            authors=list(authors),
            tags=list(tags),
            languages=list(languages),
            series=series[0] if series else None,
            series_index=series[1] if series else None,
            identifiers=identifiers,
            comments_html=comments_html,
            formats=formats,
            cover_bytes=cover_bytes,
            custom_values=custom_values,
        )

        exp_custom: Dict[str, Any] = {}
        for c in cols:
            if c.label in custom_values:
                exp_custom[c.label] = _expected_custom_value(c, custom_values[c.label])

        expected[added.book_id] = {
            "title": title,
            "authors": authors,
            "tags": tuple(sorted(set(tags))),
            "languages": tuple(sorted(set(languages))),
            "identifiers": dict(identifiers),
            "series": None if series is None else {"name": series[0], "index": float(series[1])},
            "formats": tuple(sorted(fmts)),
            "comments_html": comments_html,
            "has_cover": cover,
            "custom_values": exp_custom,
        }

    r = CalibreReader.from_root(lib.root)
    payloads = list(
        r.iter_book_payloads(
            batch_size=50,
            include_custom_values=True,
            include_formats=True,
            include_cover_path=True,
            filesystem_reconcile=True,
            strict_paths=True,
            best_effort=True,
        )
    )

    assert len(payloads) == len(expected)

    by_id = {p.calibre_book_id: p for p in payloads}
    assert set(by_id) == set(expected)

    for bid, exp in expected.items():
        p = by_id[bid]
        assert p.title == exp["title"]
        assert p.authors == exp["authors"]
        assert tuple(sorted(set(p.tags))) == exp["tags"]
        assert tuple(sorted(set(p.languages))) == exp["languages"]
        assert dict(p.identifiers) == exp["identifiers"]

        if exp["series"] is None:
            assert p.series is None
        else:
            assert p.series is not None
            assert p.series.name == exp["series"]["name"]
            assert p.series.index == exp["series"]["index"]

        got_fmts = tuple(sorted([f.fmt for f in p.formats]))
        assert got_fmts == exp["formats"]
        for fref in p.formats:
            assert fref.file_path.exists()
            # Size is best-effort; just assert it's not wildly wrong.
            assert fref.file_path.stat().st_size > 0

        if exp["comments_html"] is None:
            assert p.comments_html is None
        else:
            assert isinstance(p.comments_html, str)
            assert "<p>" in p.comments_html

        if exp["has_cover"]:
            assert p.cover_path is not None
            assert p.cover_path.exists()
        else:
            # Some Calibre libs can have cover files even when has_cover is 0 (drift),
            # but our builder sets them consistently.
            assert p.cover_path is None or not p.cover_path.exists()

        # Custom columns: only assert the ones we set (unknown columns may exist in other contexts).
        for k, v in exp["custom_values"].items():
            assert k in p.custom_values
            assert p.custom_values[k] == v


@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(st.data())
def test_e2_fuzz_datetime_normalization_is_stable(provision_calibre_library, data: st.DataObject) -> None:
    """Fuzz datetime-ish custom column values and assert ISO normalization."""
    lib = provision_calibre_library(name=f"fuzz_dt_{uuid.uuid4().hex}")
    b = CalibreLibraryBuilder(lib.root)
    b.create_custom_column(label="dt", name="Datetime", datatype="datetime", is_multiple=False)

    raw = data.draw(st.sampled_from([None, "2024-01-02T03:04:05Z", "2024-01-02 03:04:05", "2024-01-02T03:04:05+00:00"]))
    added = b.add_book(title="DT", authors=["A"], formats={"EPUB": b"e"}, custom_values={"dt": raw})

    r = CalibreReader.from_root(lib.root)
    p = next(iter(r.iter_book_payloads(batch_size=10, include_custom_values=True)))
    assert p.calibre_book_id == added.book_id
    assert p.custom_values.get("dt") == _normalize_datetime(raw)
