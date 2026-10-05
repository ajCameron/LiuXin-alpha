"""
Provide test calibre chardet utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test calibre chardet through a consuming regression::

        python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py
"""
import random

import pytest


def _import_module():
    """
    Perform the import module utility operation under explicit compatibility rules.

    Example:
        Exercise  import module through a consuming regression::

            python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.libraries import calibre_chardet

    return calibre_chardet


def _has_py3_unicode_alias(cc) -> bool:
    # In calibre-style code, `unicode` is expected to exist; on Python 3 it
    # should be an alias of `str`.
    """
    Perform the has py3 unicode alias utility operation under explicit compatibility rules.

    Example:
        Exercise  has py3 unicode alias through a consuming regression::

            python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


    :param cc: Value supplied for cc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return getattr(cc, "unicode", None) is str


def test_py3_unicode_alias_is_defined_for_runtime():
    """
    Tripwire: this module is currently py2-ish and needs a `unicode=str` alias.

    Example:
        Exercise test py3 unicode alias is defined for runtime through a consuming regression::

            python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    cc = _import_module()
    assert _has_py3_unicode_alias(cc), (
        "calibre_chardet expects a `unicode` type name. On Python 3, define "
        "`unicode = str` (or import it from a compat layer) so detect_xml_encoding() "
        "and xml_to_unicode() can run."
    )


class TestEncodingDeclarationHelpers:
    """
    Provide the TestEncodingDeclarationHelpers utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TestEncodingDeclarationHelpers through a consuming regression::

            python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py
    """
    def test_find_declared_encoding_xml_decl(self):
        """
        Perform the test find declared encoding xml decl utility operation under explicit compatibility rules.

        Example:
            Exercise TestEncodingDeclarationHelpers.test find declared encoding xml decl through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        raw = "<?xml version='1.0' encoding='UTF-8'?><root/>"
        assert cc.find_declared_encoding(raw) == "UTF-8"

    def test_find_declared_encoding_meta_charset_html5(self):
        """
        Perform the test find declared encoding meta charset html5 utility operation under explicit compatibility rules.

        Example:
            Exercise TestEncodingDeclarationHelpers.test find declared encoding meta charset html5 through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        raw = "<meta charset=\"windows-1252\"><p>hi</p>"
        assert cc.find_declared_encoding(raw) == "windows-1252"

    def test_find_declared_encoding_meta_pragma_html4(self):
        """
        Perform the test find declared encoding meta pragma html4 utility operation under explicit compatibility rules.

        Example:
            Exercise TestEncodingDeclarationHelpers.test find declared encoding meta pragma html4 through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        raw = (
            "<meta http-equiv='Content-Type' content='text/html; charset=ISO-8859-1'>"
            "<p>hi</p>"
        )
        assert cc.find_declared_encoding(raw) == "ISO-8859-1"

    def test_strip_encoding_declarations_respects_limit(self):
        """
        Perform the test strip encoding declarations respects limit utility operation under explicit compatibility rules.

        Example:
            Exercise TestEncodingDeclarationHelpers.test strip encoding declarations respects limit through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        # Put the declarations beyond the limit, so they should survive.
        head = "x" * 200
        decl1 = "<?xml version='1.0' encoding='utf-8'?><root>"
        decl2 = "<meta charset='latin-1'>"
        tail = "</root>"
        raw = head + decl1 + ("y" * 200) + decl2 + ("z" * 200) + tail

        stripped = cc.strip_encoding_declarations(raw, limit=100)
        assert "encoding='utf-8'" in stripped
        assert "charset='latin-1'" in stripped

        stripped2 = cc.strip_encoding_declarations(raw, limit=10_000)
        assert "encoding='utf-8'" not in stripped2
        assert "charset='latin-1'" not in stripped2

    def test_replace_encoding_declarations_tracks_changes(self):
        """
        Perform the test replace encoding declarations tracks changes utility operation under explicit compatibility rules.

        Example:
            Exercise TestEncodingDeclarationHelpers.test replace encoding declarations tracks changes through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        raw = "<?xml version='1.0' encoding='utf-8'?><root/>"
        out, changed = cc.replace_encoding_declarations(raw, enc="utf-8")
        assert out == raw
        assert changed is False

        out2, changed2 = cc.replace_encoding_declarations(raw, enc="utf-16")
        assert "encoding='utf-16'" in out2
        assert changed2 is True


class TestEntitySubstitution:
    """
    Provide the TestEntitySubstitution utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TestEntitySubstitution through a consuming regression::

            python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py
    """
    def test_substitute_entites_basic_and_numeric(self):
        """
        Perform the test substitute entites basic and numeric utility operation under explicit compatibility rules.

        Example:
            Exercise TestEntitySubstitution.test substitute entites basic and numeric through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        raw = "Tom &amp; Jerry &lt;3 &#169; &#x1F63A;"
        out = cc.substitute_entites(raw)
        # For safety, core XML entities remain escaped, but non-core entities
        # are decoded to their Unicode equivalents.
        assert out == "Tom &amp; Jerry &lt;3 © 😺"


class TestForceEncoding:
    """
    Provide the TestForceEncoding utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TestForceEncoding through a consuming regression::

            python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py
    """
    def test_force_encoding_maps_ascii_to_utf8(self, monkeypatch):
        """
        Perform the test force encoding maps ascii to utf8 utility operation under explicit compatibility rules.

        Example:
            Exercise TestForceEncoding.test force encoding maps ascii to utf8 through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :param monkeypatch: Value supplied for monkeypatch under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        monkeypatch.setattr(cc, "detect", lambda _b: {"encoding": "ascii", "confidence": 1.0})
        assert cc.force_encoding(b"hello", verbose=False) == "utf-8"

    def test_force_encoding_applies_aliases(self, monkeypatch):
        """
        Perform the test force encoding applies aliases utility operation under explicit compatibility rules.

        Example:
            Exercise TestForceEncoding.test force encoding applies aliases through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :param monkeypatch: Value supplied for monkeypatch under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        monkeypatch.setattr(cc, "detect", lambda _b: {"encoding": "x-sjis", "confidence": 1.0})
        assert cc.force_encoding(b"x", verbose=False) == "shift-jis"

    def test_force_encoding_assume_utf8_overrides_low_confidence(self, monkeypatch):
        """
        Perform the test force encoding assume utf8 overrides low confidence utility operation under explicit compatibility rules.

        Example:
            Exercise TestForceEncoding.test force encoding assume utf8 overrides low confidence through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :param monkeypatch: Value supplied for monkeypatch under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        monkeypatch.setattr(cc, "detect", lambda _b: {"encoding": "windows-1252", "confidence": 0.2})
        assert cc.force_encoding(b"x", verbose=False, assume_utf8=True) == "utf-8"

    def test_force_encoding_verbose_warns(self, monkeypatch):
        """
        Perform the test force encoding verbose warns utility operation under explicit compatibility rules.

        Example:
            Exercise TestForceEncoding.test force encoding verbose warns through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :param monkeypatch: Value supplied for monkeypatch under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        monkeypatch.setattr(cc, "detect", lambda _b: {"encoding": "utf-8", "confidence": 0.3})
        with pytest.warns(RuntimeWarning):
            cc.force_encoding(b"x", verbose=True)


class TestDetectXmlEncodingAndXmlToUnicode:
    """
    Provide the TestDetectXmlEncodingAndXmlToUnicode utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TestDetectXmlEncodingAndXmlToUnicode through a consuming regression::

            python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py
    """
    @pytest.fixture(autouse=True)
    def _skip_until_unicode_alias_exists(self):
        """
        Perform the skip until unicode alias exists utility operation under explicit compatibility rules.

        Example:
            Exercise TestDetectXmlEncodingAndXmlToUnicode. skip until unicode alias exists through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        if not _has_py3_unicode_alias(cc):
            pytest.skip("calibre_chardet is missing the Python 3 `unicode=str` alias")

    def test_detect_xml_encoding_returns_unicode_unchanged(self):
        """
        On Python 3, `unicode` should behave like `str` (this currently breaks).

        Example:
            Exercise TestDetectXmlEncodingAndXmlToUnicode.test detect xml encoding returns unicode unchanged through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        cc = _import_module()
        raw = "<root/>"

        try:
            out_raw, enc = cc.detect_xml_encoding(raw)
        except Exception as e:  # pragma: no cover
            pytest.fail(f"detect_xml_encoding should accept `str` on py3; got {type(e).__name__}: {e}")

        assert out_raw == raw
        assert enc is None

    @pytest.mark.parametrize(
        "bom_name, encoding_tag, text",
        [
            ("BOM_UTF8", "utf8", "snowman ☃"),
            ("BOM_UTF16_LE", "utf-16-le", "hello Δ"),
            ("BOM_UTF16_BE", "utf-16-be", "hello Δ"),
        ],
    )
    def test_detect_xml_encoding_strips_bom(self, bom_name, encoding_tag, text):
        """
        Perform the test detect xml encoding strips bom utility operation under explicit compatibility rules.

        Example:
            Exercise TestDetectXmlEncodingAndXmlToUnicode.test detect xml encoding strips bom through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :param bom_name: Value supplied for bom name under the utility contract.
        :param encoding_tag: Value supplied for encoding tag under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        import codecs

        bom = getattr(codecs, bom_name)

        if encoding_tag == "utf8":
            payload = text.encode("utf-8")
        elif encoding_tag == "utf-16-le":
            payload = text.encode("utf-16-le")
        else:
            payload = text.encode("utf-16-be")
        raw = bom + payload

        try:
            out_raw, enc = cc.detect_xml_encoding(raw)
        except Exception as e:  # pragma: no cover
            pytest.fail(f"detect_xml_encoding should handle BOM bytes; got {type(e).__name__}: {e}")

        assert enc == encoding_tag
        assert out_raw == payload

    def test_detect_xml_encoding_reads_declared_encoding_in_bytes(self):
        """
        Should detect encodings declared in XML/HTML headers (this currently breaks on py3).

        Example:
            Exercise TestDetectXmlEncodingAndXmlToUnicode.test detect xml encoding reads declared encoding in bytes through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        cc = _import_module()
        raw = (
            b"<?xml version='1.0' encoding='windows-1252'?><root>"
            + "cafe\u0301".encode("utf-8")
            + b"</root>"
        )
        try:
            out_raw, enc = cc.detect_xml_encoding(raw)
        except Exception as e:  # pragma: no cover
            pytest.fail(f"detect_xml_encoding should search bytes headers; got {type(e).__name__}: {e}")

        assert out_raw == raw
        assert enc.lower() in {"windows-1252", "cp1252"}

    def test_detect_xml_encoding_gb2312_is_upgraded_to_gbk(self):
        """
        Perform the test detect xml encoding gb2312 is upgraded to gbk utility operation under explicit compatibility rules.

        Example:
            Exercise TestDetectXmlEncodingAndXmlToUnicode.test detect xml encoding gb2312 is upgraded to gbk through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()
        # Minimal HTML with a legacy declaration; content bytes are valid GBK.
        body = "\u4e2d\u56fd".encode("gbk")
        raw = b"<meta charset='gb2312'>" + body

        try:
            out_raw, enc = cc.detect_xml_encoding(raw)
        except Exception as e:  # pragma: no cover
            pytest.fail(f"detect_xml_encoding should handle gb2312->gbk; got {type(e).__name__}: {e}")

        assert out_raw == raw
        assert enc.lower() == "gbk"

    def test_xml_to_unicode_decodes_and_can_strip_decl(self, monkeypatch):
        """
        Perform the test xml to unicode decodes and can strip decl utility operation under explicit compatibility rules.

        Example:
            Exercise TestDetectXmlEncodingAndXmlToUnicode.test xml to unicode decodes and can strip decl through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :param monkeypatch: Value supplied for monkeypatch under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()

        # Force deterministic encoding selection regardless of chardet.
        monkeypatch.setattr(cc, "force_encoding", lambda _b, *_a, **_k: "utf-8")
        raw = b"<?xml version='1.0' encoding='utf-8'?><root>hi</root>"
        try:
            text, enc = cc.xml_to_unicode(raw, strip_encoding_pats=True)
        except Exception as e:  # pragma: no cover
            pytest.fail(f"xml_to_unicode should decode bytes; got {type(e).__name__}: {e}")

        assert enc in {"utf-8", "utf8"}
        assert "encoding" not in text
        assert "<root>hi</root>" in text

    def test_xml_to_unicode_resolves_entities(self, monkeypatch):
        """
        Perform the test xml to unicode resolves entities utility operation under explicit compatibility rules.

        Example:
            Exercise TestDetectXmlEncodingAndXmlToUnicode.test xml to unicode resolves entities through a consuming regression::

                python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


        :param monkeypatch: Value supplied for monkeypatch under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cc = _import_module()

        monkeypatch.setattr(cc, "force_encoding", lambda _b, *_a, **_k: "utf-8")
        raw = b"<root>Tom &amp; Jerry</root>"
        try:
            text, enc = cc.xml_to_unicode(raw, resolve_entities=True)
        except Exception as e:  # pragma: no cover
            pytest.fail(f"xml_to_unicode should resolve entities; got {type(e).__name__}: {e}")

        assert enc in {"utf-8", "utf8"}
        # Core XML entities should remain escaped.
        assert "Tom &amp; Jerry" in text


@pytest.mark.slow
def test_nightmare_random_bytes_does_not_crash_xml_to_unicode(monkeypatch):
    """
    Fuzz-ish: random bytes should never crash decoding (should use replacement chars).

    Example:
        Exercise test nightmare random bytes does not crash xml to unicode through a consuming regression::

            python -m pytest -q tests/utils/libraries/calibre_chardet/test_calibre_chardet.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    cc = _import_module()
    if not _has_py3_unicode_alias(cc):
        pytest.skip("calibre_chardet is missing the Python 3 `unicode=str` alias")

    # Make encoding selection deterministic & permissive.
    monkeypatch.setattr(cc, "force_encoding", lambda _b, *_a, **_k: "utf-8")

    rng = random.Random(1337)
    raw = bytes(rng.getrandbits(8) for _ in range(50_000))

    try:
        text, enc = cc.xml_to_unicode(raw)
    except Exception as e:  # pragma: no cover
        pytest.fail(f"xml_to_unicode should never crash on arbitrary bytes; got {type(e).__name__}: {e}")

    assert isinstance(text, str)
    assert enc in {"utf-8", "utf8"}
