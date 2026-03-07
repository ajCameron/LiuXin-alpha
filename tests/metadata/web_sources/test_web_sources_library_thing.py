from __future__ import annotations

import socket


def _sample_html() -> str:
    return """
    <html>
      <body>
        <div class="headsummary">
          <h1>The Name of the Wind</h1>
          <h2><a>Patrick Rothfuss</a></h2>
          <h3><a>Kingkiller Chronicle (1)</a></h3>
        </div>
        <table class="wsltable">
          <tr class="wslcontent"><td></td><td></td><td></td><td><span>4.2 stars</span></td></tr>
        </table>
      </body>
    </html>
    """


def test_web_sources_library_thing_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.library_thing as library_thing

    assert library_thing is not None


def test_library_thing_check_for_cover_true_when_payload_exists() -> None:
    from LiuXin_alpha.metadata.web_sources.library_thing import check_for_cover

    assert check_for_cover("9780306406157", opener=lambda url, timeout: b"jpg-bytes")


def test_library_thing_check_for_cover_true_on_302_compat() -> None:
    from LiuXin_alpha.metadata.web_sources.library_thing import check_for_cover

    class _E(Exception):
        @staticmethod
        def getcode():
            return 302

    assert check_for_cover("9780306406157", opener=lambda url, timeout: (_ for _ in ()).throw(_E()))


def test_library_thing_get_social_metadata_parses_core_fields() -> None:
    from LiuXin_alpha.metadata.web_sources.library_thing import get_social_metadata

    mi = get_social_metadata(
        title=None,
        authors=[],
        publisher=None,
        isbn="9780756404741",
        opener=lambda url, timeout: _sample_html().encode("utf-8"),
    )
    assert mi.title == "The Name of the Wind"
    assert mi.authors == ["Patrick Rothfuss"]
    assert mi.series == "Kingkiller Chronicle"
    assert mi.series_index == 1
    assert mi.rating == 4.2


def test_library_thing_get_social_metadata_preserves_existing_title_and_authors() -> None:
    from LiuXin_alpha.metadata.web_sources.library_thing import get_social_metadata

    mi = get_social_metadata(
        title="Keep Title",
        authors=["Keep Author"],
        publisher=None,
        isbn="9780756404741",
        opener=lambda url, timeout: _sample_html().encode("utf-8"),
    )
    assert mi.title == "Keep Title"
    assert mi.authors == ["Keep Author"]
    assert mi.series == "Kingkiller Chronicle"


def test_library_thing_get_social_metadata_server_busy_detection() -> None:
    from LiuXin_alpha.metadata.web_sources.library_thing import ServerBusy, get_social_metadata

    try:
        get_social_metadata(
            title=None,
            authors=[],
            publisher=None,
            isbn="9780756404741",
            opener=lambda url, timeout: b"/wiki/index.php/HelpThing:Verify",
        )
        raise AssertionError("expected ServerBusy")
    except ServerBusy:
        pass


def test_library_thing_get_social_metadata_timeout_maps_to_server_busy() -> None:
    from LiuXin_alpha.metadata.web_sources.library_thing import ServerBusy, get_social_metadata

    class _E(Exception):
        def __init__(self):
            self.reason = socket.timeout()

    try:
        get_social_metadata(
            title=None,
            authors=[],
            publisher=None,
            isbn="9780756404741",
            opener=lambda url, timeout: (_ for _ in ()).throw(_E()),
        )
        raise AssertionError("expected ServerBusy")
    except ServerBusy:
        pass


def test_library_thing_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("library_thing")
    assert hasattr(mod, "get_social_metadata")
