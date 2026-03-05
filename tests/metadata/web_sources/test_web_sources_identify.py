from __future__ import annotations


def test_web_sources_identify_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    assert identify is not None


def test_urls_from_identifiers_default_links() -> None:
    from LiuXin_alpha.metadata.web_sources.identify import urls_from_identifiers

    urls = urls_from_identifiers(
        {
            "isbn": "9780306406157",
            "doi": "10.5555/12345678",
            "arxiv": "2401.01234",
            "oclc": "123456",
            "issn": "2049-3630",
        }
    )
    got = {(id_type, value, url) for _name, id_type, value, url in urls}

    assert ("isbn", "9780306406157", "https://www.worldcat.org/isbn/9780306406157") in got
    assert ("doi", "10.5555/12345678", "https://dx.doi.org/10.5555/12345678") in got
    assert ("arxiv", "2401.01234", "https://arxiv.org/abs/2401.01234") in got
    assert ("oclc", "123456", "https://www.worldcat.org/oclc/123456") in got
    assert ("issn", "20493630", "https://www.worldcat.org/issn/20493630") in got


def test_urls_from_identifiers_uses_plugin_links(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    class _Plugin:
        name = "PluginX"

        @staticmethod
        def get_book_urls(_identifiers):
            return (("plugin-id", "abc", "https://example.invalid/books/abc"),)

        @staticmethod
        def get_book_url_name(id_type, id_val, _url):
            return f"{id_type}:{id_val}"

    monkeypatch.setattr(identify, "_iter_all_metadata_plugins", lambda: [_Plugin()])

    urls = identify.urls_from_identifiers({"isbn": "9780306406157"})
    assert ("plugin-id:abc", "plugin-id", "abc", "https://example.invalid/books/abc") in urls


def test_urls_from_identifiers_accepts_uri_fields_and_sorting() -> None:
    from LiuXin_alpha.metadata.web_sources.identify import urls_from_identifiers

    urls = urls_from_identifiers(
        {
            "uri1": "https://example.invalid/path",
            "url": "https://another.invalid/item",
        },
        sort_results=True,
    )
    names = [name for name, *_rest in urls]

    assert "example.invalid" in names
    assert "another.invalid" in names
    assert names == sorted(names, key=str.casefold)
