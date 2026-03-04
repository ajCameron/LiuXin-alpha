from __future__ import annotations

EXPECTED_HTML_INGEST_RESULTS: dict[str, dict[str, object]] = {
    "html_ingest_case_001_comment_overrides_meta.html": {
        "title": "Comment Title — 合集",
        "authors": ["Zoë", "Łukasz"],
        "languages": [],
        "tags": ["ingest", "edge"],
        "identifiers": {},
    },
    "html_ingest_case_002_windows1252_uppercase.htm": {
        "title": "Café “naïve”",
        "authors": ["José", "Anaïs"],
        "languages": ["fr"],
        "tags": [],
        "identifiers": {},
    },
    "html_ingest_case_003_unquoted_meta.html": {
        "title": "UnquotedTitle",
        "authors": ["Jane Doe"],
        "languages": [],
        "tags": [],
        "identifiers": {"doi": ["10.4321/example"]},
    },
    "html_ingest_case_004_truncated_comment.html": {
        "title": "Broken but usable",
        "authors": ["Nina", "Omar"],
        "languages": [],
        "tags": ["one", "two"],
        "identifiers": {},
    },
    "html_ingest_case_005_binary_prefix.html": {
        "title": "Recovered",
        "authors": ["A", "B"],
        "languages": [],
        "tags": [],
        "identifiers": {},
    },
    "html_ingest_case_006_identifier_noise.html": {
        "title": "Identifier Test",
        "authors": ["Casey"],
        "languages": [],
        "tags": [],
        "identifiers": {"amazon": ["B00TESTCASE"], "doi": ["10.1000/182"]},
    },
}
