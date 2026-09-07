"""Keep developer navigation portable and reviewed local destinations real."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest
from markdown_it import MarkdownIt

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_ROOT = REPO_ROOT / "dev-docs"
REVIEWED_GUIDES = (
    REPO_ROOT / "README.md",
    DOC_ROOT / "README.md",
    DOC_ROOT / "continuous-integration.md",
    DOC_ROOT / "07 - Test Databases.md",
    DOC_ROOT / "read-only-surface-appliance-startup.md",
    DOC_ROOT / "maintainability-quality-gates.md",
    DOC_ROOT / "test-streams.md",
)


def _destinations(markdown: str) -> list[str]:
    destinations = []
    parser = MarkdownIt("commonmark")
    # Audit even file URLs that a renderer would suppress; never render this input.
    parser.validateLink = lambda destination: True
    for block in parser.parse(markdown):
        for token in block.children or []:
            attribute = {"link_open": "href", "image": "src"}.get(token.type)
            if attribute:
                destinations.append(token.attrGet(attribute) or "")
    return destinations


def _is_absolute_file_link(destination: str) -> bool:
    decoded = unquote(destination)
    return (
        decoded.startswith(("/", "\\"))
        or urlsplit(decoded).scheme.lower() == "file"
        or re.match(r"^[a-zA-Z]:", decoded) is not None
    )


def _local_targets(document: Path) -> list[Path]:
    targets = []
    for destination in _destinations(document.read_text()):
        assert not _is_absolute_file_link(destination), (document, destination)
        parsed = urlsplit(destination)
        if not parsed.scheme and parsed.path:
            target = (document.parent / unquote(parsed.path)).resolve()
            assert target.is_relative_to(REPO_ROOT.resolve()), (document, destination)
            targets.append(target)
    return targets


def test_developer_documentation_has_no_absolute_file_links() -> None:
    violations = []
    for document in [REPO_ROOT / "README.md", *sorted(DOC_ROOT.rglob("*.md"))]:
        violations.extend(
            f"{document.relative_to(REPO_ROOT)}: {destination}"
            for destination in _destinations(document.read_text())
            if _is_absolute_file_link(destination)
        )
    assert not violations, "non-portable documentation links:\n" + "\n".join(violations)


@pytest.mark.parametrize("document", REVIEWED_GUIDES, ids=lambda path: path.name)
def test_reviewed_guide_destinations_exist(document: Path) -> None:
    targets = _local_targets(document)
    assert targets, f"No navigation links found in {document}"
    assert all(target.exists() for target in targets), [
        str(t) for t in targets if not t.exists()
    ]


def test_root_readme_exposes_the_developer_index_and_key_owners() -> None:
    assert DOC_ROOT / "README.md" in _local_targets(REPO_ROOT / "README.md")
    targets = set(_local_targets(DOC_ROOT / "README.md"))
    required = {
        "00 - Style Guide.md",
        "02 - Top Level Structure.md",
        "maintainability-quality-gates.md",
        "continuous-integration.md",
        "test-streams.md",
        "packaging.md",
        "core-program-workflows.md",
        "cli-composition.md",
        "terminal-composition.md",
        "storage/storage_api.md",
        "metadata_container/metadata_container_system_guide.md",
        "file-formats/README.md",
    }
    assert {DOC_ROOT / path for path in required} <= targets


@pytest.mark.parametrize(
    "destination",
    [
        "/home/developer/repo/a.md",
        "/mnt/c/dev/repo/a.md",
        "file:///tmp/a.md",
        "C:/repo/a.md",
        "C:%5Crepo%5Ca.md",
    ],
)
def test_portability_check_rejects_unix_windows_and_file_uri_destinations(
    destination: str,
) -> None:
    assert _is_absolute_file_link(destination)
    links = _destinations(f"[example](<{destination}>)")
    assert len(links) == 1
    assert _is_absolute_file_link(links[0])


@pytest.mark.parametrize(
    "destination",
    [
        "../README.md",
        "guide.md#section",
        "https://example.org/a",
        "mailto:dev@example.org",
    ],
)
def test_portability_check_accepts_relative_and_external_links(
    destination: str,
) -> None:
    assert not _is_absolute_file_link(destination)


def test_markdown_parser_handles_reference_links_spaces_and_ignores_code() -> None:
    markdown = (
        "[guide](<a guide.md>) [reference][ref] ![diagram](diagram.png)\n\n"
        "[ref]: ../README.md\n\n"
        "`[example](/home/example)`\n\n```text\n[example](/home/example)\n```\n"
    )
    assert _destinations(markdown) == ["a%20guide.md", "../README.md", "diagram.png"]
