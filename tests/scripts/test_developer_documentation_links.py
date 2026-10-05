"""
Provide test developer documentation links utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test developer documentation links through a consuming regression::

        python -m pytest -q tests/scripts/test_developer_documentation_links.py
"""

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
    """
    Perform the destinations operation under explicit file-format and conversion rules.

    Example:
        Exercise  destinations through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :param markdown: Value supplied for markdown under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the is absolute file link operation under explicit file-format and conversion rules.

    Example:
        Exercise  is absolute file link through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :param destination: Value supplied for destination under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    decoded = unquote(destination)
    return (
        decoded.startswith(("/", "\\"))
        or urlsplit(decoded).scheme.lower() == "file"
        or re.match(r"^[a-zA-Z]:", decoded) is not None
    )


def _local_targets(document: Path) -> list[Path]:
    """
    Perform the local targets operation under explicit file-format and conversion rules.

    Example:
        Exercise  local targets through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :param document: Value supplied for document under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the test developer documentation has no absolute file links operation under explicit file-format and conversion rules.

    Example:
        Exercise test developer documentation has no absolute file links through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test reviewed guide destinations exist operation under explicit file-format and conversion rules.

    Example:
        Exercise test reviewed guide destinations exist through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :param document: Value supplied for document under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    targets = _local_targets(document)
    assert targets, f"No navigation links found in {document}"
    assert all(target.exists() for target in targets), [
        str(t) for t in targets if not t.exists()
    ]


def test_root_readme_exposes_the_developer_index_and_key_owners() -> None:
    """
    Perform the test root readme exposes the developer index and key owners operation under explicit file-format and conversion rules.

    Example:
        Exercise test root readme exposes the developer index and key owners through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test portability check rejects unix windows and file uri destinations operation under explicit file-format and conversion rules.

    Example:
        Exercise test portability check rejects unix windows and file uri destinations through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :param destination: Value supplied for destination under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test portability check accepts relative and external links operation under explicit file-format and conversion rules.

    Example:
        Exercise test portability check accepts relative and external links through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :param destination: Value supplied for destination under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert not _is_absolute_file_link(destination)


def test_markdown_parser_handles_reference_links_spaces_and_ignores_code() -> None:
    """
    Perform the test markdown parser handles reference links spaces and ignores code operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown parser handles reference links spaces and ignores code through a consuming regression::

            python -m pytest -q tests/scripts/test_developer_documentation_links.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    markdown = (
        "[guide](<a guide.md>) [reference][ref] ![diagram](diagram.png)\n\n"
        "[ref]: ../README.md\n\n"
        "`[example](/home/example)`\n\n```text\n[example](/home/example)\n```\n"
    )
    assert _destinations(markdown) == ["a%20guide.md", "../README.md", "diagram.png"]
