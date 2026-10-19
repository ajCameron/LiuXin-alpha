"""Keep developer navigation portable across clean checkouts and folder moves."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest
from markdown_it import MarkdownIt

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_ROOT = REPO_ROOT / "dev-docs"


def _documents() -> list[Path]:
    """Include every developer guide, including dated reviews and new subfolders."""
    return [REPO_ROOT / "README.md", *sorted(DOC_ROOT.rglob("*.md"))]


def _destinations(markdown: str) -> list[str]:
    """Parse inline/reference links and images while ignoring fenced examples."""
    destinations = []
    parser = MarkdownIt("commonmark").enable("table")
    # Audit even file URLs that a renderer would suppress; never render this input.
    parser.validateLink = lambda destination: True
    for block in parser.parse(markdown):
        for token in block.children or []:
            attribute = {"link_open": "href", "image": "src"}.get(token.type)
            if attribute:
                destinations.append(token.attrGet(attribute) or "")
    return destinations


def _is_absolute_file_link(destination: str) -> bool:
    """Recognize non-portable Unix, Windows and file-URI destinations."""
    decoded = unquote(destination)
    return (
        decoded.startswith(("/", "\\"))
        or urlsplit(decoded).scheme.lower() == "file"
        or re.match(r"^[a-zA-Z]:", decoded) is not None
    )


def _local_targets(document: Path) -> list[Path]:
    """Resolve local paths and reject links outside the repository boundary."""
    targets = []
    for destination in _destinations(document.read_text(encoding="utf-8")):
        assert not _is_absolute_file_link(destination), (document, destination)
        parsed = urlsplit(destination)
        if not parsed.scheme and parsed.path:
            target = (document.parent / unquote(parsed.path)).resolve()
            assert target.is_relative_to(REPO_ROOT), (document, destination)
            targets.append(target)
    return targets


def _is_portable_target(target: Path, tracked: set[Path]) -> bool:
    """Require an existing versioned file, or a directory containing versioned files."""
    if not target.exists():
        return False
    if target in tracked:
        return True
    return target.is_dir() and any(path.is_relative_to(target) for path in tracked)


def _reachable_guides(documents: set[Path], start: Path) -> set[Path]:
    """Follow guide links; a directory link enters its README when one exists."""
    reachable: set[Path] = set()
    pending = [start]
    while pending:
        document = pending.pop()
        if document in reachable or document not in documents:
            continue
        reachable.add(document)
        for target in _local_targets(document):
            pending.append(target / "README.md" if target.is_dir() else target)
    return reachable


def test_developer_documentation_has_no_absolute_file_links() -> None:
    """Reject machine-specific paths even in historical developer guides."""
    violations = [
        f"{document.relative_to(REPO_ROOT)}: {destination}"
        for document in _documents()
        for destination in _destinations(document.read_text(encoding="utf-8"))
        if _is_absolute_file_link(destination)
    ]
    assert not violations, "non-portable documentation links:\n" + "\n".join(violations)


def test_all_developer_documentation_destinations_exist_in_tracked_content() -> None:
    """Catch moves and ignored evidence that an existence-only local check misses."""
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    tracked = {REPO_ROOT / name for name in result.stdout.split("\0") if name}
    violations = [
        f"{document.relative_to(REPO_ROOT)} -> {target.relative_to(REPO_ROOT)}"
        for document in _documents()
        for target in _local_targets(document)
        if not _is_portable_target(target, tracked)
    ]
    assert not violations, "missing or untracked documentation targets:\n" + "\n".join(
        violations
    )


def test_every_developer_guide_is_reachable_from_the_index() -> None:
    """Ensure new or moved guides remain discoverable through maintained indexes."""
    documents = set(DOC_ROOT.rglob("*.md"))
    reachable = _reachable_guides(documents, DOC_ROOT / "README.md")
    assert documents == reachable, sorted(
        str(path.relative_to(REPO_ROOT)) for path in documents - reachable
    )


def test_root_readme_exposes_the_developer_index_and_key_owners() -> None:
    """Keep setup, the architecture overview and ownership policy easy to find."""
    assert DOC_ROOT / "README.md" in _local_targets(REPO_ROOT / "README.md")
    targets = set(_local_targets(DOC_ROOT / "README.md"))
    assert {
        DOC_ROOT / "02 - Top Level Structure.md",
        DOC_ROOT / "04 - Seperation of Concerns.md",
        DOC_ROOT / "hacking" / "README.md",
    } <= targets


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
    """Retain unsafe destinations for inspection even when rendering would reject them."""
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
    """Allow repository-relative paths and ordinary external URLs."""
    assert not _is_absolute_file_link(destination)


def test_markdown_parser_handles_reference_links_spaces_and_ignores_code() -> None:
    """Parse navigable links without mistaking example source for navigation."""
    markdown = (
        "[guide](<a guide.md>) [reference][ref] ![diagram](diagram.png)\n\n"
        "[ref]: ../README.md\n\n"
        "`[example](/home/example)`\n\n```text\n[example](/home/example)\n```\n"
    )
    assert _destinations(markdown) == ["a%20guide.md", "../README.md", "diagram.png"]


def test_local_evidence_must_be_versioned_even_when_it_exists(tmp_path: Path) -> None:
    """An author's local report must not hide a broken clean-checkout link."""
    report = tmp_path / "local-report.json"
    report.write_text("{}", encoding="utf-8")
    assert not _is_portable_target(report, set())
    assert _is_portable_target(report, {report})
    report.unlink()
    assert not _is_portable_target(report, {report})


def test_directory_navigation_requires_an_index_link_to_each_guide(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dossier directory becomes navigable only when its README links the guide."""
    monkeypatch.setattr(f"{__name__}.REPO_ROOT", tmp_path)
    index = tmp_path / "README.md"
    dossier = tmp_path / "dossier"
    dossier.mkdir()
    child_index = dossier / "README.md"
    guide = dossier / "guide.md"
    index.write_text("[Dossier](dossier/)\n", encoding="utf-8")
    child_index.write_text("# Dossier\n", encoding="utf-8")
    guide.write_text("# Guide\n", encoding="utf-8")
    documents = {index, child_index, guide}
    assert _reachable_guides(documents, index) == {index, child_index}
    child_index.write_text("[Guide](guide.md)\n", encoding="utf-8")
    assert _reachable_guides(documents, index) == documents
