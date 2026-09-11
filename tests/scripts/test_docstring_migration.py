"""
Exercise whole-project documentation discovery and safe reST normalization.

Embedded source strings deliberately include missing or malformed documentation.
They are audit inputs, not undocumented definitions in this test module itself.
"""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path

import pytest

from scripts.audit_project_docstrings import (
    audit_paths,
    definitions,
    documentation_issues,
    parameters,
    project_paths,
)
from scripts.normalize_docstrings import executable_structure, main, normalize_source


def test_missing_module_class_nested_and_async_docs_are_counted() -> None:
    """
    Include private, nested, and async declarations in the documentation inventory.

    Example:
        >>> test_missing_module_class_nested_and_async_docs_are_counted()


    :return: ``None`` after verifying every declaration is reported as undocumented.
    """
    source = (
        "class A:\n async def _method(self):\n  def nested(): pass\n  return nested\n"
    )
    items = list(definitions(ast.parse(source)))
    assert [item.name for item in items] == [
        "<module>",
        "A",
        "A._method",
        "A._method.nested",
    ]
    assert all(
        documentation_issues(item, source) == ("missing_docstring",) for item in items
    )
    _, audit = normalize_source(source, "sample.py")
    assert (audit.modules, audit.classes, audit.functions, audit.missing) == (
        1,
        1,
        2,
        4,
    )


def test_parameter_order_respects_binding_static_methods_and_nested_helpers() -> None:
    """
    Preserve real arguments named self and put variadic arguments before keyword-only ones.

    Example:
        >>> test_parameter_order_respects_binding_static_methods_and_nested_helpers()


    :return: ``None`` after checking free, bound, conditional, static, and nested signatures.
    """
    source = '''def free(self, /, *items, flag=False, **options):
    """Read the supplied objects."""
class A:
    if True:
        def bound(receiver, value):
            """Read one row value."""
    @staticmethod
    def static(self):
        """Accept a real argument named self."""
    def method(self):
        """Build a nested helper."""
        def nested(self):
            """Read the explicit helper argument."""
'''
    expected = {
        "free": ["self", "items", "flag", "options"],
        "A.bound": ["value"],
        "A.static": ["self"],
        "A.method": [],
        "A.method.nested": ["self"],
    }
    normalized, audit = normalize_source(source, "sample.py")
    assert audit.unsafe == 0
    for item in definitions(ast.parse(normalized)):
        if item.name in expected:
            assert parameters(item) == expected[item.name]
            docstring = ast.get_docstring(item.node) or ""
            actual = [
                line.split()[1].rstrip(":")
                for line in docstring.splitlines()
                if line.startswith(":param ")
            ]
            assert actual == expected[item.name]


@pytest.mark.parametrize(
    "source",
    [
        'def f(): "Read a value."; return 7\n',
        'def f():\n    "Read a value."; return 7\n',
        'def f():\n    "Read a value."  # retain this explanation\n    return 7\n',
        'def f():\n    r"Read a \\n escape."\n    return 7\n',
    ],
)
def test_unsafe_literals_retain_adjacent_code_comments_and_escapes(source: str) -> None:
    """
    Refuse line-based edits when a docstring shares source or contains literal escapes.

    Example:
        >>> sample = 'def f(): "Read a value."; return 7' + chr(10)
        >>> test_unsafe_literals_retain_adjacent_code_comments_and_escapes(sample)


    :param source: Valid Python containing one unsafe-to-rewrite documentation literal.
    :return: ``None`` after confirming the source is unchanged and the unsafe count is one.
    """
    normalized, audit = normalize_source(source, "sample.py")
    assert normalized == source
    assert audit.unsafe == 1
    assert executable_structure(normalized) == executable_structure(source)


def test_typed_fields_and_exception_descriptions_survive_normalization() -> None:
    """
    Preserve typed parameter prose, continuation lines, return values, and exceptions.

    Example:
        >>> test_typed_fields_and_exception_descriptions_survive_normalization()


    :return: ``None`` after checking lossless field conversion and normalization idempotence.
    """
    source = '''def read(value, *, limit=1):
    """Read at most the requested number of entries.

    :param int value: Starting entry identifier.
        Identifiers are one-based.
    :param limit: Maximum number of entries to retrieve.
    :returns: The selected entry list.
    :raises ValueError: If the starting identifier is invalid.
    """
    return value, limit
'''
    normalized, audit = normalize_source(source, "sample.py")
    assert audit.unsafe == 0
    for fragment in (
        ":param value: Starting entry identifier.",
        "Identifiers are one-based.",
        ":type value: int",
        ":return: The selected entry list.",
        ":raises ValueError: If the starting identifier is invalid.",
    ):
        assert fragment in normalized
    assert executable_structure(source) == executable_structure(normalized)
    repeated, second_audit = normalize_source(normalized, "sample.py")
    assert repeated == normalized
    assert second_audit.non_normalized == 0


@pytest.mark.parametrize(
    "fields",
    [
        ":param obsolete: Keep this historical explanation.",
        ":param value: First explanation.\n    :param value: Second explanation.",
        ":return: First result description.\n    :returns: Second result description.",
    ],
)
def test_unmatched_or_duplicate_fields_are_not_silently_discarded(fields: str) -> None:
    """
    Keep conflicting signature documentation intact for manual review.

    Example:
        >>> test_unmatched_or_duplicate_fields_are_not_silently_discarded(':param obsolete: Keep this explanation.')


    :param fields: Field text that cannot be matched safely to the function signature.
    :return: ``None`` after confirming no description is overwritten or deleted.
    """
    source = f'def read(value):\n    """Read one entry.\n\n    {fields}\n    """\n    return value\n'
    normalized, audit = normalize_source(source, "sample.py")
    assert normalized == source
    assert audit.unsafe == 1


def test_structural_guard_keeps_non_docstring_strings_and_real_behavior() -> None:
    """
    Ignore only documentation literals when comparing executable source structure.

    Example:
        >>> test_structural_guard_keeps_non_docstring_strings_and_real_behavior()


    :return: ``None`` after verifying that statement strings and return changes remain visible.
    """
    before = '"Module description."\ndef read():\n    "Read one value."\n    "Executable string statement."\n    return 7\n'
    assert executable_structure(before) == executable_structure(
        before.replace("Module description.", "Updated module description.")
    )
    assert executable_structure(before) != executable_structure(
        before.replace("return 7", "return 8")
    )
    assert executable_structure(before) != executable_structure(
        before.replace("Executable string statement.", "Changed executable string.")
    )


def test_audit_accepts_nonempty_rest_fields_and_flags_empty_ones() -> None:
    """
    Require descriptions as well as matching parameter and return field names.

    Example:
        >>> test_audit_accepts_nonempty_rest_fields_and_flags_empty_ones()


    :return: ``None`` after checking inline and continued reST descriptions.
    """
    source = '''def read(value):
    """
    Retrieve the selected entry without mutation.

    Example:
        >>> read(7)


    :param value: Entry identifier to retrieve.
    :return:
        The selected entry.
    """
    return value
'''
    item = list(definitions(ast.parse(source)))[1]
    assert documentation_issues(item, source) == ()
    spaced = source.replace(":return:\n", ":return:\n\n")
    item = list(definitions(ast.parse(spaced)))[1]
    assert documentation_issues(item, spaced) == ()
    empty = source.replace(" Entry identifier to retrieve.", "").replace(
        "        The selected entry.", ""
    )
    item = list(definitions(ast.parse(empty)))[1]
    assert set(documentation_issues(item, empty)) == {
        "empty_parameter_description",
        "empty_return_description",
    }


def test_parse_failures_remain_visible_and_check_fails_for_missing_docs(
    tmp_path: Path,
) -> None:
    """
    Prevent unreadable source or absent docstrings from appearing as a clean audit.

    Example:
        >>> test_parse_failures_remain_visible_and_check_fails_for_missing_docs(workspace)  # doctest: +SKIP


    :param tmp_path: Isolated directory supplied by pytest for source fixtures.
    :return: ``None`` after checking parse diagnostics and the normalizer's failure exit.
    """
    broken = tmp_path / "broken.py"
    broken.write_text("def broken(:\n")
    report = audit_paths([broken], tmp_path)
    assert report["failures"]
    assert report["totals"] == {}
    bare = tmp_path / "bare.py"
    bare.write_text("value = 1\n")
    assert main(["--check", str(bare)]) == 1
    assert bare.read_text() == "value = 1\n"


def test_project_discovery_includes_new_source_and_excludes_ignored_artifacts(
    tmp_path: Path,
) -> None:
    """
    Enumerate source with Git's ownership boundary rather than crawling environments.

    Example:
        >>> test_project_discovery_includes_new_source_and_excludes_ignored_artifacts(workspace)  # doctest: +SKIP


    :param tmp_path: Isolated temporary checkout containing tracked and new source.
    :return: ``None`` after verifying both tracked and newly created source are included.
    """
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / ".gitignore").write_text(".venv/\n")
    (tmp_path / "tracked.py").write_text('"Tracked module."\n')
    subprocess.run(
        ["git", "-C", str(tmp_path), "add", "tracked.py", ".gitignore"], check=True
    )
    (tmp_path / "new.py").write_text('"New module."\n')
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv/third_party.py").write_text("value = 1\n")
    assert project_paths(tmp_path) == (tmp_path / "new.py", tmp_path / "tracked.py")


def test_project_discovery_includes_initialized_submodule_source(
    tmp_path: Path,
) -> None:
    """
    Keep separately versioned data generators visible in the whole-project inventory.

    Example:
        >>> test_project_discovery_includes_initialized_submodule_source(workspace)  # doctest: +SKIP


    :param tmp_path: Temporary parent for a local checkout and its source submodule.
    :return: ``None`` after proving tracked submodule Python is returned by discovery.
    """
    root = tmp_path / "checkout"
    data = tmp_path / "data-source"
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(["git", "init", "-q", str(data)], check=True)
    (data / "generator.py").write_text('"Generate local fixture rows."\n')
    subprocess.run(["git", "-C", str(data), "add", "generator.py"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(data),
            "-c",
            "user.name=Documentation Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "Add source fixture",
        ],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            str(data),
            "data",
        ],
        check=True,
    )
    assert project_paths(root) == (root / "data/generator.py",)
