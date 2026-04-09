from __future__ import annotations

import ast
import re
import sys
import types
import warnings

from pathlib import Path
from types import SimpleNamespace

import pytest


UNICODE_SENTENCES = [
    "Smorgasbord naive cooperate facade voila resume.",
    "Smorgasbord naive cooperate facade voila resume with accents: Smorgasbord naive.",
    "Καλημέρα κόσμε — δοκιμή regex και σημείωση.",
    "Здравствуйте, мир! Это проверка регулярных выражений.",
    "مرحبا بالعالم هذا اختبار شامل للتعابير النمطية.",
    "שלום עולם בדיקת עמידות ביטויים רגולריים.",
    "नमस्ते दुनिया यह नियमित अभिव्यक्ति परीक्षण है।",
    "こんにちは世界 これは正規表現の試験です。",
    "你好，世界。这是一个正则表达式压力测试。",
    "안녕하세요 세계 이것은 정규식 테스트입니다.",
    "Emoji sequence: 👩‍🔬🧪 family 👨‍👩‍👧‍👦 flags 🇺🇸🇯🇵.",
    "Entity mix &amp; nbsp &nbsp; and soft hyphen micro\u00adscopic text.",
]


class _Log:
    def debug(self, *_args) -> None:
        pass

    def warn(self, *_args) -> None:
        pass

    def warning(self, *_args) -> None:
        pass

    def exception(self, *_args) -> None:
        pass

    def __call__(self, *_args) -> None:
        pass


def _install_html2text_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = types.ModuleType("LiuXin_alpha.utils.html2text")
    fake.html2text = lambda text: text
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.utils.html2text", fake)


def _make_opts() -> SimpleNamespace:
    return SimpleNamespace(
        debug_pipeline=None,
        unwrap_lines=True,
        markup_chapter_headings=True,
        format_scene_breaks=True,
        delete_blank_paragraphs=True,
        fix_indents=True,
        italicize_common_cases=True,
        dehyphenate=False,
        renumber_headings=True,
        replace_scene_breaks="✦ ✧ ✦",
        html_unwrap_factor=0.35,
        verbose=1,
    )


def _build_torture_html(multiplier: int = 8) -> str:
    parts = [
        "<p>CHAPTER 1</p>",
        "<p>A Beginning</p>",
        "<p>\u00a0\u00a0\u00a0Indented paragraph with nbsp</p>",
    ]
    for _ in range(multiplier):
        parts.extend(f"<p>{line}</p>" for line in UNICODE_SENTENCES)
    parts.extend(["<p>***</p>", "<p></p>", "<div></div>", "<p></p>"])
    return "<html><head><title>Unicode Torture</title></head><body>\n" + "\n".join(parts) + "\n</body></html>"


def _build_torture_txt_lines(line_sep: str, stop: str) -> str:
    chunks = [
        f"alpha beta gamma delta epsilon zeta eta theta {stop}",
        f"Καλημέρα κόσμε δοκιμή {stop}",
        f"Здравствуйте мир проверка {stop}",
        f"مرحبا بالعالم اختبار {stop}",
        f"你好 世界 压力 测试 {stop}",
        "soft\u00adhyphenated",
    ]
    return line_sep.join(chunks * 6)


def _eval_const_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _eval_const_string(node.left)
        right = _eval_const_string(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _collect_static_regex_patterns(path: Path) -> set[str]:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=SyntaxWarning, message="invalid escape sequence.*")
        tree = ast.parse(path.read_text(encoding="utf-8"))
    patterns: set[str] = set()
    regex_fns = {"compile", "sub", "findall", "finditer", "match", "search"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if not isinstance(node.func.value, ast.Name) or node.func.value.id != "re":
            continue
        if node.func.attr not in regex_fns or not node.args:
            continue
        pattern = _eval_const_string(node.args[0])
        if pattern is not None:
            patterns.add(pattern)
    return patterns


def test_regex_modules_do_not_emit_invalid_escape_syntax_warnings(project_root: Path) -> None:
    targets = (
        project_root / "src/LiuXin_alpha/file_formats/conversion/utils.py",
        project_root / "src/LiuXin_alpha/file_formats/txt/processor.py",
    )
    for target in targets:
        source = target.read_text(encoding="utf-8")
        with warnings.catch_warnings():
            warnings.filterwarnings("error", category=SyntaxWarning, message="invalid escape sequence.*")
            compile(source, str(target), "exec")


@pytest.mark.parametrize(
    ("relative_path", "min_expected"),
    [
        ("src/LiuXin_alpha/file_formats/conversion/utils.py", 50),
        ("src/LiuXin_alpha/file_formats/txt/processor.py", 20),
    ],
)
def test_static_regex_literals_compile_under_unicode_stress(
    relative_path: str,
    min_expected: int,
    project_root: Path,
) -> None:
    patterns = _collect_static_regex_patterns(project_root / relative_path)
    assert len(patterns) >= min_expected
    corpus = " ".join(UNICODE_SENTENCES)
    for pattern in patterns:
        compiled = re.compile(pattern)
        compiled.search(corpus)


def test_conversion_regex_runtime_torture_exercises_dynamic_patterns(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.conversion.utils as conv_utils

    _install_html2text_stub(monkeypatch)
    html = _build_torture_html()
    seen_patterns: list[str] = []

    original_compile = conv_utils.re.compile

    def recording_compile(pattern, flags=0):
        seen_patterns.append(str(pattern))
        return original_compile(pattern, flags)

    monkeypatch.setattr(conv_utils.re, "compile", recording_compile)

    processor = conv_utils.HeuristicProcessor(extra_opts=_make_opts(), log=_Log())

    # Exercise major regex-heavy paths directly as well as through the full pipeline.
    _ = processor.markup_italicis(html)
    _ = processor.markup_chapters(html, processor.get_word_count(html), blanks_between_paragraphs=True)
    _ = processor.punctuation_unwrap(42, _build_torture_txt_lines("\n", "."), "txt")
    _ = processor.punctuation_unwrap(42, html, "html")
    _ = processor.detect_scene_breaks(html)
    _ = processor.detect_soft_breaks(html)
    _ = processor.detect_whitespace(html)
    _ = processor.merge_blanks(html)
    out = processor(html)

    assert isinstance(out, str)
    assert len(seen_patterns) >= 40
    assert "<h2" in out


def test_conversion_regex_pipeline_is_deterministic_and_preserves_unicode(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.conversion.utils as conv_utils

    _install_html2text_stub(monkeypatch)
    html = _build_torture_html()
    opts = _make_opts()

    out_a = conv_utils.HeuristicProcessor(extra_opts=opts, log=_Log())(html)
    out_b = conv_utils.HeuristicProcessor(extra_opts=opts, log=_Log())(html)

    assert out_a == out_b
    for marker in ("Καλημέρα", "Здравствуйте", "مرحبا", "你好", "नमस्ते", "こんにちは", "안녕하세요"):
        assert marker in out_a


@pytest.mark.parametrize("fmt", ["txt", "html"])
@pytest.mark.parametrize("line_sep", ["\n", "\r\n", "\r"])
@pytest.mark.parametrize("stop", [".", "!", "?", "…", "。", "؟"])
def test_punctuation_unwrap_unicode_matrix(fmt: str, line_sep: str, stop: str) -> None:
    import LiuXin_alpha.file_formats.conversion.utils as conv_utils

    processor = conv_utils.HeuristicProcessor(extra_opts=_make_opts(), log=_Log())
    if fmt == "txt":
        content = _build_torture_txt_lines(line_sep, stop)
    else:
        content = _build_torture_html(multiplier=2).replace("\n", line_sep)

    out = processor.punctuation_unwrap(38, content, fmt)
    assert isinstance(out, str)
    assert len(out) > 0
