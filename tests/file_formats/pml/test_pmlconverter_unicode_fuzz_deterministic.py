from __future__ import annotations

import hashlib
import random
import string

import pytest

from LiuXin_alpha.file_formats.pml.pmlconverter import PML_HTMLizer


UNICODE_CHUNKS = (
    "Привет",
    "नमस्ते",
    "こんにちは",
    "مرحبا",
    "שָׁלוֹם",
    "中文測試",
    "👩🏽‍💻",
    "Cafe\u0301",
    "Ω",
    "©",
    "𝄞",
    "हिन्दी العربية русский",
    "ไทยทดสอบ",
    "Español",
    "français",
)

PML_TOKENS = (
    r"\x",
    r"\X0",
    r"\X1",
    r"\X2",
    r"\X3",
    r"\X4",
    r"\i",
    r"\u",
    r"\b",
    r"\q=\"id\"",
    r"\Fn=\"note-1\"",
    r"\Sd=\"side-1\"",
    r"\w=\"50%\"",
    r"\p",
    r"\c",
    r"\U03A9",
    r"\U03a9",
    r"\a169",
    r"\\",
)

ASCII_POOL = string.ascii_letters + string.digits + " !?.,:;+-_()[]{}<>"


def _random_bmp_char(rng: random.Random) -> str:
    ranges = (
        (0x0370, 0x03FF),  # Greek
        (0x0400, 0x04FF),  # Cyrillic
        (0x0590, 0x05FF),  # Hebrew
        (0x0600, 0x06FF),  # Arabic
        (0x0900, 0x097F),  # Devanagari
        (0x0E00, 0x0E7F),  # Thai
        (0x4E00, 0x9FFF),  # CJK
    )
    start, end = rng.choice(ranges)
    return chr(rng.randint(start, end))


def _generate_case(rng: random.Random, max_lines: int = 10, max_parts: int = 10) -> str:
    lines = []
    for _ in range(rng.randint(1, max_lines)):
        parts = []
        for _ in range(rng.randint(1, max_parts)):
            bucket = rng.random()
            if bucket < 0.33:
                parts.append(rng.choice(UNICODE_CHUNKS))
            elif bucket < 0.56:
                parts.append(rng.choice(PML_TOKENS))
            elif bucket < 0.86:
                parts.append("".join(rng.choice(ASCII_POOL) for _ in range(rng.randint(1, 14))))
            else:
                parts.append("".join(_random_bmp_char(rng) for _ in range(rng.randint(1, 4))))
        line = "".join(parts)
        # Frequently inject heading wrappers to exercise TOC extraction.
        if rng.random() < 0.25:
            line = f"\\x{line}\\x"
        lines.append(line)
    return "\n".join(lines)


def _build_corpus(seed: int, cases: int = 120) -> list[str]:
    rng = random.Random(seed)
    return [_generate_case(rng) for _ in range(cases)]


def _toc_signature(toc) -> str:
    parts = []
    for node in toc.flat():
        text = getattr(node, "text", None)
        if text:
            parts.append(text)
    return " | ".join(parts)


def _convert_case(pml: str) -> str:
    hizer = PML_HTMLizer()
    html = hizer.parse_pml(pml, "fuzz_unicode.pml")
    toc = hizer.get_toc()
    return html + "\n--TOC--\n" + _toc_signature(toc)


def _digest(payload: list[str]) -> str:
    joined = "\x1e".join(payload).encode("utf-8", "surrogatepass")
    return hashlib.sha256(joined).hexdigest()


EXPECTED_CORPUS_SHA256 = "5e4b421f52cde1398856821965228a89219fe820eead027c6d84e7ef982dd3c4"
EXPECTED_OUTPUT_SHA256 = "18e490f119b9affa43ddfd16f6e772b7a186c8a1d681f13183b87315413820b6"


def test_unicode_fuzz_generator_is_deterministic_for_fixed_seed() -> None:
    corpus_a = _build_corpus(seed=1337, cases=120)
    corpus_b = _build_corpus(seed=1337, cases=120)
    assert corpus_a == corpus_b
    assert _digest(corpus_a) == EXPECTED_CORPUS_SHA256


def test_unicode_fuzz_conversion_is_deterministic_for_fixed_seed() -> None:
    corpus = _build_corpus(seed=1337, cases=120)
    out_a = [_convert_case(case) for case in corpus]
    out_b = [_convert_case(case) for case in corpus]
    assert out_a == out_b
    assert _digest(out_a) == EXPECTED_OUTPUT_SHA256


@pytest.mark.parametrize("seed", [1, 42, 777, 1337, 9001])
def test_unicode_fuzz_parser_is_robust_across_seeds(seed: int) -> None:
    for case in _build_corpus(seed=seed, cases=60):
        hizer = PML_HTMLizer()
        html = hizer.parse_pml(case, "robustness.pml")
        toc = hizer.get_toc()
        assert isinstance(html, str)
        assert "\x00" not in html
        assert toc is not None
