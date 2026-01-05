from __future__ import annotations

from typing import List, Tuple


def test__regex_basic_operations() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import _regex

    pat = _regex.compile(r"(ab)+")
    m = _regex.search(pat, "zzababyy")
    assert m is not None
    assert m.group(0) == "abab"

    assert _regex.match(r"a+", "aa") is not None
    assert _regex.match(r"a+", "ba") is None

    assert _regex.sub(r"a+", "X", "caaad") == "cXd"
    assert _regex.split(_regex.compile(r"\s+"), "a  b\tc") == ["a", "b", "c"]
    assert _regex.findall(r"a.", "a1 a2 a3") == ["a1", "a2", "a3"]


def test_patiencediff_sequence_matcher_and_helpers() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import _patiencediff_c

    a = list("abcxabcd")
    b = list("abcdabc")

    sm = _patiencediff_c.PatienceSequenceMatcher(a, b)
    blocks = sm.get_matching_blocks()
    assert blocks

    pairs: List[Tuple[int, int]] = _patiencediff_c.unique_lcs_c(a, b)
    assert pairs
    # Pairs must point to equal items
    for i, j in pairs[:5]:
        assert a[i] == b[j]

    answer: List[Tuple[int, int, int]] = []
    _patiencediff_c.recurse_matches_c(a, b, 0, 0, len(a), len(b), answer, maxrecursion=100)
    assert answer
    for i, j, n in answer:
        assert n >= 0
        assert a[i : i + n] == b[j : j + n]
