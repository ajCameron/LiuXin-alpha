"""
Check the ``aes`` plugin: ``cryptography`` first, then poppler, then pure-Python AES.

The pure-Python layer is checked against FIPS 197 and NIST SP 800-38A, and
against ``cryptography`` on random keys and data. Which layer serves is checked
in child processes that refuse ``cryptography``, or run with a PATH without
``pdfinfo``, so this process keeps its own resolved plugins.

Example:
    Run the tests::

        python -m pytest -q tests/utils/plugins/test_aes_plugin.py
"""

from __future__ import annotations

import importlib.util
import json
import os
import random
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.utils.plugins.fallbacks import aes as pure
from LiuXin_alpha.utils.plugins.fallbacks.fallback_beta import aes as poppler_layer

HAVE_CRYPTOGRAPHY = importlib.util.find_spec("cryptography") is not None
PDFINFO = shutil.which("pdfinfo")

# Refuses cryptography when BLOCK is true, then runs the given code, which prints JSON.
BLOCK_CRYPTOGRAPHY = """
import importlib.abc, json, sys
class _Refuse(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name == "cryptography" or name.startswith("cryptography."):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None
if BLOCK:
    sys.meta_path.insert(0, _Refuse())
"""
REPORT = """
from LiuXin_alpha.utils.plugins import plugins
module, error = plugins["aes"]
print(json.dumps({"source": getattr(module, "SOURCE", None), "error": error}))
"""


def run_child(code: str, cache: Path, *, block: bool, path: str | None = None) -> dict[str, Any]:
    """
    Run code in a child Python, with or without ``cryptography`` and ``pdfinfo``, and read its JSON.

    Example:
        >>> run_child('print(json.dumps({"ok": True}))', Path("/tmp/cache.json"), block=True)  # doctest: +SKIP
        {'ok': True}


    :param code: Code that prints one JSON object as its last line.
    :param cache: Where the plugin resolver keeps its choices.
    :param block: Whether the child refuses to import ``cryptography``.
    :param path: The child's PATH, or ``None`` to keep this one's.
    :return: The object printed.
    """
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(entry for entry in sys.path if entry),
        "LIUXIN_PLUGIN_CACHE_PATH": str(cache),
    }
    if path is not None:
        env["PATH"] = path
    program = f"BLOCK = {block!r}\n" + BLOCK_CRYPTOGRAPHY + textwrap.dedent(code)
    done = subprocess.run(
        [sys.executable, "-c", program], env=env, capture_output=True, text=True, timeout=120, check=False
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


# FIPS 197 appendix C: (key, plaintext, ciphertext).
FIPS_197 = [
    ("000102030405060708090a0b0c0d0e0f", "00112233445566778899aabbccddeeff", "69c4e0d86a7b0430d8cdb78070b4c55a"),
    (
        "000102030405060708090a0b0c0d0e0f1011121314151617",
        "00112233445566778899aabbccddeeff",
        "dda97ca4864cdfe06eaf70a0ec0d7191",
    ),
    (
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        "00112233445566778899aabbccddeeff",
        "8ea2b7ca516745bfeafc49904b496089",
    ),
]
# NIST SP 800-38A F.2.1 and F.2.5: CBC-AES128 and CBC-AES256, four blocks.
SP_800_38A = [
    (
        "2b7e151628aed2a6abf7158809cf4f3c",
        "7649abac8119b246cee98e9b12e9197d5086cb9b507219ee95db113a917678b2"
        "73bed6b8e3c1743b7116e69e222295163ff1caa1681fac09120eca307586e1a7",
    ),
    (
        "603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4",
        "f58c4c04d6e5f1ba779eabfb5f7bfbd69cfc4e967edb808d679f777bc6702c7d"
        "39f23369a9d9bacfa530e26304231461b2eb05e2c39be9fcda6c19078c6a9d1b",
    ),
]
SP_PLAINTEXT = (
    "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e51"
    "30c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710"
)


@pytest.mark.parametrize("layer", ["fallbacks.aes", "fallbacks.fallback_beta.aes", "fallbacks.fallback_alpha.aes"])
def test_layer_examples_pass(layer: str) -> None:
    """
    Run each layer's examples.

    Example:
        >>> test_layer_examples_pass("fallbacks.aes")  # doctest: +SKIP


    :param layer: The layer's module, under ``LiuXin_alpha.utils.plugins``.
    :return: None; the assertions are the check.
    """
    import doctest
    import importlib

    result = doctest.testmod(importlib.import_module(f"LiuXin_alpha.utils.plugins.{layer}"), report=False)
    assert result.attempted > 0 and result.failed == 0


def test_pure_python_aes_gives_the_published_answers() -> None:
    """
    Encrypt and decrypt FIPS 197's blocks and SP 800-38A's CBC messages.

    Example:
        >>> test_pure_python_aes_gives_the_published_answers()


    :return: None; the assertions are the check.
    """
    for key, plaintext, ciphertext in FIPS_197:
        assert pure.encrypt_block(bytes.fromhex(key), bytes.fromhex(plaintext)).hex() == ciphertext
        assert pure.decrypt_block(bytes.fromhex(key), bytes.fromhex(ciphertext)).hex() == plaintext
    iv, plaintext = bytes(range(16)), bytes.fromhex(SP_PLAINTEXT)
    for key, ciphertext in SP_800_38A:
        assert pure.cbc_encrypt(bytes.fromhex(key), iv, plaintext).hex() == ciphertext
        assert pure.cbc_decrypt(bytes.fromhex(key), iv, bytes.fromhex(ciphertext)) == plaintext
    for bad in (lambda: pure.cbc_encrypt(bytes(15), bytes(16), b""), lambda: pure.cbc_decrypt(bytes(16), bytes(8), b"")):
        with pytest.raises(ValueError):
            bad()


@pytest.mark.skipif(not HAVE_CRYPTOGRAPHY, reason="the cryptography package is not installed")
def test_pure_python_aes_agrees_with_cryptography() -> None:
    """
    Give the same ciphertext and plaintext as ``cryptography`` on 2,000 random keys and messages.

    Example:
        >>> test_pure_python_aes_agrees_with_cryptography()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    from LiuXin_alpha.utils.plugins.fallbacks.fallback_alpha import aes as alpha

    chooser = random.Random(197)
    for _ in range(2000):
        key = chooser.randbytes(chooser.choice((16, 24, 32)))
        iv, data = chooser.randbytes(16), chooser.randbytes(16 * chooser.randint(0, 5))
        assert pure.cbc_encrypt(key, iv, data) == alpha.cbc_encrypt(key, iv, data)
        assert pure.cbc_decrypt(key, iv, data) == alpha.cbc_decrypt(key, iv, data)


def test_the_poppler_layer_has_no_cipher() -> None:
    """
    Refuse to run AES in the poppler layer, which sends files to ``pdfinfo`` instead.

    Example:
        >>> test_the_poppler_layer_has_no_cipher()


    :return: None; the assertions are the check.
    """
    assert poppler_layer.SOURCE == "poppler" and poppler_layer.DECRYPTS_IN_PROCESS is False
    for cipher in (poppler_layer.cbc_encrypt, poppler_layer.cbc_decrypt):
        with pytest.raises(NotImplementedError, match="pdfinfo"):
            cipher(bytes(16), bytes(16), bytes(16))


@pytest.mark.skipif(not HAVE_CRYPTOGRAPHY, reason="the cryptography package is not installed")
def test_cryptography_serves_where_it_is_installed(tmp_path: Path) -> None:
    """
    Serve the plugin from ``cryptography`` where it imports, whatever else is installed.

    Example:
        >>> test_cryptography_serves_where_it_is_installed(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the resolver's cache.
    :return: None; the assertion is the check.
    """
    assert run_child(REPORT, tmp_path / "cache.json", block=False)["source"] == "cryptography"


@pytest.mark.skipif(PDFINFO is None, reason="poppler's pdfinfo is not installed")
def test_poppler_serves_without_cryptography(tmp_path: Path) -> None:
    """
    Serve the poppler layer where ``cryptography`` is missing and ``pdfinfo`` is on the PATH.

    Example:
        >>> test_poppler_serves_without_cryptography(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the resolver's cache.
    :return: None; the assertion is the check.
    """
    assert run_child(REPORT, tmp_path / "cache.json", block=True)["source"] == "poppler"


def test_pure_python_serves_without_cryptography_or_poppler(tmp_path: Path) -> None:
    """
    Serve pure-Python AES with neither ``cryptography`` nor ``pdfinfo``, and read an AES-256 PDF with it.

    Example:
        >>> test_pure_python_serves_without_cryptography_or_poppler(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the resolver's cache, and an empty PATH.
    :return: None; the assertions are the check.
    """
    empty = tmp_path / "empty-path"
    empty.mkdir()
    pdf = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata" / "aes256_no_password.pdf"
    code = REPORT.replace(
        "print(json.dumps(",
        "from LiuXin_alpha.file_formats.pdf.objects import read_metadata\n"
        f"title = read_metadata(open({str(pdf)!r}, 'rb').read()).info['Title']\n"
        "print(json.dumps({'title': title, **",
    ).replace('"error": error}))', '"error": error}}))')
    found = run_child(code, tmp_path / "cache.json", block=True, path=str(empty))
    assert found["source"] == "pure-python"
    assert found["title"] == "Corpus Title — café"


@pytest.mark.skipif(not HAVE_CRYPTOGRAPHY, reason="the cryptography package is not installed")
def test_installing_cryptography_replaces_a_remembered_fallback(tmp_path: Path) -> None:
    """
    Move a remembered pure-Python or poppler choice back to ``cryptography`` once it imports.

    Example:
        >>> test_installing_cryptography_replaces_a_remembered_fallback(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the resolver's cache, and an empty PATH.
    :return: None; the assertions are the check.
    """
    empty = tmp_path / "empty-path"
    empty.mkdir()
    cache = tmp_path / "cache.json"
    assert run_child(REPORT, cache, block=True, path=str(empty))["source"] == "pure-python"
    assert run_child(REPORT, cache, block=False, path=str(empty))["source"] == "cryptography"
