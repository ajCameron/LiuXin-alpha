from __future__ import annotations

import hashlib
from pathlib import Path

# Hash format is intentionally consistent with legacy fixture manifests:
#   sha512(file_bytes).hexdigest() + str(file_size_in_bytes)
EXPECTED_HTML_INGEST_FIXTURE_HASHES: dict[str, str] = {
    "html_ingest_case_001_comment_overrides_meta.html": "a3c570b917c69478d040eb15fdd13a81e3078d06db7d0b84c9d1425a75637bb9916561a200e95a9574769c7cc63d81b581c34d751eb768fadd666bcc0d02aa79310",
    "html_ingest_case_002_windows1252_uppercase.htm": "8a2927508e7dc8d02e246e93efcf15a1f13edad48f844bfefde75f5d1208fcabd08efe50fbaa4212fbc2cdd533e8ccfb2a84c52fe4a80ace1e868aba41187f9f136",
    "html_ingest_case_003_unquoted_meta.html": "9a27c3288dd5d140a1f05a8fc787c41f73c0148e18cbc1d57e330342c6475efc6afbdc0b6f9e3a5c7fcc168674df6b92244be00c9a7caa18012c9109786d242a167",
    "html_ingest_case_004_truncated_comment.html": "cff19c8e004f93159ae91ff72fd8ea3dbd277c007e550b05ba512251bd2046ffe4ed3fb332e47b090190010c09457a9baf741ded746d5a9a73572e609070e538126",
    "html_ingest_case_005_binary_prefix.html": "1ed5eeae3549be1d2de6ffdd464722255de1e3be0bd2e19e3c441b77ee9e40bd5072715be22d5130e7dd01713711de4184b2d97aa7e1718ed1033797a92fa8f0131",
    "html_ingest_case_006_identifier_noise.html": "f0af5bf48d7db1d514d45fb20fcb1df14c3c459d04f3d8e4cedb623b2af8789468e8232ade7af27f6a244f1210e926a445d37a8aeee47160841272d504b69cfa531",
}


def legacy_sha512_size_hash(path: Path) -> str:
    """Return the historical LiuXin file hash format used by fixture tests."""
    hasher = hashlib.sha512()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest() + str(path.stat().st_size)
