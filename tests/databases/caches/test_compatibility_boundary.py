"""Retired cache paths stay absent and native imports do not initialize them."""

import importlib.util
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from scripts.check_modern_import_cycles import LEGACY_CACHE_PREFIXES
from scripts.verify_wheel import RETIRED_CACHE_PATHS, verify_wheel
from scripts.verify_wheel_install import WheelVerificationError, inspect_wheel

SOURCE = Path(__file__).resolve().parents[3] / "src"


@pytest.mark.parametrize("path", RETIRED_CACHE_PATHS)
def test_retired_path_has_no_source_or_import_shim(path, tmp_path):
    assert not (SOURCE / path).exists()
    module = path.removesuffix("/").removesuffix(".py").replace("/", ".")
    try:
        spec = importlib.util.find_spec(module)
    except ModuleNotFoundError:
        spec = None
    assert spec is None
    wheel = tmp_path / "stale.whl"
    member = path + "__init__.py" if path.endswith("/") else path
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(member, "")
    with pytest.raises(WheelVerificationError, match="retired modules"):
        inspect_wheel(wheel)
    with pytest.raises(ValueError, match="retired cache compatibility"):
        verify_wheel(wheel)


@pytest.mark.parametrize(
    "module",
    (
        "caches.field_metadata",
        "caches.custom_fields",
        "caches.read_cache",
        "caches.cache",
        "caches.api",
        "caches.updates.field_updates",
        "caches.updates.table_updates",
        "caches.cache_plugins.schema_backed",
        "caches.cache_plugins.database_backed",
        "caches.cache_plugins.numpy_vectorized",
        "catalog.search.database_search",
        "core.services",
    ),
)
def test_native_import_does_not_initialize_legacy_cache_contracts(module):
    program = (
        "import importlib, json, sys; "
        f"importlib.import_module({'LiuXin_alpha.' + module!r}); "
        "print(json.dumps(sorted(sys.modules)))"
    )
    result = subprocess.run(
        [sys.executable, "-c", program],
        capture_output=True,
        text=True,
        check=True,
    )
    loaded = json.loads(result.stdout.splitlines()[-1])
    assert not [
        name
        for name in loaded
        if any(
            name == prefix or name.startswith(prefix + ".")
            for prefix in LEGACY_CACHE_PREFIXES
        )
    ]
