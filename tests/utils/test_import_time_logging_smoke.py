from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_import_time_modules_do_not_print_to_stdout_or_stderr(tmp_path) -> None:
    root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    env["CALIBRE_CONFIG_DIRECTORY"] = str(tmp_path / "calibre-config")
    env["PYTHONWARNINGS"] = "ignore::SyntaxWarning"

    code = (
        "import importlib.util\n"
        "import importlib\n"
        "import sys\n"
        "import types\n"
        "from pathlib import Path\n"
        "root = Path.cwd()\n"
        "module_path = root / 'src' / 'LiuXin_alpha' / 'surfaces' / 'field_metadata.py'\n"
        "spec = importlib.util.spec_from_file_location('tests._field_metadata_smoke', module_path)\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "sys.modules[spec.name] = module\n"
        "spec.loader.exec_module(module)\n"
        "alias = types.ModuleType('LiuXin_alpha.interfaces.field_metadata')\n"
        "alias.FieldMetadata = module.FieldMetadata\n"
        "alias.CalibreFieldMetadata = getattr(module, 'CalibreFieldMetadata', None)\n"
        "alias.calibre_name_to_liuxin_name = module.calibre_name_to_liuxin_name\n"
        "sys.modules[alias.__name__] = alias\n"
        "mods = [\n"
        "    'LiuXin_alpha.utils.text.icu',\n"
        "    'LiuXin_alpha.utils.libraries.liuxin_dateutil.tz',\n"
        "    'LiuXin_alpha.file_formats.conversion.plugins.html_input',\n"
        "]\n"
        "for m in mods:\n"
        "    importlib.import_module(m)\n"
    )

    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        cwd=root,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == ""
    assert proc.stderr == ""
