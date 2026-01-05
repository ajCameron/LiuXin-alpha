from __future__ import annotations

import json
import os
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

# Layers in preference order (after compiled extension):
# - fallbacks.fallback_alpha.<name>
# - fallbacks.fallback_beta.<name>
# - fallbacks.<name> (legacy)
FALLBACK_LAYERS: Tuple[str, ...] = ("fallback_alpha", "fallback_beta", "")

CACHE_ENV = "LIUXIN_PLUGIN_CACHE_PATH"


def _machine_fingerprint() -> str:
    return "|".join([platform.system(), platform.machine(), platform.python_version()])


def _default_cache_path() -> Path:
    root = Path(os.environ.get("LIUXIN_CACHE_DIR", Path.home() / ".cache"))
    return root / "liuxin_alpha" / "plugin_selection.json"


def _cache_path() -> Path:
    p = os.environ.get(CACHE_ENV)
    return Path(p) if p else _default_cache_path()


def _load_cache(path: Path) -> dict:
    try:
        if not path.exists():
            return {"__fingerprint__": _machine_fingerprint(), "plugins": {}}
        data = json.loads(path.read_text("utf-8"))
        if data.get("__fingerprint__") != _machine_fingerprint():
            return {"__fingerprint__": _machine_fingerprint(), "plugins": {}}
        if "plugins" not in data or not isinstance(data["plugins"], dict):
            data["plugins"] = {}
        return data
    except Exception:
        return {"__fingerprint__": _machine_fingerprint(), "plugins": {}}


def _save_cache(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data["__fingerprint__"] = _machine_fingerprint()
    path.write_text(json.dumps(data, indent=2, sort_keys=True), "utf-8")


def _probe(mod) -> Tuple[bool, str]:
    fn = getattr(mod, "__liuxin_plugin_probe__", None)
    if fn is None:
        return True, "no-probe"
    try:
        ok, reason = fn()
        return bool(ok), str(reason)
    except Exception as e:
        return False, f"probe raised: {e!r}"


@dataclass(frozen=True)
class ResolvedPlugin:
    module: Optional[object]
    source: Optional[str]  # module import path chosen
    err: Optional[str]


def resolve_fallback_module_path(plugin_name: str) -> Iterable[str]:
    base = __name__.rsplit(".", 1)[0]  # LiuXin_alpha.utils.plugins
    # The fallback package lives under base + ".fallbacks"
    fb_base = f"{base}.fallbacks"
    for layer in FALLBACK_LAYERS:
        if layer:
            yield f"{fb_base}.{layer}.{plugin_name}"
        else:
            yield f"{fb_base}.{plugin_name}"


def resolve_plugin(plugin_name: str, *, import_module) -> ResolvedPlugin:
    """
    Resolve a plugin by layered fallback.
    `import_module` is injected so caller can decide how/where to import compiled modules.
    """
    cache_path = _cache_path()
    cache = _load_cache(cache_path)
    plugins = cache.get("plugins", {})

    # 1) Try cached choice first
    cached = plugins.get(plugin_name)
    if isinstance(cached, str):
        try:
            mod = import_module(cached)
            ok, reason = _probe(mod)
            if ok:
                return ResolvedPlugin(mod, cached, None)
        except Exception as e:
            # fall through
            pass

    # 2) Try each layer in order
    errors = []
    for mod_path in resolve_fallback_module_path(plugin_name):
        try:
            mod = import_module(mod_path)
        except ModuleNotFoundError as e:
            errors.append(f"{mod_path}: not found")
            continue
        except Exception as e:
            errors.append(f"{mod_path}: import failed: {e!r}")
            continue

        ok, reason = _probe(mod)
        if ok:
            plugins[plugin_name] = mod_path
            cache["plugins"] = plugins
            _save_cache(cache_path, cache)
            return ResolvedPlugin(mod, mod_path, None)
        errors.append(f"{mod_path}: probe failed: {reason}")

    return ResolvedPlugin(None, None, "\n".join(errors) or f"No candidates for {plugin_name}")


def write_selection_cache(plugin_names: Iterable[str], *, import_module) -> Path:
    """
    Probes all plugins and writes the cache (best effort). Returns cache path.
    """
    cache_path = _cache_path()
    cache = _load_cache(cache_path)
    plugins = cache.get("plugins", {})
    for name in plugin_names:
        r = resolve_plugin(name, import_module=import_module)
        if r.source:
            plugins[name] = r.source
    cache["plugins"] = plugins
    _save_cache(cache_path, cache)
    return cache_path
