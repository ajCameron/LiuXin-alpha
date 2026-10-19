"""Load explicitly supplied providers without discovering a domain's plugins.

Composition owners supply bundled classes, installed paths and accepted families.
This module owns the common failure, stream-restoration and priority rules.
"""

from __future__ import annotations

import os
import sys
import time
import traceback
from collections.abc import Iterable, Mapping

from LiuXin_alpha.plugins.base import Plugin


def build_plugins(
    bundled: Iterable[type[Plugin]],
    installed: Mapping[str, str],
    plugin_dir: str,
    *,
    families: tuple[type[Plugin], ...] = (Plugin,),
    perf: bool = False,
) -> list[Plugin]:
    """Initialize providers in stable descending priority order.

    Installed providers precede bundled providers at equal priority. A bundled
    name shadows an installed entry without deleting its files or configuration.
    Ordinary provider failures are isolated; interrupts propagate to the host.
    """
    from LiuXin_alpha.plugins.zipplugin import loader

    providers = list(bundled)
    names = {provider.name for provider in providers}
    stdout, stderr = sys.stdout, sys.stderr
    candidates: list[tuple[type[Plugin], str | None]] = []
    plugins: list[Plugin] = []
    for name, stored_path in installed.items():
        if name in names:
            continue
        path = os.path.join(plugin_dir, name + ".zip")
        if not os.path.exists(path):
            path = stored_path
        try:
            provider = loader.load(path)
            if issubclass(provider, families):
                candidates.append((provider, path))
        except Exception:
            print(f"Failed to load plugin: {name}", file=stderr)
            traceback.print_exc(file=stderr)
        finally:
            sys.stdout, sys.stderr = stdout, stderr

    candidates.extend((provider, None) for provider in providers)
    for provider, path in candidates:
        started = time.monotonic()
        try:
            plugin = provider(path)
            plugin.initialize()
            plugins.append(plugin)
        except Exception:
            print(f"Failed to initialize plugin: {provider.name}", file=stderr)
            traceback.print_exc(file=stderr)
        finally:
            sys.stdout, sys.stderr = stdout, stderr
        if perf:
            print(f"{provider.name:>50}: {time.monotonic() - started:.3f}", file=stdout)

    return sorted(plugins, key=lambda plugin: plugin.priority, reverse=True)
