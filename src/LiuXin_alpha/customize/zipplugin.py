"""Compatibility exports for the generic ZIP plugin loader."""
from LiuXin_alpha.plugin_system.zipplugin import *  # noqa: F403


if __name__ == "__main__":
    from tempfile import NamedTemporaryFile
    from LiuXin_alpha.customize.ui import add_plugin
    from LiuXin_alpha.utils.storage.local import CurrentDir

    path = sys.argv[-1]
    with NamedTemporaryFile(suffix=".zip") as f:
        with zipfile.ZipFile(f, "w") as zf:
            with CurrentDir(path):
                for x in os.listdir("."):
                    if x[0] != ".":
                        print("Adding", x)
                    zf.write(x)
                    if os.path.isdir(x):
                        for y in os.listdir(x):
                            zf.write(os.path.join(x, y))
        add_plugin(f.name)
        print("Added plugin from", sys.argv[-1])
