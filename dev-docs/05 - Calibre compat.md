

Used when we're tyring to import and load a calibre plugin.

```python

from LiuXin_alpha.utils.calibre_compat.import_diagnostics import calibre_import_failure_logging

with calibre_import_failure_logging():
    # load / run plugin code that may try: calibre.utils.*
    ...

```