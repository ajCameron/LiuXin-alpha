# Calibre compatibility fixture drop-zone (B3)

Put real Calibre library fixtures here to expand the version-compat matrix.

Supported fixture shapes:
- A directory containing `metadata.db` at its root (plus book folders)
- A `.zip` containing such a directory

Suggested naming:
- `calibre_user_version_XXXXX__short_note.zip`
- `calibre_YYYY_MM__version_string.zip`

Notes:
- Keep fixtures small (1-3 books) unless explicitly testing performance.
- Remove covers / large format files if you only need schema/version surface.
