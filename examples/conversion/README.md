# Conversion examples

- `conversion_to_oeb_example.py` converts supported input formats to an OEB
  directory and can report the current format set with `--list-formats`.
- `conversion_oeb_to_epub_example.py` produces EPUB from OEB/OPF input.
- `conversion_oeb_to_mobi_example.py` produces MOBI from OEB/OPF input.
- `conversion_batch_to_oeb_example.py` runs the input-to-OEB example for a set
  of source files.

The EPUB and MOBI examples generate a tiny sample OEB when `--input-opf` is
omitted, making them useful as local smoke tests.
