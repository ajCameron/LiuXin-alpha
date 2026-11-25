# Keywords

A document's keywords arrive as one string, and formats and tools join them
differently: "science fiction, space opera", "scifi spaceopera",
"spaceopera,military scifi history political". LiuXin splits them into tags in
`src/LiuXin_alpha/metadata/file_sources/keywords.py` (#222). The DOCX reader uses it
for `cp:keywords`; the other readers keep their own splitting for now.

## The setting

The `keyword_splitting` preference chooses:

| Value | Splitting |
| --- | --- |
| `smart` (default) | At commas and semicolons, then against the library's known tags (below). With no known tags, the same as `separators`. |
| `separators` | LiuXin's earlier rule: at commas and semicolons when there are any, so a tag may hold spaces; otherwise on spaces. |
| `calibre` | calibre's rule: on spaces and on commas, always, so "science fiction" becomes two tags. |

## Smart splitting

What is left after the separators is matched against the tags the library already
knows:

- **Matching ignores case, spaces and punctuation.** A piece that matches a known
  tag takes that tag's spelling: "spaceopera" gives "space opera", "SciFi" gives
  "sci-fi".
- **Runs of words are cut into known tags.** The cut covers as many words as
  possible with known tags, and uses the fewest tags on a tie, so "science fiction"
  stays one tag even when "science" and "fiction" are also known. "military scifi
  history political" gives "military sci-fi", "history" and "political" when those
  are known.
- **Unknown words.** Where the keywords had separators, a run of words no known tag
  covers stays one tag. Where they had none, unknown words are split on spaces, as
  before.
- **A piece between separators is cut only when known tags cover more than half its
  words.** So "hard boiled detective" stays one tag although "detective" is known,
  and "epic fantasy" stays one tag although "fantasy" is known.
- **Repeats are dropped,** ignoring case, so "Sci-Fi, scifi" gives one tag.

Where several known tags match the same words ("Sci-Fi" and "SciFi"), the first one
given keeps its spelling, so a caller can list the most used first.

## Where the known tags come from

The metadata layer cannot read the database, so whoever reads a file for a library
says which tags it knows:

```python
from LiuXin_alpha.metadata.file_sources.keywords import using_known_tags

with using_known_tags(lambda: library_tag_names()):
    metadata = get_metadata(path)
```

A callable is called at most once in the scope, and only if a reader needs the tags,
so files without keywords cost no query. Scopes nest, and each ends cleanly after an
error. Outside any scope no tags are known, and `smart` splits as `separators` does.

Nothing fills the scope yet: book ingest registers files without reading their
embedded metadata. The catalogue's add-from-file work should wrap its reads in
`using_known_tags`, giving the library's tag names.

## Checks

`tests/metadata/file_sources/test_keyword_splitting.py` covers:

- the cases above;
- `smart` with no known tags against LiuXin's earlier rule, on 500 random strings;
- `calibre` against calibre's `read_doc_props` loop, on 500 random strings;
- the preference, the scopes and the lazy source;
- the DOCX reader under each setting, `_-_` included.
