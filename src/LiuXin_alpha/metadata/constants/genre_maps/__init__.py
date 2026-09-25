

"""
Provide compact spelling and abbreviation patterns for a small set of genre labels.

This package-level mapping is separate from the larger branch-specific tables and is
not an export of the active standardize_genre classifier.

Map canonical labels to tuples of uncompiled regular-expression strings. Consumers
choose regex flags, normalization, and first-match or multi-match policy; importing
the module performs no classification.

Example:
    >>> import re
    >>> any(re.search(pattern, 'sci fi', re.IGNORECASE) for pattern in GENRE_SHORTENED_MAPPING['Science Fiction']) is not False
    True
"""
GENRE_SHORTENED_MAPPING = {
    "Science Fiction": (r"science ?fiction", r"sci ?fi", "s ?f"),
    "Fantasy": (r"fant?a?s?y?",),
    "High Fantasy": (r"h. ?fan", r"high ?fantasy"),
    "Military Science Fiction": (
        r"military science fiction",
        r"mil.? ?s ? f",
        r"military ?sf",
    ),
    "Realistic Fiction": (r"rf", r"realistic ?fiction"),
    "Urban Fantasy": (r"urban ?fantasy", r"uf"),
}
