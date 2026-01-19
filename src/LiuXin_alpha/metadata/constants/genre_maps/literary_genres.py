# Leaf mapping for the Literary & General Fiction branch only.
# Run this ONLY if you've already classified the work as Literary/General Fiction.
# Most-specific-first.

LITERARY_LEAF_MAPPING = {
    # --- Distinct traditions / labels ---
    "Magical Realism": (
        r"\bmagical\s+realism\b",
        r"\bmagic\s+realism\b",
    ),
    "Metafiction": (
        r"\bmetafiction\b",
        r"\bmeta\s+fiction\b",
        r"\bself[- ]referential\b",
    ),
    "Absurdist Fiction": (
        r"\babsurd(?:ist|ism)\b",
        r"\btheatre\s+of\s+the\s+absurd\b",
    ),
    "Experimental Fiction": (
        r"\bexperimental\s+fiction\b",
        r"\bavant[- ]garde\b",
    ),
    "Campus Novel": (
        r"\bcampus\s+novel\b",
        r"\bacademic\s+satire\b",
        r"\buniversity\s+novel\b",
    ),
    "Book Club Fiction": (
        r"\bbook\s+club\b",
        r"\bbook[- ]club\s+fiction\b",
        r"\bupmarket\b",
        r"\bup[- ]market\b",
    ),

    # --- Gothic variants (lit rather than horror, when shelved that way) ---
    "Southern Gothic": (
        r"\bsouthern\s+gothic\b",
    ),
    "Gothic Fiction": (
        r"\bgothic\s+fiction\b",
        r"\bgothic\b",
    ),

    # --- Common shelf buckets ---
    "Women's Fiction": (
        r"\bwomen(?:'s)?\s+fiction\b",
        r"\bchick\s+lit\b",
    ),
    "Coming-of-Age": (
        r"\bcoming\s*[- ]?of\s*[- ]?age\b",
        r"\bbildungsroman\b",
    ),
    "Family Saga": (
        r"\bfamily\s+saga\b",
        r"\bgenerational\s+saga\b",
        r"\bmulti[- ]generational\b",
    ),
    "Domestic Fiction": (
        r"\bdomestic\s+fiction\b",
        r"\bfamily\s+drama\b",
        r"\bmarriage\b",
        r"\brelationships?\b",
    ),
    "Slice of Life": (
        r"\bslice\s+of\s+life\b",
        r"\beveryday\s+life\b",
    ),
    "Social Realism": (
        r"\bsocial\s+realism\b",
        r"\bsocially\s+conscious\b",
    ),

    # --- Tone / mode ---
    "Humor": (
        r"\bhumou?r\b",
        r"\bcomic\s+fiction\b",
        r"\bfunny\b",
    ),
    "Satire": (
        r"\bsatire\b",
        r"\bsatirical\b",
    ),

    # --- Period / time (if you want to keep these in lit rather than a separate historical branch) ---
    "Historical Fiction": (
        r"\bhistorical\s+fiction\b",
        r"\bperiod\s+fiction\b",
    ),
    "Contemporary Fiction": (
        r"\bcontemporary\s+fiction\b",
        r"\bmodern\s+fiction\b",
    ),

    # --- Short-form containers (format-ish but extremely common) ---
    "Short Stories": (
        r"\bshort\s+stories\b",
        r"\bshort\s+fiction\b",
    ),
    "Anthology": (
        r"\banthology\b",
        r"\bcollection\b",
        r"\bcollected\s+stories\b",
        r"\bshort\s+story\s+collection\b",
    ),
    "Novella": (
        r"\bnovella\b",
        r"\bshort\s+novel\b",
    ),

    # --- Catch-all within branch ---
    "Literary Fiction": (
        r"\bliterary\s+fiction\b",
        r"\blit\s*fic\b",
        r"\blitfic\b",
    ),
    "Fiction": (
        r"\bfiction\b",
        r"\bnovel\b",
    ),
}
