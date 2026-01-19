# Leaf mapping for the Romance branch only.
# Run this ONLY if you've already classified the work as "Romance".
# Most-specific-first.

ROMANCE_LEAF_MAPPING = {
    # --- Cross-genre romance (specific) ---
    "Romantic Suspense": (
        r"\bromantic\s+suspense\b",
        r"\brom\s+suspense\b",
    ),
    "Paranormal Romance": (
        r"\bparanormal\s+romance\b",
        r"\bpnr\b",
        r"\bvampire\s+romance\b",
        r"\bwerewolf\s+romance\b",
        r"\bshifter\s+romance\b",
    ),
    "Fantasy Romance": (
        r"\bfantasy\s+romance\b",
        r"\bromantasy\b",
        r"\bfae\s+romance\b",
    ),
    "Science Fiction Romance": (
        r"\bscience\s+fiction\s+romance\b",
        r"\bsci\s*[- ]?fi\s+romance\b",
        r"\bsf\s+romance\b",
        r"\balien\s+romance\b",
    ),

    # --- Sub-market labels / tones ---
    "Dark Romance": (
        r"\bdark\s+romance\b",
    ),
    "Romantic Comedy": (
        r"\brom\s*[- ]?com\b",
        r"\bromcom\b",
        r"\bromantic\s+comedy\b",
    ),

    # --- Period / setting ---
    "Regency Romance": (
        r"\bregency\s+romance\b",
        r"\bregency\b",
    ),
    "Historical Romance": (
        r"\bhistorical\s+romance\b",
        r"\bperiod\s+romance\b",
    ),
    "Contemporary Romance": (
        r"\bcontemporary\s+romance\b",
        r"\bmodern\s+romance\b",
    ),

    # --- Audience / identity / pairing labels ---
    "LGBTQ+ Romance": (
        r"\blgbtq\+?\s+romance\b",
        r"\bqueer\s+romance\b",
        r"\bgay\s+romance\b",
        r"\blesbian\s+romance\b",
        r"\bmm\s+romance\b",
        r"\bff\s+romance\b",
    ),
    "New Adult Romance": (
        r"\bnew\s+adult\s+romance\b",
        r"\bna\s+romance\b",
        r"\bnew\s+adult\b",
    ),
    "Sports Romance": (
        r"\bsports?\s+romance\b",
        r"\bathlete\s+romance\b",
    ),

    # --- Heat level / explicitness ---
    "Erotica": (
        r"\berotica\b",
        r"\berotic\s+romance\b",
        r"\badult\s+erotic\b",
        r"\bexplicit\b",
    ),

    # --- Catch-all ---
    "Romance": (
        r"\bromance\b",
        r"\blove\s+story\b",
        r"\bromantic\b",
    ),
}
