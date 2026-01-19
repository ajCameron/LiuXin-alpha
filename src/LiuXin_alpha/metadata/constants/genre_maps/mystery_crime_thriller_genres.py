# Leaf mapping for the combined Mystery / Crime / Thriller branch.
# Run this ONLY after you've classified the top-level branch as Mystery/Crime/Thriller.
# Most-specific-first (so e.g. "Legal Thriller" beats "Thriller").

MYSTERY_CRIME_THRILLER_LEAF_MAPPING = {
    # --- Cozy & structured mystery forms ---
    "Cozy Mystery": (
        r"\bcozy\s+mystery\b",
        r"\bcosy\s+mystery\b",
        r"\btea\s+shop\s+mystery\b",
        r"\bcraft\s+mystery\b",
    ),
    "Locked Room Mystery": (
        r"\blocked\s+room\b",
        r"\bclosed\s+circle\b",
        r"\bimpossible\s+crime\b",
    ),
    "Whodunit": (
        r"\bwhodunit\b",
        r"\bwho\s*dunnit\b",
    ),

    # --- Procedural / investigative ---
    "Police Procedural": (
        r"\bpolice\s+procedural\b",
        r"\bcrime\s+procedural\b",
        r"\bprocedural\b",
        r"\bdetective\s+unit\b",
    ),
    "Detective": (
        r"\bdetective\b",
        r"\bprivate\s+eye\b",
        r"\bpi\b",
        r"\bprivate\s+investigator\b",
    ),

    # --- Noir family ---
    "Noir": (
        r"\bnoir\b",
        r"\bneo\s*[- ]?noir\b",
    ),
    "Hardboiled": (
        r"\bhard\s*[- ]?boiled\b",
        r"\bhardboiled\b",
    ),

    # --- Thieves / capers ---
    "Heist": (
        r"\bheist\b",
        r"\bcaper\b",
        r"\bscore\b",
        r"\bbank\s+job\b",
    ),

    # --- Thriller specialisms ---
    "Legal Thriller": (
        r"\blegal\s+thriller\b",
        r"\bcourtroom\s+thriller\b",
        r"\blawyer\s+thriller\b",
    ),
    "Political Thriller": (
        r"\bpolitical\s+thriller\b",
        r"\bstate\s+secrets?\b",
        r"\bconspiracy\b",
    ),
    "Espionage Thriller": (
        r"\bespionage\b",
        r"\bspy\s+thriller\b",
        r"\bspies\b",
        r"\bintelligence\s+agency\b",
        r"\bcia\b",
        r"\bmi6\b",
    ),
    "Techno-thriller": (
        r"\btechno\s*[- ]?thriller\b",
        r"\btechnothriller\b",
        r"\bcyber\s*[- ]?thriller\b",
    ),
    "Medical Thriller": (
        r"\bmedical\s+thriller\b",
        r"\bhospital\s+thriller\b",
        r"\bbiomedical\s+thriller\b",
    ),
    "Domestic Thriller": (
        r"\bdomestic\s+thriller\b",
        r"\bmarriage\s+thriller\b",
        r"\bfamily\s+secrets?\b",
    ),
    "Psychological Thriller": (
        r"\bpsychological\s+thriller\b",
        r"\bpsycho\s+thriller\b",
        r"\bunreliable\s+narrator\b",
        r"\bgaslight(?:ing)?\b",
        r"\bcat\s+and\s+mouse\b",
    ),
    "Action Thriller": (
        r"\baction\s+thriller\b",
        r"\bhigh\s+stakes\b",
        r"\bmanhunt\b",
    ),

    # --- Crime flavours ---
    "Organized Crime": (
        r"\borganized\s+crime\b",
        r"\bmafia\b",
        r"\bgodfather\b",
        r"\bgangster(?:s)?\b",
        r"\bcartel\b",
    ),
    "Serial Killer": (
        r"\bserial\s+killer(?:s)?\b",
        r"\bcopycat\s+killer\b",
    ),
    "True Crime": (
        r"\btrue\s+crime\b",
        r"\bnonfiction\s+crime\b",
        r"\bcasefile\b",
    ),

    # --- Broad umbrellas (late) ---
    "Thriller": (
        r"\bthriller\b",
        r"\bsuspense\b",
    ),
    "Mystery": (
        r"\bmystery\b",
        r"\binvestigation\b",
        r"\bcase\b",
    ),
    "Crime": (
        r"\bcrime\b",
        r"\bcriminal\b",
        r"\bmurder\b",
        r"\btheft\b",
    ),
}
