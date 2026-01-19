# Leaf mapping for the Science Fiction branch only.
# Run this ONLY if you've already classified the work as "Science Fiction".
# Most-specific-first.

SCI_FI_LEAF_MAPPING = {
    # --- Punk families (very specific) ---
    "Solarpunk": (
        r"\bsolarpunk\b",
        r"\bsolar\s*[- ]?punk\b",
    ),
    "Dieselpunk": (
        r"\bdieselpunk\b",
        r"\bdiesel\s*[- ]?punk\b",
    ),
    "Steampunk": (
        r"\bsteampunk\b",
        r"\bsteam\s*[- ]?punk\b",
        r"\bclockpunk\b",
    ),
    "Biopunk": (
        r"\bbiopunk\b",
        r"\bbio\s*[- ]?punk\b",
        r"\bgenepunk\b",
    ),
    "Cyberpunk": (
        r"\bcyberpunk\b",
        r"\bcyber\s*[- ]?punk\b",
        r"\bnetrunner(?:s)?\b",
    ),

    # --- Space / scale ---
    "Space Western": (
        r"\bspace\s+western\b",
        r"\bcowboy\s+in\s+space\b",
    ),
    "Planetary Romance": (
        r"\bplanetary\s+romance\b",
        r"\bsword\s+and\s+planet\b",
    ),
    "Space Opera": (
        r"\bspace\s*[- ]?opera\b",
        r"\bspace\s+epic\b",
        r"\bgalactic\s+epic\b",
    ),
    "Military Science Fiction": (
        r"\bmilitary\s+science\s+fiction\b",
        r"\bmilitary\s+sf\b",
        r"\bmil\s+sf\b",
        r"\bmil(?:\.|itary)?\s*s(?:ci)?\s*[- ]?fi\b",
    ),

    # --- Contact / aliens ---
    "First Contact": (
        r"\bfirst\s+contact\b",
        r"\bcontact\s+(with\s+)?aliens\b",
    ),
    "Alien Invasion": (
        r"\balien\s+invasion\b",
        r"\binvasion\s+of\s+earth\b",
    ),
    "Xenofiction": (
        r"\bxeno\s*[- ]?fiction\b",
        r"\bxenofiction\b",
        r"\bnonhuman\s+protagonist\b",
    ),

    # --- Time / history ---
    "Time Travel": (
        r"\btime\s*[- ]?travel\b",
        r"\btime\s+trav(?:el|elling|eling)\b",
        r"\btime\s+loop\b",
        r"\btemporal\s+paradox\b",
    ),
    "Alternate History": (
        r"\balternate\s+history\b",
        r"\balt\s+history\b",
        r"\bwhat\s+if\s+history\b",
    ),

    # --- Future modes / society ---
    "Dystopian": (
        r"\bdystopi(an|a)\b",
        r"\bdystopia\b",
        r"\btotalitarian\b",
        r"\boppressive\s+regime\b",
    ),
    "Utopian": (
        r"\butopi(an|a)\b",
        r"\butopia\b",
    ),
    "Post-Apocalyptic": (
        r"\bpost\s*[- ]?apoc(?:alyptic)?\b",
        r"\bpostapoc\b",
        r"\bafter\s+the\s+fall\b",
    ),
    "Apocalyptic": (
        r"\bapoc(?:alyptic)?\b",
        r"\bapocalypse\b",
        r"\bend\s+of\s+the\s+world\b",
    ),

    # --- Sub-flavours of “science-iness” ---
    "Hard Science Fiction": (
        r"\bhard\s+science\s+fiction\b",
        r"\bhard\s+sf\b",
        r"\bhard\s+s(?:ci)?\s*[- ]?fi\b",
        r"\bengineering\s+focused\b",
    ),
    "Soft Science Fiction": (
        r"\bsoft\s+science\s+fiction\b",
        r"\bsoft\s+s(?:ci)?\s*[- ]?fi\b",
        r"\bsocial\s+science\s+fiction\b",
    ),

    # --- Eco / climate ---
    "Climate Fiction": (
        r"\bclimate\s*[- ]?fic(?:tion)?\b",
        r"\bcli\s*[- ]?fi\b",
        r"\beco\s*[- ]?fic(?:tion)?\b",
    ),

    # --- Tech motifs (kept as leaf-y categories; can also be tags) ---
    "Robots & AI": (
        r"\brobot(?:s)?\b",
        r"\bandroid(?:s)?\b",
        r"\bautomaton(?:s)?\b",
        r"\bcyborg(?:s)?\b",
        r"\bartificial\s+intelligence\b",
        r"\bsentient\s+machine(?:s)?\b",
    ),
    "Virtual Reality": (
        r"\bvirtual\s+reality\b",
        r"\bvr\b",
        r"\bsimulation\b",
    ),

    # --- Borderline / adjacency (still useful in SF shelves) ---
    "Science Fantasy": (
        r"\bscience\s+fantasy\b",
        r"\bspace\s+fantasy\b",
    ),
    "Slipstream": (
        r"\bslipstream\b",
        r"\bnew\s+weird\b",
    ),
    "New Weird": (
        r"\bnew\s+weird\b",
    ),

    # --- Catch-alls for the branch ---
    "Near-Future": (
        r"\bnear\s*[- ]?future\b",
        r"\bnear\s+term\s+future\b",
    ),
    "Science Fiction": (
        r"\bscience\s*[- ]?fiction\b",
        r"\bsci\s*[- ]?fi\b",
        r"\bscifi\b",
        r"\bsf\b",
        r"\bs\s*f\b",
    ),
}
