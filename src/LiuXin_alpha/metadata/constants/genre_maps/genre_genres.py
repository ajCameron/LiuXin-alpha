# Leaf mapping for "Genre / Commercial Fiction" branch.
# Run this ONLY if you've classified the work as Genre Fiction (general commercial).
# Most-specific-first.

GENRE_FICTION_LEAF_MAPPING = {
    # --- War / military (fiction) ---
    "War Fiction": (
        r"\bwar\s+fiction\b",
        r"\bmilitary\s+fiction\b",
        r"\bcombat\b",
        r"\bbattlefield\b",
    ),
    "Spy Fiction": (
        r"\bspy\s+fiction\b",
        r"\bespionage\b",
        r"\bsecret\s+agent\b",
    ),

    # --- Western variants ---
    "Western Romance": (
        r"\bwestern\s+romance\b",
    ),
    "Western": (
        r"\bwestern\b",
        r"\bwild\s+west\b",
        r"\bcowboy\b",
        r"\bfrontier\b",
        r"\bgunfighter\b",
    ),

    # --- Adventure / action core ---
    "Swashbuckling": (
        r"\bswashbuckl(?:e|ing)\b",
        r"\bpirate(?:s)?\b",
        r"\bprivateer\b",
        r"\bhigh\s+seas\b",
    ),
    "Treasure Hunt": (
        r"\btreasure\s+hunt\b",
        r"\blost\s+treasure\b",
        r"\bancient\s+treasure\b",
    ),
    "Survival Adventure": (
        r"\bsurvival\b",
        r"\bstranded\b",
        r"\bwilderness\b",
        r"\bcastaway\b",
    ),
    "Historical Adventure": (
        r"\bhistorical\s+adventure\b",
        r"\bperiod\s+adventure\b",
    ),
    "Adventure": (
        r"\badventure\b",
        r"\bquest\b",
        r"\bexpedition\b",
        r"\bjourney\b",
    ),
    "Action": (
        r"\baction\b",
        r"\bhigh\s+stakes\b",
        r"\bfast[- ]paced\b",
    ),

    # --- Humorous commercial fiction (distinct from lit satire) ---
    "Comic Fiction": (
        r"\bcomic\s+fiction\b",
        r"\bhumou?r\b",
        r"\bhilarious\b",
        r"\bfunny\b",
    ),

    # --- Historical (commercial shelf label) ---
    "Historical Fiction": (
        r"\bhistorical\s+fiction\b",
        r"\bperiod\s+fiction\b",
    ),

    # --- General commercial buckets ---
    "General Fiction": (
        r"\bgeneral\s+fiction\b",
        r"\bmainstream\s+fiction\b",
        r"\bcommercial\s+fiction\b",
    ),

    # --- Catch-all inside branch ---
    "Fiction": (
        r"\bfiction\b",
        r"\bnovel\b",
    ),
}
