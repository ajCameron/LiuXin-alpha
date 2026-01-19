# Leaf mapping for the Fantasy branch only.
# Run this ONLY if you've already classified the work as "Fantasy".
# Most-specific-first.

FANTASY_LEAF_MAPPING = {
    # --- Very specific modern shelf labels ---
    "Romantasy": (
        r"\bromantasy\b",
        r"\bfantasy\s+romance\b",
    ),
    "Cozy Fantasy": (
        r"\bcozy\s+fantasy\b",
        r"\bcosy\s+fantasy\b",
        r"\bcomfort\s+fantasy\b",
    ),
    "Grimdark": (
        r"\bgrim\s*[- ]?dark\b",
        r"\bgrimdark\b",
    ),
    "Dark Fantasy": (
        r"\bdark\s+fantasy\b",
        r"\bgrim\s+fantasy\b",
    ),

    # --- Setting / era / aesthetic ---
    "Gaslamp Fantasy": (
        r"\bgaslamp\s+fantasy\b",
        r"\bvictorian\s+fantasy\b",
    ),
    "Mythic Fantasy": (
        r"\bmythic\s+fantasy\b",
        r"\bmyth(?:ology|ological)\s+fantasy\b",
        r"\bmyth\s+retelling\b",
        r"\bretelling\b",  # branch-only; still a bit broad
    ),
    "Fairy Tale": (
        r"\bfairy\s*[- ]?tale\b",
        r"\bfaerie\s+tale\b",
        r"\bfairytale\b",
    ),
    "Arthurian": (
        r"\barthuri(an)?\b",
        r"\bking\s+arthur\b",
        r"\bcamelot\b",
        r"\bround\s+table\b",
    ),

    # --- Subgenre clusters ---
    "Urban Fantasy": (
        r"\burban\s+fantasy\b",
        r"\buf\b",
        r"\bmodern\s+magic\b",
    ),
    "Contemporary Fantasy": (
        r"\bcontemporary\s+fantasy\b",
        r"\bmodern\s+fantasy\b",
    ),
    "Historical Fantasy": (
        r"\bhistorical\s+fantasy\b",
        r"\bperiod\s+fantasy\b",
    ),
    "Low Fantasy": (
        r"\blow\s+fantasy\b",
        r"\bgrounded\s+fantasy\b",
    ),
    "High Fantasy": (
        r"\bhigh\s+fantasy\b",
        r"\bh\.\s*fan\b",
        r"\bsecondary\s+world\b",
    ),
    "Epic Fantasy": (
        r"\bepic\s+fantasy\b",
        r"\bfantasy\s+epic\b",
        r"\bworld\s*[- ]?spanning\b",
    ),

    # --- Adventure flavours ---
    "Sword and Sorcery": (
        r"\bsword\s+and\s+sorcery\b",
        r"\bswords\s+and\s+sorcery\b",
        r"\bs\s*&\s*s\b",
        r"\bs&s\b",
    ),
    "Heroic Fantasy": (
        r"\bheroic\s+fantasy\b",
        r"\bchosen\s+one\b",
    ),

    # --- Isekai / portals / progression ---
    "Portal Fantasy": (
        r"\bportal\s+fantasy\b",
        r"\bother\s+world\b",
        r"\bisekai\b",
    ),
    "LitRPG": (
        r"\blit\s*rpg\b",
        r"\blitrpg\b",
    ),
    "Progression Fantasy": (
        r"\bprogression\s+fantasy\b",
        r"\bpower\s+progression\b",
        r"\bcultivation\b",
        r"\bxianxia\b",
        r"\bxuanhuan\b",
    ),

    # --- Magical modes / vibes (borderline taggy, but common fantasy buckets) ---
    "Witchy Fantasy": (
        r"\bwitch(?:es)?\b",
        r"\bwitchcraft\b",
        r"\bcoven\b",
    ),
    "Necromancy": (
        r"\bnecroman(?:cy|cer|cers)\b",
        r"\blich(?:es)?\b",
        r"\brais(?:e|ing)\s+the\s+dead\b",
    ),
    "Dragon Fantasy": (
        r"\bdragon(?:s)?\b",
        r"\bwyrm(?:s)?\b",
        r"\bdrake(?:s)?\b",
    ),

    # --- Paranormal / creatures (kept in fantasy branch; horror overlap is fine) ---
    "Vampire Fantasy": (
        r"\bvampire(?:s)?\b",
        r"\bvampiric\b",
        r"\bdracula\b",
    ),
    "Werewolf Fantasy": (
        r"\bwerewolf(?:s)?\b",
        r"\blycan(?:thrope)?s?\b",
        r"\blycanthropy\b",
    ),
    "Faerie / Fae": (
        r"\bfae\b",
        r"\bfaerie(?:s)?\b",
        r"\bfairy(?:ies)?\b",
        r"\bfair\s*folk\b",
    ),

    # --- Cross-genre adjacency ---
    "Science Fantasy": (
        r"\bscience\s+fantasy\b",
        r"\bspace\s+fantasy\b",
    ),
    "Mythpunk": (
        r"\bmythpunk\b",
    ),
    "New Weird": (
        r"\bnew\s+weird\b",
    ),

    # --- Catch-alls for the branch ---
    "Fantasy": (
        r"\bfantasy\b",
        r"\bfant?a?s?y?\b",
    ),
}
