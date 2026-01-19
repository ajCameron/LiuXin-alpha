# Substances / creatures / motifs bucket (separate from genre + region).
# Intended to tag content themes: vampires, witches, dragons, alchemy, etc.

SUBSTANCE_BUCKET_MAPPING = {
    # --- Undead / Vampires / Zombies ---
    "Vampires": (
        r"\bvampire(?:s)?\b",
        r"\bdracula\b",
        r"\bvampiric\b",
        r"\bnosferatu\b",
    ),
    "Zombies": (
        r"\bzombie(?:s)?\b",
        r"\bundead\b",
        r"\bwalking\s+dead\b",
    ),
    "Ghosts": (
        r"\bghost(?:s)?\b",
        r"\bhaunting\b",
        r"\bhaunted\b",
        r"\bpoltergeist\b",
        r"\bspecter\b",
        r"\bspectre\b",
        r"\bwraith\b",
    ),

    # --- Shapeshifters / Werewolves ---
    "Werewolves": (
        r"\bwerewolf(?:s)?\b",
        r"\blycan(?:thrope)?s?\b",
        r"\blycanthropy\b",
    ),
    "Shapeshifters": (
        r"\bshape\s*[- ]?shifter(?:s)?\b",
        r"\bshapeshifter(?:s)?\b",
        r"\bchangeling(?:s)?\b",
        r"\bdoppelg[aä]nger(?:s)?\b",
    ),

    # --- Witches / Magic practice ---
    "Witches": (
        r"\bwitch(?:es)?\b",
        r"\bwitchcraft\b",
        r"\bwitchery\b",
        r"\bcoven\b",
    ),
    "Wizards": (
        r"\bwizard(?:s)?\b",
        r"\bsorcerer(?:s)?\b",
        r"\bsorceress(?:es)?\b",
        r"\bmage(?:s)?\b",
        r"\bmagician(?:s)?\b",
    ),
    "Necromancy": (
        r"\bnecroman(?:cy|cer|cers)\b",
        r"\brais(?:e|ing)\s+the\s+dead\b",
    ),
    "Demonology": (
        r"\bdemon(?:s)?\b",
        r"\bdemonology\b",
        r"\binfernal\b",
        r"\bhellspawn\b",
    ),
    "Angels": (
        r"\bangel(?:s)?\b",
        r"\barchangel(?:s)?\b",
        r"\bseraph(?:im)?\b",
    ),
    "Faeries": (
        r"\bfae\b",
        r"\bfaerie(?:s)?\b",
        r"\bfairy(?:ies)?\b",
        r"\bfair\s*folk\b",
    ),

    # --- Myth / Monsters ---
    "Dragons": (
        r"\bdragon(?:s)?\b",
        r"\bwyrm(?:s)?\b",
        r"\bdrake(?:s)?\b",
    ),
    "Giants": (
        r"\bgiant(?:s)?\b",
        r"\btitan(?:s)?\b",
    ),
    "Goblins": (
        r"\bgoblin(?:s)?\b",
        r"\bhobgoblin(?:s)?\b",
    ),
    "Orcs": (
        r"\borc(?:s)?\b",
        r"\borgre(?:s)?\b",
    ),
    "Elves": (
        r"\belf(?:s)?\b",
        r"\belven\b",
    ),
    "Dwarves": (
        r"\bdwarf(?:ves)?\b",
        r"\bdwarven\b",
    ),
    "Merfolk": (
        r"\bmermaid(?:s)?\b",
        r"\bmerman\b",
        r"\bmerfolk\b",
        r"\bsiren(?:s)?\b",
    ),
    "Sea Monsters": (
        r"\bkraken\b",
        r"\bleviathan\b",
        r"\bsea\s+monster(?:s)?\b",
    ),

    # --- Horror-adjacent entities ---
    "Lovecraftian": (
        r"\blovecraft(?:ian)?\b",
        r"\beldritch\b",
        r"\bcthulhu\b",
        r"\bancient\s+ones\b",
    ),

    # --- Crime / violence motifs (careful: can be too broad) ---
    "Serial Killers": (
        r"\bserial\s+killer(?:s)?\b",
        r"\bmurderer(?:s)?\b",
    ),
    "Assassins": (
        r"\bassassin(?:s)?\b",
        r"\bhitman\b",
        r"\bcontract\s+killer\b",
    ),

    # --- Tech / sci motifs ---
    "Artificial Intelligence": (
        r"\bartificial\s+intelligence\b",
        r"\bai\b",
        r"\bsentient\s+machine(?:s)?\b",
    ),
    "Robots": (
        r"\brobot(?:s)?\b",
        r"\bandroid(?:s)?\b",
        r"\bautomaton(?:s)?\b",
        r"\bmech(?:s)?\b",
    ),
    "Cybernetics": (
        r"\bcybernetics?\b",
        r"\bcyborg(?:s)?\b",
        r"\baugment(?:ation|ed)\b",
    ),
    "Virtual Reality": (
        r"\bvirtual\s+reality\b",
        r"\bvr\b",
        r"\bsimulation\b",
        r"\bthe\s+matrix\b",
    ),

    # --- Bio / plague / infection ---
    "Plagues": (
        r"\bplague(?:s)?\b",
        r"\bpandemic(?:s)?\b",
        r"\bepidemic(?:s)?\b",
    ),
    "Viruses": (
        r"\bvirus(?:es)?\b",
        r"\bviral\b",
        r"\boutbreak\b",
        r"\binfection\b",
    ),
    "Bioweapons": (
        r"\bbio\s*[- ]?weapon(?:s)?\b",
        r"\bbioweapon(?:s)?\b",
        r"\bweaponized\s+virus\b",
    ),

    # --- Drugs / intoxication (kept descriptive, non-instructional) ---
    "Addiction": (
        r"\baddiction\b",
        r"\bsubstance\s+abuse\b",
        r"\bdependency\b",
    ),
    "Alcohol": (
        r"\balcohol\b",
        r"\bbooze\b",
        r"\bdrunk\b",
        r"\bintoxicated\b",
    ),
    "Opioids": (
        r"\bopioid(?:s)?\b",
        r"\bopiate(?:s)?\b",
        r"\bheroin\b",
        r"\bmorphine\b",
        r"\bfentanyl\b",
    ),
    "Stimulants": (
        r"\bstimulant(?:s)?\b",
        r"\bamphetamine(?:s)?\b",
        r"\bmethamphetamine\b",
        r"\bcocaine\b",
    ),
    "Psychedelics": (
        r"\bpsychedelic(?:s)?\b",
        r"\blsd\b",
        r"\bpsilocybin\b",
        r"\bmagic\s+mushroom(?:s)?\b",
    ),

    # --- Alchemy / occult-ish materials ---
    "Alchemy": (
        r"\balchemy\b",
        r"\balchemist(?:s)?\b",
        r"\bphilosopher'?s\s+stone\b",
        r"\btransmut(?:e|ation)\b",
    ),
    "Artifacts": (
        r"\bartifact(?:s)?\b",
        r"\brelic(?:s)?\b",
        r"\bancient\s+relic(?:s)?\b",
        r"\bcursed\s+object(?:s)?\b",
    ),
    "Curses": (
        r"\bcurse(?:s|d)?\b",
        r"\bhex(?:es|ed)?\b",
        r"\bjinx(?:ed)?\b",
    ),

    # --- Broad umbrellas (place late) ---
    "Supernatural": (
        r"\bsupernatural\b",
        r"\bparanormal\b",
        r"\boccult\b",
    ),
    "Magic": (
        r"\bmagic\b",
        r"\bmagical\b",
        r"\bspell(?:s|casting)?\b",
        r"\benchantment\b",
    ),
}
