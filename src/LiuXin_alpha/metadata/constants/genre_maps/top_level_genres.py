# Fiction-only genre mapping (most-specific-first).
# Assumes input has been normalized: lowercase, accents stripped, separators -> spaces.
# Compile with re.IGNORECASE|re.UNICODE if you don't normalize first.

FICTION_GENRE_MAPPING = {
    # =========================
    # Romance (specific first)
    # =========================
    "Romantic Suspense": (
        r"\bromantic\s+suspense\b",
        r"\brom\s+suspense\b",
    ),
    "Romantic Comedy": (
        r"\brom\s*[- ]?com\b",
        r"\bromcom\b",
        r"\bromantic\s+comedy\b",
    ),
    "Sports Romance": (
        r"\bsports?\s+romance\b",
    ),
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
    "Paranormal Romance": (
        r"\bparanormal\s+romance\b",
        r"\bpnr\b",
    ),
    "Fantasy Romance": (
        r"\bfantasy\s+romance\b",
        r"\bromantasy\b",
    ),
    "Science Fiction Romance": (
        r"\bscience\s+fiction\s+romance\b",
        r"\bsci\s*[- ]?fi\s+romance\b",
        r"\bsf\s+romance\b",
    ),
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
    ),
    "Dark Romance": (
        r"\bdark\s+romance\b",
    ),
    "Erotica": (
        r"\berotica\b",
        r"\berotic\s+fiction\b",
        r"\badult\s+erotic\b",
    ),
    "Romance": (
        r"\bromance\b",
        r"\blove\s+story\b",
    ),

    # =========================
    # Mystery / Crime / Thriller
    # =========================
    "Cozy Mystery": (
        r"\bcozy\s+mystery\b",
        r"\bcosy\s+mystery\b",
    ),
    "Locked Room Mystery": (
        r"\blocked\s+room\b",
        r"\bclosed\s+circle\b",
    ),
    "Police Procedural": (
        r"\bpolice\s+procedural\b",
        r"\bcrime\s+procedural\b",
    ),
    "Noir": (
        r"\bnoir\b",
        r"\bneo\s*[- ]?noir\b",
    ),
    "Hardboiled": (
        r"\bhard\s*[- ]?boiled\b",
        r"\bhardboiled\b",
    ),
    "Heist": (
        r"\bheist\b",
        r"\bcaper\b",
    ),
    "Detective": (
        r"\bdetective\b",
        r"\bprivate\s+eye\b",
        r"\bpi\b",
    ),
    "Psychological Thriller": (
        r"\bpsychological\s+thriller\b",
        r"\bpsycho\s+thriller\b",
    ),
    "Legal Thriller": (
        r"\blegal\s+thriller\b",
        r"\bcourtroom\s+thriller\b",
    ),
    "Political Thriller": (
        r"\bpolitical\s+thriller\b",
    ),
    "Espionage Thriller": (
        r"\bespionage\b",
        r"\bspy\s+thriller\b",
        r"\bspies\b",
    ),
    "Techno-thriller": (
        r"\btechno\s*[- ]?thriller\b",
        r"\btechnothriller\b",
    ),
    "Medical Thriller": (
        r"\bmedical\s+thriller\b",
    ),
    "Domestic Thriller": (
        r"\bdomestic\s+thriller\b",
    ),
    "Action Thriller": (
        r"\baction\s+thriller\b",
    ),
    "Thriller": (
        r"\bthriller\b",
    ),
    "Mystery": (
        r"\bmystery\b",
        r"\bwhodunit\b",
        r"\bwho\s*dunnit\b",
    ),
    "Crime Fiction": (
        r"\bcrime\s+fiction\b",
        r"\bcriminal\b",
    ),

    # =========================
    # Horror
    # =========================
    "Cosmic Horror": (
        r"\bcosmic\s+horror\b",
        r"\blovecraft(?:ian)?\b",
        r"\beldritch\b",
    ),
    "Folk Horror": (
        r"\bfolk\s+horror\b",
        r"\bpagan\s+horror\b",
    ),
    "Gothic Horror": (
        r"\bgothic\s+horror\b",
    ),
    "Body Horror": (
        r"\bbody\s+horror\b",
    ),
    "Supernatural Horror": (
        r"\bsupernatural\s+horror\b",
        r"\bparanormal\s+horror\b",
    ),
    "Haunted House": (
        r"\bhaunted\s+house\b",
        r"\bghost\s+house\b",
    ),
    "Slasher": (
        r"\bslasher\b",
        r"\bserial\s+killer\s+horror\b",
    ),
    "Horror": (
        r"\bhorror\b",
    ),

    # =========================
    # Science Fiction & adjacent
    # =========================
    "Military Science Fiction": (
        r"\bmilitary\s+science\s+fiction\b",
        r"\bmil(?:\.|itary)?\s*s(?:ci)?\s*[- ]?fi\b",
        r"\bmilitary\s+sf\b",
        r"\bmil\s+sf\b",
    ),
    "Hard Science Fiction": (
        r"\bhard\s+s(?:ci)?\s*[- ]?fi\b",
        r"\bhard\s+science\s+fiction\b",
        r"\bhard\s+sf\b",
    ),
    "Soft Science Fiction": (
        r"\bsoft\s+s(?:ci)?\s*[- ]?fi\b",
        r"\bsoft\s+science\s+fiction\b",
    ),
    "Space Opera": (
        r"\bspace\s*[- ]?opera\b",
        r"\bspace\s+epic\b",
    ),
    "Space Western": (
        r"\bspace\s+western\b",
    ),
    "Planetary Romance": (
        r"\bplanetary\s+romance\b",
        r"\bsword\s+and\s+planet\b",
    ),
    "First Contact": (
        r"\bfirst\s+contact\b",
        r"\bcontact\s+(with\s+)?aliens\b",
    ),
    "Alien Invasion": (
        r"\balien\s+invasion\b",
        r"\binvasion\s+of\s+earth\b",
    ),
    "Time Travel": (
        r"\btime\s*[- ]?travel\b",
        r"\btime\s+trav(?:el|elling|eling)\b",
        r"\btime\s+loop\b",
    ),
    "Alternate History": (
        r"\balternate\s+history\b",
        r"\balt\s+history\b",
        r"\bwhat\s+if\s+history\b",
    ),
    "Dystopian": (
        r"\bdystopi(an|a)\b",
        r"\bdystopia\b",
    ),
    "Utopian": (
        r"\butopi(an|a)\b",
        r"\butopia\b",
    ),
    "Post-Apocalyptic": (
        r"\bpost\s*[- ]?apoc(?:alyptic)?\b",
        r"\bpostapoc\b",
    ),
    "Apocalyptic": (
        r"\bapoc(?:alyptic)?\b",
        r"\bapocalypse\b",
        r"\bend\s+of\s+the\s+world\b",
    ),
    "Cyberpunk": (
        r"\bcyber\s*[- ]?punk\b",
        r"\bcyberpunk\b",
    ),
    "Steampunk": (
        r"\bsteam\s*[- ]?punk\b",
        r"\bsteampunk\b",
    ),
    "Dieselpunk": (
        r"\bdiesel\s*[- ]?punk\b",
        r"\bdieselpunk\b",
    ),
    "Biopunk": (
        r"\bbio\s*[- ]?punk\b",
        r"\bbiopunk\b",
    ),
    "Solarpunk": (
        r"\bsolar\s*[- ]?punk\b",
        r"\bsolarpunk\b",
    ),
    "Climate Fiction": (
        r"\bclimate\s*[- ]?fic(?:tion)?\b",
        r"\bcli\s*[- ]?fi\b",
        r"\beco\s*[- ]?fic(?:tion)?\b",
    ),
    "Science Fantasy": (
        r"\bscience\s+fantasy\b",
    ),
    "Speculative Fiction": (
        r"\bspec(ulative)?\s*fic(tion)?\b",
        r"\bsff\b",
    ),
    "Science Fiction": (
        r"\bscience\s*[- ]?fiction\b",
        r"\bsci\s*[- ]?fi\b",
        r"\bscifi\b",
        r"\bs\s*f\b",
        r"\bsf\b",
    ),

    # =========================
    # Fantasy
    # =========================
    "Cozy Fantasy": (
        r"\bcozy\s+fantasy\b",
        r"\bcosy\s+fantasy\b",
    ),
    "Urban Fantasy": (
        r"\burban\s+fantasy\b",
        r"\buf\b",
    ),
    "Contemporary Fantasy": (
        r"\bcontemporary\s+fantasy\b",
        r"\bmodern\s+fantasy\b",
    ),
    "Historical Fantasy": (
        r"\bhistorical\s+fantasy\b",
        r"\bperiod\s+fantasy\b",
    ),
    "Dark Fantasy": (
        r"\bdark\s+fantasy\b",
        r"\bgrim\s+fantasy\b",
    ),
    "Grimdark": (
        r"\bgrim\s*[- ]?dark\b",
        r"\bgrimdark\b",
    ),
    "Epic Fantasy": (
        r"\bepic\s+fantasy\b",
        r"\bfantasy\s+epic\b",
    ),
    "High Fantasy": (
        r"\bhigh\s+fantasy\b",
        r"\bh\.\s*fan\b",
    ),
    "Low Fantasy": (
        r"\blow\s+fantasy\b",
    ),
    "Sword and Sorcery": (
        r"\bsword\s+and\s+sorcery\b",
        r"\bswords\s+and\s+sorcery\b",
        r"\bs\s*&\s*s\b",
        r"\bs&s\b",
    ),
    "Portal Fantasy": (
        r"\bportal\s+fantasy\b",
        r"\bother\s+world\b",
        r"\bisekai\b",
    ),
    "Mythic Fantasy": (
        r"\bmythic\s+fantasy\b",
        r"\bmyth(?:ology|ological)\s+fantasy\b",
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
    ),
    "Fantasy": (
        r"\bfantasy\b",
        r"\bfant?a?s?y?\b",
    ),

    # =========================
    # Literary / General Fiction
    # =========================
    "Magical Realism": (
        r"\bmagical\s+realism\b",
        r"\bmagic\s+realism\b",
    ),
    "Southern Gothic": (
        r"\bsouthern\s+gothic\b",
    ),
    "Gothic Fiction": (
        r"\bgothic\s+fiction\b",
        r"\bgothic\b",
    ),
    "Literary Fiction": (
        r"\bliterary\s+fiction\b",
        r"\blit\s*fic\b",
        r"\blitfic\b",
    ),
    "Historical Fiction": (
        r"\bhistorical\s+fiction\b",
        r"\bperiod\s+fiction\b",
    ),
    "Contemporary Fiction": (
        r"\bcontemporary\s+fiction\b",
        r"\bmodern\s+fiction\b",
    ),
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
    ),
    "Realistic Fiction": (
        r"\brealistic\s+fiction\b",
        r"\brf\b",
    ),
    "Humor": (
        r"\bhumou?r\b",
        r"\bcomic\s+fiction\b",
    ),
    "Satire": (
        r"\bsatire\b",
        r"\bsatirical\b",
    ),
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
    ),
    "Fiction": (
        r"\bfiction\b",
        r"\bnovel\b",
    ),

    # =========================
    # Adventure / Action / War / Western
    # =========================
    "War Fiction": (
        r"\bwar\s+fiction\b",
        r"\bmilitary\s+fiction\b",
    ),
    "Western": (
        r"\bwestern\b",
        r"\bwild\s+west\b",
    ),
    "Adventure": (
        r"\badventure\b",
        r"\bquest\b",
        r"\bexpedition\b",
    ),
    "Action": (
        r"\baction\b",
    ),
}
