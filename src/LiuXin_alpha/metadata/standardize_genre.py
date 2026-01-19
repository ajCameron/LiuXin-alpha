from __future__ import annotations

import re
import unicodedata
from typing import Dict, Tuple, Pattern, Optional


# ---------------------------
# Normalization + compilation
# ---------------------------

_WS_RE = re.compile(r"\s+", flags=re.UNICODE)

def normalize_genre_text(text: str) -> str:
    """
    Normalize input for genre matching:
      - ensure str
      - unicode NFKD + strip combining marks (accent-insensitive)
      - lowercase
      - replace common separators with spaces
      - collapse whitespace
    """
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)

    # Normalize + strip accents
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))

    # Lower + unify separators
    text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[\/\|\.,;:\(\)\[\]\{\}]+", " ", text)

    # Collapse whitespace
    text = _WS_RE.sub(" ", text).strip()
    return text


def compile_genre_mapping(
    mapping: Dict[str, Tuple[str, ...]],
    *,
    flags: int = re.IGNORECASE | re.UNICODE,
) -> Dict[str, Tuple[Pattern[str], ...]]:
    """
    Compile all regex patterns once. Keep dict insertion order (most-specific-first).
    """
    compiled: Dict[str, Tuple[Pattern[str], ...]] = {}
    for genre, patterns in mapping.items():
        compiled[genre] = tuple(re.compile(p, flags=flags) for p in patterns)
    return compiled


def standardize_genre(
    raw: str,
    compiled_mapping: Dict[str, Tuple[Pattern[str], ...]],
    *,
    default: Optional[str] = None,
) -> Optional[str]:
    """
    First-match-wins standardizer. Assumes mapping is ordered most-specific-first.
    """
    s = normalize_genre_text(raw)
    if not s:
        return default
    for genre, patterns in compiled_mapping.items():
        for pat in patterns:
            if pat.search(s):
                return genre
    return default


# ---------------------------
# Expanded mapping (ordered)
# ---------------------------

# NOTE: Order matters. Keep more specific genres above broader umbrellas.
GENRE_SHORTENED_MAPPING: Dict[str, Tuple[str, ...]] = {
    # ========= Speculative umbrella =========
    "Speculative Fiction": (
        r"\bspec(ulative)?\s*fic(tion)?\b",
        r"\bsff\b",
        r"\bspec\s*fic\b",
    ),

    # ========= Science Fiction subgenres =========
    "Military Science Fiction": (
        r"\bmilitary\s+science\s+fiction\b",
        # Legacy abbreviations like "mil s f" should map here (not to generic SF)
        r"\bmil(?:\.|itary)?\s*s\s*f\b",
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
    "Near-Future": (
        r"\bnear\s*[- ]?future\b",
        r"\bnear\s+term\s+future\b",
    ),
    "Science Fiction": (
        r"\bscience\s*[- ]?fiction\b",
        r"\bsci\s*[- ]?fi\b",
        r"\bscifi\b",
        r"\bs\s*f\b",          # catches "s f"
        r"\bsf\b",             # safe due to word boundaries
    ),

    # ========= Fantasy subgenres =========
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

    # ========= LitRPG / progression =========
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
    "GameLit": (
        r"\bgamelit\b",
        r"\bgame\s+lit\b",
    ),

    # ========= Horror subgenres =========
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

    # ========= Mystery / Crime / Thriller =========
    "Cozy Mystery": (
        r"\bcozy\s+mystery\b",
        r"\bcosy\s+mystery\b",
    ),
    "Locked Room Mystery": (
        r"\blocked\s+room\b",
        r"\bclosed\s+circle\b",
    ),
    "Detective": (
        r"\bdetective\b",
        r"\bprivate\s+eye\b",
        r"\bpi\b",
    ),
    "Heist": (
        r"\bheist\b",
        r"\bcaper\b",
    ),
    "Noir": (
        r"\bnoir\b",
        r"\bneo\s*[- ]?noir\b",
    ),
    "Hardboiled": (
        r"\bhard\s*[- ]?boiled\b",
        r"\bhardboiled\b",
    ),
    "Police Procedural": (
        r"\bpolice\s+procedural\b",
        r"\bcrime\s+procedural\b",
    ),
    "True Crime": (
        r"\btrue\s+crime\b",
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
    "Espionage": (
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
    "Crime": (
        r"\bcrime\b",
        r"\bcriminal\b",
    ),

    # ========= Romance (expanded) =========
    "Romantic Suspense": (
        r"\bromantic\s+suspense\b",
    ),
    "Romantic Comedy": (
        r"\brom\s*[- ]?com\b",
        r"\bromcom\b",
        r"\bromantic\s+comedy\b",
    ),
    "Sports Romance": (
        r"\bsports?\s+romance\b",
    ),
    "Dark Romance": (
        r"\bdark\s+romance\b",
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
    "Paranormal Romance": (
        r"\bparanormal\s+romance\b",
        r"\bpnr\b",
    ),
    "Historical Romance": (
        r"\bhistorical\s+romance\b",
        r"\bperiod\s+romance\b",
    ),
    "Regency Romance": (
        r"\bregency\s+romance\b",
        r"\bregency\b",
    ),
    "Contemporary Romance": (
        r"\bcontemporary\s+romance\b",
        r"\bmodern\s+romance\b",
    ),
    "New Adult Romance": (
        r"\bnew\s+adult\s+romance\b",
        r"\bna\s+romance\b",
        r"\bnew\s+adult\b",
    ),
    "LGBTQ+ Romance": (
        r"\blgbtq\+?\s+romance\b",
        r"\bqueer\s+romance\b",
        r"\bgay\s+romance\b",
        r"\blesbian\s+romance\b",
        r"\bmm\s+romance\b",
        r"\bff\s+romance\b",
    ),
    "Romance": (
        r"\bromance\b",
        r"\blove\s+story\b",
    ),
    "Erotica": (
        r"\berotica\b",
        r"\badult\s+erotic\b",
    ),

    # ========= Literary / general fiction =========
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
    "Fiction": (
        r"\bfiction\b",
        r"\bnovel\b",
    ),

    # ========= YA / Children =========
    "Young Adult": (
        r"\byoung\s+adult\b",
        r"\bya\b",
    ),
    "Middle Grade": (
        r"\bmiddle\s+grade\b",
        r"\bmg\b",
    ),
    "Children's": (
        r"\bchildren(?:'s)?\b",
        r"\bkids\b",
        r"\bjuvenile\b",
    ),
    "Picture Book": (
        r"\bpicture\s+book\b",
    ),

    # ========= Comics / graphic =========
    "Graphic Memoir": (
        r"\bgraphic\s+memoir\b",
    ),
    "Graphic Novel": (
        r"\bgraphic\s+novel\b",
        r"\bgn\b",
    ),
    "Manga": (
        r"\bmanga\b",
        r"\blight\s+novel\b",
    ),
    "Manhwa": (
        r"\bmanhwa\b",
    ),
    "Webtoon": (
        r"\bwebtoon\b",
    ),
    "Comics": (
        r"\bcomics?\b",
        r"\bcomic\s+book\b",
    ),

    # ========= Poetry / drama / short =========
    "Poetry": (
        r"\bpoetry\b",
        r"\bpoems?\b",
        r"\bverse\b",
    ),
    "Drama": (
        r"\bdrama\b",
        r"\bplay\b",
        r"\btheatre\b",
        r"\btheater\b",
    ),
    "Short Stories": (
        r"\bshort\s+stories\b",
        r"\bshort\s+fiction\b",
    ),
    "Anthology": (
        r"\banthology\b",
        r"\bcollection\b",
        r"\bcollected\s+works\b",
    ),
    "Novella": (
        r"\bnovella\b",
    ),

    # ========= Western / war =========
    "War Fiction": (
        r"\bwar\s+fiction\b",
        r"\bmilitary\s+fiction\b",
    ),
    "Western": (
        r"\bwestern\b",
        r"\bwild\s+west\b",
    ),

    # ========= Nonfiction =========
    "Autobiography": (
        r"\bautobiograph(?:y|ies)\b",
        r"\bauto\s*[- ]?bio\b",
    ),
    "Biography": (
        r"\bbiograph(?:y|ies)\b",
        r"\blife\s+of\b",
    ),
    "Memoir": (
        r"\bmemoir\b",
        r"\bpersonal\s+history\b",
    ),
    "Essays": (
        r"\bessays?\b",
    ),
    "History": (
        r"\bhistory\b",
        r"\bhistorical\s+nonfiction\b",
    ),
    "Military History": (
        r"\bmilitary\s+history\b",
        r"\bwar\s+history\b",
        r"\bwwi\b",
        r"\bwwii\b",
        r"\bworld\s+war\s+i\b",
        r"\bworld\s+war\s+ii\b",
    ),

    # ========= Politics / society =========
    "Politics": (
        r"\bpolitics\b",
        r"\bpolitical\b",
        r"\bgovernment\b",
    ),
    "Sociology": (
        r"\bsociology\b",
        r"\bsocial\s+science\b",
    ),
    "Anthropology": (
        r"\banthropology\b",
    ),

    # ========= Science / tech nonfiction =========
    "Popular Science": (
        r"\bpopular\s+science\b",
        r"\bpop\s+sci\b",
    ),
    "Science": (
        r"\bscience\b",
        r"\bscientific\b",
    ),
    "Mathematics": (
        r"\bmath(?:s|ematics)?\b",
        r"\bmathematics\b",
    ),
    "Physics": (
        r"\bphysics\b",
        r"\bquantum\b",
        r"\brelativity\b",
    ),
    "Astronomy": (
        r"\bastronomy\b",
        r"\bastrophysics\b",
        r"\bcosmology\b",
    ),
    "Computer Science": (
        r"\bcomputer\s+science\b",
        r"\bcomp\s+sci\b",
    ),
    "Programming": (
        r"\bprogramming\b",
        r"\bcoding\b",
        r"\bsoftware\s+development\b",
    ),
    "Python": (
        r"\bpython\b",
    ),
    "Data Science": (
        r"\bdata\s+science\b",
        r"\bdata\s+analysis\b",
        r"\banalytics\b",
    ),
    "Machine Learning": (
        r"\bmachine\s+learning\b",
        r"\bml\b",
    ),
    "Artificial Intelligence": (
        r"\bartificial\s+intelligence\b",
        r"\bai\b",
    ),
    "Cybersecurity": (
        r"\bcyber\s*[- ]?security\b",
        r"\binfosec\b",
        r"\binfo\s*[- ]?sec\b",
    ),

    # ========= Business / money =========
    "Entrepreneurship": (
        r"\bentrepreneur(?:ship)?\b",
        r"\bstartup\b",
    ),
    "Management": (
        r"\bmanagement\b",
    ),
    "Leadership": (
        r"\bleadership\b",
    ),
    "Marketing": (
        r"\bmarketing\b",
        r"\bbranding\b",
    ),
    "Finance": (
        r"\bfinance\b",
        r"\bfinancial\b",
    ),
    "Investing": (
        r"\binvesting\b",
        r"\binvestment\b",
        r"\bstocks?\b",
        r"\bshares?\b",
    ),
    "Economics": (
        r"\beconomics\b",
        r"\bmacro(?:economics)?\b",
        r"\bmicro(?:economics)?\b",
    ),
    "Business": (
        r"\bbusiness\b",
        r"\bcorporate\b",
    ),

    # ========= Mind / philosophy / religion =========
    "Self-Help": (
        r"\bself\s*[- ]?help\b",
        r"\bpersonal\s+development\b",
        r"\bself\s+improvement\b",
    ),
    "Psychology": (
        r"\bpsychology\b",
        r"\bpsychological\b",
    ),
    "Mindfulness": (
        r"\bmindful(?:ness)?\b",
        r"\bmeditation\b",
    ),
    "Philosophy": (
        r"\bphilosophy\b",
        r"\bethics\b",
    ),
    "Spirituality": (
        r"\bspiritual(?:ity)?\b",
        r"\bnew\s+age\b",
    ),
    "Religion": (
        r"\breligion\b",
        r"\btheology\b",
        r"\bfaith\b",
    ),

    # ========= Health / food / lifestyle =========
    "Medicine": (
        r"\bmedicine\b",
        r"\bmedical\b",
        r"\bclinical\b",
    ),
    "Nutrition": (
        r"\bnutrition\b",
        r"\bdiet\b",
    ),
    "Fitness": (
        r"\bfitness\b",
        r"\bworkout\b",
    ),
    "Cooking": (
        r"\bcook(?:ing|book)?\b",
        r"\brecipes?\b",
    ),
    "Baking": (
        r"\bbaking\b",
        r"\bbread\b",
        r"\bpastry\b",
    ),
    "Health": (
        r"\bhealth\b",
        r"\bwellness\b",
    ),

    # ========= Travel / nature / practical =========
    "Travel": (
        r"\btravel\b",
        r"\btravelogue\b",
        r"\bguidebook\b",
    ),
    "Outdoors": (
        r"\boutdoors\b",
        r"\bhiking\b",
        r"\bcamping\b",
    ),
    "Nature": (
        r"\bnature\b",
        r"\bwildlife\b",
        r"\benvironment\b",
    ),
    "Gardening": (
        r"\bgardening\b",
        r"\bhorticulture\b",
    ),

    # ========= Arts / hobbies / home =========
    "Art": (
        r"\bart\b",
        r"\bdrawing\b",
        r"\bpainting\b",
    ),
    "Photography": (
        r"\bphotography\b",
    ),
    "Music": (
        r"\bmusic\b",
    ),
    "Film": (
        r"\bfilm\b",
        r"\bcinema\b",
    ),
    "Crafts": (
        r"\bcrafts?\b",
        r"\bknitting\b",
        r"\bsewing\b",
        r"\bwoodwork(?:ing)?\b",
    ),
    "DIY": (
        r"\bdiy\b",
        r"\bdo\s*it\s*yourself\b",
    ),
    "Home Improvement": (
        r"\bhome\s+improvement\b",
        r"\brenovation\b",
        r"\bremodel(?:ling|ing)?\b",
    ),

    # ========= Education / reference =========
    "Language Learning": (
        r"\blanguage\s+learning\b",
        r"\blearn\s+(a\s+)?language\b",
        r"\besl\b",
        r"\beal\b",
    ),
    "Textbook": (
        r"\btext\s*book\b",
        r"\btextbook\b",
    ),
    "Reference": (
        r"\breference\b",
        r"\bhandbook\b",
        r"\bmanual\b",
        r"\bencyclopedia\b",
        r"\bdictionary\b",
    ),

    # ========= Catch-all =========
    "Nonfiction": (
        r"\bnon\s*[- ]?fiction\b",
        r"\bnonfiction\b",
    ),
}

# Example usage:
# COMPILED_GENRE_MAPPING = compile_genre_mapping(GENRE_SHORTENED_MAPPING)
# canonical = standardize_genre("Sci-Fi / Space Opera", COMPILED_GENRE_MAPPING, default="Fiction")


# ============================================================================
# Fiction branch -> leaf classification
#
# This is intentionally separate from the big, flat GENRE_SHORTENED_MAPPING above.
# The goal here is to:
#   1) choose a broad fiction branch (Science Fiction, Fantasy, Horror, ...)
#   2) then run only the matching leaf map (reduces collisions and false matches)
#
# Notes:
#   - Order matters (dict insertion order): most-specific-first.
#   - Normalization is handled by normalize_genre_text().
#   - Multi-leaf classification is supported (useful for e.g. "High Fantasy" + "Dragon Fantasy").
# ============================================================================

from dataclasses import dataclass
from typing import Any, Iterable, FrozenSet


# ---------------------------
# Branch mapping (fiction)
# ---------------------------

# Keep these patterns broad enough to catch common labels, but avoid absurdly short tokens.
FICTION_BRANCH_MAPPING: Dict[str, Tuple[str, ...]] = {
    # Romance first: avoids shelving "Science Fiction Romance" as "Science Fiction".
    "Romance": (
        r"\bromance\b",
        r"\bromcom\b",
        r"\brom\s*[- ]?com\b",
        r"\bromantic\s+suspense\b",
        r"\bromantasy\b",
        r"\berotica\b",
        r"\bparanormal\s+romance\b",
        r"\bnew\s+adult\b",
    ),

    "Mystery/Crime/Thriller": (
        r"\bthriller\b",
        r"\bmystery\b",
        r"\bcrime\b",
        r"\bnoir\b",
        r"\bwhodunit\b",
        r"\bwho\s*dunnit\b",
        r"\bprocedural\b",
        r"\bheist\b",
        r"\bespionage\b",
        r"\bspy\b",
        r"\blegal\s+thriller\b",
        r"\bpsychological\s+thriller\b",
    ),

    "Horror": (
        r"\bhorror\b",
        r"\bslasher\b",
        r"\bhaunted\b",
        r"\bhaunting\b",
        r"\bghost\b",
        r"\bpossession\b",
        r"\bcosmic\s+horror\b",
        r"\blovecraft(?:ian)?\b",
    ),

    "Science Fiction": (
        r"\bscience\s*[- ]?fiction\b",
        r"\bsci\s*[- ]?fi\b",
        r"\bscifi\b",
        r"\bspace\s*[- ]?opera\b",
        r"\bcyberpunk\b",
        r"\bsteampunk\b",
        r"\bsolarpunk\b",
        r"\bdieselpunk\b",
        r"\btime\s*[- ]?travel\b",
        r"\bfirst\s+contact\b",
        r"\balien\s+invasion\b",
        r"\bpost\s*[- ]?apoc\b",
        r"\bdystopi(an|a)\b",
        r"\bcli\s*[- ]?fi\b",
        # NOTE: we deliberately avoid bare "\bsf\b" at branch level.
    ),

    "Fantasy": (
        r"\bfantasy\b",
        r"\burban\s+fantasy\b",
        r"\bgrimdark\b",
        r"\bdark\s+fantasy\b",
        r"\bhigh\s+fantasy\b",
        r"\bepic\s+fantasy\b",
        r"\bsword\s+and\s+sorcery\b",
        r"\bisekai\b",
        r"\bportal\s+fantasy\b",
        r"\blitrpg\b",
        r"\bprogression\s+fantasy\b",
    ),

    "Literary & General": (
        r"\bliterary\s+fiction\b",
        r"\blit\s*fic\b",
        r"\blitfic\b",
        r"\bmagical\s+realism\b",
        r"\bcoming\s*[- ]?of\s*[- ]?age\b",
        r"\bbildungsroman\b",
        r"\bfamily\s+saga\b",
        r"\bcontemporary\s+fiction\b",
        r"\bhistorical\s+fiction\b",
        r"\bsatire\b",
    ),

    "Genre Fiction": (
        r"\badventure\b",
        r"\baction\b",
        r"\bwestern\b",
        r"\bwild\s+west\b",
        r"\bwar\s+fiction\b",
        r"\bmilitary\s+fiction\b",
        r"\bswashbuckl(?:e|ing)\b",
        r"\bpirate(?:s)?\b",
        r"\btreasure\s+hunt\b",
        r"\bsurvival\b",
    ),
}


# ---------------------------
# Leaf mappings (fiction)
# ---------------------------

SCI_FI_LEAF_MAPPING: Dict[str, Tuple[str, ...]] = {
    "Solarpunk": (r"\bsolarpunk\b", r"\bsolar\s*[- ]?punk\b"),
    "Dieselpunk": (r"\bdieselpunk\b", r"\bdiesel\s*[- ]?punk\b"),
    "Steampunk": (r"\bsteampunk\b", r"\bsteam\s*[- ]?punk\b", r"\bclockpunk\b"),
    "Biopunk": (r"\bbiopunk\b", r"\bbio\s*[- ]?punk\b", r"\bgenepunk\b"),
    "Cyberpunk": (r"\bcyberpunk\b", r"\bcyber\s*[- ]?punk\b", r"\bnetrunner(?:s)?\b"),
    "Space Western": (r"\bspace\s+western\b", r"\bcowboy\s+in\s+space\b"),
    "Planetary Romance": (r"\bplanetary\s+romance\b", r"\bsword\s+and\s+planet\b"),
    "Space Opera": (r"\bspace\s*[- ]?opera\b", r"\bspace\s+epic\b", r"\bgalactic\s+epic\b"),
    "Military Science Fiction": (
        r"\bmilitary\s+science\s+fiction\b",
        r"\bmilitary\s+sf\b",
        r"\bmil\s+sf\b",
        r"\bmil(?:\.|itary)?\s*s(?:ci)?\s*[- ]?fi\b",
    ),
    "First Contact": (r"\bfirst\s+contact\b", r"\bcontact\s+(with\s+)?aliens\b"),
    "Alien Invasion": (r"\balien\s+invasion\b", r"\binvasion\s+of\s+earth\b"),
    "Xenofiction": (r"\bxeno\s*[- ]?fiction\b", r"\bxenofiction\b", r"\bnonhuman\s+protagonist\b"),
    "Time Travel": (
        r"\btime\s*[- ]?travel\b",
        r"\btime\s+trav(?:el|elling|eling)\b",
        r"\btime\s+loop\b",
        r"\btemporal\s+paradox\b",
    ),
    "Alternate History": (r"\balternate\s+history\b", r"\balt\s+history\b", r"\bwhat\s+if\s+history\b"),
    "Dystopian": (r"\bdystopi(an|a)\b", r"\bdystopia\b", r"\btotalitarian\b", r"\boppressive\s+regime\b"),
    "Utopian": (r"\butopi(an|a)\b", r"\butopia\b"),
    "Post-Apocalyptic": (r"\bpost\s*[- ]?apoc(?:alyptic)?\b", r"\bpostapoc\b", r"\bafter\s+the\s+fall\b"),
    "Apocalyptic": (r"\bapoc(?:alyptic)?\b", r"\bapocalypse\b", r"\bend\s+of\s+the\s+world\b"),
    "Hard Science Fiction": (
        r"\bhard\s+science\s+fiction\b",
        r"\bhard\s+sf\b",
        r"\bhard\s+s(?:ci)?\s*[- ]?fi\b",
    ),
    "Soft Science Fiction": (r"\bsoft\s+science\s+fiction\b", r"\bsoft\s+s(?:ci)?\s*[- ]?fi\b"),
    "Climate Fiction": (r"\bclimate\s*[- ]?fic(?:tion)?\b", r"\bcli\s*[- ]?fi\b", r"\beco\s*[- ]?fic(?:tion)?\b"),
    "Robots & AI": (
        r"\brobot(?:s)?\b",
        r"\bandroid(?:s)?\b",
        r"\bautomaton(?:s)?\b",
        r"\bcyborg(?:s)?\b",
        r"\bartificial\s+intelligence\b",
        r"\bsentient\s+machine(?:s)?\b",
    ),
    "Virtual Reality": (r"\bvirtual\s+reality\b", r"\bvr\b", r"\bsimulation\b"),
    "Science Fantasy": (r"\bscience\s+fantasy\b", r"\bspace\s+fantasy\b"),
    "Slipstream": (r"\bslipstream\b", r"\bnew\s+weird\b"),
    "New Weird": (r"\bnew\s+weird\b",),
    "Near-Future": (r"\bnear\s*[- ]?future\b", r"\bnear\s+term\s+future\b"),
    "Science Fiction": (r"\bscience\s*[- ]?fiction\b", r"\bsci\s*[- ]?fi\b", r"\bscifi\b", r"\bsf\b", r"\bs\s*f\b"),
}


FANTASY_LEAF_MAPPING: Dict[str, Tuple[str, ...]] = {
    "Fantasy Romance": (r"\bfantasy\s+romance\b", r"\bromantasy\b"),
    "Cozy Fantasy": (r"\bcozy\s+fantasy\b", r"\bcosy\s+fantasy\b", r"\bcomfort\s+fantasy\b"),
    "Grimdark": (r"\bgrim\s*[- ]?dark\b", r"\bgrimdark\b"),
    "Dark Fantasy": (r"\bdark\s+fantasy\b", r"\bgrim\s+fantasy\b"),
    "Gaslamp Fantasy": (r"\bgaslamp\s+fantasy\b", r"\bvictorian\s+fantasy\b"),
    "Mythic Fantasy": (
        r"\bmythic\s+fantasy\b",
        r"\bmyth(?:ology|ological)\s+fantasy\b",
        r"\bmyth\s+retelling\b",
        r"\bretelling\b",
    ),
    "Fairy Tale": (r"\bfairy\s*[- ]?tale\b", r"\bfaerie\s+tale\b", r"\bfairytale\b"),
    "Arthurian": (r"\barthuri(an)?\b", r"\bking\s+arthur\b", r"\bcamelot\b", r"\bround\s+table\b"),
    "Urban Fantasy": (r"\burban\s+fantasy\b", r"\buf\b", r"\bmodern\s+magic\b"),
    "Contemporary Fantasy": (r"\bcontemporary\s+fantasy\b", r"\bmodern\s+fantasy\b"),
    "Historical Fantasy": (r"\bhistorical\s+fantasy\b", r"\bperiod\s+fantasy\b"),
    "Low Fantasy": (r"\blow\s+fantasy\b", r"\bgrounded\s+fantasy\b"),
    "High Fantasy": (r"\bhigh\s+fantasy\b", r"\bh\.\s*fan\b", r"\bsecondary\s+world\b"),
    "Epic Fantasy": (r"\bepic\s+fantasy\b", r"\bfantasy\s+epic\b", r"\bworld\s*[- ]?spanning\b"),
    "Sword and Sorcery": (
        r"\bsword\s+and\s+sorcery\b",
        r"\bswords\s+and\s+sorcery\b",
        r"\bs\s*&\s*s\b",
        r"\bs&s\b",
    ),
    "Heroic Fantasy": (r"\bheroic\s+fantasy\b", r"\bchosen\s+one\b"),
    "Portal Fantasy": (r"\bportal\s+fantasy\b", r"\bother\s+world\b", r"\bisekai\b"),
    "LitRPG": (r"\blit\s*rpg\b", r"\blitrpg\b"),
    "Progression Fantasy": (
        r"\bprogression\s+fantasy\b",
        r"\bpower\s+progression\b",
        r"\bcultivation\b",
        r"\bxianxia\b",
        r"\bxuanhuan\b",
    ),
    "Witchy Fantasy": (r"\bwitch(?:es)?\b", r"\bwitchcraft\b", r"\bcoven\b"),
    "Necromancy": (r"\bnecroman(?:cy|cer|cers)\b", r"\blich(?:es)?\b", r"\brais(?:e|ing)\s+the\s+dead\b"),
    "Dragon Fantasy": (r"\bdragon(?:s)?\b", r"\bwyrm(?:s)?\b", r"\bdrake(?:s)?\b"),
    "Vampire Fantasy": (r"\bvampire(?:s)?\b", r"\bvampiric\b", r"\bdracula\b"),
    "Werewolf Fantasy": (r"\bwerewolf(?:s)?\b", r"\blycan(?:thrope)?s?\b", r"\blycanthropy\b"),
    "Faerie / Fae": (r"\bfae\b", r"\bfaerie(?:s)?\b", r"\bfairy(?:ies)?\b", r"\bfair\s*folk\b"),
    "Science Fantasy": (r"\bscience\s+fantasy\b", r"\bspace\s+fantasy\b"),
    "Mythpunk": (r"\bmythpunk\b",),
    "New Weird": (r"\bnew\s+weird\b",),
    "Fantasy": (r"\bfantasy\b", r"\bfant?a?s?y?\b"),
}


HORROR_LEAF_MAPPING: Dict[str, Tuple[str, ...]] = {
    "Cosmic Horror": (
        r"\bcosmic\s+horror\b",
        r"\blovecraft(?:ian)?\b",
        r"\beldritch\b",
        r"\bcthulhu\b",
        r"\bancient\s+ones\b",
    ),
    "Folk Horror": (r"\bfolk\s+horror\b", r"\bpagan\s+horror\b", r"\brural\s+horror\b", r"\bcult\s+horror\b"),
    "Gothic Horror": (r"\bgothic\s+horror\b", r"\bgothic\b"),
    "Southern Gothic": (r"\bsouthern\s+gothic\b",),
    "Vampire Horror": (r"\bvampire(?:s)?\b", r"\bvampiric\b", r"\bdracula\b", r"\bnosferatu\b"),
    "Werewolf Horror": (r"\bwerewolf(?:s)?\b", r"\blycan(?:thrope)?s?\b", r"\blycanthropy\b"),
    "Zombie Horror": (r"\bzombie(?:s)?\b", r"\bwalking\s+dead\b", r"\bundead\b"),
    "Demonic / Possession": (r"\bpossession\b", r"\bdemon(?:ic|s)?\b", r"\bexorcism\b", r"\binfernal\b"),
    "Witchcraft Horror": (r"\bwitch(?:es)?\b", r"\bwitchcraft\b", r"\bcoven\b", r"\bhex\b", r"\bcurse\b"),
    "Haunted House": (r"\bhaunted\s+house\b", r"\bghost\s+house\b", r"\bhaunting\b", r"\bpoltergeist\b"),
    "Ghost Story": (r"\bghost\s+story\b", r"\bghost(?:s)?\b", r"\bspecter\b", r"\bspectre\b", r"\bwraith\b"),
    "Body Horror": (r"\bbody\s+horror\b", r"\bmutation\b", r"\bmetamorphosis\b", r"\bparasit(?:e|ic)\b"),
    "Medical Horror": (r"\bmedical\s+horror\b", r"\bhospital\s+horror\b", r"\bsurgical\s+horror\b", r"\bmad\s+doctor\b"),
    "Slasher": (r"\bslasher\b", r"\bmasked\s+killer\b", r"\bfinal\s+girl\b"),
    "Serial Killer Horror": (r"\bserial\s+killer(?:s)?\b", r"\bcopycat\s+killer\b", r"\bhuman\s+monster\b"),
    "Home Invasion": (r"\bhome\s+invasion\b", r"\bbreak\s*[- ]?in\b"),
    "Psychological Horror": (r"\bpsychological\s+horror\b", r"\bunreliable\s+narrator\b", r"\bgaslight(?:ing)?\b"),
    "Sci-Fi Horror": (r"\bsci\s*[- ]?fi\s+horror\b", r"\bscience\s+fiction\s+horror\b", r"\bspace\s+horror\b", r"\balien\s+horror\b"),
    "Apocalyptic Horror": (r"\bapocalyptic\s+horror\b", r"\bapocalypse\b"),
    "Supernatural Horror": (r"\bsupernatural\s+horror\b", r"\bparanormal\s+horror\b", r"\bparanormal\b", r"\bsupernatural\b"),
    "Horror": (r"\bhorror\b", r"\bterror\b", r"\bfright\b"),
}


MYSTERY_CRIME_THRILLER_LEAF_MAPPING: Dict[str, Tuple[str, ...]] = {
    "Cozy Mystery": (r"\bcozy\s+mystery\b", r"\bcosy\s+mystery\b", r"\btea\s+shop\s+mystery\b", r"\bcraft\s+mystery\b"),
    "Locked Room Mystery": (r"\blocked\s+room\b", r"\bclosed\s+circle\b", r"\bimpossible\s+crime\b"),
    "Whodunit": (r"\bwhodunit\b", r"\bwho\s*dunnit\b"),
    "Police Procedural": (r"\bpolice\s+procedural\b", r"\bcrime\s+procedural\b", r"\bprocedural\b"),
    "Detective": (r"\bdetective\b", r"\bprivate\s+eye\b", r"\bpi\b", r"\bprivate\s+investigator\b"),
    "Noir": (r"\bnoir\b", r"\bneo\s*[- ]?noir\b"),
    "Hardboiled": (r"\bhard\s*[- ]?boiled\b", r"\bhardboiled\b"),
    "Heist": (r"\bheist\b", r"\bcaper\b", r"\bbank\s+job\b"),
    "Legal Thriller": (r"\blegal\s+thriller\b", r"\bcourtroom\s+thriller\b", r"\blawyer\s+thriller\b"),
    "Political Thriller": (r"\bpolitical\s+thriller\b", r"\bstate\s+secrets?\b", r"\bconspiracy\b"),
    "Espionage Thriller": (r"\bespionage\b", r"\bspy\s+thriller\b", r"\bintelligence\s+agency\b", r"\bmi6\b", r"\bcia\b"),
    "Techno-thriller": (r"\btechno\s*[- ]?thriller\b", r"\btechnothriller\b", r"\bcyber\s*[- ]?thriller\b"),
    "Medical Thriller": (r"\bmedical\s+thriller\b", r"\bhospital\s+thriller\b"),
    "Domestic Thriller": (r"\bdomestic\s+thriller\b", r"\bmarriage\s+thriller\b"),
    "Psychological Thriller": (r"\bpsychological\s+thriller\b", r"\bunreliable\s+narrator\b", r"\bgaslight(?:ing)?\b"),
    "Action Thriller": (r"\baction\s+thriller\b", r"\bmanhunt\b"),
    "Organized Crime": (r"\borganized\s+crime\b", r"\bmafia\b", r"\bgangster(?:s)?\b", r"\bcartel\b"),
    "Serial Killer": (r"\bserial\s+killer(?:s)?\b", r"\bcopycat\s+killer\b"),
    "True Crime": (r"\btrue\s+crime\b", r"\bnonfiction\s+crime\b"),
    "Thriller": (r"\bthriller\b", r"\bsuspense\b"),
    "Mystery": (r"\bmystery\b", r"\binvestigation\b", r"\bcase\b"),
    "Crime": (r"\bcrime\b", r"\bcriminal\b", r"\bmurder\b", r"\btheft\b"),
}


LITERARY_LEAF_MAPPING: Dict[str, Tuple[str, ...]] = {
    "Magical Realism": (r"\bmagical\s+realism\b", r"\bmagic\s+realism\b"),
    "Metafiction": (r"\bmetafiction\b", r"\bmeta\s+fiction\b", r"\bself[- ]referential\b"),
    "Absurdist Fiction": (r"\babsurd(?:ist|ism)\b", r"\btheatre\s+of\s+the\s+absurd\b"),
    "Experimental Fiction": (r"\bexperimental\s+fiction\b", r"\bavant[- ]garde\b"),
    "Campus Novel": (r"\bcampus\s+novel\b", r"\bacademic\s+satire\b", r"\buniversity\s+novel\b"),
    "Book Club Fiction": (r"\bbook\s+club\b", r"\bbook[- ]club\s+fiction\b", r"\bupmarket\b", r"\bup[- ]market\b"),
    "Southern Gothic": (r"\bsouthern\s+gothic\b",),
    "Gothic Fiction": (r"\bgothic\s+fiction\b", r"\bgothic\b"),
    "Women's Fiction": (r"\bwomen(?:'s)?\s+fiction\b", r"\bchick\s+lit\b"),
    "Coming-of-Age": (r"\bcoming\s*[- ]?of\s*[- ]?age\b", r"\bbildungsroman\b"),
    "Family Saga": (r"\bfamily\s+saga\b", r"\bgenerational\s+saga\b", r"\bmulti[- ]generational\b"),
    "Domestic Fiction": (r"\bdomestic\s+fiction\b", r"\bfamily\s+drama\b", r"\bmarriage\b"),
    "Slice of Life": (r"\bslice\s+of\s+life\b", r"\beveryday\s+life\b"),
    "Social Realism": (r"\bsocial\s+realism\b", r"\bsocially\s+conscious\b"),
    "Humor": (r"\bhumou?r\b", r"\bcomic\s+fiction\b", r"\bhilarious\b", r"\bfunny\b"),
    "Satire": (r"\bsatire\b", r"\bsatirical\b"),
    "Historical Fiction": (r"\bhistorical\s+fiction\b", r"\bperiod\s+fiction\b"),
    "Contemporary Fiction": (r"\bcontemporary\s+fiction\b", r"\bmodern\s+fiction\b"),
    "Short Stories": (r"\bshort\s+stories\b", r"\bshort\s+fiction\b"),
    "Anthology": (r"\banthology\b", r"\bcollection\b", r"\bcollected\s+stories\b", r"\bshort\s+story\s+collection\b"),
    "Novella": (r"\bnovella\b", r"\bshort\s+novel\b"),
    "Literary Fiction": (r"\bliterary\s+fiction\b", r"\blit\s*fic\b", r"\blitfic\b"),
    "Fiction": (r"\bfiction\b", r"\bnovel\b"),
}


GENRE_FICTION_LEAF_MAPPING: Dict[str, Tuple[str, ...]] = {
    "War Fiction": (r"\bwar\s+fiction\b", r"\bmilitary\s+fiction\b", r"\bcombat\b", r"\bbattlefield\b"),
    "Spy Fiction": (r"\bspy\s+fiction\b", r"\bespionage\b", r"\bsecret\s+agent\b"),
    "Western Romance": (r"\bwestern\s+romance\b",),
    "Western": (r"\bwestern\b", r"\bwild\s+west\b", r"\bcowboy\b", r"\bfrontier\b", r"\bgunfighter\b"),
    "Swashbuckling": (r"\bswashbuckl(?:e|ing)\b", r"\bpirate(?:s)?\b", r"\bprivateer\b", r"\bhigh\s+seas\b"),
    "Treasure Hunt": (r"\btreasure\s+hunt\b", r"\blost\s+treasure\b", r"\bancient\s+treasure\b"),
    "Survival Adventure": (r"\bsurvival\b", r"\bstranded\b", r"\bwilderness\b", r"\bcastaway\b"),
    "Historical Adventure": (r"\bhistorical\s+adventure\b", r"\bperiod\s+adventure\b"),
    "Adventure": (r"\badventure\b", r"\bquest\b", r"\bexpedition\b", r"\bjourney\b"),
    "Action": (r"\baction\b", r"\bhigh\s+stakes\b", r"\bfast[- ]paced\b"),
    "Comic Fiction": (r"\bcomic\s+fiction\b", r"\bhumou?r\b", r"\bhilarious\b", r"\bfunny\b"),
    "Historical Fiction": (r"\bhistorical\s+fiction\b", r"\bperiod\s+fiction\b"),
    "General Fiction": (r"\bgeneral\s+fiction\b", r"\bmainstream\s+fiction\b", r"\bcommercial\s+fiction\b"),
    "Fiction": (r"\bfiction\b", r"\bnovel\b"),
}


ROMANCE_LEAF_MAPPING: Dict[str, Tuple[str, ...]] = {
    "Romantic Suspense": (r"\bromantic\s+suspense\b", r"\brom\s+suspense\b"),
    "Paranormal Romance": (
        r"\bparanormal\s+romance\b",
        r"\bpnr\b",
        r"\bvampire\s+romance\b",
        r"\bwerewolf\s+romance\b",
        r"\bshifter\s+romance\b",
    ),
    "Fantasy Romance": (r"\bfantasy\s+romance\b", r"\bromantasy\b", r"\bfae\s+romance\b"),
    "Science Fiction Romance": (
        r"\bscience\s+fiction\s+romance\b",
        r"\bsci\s*[- ]?fi\s+romance\b",
        r"\bsf\s+romance\b",
        r"\balien\s+romance\b",
    ),
    "Dark Romance": (r"\bdark\s+romance\b",),
    "Romantic Comedy": (r"\brom\s*[- ]?com\b", r"\bromcom\b", r"\bromantic\s+comedy\b"),
    "Regency Romance": (r"\bregency\s+romance\b", r"\bregency\b"),
    "Historical Romance": (r"\bhistorical\s+romance\b", r"\bperiod\s+romance\b"),
    "Contemporary Romance": (r"\bcontemporary\s+romance\b", r"\bmodern\s+romance\b"),
    "LGBTQ+ Romance": (
        r"\blgbtq\+?\s+romance\b",
        r"\bqueer\s+romance\b",
        r"\bgay\s+romance\b",
        r"\blesbian\s+romance\b",
        r"\bmm\s+romance\b",
        r"\bff\s+romance\b",
    ),
    "New Adult Romance": (r"\bnew\s+adult\s+romance\b", r"\bna\s+romance\b", r"\bnew\s+adult\b"),
    "Sports Romance": (r"\bsports?\s+romance\b", r"\bathlete\s+romance\b"),
    "Erotica": (r"\berotica\b", r"\berotic\s+romance\b", r"\badult\s+erotic\b", r"\bexplicit\b"),
    "Romance": (r"\bromance\b", r"\blove\s+story\b", r"\bromantic\b"),
}


FICTION_LEAF_MAPS: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "Science Fiction": SCI_FI_LEAF_MAPPING,
    "Fantasy": FANTASY_LEAF_MAPPING,
    "Horror": HORROR_LEAF_MAPPING,
    "Mystery/Crime/Thriller": MYSTERY_CRIME_THRILLER_LEAF_MAPPING,
    "Literary & General": LITERARY_LEAF_MAPPING,
    "Genre Fiction": GENRE_FICTION_LEAF_MAPPING,
    "Romance": ROMANCE_LEAF_MAPPING,
}


@dataclass(frozen=True)
class FictionGenreClassification:
    """Result of branch->leaf classification for fiction."""

    branch: Optional[str]
    leaf: Optional[str]
    leaves: FrozenSet[str]
    normalized: str


# Precompiled regexes (module-level cache)
COMPILED_FICTION_BRANCH_MAPPING = compile_genre_mapping(FICTION_BRANCH_MAPPING)
COMPILED_FICTION_LEAF_MAPPINGS: Dict[str, Dict[str, Tuple[Pattern[str], ...]]] = {
    branch: compile_genre_mapping(leaf_map)
    for branch, leaf_map in FICTION_LEAF_MAPS.items()
}


def _first_match(
    normalized: str,
    compiled_mapping: Dict[str, Tuple[Pattern[str], ...]],
) -> Optional[str]:
    for key, patterns in compiled_mapping.items():
        for pat in patterns:
            if pat.search(normalized):
                return key
    return None


def _all_matches(
    normalized: str,
    compiled_mapping: Dict[str, Tuple[Pattern[str], ...]],
) -> FrozenSet[str]:
    hits: set[str] = set()
    for key, patterns in compiled_mapping.items():
        for pat in patterns:
            if pat.search(normalized):
                hits.add(key)
                break
    return frozenset(hits)


def classify_fiction_genre(
    raw: Any,
    *,
    multi_leaf: bool = False,
    default_branch: Optional[str] = None,
    default_leaf: Optional[str] = None,
) -> FictionGenreClassification:
    """Classify a raw genre-ish string into (branch, leaf).

    This is a convenience wrapper intended for metadata cleanup:
      - Normalize once
      - Pick the broad fiction branch (first-match-wins)
      - Run only that branch's leaf mapping

    Args:
        raw: input string (or anything stringify-able)
        multi_leaf: if True, return *all* matching leaves (as a set)
        default_branch: branch to use if nothing matches
        default_leaf: leaf to use if nothing matches within the chosen branch

    Returns:
        FictionGenreClassification
    """

    normalized = normalize_genre_text("" if raw is None else str(raw))
    if not normalized:
        return FictionGenreClassification(
            branch=default_branch,
            leaf=default_leaf,
            leaves=frozenset(),
            normalized="",
        )

    branch = _first_match(normalized, COMPILED_FICTION_BRANCH_MAPPING) or default_branch

    compiled_leaf = COMPILED_FICTION_LEAF_MAPPINGS.get(branch or "")
    if compiled_leaf is None:
        # Unknown branch or no leaf map configured
        return FictionGenreClassification(
            branch=branch,
            leaf=default_leaf,
            leaves=frozenset(),
            normalized=normalized,
        )

    if multi_leaf:
        leaves = _all_matches(normalized, compiled_leaf)

        # Prefer specific leaves: if we matched anything beyond generic catch-alls,
        # drop the obvious umbrellas (the branch name itself should already convey that).
        if len(leaves) > 1:
            generic = {
                "Fiction",
                "General Fiction",
                "Science Fiction",
                "Fantasy",
                "Horror",
                "Romance",
                "Thriller",
                "Mystery",
                "Crime",
            }
            pruned = frozenset(x for x in leaves if x not in generic)
            if pruned:
                leaves = pruned

        leaf = next(iter(leaves), default_leaf)
        return FictionGenreClassification(
            branch=branch,
            leaf=leaf,
            leaves=leaves,
            normalized=normalized,
        )

    leaf = _first_match(normalized, compiled_leaf) or default_leaf
    return FictionGenreClassification(
        branch=branch,
        leaf=leaf,
        leaves=frozenset({leaf}) if leaf else frozenset(),
        normalized=normalized,
    )

