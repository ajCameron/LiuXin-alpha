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
