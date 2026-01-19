# Regional / cultural buckets (keep separate from core genre mapping).
# Order matters if you use first-match-wins: most specific first.

REGIONAL_BUCKET_MAPPING = {
    # ---- Europe (specific sub-labels people actually type) ----
    "Nordic Noir": (
        r"\bnordic\s+noir\b",
        r"\bscandi(?:navian)?\s+noir\b",
        r"\bscandi\s+crime\b",
        r"\bnordic\s+crime\b",
        r"\bscandinavian\s+crime\b",
        r"\bswedish\s+noir\b",
        r"\bnorwegian\s+noir\b",
        r"\bdanish\s+noir\b",
        r"\bicelandic\s+noir\b",
        r"\bfinnish\s+noir\b",
    ),
    "Tartan Noir": (
        r"\btartan\s+noir\b",
        r"\bscottish\s+noir\b",
    ),
    "Celtic Noir": (
        r"\bceltic\s+noir\b",
        r"\birish\s+noir\b",
        r"\bwelsh\s+noir\b",
    ),
    "Mediterranean Noir": (
        r"\bmediterranean\s+noir\b",
        r"\bitalian\s+noir\b",
        r"\bsicilian\s+noir\b",
        r"\bspanish\s+noir\b",
        r"\bportuguese\s+noir\b",
        r"\bgreek\s+noir\b",
        r"\bturkish\s+noir\b",
    ),
    "Baltic Noir": (
        r"\bbaltic\s+noir\b",
        r"\bestonian\s+noir\b",
        r"\blatvian\s+noir\b",
        r"\blithuanian\s+noir\b",
    ),
    "French Noir": (
        r"\bfrench\s+noir\b",
        r"\broman\s+noir\b",
    ),
    "German Krimi": (
        r"\bkrimi\b",
        r"\bgerman\s+krimi\b",
        r"\bdeutsch(?:e|er)?\s+krimi\b",
    ),
    "British & Irish": (
        r"\buk\b",
        r"\bu\.k\.\b",
        r"\bbritish\b",
        r"\bengland\b",
        r"\benglish\b",
        r"\bscotland\b",
        r"\bscottish\b",
        r"\bwales\b",
        r"\bwelsh\b",
        r"\bnorthern\s+ireland\b",
        r"\birish\b",
        r"\bgb\b",
        r"\bgreat\s+britain\b",
        r"\bunited\s+kingdom\b",
    ),
    "Irish": (
        r"\bireland\b",
        r"\birish\b",
    ),
    "Scottish": (
        r"\bscotland\b",
        r"\bscottish\b",
    ),
    "Welsh": (
        r"\bwales\b",
        r"\bwelsh\b",
    ),
    "English": (
        r"\bengland\b",
        r"\benglish\b",
    ),
    "Benelux": (
        r"\bbenelux\b",
        r"\bnetherlands\b",
        r"\bdutch\b",
        r"\bholland\b",
        r"\bbelgium\b",
        r"\bbelgian\b",
        r"\bflanders\b",
        r"\bflemish\b",
        r"\bwallonia\b",
        r"\bluxembourg\b",
        r"\bluxembourgish\b",
    ),
    "Iberian": (
        r"\biberia\b",
        r"\biberian\b",
        r"\bspain\b",
        r"\bspanish\b",
        r"\bportugal\b",
        r"\bportuguese\b",
        r"\bcatalan\b",
        r"\bbasque\b",
        r"\bgalician\b",
    ),
    "Italian": (
        r"\bitaly\b",
        r"\bitalian\b",
        r"\bsicily\b",
        r"\bsicilian\b",
        r"\bnaples\b",
        r"\bneapolitan\b",
    ),
    "French": (
        r"\bfrance\b",
        r"\bfrench\b",
        r"\bparis\b",
        r"\bprovencal\b",
        r"\bproven[cç]e\b",
    ),
    "German": (
        r"\bgermany\b",
        r"\bgerman\b",
        r"\baustria\b",
        r"\baustrian\b",
        r"\bswitzerland\b",
        r"\bswiss\b",
    ),
    "Eastern European": (
        r"\beastern\s+europe(?:an)?\b",
        r"\bpoland\b",
        r"\bpolish\b",
        r"\bczech(?:ia)?\b",
        r"\bczech\b",
        r"\bslovak(?:ia)?\b",
        r"\bhungary\b",
        r"\bhungarian\b",
        r"\bromania\b",
        r"\bromanian\b",
        r"\bbulgaria\b",
        r"\bbulgarian\b",
    ),
    "Balkan": (
        r"\bbalkan(?:s)?\b",
        r"\bserbia\b",
        r"\bserbian\b",
        r"\bcroatia\b",
        r"\bcroatian\b",
        r"\bbosnia\b",
        r"\bbosnian\b",
        r"\bmontenegro\b",
        r"\bnorth\s+macedonia\b",
        r"\bmacedoni(?:a|an)\b",
        r"\balbania\b",
        r"\balbanian\b",
        r"\bslovenia\b",
        r"\bslovenian\b",
    ),
    "Russian & Slavic": (
        r"\brussia\b",
        r"\brussian\b",
        r"\bslavic\b",
        r"\bukraine\b",
        r"\bukrainian\b",
        r"\bbelarus\b",
        r"\bbelarusian\b",
        r"\bserbo[- ]?croatian\b",
    ),
    "Nordic / Scandinavian": (
        r"\bscandinavia\b",
        r"\bscandinavian\b",
        r"\bnordic\b",
        r"\bsweden\b",
        r"\bswedish\b",
        r"\bnorway\b",
        r"\bnorwegian\b",
        r"\bdenmark\b",
        r"\bdanish\b",
        r"\bfinland\b",
        r"\bfinnish\b",
        r"\biceland\b",
        r"\bicelandic\b",
    ),

    # ---- Middle East / North Africa (MENA) ----
    "MENA": (
        r"\bmena\b",
        r"\bmiddle\s+east(?:ern)?\b",
        r"\bnorth\s+africa(?:n)?\b",
        r"\bmaghreb\b",
        r"\blevant\b",
    ),
    "Arabic": (
        r"\barabic\b",
        r"\barab\b",
        r"\bsaudi\b",
        r"\byemen\b",
        r"\bomani\b",
        r"\bqatari\b",
        r"\bkuwait\b",
        r"\bkuwaiti\b",
        r"\buae\b",
        r"\bunited\s+arab\s+emirates\b",
    ),
    "Iranian / Persian": (
        r"\biran\b",
        r"\biranian\b",
        r"\bpersia\b",
        r"\bpersian\b",
    ),
    "Turkish": (
        r"\bturkey\b",
        r"\bturkish\b",
    ),
    "Israeli / Palestinian": (
        r"\bisrael(?:i)?\b",
        r"\bpalestin(?:e|ian)\b",
    ),
    "Egyptian": (
        r"\begypt\b",
        r"\begyptian\b",
    ),
    "North African": (
        r"\bmorocco\b",
        r"\bmoroccan\b",
        r"\balgeria\b",
        r"\balgerian\b",
        r"\btunisia\b",
        r"\btunisian\b",
        r"\blibya\b",
        r"\blibyan\b",
    ),

    # ---- Sub-Saharan Africa ----
    "African": (
        r"\bafrica(?:n)?\b",
        r"\bsub[- ]?saharan\b",
    ),
    "West African": (
        r"\bwest\s+africa(?:n)?\b",
        r"\bnigeria(?:n)?\b",
        r"\bghana(?:ian)?\b",
        r"\bsenegal(?:ese)?\b",
        r"\bivory\s+coast\b",
        r"\bcote\s+d[' ]?ivoire\b",
        r"\bguinea(?:n)?\b",
        r"\bsierra\s+leone\b",
        r"\bliberia(?:n)?\b",
    ),
    "East African": (
        r"\beast\s+africa(?:n)?\b",
        r"\bkenya(?:n)?\b",
        r"\btanzania(?:n)?\b",
        r"\buganda(?:n)?\b",
        r"\brwanda(?:n)?\b",
        r"\bburundi(?:an)?\b",
        r"\bethiopia(?:n)?\b",
        r"\beritrea(?:n)?\b",
        r"\bsomalia(?:n)?\b",
    ),
    "Southern African": (
        r"\bsouthern\s+africa(?:n)?\b",
        r"\bsouth\s+africa(?:n)?\b",
        r"\bnamibia(?:n)?\b",
        r"\bbotswana(?:n)?\b",
        r"\bzimbabwe(?:an)?\b",
        r"\bzambia(?:n)?\b",
        r"\bmozambique(?:an)?\b",
        r"\bangola(?:n)?\b",
    ),

    # ---- South Asia ----
    "South Asian": (
        r"\bsouth\s+asia(?:n)?\b",
        r"\bindian\s+subcontinent\b",
    ),
    "Indian": (
        r"\bindia(?:n)?\b",
        r"\bhindi\b",
        r"\bpunjab(?:i)?\b",
        r"\bbengal(?:i)?\b",
        r"\btamil\b",
        r"\bmalayalam\b",
        r"\bmarathi\b",
        r"\bgujarati\b",
    ),
    "Pakistani": (
        r"\bpakistan(?:i)?\b",
        r"\burdu\b",
    ),
    "Bangladeshi": (
        r"\bbangladesh(?:i)?\b",
    ),
    "Sri Lankan": (
        r"\bsri\s+lanka(?:n)?\b",
        r"\bsinhal(?:a|ese)\b",
        r"\btamil\b",
    ),
    "Nepalese": (
        r"\bnepal(?:ese)?\b",
    ),

    # ---- Southeast Asia ----
    "Southeast Asian": (
        r"\bsouth(?:e)?ast\s+asia(?:n)?\b",
        r"\basean\b",
    ),
    "Vietnamese": (
        r"\bvietnam(?:ese)?\b",
    ),
    "Thai": (
        r"\bthailand\b",
        r"\bthai\b",
    ),
    "Indonesian": (
        r"\bindonesia(?:n)?\b",
        r"\bjavanese\b",
        r"\bbalinese\b",
    ),
    "Filipino": (
        r"\bphilippines\b",
        r"\bfilipin(?:o|a)\b",
        r"\btagalog\b",
    ),
    "Malaysian / Singaporean": (
        r"\bmalaysia(?:n)?\b",
        r"\bsingapore(?:an)?\b",
    ),
    "Cambodian / Khmer": (
        r"\bcambodia(?:n)?\b",
        r"\bkhmer\b",
    ),
    "Burmese / Myanmar": (
        r"\bmyanmar\b",
        r"\bburma(?:n|ese)?\b",
    ),

    # ---- East Asia ----
    "East Asian": (
        r"\beast\s+asia(?:n)?\b",
    ),
    "Japanese": (
        r"\bjapan(?:ese)?\b",
    ),
    "Korean": (
        r"\bkorea(?:n)?\b",
        r"\bsouth\s+korea(?:n)?\b",
        r"\bnorth\s+korea(?:n)?\b",
    ),
    "Chinese": (
        r"\bchina\b",
        r"\bchinese\b",
        r"\bmandarin\b",
        r"\bcantonese\b",
        r"\btaiwan(?:ese)?\b",
        r"\bhong\s+kong\b",
    ),

    # ---- Oceania ----
    "Australian": (
        r"\baustralia(?:n)?\b",
    ),
    "New Zealand": (
        r"\bnew\s+zealand\b",
        r"\bkiwi\b",
        r"\baotearoa\b",
    ),
    "Pacific Islands": (
        r"\bpacific\s+islands?\b",
        r"\bpolynesia(?:n)?\b",
        r"\bmelanesia(?:n)?\b",
        r"\bmicronesia(?:n)?\b",
        r"\bfiji(?:an)?\b",
        r"\bsamoa(?:n)?\b",
        r"\btonga(?:n)?\b",
        r"\bvanuatu\b",
        r"\bpapua\s+new\s+guinea\b",
    ),

    # ---- The Americas ----
    "North American": (
        r"\bnorth\s+america(?:n)?\b",
    ),
    "United States": (
        r"\bunited\s+states\b",
        r"\busa\b",
        r"\bu\.s\.a\.\b",
        r"\bus\b",
        r"\bamerican\b",
    ),
    "Canadian": (
        r"\bcanada(?:n)?\b",
    ),
    "Mexican": (
        r"\bmexico\b",
        r"\bmexican\b",
    ),
    "Caribbean": (
        r"\bcaribbean\b",
        r"\bjamaica(?:n)?\b",
        r"\bhaiti(?:an)?\b",
        r"\bdominican\b",
        r"\btrinidad(?:ian)?\b",
        r"\bbarbados\b",
        r"\bpuerto\s+rico\b",
        r"\bcuba(?:n)?\b",
    ),
    "Central American": (
        r"\bcentral\s+america(?:n)?\b",
        r"\bguatemala(?:n)?\b",
        r"\bhondura(?:n)?\b",
        r"\bel\s+salvador(?:an)?\b",
        r"\bnicaragua(?:n)?\b",
        r"\bcosta\s+rica(?:n)?\b",
        r"\bpanama(?:nian)?\b",
        r"\bbelize(?:an)?\b",
    ),
    "Latin American": (
        r"\blatin\s+america(?:n)?\b",
        r"\blatam\b",
    ),
    "South American": (
        r"\bsouth\s+america(?:n)?\b",
        r"\bandean\b",
        r"\bpatagonia\b",
    ),
    "Brazilian": (
        r"\bbrazil\b",
        r"\bbrazilian\b",
        r"\bportuguese\b",  # be careful: Portuguese also appears in Iberian; ordering matters
    ),
    "Argentinian": (
        r"\bargentina\b",
        r"\bargentin(?:e|ian)\b",
    ),
    "Chilean": (
        r"\bchile(?:an)?\b",
    ),
    "Colombian": (
        r"\bcolombia(?:n)?\b",
    ),
    "Peruvian": (
        r"\bperu(?:vian)?\b",
    ),

    # ---- Indigenous / diaspora-ish (often used as “regional” tags in the wild) ----
    "Indigenous / First Nations": (
        r"\bindigenous\b",
        r"\bfirst\s+nations\b",
        r"\bnative\s+american\b",
        r"\baboriginal\b",
        r"\bmaori\b",
        r"\binuit\b",
        r"\bmetis\b",
    ),
}

# Optional: helper for layering maps in a controlled order (later maps appended last)
def merge_mappings(*maps: dict) -> dict:
    merged = {}
    for m in maps:
        merged.update(m)
    return merged
