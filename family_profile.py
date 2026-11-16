"""
Matcher et Spond-/kalender-gruppenavn (f.eks. "Lillegutt 2015 Ready") mot nøyaktig
ett barn - UTEN noen hardkodede lagnavn eller aldersklasser i kildekoden. Alt den vet
om lagene kommer fra familieprofilen (user_settings.family_context), som brukeren selv
skriver og oppdaterer i appen. Når et lag bytter navn (f.eks. ved aldersovergang),
oppdaterer brukeren bare profilteksten - ingen kode trenger å endres eller deployes.

Forventet format i familieprofilen: hver persons seksjon starter med en linje som
"**Navn**:" eller "Navn:" (fet skrift valgfritt), og ALLE lagnavn/gruppenavn i
seksjonen står i anførselstegn, f.eks:
    **Gustav**: gutt, født 2015, går på Holmen Skole 6C.
    - Fotball: "Ready G2015 Rød"
    - Bandy (vinter): "Knøtt 2015 Ready" eller "Lillegutt 2015 Ready"

Hvis familieprofilen ikke følger dette mønsteret (ingen anførselstegn, eller ingen
gjenkjennbare "Navn:"-overskrifter), finner denne modulen rett og slett ingen match -
den gjetter aldri. Det er et bevisst valg: heller ingen match enn feil barn.
"""
import re

# En persons seksjon starter KUN med en linje skrevet i fet skrift: "**Navn**:"
# (f.eks. "**Gustav**:" eller "**Kristin Dahle**:"). Dette er et eksplisitt,
# strukturelt signal brukeren selv setter - ingen gjetting basert på ord. Uten fet
# skrift blir en linje som "Fotball: Ready G2015 Rød" ALDRI tolket som en ny persons
# overskrift, uansett hva den heter - den blir automatisk et punkt UNDER personen
# over, siden den ikke matcher dette mønsteret.
_HEADER_RE = re.compile(r'^\*\*([^*\n]+)\*\*\s*:', re.MULTILINE)
# Godtar rette anførselstegn ("..."), buede/"smarte" anførselstegn (“...” eller „...“)
# og anførselstegn (« ... »), siden tekstbehandlere ofte bytter rette til buede
# automatisk når man skriver eller limer inn tekst.
_QUOTED_RE = re.compile(r'[«"“„]([^»"”“]+)[»"”]')


# Fjerner "IKKE ..."-setninger (f.eks. "IKKE 'Minigutt 2017' eller 'G17' - det er
# Rasmus sitt lag.") før vi plukker ut anførselstegn-navn - uten dette ville et
# lagnavn som nevnes for å utelukke det (fordi det tilhører SØSKENET) feilaktig blitt
# lagt til som om det var personens eget lagnavn, og gjort matchingen tvetydig for
# begge barna.
_EXCLUSION_RE = re.compile(r'\bIKKE\b[^.]*\.?', re.IGNORECASE)


def extract_child_team_names(family_context: str) -> dict[str, list[str]]:
    """Trekker ut alle anførselstegn-navn per person fra familieprofilen."""
    if not family_context:
        return {}
    headers = list(_HEADER_RE.finditer(family_context))
    result: dict[str, list[str]] = {}
    for i, h in enumerate(headers):
        name = h.group(1)
        start = h.end()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(family_context)
        section = _EXCLUSION_RE.sub(' ', family_context[start:end])
        names = _QUOTED_RE.findall(section)
        if names:
            result.setdefault(name, [])
            result[name].extend(n.strip() for n in names if n.strip())
    return result


def match_child_from_group(group_name: str, family_context: str) -> str:
    """Returnerer navnet på barnet et gruppenavn tydelig tilhører, basert KUN på
    anførselstegn-navnene i familieprofilen - eller "" hvis ingen/flere barn matcher.
    Matcher case-insensitivt, og godtar at det ene er en delstreng av det andre (så
    "Lillegutt 2015 Ready" og "lillegutt 2015" matcher hverandre begge veier)."""
    if not group_name or not family_context:
        return ""
    children = extract_child_team_names(family_context)
    if not children:
        return ""
    group_lower = group_name.strip().lower()
    if not group_lower:
        return ""

    matched = []
    for child, names in children.items():
        for name in names:
            name_lower = name.strip().lower()
            if not name_lower:
                continue
            if name_lower == group_lower or name_lower in group_lower or group_lower in name_lower:
                matched.append(child)
                break
    matched = list(dict.fromkeys(matched))  # unike, behold rekkefølge
    return matched[0] if len(matched) == 1 else ""
