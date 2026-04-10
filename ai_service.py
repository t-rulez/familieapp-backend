import os
import json
import logging
import httpx
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-4-20250514"


async def _call_claude(system: str, user: str, max_tokens: int = 500) -> str:
    """Kaller Claude API og returnerer tekstsvaret."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY er ikke satt")

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": MODEL,
                "max_tokens": max_tokens,
                "system": system,
                "messages": [{"role": "user", "content": user}]
            }
        )
        response.raise_for_status()
        data = response.json()
        return data["content"][0]["text"].strip()


async def generate_tldr(title: str, body: str, source_label: str) -> str:
    """
    Genererer en kort TL;DR for en melding.
    Returnerer original TL;DR (første 150 tegn) hvis API-kall feiler.
    """
    if not body or len(body) < 80:
        return body[:150]

    try:
        system = """Du er en assistent som hjelper travle foreldre med å holde oversikt over 
meldinger fra skole, idrettslag og foreldregrupper. 
Skriv alltid på norsk. Vær kortfattet og konkret."""

        user = f"""Oppsummer denne meldingen i 1-2 setninger (maks 120 tegn).
Fokuser på: hva skjer, når, og om det krever handling.

Kilde: {source_label}
Tittel: {title}
Melding: {body[:800]}

Svar med bare oppsummeringsteksten, ingen forklaring."""

        tldr = await _call_claude(system, user, max_tokens=150)
        # Rens eventuelle anførselstegn
        tldr = tldr.strip('"\'')
        return tldr[:200]

    except Exception as e:
        logger.error(f"TL;DR-generering feilet: {e}")
        # Fallback til første 150 tegn
        return body[:150].rsplit(" ", 1)[0] + "..."


async def score_relevance(
    message: dict,
    read_examples: list[dict],
    skipped_examples: list[dict],
    user_profile: str | None
) -> dict:
    """
    Vurderer relevansen av en melding basert på brukerens tidligere valg.
    
    Returnerer:
        {
            "score": "high" | "medium" | "low",
            "auto_skip": bool,
            "reason": str
        }
    """
    # Ikke nok data til å lære ennå
    total_examples = len(read_examples) + len(skipped_examples)
    if total_examples < 5:
        return {"score": "medium", "auto_skip": False, "reason": "For lite data til å vurdere"}

    try:
        system = """Du er et filtreringssystem for en familie-app som samler meldinger fra 
skole, idrettslag og foreldregrupper. Din jobb er å vurdere om en melding er 
relevant for denne spesifikke brukeren basert på deres tidligere valg.

Svar ALLTID med gyldig JSON og ingenting annet."""

        # Bygg eksempel-seksjoner
        read_section = ""
        if read_examples:
            examples = [f"- [{m['source_label']}] {m['title']}: {m['tldr']}" 
                       for m in read_examples[:15]]
            read_section = "MELDINGER BRUKEREN HAR LEST (interessant):\n" + "\n".join(examples)

        skip_section = ""
        if skipped_examples:
            examples = [f"- [{m['source_label']}] {m['title']}: {m['tldr']}" 
                       for m in skipped_examples[:15]]
            skip_section = "MELDINGER BRUKEREN HAR IGNORERT (ikke relevant):\n" + "\n".join(examples)

        profile_section = ""
        if user_profile:
            profile_section = f"BRUKERPROFIL (oppsummert fra tidligere valg):\n{user_profile}"

        user_prompt = f"""{profile_section}

{read_section}

{skip_section}

NY MELDING SOM SKAL VURDERES:
Kilde: {message.get('source_label', '')}
Kategori: {message.get('category', '')}
Tittel: {message.get('title', '')}
Innhold: {message.get('tldr', '')}

Vurder om denne meldingen sannsynligvis er relevant for brukeren.

Svar med JSON i dette formatet:
{{
  "score": "high" | "medium" | "low",
  "auto_skip": true | false,
  "reason": "kort forklaring på norsk (maks 80 tegn)"
}}

Regler:
- "high": tydelig relevant basert på brukerens mønster
- "medium": usikker, vis til brukeren
- "low": tydelig ikke relevant basert på brukerens mønster  
- auto_skip: sett true KUN hvis du er meget sikker (low score + klart mønster)
- Vær konservativ – tvilstilfeller skal alltid vises til brukeren"""

        response = await _call_claude(system, user_prompt, max_tokens=200)

        # Parse JSON-svar
        # Finn JSON-blokken i svaret
        start = response.find("{")
        end = response.rfind("}") + 1
        if start >= 0 and end > start:
            result = json.loads(response[start:end])
            return {
                "score": result.get("score", "medium"),
                "auto_skip": result.get("auto_skip", False),
                "reason": result.get("reason", "")
            }

    except Exception as e:
        logger.error(f"Relevansvurdering feilet: {e}")

    return {"score": "medium", "auto_skip": False, "reason": "Vurdering feilet"}


async def generate_user_profile(
    read_examples: list[dict],
    skipped_examples: list[dict],
    existing_profile: str | None
) -> str | None:
    """
    Genererer/oppdaterer en tekstlig brukerprofil basert på klassifiseringer.
    Kjøres når brukeren har gjort nok valg (20+).
    """
    total = len(read_examples) + len(skipped_examples)
    if total < 20:
        return existing_profile

    try:
        system = """Du lager korte brukerprofiler for et filtreringssystem. 
Skriv på norsk. Vær konkret og presis."""

        read_list = [f"- [{m['source_label']}] {m['title']}" for m in read_examples[:25]]
        skip_list = [f"- [{m['source_label']}] {m['title']}" for m in skipped_examples[:25]]

        existing = f"\nEksisterende profil (oppdater denne):\n{existing_profile}" if existing_profile else ""

        user_prompt = f"""Basert på disse meldingene en bruker har klassifisert, lag en kort profil 
(maks 200 tegn) som beskriver hva brukeren bryr seg om og ikke.{existing}

LEST (interessant):
{chr(10).join(read_list)}

IGNORERT (ikke relevant):
{chr(10).join(skip_list)}

Skriv en kort profil som starter med "Brukeren bryr seg om:" og inkluderer "Ignorerer:"
Eksempel: "Bryr seg om: avlysninger, foreldremøter, dugnad. Ignorerer: generelle nyhetsbrev, rutinemeldinger."
"""

        profile = await _call_claude(system, user_prompt, max_tokens=150)
        return profile.strip('"\'')

    except Exception as e:
        logger.error(f"Profilgenerering feilet: {e}")
        return existing_profile
