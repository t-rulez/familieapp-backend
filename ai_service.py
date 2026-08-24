import os
import json
import logging
import httpx
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-5"


async def _call_claude(system: str, user: str, max_tokens: int = 500, user_api_key: str = None) -> str:
    """Kaller Claude API med automatisk retry ved overbelastning (529) eller rate limit (429)."""
    import asyncio
    api_key = user_api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY er ikke satt")

    delays = [2, 5, 15]  # sekunder mellom forsøk

    for attempt, delay in enumerate(delays + [None], 1):
        try:
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

                if response.status_code in (429, 529):
                    if delay is not None:
                        logger.warning(f"API overbelastet ({response.status_code}), venter {delay}s (forsok {attempt}/3)...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        raise RuntimeError(f"API fortsatt utilgjengelig etter 3 forsok ({response.status_code})")

                response.raise_for_status()
                data = response.json()
                usage = data.get("usage", {})
                tokens_in = usage.get("input_tokens", 0)
                tokens_out = usage.get("output_tokens", 0)
                text = data["content"][0]["text"].strip()
                return text, tokens_in, tokens_out

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            if delay is not None:
                logger.warning(f"Nettverksfeil, venter {delay}s (forsok {attempt}/3): {e}")
                await asyncio.sleep(delay)
            else:
                raise

    raise RuntimeError("API-kall feilet etter alle forsok")


async def generate_tldr(title: str, body: str, source_label: str, user_id: str = None) -> str:
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

        tldr, tokens_in, tokens_out = await _call_claude(system, user, max_tokens=150)
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
    user_profile: str | None,
    user_id: str = None,
    user_api_key: str = None
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
- "high": tydelig relevant – angår direkte denne familien (barnets navn, spesifikk aktivitet, viktig beskjed)
- "medium": kan være relevant, men usikkert
- "low": generell diskusjon, informasjon til mange, ikke spesifikt for denne familien
- auto_skip: sett true hvis "low" score OG meldingen er generell diskusjon, avstemning, eller masseutsendelse uten spesifikk info
- Vær STRENG: generelle diskusjoner i foreldregrupper, "hvem kan kjøre?" og lignende koordineringsmeldinger uten svar = low + auto_skip
- Viktige beskjeder om tid/sted/påmelding = medium eller high selv om de er brede"""

        response, tokens_in, tokens_out = await _call_claude(system, user_prompt, max_tokens=200, user_api_key=user_api_key)

        # Parse JSON-svar
        # Finn JSON-blokken i svaret
        start = response.find("{")
        end = response.rfind("}") + 1
        if start >= 0 and end > start:
            result = json.loads(response[start:end])
            if user_id:
                try:
                    from database import log_api_usage
                    await log_api_usage(user_id, message.get("source","unknown"), "scoring", tokens_in, tokens_out)
                except Exception:
                    pass
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

        profile, _, _ = await _call_claude(system, user_prompt, max_tokens=150)
        return profile.strip('"\'')

    except Exception as e:
        logger.error(f"Profilgenerering feilet: {e}")
        return existing_profile
