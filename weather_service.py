"""Værvarsel fra MET Norway (Yr) med lagring per dato.

Yr har ikke et eget API - Yr er MET Norways tjeneste, og dataene hentes fra
api.met.no (Locationforecast 2.0).

Tre krav fra MET sine vilkår som koden må overholde:
  1. En identifiserende User-Agent med kontaktinfo. Uidentifisert trafikk kan
     bli blokkert uten varsel, og falske/tilfeldige strenger regnes som misbruk.
  2. Respektere "Expires" og sende "If-Modified-Since" (med nøyaktig samme verdi
     som forrige "Last-Modified"), slik at uendrede data ikke hentes på nytt.
  3. Kreditering etter CC BY 4.0 - vises i frontend.
Maks 4 desimaler i lat/lon; flere gir 403 på nyere produkter.

LAGRING: Varselet gjelder bare framover. For at mandag fortsatt skal vise
sol-ikonet sitt når man er kommet til tirsdag, lagres hver dag i databasen.
Rader for datoer som har passert oppdateres ALDRI igjen - de fryses med den
siste verdien de fikk mens dagen fortsatt pågikk. Det som fryses er altså
siste varsel for dagen, ikke en observasjon av hva været faktisk ble.
"""

import os
import logging
from datetime import datetime, timezone, date, timedelta

import httpx

from database import get_pool

logger = logging.getLogger(__name__)

MET_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"

# Oslo. Maks 4 desimaler (MET gir 403 ved 5 eller flere).
WEATHER_LAT = float(os.environ.get("WEATHER_LAT", "59.9139"))
WEATHER_LON = float(os.environ.get("WEATHER_LON", "10.7522"))

# MET krever en identifiserende User-Agent med kontaktpunkt. Kan overstyres med
# miljøvariabel hvis kontaktadressen endres.
USER_AGENT = os.environ.get(
    "WEATHER_USER_AGENT",
    "BriefMessages/1.0 (https://briefmessages.app)"
)

# Hentes ikke oftere enn dette, uavhengig av hvor ofte endepunktet kalles.
MIN_FETCH_INTERVAL_MIN = 30

# Modulnivå-cache for betinget henting (If-Modified-Since) og rate-begrensning.
_last_modified: str | None = None
_last_fetch_at: datetime | None = None


# ─── Symbol -> emoji ─────────────────────────────────────────────────────────

def symbol_to_emoji(symbol_code: str) -> str:
    """Oversetter MET sin symbolkode til ett emoji.

    Kodene ser ut som 'clearsky_day', 'lightrainshowers_night',
    'heavysnowandthunder'. Suffikset _day/_night/_polartwilight fjernes, og
    resten matches på nøkkelord. Nøkkelord-matching (framfor en fast liste med
    ~100 koder) gjør at nye eller uventede koder fortsatt gir et fornuftig
    ikon. Rekkefølgen er viktig: 'lightrainshowersandthunder' inneholder både
    regn og torden, og torden er det mest informative å vise.
    """
    if not symbol_code:
        return ""
    s = symbol_code.lower()
    for suffix in ("_day", "_night", "_polartwilight"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break

    if "thunder" in s:
        return "⛈️"
    if "snow" in s:
        return "🌨️" if "light" in s else "❄️"
    if "sleet" in s:
        return "🌨️"
    if "rain" in s:
        if "heavy" in s:
            return "🌧️"
        if "light" in s or "showers" in s:
            return "🌦️"
        return "🌧️"
    if "fog" in s:
        return "🌫️"
    if s == "cloudy":
        return "☁️"
    if "partlycloudy" in s:
        return "⛅"
    if "fair" in s:
        return "🌤️"
    if "clearsky" in s:
        return "☀️"
    return "🌡️"


# ─── Tabell ──────────────────────────────────────────────────────────────────

async def init_weather_table():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS weather_daily (
                date        DATE PRIMARY KEY,
                symbol      TEXT NOT NULL,
                emoji       TEXT NOT NULL,
                temp_max    REAL,
                temp_min    REAL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
    logger.info("[VÆR] Værtabell initialisert")


# ─── Henting og tolkning ─────────────────────────────────────────────────────

async def _fetch_forecast() -> dict | None:
    """Henter varselet fra MET. Returnerer None hvis uendret (HTTP 304) eller feil."""
    global _last_modified, _last_fetch_at

    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if _last_modified:
        headers["If-Modified-Since"] = _last_modified

    params = {"lat": round(WEATHER_LAT, 4), "lon": round(WEATHER_LON, 4)}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.get(MET_URL, params=params, headers=headers)
    except Exception as e:
        logger.warning(f"[VÆR] Henting feilet: {e}")
        return None

    _last_fetch_at = datetime.now(timezone.utc)

    if res.status_code == 304:
        logger.info("[VÆR] Uendret siden sist (304)")
        return None
    if res.status_code != 200:
        logger.warning(f"[VÆR] MET svarte {res.status_code}: {res.text[:200]}")
        return None

    _last_modified = res.headers.get("Last-Modified") or _last_modified
    return res.json()


def _parse_days(payload: dict) -> dict[date, dict]:
    """Samler timesvarselet til én oppsummering per dato.

    Symbolet hentes fra midt på dagen (nærmest kl. 12), som er det Yr selv
    bruker som representativt dagssymbol. Temperaturene er høyeste og laveste
    verdi gjennom hele døgnet.
    """
    per_dag: dict[date, dict] = {}

    for entry in payload.get("properties", {}).get("timeseries", []):
        tid = entry.get("time")
        if not tid:
            continue
        try:
            dt = datetime.fromisoformat(tid.replace("Z", "+00:00"))
        except ValueError:
            continue

        d = dt.date()
        bøtte = per_dag.setdefault(d, {
            "symbol": "", "symbol_time_diff": None,
            "temp_max": None, "temp_min": None,
        })

        detaljer = entry.get("data", {}).get("instant", {}).get("details", {})
        temp = detaljer.get("air_temperature")
        if temp is not None:
            if bøtte["temp_max"] is None or temp > bøtte["temp_max"]:
                bøtte["temp_max"] = temp
            if bøtte["temp_min"] is None or temp < bøtte["temp_min"]:
                bøtte["temp_min"] = temp

        # Symbolet ligger under next_1_hours / next_6_hours / next_12_hours,
        # avhengig av hvor langt fram i tid punktet er.
        data = entry.get("data", {})
        symbol = ""
        for blokk in ("next_1_hours", "next_6_hours", "next_12_hours"):
            kode = data.get(blokk, {}).get("summary", {}).get("symbol_code")
            if kode:
                symbol = kode
                break

        if symbol:
            diff = abs(dt.hour - 12)
            if bøtte["symbol_time_diff"] is None or diff < bøtte["symbol_time_diff"]:
                bøtte["symbol"] = symbol
                bøtte["symbol_time_diff"] = diff

    return per_dag


async def _store_days(per_dag: dict[date, dict]) -> int:
    """Lagrer dager fra og med i dag. Passerte datoer røres ikke.

    Fryselogikken ligger i WHERE-leddet på DO UPDATE: en eksisterende rad
    oppdateres kun hvis datoen ikke har passert.
    """
    i_dag = datetime.now(timezone.utc).date()
    lagret = 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Rydd bort vær fra tidligere uker. Grensen er mandag i inneværende uke,
        # slik at dager som alt har passert DENNE uken beholdes (mandag skal
        # fortsatt vise sitt ikon når man er kommet til fredag) - mens forrige
        # ukes rader forsvinner av seg selv så snart en ny uke begynner.
        mandag_denne_uken = i_dag - timedelta(days=i_dag.weekday())
        slettet = await conn.execute(
            "DELETE FROM weather_daily WHERE date < $1", mandag_denne_uken
        )
        if slettet and slettet != "DELETE 0":
            logger.info(f"[VÆR] Ryddet bort vær fra tidligere uker ({slettet})")

        for d, v in sorted(per_dag.items()):
            if d < i_dag or not v.get("symbol"):
                continue
            await conn.execute("""
                INSERT INTO weather_daily (date, symbol, emoji, temp_max, temp_min, updated_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (date) DO UPDATE SET
                    symbol     = EXCLUDED.symbol,
                    emoji      = EXCLUDED.emoji,
                    temp_max   = EXCLUDED.temp_max,
                    temp_min   = EXCLUDED.temp_min,
                    updated_at = NOW()
                WHERE weather_daily.date >= CURRENT_DATE
            """, d, v["symbol"], symbol_to_emoji(v["symbol"]),
                 v.get("temp_max"), v.get("temp_min"))
            lagret += 1
    return lagret


async def refresh_weather(force: bool = False) -> int:
    """Henter nytt varsel og lagrer det. Returnerer antall dager lagret."""
    if not force and _last_fetch_at:
        alder_min = (datetime.now(timezone.utc) - _last_fetch_at).total_seconds() / 60
        if alder_min < MIN_FETCH_INTERVAL_MIN:
            return 0

    payload = await _fetch_forecast()
    if not payload:
        return 0

    per_dag = _parse_days(payload)
    lagret = await _store_days(per_dag)
    logger.info(f"[VÆR] Oppdatert {lagret} dager (Oslo)")
    return lagret


# ─── Oppslag ─────────────────────────────────────────────────────────────────

async def get_week_weather(monday: date) -> list[dict]:
    """Henter lagret vær for en uke (mandag til søndag).

    Dager uten lagret vær får tomme felt - det gjelder dager som passerte før
    denne funksjonaliteten ble tatt i bruk, og dager for langt fram i tid til
    at varselet dekker dem.
    """
    sunday = monday + timedelta(days=6)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT date, symbol, emoji, temp_max, temp_min
            FROM weather_daily
            WHERE date >= $1 AND date <= $2
        """, monday, sunday)

    per_dato = {r["date"]: r for r in rows}
    resultat = []
    for i in range(7):
        d = monday + timedelta(days=i)
        r = per_dato.get(d)
        resultat.append({
            "date": d.isoformat(),
            "emoji": r["emoji"] if r else "",
            "symbol": r["symbol"] if r else "",
            "temp_max": round(r["temp_max"]) if r and r["temp_max"] is not None else None,
            "temp_min": round(r["temp_min"]) if r and r["temp_min"] is not None else None,
        })
    return resultat
