import os
import asyncio
import logging
from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo

import httpx

OSLO_TZ = ZoneInfo("Europe/Oslo")

# Skru av støy fra tredjeparts biblioteker
logging.getLogger("icalendar").setLevel(logging.CRITICAL)
logging.getLogger("caldav").setLevel(logging.CRITICAL)
logging.getLogger("httpx").setLevel(logging.WARNING)

class _NoiseFilter(logging.Filter):
    """Filtrer bort støy-endepunkter fra uvicorn access-logg."""
    _SKIP = {"/ai/history", "/messages", "/health"}
    def filter(self, record):
        msg = record.getMessage()
        return not any(s in msg for s in self._SKIP)

logging.getLogger("uvicorn.access").addFilter(_NoiseFilter())

class _IcalNoiseFilter(logging.Filter):
    """Den harmløse 'Ical data was modified ... breaks the icalendar standard'-meldingen
    (og selve diff-dumpen biblioteket logger ved siden av den) kommer fra icalendar-
    biblioteket via logging.error() uten eget loggernavn, så den lander på selve
    root-loggeren og unngår setLevel()-kallene over. Filtrer den bort her i stedet,
    uten å påvirke andre feilmeldinger som også skulle gå gjennom root."""
    _MARKERS = ("Ical data was modified", "breaks the icalendar standard")
    def filter(self, record):
        msg = record.getMessage()
        if any(m in msg for m in self._MARKERS):
            return False
        # Selve diff-dumpen av iCal-fiksen: en unified diff som nevner VEVENT/DTSTAMP
        if msg.startswith("---") and ("BEGIN:VEVENT" in msg or "DTSTAMP:" in msg):
            return False
        return True

logging.getLogger().addFilter(_IcalNoiseFilter())
from contextlib import asynccontextmanager
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, EmailStr

from spond_service import SpondService
from email_service import EmailService
from database import (
    init_db, init_ai_tables, init_push_tables,
    get_api_usage_stats, get_pool,
    upsert_messages, get_all_messages, update_message_status,
    get_stats, get_classified_messages, get_user_profile,
    save_user_profile, save_ai_result, get_unprocessed_messages,
    save_push_subscription, delete_push_subscription, get_push_subscriptions,
    count_unread_messages, skip_unprocessed_messages
)
from push_service import send_push_to_user
from users import (
    init_user_tables, create_user, get_user_by_email, get_user_by_id,
    update_last_login, count_users, get_user_settings, get_user_settings_safe,
    update_user_settings, get_all_active_users
)
from auth import (
    verify_password, create_token, decode_token
)
from ai_service import generate_tldr, score_relevance, generate_user_profile, _extract_text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

last_sync: Optional[str] = None

# Delt regel brukt i alle AI-prompter (ukesoversikt, AI-assistent, kveldssammendrag):
# kalenderen inneholder ofte en generisk kopi av Spond-aktiviteter uten lagnavn.
# Spond-meldingen selv VET hvilket lag/barn det gjelder, og skal derfor overstyre.
def _spond_meta(m: dict) -> dict:
    """Henter ut meta-feltet fra en meldings-rad som et dict, uansett om det kom fra
    databasen som json-tekst eller allerede som dict."""
    meta = m.get("meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json
            return _json.loads(meta)
        except Exception:
            return {}
    return meta or {}


def _spond_child_name(m: dict, family_ctx: str) -> str:
    """Returnerer BARE barnets navn (uten "Gjelder: "-prefiks) for en Spond-melding,
    hvis gruppenavnet (meta.group, med category som fallback) tydelig matcher ett
    barns lagnavn i familieprofilen - ellers "". Kjøres LIVE ved hvert søk/spørring
    (ikke ved innlesing fra Spond), slik at den alltid bruker den nåværende
    familieprofilen - ingen "gammel data"-problem hvis et lagnavn endres, og ingen
    egen backfill-jobb trengs."""
    if m.get("source") != "spond" or not family_ctx:
        return ""
    meta = _spond_meta(m)
    group_name = meta.get("group", "") or m.get("category", "")
    from family_profile import match_child_from_group
    return match_child_from_group(group_name, family_ctx)


def _spond_child_tag(m: dict, family_ctx: str) -> str:
    """Returnerer "Gjelder: <barn>" (se _spond_child_name), eller "" hvis ingen
    tydelig match."""
    child = _spond_child_name(m, family_ctx)
    return f"Gjelder: {child}" if child else ""


def _spond_event_start(m: dict) -> Optional[datetime]:
    """Starttidspunktet til et Spond-arrangement (meta.eventStart), hvis det finnes.
    Vanlige chat-meldinger fra Spond har ikke dette feltet - bare arrangementer."""
    ts = _spond_meta(m).get("eventStart")
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _tag_calendar_events_with_spond(events: list[dict], spond_rows: list[dict], family_ctx: str) -> list[dict]:
    """Merker hver kalenderoppføring med hvilket barn den gjelder (feltet "child"),
    HVIS den tidsmessig faller sammen med et Spond-arrangement som selv er tydelig
    tagget til ett barn (via family_profile.py).

    Dette gjøres HER, i kode, og ALDRI av AI-en: tidligere ba vi AI-en selv
    gjenkjenne at en kalenderoppføring og en Spond-melding var "samme aktivitet" og
    overføre riktig barn - det førte til ustabile og inkonsekvente resultater
    (samme type feil som da vi hardkodet laginfo, bare flyttet til prompten i
    stedet for koden). Nå er svaret allerede riktig når teksten når AI-en, som bare
    skal lese av "(Gjelder: <barn>)" direkte - se format_events_for_ai og
    SPOND_CALENDAR_RULE.

    Matcher KUN på tidspunkt (innenfor et SMALT vindu), ikke på tittel/tekst -
    tittelen alene (f.eks. bare "Trening") er ikke pålitelig for å skille mellom
    barna. Vinduet må være smalt (se MATCH_WINDOW_MINUTES) - et bredt vindu (testet
    med 90 min) ga falske treff: en helt urelatert avtale samme formiddag (en
    veterinær-vaksinasjon, en av foreldrenes egen trening) ble feilaktig tagget med
    et barn bare fordi den lå i nærheten i tid av en barneaktivitet. Blant flere
    treff innenfor vinduet velges alltid det NÆRMESTE i tid, ikke det første funnet."""
    MATCH_WINDOW_MINUTES = 25

    tagged_spond = []
    for m in spond_rows:
        if m.get("source") != "spond":
            continue
        child = _spond_child_name(m, family_ctx)
        if not child:
            continue
        start = _spond_event_start(m)
        if not start:
            continue
        tagged_spond.append((start, child))

    logger.info(
        f"[UKESOVERSIKT] Spond-rader med kjent barn+tidspunkt (brukes til kalender-matching): "
        f"{[(c, s.isoformat()) for s, c in tagged_spond]}"
    )
    if not tagged_spond:
        return events

    result = []
    for e in events:
        ev = dict(e)
        ev_start = None
        if e.get("start"):
            try:
                ev_start = datetime.fromisoformat(e["start"])
            except Exception:
                ev_start = None
        if ev_start:
            best = None
            for sp_start, child in tagged_spond:
                delta = abs((ev_start - sp_start).total_seconds()) / 60
                if delta <= MATCH_WINDOW_MINUTES and (best is None or delta < best[0]):
                    best = (delta, child)
            if best:
                ev["child"] = best[1]
        result.append(ev)

    # To kalenderoppføringer med NØYAKTIG samme tittel samme dag (f.eks. to
    # påfølgende treningsøkter samme kveld, i to tidsluker) er med overveldende
    # sannsynlighet samme gjentakende aktivitet - gi dem samme barn-tag selv om
    # bare én av dem lå nær nok et Spond-tidspunkt til å bli direkte matchet. Dette
    # er EKSAKT tekstlikhet (ikke en gjetning på innhold), og kompenserer for at det
    # smale tidsvinduet over ellers ville mistet den andre økten.
    by_key: dict[tuple, list[dict]] = {}
    for ev in result:
        start = ev.get("start") or ""
        key = (start[:10], ev.get("summary", "").strip().lower())
        by_key.setdefault(key, []).append(ev)
    for group in by_key.values():
        children = {ev["child"] for ev in group if ev.get("child")}
        if len(children) == 1:
            only_child = next(iter(children))
            for ev in group:
                ev.setdefault("child", only_child)

    return result


SPOND_CALENDAR_RULE = (
    "VIKTIG REGEL om kalender vs. Spond, og om hvem en aktivitet gjelder: dette er ALLEREDE "
    "regnet ut av et program før teksten under ble satt sammen - IKKE noe du selv skal tolke, "
    "gjette eller overstyre.\n"
    "  - En Spond-melding/arrangement som har en linje 'Gjelder: <barn>' gjelder KUN det "
    "barnet. Mangler linjen helt, er det ikke mulig å si hvem det gjelder - behandle den da "
    "som felles/ukjent, gjett ALDRI basert på tittel, kategori eller noe annet.\n"
    "  - En KALENDER-oppføring som har '(Gjelder: <barn>)' i teksten (dette er lagt til fordi "
    "programmet har funnet at den faller sammen i tid med en tagget Spond-oppføring) gjelder "
    "KUN det barnet. En kalenderoppføring UTEN denne taggen er felles/ukjent, selv om du tror "
    "den kan være en kopi av en Spond-aktivitet - ikke prøv å koble den til noe selv.\n"
    "  - En KALENDER-oppføring kan vare over flere dager (teksten viser da både start- og "
    "sluttdag/-tid, f.eks. 'fredag 20. nov kl. 17:00 til søndag 22. nov kl. 18:00'). Slike "
    "arrangementer skal vises på HVER ENKELT dag i det tidsrommet (fredag OG lørdag OG "
    "søndag i eksempelet), ikke bare på startdagen.\n"
    "  - En kalenderoppføring som er felles/ukjent (ingen '(Gjelder: <barn>)'-tag, og "
    "tittelen nevner heller ikke noe barn direkte) skal ALDRI utelates. Den skal vises under "
    "BÅDE Kristin OG Truls (ikke under Gustav eller Rasmus) - det er standard-plasseringen "
    "for avtaler som ikke er spesifikt knyttet til ett barn, f.eks. 'Julebord', 'Foreldremøte' "
    "eller andre familie-/foreldre-arrangementer.\n"
    "VIKTIG om klokkeslett: bruk ALLTID klokkeslettet NØYAKTIG som det står i kilden (kalender "
    "eller Spond) - f.eks. '16:50' skal forbli '16:50', ALDRI rundes av til '17:00' eller annet "
    "'pent' tidspunkt. Kopier minuttallet ordrett, ikke avrund eller anslå."
)

# ─── Auth-avhengighet ─────────────────────────────────────────────────────────

async def get_current_user(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Ikke autentisert")
    token = authorization.replace("Bearer ", "")
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Ugyldig eller utløpt token")
    user = await get_user_by_id(payload["sub"])
    if not user:
        raise HTTPException(status_code=401, detail="Bruker ikke funnet")
    return user

CurrentUser = Annotated[dict, Depends(get_current_user)]

# ─── AI-behandling ────────────────────────────────────────────────────────────

async def _scoring_enabled(user_id: str) -> bool:
    """Er AI-scoring skrudd på for denne brukeren? (på som standard)"""
    try:
        settings = await get_user_settings(user_id) or {}
        return settings.get("ai_scoring_enabled") is not False
    except Exception:
        return True


async def run_ai_processing(user_id: str):
    # AI-scoring av: arkiver nye meldinger som «Ikke relevant» uten å kalle Claude
    if not await _scoring_enabled(user_id):
        n = await skip_unprocessed_messages(user_id)
        if n:
            logger.info(f"[AI] Scoring av – {n} nye meldinger arkivert som ikke relevant")
        return
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return
    unprocessed = await get_unprocessed_messages(limit=20, user_id=user_id)
    if not unprocessed:
        return
    logger.info(f"[AI] Behandler {len(unprocessed)} meldinger for bruker {user_id[:8]}...")
    read_examples, skipped_examples = await get_classified_messages(user_id=user_id)
    user_profile = await get_user_profile(user_id=user_id)
    for msg in unprocessed:
        try:
            tldr = await generate_tldr(msg["title"], msg.get("body", ""), msg.get("source_label", ""), user_id=user_id)
            relevance = await score_relevance(msg, read_examples, skipped_examples, user_profile, user_id=user_id)
            await save_ai_result(msg["id"], tldr, relevance["score"], relevance["reason"], relevance["auto_skip"])
            src = msg.get('source','').upper()
            logger.info(f"[AI/{src}] [{relevance['score']}] {msg['title'][:50]}")
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"AI-behandling feilet for {msg.get('id')}: {e}")
    try:
        total = len(read_examples) + len(skipped_examples)
        if total >= 20:
            new_profile = await generate_user_profile(read_examples, skipped_examples, user_profile)
            if new_profile and new_profile != user_profile:
                await save_user_profile(new_profile, user_id=user_id)
    except Exception as e:
        logger.error(f"Profiloppdatering feilet: {e}")

# ─── Synk per bruker ─────────────────────────────────────────────────────────

async def sync_user(user: dict, since: Optional[datetime] = None) -> int:
    """Henter nye meldinger fra alle kilder parallelt.
    since: hvis satt, hentes bare meldinger nyere enn denne datoen.
    """
    user_id = user["id"]

    async def fetch_spond():
        if not (user.get("spond_enabled") and user.get("spond_username")):
            return []
        try:
            logger.info(f"[SPOND] Synkroniserer for {user['email']}...")
            svc = SpondService(username=user["spond_username"], password=user["spond_password"])
            msgs = await svc.fetch_all(since=since)
            for m in msgs: m["user_id"] = user_id
            # Merk: hvilket barn en Spond-aktivitet gjelder avgjøres IKKE her ved
            # innlesing, men live ved hvert søk/spørring (se _spond_child_tag i
            # main.py / family_profile.py) - det er like treffsikkert (ren
            # tekstmatching, ingen AI involvert), men unngår at gamle meldinger blir
            # stående med utdatert tagging hvis familieprofilen endres senere.
            logger.info(f"[SPOND] {len(msgs)} meldinger hentet")
            return msgs
        except Exception as e:
            logger.error(f"[SPOND] Synk feilet: {e}")
            return []

    async def fetch_showbie():
        if not (user.get("showbie_enabled") and user.get("showbie_token") and user.get("showbie_user_id")):
            return []
        try:
            from showbie_service import ShowbieService
            child_ids = [c.strip() for c in (user.get("showbie_child_ids") or "").split(",") if c.strip()]
            svc = ShowbieService(token=user["showbie_token"], user_id=user["showbie_user_id"], fingerprint=user.get("showbie_fingerprint",""), child_ids=child_ids, user_api_key=_get_api_key(user))
            # Hent eksisterende Showbie-IDer fra DB for å unngå re-prosessering
            pool = await get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id FROM messages WHERE user_id = $1 AND source = 'showbie'", user_id
                )
            existing_ids = {r["id"] for r in rows}
            logger.info(f"[SHOWBIE] {len(existing_ids)} eksisterende meldinger i DB")
            msgs = await svc.fetch_all(since=since, upsert_fn=upsert_messages, user_id=user_id, existing_ids=existing_ids)
            logger.info(f"[SHOWBIE] {len(msgs)} nye meldinger hentet")
            return msgs
        except Exception as e:
            logger.error(f"[SHOWBIE] Synk feilet: {e}")
            return []

    async def fetch_email(num):
        if not (user.get(f"email_{num}_enabled") and user.get(f"email_{num}_username")):
            return []
        label = config_label = _build_email_config(user, num).get("source_label", f"E-post {num}") if (user.get(f"email_{num}_enabled") and user.get(f"email_{num}_username")) else f"E-post {num}"
        try:
            config = _build_email_config(user, num)
            label = config.get("source_label", f"E-post {num}").upper()
            svc = EmailService(config)
            msgs = await asyncio.to_thread(svc.fetch_all)
            for m in msgs: m["user_id"] = user_id
            logger.info(f"[{label}] {len(msgs)} meldinger hentet")
            return msgs
        except Exception as e:
            logger.error(f"[E-POST {num}] Synk feilet: {e}")
            return []

    # Kjør alle kilder parallelt
    results = await asyncio.gather(
        fetch_spond(),
        fetch_email(1),
        fetch_showbie(),
        return_exceptions=True
    )

    all_messages = []
    for r in results:
        if isinstance(r, list):
            all_messages.extend(r)

    inserted_ids = [m["id"] for m in all_messages]
    await upsert_messages(all_messages)

    if inserted_ids:
        asyncio.create_task(_ai_then_notify(user_id, inserted_ids))

    return len(all_messages)


async def _ai_then_notify(user_id: str, inserted_ids: list):
    """Kjør AI-behandling, send push kun for nye meldinger AI beholdt som uleste."""
    await run_ai_processing(user_id)
    # Tell hvor mange av de nye meldingene som fortsatt er uleste etter AI
    if not inserted_ids:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM messages WHERE id = ANY($1::text[]) AND status = 'unread'",
            inserted_ids
        )
    if count and count > 0:
        await notify_new_messages(user_id, count)


def _build_email_config(user: dict, num: int) -> dict:
    return {
        "host": user.get(f"email_{num}_host", "imap.one.com"),
        "port": user.get(f"email_{num}_port", 993),
        "username": user.get(f"email_{num}_username", ""),
        "password": user.get(f"email_{num}_password", ""),
        "from_filter": user.get(f"email_{num}_from_filter", ""),
        "subject_filter": user.get(f"email_{num}_subject_filter", ""),
        "delete_after_fetch": user.get(f"email_{num}_delete_after", False),
        "source_label": user.get(f"email_{num}_source_label", "Skolemelding"),
        "source": user.get(f"email_{num}_source", "skolemelding"),
        "category": user.get(f"email_{num}_category", "skole"),
    }


async def run_sync_all():
    global last_sync
    users = await get_all_active_users()
    logger.info(f"Synkroniserer {len(users)} brukere...")
    tasks = [sync_user(u) for u in users]
    await asyncio.gather(*tasks, return_exceptions=True)
    last_sync = datetime.now(timezone.utc).isoformat()
    logger.info("Synk ferdig for alle brukere")


async def sync_loop():
    import pytz as _pytz
    _oslo = _pytz.timezone("Europe/Oslo")
    while True:
        try:
            await run_sync_all()
        except Exception as e:
            logger.error(f"Synkronisering feilet: {e}")
        # Natt (23:00-06:00): 90 min, dag: 30 min
        hour = datetime.now(_oslo).hour
        is_night = hour >= 23 or hour < 6
        interval = 90 * 60 if is_night else 30 * 60
        logger.info(f"[SYNC] Neste synk om {interval//60} min ({'natt' if is_night else 'dag'})")
        await asyncio.sleep(interval)


async def evening_summary_loop():
    """Sender kveldssammendrag kl 20:00 norsk tid hver dag."""
    import pytz
    oslo_tz = pytz.timezone("Europe/Oslo")
    # Format: {uid: "2026-04-15"} – dato for siste sendte varsel per bruker
    last_sent = {}

    while True:
        try:
            now_oslo = datetime.now(oslo_tz)
            today_key = now_oslo.strftime("%Y-%m-%d")

            users = await get_all_active_users()
            for user in users:
                if not user.get("evening_summary_enabled"):
                    continue
                uid = user["id"]
                if last_sent.get(uid) == today_key:
                    continue
                # Hent konfigurert tidspunkt – kun hele timer
                time_str = user.get("evening_summary_time") or "20:00"
                try:
                    h = int(time_str.split(":")[0])
                except Exception:
                    h = 20
                if now_oslo.hour == h and now_oslo.minute == 0:
                    logger.info(f"[EVENING] Sender til {uid[:8]}...")
                    last_sent[uid] = today_key
                    try:
                        await send_evening_summary(user)
                    except Exception as e:
                        logger.error(f"[EVENING] Feilet for {uid}: {e}")

            # Sleep frem til neste hele time
            now_oslo = datetime.now(oslo_tz)
            mins_to_next_hour = 60 - now_oslo.minute
            secs_to_next_hour = mins_to_next_hour * 60 - now_oslo.second
            logger.debug(f"[EVENING] Neste sjekk om {mins_to_next_hour} min")
            await asyncio.sleep(secs_to_next_hour)
        except Exception as e:
            logger.error(f"[EVENING] Loop feilet: {e}")
            await asyncio.sleep(3600)


async def send_evening_summary(user: dict):
    """Genererer og sender morgendagens oversikt til en bruker."""
    import re
    from datetime import date as _date, timedelta

    uid = user["id"]
    subs = await get_push_subscriptions(uid)
    if not subs:
        return

    # Beregn morgendagens dato eksplisitt
    today = _date.today()
    tomorrow = today + timedelta(days=1)
    days_no = ["mandag","tirsdag","onsdag","torsdag","fredag","lordag","sondag"]
    months_no = ["januar","februar","mars","april","mai","juni","juli","august","september","oktober","november","desember"]
    today_str = f"{days_no[today.weekday()]} {today.day}. {months_no[today.month-1]} {today.year}"
    tomorrow_str = f"{days_no[tomorrow.weekday()]} {tomorrow.day}. {months_no[tomorrow.month-1]} {tomorrow.year}"

    family_ctx = user.get("family_context", "")
    question = f"Gi meg en kort oversikt over morgendagen ({tomorrow_str}): skole og aktiviteter. Maks 5-6 setninger. Ikke ta med forberedelser, ikke lag oppsummering. Kun kort svar."

    stopwords = {"jeg","deg","han","hun","den","det","de","vi","ikke","har","er","var",
                 "med","for","til","fra","pa","av","om","og","eller","men","som","hva","nar"}
    words = [w.lower().strip("?.,!") for w in question.split() if len(w) > 3 and w.lower() not in stopwords]

    pool = await get_pool()
    seen_ids = set()
    relevant = []

    sql = (
        "SELECT id, source, source_label, category, title, tldr, body,"
        " time_label AS time, timestamp, priority, status, meta"
        " FROM messages WHERE user_id = $1"
        # Tar med alle meldinger, også de som er markert som ikke relevant
        " AND (LOWER(title) LIKE $2 OR LOWER(tldr) LIKE $2 OR LOWER(body) LIKE $2 OR LOWER(category) LIKE $2)"
        " ORDER BY timestamp DESC LIMIT 100"
    )
    async with pool.acquire() as conn:
        for word in words:
            rows = await conn.fetch(sql, uid, f"%{word}%")
            for row in rows:
                m = dict(row)
                if m["id"] not in seen_ids:
                    seen_ids.add(m["id"])
                    relevant.append(m)

    recent = await get_all_messages(user_id=uid, limit=30)
    for m in recent:
        if m["id"] not in seen_ids:
            seen_ids.add(m["id"])
            relevant.append(m)

    def fmt(m):
        src = m.get("source_label") or m.get("source", "?")
        parts = [f"[{src} - {m.get('time','')}] {m.get('title','')}"]
        tag = _spond_child_tag(m, family_ctx)
        if tag:
            parts.append(f"  {tag}")
        if m.get("tldr"):
            parts.append(f"  Sammendrag: {m['tldr']}")
        if m.get("body") and m["body"] != m.get("tldr", ""):
            max_b = 2000 if m.get("source") == "showbie" else 800
            parts.append(f"  Innhold: {m['body'][:max_b]}")
        return "\n".join(parts)

    def score_m(m):
        text = f"{m.get('title','')} {m.get('tldr','')} {m.get('body','')}".lower()
        return sum(1 for w in words if w in text)

    relevant = sorted(relevant, key=score_m, reverse=True)[:80]
    msg_context = "\n\n".join([fmt(m) for m in relevant])

    settings = await get_user_settings(uid)
    family_ctx = settings.get("family_context", "") if settings else ""
    auto_ctx = settings.get("ai_learned_context", "") if settings else ""
    user_api_key = _get_api_key(settings) if settings else os.environ.get("ANTHROPIC_API_KEY", "")

    # Hent kalenderdata – samme som AI-assistenten
    calendar_section = ""
    if settings and settings.get("caldav_enabled") and settings.get("caldav_username") and settings.get("caldav_password"):
        try:
            from caldav_service import fetch_calendar_events, format_events_for_ai
            events = await fetch_calendar_events(
                url=settings.get("caldav_url", "https://caldav.icloud.com"),
                username=settings.get("caldav_username", ""),
                password=settings.get("caldav_password", ""),
                calendar_filter=settings.get("caldav_calendars", ""),
                days_ahead=3
            )
            events = _tag_calendar_events_with_spond(events, relevant, family_ctx)
            calendar_section = f"KALENDER:\n{format_events_for_ai(events)}\n\n"
        except Exception as e:
            logger.warning(f"[EVENING] Kalender-henting feilet: {e}")

    system = (
        f"Du er en familieassistent som gir daglige oversikter.\n"
        f"Dagens dato er {today_str}. I morgen er det {tomorrow_str}.\n"
        f"{SPOND_CALENDAR_RULE}\n\n"
        f"{('FAMILIEPROFIL: ' + family_ctx) if family_ctx else ''}\n"
        f"{('LAERT KONTEKST: ' + auto_ctx) if auto_ctx else ''}\n\n"
        f"{calendar_section}"
        f"Her er meldinger og informasjon fra familie-appen:\n{msg_context}\n\n"
        f"Gi en kort oversikt over morgendagen ({tomorrow_str}): skole og aktiviteter.\n"
        f"Maks 5-6 korte punkter. Ikke ta med forberedelser. Ikke lag oppsummering. Kun kort svar.\n"
        f"Svar pa norsk."
    )

    api_key = user_api_key
    if not api_key:
        return

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-5", "max_tokens": 150,
                  "system": system, "messages": [{"role": "user", "content": question}]}
        )
        if resp.status_code != 200:
            logger.error(f"[EVENING] API feil: {resp.status_code}")
            return
        answer = _extract_text(resp.json().get("content")).strip()

    clean = re.sub(r'\*\*(.+?)\*\*', r'\1', answer)
    clean = re.sub(r'\*(.+?)\*', r'\1', clean)
    clean = re.sub(r'#{1,3} ', '', clean)
    # Fjern dato-linjer
    clean = re.sub(r'^.{0,30}\d{1,2}\.\s*\w+\s*\d{4}[:\s]*\n?', '', clean, flags=re.MULTILINE)
    # Håndhev stikkord-format: kutt linjer over 55 tegn ved første punktum
    lines = []
    for line in clean.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if len(line) > 55:
            dot = line.find('.', 15)
            if 15 < dot < 60:
                line = line[:dot+1]
            else:
                line = line[:53] + ".."
        lines.append(line)
    clean = "\n".join(lines[:5])  # Maks 5 linjer

    logger.info(f"[EVENING] Prompt:\n---QUESTION---\n{question}\n---SYSTEM (siste 500 tegn)---\n{system[-500:]}\n---")
    logger.info(f"[EVENING] Resultat:\n{clean}")
    short_date = tomorrow.strftime("%-d. %b").lower()
    short_day = days_no[tomorrow.weekday()][:3]
    await send_push_to_user(subs, f"I morgen: {short_day} {short_date}", clean, "evening-summary")




@asynccontextmanager
async def lifespan(app: FastAPI):
    # Railways private nettverk (postgres.railway.internal) kan bruke noen
    # sekunder på å bli klart etter oppstart. Prøv databasen flere ganger
    # før vi gir opp.
    for attempt in range(1, 11):
        try:
            logger.info(f"Starter init_db (forsøk {attempt}/10)...")
            await init_db()
            break
        except Exception as e:
            if attempt == 10:
                logger.error(f"FATAL: Fikk ikke kontakt med databasen: {e}", exc_info=True)
                raise
            logger.warning(f"Database ikke klar ennå ({e}) – prøver igjen om 3 sek")
            try:
                import database as _db
                _db._pool = None
            except Exception:
                pass
            await asyncio.sleep(3)
    try:
        logger.info("Starter init_ai_tables...")
        await init_ai_tables()
        logger.info("Starter init_user_tables...")
        await init_user_tables()
        logger.info("Starter init_push_tables...")
        await init_push_tables()
        logger.info("Alle tabeller initialisert OK")
    except Exception as e:
        logger.error(f"FATAL: Tabellinitialisering feilet: {e}", exc_info=True)
        raise
    task = asyncio.create_task(sync_loop())
    task2 = asyncio.create_task(evening_summary_loop())
    yield
    task.cancel()
    task2.cancel()

# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="Familie API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Komprimer svar over 1 KB (typisk 70–85 % mindre trafikk på /messages)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# ─── Auth-endepunkter ─────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: str
    display_name: str
    password: str

class LoginRequest(BaseModel):
    email: str
    password: str

@app.post("/auth/register")
async def register(req: RegisterRequest):
    # Sjekk maks 5 brukere
    if await count_users() >= 5:
        raise HTTPException(status_code=403, detail="Maks antall brukere nådd")
    if len(req.password) < 8:
        raise HTTPException(status_code=400, detail="Passord må være minst 8 tegn")
    if len(req.password.encode('utf-8')) > 72:
        raise HTTPException(status_code=400, detail="Passord kan ikke være lengre enn 72 tegn")
    existing = await get_user_by_email(req.email)
    if existing:
        raise HTTPException(status_code=400, detail="E-postadressen er allerede i bruk")
    user = await create_user(req.email, req.display_name, req.password)
    token = create_token(user["id"], user["email"])
    return {"token": token, "user": user}



@app.post("/auth/change-password")
async def change_password(req: dict, user: CurrentUser):
    """Bytter passord for innlogget bruker."""
    from auth import verify_password, hash_password

    old_pw = req.get("old_password", "")
    new_pw = req.get("new_password", "")

    if not old_pw or not new_pw:
        raise HTTPException(status_code=400, detail="Fyll inn begge feltene")
    if len(new_pw) < 8:
        raise HTTPException(status_code=400, detail="Nytt passord må være minst 8 tegn")

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT password_hash FROM users WHERE id = $1", user["id"])
    if not row or not verify_password(old_pw, row["password_hash"]):
        raise HTTPException(status_code=400, detail="Nåværende passord er feil")

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET password_hash = $1 WHERE id = $2",
            hash_password(new_pw), user["id"]
        )
    return {"ok": True}

@app.post("/auth/forgot-password")
async def forgot_password(req: dict):
    """Sender nytt midlertidig passord på e-post."""
    import secrets, string
    from email.mime.text import MIMEText
    from auth import hash_password

    email = (req.get("email") or "").strip().lower()
    if not email:
        return {"ok": False, "error": "E-post mangler"}

    # Finn bruker
    user = await get_user_by_email(email)
    if not user:
        # Ikke avslør om brukeren finnes
        return {"ok": True}

    # Hent e-post-innstillinger for SMTP
    settings = await get_user_settings(user["id"])
    smtp_user = settings.get("email_1_username", "") if settings else ""
    smtp_pass = settings.get("email_1_password", "") if settings else ""

    if not smtp_user or not smtp_pass:
        logger.error("[AUTH] Mangler email_1 innstillinger for SMTP")
        return {"ok": False, "error": "E-post ikke konfigurert"}

    # Generer nytt midlertidig passord
    alphabet = string.ascii_letters + string.digits
    new_password = "".join(secrets.choice(alphabet) for _ in range(12))

    # Oppdater passord i DB
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET password_hash = $1 WHERE id = $2",
            hash_password(new_password), user["id"]
        )

    # Send e-post
    try:
        msg = MIMEText(
            f"Hei {user.get('display_name', '')}!\n\n"
            f"Ditt nye midlertidige passord er:\n\n"
            f"  {new_password}\n\n"
            f"Logg inn og bytt passord i innstillingene.\n\n"
            f"Brief Messages",
            "plain", "utf-8"
        )
        msg["Subject"] = "Nytt passord – Brief Messages"
        msg["From"] = smtp_user
        msg["To"] = email

        resend_key = os.environ.get("RESEND_API_KEY", "")
        if not resend_key:
            raise Exception("RESEND_API_KEY er ikke satt")
        async with httpx.AsyncClient(timeout=10) as _client:
            _res = await _client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
                json={
                    "from": "Brief Messages <onboarding@resend.dev>",
                    "to": ["trulsvaagan@hotmail.com"],
                    "subject": "Nytt passord – Brief Messages",
                    "text": (
                        f"Hei {user.get('display_name', '')}!\n\n"
                        f"Ditt nye midlertidige passord er:\n\n"
                        f"  {new_password}\n\n"
                        f"Logg inn og bytt passord i innstillingene.\n\n"
                        f"Brief Messages"
                    )
                }
            )
        if _res.status_code not in (200, 201):
            raise Exception(f"Resend feil: {_res.text}")

        logger.info(f"[AUTH] Nytt passord sendt til {email}")
        return {"ok": True}
    except Exception as e:
        logger.error(f"[AUTH] SMTP-feil: {e}")
        return {"ok": False, "error": "Kunne ikke sende e-post. Prøv igjen."}

@app.post("/auth/login")
async def login(req: LoginRequest):
    user = await get_user_by_email(req.email)
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Feil e-post eller passord")
    await update_last_login(user["id"])
    token = create_token(user["id"], user["email"])
    return {
        "token": token,
        "user": {
            "id": user["id"],
            "email": user["email"],
            "display_name": user["display_name"]
        }
    }

@app.get("/auth/me")
async def me(user: CurrentUser):
    return {
        "id": user["id"],
        "email": user["email"],
        "display_name": user["display_name"]
    }

# ─── Innstillinger ────────────────────────────────────────────────────────────

@app.get("/settings")
async def get_settings(user: CurrentUser):
    settings = await get_user_settings_safe(user["id"])
    return settings or {}

@app.patch("/settings")
async def update_settings(updates: dict, user: CurrentUser):
    allowed = {
        "spond_username", "spond_password", "spond_enabled",
        "email_1_host", "email_1_port", "email_1_username", "email_1_password",
        "email_1_from_filter", "email_1_subject_filter", "email_1_delete_after",
        "email_1_source_label", "email_1_source", "email_1_category", "email_1_enabled",

        "wa_service_url", "wa_group_filter", "wa_category", "wa_source_label", "wa_enabled",
        "caldav_enabled", "caldav_url", "caldav_username", "caldav_password", "caldav_calendars",
        "showbie_token", "showbie_user_id", "showbie_token_expires", "showbie_enabled", "showbie_fingerprint", "showbie_child_ids",
        "evening_summary_enabled", "evening_summary_time",
        "family_context", "ai_learned_context", "claude_api_key",
        "ai_scoring_enabled"
    }
    filtered = {k: v for k, v in updates.items() if k in allowed}
    await update_user_settings(user["id"], filtered)
    return {"ok": True}

# ─── Meldinger ────────────────────────────────────────────────────────────────

@app.get("/messages")
async def get_messages(user: CurrentUser, limit: int = 500):
    limit = max(50, min(limit, 2000))  # Mellom 50 og 2000
    messages = await get_all_messages(user_id=user["id"], limit=limit)
    # Hent nyeste updated_at fra brukerens meldinger som "sist synkronisert"
    pool = await get_pool()
    async with pool.acquire() as conn:
        newest = await conn.fetchval(
            "SELECT MAX(updated_at) FROM messages WHERE user_id = $1", user["id"]
        )
    user_last_sync = newest.isoformat() if newest else last_sync
    return {"messages": messages, "last_sync": user_last_sync, "count": len(messages)}

class StatusUpdate(BaseModel):
    status: str

@app.patch("/messages/{message_id}")
async def update_message(message_id: str, update: StatusUpdate, user: CurrentUser):
    if update.status not in ("read", "skipped", "unread"):
        raise HTTPException(status_code=400, detail="Ugyldig status")
    found = await update_message_status(message_id, update.status, user_id=user["id"])
    if not found:
        raise HTTPException(status_code=404, detail="Melding ikke funnet")
    asyncio.create_task(_maybe_update_profile(user["id"]))
    return {"ok": True}

async def _maybe_update_profile(user_id: str):
    if not await _scoring_enabled(user_id):
        return
    try:
        read_ex, skip_ex = await get_classified_messages(user_id=user_id)
        if len(read_ex) + len(skip_ex) >= 20:
            existing = await get_user_profile(user_id=user_id)
            new_profile = await generate_user_profile(read_ex, skip_ex, existing)
            if new_profile and new_profile != existing:
                await save_user_profile(new_profile, user_id=user_id)
    except Exception as e:
        logger.error(f"Bakgrunnsprofilering feilet: {e}")

# ─── Synk ─────────────────────────────────────────────────────────────────────

class SyncRequest(BaseModel):
    since: Optional[str] = None  # ISO timestamp


@app.post("/admin/process-all")
async def process_all_messages(user: CurrentUser):
    """Kjører AI-scoring på alle ubehandlede meldinger i bakgrunnen."""
    asyncio.create_task(_process_all_loop(user["id"]))
    return {"ok": True, "message": "AI-scoring startet i bakgrunnen"}

async def _process_all_loop(user_id: str):
    """Kjører AI-scoring i loop til alle meldinger er behandlet."""
    total = 0
    if not await _scoring_enabled(user_id):
        n = await skip_unprocessed_messages(user_id)
        logger.info(f"[AI] Scoring av – {n} meldinger arkivert som ikke relevant")
        return
    settings = await get_user_settings(user_id)
    api_key = _get_api_key(settings)
    if not api_key:
        logger.warning("[AI] Ingen API-nøkkel – avbryter process-all")
        return
    while True:
        unprocessed = await get_unprocessed_messages(limit=20, user_id=user_id)
        if not unprocessed:
            logger.info(f"[AI] process-all ferdig – {total} meldinger behandlet")
            break
        read_examples, skipped_examples = await get_classified_messages(user_id=user_id)
        user_profile = await get_user_profile(user_id=user_id)
        for msg in unprocessed:
            try:
                tldr = await generate_tldr(msg["title"], msg.get("body", ""), msg.get("source_label", ""), user_id=user_id)
                relevance = await score_relevance(msg, read_examples, skipped_examples, user_profile, user_id=user_id, user_api_key=api_key)
                await save_ai_result(msg["id"], tldr, relevance["score"], relevance["reason"], relevance["auto_skip"])
                total += 1
                logger.info(f"[AI/PROCESS-ALL] [{relevance['score']}] {msg['title'][:50]}")
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.error(f"[AI/PROCESS-ALL] Feil: {e}")
                await asyncio.sleep(1)
        await asyncio.sleep(1)

@app.post("/sync")
async def trigger_sync(user: CurrentUser, req: SyncRequest = SyncRequest()):
    try:
        settings = await get_user_settings(user["id"])
        if not settings:
            raise HTTPException(status_code=400, detail="Ingen innstillinger funnet")
        user_with_settings = {**user, **settings}
        since_dt = None
        if req.since:
            try:
                since_dt = datetime.fromisoformat(req.since.replace("Z", "+00:00"))
            except Exception:
                pass
        count = await sync_user(user_with_settings, since=since_dt)
        return {"ok": True, "count": count, "last_sync": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── Status ───────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "last_sync": last_sync}

@app.get("/profile")
async def get_profile(user: CurrentUser):
    profile = await get_user_profile(user_id=user["id"])
    read_ex, skip_ex = await get_classified_messages(user_id=user["id"])
    return {
        "profile": profile,
        "classified_total": len(read_ex) + len(skip_ex),
        "read_count": len(read_ex),
        "skipped_count": len(skip_ex),
        "profile_active": (len(read_ex) + len(skip_ex)) >= 5
    }

# ─── WhatsApp ingest ──────────────────────────────────────────────────────────

class IngestPayload(BaseModel):
    messages: list[dict]
    user_id: Optional[str] = None

@app.post("/messages/ingest")
async def ingest_messages(
    payload: IngestPayload,
    x_api_token: Optional[str] = Header(None)
):
    expected = os.environ.get("API_TOKEN")
    if expected and x_api_token != expected:
        raise HTTPException(status_code=401, detail="Ugyldig token")

    # Finn første bruker hvis user_id ikke er satt
    resolved_user_id = payload.user_id
    if not resolved_user_id:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT id FROM users ORDER BY created_at ASC LIMIT 1")
        if row:
            resolved_user_id = str(row["id"])

    for msg in payload.messages:
        if resolved_user_id:
            msg["user_id"] = resolved_user_id
        # Sett category til gruppenavn for WhatsApp-meldinger
        if msg.get("source") == "whatsapp":
            group = (msg.get("meta") or {}).get("group", "")
            if group:
                msg["category"] = group
        # Fiks time_label hvis den er tom eller generisk
        if not msg.get("time") or msg.get("time") in ("nå nettopp", ""):
            ts = msg.get("timestamp")
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    msg["time"] = dt.strftime("%-d. %b %H:%M")
                except Exception:
                    pass
    await upsert_messages(payload.messages)
    if resolved_user_id:
        asyncio.create_task(run_ai_processing(resolved_user_id))
    return {"ok": True, "count": len(payload.messages)}


# ─── WhatsApp-tilkobling: varsle ved frakobling ──────────────────────────────

class WaStatusPayload(BaseModel):
    event: str  # "disconnected" | "logged_out" | "reconnected"
    detail: str = ""

@app.post("/integrations/whatsapp/status")
async def whatsapp_status(
    payload: WaStatusPayload,
    x_api_token: Optional[str] = Header(None)
):
    """Kalles av whatsapp-service når tilkoblingen faller ut, logges ut, eller kommer tilbake.
    Sender push-varsel til alle brukere som har et aktivt push-abonnement."""
    expected = os.environ.get("API_TOKEN")
    if expected and x_api_token != expected:
        raise HTTPException(status_code=401, detail="Ugyldig token")

    if payload.event == "logged_out":
        title = "⚠️ WhatsApp logget ut"
        body = "Må skannes på nytt i appen for å fortsette å motta meldinger"
    elif payload.event == "disconnected":
        title = "⚠️ WhatsApp koblet fra"
        body = f"Har ikke klart å koble til på en stund ({payload.detail})" if payload.detail else "Har ikke klart å koble til på en stund"
    elif payload.event == "reconnected":
        title = "✓ WhatsApp tilkoblet igjen"
        body = "Alt er i orden igjen"
    else:
        logger.warning(f"Ukjent WhatsApp-statushendelse: {payload.event}")
        return {"ok": True}

    try:
        users = await get_all_active_users()
        sent = 0
        for u in users:
            subs = await get_push_subscriptions(str(u["id"]))
            if subs:
                await send_push_to_user(subs, title, body, "whatsapp-status")
                sent += len(subs)
        logger.info(f"[WHATSAPP-STATUS] {payload.event} – varslet {sent} enhet(er)")
    except Exception as e:
        logger.error(f"Kunne ikke sende WhatsApp-statusvarsel: {e}")

    return {"ok": True}


# ─── WhatsApp proxy ──────────────────────────────────────────────────────────

def _get_wa_url(settings: dict) -> str:
    """Henter WhatsApp-URL fra brukerinnstillinger, med fallback til miljøvariabel."""
    return settings.get("wa_service_url") or os.environ.get("WHATSAPP_SERVICE_URL", "")

def _get_api_key(settings: dict) -> str:
    """Henter Claude API-nøkkel fra brukerinnstillinger, med fallback til miljøvariabel."""
    return settings.get("claude_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")

@app.get("/whatsapp/status")
async def whatsapp_status(user: CurrentUser):
    """Proxy til WhatsApp-tjenesten for å unngå CORS."""
    settings = await get_user_settings(user["id"])
    wa_url = _get_wa_url(settings)
    if not wa_url:
        return {"ready": False, "hasQr": False, "error": "WhatsApp-tjeneste ikke konfigurert"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            res = await client.get(f"{wa_url}/")
            return res.json()
    except Exception as e:
        return {"ready": False, "hasQr": False, "error": str(e)}

@app.get("/whatsapp/qr")
async def whatsapp_qr(user: CurrentUser):
    """Proxy QR-kode fra WhatsApp-tjenesten."""
    settings = await get_user_settings(user["id"])
    wa_url = _get_wa_url(settings)
    if not wa_url:
        raise HTTPException(status_code=503, detail="WhatsApp-tjeneste ikke konfigurert")
    async with httpx.AsyncClient(timeout=10) as client:
        res = await client.get(f"{wa_url}/qr")
        return {"html": res.text}

@app.post("/whatsapp/logout")
async def whatsapp_logout(user: CurrentUser):
    """Logger ut av WhatsApp og sletter sesjonen."""
    settings = await get_user_settings(user["id"])
    wa_url = _get_wa_url(settings)
    if not wa_url:
        raise HTTPException(status_code=503, detail="WhatsApp-tjeneste ikke konfigurert")
    async with httpx.AsyncClient(timeout=10) as client:
        res = await client.post(f"{wa_url}/logout")
        return res.json()

# ─── Admin: opprydding ───────────────────────────────────────────────────────

@app.post("/admin/fix-null-users")
async def fix_null_users(user: CurrentUser):
    """Knytter alle meldinger med user_id=NULL til innlogget bruker."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE messages SET user_id = $1 WHERE user_id IS NULL
        """, user["id"])
    count = int(result.split()[-1])
    return {"ok": True, "fixed": count}

@app.post("/whatsapp/fetch-history")
async def whatsapp_fetch_history(user: CurrentUser):
    """Trigger historisk henting fra WhatsApp-tjenesten."""
    settings = await get_user_settings(user["id"])
    wa_url = _get_wa_url(settings)
    if not wa_url:
        raise HTTPException(status_code=503, detail="WhatsApp-tjeneste ikke konfigurert")
    async with httpx.AsyncClient(timeout=300) as client:
        res = await client.post(f"{wa_url}/fetch-history")
        return res.json()


class FetchHistoryRequest(BaseModel):
    from_date: str = "2026-01-01"  # ISO format YYYY-MM-DD

@app.post("/fetch-history")
async def fetch_history(req: FetchHistoryRequest, user: CurrentUser):
    """Henter historiske meldinger fra alle aktive kilder."""
    try:
        from_dt = datetime.fromisoformat(req.from_date).replace(tzinfo=timezone.utc)
    except Exception:
        from_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)

    settings = await get_user_settings(user["id"])
    results = {"spond": 0, "whatsapp": 0, "email1": 0, "email2": 0, "errors": [], "email1_label": "Skolemelding", "email2_label": "Showbie"}
    all_messages = []

    # ── Spond ──────────────────────────────────────────────────────────────────
    if settings.get("spond_enabled"):
        try:
            pw = settings.get("spond_password", "")
            if pw and settings.get("spond_username"):
                svc = SpondService(settings["spond_username"], pw)
                msgs = await svc.fetch_all()
                # fetch_all already filters from history_from in spond_service
                for m in msgs:
                    m["user_id"] = user["id"]
                # Barn-matching skjer live ved søk, ikke her ved innlesing - se
                # _spond_child_tag i main.py.
                all_messages.extend(msgs)
                results["spond"] = len(msgs)
        except Exception as e:
            results["errors"].append(f"Spond: {str(e)}")

    # ── E-post (Skolemelding + Showbie) ────────────────────────────────────────
    for idx in [1, 2]:
        enabled = settings.get(f"email_{idx}_enabled")
        host = settings.get(f"email_{idx}_host")
        if not enabled or not host:
            continue
        try:
            pw = settings.get(f"email_{idx}_password", "")
            email_cfg = {
                "host": host,
                "port": settings.get(f"email_{idx}_port", 993),
                "username": settings.get(f"email_{idx}_username", ""),
                "password": pw or "",
                "from_filter": settings.get(f"email_{idx}_from_filter", ""),
                "subject_filter": settings.get(f"email_{idx}_subject_filter", ""),
                "delete_after_fetch": False,  # Aldri slett ved historisk henting
                "source_label": settings.get(f"email_{idx}_source_label", "Skolemelding"),
                "source": settings.get(f"email_{idx}_source", "skolemelding"),
                "category": settings.get(f"email_{idx}_category", "skole"),
            }
            svc = EmailService(email_cfg)
            msgs = await asyncio.to_thread(svc.fetch_history, from_dt)
            for m in msgs:
                m["user_id"] = user["id"]
            all_messages.extend(msgs)
            results[f"email{idx}"] = len(msgs)
            results[f"email{idx}_label"] = email_cfg.get("source_label", f"E-post {idx}")
        except Exception as e:
            results["errors"].append(f"E-post {idx}: {str(e)}")

    # ── WhatsApp ───────────────────────────────────────────────────────────────
    if settings.get("wa_enabled"):
        wa_url = _get_wa_url(settings)
        if wa_url:
            try:
                async with httpx.AsyncClient(timeout=300) as client:
                    res = await client.post(
                        f"{wa_url}/fetch-history",
                        json={"from_date": req.from_date, "user_id": user["id"]}
                    )
                    wa_result = res.json()
                    results["whatsapp"] = wa_result.get("count", 0)
            except Exception as e:
                results["errors"].append(f"WhatsApp: {str(e)}")

    # Lagre alle meldinger
    if all_messages:
        await upsert_messages(all_messages)

    total = results["spond"] + results["email1"] + results["email2"] + results["whatsapp"]
    return {"ok": True, "total": total, "details": results}

@app.post("/showbie/test")
async def test_showbie(user: CurrentUser):
    settings = await get_user_settings(user["id"])
    token = settings.get("showbie_token", "")
    user_id = settings.get("showbie_user_id", "")
    if not token:
        raise HTTPException(status_code=400, detail="Showbie token ikke konfigurert")
    from showbie_service import test_showbie_connection
    fingerprint = settings.get("showbie_fingerprint", "")
    result = await test_showbie_connection(token, user_id, fingerprint)
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Tilkobling feilet"))
    return result


@app.get("/caldav/calendars")
async def list_calendars(user: CurrentUser):
    """Lister tilgjengelige kalendere fra CalDAV."""
    settings = await get_user_settings(user["id"])
    if not settings or not settings.get("caldav_username"):
        raise HTTPException(status_code=400, detail="Fyll inn Apple ID og passord først")
    if not settings.get("caldav_password"):
        raise HTTPException(status_code=400, detail="Fyll inn app-spesifikt passord først")
    try:
        import caldav as caldav_lib
        def _list():
            client = caldav_lib.DAVClient(
                url=settings.get("caldav_url", "https://caldav.icloud.com"),
                username=settings.get("caldav_username"),
                password=settings.get("caldav_password")
            )
            return [c.name for c in client.principal().calendars()]
        names = await asyncio.to_thread(_list)
        return {"calendars": names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats/api")
async def get_api_stats(user: CurrentUser):
    """Henter API-forbruksstatistikk."""
    stats = await get_api_usage_stats(user["id"])
    return stats


@app.post("/admin/fix-spond-categories")
async def fix_spond_categories(user: CurrentUser):
    """Oppdaterer category på Spond-meldinger til gruppenavn fra meta."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE messages
            SET category = meta->>'group'
            WHERE source = 'spond'
              AND meta->>'group' IS NOT NULL
              AND meta->>'group' != ''
        """)
    count = int(result.split()[-1])
    return {"ok": True, "updated": count}

@app.post("/admin/fix-whatsapp-categories")
async def fix_whatsapp_categories(user: CurrentUser):
    """Oppdaterer category på WhatsApp-meldinger til gruppenavn fra meta."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE messages
            SET category = meta->>'group'
            WHERE source = 'whatsapp'
              AND meta->>'group' IS NOT NULL
              AND meta->>'group' != ''
        """)
    count = int(result.split()[-1])
    return {"ok": True, "updated": count}

@app.post("/admin/fix-categories")
async def fix_categories(user: CurrentUser):
    """Normaliserer kategorinavn i databasen til lowercase."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Normaliser alle kategorier til lowercase
        result = await conn.execute("""
            UPDATE messages SET category = LOWER(category)
            WHERE category != LOWER(category)
        """)
        count = int(result.split()[-1])
        # Rename foreldre -> whatsapp for whatsapp-meldinger
        result2 = await conn.execute("""
            UPDATE messages SET category = 'whatsapp'
            WHERE source = 'whatsapp' AND category = 'foreldre'
        """)
        count2 = int(result2.split()[-1])
    return {"ok": True, "normalized": count, "whatsapp_fixed": count2}

# ─── Push-abonnementer ────────────────────────────────────────────────────────

class PushSubscription(BaseModel):
    endpoint: str
    keys: dict  # p256dh, auth

@app.post("/push/subscribe")
async def push_subscribe(sub: PushSubscription, user: CurrentUser):
    await save_push_subscription(
        user["id"],
        sub.endpoint,
        sub.keys.get("p256dh", ""),
        sub.keys.get("auth", "")
    )
    return {"ok": True}

@app.delete("/push/subscribe")
async def push_unsubscribe(sub: PushSubscription, user: CurrentUser):
    await delete_push_subscription(sub.endpoint)
    return {"ok": True}

@app.delete("/push/all")
async def push_unsubscribe_all(user: CurrentUser):
    """Sletter alle push-abonnementer for brukeren."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM push_subscriptions WHERE user_id = $1", user["id"]
        )
    return {"ok": True}

@app.get("/push/vapid-key")
async def get_vapid_key():
    return {"publicKey": os.environ.get("VAPID_PUBLIC_KEY", "")}

@app.post("/push/test")
async def push_test(user: CurrentUser):
    subs = await get_push_subscriptions(user["id"])
    if not subs:
        raise HTTPException(status_code=400, detail="Ingen push-abonnementer registrert")
    await send_push_to_user(subs, "Familie 👋", "Push-varsler fungerer!", "test")
    return {"ok": True, "sent_to": len(subs)}


# ─── Send push ved nye meldinger (kalles etter synk) ─────────────────────────

async def notify_new_messages(user_id: str, new_count: int):
    if new_count <= 0:
        return
    try:
        subs = await get_push_subscriptions(user_id)
        if not subs:
            return
        title = f"{new_count} ny{'e' if new_count > 1 else ''} melding{'er' if new_count > 1 else ''}"
        body  = "Trykk for å se hva som er nytt"
        await send_push_to_user(subs, title, body, "new-messages")
    except Exception as e:
        logger.error(f"Push-varsling feilet: {e}")


# ─── AI Chat proxy ────────────────────────────────────────────────────────────

class AiChatRequest(BaseModel):
    question: str
    family_context: str = ""
    auto_context: str = ""
    history: list[dict] = []

@app.post("/ai/chat")
async def ai_chat(req: AiChatRequest, user: CurrentUser):
    settings = await get_user_settings(user["id"])
    api_key = _get_api_key(settings)
    if not api_key:
        return {"answer": "Claude API-nøkkel er ikke konfigurert. Legg inn din API-nøkkel i Innstillinger → Annet."}

    # Fjern kun veldig korte ord og vanlige stopwords
    stopwords = {'jeg', 'deg', 'han', 'hun', 'den', 'det', 'de', 'vi', 'dere', 'ikke', 'har',
                 'er', 'var', 'blir', 'ble', 'vil', 'kan', 'skal', 'skulle', 'ville', 'være',
                 'med', 'for', 'til', 'fra', 'på', 'av', 'om', 'og', 'eller', 'men', 'som',
                 'hva', 'når', 'hvor', 'hvem', 'hvilken', 'noe', 'noen'}
    words = [w.lower().strip('?.,!') for w in req.question.split() if len(w) > 1 and w.lower() not in stopwords]

    import json as _json
    pool = await get_pool()
    seen_ids = set()
    relevant = []

    # Sjekk om spørsmålet handler om tid (begrens da til 14 dager)
    time_words = {'morgen','dag','uke','uken','ukens','ukes','dager','imorgen','idag','fredag','mandag',
                  'tirsdag','onsdag','torsdag','lørdag','søndag','helgen','neste','denne','kommende'}
    is_time_query = any(w in time_words for w in words)
    date_filter = "AND timestamp > NOW() - INTERVAL '14 days'" if is_time_query else ""
    if is_time_query:
        logger.info(f"[AI/CHAT] Tidsspørsmål – begrenser til siste 14 dager")

    # Rene substreng-søk ("LIKE %ord%") bommer på sammensatte ord som ikke finnes
    # ordrett i kilden - f.eks. "skoleuke" finnes ingen steder i teksten selv om
    # meldingen tydelig handler om skole (Showbie-ukeplan, "6C", osv). Når et søkeord
    # inneholder en av disse stammene, utvider vi derfor søket til også å ta med
    # meldinger fra de kildene/kategoriene som stammen peker på, uavhengig av om selve
    # ordet finnes ordrett i teksten.
    SOURCE_HINT_STEMS = {
        'skole': "(source IN ('showbie','skolemelding') OR LOWER(category) LIKE '%skole%')",
        'lekse': "(source IN ('showbie','skolemelding') OR LOWER(body) LIKE '%lekse%')",
        'showbie': "source = 'showbie'",
        'ukeplan': "source IN ('showbie','skolemelding')",
    }

    if words:
        logger.info(f"[AI/CHAT] Søkeord: {words}")
        sql = (
            "SELECT id, source, source_label, category, title, tldr, body,"
            " time_label AS time, timestamp, priority, status, meta"
            " FROM messages"
            " WHERE user_id = $1"
            # Tar med alle meldinger, også de som er markert som ikke relevant
            f" {date_filter}"
            " AND (LOWER(title) LIKE $2"
            " OR LOWER(tldr) LIKE $2"
            " OR LOWER(body) LIKE $2"
            " OR LOWER(category) LIKE $2"
            " OR LOWER(meta::text) LIKE $2)"
            " ORDER BY timestamp DESC LIMIT 100"
        )
        async with pool.acquire() as conn:
            for word in words:
                rows = await conn.fetch(sql, user["id"], f"%{word}%")
                for row in rows:
                    m = dict(row)
                    if m["id"] in seen_ids:
                        continue
                    seen_ids.add(m["id"])
                    if m.get("meta") and not isinstance(m["meta"], dict):
                        try: m["meta"] = _json.loads(m["meta"])
                        except: m["meta"] = {}
                    m["sourceLabel"] = m.pop("source_label", m.get("source", ""))
                    relevant.append(m)

                for stem, extra_cond in SOURCE_HINT_STEMS.items():
                    if stem not in word:
                        continue
                    hint_sql = (
                        "SELECT id, source, source_label, category, title, tldr, body,"
                        " time_label AS time, timestamp, priority, status, meta"
                        " FROM messages"
                        f" WHERE user_id = $1 {date_filter} AND {extra_cond}"
                        " ORDER BY timestamp DESC LIMIT 30"
                    )
                    hint_rows = await conn.fetch(hint_sql, user["id"])
                    for row in hint_rows:
                        m = dict(row)
                        if m["id"] in seen_ids:
                            continue
                        seen_ids.add(m["id"])
                        if m.get("meta") and not isinstance(m["meta"], dict):
                            try: m["meta"] = _json.loads(m["meta"])
                            except: m["meta"] = {}
                        m["sourceLabel"] = m.pop("source_label", m.get("source", ""))
                        relevant.append(m)
        logger.info(f"[AI/CHAT] Fant {len(relevant)} relevante meldinger")

    def score_msg(m):
        text = f"{m['title']} {m['tldr']} {m.get('body','')} {m.get('category','')}".lower()
        return sum(1 for w in words if w in text)

    relevant = sorted(relevant, key=score_msg, reverse=True)[:50]
    if not relevant:
        relevant = await get_all_messages(user_id=user["id"], limit=10)


    def format_msg(m):
        parts = [f"[{m['sourceLabel']} - {m.get('category','')} - {m['time']}]", f"Tittel: {m['title']}"]
        tag = _spond_child_tag(m, req.family_context)
        if tag:
            parts.append(tag)
        if m.get('tldr'):
            parts.append(f"Sammendrag: {m['tldr']}")
        if m.get('body') and m['body'] != m.get('tldr',''):
            max_body = 2000 if m.get('source') == 'showbie' else 800
            body_preview = m['body'][:max_body] + ('...' if len(m['body']) > max_body else '')
            parts.append(f"Innhold: {body_preview}")
        return "\n".join(parts)

    parts = [format_msg(m) for m in relevant]
    # Begrens total kontekststørrelse til ~30000 tegn
    msg_context = ""
    for part in parts:
        if len(msg_context) + len(part) > 30000:
            break
        msg_context += ("\n\n" if msg_context else "") + part

    family_section = f"FAMILIEPROFIL:\n{req.family_context}\n" if req.family_context else ""
    auto_section = f"LAERT KONTEKST:\n{req.auto_context}\n" if req.auto_context else ""

    from datetime import date as _date
    today = _date.today()
    days_no = ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"]
    months_no = ["januar","februar","mars","april","mai","juni","juli","august","september","oktober","november","desember"]
    today_str = f"{days_no[today.weekday()]} {today.day}. {months_no[today.month-1]} {today.year}"

    # Hent kalenderdata hvis aktivert
    calendar_section = ""
    if settings and settings.get("caldav_enabled") and settings.get("caldav_username") and settings.get("caldav_password"):
        try:
            from caldav_service import fetch_calendar_events, format_events_for_ai
            events = await fetch_calendar_events(
                url=settings.get("caldav_url", "https://caldav.icloud.com"),
                username=settings.get("caldav_username", ""),
                password=settings.get("caldav_password", ""),
                calendar_filter=settings.get("caldav_calendars", ""),
                days_ahead=14
            )
            events = _tag_calendar_events_with_spond(events, relevant, req.family_context)
            calendar_section = f"KALENDER (neste 14 dager):\n{format_events_for_ai(events)}\n\n"
        except Exception as e:
            logger.warning(f"Kalender-henting feilet: {e}")

    system = (
        f"Du er en hjelpsom familieassistent som hjelper med å finne informasjon fra familiens meldinger og kalender.\n"
        f"Dagens dato er {today_str}. Bruk denne datoen når du svarer på spørsmål om tid.\n"
        "Svar alltid på norsk. Vaer konkret og presis. Hvis du ikke finner svaret, si det tydelig. Ikke finn opp informasjon.\n\n"
        "VIKTIG om Showbie/skolemelding-innhold (ukeplaner, lekser): en Showbie-ukeplan har "
        "ofte en lekse-del med FLERE FAG under hverandre (f.eks. Norsk, Matematikk, Engelsk, "
        "hver med sin egen lekse). Når du svarer på spørsmål om ukeplan, lekser eller 'skoleuke', "
        "skal du lese GJENNOM HELE meldingsteksten og ta med ALLE fagene som har en lekse der - "
        "ikke bare det første faget du finner. Ikke konkluder med at 'det er ikke mer' eller 'ikke "
        "noe utover X' før du faktisk har lett gjennom hele innholdet i meldingen etter flere fag.\n\n"
        f"{SPOND_CALENDAR_RULE}\n\n"
        "FORMATERING (følg alltid dette, uansett lengde på svar):\n"
        "- Ved oversikter og dagsoversikter: del alltid opp i seksjoner med ### og emoji\n"
        "- Eksempel: ### 🏫 SKOLE:, ### ⚽ FOTBALL:, ### 👤 VOKSNE:, ### 🎯 AKTIVITETER:\n"
        "- Bruk alltid - for punktlister under hver seksjon\n"
        "- Selv ved korte svar: bruk seksjoner hvis det er flere kategorier\n"
        "- IKKE skriv lange løpende setninger – bruk alltid punktlister\n\n"
        + family_section
        + auto_section
        + calendar_section
        + "RELEVANTE MELDINGER:\n"
        + msg_context
    )

    messages = [
        m for m in req.history[-6:]
        if m.get("role") in ("user", "assistant") and "⏳" not in m.get("content", "")
    ]
    messages.append({"role": "user", "content": req.question})

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                res = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={"model": "claude-sonnet-5", "max_tokens": 1200, "system": system, "messages": messages}
                )
            if res.status_code in (429, 529):
                wait = [5, 15, 30][attempt]
                logger.warning(f"[AI/CHAT] API overbelastet ({res.status_code}), venter {wait}s...")
                await asyncio.sleep(wait)
                continue
            if res.status_code != 200:
                logger.error(f"[AI/CHAT] API feil: {res.status_code} {res.text[:200]}")
                return {"answer": f"Beklager, API-feil ({res.status_code}). Prøv igjen."}
            data = res.json()
            answer = _extract_text(data.get("content"))
            if not answer:
                logger.error(f"[AI/CHAT] Tomt svar: {data}")
                return {"answer": "Beklager, fikk tomt svar. Prøv igjen."}
            return {"answer": answer}
        except httpx.TimeoutException:
            logger.warning(f"[AI/CHAT] Timeout (forsøk {attempt+1}/3)")
            if attempt == 2:
                return {"answer": "Beklager, forespørselen tok for lang tid. Prøv igjen."}
        except Exception as e:
            logger.error(f"[AI/CHAT] Feil: {e}")
            return {"answer": "Beklager, noe gikk galt. Prøv igjen."}
    return {"answer": "Beklager, noe gikk galt etter 3 forsøk."}


# ─── Ukesoversikt (til iPad-visningen) ────────────────────────────────────────

WEEK_PEOPLE = ["Gustav", "Rasmus", "Kristin", "Truls"]

# Enkel per-bruker, per-uke mellomlagring i minnet. Nullstilles automatisk hver
# mandag (nøkkelen inkluderer mandagsdatoen), og kan tvinges med ?refresh=true.
_week_cache: dict = {}

@app.get("/ai/week-overview")
async def ai_week_overview(user: CurrentUser, refresh: bool = False, week_offset: int = 0):
    """Strukturert ukesoversikt (mandag-søndag) per familiemedlem, til uke.html på iPad.
    Bygger på samme kilder som AI-assistenten (meldinger + kalender + familiekontekst),
    men ber Claude svare med strukturert JSON i stedet for fritekst.
    week_offset: 0 = denne uken, 1 = neste uke."""
    if week_offset not in (0, 1):
        raise HTTPException(status_code=400, detail="week_offset må være 0 eller 1")

    uid = user["id"]
    today = date.today()
    monday = today - timedelta(days=today.weekday()) + timedelta(days=7 * week_offset)
    sunday = monday + timedelta(days=6)
    cache_key = f"{uid}:{monday.isoformat()}"

    cached = _week_cache.get(cache_key)
    if not refresh and cached:
        return cached

    settings = await get_user_settings(uid)
    api_key = _get_api_key(settings) if settings else os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=400, detail="Claude API-nøkkel er ikke konfigurert")

    family_ctx = settings.get("family_context", "") if settings else ""
    auto_ctx = settings.get("ai_learned_context", "") if settings else ""

    # Meldinger siste 14 dager – alle, uansett status, slik AI-assistenten ser dem
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT source, source_label, category, title, tldr, body,"
            " time_label AS time, timestamp, meta"
            " FROM messages WHERE user_id = $1 AND timestamp > NOW() - INTERVAL '14 days'"
            " ORDER BY timestamp DESC LIMIT 150",
            uid
        )

    def fmt(m):
        src = m.get("source_label") or m.get("source", "?")
        parts = [f"[{src} - {m.get('category','')} - {m.get('time','')}] {m.get('title','')}"]
        tag = _spond_child_tag(m, family_ctx)
        if tag:
            parts.append(f"  {tag}")
        if m.get("tldr"):
            parts.append(f"  Sammendrag: {m['tldr']}")
        if m.get("body") and m["body"] != m.get("tldr", ""):
            # Var 1500 for showbie - kuttet av midt i en vanlig ukeplan, rett FØR
            # "Lekser"-delen (den viktigste delen!), siden selve timeplanen for 5
            # dager alene fort passerer 1500 tegn. Økt til 2500 - mer enn de andre
            # to AI-endepunktene (2000) bruker, siden ukesoversikten trenger mest
            # detalj for å klare å dele opp en hel ukeplan per dag.
            max_b = 2500 if m.get("source") == "showbie" else 600
            parts.append(f"  Innhold: {m['body'][:max_b]}")
        return "\n".join(parts)

    msg_context = ""
    for row in rows:
        part = fmt(dict(row))
        if len(msg_context) + len(part) > 25000:
            break
        msg_context += ("\n\n" if msg_context else "") + part

    # Kalender for resten av uken (kan ikke hente bakover i tid)
    calendar_section = ""
    if settings and settings.get("caldav_enabled") and settings.get("caldav_username") and settings.get("caldav_password"):
        try:
            from caldav_service import fetch_calendar_events, format_events_for_ai
            # Hent fra midnatt MANDAG denne uken (ikke "nå") - ellers faller allerede
            # passerte aktiviteter tidligere i uken/dagen helt ut av kalendersøket, og
            # ukesoversikten viser da ikke ferdige ting som skjedde tidligere i dag.
            week_start_dt = datetime(monday.year, monday.month, monday.day, 0, 0, tzinfo=OSLO_TZ)
            days_ahead = max((sunday - monday).days + 1, 1)
            events = await fetch_calendar_events(
                url=settings.get("caldav_url", "https://caldav.icloud.com"),
                username=settings.get("caldav_username", ""),
                password=settings.get("caldav_password", ""),
                calendar_filter=settings.get("caldav_calendars", ""),
                days_ahead=days_ahead,
                start_date=week_start_dt
            )
            # "rows" over er filtrert på NÅR meldingen ble LAGRET (siste 14 dager) -
            # det ekskluderer faste/gjentakende Spond-arrangementer (f.eks. en fast
            # ukentlig trening, eller en cup kunngjort for lenge siden) som ble synket
            # for mer enn 14 dager siden, selv om selve AKTIVITETEN (eventStart) er
            # denne uken. Slike arrangementer manglet derfor helt i grunnlaget for
            # kalender-matchingen under, uansett hvor riktig selve matche-logikken var.
            # Henter derfor i tillegg ALLE Spond-arrangementer der eventStart faller i
            # DENNE UKEN, uavhengig av når de ble lagret.
            week_end_dt = datetime(sunday.year, sunday.month, sunday.day, 23, 59, 59, tzinfo=OSLO_TZ)
            async with pool.acquire() as conn2:
                spond_event_rows = await conn2.fetch(
                    "SELECT source, category, meta FROM messages"
                    " WHERE user_id = $1 AND source = 'spond' AND meta ? 'eventStart'"
                    " AND (meta->>'eventStart')::timestamptz >= $2"
                    " AND (meta->>'eventStart')::timestamptz <= $3",
                    uid, week_start_dt, week_end_dt
                )
            logger.info(f"[UKESOVERSIKT] Spond-arrangementer denne uken (uavh. av lagringsdato): {len(spond_event_rows)}")
            from family_profile import extract_child_team_names
            logger.info(f"[UKESOVERSIKT] family_ctx (repr, {len(family_ctx or '')} tegn): {family_ctx!r}")
            logger.info(f"[UKESOVERSIKT] Utpakkede lagnavn per person: {extract_child_team_names(family_ctx)!r}")
            for r in spond_event_rows:
                _m = dict(r)
                _meta = _spond_meta(_m)
                logger.info(
                    f"[UKESOVERSIKT]   rad: category={_m.get('category')!r} "
                    f"meta.group={_meta.get('group')!r} eventStart={_meta.get('eventStart')!r} "
                    f"-> match={_spond_child_name(_m, family_ctx)!r}"
                )
            spond_rows_for_tagging = [dict(r) for r in rows] + [dict(r) for r in spond_event_rows]
            events = _tag_calendar_events_with_spond(events, spond_rows_for_tagging, family_ctx)
            calendar_section = f"KALENDER (hele uken, mandag {monday.day}. til søndag {sunday.day}.):\n{format_events_for_ai(events)}\n\n"
            # Midlertidig debug-logg: viser EKSAKT hva AI-en fikk servert for kalenderen,
            # slik at vi kan se om "(Gjelder: ...)" faktisk ble satt riktig av koden,
            # eller om problemet ligger i hvordan AI-en leser/bruker teksten videre.
            n_tagged = sum(1 for e in events if e.get("child"))
            logger.info(f"[UKESOVERSIKT] Kalender: {len(events)} oppføringer, {n_tagged} tagget med barn")
            logger.info(f"[UKESOVERSIKT] Kalender-tekst til AI:\n{format_events_for_ai(events)}")
        except Exception as e:
            logger.warning(f"[UKESOVERSIKT] Kalender-henting feilet: {e}")

    days_no = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]
    day_dates = [monday + timedelta(days=i) for i in range(7)]
    day_labels = [f"{days_no[i].capitalize()} {d.day}." for i, d in enumerate(day_dates)]

    system = (
        "Du er en familieassistent som lager en ukesoversikt for en tavle på kjøkkenet.\n"
        f"Dagens dato er {today.isoformat()}. Uken du skal lage oversikt for er "
        f"{'denne uken' if week_offset == 0 else 'NESTE uke'}: mandag {day_dates[0].day}. "
        f"til søndag {day_dates[6].day}.\n"
        "Vis HELE uken konsekvent, fra mandag til søndag - også dager og aktiviteter som "
        "allerede har passert eller er gjennomført (f.eks. en trening tidligere i dag, eller "
        "tidligere i uken). Ikke utelat noe bare fordi det ligger bakover i tid denne uken.\n"
        f"{SPOND_CALENDAR_RULE}\n\n"
        "EKSEMPEL på hvordan dette brukes (oppdiktet, kun for å illustrere mønsteret):\n"
        "  Hvis MELDINGER inneholder '[Spond - Lillegutt 2015 Ready - ...] Trening\\n  "
        "Gjelder: Gustav', skal denne treningen KUN vises under Gustav - aldri under Rasmus, "
        "selv om tittelen bare sier 'Trening'.\n"
        "  Hvis KALENDER inneholder '- torsdag 19. nov kl. 16:50: Trening Eckbobanen "
        "(Gjelder: Gustav) [Kalender: Familie]', skal også DENNE kun vises under Gustav - "
        "det er samme regel, bare på en kalenderoppføring i stedet for en Spond-melding.\n"
        "  Hvis KALENDER i stedet inneholder '- fredag 20. nov kl. 18:00: Julebord "
        "[Kalender: Familie]' (ingen '(Gjelder: ...)' i det hele tatt), skal denne vises "
        "under BÅDE Kristin og Truls, ikke under Gustav eller Rasmus, og ikke utelates.\n\n"
        f"{('FAMILIEPROFIL: ' + family_ctx) if family_ctx else ''}\n"
        f"{('LAERT KONTEKST: ' + auto_ctx) if auto_ctx else ''}\n\n"
        f"{calendar_section}"
        f"MELDINGER (siste 14 dager):\n{msg_context}\n\n"
        "Lag en ukesoversikt fordelt på person og dag, for disse fire personene: "
        f"{', '.join(WEEK_PEOPLE)} (Gustav og Rasmus er barn, Kristin og Truls er foreldre).\n"
        "For hver dag mandag-søndag og hver person skal du dele stikkordene i to grupper:\n"
        "1) \"skole\": ALT som gjelder skolearbeid og skole - lekser, prøver, innleveringer, "
        "ting som må tas med, turdag, tema-/aktivitetsdag, skolearrangement, foreldremøte, "
        "utflukt og lignende, NÅR DET KOMMER FRA KILDER UNDER \"MELDINGER\" (typisk Showbie "
        "eller skolemelding - kategori/tittel kan inneholde klassenavn som '6C' eller '7B', "
        "eller barnets navn direkte - bruk det til å vite hvem det gjelder for). Vær like "
        "grundig her som du ville vært hvis noen spurte deg direkte om 'skoleuke' - IKKE utelat "
        "skoleinnhold som finnes i kildene. En Showbie-ukeplan inneholder ofte både en daglig "
        "timeplan (skolefag/timer - denne skal IKKE gjentas dag for dag i oversikten, det er for "
        "mye detalj) og en lekse-del som gjelder HELE uken (f.eks. 'les 30 min i løpet av uken' "
        "eller 'gjør X hver dag', uten noen bestemt dag). Lekse-delen har ofte FLERE FAG under "
        "hverandre (f.eks. Norsk, Matematikk, Engelsk hver med sin egen lekse) - ALLE fagene med "
        "lekse skal tas med, ikke bare det første/øverste faget i lista. Lag ETT eget stikkord "
        "per fag (f.eks. '📓 Norsk: les 30 min', '🔢 Matte: s.12-13', '🇬🇧 Engelsk: ord-liste'), "
        "legg dem alle under mandag denne uken. IKKE slå sammen eller utelat fag for å spare "
        "plass her - 3-stikkords-grensen nedenfor gjelder \"annet\", ikke lekser i \"skole\".\n"
        "2) \"annet\": fritidsaktiviteter (trening, kamper, møter) og andre avtaler - alt som "
        "ikke er skolerelatert. VIKTIG: ALT som kommer fra kalenderen (seksjonen \"KALENDER\" "
        "over) skal ALLTID legges i \"annet\", aldri i \"skole\" - uansett hva aktiviteten "
        "heter eller handler om (f.eks. 'Gymtøy', 'ta med X', en treningstime, osv). Kalender-"
        "oppføringer er avtaler/påminnelser, ikke skolearbeid, selv om teksten kan ligne på "
        "noe skolerelatert.\n"
        "Format på hvert stikkord: '<emoji> <HH:MM> <aktivitet>' når tidspunkt er kjent "
        "(24-timers format med kolon, f.eks. '18:00'), ellers '<emoji> <aktivitet>' uten tid. "
        "Velg selv ett emoji som passer aktiviteten (f.eks. ⚽🏒🤾🎿📓📝💉🎉🍷👔🎂), ett emoji per stikkord. "
        "REGEL for skøyte-/ballidrett ved usikkerhet: hvis det er uklart om en trening/kamp er "
        "bandy eller fotball (f.eks. bare 'trening' eller 'kamp' uten at idretten er eksplisitt "
        "nevnt i kilden), avgjør ut fra hvilken måned aktiviteten er i - april til og med oktober "
        "regnes som sommersesong og skal få ⚽, november til og med mars regnes som vintersesong "
        "og skal alltid få 🏒 (hockeykølle-emojien). Hvis idretten er eksplisitt nevnt i kilden "
        "(f.eks. 'bandytrening' eller 'fotballtrening'), bruk emoji for den faktiske idretten "
        "uansett sesong - bandy skal også alltid få 🏒. "
        "Maks 3 stikkord i \"annet\" per celle (ingen tilsvarende grense for \"skole\" - der skal "
        "ALT relevant skoleinnhold/alle lekse-fag være med, se over). Cellene er smale, så HVER "
        "stikkord-linje må holdes på "
        "MAKS ca 34 tegn inkl. emoji og tid, og vises alltid på én linje - den blir aldri "
        "brutt over to linjer (teksten klippes automatisk med \"...\" hvis den likevel blir for "
        "lang, så det er bedre å ta med litt for mye enn å kutte bort viktig informasjon). "
        "Forkort kun når nødvendig: bruk korte stedsnavn/forkortelser, og forkort personnavn til "
        "forbokstav (f.eks. 'med Ola' -> 'm/O') hvis hele setningen ellers ikke får plass på én "
        "linje. Behold likevel så mye konkret informasjon som mulig innenfor denne grensen - "
        "generaliser kun det som må til for å få plass. Tom liste hvis ingenting er kjent for "
        "personen den dagen. "
        "IKKE finn på noe som ikke støttes av kildene over.\n\n"
        "Svar KUN med gyldig JSON, nøyaktig i dette formatet, ingen annen tekst før eller etter:\n"
        '{"days": [\n'
        '  {"day": "mandag", "people": {'
        '"Gustav": {"skole": ["..."], "annet": ["..."]}, '
        '"Rasmus": {"skole": ["..."], "annet": ["..."]}, '
        '"Kristin": {"skole": ["..."], "annet": ["..."]}, '
        '"Truls": {"skole": ["..."], "annet": ["..."]}}},\n'
        '  ...\n'
        "]} (nøyaktig 7 dager totalt, i rekkefølge mandag til søndag - skole og annet er "
        "tomme lister [] hvis ingenting er kjent)"
    )

    import json
    # Timeout økt fra 60s: med mye kontekst (meldinger + kalender) kan selve
    # Claude-kallet ta god tid, og 60s ga httpcore.ReadTimeout -> 500.
    # "temperature" er IKKE en gyldig parameter for denne modellen (API-et svarer
    # 400 "temperature is deprecated for this model") - fjernet for godt, kan ikke
    # brukes til å gjøre svaret mer konsekvent.
    # "thinking" derimot MÅ være med (med et begrenset "effort"): uten den eksplisitte
    # innstillingen bruker modellen likevel intern "tenking" før selve svaret, men da
    # med et ubegrenset/for stort budsjett - det brukte opp HELE max_tokens og ga tomt
    # svar (stop_reason: max_tokens). "low" holder siden oppgaven i bunn og grunn er
    # mekanisk kategorisering. max_tokens økt til 12000 som ekstra margin siden
    # prompten har blitt lengre (kalender-tagging, eksempler).
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(150, connect=10)) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-sonnet-5", "max_tokens": 12000,
                      "thinking": {"type": "adaptive"},
                      "output_config": {"effort": "low"},
                      "system": system, "messages": [{"role": "user", "content": "Lag ukesoversikten."}]}
            )
    except httpx.TimeoutException:
        logger.error("[UKESOVERSIKT] Timeout mot Claude API")
        raise HTTPException(status_code=504, detail="AI-en brukte for lang tid, prøv igjen")

    if resp.status_code != 200:
        logger.error(f"[UKESOVERSIKT] API feil: {resp.status_code} {resp.text[:200]}")
        raise HTTPException(status_code=502, detail="Klarte ikke å hente ukesoversikt (API-feil)")

    resp_json = resp.json()
    raw = _extract_text(resp_json.get("content"))
    if not raw:
        # Skjedde tidligere ved for lavt max_tokens: modellen brukte hele budsjettet på
        # "thinking" og rakk aldri å skrive selve JSON-svaret (stop_reason: max_tokens).
        logger.error(f"[UKESOVERSIKT] Tomt tekst-svar (stop_reason={resp_json.get('stop_reason')}): {str(resp_json)[:300]}")
        raise HTTPException(status_code=502, detail="Fikk tomt svar fra AI-en, prøv igjen")
    try:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        parsed = json.loads(raw[start:end])
    except Exception as e:
        logger.error(f"[UKESOVERSIKT] Klarte ikke å tolke JSON-svar: {e} – raw: {raw[:300]}")
        raise HTTPException(status_code=502, detail="Klarte ikke å tolke svaret fra AI-en")

    def _str_list(v):
        return v if isinstance(v, list) else []

    parsed_days = parsed.get("days", []) if isinstance(parsed, dict) else []
    result_days = []
    for i, d in enumerate(day_dates):
        day_data = parsed_days[i] if i < len(parsed_days) and isinstance(parsed_days[i], dict) else {}
        people = day_data.get("people", {}) if isinstance(day_data.get("people"), dict) else {}
        day_people = {}
        for p in WEEK_PEOPLE:
            entry = people.get(p)
            if isinstance(entry, dict):
                day_people[p] = {
                    "skole": _str_list(entry.get("skole")),
                    "annet": _str_list(entry.get("annet")),
                }
            elif isinstance(entry, list):
                # Fallback hvis AI-en skulle svare med gammelt flatt listeformat
                day_people[p] = {"skole": [], "annet": entry}
            else:
                day_people[p] = {"skole": [], "annet": []}
        result_days.append({
            "day": days_no[i],
            "label": day_labels[i],
            "date": d.isoformat(),
            "is_today": d == today,
            "people": day_people
        })

    week_number = monday.isocalendar()[1]
    data = {
        "week_start": monday.isoformat(),
        "week_end": sunday.isoformat(),
        "week_number": week_number,
        "week_offset": week_offset,
        "people": WEEK_PEOPLE,
        "days": result_days,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    _week_cache[cache_key] = data
    return data


@app.post("/ai/learn")
async def ai_learn(req: dict, user: CurrentUser):
    settings = await get_user_settings(user["id"])
    api_key = _get_api_key(settings)
    if not api_key:
        return {"learned": ""}

    async with httpx.AsyncClient(timeout=15) as client:
        res = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 150,
                "system": "Trekk ut ny personkontekst fra spoersmaal og svar. Returner BARE en kort setning med ny info om person/lag/aktivitet, eller tom streng hvis ingen ny info. Maks 100 tegn. Eksempel: 'Gustav: G2015, Ready Rod, fotball'",
                "messages": [{"role": "user", "content": f"Sporsmal: {req.get('question','')}\nSvar: {req.get('answer','')}"}]
            }
        )
    learned = res.json().get("content", [{}])[0].get("text", "").strip()
    return {"learned": learned}


# ─── AI historikk ─────────────────────────────────────────────────────────────

@app.get("/ai/history")
async def get_ai_history(user: CurrentUser):
    import json
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT ai_chat_history FROM user_settings WHERE user_id = $1", user["id"]
        )
    history = json.loads(row["ai_chat_history"]) if row and row["ai_chat_history"] else []
    return {"history": history}

@app.post("/ai/history")
async def save_ai_history(payload: dict, user: CurrentUser):
    import json
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE user_settings SET ai_chat_history = $1 WHERE user_id = $2
        """, json.dumps(payload.get("history", [])), user["id"])
    return {"ok": True}
