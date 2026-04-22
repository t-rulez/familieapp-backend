import os
import asyncio
import logging
from datetime import datetime, timezone

# Skru av støy fra tredjeparts biblioteker
logging.getLogger("icalendar").setLevel(logging.CRITICAL)
logging.getLogger("caldav").setLevel(logging.CRITICAL)
logging.getLogger("root").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

class _NoiseFilter(logging.Filter):
    """Filtrer bort støy-endepunkter fra uvicorn access-logg."""
    _SKIP = {"/ai/history", "/messages", "/health"}
    def filter(self, record):
        msg = record.getMessage()
        return not any(s in msg for s in self._SKIP)

logging.getLogger("uvicorn.access").addFilter(_NoiseFilter())
from contextlib import asynccontextmanager
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from spond_service import SpondService
from email_service import EmailService
from database import (
    init_db, init_ai_tables, init_push_tables,
    get_api_usage_stats,
    upsert_messages, get_all_messages, update_message_status,
    get_stats, get_classified_messages, get_user_profile,
    save_user_profile, save_ai_result, get_unprocessed_messages,
    save_push_subscription, delete_push_subscription, get_push_subscriptions,
    count_unread_messages
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
from ai_service import generate_tldr, score_relevance, generate_user_profile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

last_sync: Optional[str] = None

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

async def run_ai_processing(user_id: str):
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
            from database import get_pool
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

    from database import count_unread_messages
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
    from database import get_pool
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
    while True:
        try:
            await run_sync_all()
        except Exception as e:
            logger.error(f"Synkronisering feilet: {e}")
        await asyncio.sleep(30 * 60)


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
                # Allerede sendt i dag?
                if last_sent.get(uid) == today_key:
                    continue
                # Hent konfigurert tidspunkt
                time_str = user.get("evening_summary_time") or "20:00"
                try:
                    h, m = int(time_str.split(":")[0]), int(time_str.split(":")[1])
                except Exception:
                    h, m = 20, 0
                if now_oslo.hour == h and now_oslo.minute == m:
                    logger.info(f"[EVENING] Sender til {uid[:8]}...")
                    last_sent[uid] = today_key  # Marker som sendt FØR kallet
                    try:
                        await send_evening_summary(user)
                    except Exception as e:
                        logger.error(f"[EVENING] Feilet for {uid}: {e}")
            await asyncio.sleep(50)  # 50 sek – aldri to kjøringer innen samme minutt
        except Exception as e:
            logger.error(f"[EVENING] Loop feilet: {e}")
            await asyncio.sleep(60)


async def send_evening_summary(user: dict):
    """Genererer og sender morgendagens oversikt til en bruker."""
    import re, httpx, os
    from datetime import date as _date, timedelta
    from database import get_pool

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
        " AND status != 'skipped'"
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
            calendar_section = f"KALENDER:\n{format_events_for_ai(events)}\n\n"
        except Exception as e:
            logger.warning(f"[EVENING] Kalender-henting feilet: {e}")

    system = (
        f"Du er en familieassistent som gir daglige oversikter.\n"
        f"Dagens dato er {today_str}. I morgen er det {tomorrow_str}.\n"
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
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 150,
                  "system": system, "messages": [{"role": "user", "content": question}]}
        )
        if resp.status_code != 200:
            logger.error(f"[EVENING] API feil: {resp.status_code}")
            return
        answer = resp.json()["content"][0]["text"].strip()

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
    try:
        logger.info("Starter init_db...")
        await init_db()
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
    from database import get_pool

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
    from users import get_user_by_email, get_user_settings
    from auth import hash_password
    from database import get_pool

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
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=10) as _client:
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
        "family_context", "ai_learned_context", "claude_api_key"
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
    from database import get_pool
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
    from users import get_user_settings
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
        from database import get_pool
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
            from datetime import timezone
            ts = msg.get("timestamp")
            if ts:
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    msg["time"] = dt.strftime("%-d. %b %H:%M")
                except Exception:
                    pass
    await upsert_messages(payload.messages)
    if payload.user_id:
        asyncio.create_task(run_ai_processing(payload.user_id))
    return {"ok": True, "count": len(payload.messages)}


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
    from users import get_user_settings
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
    from users import get_user_settings
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
    from users import get_user_settings
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
    from users import get_user_settings
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
    from datetime import datetime, timezone
    from users import get_user_settings

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
            from spond_service import SpondService
            pw = settings.get("spond_password", "")
            if pw and settings.get("spond_username"):
                svc = SpondService(settings["spond_username"], pw)
                msgs = await svc.fetch_all()
                # fetch_all already filters from history_from in spond_service
                for m in msgs:
                    m["user_id"] = user["id"]
                all_messages.extend(msgs)
                results["spond"] = len(msgs)
        except Exception as e:
            results["errors"].append(f"Spond: {str(e)}")

    # ── E-post (Skolemelding + Showbie) ────────────────────────────────────────
    from email_service import EmailService
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
                import httpx
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
    from database import get_pool
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
    from database import get_pool
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
    from database import get_pool
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
    import httpx
    from users import get_user_settings
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

    from database import get_pool
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

    if words:
        logger.info(f"[AI/CHAT] Søkeord: {words}")
        sql = (
            "SELECT id, source, source_label, category, title, tldr, body,"
            " time_label AS time, timestamp, priority, status, meta"
            " FROM messages"
            " WHERE user_id = $1"
            " AND status != 'skipped'"
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
        logger.info(f"[AI/CHAT] Fant {len(relevant)} relevante meldinger")

    def score_msg(m):
        text = f"{m['title']} {m['tldr']} {m.get('body','')} {m.get('category','')}".lower()
        return sum(1 for w in words if w in text)

    relevant = sorted(relevant, key=score_msg, reverse=True)[:50]
    if not relevant:
        relevant = await get_all_messages(user_id=user["id"], limit=10)


    def format_msg(m):
        parts = [f"[{m['sourceLabel']} - {m.get('category','')} - {m['time']}]", f"Tittel: {m['title']}"]
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
    settings = await get_user_settings(user["id"])
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
            calendar_section = f"KALENDER (neste 14 dager):\n{format_events_for_ai(events)}\n\n"
        except Exception as e:
            logger.warning(f"Kalender-henting feilet: {e}")

    system = (
        f"Du er en hjelpsom familieassistent som hjelper med å finne informasjon fra familiens meldinger og kalender.\n"
        f"Dagens dato er {today_str}. Bruk denne datoen når du svarer på spørsmål om tid.\n"
        "Svar alltid på norsk. Vaer konkret og presis. Hvis du ikke finner svaret, si det tydelig. Ikke finn opp informasjon.\n\n"
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

    import asyncio as _asyncio
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                res = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={"model": "claude-sonnet-4-20250514", "max_tokens": 1200, "system": system, "messages": messages}
                )
            if res.status_code in (429, 529):
                wait = [5, 15, 30][attempt]
                logger.warning(f"[AI/CHAT] API overbelastet ({res.status_code}), venter {wait}s...")
                await _asyncio.sleep(wait)
                continue
            if res.status_code != 200:
                logger.error(f"[AI/CHAT] API feil: {res.status_code} {res.text[:200]}")
                return {"answer": f"Beklager, API-feil ({res.status_code}). Prøv igjen."}
            data = res.json()
            answer = data.get("content", [{}])[0].get("text", "")
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


@app.post("/ai/learn")
async def ai_learn(req: dict, user: CurrentUser):
    import httpx
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
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT ai_chat_history FROM user_settings WHERE user_id = $1", user["id"]
        )
    import json
    history = json.loads(row["ai_chat_history"]) if row and row["ai_chat_history"] else []
    return {"history": history}

@app.post("/ai/history")
async def save_ai_history(payload: dict, user: CurrentUser):
    from database import get_pool
    import json
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE user_settings SET ai_chat_history = $1 WHERE user_id = $2
        """, json.dumps(payload.get("history", [])), user["id"])
    return {"ok": True}
