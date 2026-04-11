import os
import asyncio
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from spond_service import SpondService
from email_service import EmailService
from database import (
    init_db, init_ai_tables, init_push_tables,
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
    unprocessed = await get_unprocessed_messages(limit=5, user_id=user_id)
    if not unprocessed:
        return
    logger.info(f"AI-behandler {len(unprocessed)} meldinger for bruker {user_id}...")
    read_examples, skipped_examples = await get_classified_messages(user_id=user_id)
    user_profile = await get_user_profile(user_id=user_id)
    for msg in unprocessed:
        try:
            tldr = await generate_tldr(msg["title"], msg.get("body", ""), msg.get("source_label", ""))
            relevance = await score_relevance(msg, read_examples, skipped_examples, user_profile)
            await save_ai_result(msg["id"], tldr, relevance["score"], relevance["reason"], relevance["auto_skip"])
            logger.info(f"AI: [{relevance['score']}] {msg['title'][:50]}")
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
            logger.info(f"Spond for {user['email']}...")
            svc = SpondService(username=user["spond_username"], password=user["spond_password"])
            msgs = await svc.fetch_all(since=since)
            for m in msgs: m["user_id"] = user_id
            logger.info(f"Spond: {len(msgs)} meldinger")
            return msgs
        except Exception as e:
            logger.error(f"Spond-synk feilet: {e}")
            return []

    async def fetch_email(num):
        if not (user.get(f"email_{num}_enabled") and user.get(f"email_{num}_username")):
            return []
        try:
            config = _build_email_config(user, num)
            svc = EmailService(config)
            msgs = await asyncio.to_thread(svc.fetch_all)
            for m in msgs: m["user_id"] = user_id
            logger.info(f"E-post {num}: {len(msgs)} meldinger")
            return msgs
        except Exception as e:
            logger.error(f"E-post-{num}-synk feilet: {e}")
            return []

    # Kjør alle kilder parallelt
    results = await asyncio.gather(
        fetch_spond(),
        fetch_email(1),
        fetch_email(2),
        return_exceptions=True
    )

    all_messages = []
    for r in results:
        if isinstance(r, list):
            all_messages.extend(r)

    from database import count_unread_messages
    before = await count_unread_messages(user_id)
    await upsert_messages(all_messages)
    after = await count_unread_messages(user_id)
    new_count = after - before

    asyncio.create_task(run_ai_processing(user_id))
    if new_count > 0:
        asyncio.create_task(notify_new_messages(user_id, new_count))

    return len(all_messages)


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
    yield
    task.cancel()

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
        "email_2_host", "email_2_port", "email_2_username", "email_2_password",
        "email_2_from_filter", "email_2_subject_filter", "email_2_delete_after",
        "email_2_source_label", "email_2_source", "email_2_category", "email_2_enabled",
        "wa_group_filter", "wa_category", "wa_source_label", "wa_enabled",
        "family_context", "ai_learned_context"
    }
    filtered = {k: v for k, v in updates.items() if k in allowed}
    await update_user_settings(user["id"], filtered)
    return {"ok": True}

# ─── Meldinger ────────────────────────────────────────────────────────────────

@app.get("/messages")
async def get_messages(user: CurrentUser):
    messages = await get_all_messages(user_id=user["id"])
    return {"messages": messages, "last_sync": last_sync, "count": len(messages)}

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

@app.get("/whatsapp/status")
async def wa_status(user: CurrentUser):
    """Proxy til WhatsApp-tjenesten – unngår CORS."""
    wa_url = os.environ.get("WHATSAPP_SERVICE_URL", "")
    if not wa_url:
        return {"ready": False, "hasQr": False, "error": "WHATSAPP_SERVICE_URL ikke satt"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            res = await client.get(f"{wa_url}/")
            return res.json()
    except Exception as e:
        return {"ready": False, "hasQr": False, "error": str(e)}

@app.get("/whatsapp/qr")
async def wa_qr(user: CurrentUser):
    """Proxy QR-HTML fra WhatsApp-tjenesten."""
    wa_url = os.environ.get("WHATSAPP_SERVICE_URL", "")
    if not wa_url:
        raise HTTPException(status_code=503, detail="WHATSAPP_SERVICE_URL ikke satt")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            res = await client.get(f"{wa_url}/qr")
            from fastapi.responses import HTMLResponse
            return HTMLResponse(content=res.text)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

# ─── WhatsApp proxy ──────────────────────────────────────────────────────────

@app.get("/whatsapp/status")
async def whatsapp_status(user: CurrentUser):
    """Proxy til WhatsApp-tjenesten for å unngå CORS."""
    wa_url = os.environ.get("WHATSAPP_SERVICE_URL", "")
    if not wa_url:
        raise HTTPException(status_code=503, detail="WhatsApp-tjeneste ikke konfigurert")
    import httpx
    async with httpx.AsyncClient(timeout=10) as client:
        res = await client.get(f"{wa_url}/")
        return res.json()

@app.get("/whatsapp/qr")
async def whatsapp_qr(user: CurrentUser):
    """Proxy QR-kode fra WhatsApp-tjenesten."""
    wa_url = os.environ.get("WHATSAPP_SERVICE_URL", "")
    if not wa_url:
        raise HTTPException(status_code=503, detail="WhatsApp-tjeneste ikke konfigurert")
    import httpx
    async with httpx.AsyncClient(timeout=10) as client:
        res = await client.get(f"{wa_url}/qr")
        return {"html": res.text}

@app.post("/whatsapp/logout")
async def whatsapp_logout(user: CurrentUser):
    """Logger ut av WhatsApp og sletter sesjonen."""
    wa_url = os.environ.get("WHATSAPP_SERVICE_URL", "")
    if not wa_url:
        raise HTTPException(status_code=503, detail="WhatsApp-tjeneste ikke konfigurert")
    import httpx
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
    wa_url = os.environ.get("WHATSAPP_SERVICE_URL", "")
    if not wa_url:
        raise HTTPException(status_code=503, detail="WhatsApp-tjeneste ikke konfigurert")
    import httpx
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
        wa_url = os.environ.get("WHATSAPP_SERVICE_URL", "")
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
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=503, detail="AI ikke konfigurert")

    words = [w.lower() for w in req.question.split() if len(w) > 2]

    from database import get_pool
    import json as _json
    pool = await get_pool()
    seen_ids = set()
    relevant = []

    if words:
        sql = (
            "SELECT id, source, source_label, category, title, tldr, body,"
            " time_label AS time, timestamp, priority, status, meta"
            " FROM messages"
            " WHERE user_id = $1"
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
            body_preview = m['body'][:300] + ('...' if len(m['body']) > 300 else '')
            parts.append(f"Innhold: {body_preview}")
        return "\n".join(parts)

    msg_context = "\n\n".join([format_msg(m) for m in relevant])

    family_section = f"FAMILIEPROFIL:\n{req.family_context}\n" if req.family_context else ""
    auto_section = f"LAERT KONTEKST:\n{req.auto_context}\n" if req.auto_context else ""

    system = (
        "Du er en hjelpsom familieassistent som hjelper med å finne informasjon fra familiens meldinger.\n"
        "Svar alltid på norsk. Vaer konkret og presis. Hvis du ikke finner svaret, si det tydelig. Ikke finn opp informasjon.\n\n"
        + family_section
        + auto_section
        + "RELEVANTE MELDINGER:\n"
        + msg_context
    )

    messages = [
        m for m in req.history[-6:]
        if m.get("role") in ("user", "assistant") and "⏳" not in m.get("content", "")
    ]
    messages.append({"role": "user", "content": req.question})

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 800, "system": system, "messages": messages}
        )
        data = res.json()

    answer = data.get("content", [{}])[0].get("text", "Beklager, noe gikk galt.")
    return {"answer": answer}


@app.post("/ai/learn")
async def ai_learn(req: dict, user: CurrentUser):
    import httpx
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
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
