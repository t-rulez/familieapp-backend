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
    init_db, init_ai_tables,
    upsert_messages, get_all_messages, update_message_status,
    get_stats, get_classified_messages, get_user_profile,
    save_user_profile, save_ai_result, get_unprocessed_messages
)
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

async def sync_user(user: dict) -> int:
    user_id = user["id"]
    all_messages = []

    # Spond
    if user.get("spond_enabled") and user.get("spond_username"):
        try:
            logger.info(f"Spond for {user['email']}...")
            svc = SpondService(username=user["spond_username"], password=user["spond_password"])
            msgs = await svc.fetch_all()
            for m in msgs:
                m["user_id"] = user_id
            all_messages.extend(msgs)
            logger.info(f"Spond: {len(msgs)} meldinger")
        except Exception as e:
            logger.error(f"Spond-synk feilet ({user['email']}): {e}")

    # E-post 1
    if user.get("email_1_enabled") and user.get("email_1_username"):
        try:
            config = _build_email_config(user, 1)
            svc = EmailService(config)
            msgs = await asyncio.to_thread(svc.fetch_all)
            for m in msgs:
                m["user_id"] = user_id
            all_messages.extend(msgs)
            logger.info(f"E-post 1: {len(msgs)} meldinger")
        except Exception as e:
            logger.error(f"E-post-1-synk feilet ({user['email']}): {e}")

    # E-post 2
    if user.get("email_2_enabled") and user.get("email_2_username"):
        try:
            config = _build_email_config(user, 2)
            svc = EmailService(config)
            msgs = await asyncio.to_thread(svc.fetch_all)
            for m in msgs:
                m["user_id"] = user_id
            all_messages.extend(msgs)
            logger.info(f"E-post 2: {len(msgs)} meldinger")
        except Exception as e:
            logger.error(f"E-post-2-synk feilet ({user['email']}): {e}")

    await upsert_messages(all_messages)
    asyncio.create_task(run_ai_processing(user_id))
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
    await init_db()
    await init_ai_tables()
    await init_user_tables()
    task = asyncio.create_task(sync_loop())
    yield
    task.cancel()

# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="Familie API")

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
        "wa_group_filter", "wa_category", "wa_source_label", "wa_enabled"
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

@app.post("/sync")
async def trigger_sync(user: CurrentUser):
    try:
        settings = await get_user_settings(user["id"])
        if not settings:
            raise HTTPException(status_code=400, detail="Ingen innstillinger funnet")
        user_with_settings = {**user, **settings}
        count = await sync_user(user_with_settings)
        return {"ok": True, "count": count}
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
    for msg in payload.messages:
        if payload.user_id:
            msg["user_id"] = payload.user_id
    await upsert_messages(payload.messages)
    if payload.user_id:
        asyncio.create_task(run_ai_processing(payload.user_id))
    return {"ok": True, "count": len(payload.messages)}
