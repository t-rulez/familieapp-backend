import os
import asyncio
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from spond_service import SpondService
from email_service import EmailService, get_email_config
from database import (
    init_db, init_ai_tables,
    upsert_messages, get_all_messages, update_message_status, get_stats,
    get_classified_messages, get_user_profile, save_user_profile,
    save_ai_result, get_unprocessed_messages
)
from ai_service import generate_tldr, score_relevance, generate_user_profile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

last_sync: Optional[str] = None

# ─── AI-behandling ────────────────────────────────────────────────────────────

async def run_ai_processing():
    """
    Kjorer AI-behandling pa nye meldinger:
    1. Generer TL;DR
    2. Score relevans basert pa brukerens historikk
    3. Oppdater brukerprofil hvis nok data
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.info("ANTHROPIC_API_KEY ikke satt – hopper over AI-behandling")
        return

    unprocessed = await get_unprocessed_messages(limit=20)
    if not unprocessed:
        logger.info("Ingen nye meldinger a behandle med AI")
        return

    logger.info(f"AI-behandler {len(unprocessed)} meldinger...")

    read_examples, skipped_examples = await get_classified_messages()
    user_profile = await get_user_profile()

    for msg in unprocessed:
        try:
            # 1. Generer TL;DR
            tldr = await generate_tldr(
                title=msg["title"],
                body=msg.get("body", ""),
                source_label=msg.get("source_label", "")
            )

            # 2. Score relevans
            relevance = await score_relevance(
                message=msg,
                read_examples=read_examples,
                skipped_examples=skipped_examples,
                user_profile=user_profile
            )

            # 3. Lagre AI-resultat
            await save_ai_result(
                message_id=msg["id"],
                tldr=tldr,
                score=relevance["score"],
                reason=relevance["reason"],
                auto_skip=relevance["auto_skip"]
            )

            logger.info(
                f"AI: [{relevance['score']}] {msg['title'][:50]} "
                f"– {relevance['reason']}"
            )

            # Liten pause for a ikke hammere API
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"AI-behandling feilet for {msg.get('id')}: {e}")

    # 4. Oppdater brukerprofil hvis nok data
    try:
        total_classified = len(read_examples) + len(skipped_examples)
        if total_classified >= 20:
            new_profile = await generate_user_profile(
                read_examples=read_examples,
                skipped_examples=skipped_examples,
                existing_profile=user_profile
            )
            if new_profile and new_profile != user_profile:
                await save_user_profile(new_profile)
    except Exception as e:
        logger.error(f"Profiloppdatering feilet: {e}")


# ─── Synk-funksjon ────────────────────────────────────────────────────────────

async def run_sync() -> int:
    global last_sync
    all_messages = []

    # Spond
    if os.environ.get("SPOND_USERNAME"):
        try:
            logger.info("Synkroniserer Spond...")
            spond_svc = SpondService(
                username=os.environ["SPOND_USERNAME"],
                password=os.environ["SPOND_PASSWORD"]
            )
            msgs = await spond_svc.fetch_all()
            all_messages.extend(msgs)
            logger.info(f"Spond: {len(msgs)} meldinger")
        except Exception as e:
            logger.error(f"Spond-synk feilet: {e}")

    # E-post
    for config in get_email_config():
        try:
            logger.info(f"Synkroniserer e-post: {config['source_label']}")
            svc = EmailService(config)
            msgs = await asyncio.to_thread(svc.fetch_all)
            all_messages.extend(msgs)
            logger.info(f"{config['source_label']}: {len(msgs)} meldinger")
        except Exception as e:
            logger.error(f"E-post-synk feilet ({config.get('source_label')}): {e}")

    # Lagre i database
    await upsert_messages(all_messages)

    # Kjor AI-behandling pa nye meldinger
    await run_ai_processing()

    last_sync = datetime.now(timezone.utc).isoformat()
    total = len(await get_all_messages())
    logger.info(f"Synk ferdig – {total} totalt i DB")
    return len(all_messages)


async def sync_loop():
    while True:
        try:
            await run_sync()
        except Exception as e:
            logger.error(f"Synkronisering feilet: {e}")
        await asyncio.sleep(30 * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_ai_tables()
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

def verify_token(x_api_token: Optional[str] = Header(None)):
    expected = os.environ.get("API_TOKEN")
    if expected and x_api_token != expected:
        raise HTTPException(status_code=401, detail="Ugyldig token")

# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    stats = await get_stats()
    return {"status": "ok", "last_sync": last_sync, **stats}


@app.get("/messages")
async def get_messages(x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    messages = await get_all_messages()
    return {"messages": messages, "last_sync": last_sync, "count": len(messages)}


class StatusUpdate(BaseModel):
    status: str


@app.patch("/messages/{message_id}")
async def update_message(
    message_id: str,
    update: StatusUpdate,
    x_api_token: Optional[str] = Header(None)
):
    verify_token(x_api_token)
    if update.status not in ("read", "skipped", "unread"):
        raise HTTPException(status_code=400, detail="Ugyldig status")
    found = await update_message_status(message_id, update.status)
    if not found:
        raise HTTPException(status_code=404, detail="Melding ikke funnet")

    # Trigger profiloppdatering i bakgrunnen etter nok klassifiseringer
    asyncio.create_task(_maybe_update_profile())
    return {"ok": True}


async def _maybe_update_profile():
    """Oppdater brukerprofil i bakgrunnen etter en klassifisering."""
    try:
        read_ex, skip_ex = await get_classified_messages()
        if len(read_ex) + len(skip_ex) >= 20:
            existing = await get_user_profile()
            new_profile = await generate_user_profile(read_ex, skip_ex, existing)
            if new_profile and new_profile != existing:
                await save_user_profile(new_profile)
    except Exception as e:
        logger.error(f"Bakgrunnsprofilering feilet: {e}")


@app.post("/sync")
async def trigger_sync(x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    try:
        count = await run_sync()
        return {"ok": True, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/profile")
async def get_profile(x_api_token: Optional[str] = Header(None)):
    """Viser gjeldende brukerprofil og klassifiseringsstatistikk."""
    verify_token(x_api_token)
    profile = await get_user_profile()
    read_ex, skip_ex = await get_classified_messages()
    return {
        "profile": profile,
        "classified_total": len(read_ex) + len(skip_ex),
        "read_count": len(read_ex),
        "skipped_count": len(skip_ex),
        "profile_active": (len(read_ex) + len(skip_ex)) >= 5
    }


@app.get("/config/email")
async def email_config_status(x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    configs = get_email_config()
    return {
        "configured_accounts": [
            {
                "source_label": c["source_label"],
                "username": c["username"],
                "host": c["host"],
                "from_filter": c["from_filter"],
                "subject_filter": c["subject_filter"],
                "delete_after_fetch": c["delete_after_fetch"],
                "category": c["category"],
            }
            for c in configs
        ]
    }
