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
from database import init_db, upsert_messages, get_all_messages, update_message_status, get_stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Synk-tidspunkt (i minne) ─────────────────────────────────────────────────
last_sync: Optional[str] = None

# ─── Sync-funksjon ────────────────────────────────────────────────────────────

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

    # Lagre i database (status beholdes automatisk)
    await upsert_messages(all_messages)

    last_sync = datetime.now(timezone.utc).isoformat()
    total = len(await get_all_messages())
    logger.info(f"Synk ferdig – {len(all_messages)} hentet, {total} totalt i DB")
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
    return {
        "status": "ok",
        "last_sync": last_sync,
        **stats
    }

@app.get("/messages")
async def get_messages(x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    messages = await get_all_messages()
    return {
        "messages": messages,
        "last_sync": last_sync,
        "count": len(messages)
    }

class StatusUpdate(BaseModel):
    status: str

@app.patch("/messages/{message_id}")
async def update_message(message_id: str, update: StatusUpdate, x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    if update.status not in ("read", "skipped", "unread"):
        raise HTTPException(status_code=400, detail="Ugyldig status")
    found = await update_message_status(message_id, update.status)
    if not found:
        raise HTTPException(status_code=404, detail="Melding ikke funnet")
    return {"ok": True}

@app.post("/sync")
async def trigger_sync(x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    try:
        count = await run_sync()
        return {"ok": True, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
