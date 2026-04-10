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
from models import Message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── In-memory cache ──────────────────────────────────────────────────────────

cache: dict = {
    "messages": [],
    "last_sync": None
}

# ─── Sync-funksjon ────────────────────────────────────────────────────────────

async def run_sync():
    """Henter meldinger fra alle kilder og merger med eksisterende cache."""
    all_messages = []

    # Spond
    if os.environ.get("SPOND_USERNAME"):
        try:
            logger.info("Synkroniserer Spond...")
            spond_svc = SpondService(
                username=os.environ["SPOND_USERNAME"],
                password=os.environ["SPOND_PASSWORD"]
            )
            spond_msgs = await spond_svc.fetch_all()
            all_messages.extend(spond_msgs)
            logger.info(f"Spond: {len(spond_msgs)} meldinger")
        except Exception as e:
            logger.error(f"Spond-synk feilet: {e}")

    # E-post (Skolemelding, Showbie, osv.)
    email_configs = get_email_config()
    if email_configs:
        for config in email_configs:
            try:
                logger.info(f"Synkroniserer e-post: {config['source_label']} ({config['username']})")
                svc = EmailService(config)
                email_msgs = await asyncio.to_thread(svc.fetch_all)
                all_messages.extend(email_msgs)
                logger.info(f"{config['source_label']}: {len(email_msgs)} meldinger")
            except Exception as e:
                logger.error(f"E-post-synk feilet ({config.get('source_label')}): {e}")

    # Merger med eksisterende cache (behold status-endringer)
    existing = {m["id"]: m for m in cache["messages"]}
    for msg in all_messages:
        if msg["id"] in existing:
            msg["status"] = existing[msg["id"]]["status"]

    all_messages.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

    cache["messages"] = all_messages
    cache["last_sync"] = datetime.now(timezone.utc).isoformat()
    logger.info(f"Synk ferdig - totalt {len(all_messages)} meldinger")


async def sync_loop():
    """Kjorer synkronisering i bakgrunnen hvert 30. minutt."""
    while True:
        try:
            await run_sync()
        except Exception as e:
            logger.error(f"Synkronisering feilet: {e}")
        await asyncio.sleep(30 * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
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
def root():
    sources = list(set(m["source"] for m in cache["messages"]))
    return {
        "status": "ok",
        "last_sync": cache["last_sync"],
        "message_count": len(cache["messages"]),
        "sources": sources
    }

@app.get("/messages")
def get_messages(x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    return {
        "messages": cache["messages"],
        "last_sync": cache["last_sync"],
        "count": len(cache["messages"])
    }

class StatusUpdate(BaseModel):
    status: str

@app.patch("/messages/{message_id}")
def update_message(message_id: str, update: StatusUpdate, x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    if update.status not in ("read", "skipped", "unread"):
        raise HTTPException(status_code=400, detail="Ugyldig status")
    for msg in cache["messages"]:
        if msg["id"] == message_id:
            msg["status"] = update.status
            return {"ok": True}
    raise HTTPException(status_code=404, detail="Melding ikke funnet")

@app.post("/sync")
async def trigger_sync(x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    try:
        await run_sync()
        return {"ok": True, "count": len(cache["messages"])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/config/email")
def get_email_config_status(x_api_token: Optional[str] = Header(None)):
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
