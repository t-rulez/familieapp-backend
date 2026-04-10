import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from spond_service import SpondService
from models import Message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── In-memory cache (ersattes med DB i fase 3) ───────────────────────────────

cache: dict = {
    "messages": [],
    "last_sync": None
}

# ─── Background sync ──────────────────────────────────────────────────────────

async def sync_loop():
    """Synkroniserer Spond i bakgrunnen hvert 30. minutt."""
    while True:
        try:
            logger.info("Starter Spond-synkronisering...")
            spond_svc = SpondService(
                username=os.environ["SPOND_USERNAME"],
                password=os.environ["SPOND_PASSWORD"]
            )
            messages = await spond_svc.fetch_all()
            
            # Behold status (lest/ikke relevant) fra eksisterende meldinger
            existing = {m["id"]: m for m in cache["messages"]}
            for msg in messages:
                if msg["id"] in existing:
                    msg["status"] = existing[msg["id"]]["status"]
            
            cache["messages"] = messages
            cache["last_sync"] = datetime.now(timezone.utc).isoformat()
            logger.info(f"Synkronisert {len(messages)} meldinger fra Spond")
        except Exception as e:
            logger.error(f"Synkronisering feilet: {e}")
        
        await asyncio.sleep(30 * 60)  # 30 minutter

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(sync_loop())
    yield
    task.cancel()

# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="Familie API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Begrens til Vercel-domenet ditt i prod
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
    return {"status": "ok", "last_sync": cache["last_sync"]}

@app.get("/messages")
def get_messages(x_api_token: Optional[str] = Header(None)):
    verify_token(x_api_token)
    return {
        "messages": cache["messages"],
        "last_sync": cache["last_sync"],
        "count": len(cache["messages"])
    }

class StatusUpdate(BaseModel):
    status: str  # "read", "skipped", "unread"

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
    """Tving en manuell synkronisering."""
    verify_token(x_api_token)
    try:
        spond_svc = SpondService(
            username=os.environ["SPOND_USERNAME"],
            password=os.environ["SPOND_PASSWORD"]
        )
        messages = await spond_svc.fetch_all()
        existing = {m["id"]: m for m in cache["messages"]}
        for msg in messages:
            if msg["id"] in existing:
                msg["status"] = existing[msg["id"]]["status"]
        cache["messages"] = messages
        cache["last_sync"] = datetime.now(timezone.utc).isoformat()
        return {"ok": True, "count": len(messages)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
