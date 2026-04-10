import os
import asyncpg
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL er ikke satt")
        # Railway bruker postgres:// men asyncpg vil ha postgresql://
        database_url = database_url.replace("postgres://", "postgresql://")
        _pool = await asyncpg.create_pool(database_url, min_size=1, max_size=5)
        logger.info("Databasetilkobling opprettet")
    return _pool


async def init_db():
    """Oppretter tabeller hvis de ikke finnes."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id          TEXT PRIMARY KEY,
                source      TEXT NOT NULL,
                source_label TEXT NOT NULL,
                category    TEXT NOT NULL,
                title       TEXT NOT NULL,
                tldr        TEXT NOT NULL,
                body        TEXT NOT NULL,
                time_label  TEXT NOT NULL,
                timestamp   TIMESTAMPTZ NOT NULL,
                priority    TEXT NOT NULL DEFAULT 'medium',
                status      TEXT NOT NULL DEFAULT 'unread',
                meta        JSONB,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS messages_status_idx ON messages(status);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS messages_timestamp_idx ON messages(timestamp DESC);
        """)
        logger.info("Database initialisert")


async def upsert_messages(messages: list[dict]):
    """
    Lagrer nye meldinger i databasen.
    Hvis meldingen allerede finnes (samme id), oppdateres alt UNNTATT status –
    slik at brukerens lest/ikke-relevant-valg beholdes.
    """
    if not messages:
        return 0

    pool = await get_pool()
    inserted = 0

    async with pool.acquire() as conn:
        for msg in messages:
            try:
                ts = msg.get("timestamp")
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                if ts and ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                import json
                meta = json.dumps(msg.get("meta") or {})

                result = await conn.execute("""
                    INSERT INTO messages
                        (id, source, source_label, category, title, tldr, body,
                         time_label, timestamp, priority, status, meta)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
                    ON CONFLICT (id) DO UPDATE SET
                        title       = EXCLUDED.title,
                        tldr        = EXCLUDED.tldr,
                        body        = EXCLUDED.body,
                        time_label  = EXCLUDED.time_label,
                        priority    = EXCLUDED.priority,
                        meta        = EXCLUDED.meta,
                        updated_at  = NOW()
                    -- status berøres IKKE – brukerens valg beholdes
                """,
                    msg["id"],
                    msg["source"],
                    msg.get("sourceLabel", msg["source"]),
                    msg.get("category", "annet"),
                    msg.get("title", ""),
                    msg.get("tldr", ""),
                    msg.get("body", ""),
                    msg.get("time", ""),
                    ts or datetime.now(timezone.utc),
                    msg.get("priority", "medium"),
                    msg.get("status", "unread"),
                    meta
                )
                if "INSERT 0 1" in result:
                    inserted += 1
            except Exception as e:
                logger.error(f"Feil ved lagring av melding {msg.get('id')}: {e}")

    logger.info(f"Lagret {inserted} nye meldinger i databasen")
    return inserted


async def get_all_messages(limit: int = 200, user_id: str = None) -> list[dict]:
    """Henter meldinger fra databasen, nyeste først."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if user_id:
            rows = await conn.fetch("""
                SELECT id, source, source_label, category, title, tldr, body,
                       time_label, timestamp, priority, status, meta
                FROM messages
                WHERE user_id = $2
                ORDER BY timestamp DESC
                LIMIT $1
            """, limit, user_id)
        else:
            rows = await conn.fetch("""
                SELECT id, source, source_label, category, title, tldr, body,
                       time_label, timestamp, priority, status, meta
                FROM messages
                ORDER BY timestamp DESC
                LIMIT $1
            """, limit)
    return [_row_to_dict(row) for row in rows]


async def update_message_status(message_id: str, status: str, user_id: str = None) -> bool:
    """Oppdaterer status på en melding. Returnerer True hvis meldingen ble funnet."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if user_id:
            result = await conn.execute("""
                UPDATE messages
                SET status = $1, updated_at = NOW()
                WHERE id = $2 AND user_id = $3
            """, status, message_id, user_id)
        else:
            result = await conn.execute("""
                UPDATE messages
                SET status = $1, updated_at = NOW()
                WHERE id = $2
            """, status, message_id)
    return result == "UPDATE 1"


async def get_stats() -> dict:
    """Henter statistikk fra databasen."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*)                                    AS total,
                COUNT(*) FILTER (WHERE status = 'unread')  AS unread,
                COUNT(*) FILTER (WHERE status = 'read')    AS read,
                COUNT(*) FILTER (WHERE status = 'skipped') AS skipped
            FROM messages
        """)
        sources = await conn.fetch("""
            SELECT source, source_label, COUNT(*) AS count
            FROM messages
            GROUP BY source, source_label
            ORDER BY count DESC
        """)
    return {
        "total": row["total"],
        "unread": row["unread"],
        "read": row["read"],
        "skipped": row["skipped"],
        "sources": [{"source": r["source"], "label": r["source_label"], "count": r["count"]} for r in sources]
    }


def _row_to_dict(row) -> dict:
    """Konverterer en databaserad til vårt meldingsformat."""
    import json
    meta = row["meta"]
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}

    return {
        "id": row["id"],
        "source": row["source"],
        "sourceLabel": row["source_label"],
        "category": row["category"],
        "title": row["title"],
        "tldr": row["tldr"],
        "body": row["body"],
        "time": row["time_label"],
        "timestamp": row["timestamp"].isoformat() if row["timestamp"] else "",
        "priority": row["priority"],
        "status": row["status"],
        "meta": meta or {}
    }


# ─── AI / profil-funksjoner ───────────────────────────────────────────────────

async def init_ai_tables():
    """Oppretter AI-relaterte tabeller."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Lagre AI-score og TL;DR per melding
        await conn.execute("""
            ALTER TABLE messages
                ADD COLUMN IF NOT EXISTS ai_score      TEXT,
                ADD COLUMN IF NOT EXISTS ai_reason     TEXT,
                ADD COLUMN IF NOT EXISTS ai_auto_skip  BOOLEAN DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS ai_tldr       TEXT
        """)
        # Brukerprofil-tabell
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_profile (
                id          INTEGER PRIMARY KEY DEFAULT 1,
                profile     TEXT,
                updated_at  TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT single_row CHECK (id = 1)
            )
        """)
        await conn.execute("""
            INSERT INTO user_profile (id, profile)
            VALUES (1, NULL)
            ON CONFLICT (id) DO NOTHING
        """)
    logger.info("AI-tabeller initialisert")


async def get_classified_messages(limit: int = 30, user_id: str = None) -> tuple[list[dict], list[dict]]:
    """Henter eksempler på leste og ignorerte meldinger for AI-læring."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if user_id:
            read_rows = await conn.fetch("""
                SELECT id, source, source_label, category, title, tldr
                FROM messages WHERE status = 'read' AND user_id = $2
                ORDER BY updated_at DESC LIMIT $1
            """, limit, user_id)
            skip_rows = await conn.fetch("""
                SELECT id, source, source_label, category, title, tldr
                FROM messages WHERE status = 'skipped' AND user_id = $2
                ORDER BY updated_at DESC LIMIT $1
            """, limit, user_id)
        else:
            read_rows = await conn.fetch("""
                SELECT id, source, source_label, category, title, tldr
                FROM messages WHERE status = 'read'
                ORDER BY updated_at DESC LIMIT $1
            """, limit)
            skip_rows = await conn.fetch("""
                SELECT id, source, source_label, category, title, tldr
                FROM messages WHERE status = 'skipped'
                ORDER BY updated_at DESC LIMIT $1
            """, limit)

    def row_to_dict(r):
        return {
            "id": r["id"],
            "source": r["source"],
            "source_label": r["source_label"],
            "category": r["category"],
            "title": r["title"],
            "tldr": r["tldr"]
        }

    return [row_to_dict(r) for r in read_rows], [row_to_dict(r) for r in skip_rows]


async def get_user_profile(user_id: str = None) -> str | None:
    """Henter lagret brukerprofil."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if user_id:
            row = await conn.fetchrow(
                "SELECT profile FROM user_profile WHERE user_id = $1", user_id)
        else:
            row = await conn.fetchrow("SELECT profile FROM user_profile WHERE id = 1")
        return row["profile"] if row else None


async def save_user_profile(profile: str, user_id: str = None):
    """Lagrer oppdatert brukerprofil."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if user_id:
            await conn.execute("""
                INSERT INTO user_profile (user_id, profile)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET profile = $2, updated_at = NOW()
            """, user_id, profile)
        else:
            await conn.execute("""
                UPDATE user_profile SET profile = $1, updated_at = NOW() WHERE id = 1
            """, profile)
    logger.info(f"Brukerprofil oppdatert: {profile[:80]}...")


async def save_ai_result(message_id: str, tldr: str, score: str, reason: str, auto_skip: bool):
    """Lagrer AI-vurdering på en melding."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE messages SET
                ai_tldr      = $1,
                ai_score     = $2,
                ai_reason    = $3,
                ai_auto_skip = $4,
                tldr         = COALESCE($1, tldr),
                status       = CASE WHEN $4 = TRUE AND status = 'unread'
                                    THEN 'skipped' ELSE status END,
                updated_at   = NOW()
            WHERE id = $5
        """, tldr, score, reason, auto_skip, message_id)


async def get_unprocessed_messages(limit: int = 20, user_id: str = None) -> list[dict]:
    """Henter nye meldinger som ikke har fått AI-vurdering ennå."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if user_id:
            rows = await conn.fetch("""
                SELECT id, source, source_label, category, title, tldr, body
                FROM messages WHERE ai_score IS NULL AND user_id = $2
                ORDER BY timestamp DESC LIMIT $1
            """, limit, user_id)
        else:
            rows = await conn.fetch("""
                SELECT id, source, source_label, category, title, tldr, body
                FROM messages WHERE ai_score IS NULL
                ORDER BY timestamp DESC LIMIT $1
            """, limit)
    return [dict(r) for r in rows]
