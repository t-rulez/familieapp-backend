import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from database import get_pool
from auth import hash_password, verify_password, encrypt, decrypt

logger = logging.getLogger(__name__)


# ─── Init tabeller ────────────────────────────────────────────────────────────

async def init_user_tables():
    pool = await get_pool()
    async with pool.acquire() as conn:

        # Brukere
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id           TEXT PRIMARY KEY,
                email        TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                is_admin     BOOLEAN DEFAULT FALSE,
                created_at   TIMESTAMPTZ DEFAULT NOW(),
                last_login   TIMESTAMPTZ
            )
        """)

        # Innstillinger per bruker (sensitiv data kryptert)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id         TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,

                -- Spond
                spond_username  TEXT,
                spond_password  TEXT,  -- kryptert
                spond_enabled   BOOLEAN DEFAULT FALSE,

                -- E-post / IMAP (støtter inntil 3 kontoer)
                email_1_host            TEXT,
                email_1_port            INTEGER DEFAULT 993,
                email_1_username        TEXT,
                email_1_password        TEXT,  -- kryptert
                email_1_from_filter     TEXT,
                email_1_subject_filter  TEXT,
                email_1_delete_after    BOOLEAN DEFAULT FALSE,
                email_1_source_label    TEXT DEFAULT 'Skolemelding',
                email_1_source          TEXT DEFAULT 'skolemelding',
                email_1_category        TEXT DEFAULT 'skole',
                email_1_enabled         BOOLEAN DEFAULT FALSE,

                email_2_host            TEXT,
                email_2_port            INTEGER DEFAULT 993,
                email_2_username        TEXT,
                email_2_password        TEXT,  -- kryptert
                email_2_from_filter     TEXT,
                email_2_subject_filter  TEXT,
                email_2_delete_after    BOOLEAN DEFAULT FALSE,
                email_2_source_label    TEXT DEFAULT 'Showbie',
                email_2_source          TEXT DEFAULT 'showbie',
                email_2_category        TEXT DEFAULT 'skole',
                email_2_enabled         BOOLEAN DEFAULT FALSE,

                -- WhatsApp
                wa_group_filter TEXT,  -- kommaseparert
                wa_category     TEXT DEFAULT 'foreldre',
                wa_source_label TEXT DEFAULT 'WhatsApp',
                wa_enabled      BOOLEAN DEFAULT FALSE,

                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Meldinger kobles til bruker
        await conn.execute("""
            ALTER TABLE messages
                ADD COLUMN IF NOT EXISTS user_id TEXT REFERENCES users(id) ON DELETE CASCADE
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS messages_user_idx ON messages(user_id)
        """)

        # Brukerprofil kobles til bruker
        await conn.execute("""
            ALTER TABLE user_profile
                ADD COLUMN IF NOT EXISTS user_id TEXT REFERENCES users(id) ON DELETE CASCADE
        """)

    logger.info("Brukertabeller initialisert")


# ─── Bruker-CRUD ──────────────────────────────────────────────────────────────

async def create_user(email: str, display_name: str, password: str) -> dict:
    pool = await get_pool()
    user_id = str(uuid.uuid4())
    pw_hash = hash_password(password)

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (id, email, display_name, password_hash)
            VALUES ($1, $2, $3, $4)
        """, user_id, email.lower(), display_name, pw_hash)

        # Opprett tomme innstillinger
        await conn.execute("""
            INSERT INTO user_settings (user_id) VALUES ($1)
        """, user_id)

    logger.info(f"Ny bruker opprettet: {email}")
    return {"id": user_id, "email": email, "display_name": display_name}


async def get_user_by_email(email: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id, email, display_name, password_hash, is_admin, last_login
            FROM users WHERE email = $1
        """, email.lower())
    return dict(row) if row else None


async def get_user_by_id(user_id: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id, email, display_name, is_admin, last_login
            FROM users WHERE id = $1
        """, user_id)
    return dict(row) if row else None


async def update_last_login(user_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET last_login = NOW() WHERE id = $1
        """, user_id)


async def count_users() -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM users")


# ─── Innstillinger ────────────────────────────────────────────────────────────

async def get_user_settings(user_id: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT * FROM user_settings WHERE user_id = $1
        """, user_id)
    if not row:
        return None

    s = dict(row)

    # Dekrypter sensitive felter for intern bruk
    for field in ["spond_password", "email_1_password", "email_2_password"]:
        if s.get(field):
            try:
                s[field] = decrypt(s[field])
            except Exception:
                s[field] = ""
    return s


async def get_user_settings_safe(user_id: str) -> Optional[dict]:
    """Henter innstillinger uten passord – for visning i appen."""
    settings = await get_user_settings(user_id)
    if not settings:
        return None
    # Fjern passord fra responsen
    for field in ["spond_password", "email_1_password", "email_2_password", "password_hash"]:
        settings.pop(field, None)
    # Vis om passord er satt (uten å vise selve passordet)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                spond_password IS NOT NULL AND spond_password != '' AS spond_has_password,
                email_1_password IS NOT NULL AND email_1_password != '' AS email_1_has_password,
                email_2_password IS NOT NULL AND email_2_password != '' AS email_2_has_password
            FROM user_settings WHERE user_id = $1
        """, user_id)
    if row:
        settings.update(dict(row))
    return settings


async def update_user_settings(user_id: str, updates: dict):
    """Oppdaterer innstillinger – krypterer passord automatisk."""
    # Krypter passord-felter
    for field in ["spond_password", "email_1_password", "email_2_password"]:
        if field in updates and updates[field]:
            updates[field] = encrypt(updates[field])
        elif field in updates and not updates[field]:
            # Tom streng = ikke endre eksisterende passord
            del updates[field]

    if not updates:
        return

    pool = await get_pool()
    fields = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(updates.keys()))
    values = list(updates.values())

    async with pool.acquire() as conn:
        await conn.execute(f"""
            UPDATE user_settings
            SET {fields}, updated_at = NOW()
            WHERE user_id = $1
        """, user_id, *values)

    logger.info(f"Innstillinger oppdatert for bruker {user_id}")


# ─── Hent Spond/IMAP-konfig for synk ─────────────────────────────────────────

async def get_all_active_users() -> list[dict]:
    """Henter alle brukere med aktive integrasjoner for synkronisering."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                u.id, u.email, u.display_name,
                s.spond_username, s.spond_password, s.spond_enabled,
                s.email_1_host, s.email_1_port, s.email_1_username,
                s.email_1_password, s.email_1_from_filter, s.email_1_subject_filter,
                s.email_1_delete_after, s.email_1_source_label, s.email_1_source,
                s.email_1_category, s.email_1_enabled,
                s.email_2_host, s.email_2_port, s.email_2_username,
                s.email_2_password, s.email_2_from_filter, s.email_2_subject_filter,
                s.email_2_delete_after, s.email_2_source_label, s.email_2_source,
                s.email_2_category, s.email_2_enabled,
                s.wa_group_filter, s.wa_category, s.wa_source_label, s.wa_enabled
            FROM users u
            JOIN user_settings s ON u.id = s.user_id
            WHERE s.spond_enabled = TRUE
               OR s.email_1_enabled = TRUE
               OR s.email_2_enabled = TRUE
               OR s.wa_enabled = TRUE
        """)

    users = []
    for row in rows:
        u = dict(row)
        # Dekrypter passord
        for field in ["spond_password", "email_1_password", "email_2_password"]:
            if u.get(field):
                try:
                    u[field] = decrypt(u[field])
                except Exception:
                    u[field] = ""
        users.append(u)
    return users
