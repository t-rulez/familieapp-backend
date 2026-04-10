import os
import base64
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import bcrypt
from jose import JWTError, jwt
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)

# ─── Passord-hashing (bcrypt) ─────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode('utf-8'), hashed.encode('utf-8'))

# ─── JWT-tokens ───────────────────────────────────────────────────────────────

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_DAYS = 30

def create_token(user_id: str, email: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS)
    payload = {
        "sub": user_id,
        "email": email,
        "exp": expire
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def decode_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        return None

# ─── Kryptering av sensitiv data (Fernet/AES) ────────────────────────────────
# Brukes til å kryptere Spond-passord, IMAP-passord osv. i databasen.
# ENCRYPTION_KEY må settes som miljøvariabel på Railway.

def _get_fernet() -> Fernet:
    key = os.environ.get("ENCRYPTION_KEY")
    if not key:
        raise RuntimeError("ENCRYPTION_KEY er ikke satt som miljøvariabel")
    # Aksepter både rå base64-nøkkel og vanlig streng
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception:
        # Hvis nøkkelen ikke er gyldig Fernet-format, utled fra den
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b"familie-app-salt",
            iterations=100_000,
        )
        derived = base64.urlsafe_b64encode(kdf.derive(key.encode()))
        return Fernet(derived)

def encrypt(value: str) -> str:
    """Krypterer en streng og returnerer base64-kodet ciphertext."""
    if not value:
        return ""
    f = _get_fernet()
    return f.encrypt(value.encode()).decode()

def decrypt(value: str) -> str:
    """Dekrypterer en kryptert streng."""
    if not value:
        return ""
    f = _get_fernet()
    return f.decrypt(value.encode()).decode()

def generate_encryption_key() -> str:
    """Generer en ny krypteringsnøkkel – kjør én gang og legg i Railway Variables."""
    return Fernet.generate_key().decode()


if __name__ == "__main__":
    # Kjør denne filen direkte for å generere en nøkkel:
    # python auth.py
    print("Generer ENCRYPTION_KEY for Railway:")
    print(generate_encryption_key())
