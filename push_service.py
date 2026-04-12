import os
import json
import logging
import asyncio
import base64
from pywebpush import webpush, WebPushException

logger = logging.getLogger(__name__)

VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_EMAIL = os.environ.get("VAPID_EMAIL", "mailto:admin@familie.app")


def _get_private_key():
    """Returnerer privat nøkkel i riktig format for pywebpush."""
    key = VAPID_PRIVATE_KEY.strip()
    if not key:
        return None
    # Hvis det er PEM-format, konverter til raw DER base64
    if "BEGIN" in key:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption
        pem = key.encode()
        private_key = load_pem_private_key(pem, password=None)
        der = private_key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
        return base64.urlsafe_b64encode(der).decode().rstrip('=')
    return key


async def send_push(subscription: dict, title: str, body: str, tag: str = "familie") -> bool:
    """Sender ett push-varsel til én enhet."""
    private_key = _get_private_key()
    if not private_key:
        logger.warning("VAPID_PRIVATE_KEY ikke satt – push-varsler deaktivert")
        return False
    try:
        data = json.dumps({"title": title, "body": body, "tag": tag, "url": "/?filter=unread"})
        await asyncio.to_thread(
            webpush,
            subscription_info=subscription,
            data=data,
            vapid_private_key=private_key,
            vapid_claims={"sub": VAPID_EMAIL}
        )
        return True
    except WebPushException as e:
        if "410" in str(e) or "404" in str(e):
            logger.info(f"Push-abonnement utløpt: {e}")
        else:
            logger.error(f"Push-feil: {e}")
        return False
    except Exception as e:
        logger.error(f"Push-feil: {e}")
        return False


async def send_push_to_user(subscriptions: list[dict], title: str, body: str, tag: str = "familie"):
    """Sender push til alle enheter for en bruker."""
    if not subscriptions:
        return
    results = await asyncio.gather(*[
        send_push(sub, title, body, tag) for sub in subscriptions
    ], return_exceptions=True)
    ok = sum(1 for r in results if r is True)
    logger.info(f"Push sendt: {ok}/{len(subscriptions)} enheter")
