import imaplib
import email
import os
import re
import hashlib
import logging
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)


def get_email_config() -> list[dict]:
    """
    Henter e-postkonfigurasjon fra miljøvariabler.
    
    Støtter flere e-postkontoer/filtre via nummererte variabler:
      EMAIL_1_HOST, EMAIL_1_PORT, EMAIL_1_USERNAME, EMAIL_1_PASSWORD,
      EMAIL_1_FROM_FILTER, EMAIL_1_SUBJECT_FILTER, EMAIL_1_DELETE_AFTER_FETCH,
      EMAIL_1_CATEGORY, EMAIL_1_SOURCE_LABEL
      
      EMAIL_2_HOST, EMAIL_2_PORT, ...  (for fremtidige kontoer)
    
    Eller enkelt oppsett uten nummer (bakoverkompatibelt):
      EMAIL_HOST, EMAIL_PORT, EMAIL_USERNAME, EMAIL_PASSWORD,
      EMAIL_FROM_FILTER, EMAIL_SUBJECT_FILTER, EMAIL_DELETE_AFTER_FETCH,
      EMAIL_CATEGORY, EMAIL_SOURCE_LABEL
    """
    configs = []

    # Sjekk enkelt oppsett (uten nummer)
    if os.environ.get("EMAIL_HOST"):
        configs.append(_build_config("EMAIL"))

    # Sjekk nummererte oppsett (EMAIL_1_, EMAIL_2_, osv.)
    for i in range(1, 10):
        prefix = f"EMAIL_{i}"
        if os.environ.get(f"{prefix}_HOST"):
            configs.append(_build_config(prefix))

    return configs


def _build_config(prefix: str) -> dict:
    """Bygger en konfig-dict fra miljøvariabler med gitt prefix."""
    return {
        "host":               os.environ.get(f"{prefix}_HOST", "imap.one.com"),
        "port":               int(os.environ.get(f"{prefix}_PORT", "993")),
        "username":           os.environ.get(f"{prefix}_USERNAME", ""),
        "password":           os.environ.get(f"{prefix}_PASSWORD", ""),
        # Filtrering – begge kan være tomme (da hentes alle e-poster)
        # Kan kombineres: fra_filter OG emne_filter må begge matche
        "from_filter":        os.environ.get(f"{prefix}_FROM_FILTER", ""),
        "subject_filter":     os.environ.get(f"{prefix}_SUBJECT_FILTER", ""),
        # Slett e-post fra IMAP etter henting (true/false)
        "delete_after_fetch": os.environ.get(f"{prefix}_DELETE_AFTER_FETCH", "false").lower() == "true",
        # Hvordan meldingen kategoriseres i appen
        "category":           os.environ.get(f"{prefix}_CATEGORY", "skole"),
        "source_label":       os.environ.get(f"{prefix}_SOURCE_LABEL", "Skolemelding"),
        "source":             os.environ.get(f"{prefix}_SOURCE", "skolemelding"),
    }


class EmailService:
    def __init__(self, config: dict):
        self.config = config

    def fetch_all(self) -> list[dict]:
        """Kobler til IMAP og henter e-poster basert på filtre."""
        messages = []

        try:
            label = self.config.get("source_label", "E-POST").upper()
            logger.info(f"[{label}] Kobler til IMAP: {self.config['host']} som {self.config['username']}")
            mail = imaplib.IMAP4_SSL(self.config["host"], self.config["port"])
            mail.login(self.config["username"], self.config["password"])
            mail.select("INBOX")

            # Bygg søkekriterier
            search_criteria = self._build_search_criteria()
            logger.info(f"[{label}] IMAP-søk: {search_criteria}")

            _, message_ids = mail.search(None, search_criteria)
            ids = message_ids[0].split()
            logger.info(f"[{label}] Fant {len(ids)} e-poster")

            ids_to_delete = []

            for msg_id in ids:
                try:
                    _, msg_data = mail.fetch(msg_id, "(RFC822)")
                    raw = msg_data[0][1]
                    msg = email.message_from_bytes(raw)

                    normalized = self._normalize_email(msg)
                    if normalized:
                        # Dobbeltsjekk filtre (IMAP-søk er ikke alltid eksakt)
                        if self._passes_filters(normalized["_from_raw"], normalized["_subject_raw"]):
                            messages.append(normalized)
                            if self.config["delete_after_fetch"]:
                                ids_to_delete.append(msg_id)

                except Exception as e:
                    logger.error(f"Feil ved henting av e-post {msg_id}: {e}")

            # Slett e-poster hvis konfigurert
            if ids_to_delete:
                logger.info(f"Sletter {len(ids_to_delete)} e-poster fra IMAP")
                for msg_id in ids_to_delete:
                    mail.store(msg_id, "+FLAGS", "\\Deleted")
                mail.expunge()

            mail.logout()

        except Exception as e:
            logger.error(f"IMAP-feil ({self.config['host']}): {e}")

        # Fjern interne felter før retur
        for m in messages:
            m.pop("_from_raw", None)
            m.pop("_subject_raw", None)

        return messages

    def fetch_history(self, from_date: datetime) -> list[dict]:
        """Henter historiske e-poster siden from_date. Sletter aldri e-poster."""
        messages = []
        date_str = from_date.strftime("%d-%b-%Y")  # IMAP format: 01-Jan-2026

        try:
            mail = imaplib.IMAP4_SSL(self.config["host"], self.config["port"])
            mail.login(self.config["username"], self.config["password"])
            mail.select("INBOX")

            # Bygg søk med dato-filter
            criteria_parts = [f'SINCE {date_str}']
            if self.config["from_filter"]:
                criteria_parts.append(f'FROM "{self.config["from_filter"]}"')
            if self.config["subject_filter"]:
                criteria_parts.append(f'SUBJECT "{self.config["subject_filter"]}"')

            criteria = " ".join(criteria_parts)
            logger.info(f"Historisk IMAP-søk: {criteria}")

            _, message_ids = mail.search(None, criteria)
            ids = message_ids[0].split()
            logger.info(f"Fant {len(ids)} historiske e-poster")

            for msg_id in ids:
                try:
                    _, msg_data = mail.fetch(msg_id, "(RFC822)")
                    raw = msg_data[0][1]
                    msg = email.message_from_bytes(raw)
                    normalized = self._normalize_email(msg)
                    if normalized:
                        if self._passes_filters(normalized["_from_raw"], normalized["_subject_raw"]):
                            normalized.pop("_from_raw", None)
                            normalized.pop("_subject_raw", None)
                            messages.append(normalized)
                except Exception as e:
                    logger.error(f"Feil ved henting av historisk e-post {msg_id}: {e}")

            mail.logout()
        except Exception as e:
            logger.error(f"Historisk IMAP-feil: {e}")

        return messages

    def _build_search_criteria(self) -> str:
        """Bygger IMAP-søkestreng basert på filtre."""
        criteria = []

        if self.config["from_filter"]:
            criteria.append(f'FROM "{self.config["from_filter"]}"')

        if self.config["subject_filter"]:
            criteria.append(f'SUBJECT "{self.config["subject_filter"]}"')

        if not criteria:
            return "ALL"

        if len(criteria) == 1:
            return criteria[0]

        # Kombiner med AND (IMAP standard: bare list dem opp)
        return " ".join(criteria)

    def _passes_filters(self, from_addr: str, subject: str) -> bool:
        """Dobbeltsjekker at e-posten faktisk matcher filtrene."""
        from_filter = self.config["from_filter"].lower()
        subject_filter = self.config["subject_filter"].lower()

        if from_filter and from_filter not in from_addr.lower():
            return False
        if subject_filter and subject_filter not in subject.lower():
            return False
        return True

    def _normalize_email(self, msg) -> dict | None:
        """Gjør en rå e-post om til vårt interne meldingsformat."""
        try:
            subject = self._decode_header_value(msg.get("Subject", ""))
            from_addr = self._decode_header_value(msg.get("From", ""))
            date_str = msg.get("Date", "")
            msg_id = msg.get("Message-ID", "")

            # Generer unik ID basert på Message-ID
            unique_id = hashlib.md5(msg_id.encode()).hexdigest()[:12] if msg_id else \
                        hashlib.md5(f"{from_addr}{subject}{date_str}".encode()).hexdigest()[:12]

            # Parse dato
            try:
                dt = parsedate_to_datetime(date_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            except Exception:
                dt = datetime.now(timezone.utc)

            # Hent e-posttekst
            body = self._extract_body(msg)
            if not body:
                return None

            # Lag TL;DR (første 150 tegn av body, ryddet opp)
            clean_body = self._clean_text(body)
            tldr = clean_body[:150].strip()
            if len(clean_body) > 150:
                # Kutt ved siste mellomrom
                tldr = tldr.rsplit(" ", 1)[0] + "..."

            return {
                "id": f"email-{self.config['source']}-{unique_id}",
                "source": self.config["source"],
                "sourceLabel": self.config["source_label"],
                "category": self.config["category"],
                "title": subject or "Melding fra skolen",
                "tldr": tldr,
                "body": clean_body,
                "time": self._format_relative_time(dt),
                "timestamp": dt.isoformat(),
                "priority": "medium",
                "status": "unread",
                "meta": {
                    "from": from_addr,
                    "subject": subject,
                },
                # Interne felter for filtrering (fjernes før retur)
                "_from_raw": from_addr,
                "_subject_raw": subject,
            }

        except Exception as e:
            logger.error(f"Feil ved normalisering av e-post: {e}")
            return None

    def _decode_header_value(self, value: str) -> str:
        """Dekoder e-posthoder som kan være encoded (f.eks. UTF-8 eller latin-1)."""
        if not value:
            return ""
        parts = decode_header(value)
        decoded = []
        for part, encoding in parts:
            if isinstance(part, bytes):
                decoded.append(part.decode(encoding or "utf-8", errors="replace"))
            else:
                decoded.append(part)
        return " ".join(decoded).strip()

    def _extract_body(self, msg) -> str:
        """Henter ren tekst fra e-posten (foretrekker plain text over HTML)."""
        plain = ""
        html = ""

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                disposition = str(part.get("Content-Disposition", ""))
                if "attachment" in disposition:
                    continue
                charset = part.get_content_charset() or "utf-8"
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        text = payload.decode(charset, errors="replace")
                        if content_type == "text/plain":
                            plain += text
                        elif content_type == "text/html":
                            html += text
                except Exception:
                    pass
        else:
            charset = msg.get_content_charset() or "utf-8"
            try:
                payload = msg.get_payload(decode=True)
                if payload:
                    text = payload.decode(charset, errors="replace")
                    if msg.get_content_type() == "text/html":
                        html = text
                    else:
                        plain = text
            except Exception:
                pass

        if plain:
            return self._clean_text(plain)
        elif html:
            return self._html_to_text(html)
        return ""

    def _html_to_text(self, html: str) -> str:
        """Enkel HTML-til-tekst konvertering uten eksterne biblioteker."""
        # Fjern script og style
        html = re.sub(r"<(script|style)[^>]*>.*?</(script|style)>", "", html, flags=re.DOTALL | re.IGNORECASE)
        # Erstatt vanlige block-elementer med linjeskift
        html = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
        html = re.sub(r"</(p|div|tr|li|h[1-6])>", "\n", html, flags=re.IGNORECASE)
        # Fjern alle gjenværende HTML-tagger
        html = re.sub(r"<[^>]+>", "", html)
        # Dekod HTML-entiteter
        html = html.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">") \
                   .replace("&nbsp;", " ").replace("&quot;", '"').replace("&#39;", "'")
        return self._clean_text(html)

    def _clean_text(self, text: str) -> str:
        """Rydder opp i tekst – fjerner overflødige linjeskift og mellomrom."""
        lines = [line.strip() for line in text.splitlines()]
        # Fjern mer enn to påfølgende tomme linjer
        result = []
        empty_count = 0
        for line in lines:
            if not line:
                empty_count += 1
                if empty_count <= 2:
                    result.append(line)
            else:
                empty_count = 0
                result.append(line)
        return "\n".join(result).strip()

    def _format_relative_time(self, dt: datetime) -> str:
        """Formaterer dato/tid som absolutt verdi, f.eks. '7. apr 14:32'."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%-d. %b %H:%M")
