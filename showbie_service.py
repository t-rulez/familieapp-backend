import asyncio
import logging
import base64
from datetime import datetime, timezone
from typing import Optional
import httpx

from ai_service import extract_text

logger = logging.getLogger(__name__)

SHOWBIE_HEADERS = {
    "X-Showbie-Scale": "1",
    "X-Showbie-ClientApiKey": "HYDVER0f33531300a0d39439cc3438320a37383436fb32",
    "X-Showbie-OS": "Linux",
    "X-Showbie-Device": "Brief Server",
    "X-Showbie-AppVersion": "6.7.2+59a63a6b",
    "X-Showbie-DeviceTimezone": "2",
    "X-Showbie-Accept-Language": "no",
    "Accept": "application/json",
}

DAYS_NO = ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"]
MONTHS_NO = ["januar","februar","mars","april","mai","juni","juli","august","september","oktober","november","desember"]


class ShowbieService:
    def __init__(self, token: str, user_id: str, fingerprint: str = "", child_ids: list = None, user_api_key: str = None):
        self.token = token
        self.user_id = user_id
        self.fingerprint = fingerprint or "brief-default-fp"
        self.child_ids = child_ids or []
        self.user_api_key = user_api_key or ""
        self.base_url = "https://eu.showbie.com/core/v5"
        self._token_b64 = base64.b64encode(token.encode()).decode()

    def _auth_headers(self):
        fp_b64 = base64.b64encode(self.fingerprint.encode()).decode()
        return {
            **SHOWBIE_HEADERS,
            "Authorization": f"sbe token={self._token_b64},fp={fp_b64}",
        }

    async def fetch_all(self, since: datetime = None, upsert_fn=None, user_id: str = None, existing_ids: set = None) -> list[dict]:
        """Henter alle relevante meldinger fra Showbie.
        upsert_fn: async funksjon for å lagre meldinger fortløpende
        existing_ids: sett med allerede lagrede message-IDer (hopp over Claude Vision)
        """
        results = []
        existing_ids = existing_ids or set()
        async with httpx.AsyncClient(timeout=120) as client:
            classes = await self._get_classes(client)
            class_map = {c["id"]: c for c in classes}
            logger.info(f"[SHOWBIE] Fant {len(classes)} klasser")

            for child_id in self.child_ids:
                assignments = await self._get_child_assignments(client, child_id)
                logger.info(f"[SHOWBIE] Barn {child_id[:8]}: {len(assignments)} assignments")

                for assignment in assignments:
                    class_id = assignment.get("class", "")
                    group = class_map.get(class_id, {})
                    assignment["_group_name"] = group.get("name", "Showbie")
                    assignment["_child_id"] = child_id
                    aid = assignment.get("id")
                    aname = assignment.get("name", "Oppgave")
                    modified = assignment.get("sortDateStamp", 0)
                    due = assignment.get("dueDate")

                    if not aid:
                        continue

                    # Filtrer på dato – hopp over gamle assignments
                    if since and modified:
                        mod_dt = datetime.fromtimestamp(modified / 1000, tz=timezone.utc)
                        if mod_dt < since:
                            continue

                    folders = await self._get_folders(client, aid, child_id)
                    for folder in folders:
                        folder_id = folder.get("id")
                        folder_name = folder.get("name", "")
                        if not folder_id:
                            continue

                        posts = await self._get_folder_posts(client, folder_id)
                        # Hent brukernavn fra folder-posts response (for kommentarer)
                        for post in posts:
                            post_type = post.get("type", "F")
                            created = post.get("createdDateStamp", 0)

                            # Bygg ID og sjekk om allerede finnes
                            if post_type in ("C", "V", "A"):  # Comment/Voice/Annotation
                                post_id = f"showbie-comment-{post.get('id', created)}-{created}"
                            else:
                                file_name = post.get("fileName", post.get("name", ""))
                                post_id = f"showbie-{file_name.replace('.','_')}-{created}"

                            if post_id in existing_ids:
                                continue

                            if post_type in ("C", "V", "A"):
                                # Tekstkommentar – hent avsendernavn
                                sender = post.get("authorName") or post.get("author", {}).get("displayName", "") if isinstance(post.get("author"), dict) else ""
                                msg = await self._normalize_comment(
                                    post, aname, assignment["_group_name"], folder_name, sender
                                )
                            else:
                                # Fil/PDF/bilde
                                msg = await self._normalize_post(
                                    client, post, aname, assignment["_group_name"], folder_name, due
                                )

                            if msg:
                                if user_id:
                                    msg["user_id"] = user_id
                                results.append(msg)
                                if upsert_fn:
                                    try:
                                        await upsert_fn([msg])
                                    except Exception as e:
                                        logger.error(f"[SHOWBIE] Lagring feilet: {e}")
                                # Lagre fortløpende
                                if upsert_fn:
                                    try:
                                        await upsert_fn([msg])
                                        logger.info(f"[SHOWBIE] Lagret: {msg['title'][:50]}")
                                    except Exception as e:
                                        logger.error(f"[SHOWBIE] Lagring feilet: {e}")

        return results

    async def _get_child_assignments(self, client, child_id: str) -> list:
        """Henter assignments for et barn via barnets user_id."""
        try:
            r = await client.get(
                f"{self.base_url}/users/{child_id}/assignments",
                headers=self._auth_headers()
            )
            if r.status_code == 200:
                return r.json().get("assignments", [])
            logger.error(f"[SHOWBIE]  child assignments {child_id[:8]}: {r.status_code} {r.text[:100]}")
        except Exception as e:
            logger.error(f"[SHOWBIE]  child assignments feilet: {e}")
        return []

    async def _get_classes(self, client) -> list:
        """Henter klasser direkte fra foreldrekontoen."""
        try:
            r = await client.get(
                f"{self.base_url}/users/{self.user_id}/classes",
                headers=self._auth_headers()
            )
            if r.status_code == 200:
                return r.json().get("classes", [])
            else:
                logger.error(f"[SHOWBIE]  classes feil: {r.status_code} {r.text[:200]}")
        except Exception as e:
            logger.error(f"[SHOWBIE]  classes feilet: {e}")
        return []

    async def _get_child_ids(self, client) -> list:
        """Henter barn-IDer fra /users/me metadata."""
        try:
            r = await client.get(
                f"{self.base_url}/users/me",
                headers=self._auth_headers()
            )
            if r.status_code == 200:
                data = r.json()
                user = data.get("user", {})
                # Logg alle nøkler for debugging
                logger.info(f"[SHOWBIE]  me nøkler: {list(user.keys())[:15]}")
                # Prøv ulike felt som kan inneholde barn
                children = (
                    user.get("children") or
                    user.get("relatives") or
                    user.get("students") or
                    []
                )
                if children:
                    return [c.get("id") for c in children if c.get("id")]
                # Fallback: bruk stored child_ids hvis de er lagret
                if self.child_ids:
                    return self.child_ids
            else:
                logger.error(f"[SHOWBIE]  me feil: {r.status_code}")
        except Exception as e:
            logger.error(f"[SHOWBIE]  child_ids feilet: {e}")
        return self.child_ids or []

    async def _get_groups(self, client, user_id: str = None) -> list:
        try:
            headers = self._auth_headers()
            uid = user_id or self.user_id
            logger.info(f"[SHOWBIE]  groups URL: {self.base_url}/users/{uid}/groups")
            logger.info(f"[SHOWBIE]  auth: {headers.get('Authorization','')[:40]}...")
            uid = user_id or self.user_id
            r = await client.get(
                f"{self.base_url}/users/{uid}/groups",
                headers=headers
            )
            logger.info(f"[SHOWBIE]  groups status: {r.status_code}")
            if r.status_code == 200:
                return r.json().get("groups", [])
            else:
                logger.error(f"[SHOWBIE]  groups feil: {r.status_code} {r.text[:200]}")
        except Exception as e:
            logger.error(f"[SHOWBIE]  groups feilet: {e}")
        return []

    async def _get_assignments(self, client, group_id: str) -> list:
        try:
            # Prøv klasse-endepunktet
            r = await client.get(
                f"{self.base_url}/classes/{group_id}/assignments",
                headers=self._auth_headers()
            )
            if r.status_code == 200:
                return r.json().get("assignments", [])
            # Fallback: prøv via bruker
            r2 = await client.get(
                f"{self.base_url}/users/{self.user_id}/classes/{group_id}/assignments",
                headers=self._auth_headers()
            )
            if r2.status_code == 200:
                return r2.json().get("assignments", [])
            logger.error(f"[SHOWBIE]  assignments {group_id}: {r.status_code}, {r2.status_code} - {r.text[:100]}")
        except Exception as e:
            logger.error(f"[SHOWBIE]  assignments feilet: {e}")
        return []

    async def _get_folders(self, client, assignment_id, child_id: str = None) -> list:
        try:
            # Hent kun barnets mappe hvis child_id er kjent
            if child_id:
                r = await client.get(
                    f"{self.base_url}/assignments/{assignment_id}/folders?student={child_id}",
                    headers=self._auth_headers()
                )
            else:
                r = await client.get(
                    f"{self.base_url}/assignments/{assignment_id}/folders",
                    headers=self._auth_headers()
                )
            if r.status_code == 200:
                return r.json().get("folders", [])
            logger.debug(f"[SHOWBIE] Ingen tilgang til assignment {assignment_id} (403 – ikke barnets)")
        except Exception as e:
            logger.error(f"[SHOWBIE]  folders feilet: {e}")
        return []

    async def _get_folder_posts(self, client, folder_id) -> list:
        try:
            r = await client.get(
                f"{self.base_url}/folders/{folder_id}/folder-posts",
                headers=self._auth_headers()
            )
            if r.status_code == 200:
                return r.json().get("folderPosts", [])
        except Exception as e:
            logger.error(f"[SHOWBIE]  folder-posts feilet: {e}")
        return []

    def _clean_comment_text(self, text: str) -> str:
        """Renser Showbie-kommentartekst for støy og boilerplate."""
        import re as _re
        # Fjern URL-er
        text = _re.sub(r'https?://\S+', '', text)
        # Fjern Showbie boilerplate
        boilerplate = [
            r'Email notification sent by Showbie[^\n]*',
            r'Notification Preferences[^\n]*',
            r'Reply to [^\n]*Comment[^\n]*',
            r'\w+ \w+ \w+ \d+,? \d+:\d+[ap]m',
            r'\b\w+ \d+,? \d+:\d+[ap]m',
            r'Showbie\s*\(\s*\)',
            r'\(\s*\)',
        ]
        for pattern in boilerplate:
            text = _re.sub(pattern, '', text, flags=_re.IGNORECASE)
        # Fjern duplikatlinjer
        seen = []
        unique_lines = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            normalized = ' '.join(stripped.lower().split())
            if normalized not in seen and len(normalized) > 3:
                seen.append(normalized)
                unique_lines.append(stripped)
        text = '\n'.join(unique_lines)
        return _re.sub(r'\n{3,}', '\n\n', text).strip()

    async def _normalize_comment(
        self, post: dict, assignment_name: str,
        group_name: str, folder_name: str, sender_name: str
    ) -> Optional[dict]:
        """Normaliserer en tekstkommentar fra Showbie."""
        try:
            created = post.get("createdDateStamp", 0)
            post_id = f"showbie-comment-{post.get('id', created)}-{created}"
            timestamp = datetime.fromtimestamp(created / 1000, tz=timezone.utc) if created else datetime.now(timezone.utc)
            time_str = timestamp.strftime("%-d. %b %H:%M")

            # Hent kommentartekst – kan ligge i ulike felt
            raw_text = (
                post.get("body") or
                post.get("text") or
                post.get("content") or
                post.get("comment") or ""
            )
            if not raw_text or len(raw_text.strip()) < 5:
                return None

            clean = self._clean_comment_text(raw_text)
            if not clean:
                return None

            title = f"{assignment_name}: {sender_name}" if sender_name else assignment_name
            tldr = clean[:120] + ("..." if len(clean) > 120 else "")

            body = f"Fra: {sender_name}\nOppgave: {assignment_name}\nKlasse: {group_name}"
            if folder_name:
                body += f"\nMappe: {folder_name}"
            body += f"\n\n{clean}"

            return {
                "id": post_id,
                "source": "showbie",
                "sourceLabel": "Showbie",
                "category": group_name,
                "title": title,
                "tldr": tldr,
                "body": body,
                "time": time_str,
                "timestamp": timestamp.isoformat(),
                "priority": "medium",
                "status": "unread",
                "meta": {
                    "assignment": assignment_name,
                    "folder": folder_name,
                    "group": group_name,
                    "sender": sender_name,
                }
            }
        except Exception as e:
            logger.error(f"[SHOWBIE] Kommentar normalisering feilet: {e}")
            return None

    async def _normalize_post(
        self, client, post: dict, assignment_name: str,
        group_name: str, folder_name: str, due_date
    ) -> Optional[dict]:
        try:
            file_name = post.get("name", "")
            file_type = post.get("type", "")
            created = post.get("createdDateStamp", 0)
            preview_url = post.get("previewHiResUrl", "")

            # Bygg unik ID
            post_id = f"showbie-{post.get('fileName','').replace('.','_')}-{created}"

            timestamp = datetime.fromtimestamp(created / 1000, tz=timezone.utc) if created else datetime.now(timezone.utc)

            # Formater dato
            time_str = timestamp.strftime("%-d. %b %H:%M")

            # Hent innhold fra bilde via Claude Vision hvis det er en PDF/bilde
            content_text = ""
            if preview_url:
                content_text = await self._extract_content_from_preview(client, preview_url, assignment_name, file_name)

            # Hopp over meldinger uten filnavn og uten innhold
            if not file_name and not content_text:
                return None

            body = f"Fil: {file_name}\nOppgave: {assignment_name}\nKlasse: {group_name}"
            if folder_name:
                body += f"\nMappe: {folder_name}"
            if content_text:
                body += f"\n\nInnhold:\n{content_text}"

            title = f"{assignment_name}: {file_name}".rstrip(": ").rstrip()
            tldr = content_text[:120] if content_text else f"{assignment_name} – {file_name}"

            return {
                "id": post_id,
                "source": "showbie",
                "sourceLabel": "Showbie",
                "category": group_name,
                "title": title,
                "tldr": tldr,
                "body": body,
                "time": time_str,
                "timestamp": timestamp.isoformat(),
                "priority": "medium",
                "status": "unread",
                "meta": {
                    "assignment": assignment_name,
                    "file": file_name,
                    "folder": folder_name,
                    "group": group_name,
                }
            }
        except Exception as e:
            logger.error(f"[SHOWBIE]  normalisering feilet: {e}")
            return None

    async def _extract_content_from_preview(self, client, preview_url: str, assignment_name: str, file_name: str) -> str:
        """Henter preview-bilder (alle sider) og sender til Claude Vision for tolkning."""
        try:
            import os, re as _re
            api_key = self.user_api_key or os.environ.get("ANTHROPIC_API_KEY", "")
            if not api_key:
                return ""

            # Hent alle tilgjengelige sider (preview_000, preview_001, ...)
            image_contents = []
            base_url = _re.sub(r'preview_\d+\.jpg', 'preview_{:03d}.jpg', preview_url)

            for page_num in range(10):  # Maks 10 sider
                page_url = base_url.format(page_num)
                img_response = await client.get(page_url)
                if img_response.status_code != 200:
                    break  # Ingen flere sider
                img_data = base64.b64encode(img_response.content).decode()
                content_type = img_response.headers.get("content-type", "image/jpeg")
                image_contents.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": content_type, "data": img_data}
                })
                logger.info(f"[SHOWBIE] Hentet side {page_num + 1} av '{file_name}'")

            if not image_contents:
                return ""

            # Legg til instruksjonstekst
            image_contents.append({
                "type": "text",
                "text": f"Dette er {len(image_contents)} side(r) fra Showbie-oppgaven '{assignment_name}' (fil: {file_name}). Trekk ut ALL viktig informasjon fra ALLE sider på norsk: aktiviteter, datoer, ukeplan, lekser, oppgaver, beskjeder. Ta med alt av betydning. Ingen begrensning på lengde."
            })

            # Send til Claude Vision
            async with httpx.AsyncClient(timeout=60) as ai_client:
                res = await ai_client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json"
                    },
                    json={
                        "model": "claude-sonnet-5",
                        "max_tokens": 2000,
                        "messages": [{"role": "user", "content": image_contents}]
                    }
                )
                if res.status_code == 200:
                    data = res.json()
                    usage = data.get("usage", {})
                    if self.user_id:
                        try:
                            from database import log_api_usage
                            await log_api_usage(
                                self.user_id, "showbie", "vision",
                                usage.get("input_tokens", 0),
                                usage.get("output_tokens", 0)
                            )
                        except Exception:
                            pass
                    return extract_text(data.get("content", []))
        except Exception as e:
            logger.error(f"Claude Vision feilet: {e}")
        return ""


async def test_showbie_connection(token: str, user_id: str, fingerprint: str = "") -> dict:
    """Tester Showbie-tilkobling og returnerer brukerinfo."""
    token_b64 = base64.b64encode(token.encode()).decode()
    fp_raw = fingerprint or "brief-default-fp"
    fp_b64 = base64.b64encode(fp_raw.encode()).decode()
    headers = {
        **SHOWBIE_HEADERS,
        "Authorization": f"sbe token={token_b64},fp={fp_b64}",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://eu.showbie.com/core/v5/users/me",
                headers=headers
            )
            if r.status_code == 200:
                user = r.json().get("user", {})
                return {
                    "ok": True,
                    "name": user.get("displayName", ""),
                    "email": user.get("email", ""),
                    "user_id": user.get("id", "")
                }
            return {"ok": False, "error": f"Status {r.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
