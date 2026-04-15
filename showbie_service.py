import asyncio
import logging
import base64
from datetime import datetime, timezone
from typing import Optional
import httpx

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
    def __init__(self, token: str, user_id: str, fingerprint: str = "", child_ids: list = None):
        self.token = token
        self.user_id = user_id
        self.fingerprint = fingerprint or "brief-default-fp"
        self.child_ids = child_ids or []
        self.base_url = "https://eu.showbie.com/core/v5"
        self._token_b64 = base64.b64encode(token.encode()).decode()

    def _auth_headers(self):
        fp_b64 = base64.b64encode(self.fingerprint.encode()).decode()
        return {
            **SHOWBIE_HEADERS,
            "Authorization": f"sbe token={self._token_b64},fp={fp_b64}",
        }

    async def fetch_all(self, since: datetime = None) -> list[dict]:
        """Henter alle relevante meldinger fra Showbie."""
        results = []
        async with httpx.AsyncClient(timeout=60) as client:
            # Hent barn-IDer fra /users/me
            child_ids = await self._get_child_ids(client)
            logger.info(f"Showbie: fant {len(child_ids)} barn")

            all_groups = []
            for child_id in child_ids:
                groups = await self._get_groups(client, child_id)
                all_groups.extend(groups)
            logger.info(f"Showbie: fant {len(all_groups)} grupper")

            for group in all_groups:
                group_id = group.get("id")
                group_name = group.get("name", "Showbie")
                if not group_id:
                    continue

                # Hent assignments for gruppen
                assignments = await self._get_assignments(client, group_id)
                logger.info(f"  Gruppe '{group_name}': {len(assignments)} assignments")

                for assignment in assignments:
                    aid = assignment.get("id")
                    aname = assignment.get("name", "Oppgave")
                    modified = assignment.get("sortDateStamp", 0)
                    due = assignment.get("dueDate")

                    if not aid:
                        continue

                    # Filtrer på dato
                    if since and modified:
                        mod_dt = datetime.fromtimestamp(modified / 1000, tz=timezone.utc)
                        if mod_dt < since:
                            continue

                    # Hent folders (én per elev + lærer)
                    folders = await self._get_folders(client, aid)
                    for folder in folders:
                        folder_id = folder.get("id")
                        folder_name = folder.get("name", "")
                        if not folder_id:
                            continue

                        # Hent filer i mappen
                        posts = await self._get_folder_posts(client, folder_id)
                        for post in posts:
                            msg = await self._normalize_post(
                                client, post, aname, group_name, folder_name, due
                            )
                            if msg:
                                results.append(msg)

        return results

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
                logger.info(f"Showbie me nøkler: {list(user.keys())[:15]}")
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
                logger.error(f"Showbie me feil: {r.status_code}")
        except Exception as e:
            logger.error(f"Showbie child_ids feilet: {e}")
        return self.child_ids or []

    async def _get_groups(self, client, user_id: str = None) -> list:
        try:
            headers = self._auth_headers()
            uid = user_id or self.user_id
            logger.info(f"Showbie groups URL: {self.base_url}/users/{uid}/groups")
            logger.info(f"Showbie auth: {headers.get('Authorization','')[:40]}...")
            uid = user_id or self.user_id
            r = await client.get(
                f"{self.base_url}/users/{uid}/groups",
                headers=headers
            )
            logger.info(f"Showbie groups status: {r.status_code}")
            if r.status_code == 200:
                return r.json().get("groups", [])
            else:
                logger.error(f"Showbie groups feil: {r.status_code} {r.text[:200]}")
        except Exception as e:
            logger.error(f"Showbie groups feilet: {e}")
        return []

    async def _get_assignments(self, client, group_id: str) -> list:
        try:
            r = await client.get(
                f"{self.base_url}/groups/{group_id}/assignments",
                headers=self._auth_headers()
            )
            if r.status_code == 200:
                return r.json().get("assignments", [])
        except Exception as e:
            logger.error(f"Showbie assignments feilet: {e}")
        return []

    async def _get_folders(self, client, assignment_id) -> list:
        try:
            r = await client.get(
                f"{self.base_url}/assignments/{assignment_id}/folders",
                headers=self._auth_headers()
            )
            if r.status_code == 200:
                return r.json().get("folders", [])
        except Exception as e:
            logger.error(f"Showbie folders feilet: {e}")
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
            logger.error(f"Showbie folder-posts feilet: {e}")
        return []

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
            time_str = f"{DAYS_NO[timestamp.weekday()]} {timestamp.day}. {MONTHS_NO[timestamp.month-1]}"

            # Hent innhold fra bilde via Claude Vision hvis det er en PDF/bilde
            content_text = ""
            if preview_url:
                content_text = await self._extract_content_from_preview(client, preview_url, assignment_name, file_name)

            body = f"Fil: {file_name}\nOppgave: {assignment_name}\nKlasse: {group_name}"
            if folder_name:
                body += f"\nMappe: {folder_name}"
            if content_text:
                body += f"\n\nInnhold:\n{content_text}"

            title = f"{assignment_name}: {file_name}"
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
            logger.error(f"Showbie normalisering feilet: {e}")
            return None

    async def _extract_content_from_preview(self, client, preview_url: str, assignment_name: str, file_name: str) -> str:
        """Henter preview-bilde og sender til Claude Vision for tolkning."""
        try:
            import os
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if not api_key:
                return ""

            # Last ned preview-bilde
            img_response = await client.get(preview_url)
            if img_response.status_code != 200:
                return ""

            img_data = base64.b64encode(img_response.content).decode()
            content_type = img_response.headers.get("content-type", "image/jpeg")

            # Send til Claude Vision
            async with httpx.AsyncClient(timeout=30) as ai_client:
                res = await ai_client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json"
                    },
                    json={
                        "model": "claude-sonnet-4-20250514",
                        "max_tokens": 500,
                        "messages": [{
                            "role": "user",
                            "content": [
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": content_type,
                                        "data": img_data
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": f"Dette er et bilde fra Showbie-oppgaven '{assignment_name}' (fil: {file_name}). Trekk ut all viktig informasjon på norsk: aktiviteter, datoer, ukeplan, lekser, beskjeder. Vær konkret og presis. Maks 400 tegn."
                                }
                            ]
                        }]
                    }
                )
                if res.status_code == 200:
                    return res.json().get("content", [{}])[0].get("text", "")
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
