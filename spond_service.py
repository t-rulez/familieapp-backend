import asyncio
import logging
from datetime import datetime, timezone, timedelta
from spond import spond as spond_lib

logger = logging.getLogger(__name__)


class SpondService:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    async def fetch_all(self, since: datetime = None) -> list[dict]:
        """Henter meldinger og arrangementer fra Spond og normaliserer dem.
        since: hvis satt, hentes bare meldinger nyere enn denne datoen.
        """
        s = spond_lib.Spond(username=self.username, password=self.password)
        
        try:
            import time as _time
            t0 = _time.time()
            await s.login()
            logger.info(f"[SPOND] Login: {_time.time()-t0:.1f}s")
            
            results = []
            now = datetime.now(timezone.utc)
            history_from = since if since else datetime(2026, 1, 1, tzinfo=timezone.utc)
            logger.info(f"[SPOND] Synk siden: {history_from.strftime('%Y-%m-%d %H:%M') if since else 'alt'}")

            # ── Hent gruppe-meldinger ─────────────────────────────────────────
            try:
                # Bygg kart over gruppe-IDer til gruppenavn
                group_map = {}
                try:
                    groups = await s.get_groups()
                    if groups:
                        for g in groups:
                            gid = g.get("id", "")
                            gname = g.get("name", "")
                            if gid and gname:
                                group_map[gid.upper()] = gname
                    logger.info(f"[SPOND] {len(group_map)} grupper lastet")
                except Exception as ge:
                    logger.warning(f"[SPOND] Kunne ikke hente grupper: {ge}")

                chats = await s.get_messages()
                t1 = _time.time()
                logger.info(f"[SPOND] get_messages: {t1-t0:.1f}s")
                if chats:
                    logger.info(f"[SPOND] Fant {len(chats)} chatter")
                    seen_chat_ids = set()
                    for chat in chats:
                        chat_id = chat.get("id", "")
                        chat_type = chat.get("type", "")
                        # Hopp over 1:1 direktemeldinger
                        if chat_type in ("DIRECT", "direct"):
                            continue
                        # Hopp over chatter uten groupId (private chatter)
                        if not chat.get("groupId") and chat_type not in ("GROUP", "group", "COMMUNITY", "community"):
                            participants = chat.get("participants", [])
                            if len(participants) <= 2:
                                continue  # 1:1 chat
                        community_id = (chat.get("community") or {}).get("id", "").upper()
                        group_id = (chat.get("groupId") or "").upper()
                        chat_name = (
                            chat.get("name") or
                            group_map.get(community_id) or
                            group_map.get(group_id) or
                            (chat.get("community") or {}).get("name", "") or
                            ""
                        )
                        if not chat_id or chat_id in seen_chat_ids:
                            continue
                        seen_chat_ids.add(chat_id)
                        logger.debug(f"[SPOND] Behandler chat '{chat_name}' type={chat_type} groupId={bool(chat.get('groupId'))}")
                        # Hent meldinger per chat via direkte API-kall
                        try:
                            url = f"{s._chat_url}/chats/{chat_id}/messages/?max=50"
                            async with s.clientsession.get(
                                url, headers={"auth": s._auth}
                            ) as r:
                                chat_msgs = await r.json()
                            # chat log fjernet
                            # chat_msgs kan være en liste, dict eller noe annet
                            if isinstance(chat_msgs, dict):
                                chat_msgs = chat_msgs.get("messages", []) or []
                            elif not isinstance(chat_msgs, list):
                                chat_msgs = []
                            logger.info(f"[SPOND] Chat '{chat_name}' (id={chat_id[:8] if chat_id else 'none'}): {len(chat_msgs)} meldinger")
                            if not chat_name:
                                logger.info(f"[SPOND] Tom chat-keys: {list(chat.keys())}, community={chat.get('community')}, type={chat.get('type')}, participants={len(chat.get('participants',[]))}")
                            for msg in chat_msgs:
                                    # Hopp over hvis msg ikke er en dict
                                    if not isinstance(msg, dict):
                                        continue
                                    ts_str = msg.get("timestamp", "")
                                    if ts_str:
                                        try:
                                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                                            if ts < history_from:
                                                continue
                                        except Exception:
                                            pass
                                    normalized = self._normalize_message(msg, chat)
                                    if normalized:
                                        results.append(normalized)
                                    else:
                                        logger.warning(f"[SPOND] Melding ignorert (type={msg.get('type')}, text='{msg.get('text','')[:30]}')")
                        except Exception as e:
                            logger.warning(f"[SPOND] Feil fra chat '{chat_name}': {e}")
                else:
                    logger.info("[SPOND] Ingen chatter funnet")
            except Exception as e:
                logger.error(f"[SPOND] Meldinger feilet: {e}")

            # ── Hent arrangementer (historisk + fremtid) ──────────────────────
            try:
                future = now + timedelta(days=90)
                # Hvis since er satt, hent bare fremtidige arrangementer + siste 24t
                event_from = history_from if not since else (since - timedelta(hours=24))
                raw_events = await s.get_events(
                    min_end=event_from,
                    max_start=future,
                    include_scheduled=True
                )
                if raw_events:
                    for event in raw_events:
                        normalized = self._normalize_event(event)
                        if normalized:
                            results.append(normalized)
            except Exception as e:
                logger.error(f"[SPOND] Arrangementer feilet: {e}")

        finally:
            await s.clientsession.close()

        # Sorter nyeste først
        results.sort(key=lambda x: x["timestamp"], reverse=True)
        return results

    def _normalize_message(self, msg: dict, chat: dict) -> dict | None:
        """Gjør en rå Spond-melding om til vårt interne format."""
        try:
            text = msg.get("text", "").strip()
            if not text:
                return None

            msg_type = msg.get("type", "TEXT")
            msg_num  = msg.get("msgNum")
            chat_id  = msg.get("chatId") or chat.get("id", "")
            # Bygg unik ID fra chatId + msgNum, eller fall tilbake på andre felt
            if chat_id and msg_num is not None:
                msg_id = f"{chat_id}-{msg_num}"
            elif msg.get("identifier"):
                msg_id = msg["identifier"]
            elif msg.get("id"):
                msg_id = msg["id"]
            else:
                import hashlib
                msg_id = hashlib.md5(f"{chat_id}-{text[:50]}-{msg.get('timestamp','')}".encode()).hexdigest()[:16]

            chat_name = chat.get("name", "") or ""
            timestamp_str = msg.get("timestamp", "")
            timestamp = self._parse_timestamp(timestamp_str)

            if msg_type == "CLUB_MESSAGE":
                # Klubbmelding fra idrettslag
                club_info = msg.get("clubMessage", {}) or {}
                source_label = "Melding"
                category = chat_name or "Spond"
                title = club_info.get("subject") or text[:80]
                sender_name = club_info.get("senderName") or club_info.get("clubName", chat_name)
                raw_body = club_info.get("message") or text
                # Strip HTML-tagger
                import re
                body_text = re.sub(r'<[^>]+>', ' ', raw_body).strip()
                body_text = re.sub(r'\s+', ' ', body_text).strip()

            elif msg_type in ("TEXT", "SPOND"):
                # Direkte melding eller gruppechat
                source_label = "Chat"
                user_info = msg.get("user", {}) or {}
                sender_name = f"{user_info.get('firstName', '')} {user_info.get('lastName', '')}".strip()
                if chat_name and chat_name.lower() != "ukjent":
                    # Navngitt gruppechat
                    category = chat_name
                    title = f"{sender_name}: {text[:60]}" if sender_name else text[:80]
                else:
                    # 1:1 chat – bruk personnavnet som kategori
                    category = sender_name or "Chat"
                    title = f"{sender_name}: {text[:60]}" if sender_name else text[:80]
            else:
                source_label = "Melding"
                category = chat_name or "Spond"
                title = text[:80]
                sender_name = ""

            # For CLUB_MESSAGE, bruk body_text hvis tilgjengelig
            final_body = body_text if msg_type == "CLUB_MESSAGE" else text
            return {
                "id": f"spond-msg-{msg_id}",
                "source": "spond",
                "sourceLabel": source_label,
                "category": category,
                "title": title,
                "tldr": text[:120] + ("..." if len(text) > 120 else ""),
                "body": final_body,
                "time": self._format_relative_time(timestamp),
                "timestamp": timestamp.isoformat(),
                "priority": "medium",
                "status": "unread",
                "meta": {
                    "group": chat_name,
                    "sender": sender_name,
                    "type": msg_type
                }
            }
        except Exception as e:
            # normalisering feil ignorert
            return None

    def _normalize_event(self, event: dict) -> dict | None:
        """Gjør et rått Spond-arrangement om til vårt interne format."""
        try:
            heading = event.get("heading", "Arrangement")
            description = event.get("description", "").strip()
            
            start_str = event.get("startTimestamp", "")
            start = self._parse_timestamp(start_str)
            
            # Formater dato for TL;DR
            date_str = start.strftime("%-d. %B kl. %H:%M") if start else "ukjent tid"
            
            location = event.get("location", {})
            location_str = location.get("feature", "") if location else ""
            
            # Bygg TL;DR
            tldr_parts = [f"{heading} {date_str}"]
            if location_str:
                tldr_parts.append(f"Sted: {location_str}")
            
            # Sjekk påmeldingsstatus
            responses = event.get("responses", {})
            accepted = len(responses.get("acceptedIds", []))
            declined = len(responses.get("declinedIds", []))
            
            body = description if description else f"Arrangement: {heading}\nTidspunkt: {date_str}"
            if location_str:
                body += f"\nSted: {location_str}"
            if accepted or declined:
                body += f"\nPåmeldte: {accepted}, Avmeldte: {declined}"

            event_id = event.get("id", "")
            created_str = event.get("createdTime", start_str)
            created = self._parse_timestamp(created_str)

            # Hent gruppenavn fra recipients.group.name
            recipients = event.get("recipients") or {}
            group_obj = recipients.get("group") or {}
            group_name = (
                event.get("groupName") or
                (event.get("group") or {}).get("name") or
                group_obj.get("name") or
                "Spond"
            )
            return {
                "id": f"spond-event-{event_id}",
                "source": "spond",
                "sourceLabel": "Arrangement",
                "category": group_name,
                "title": heading,
                "tldr": " · ".join(tldr_parts),
                "body": body,
                "time": self._format_relative_time(created),
                "timestamp": created.isoformat(),
                "priority": "high" if start and (start - datetime.now(timezone.utc)).days <= 7 else "medium",
                "status": "unread",
                "meta": {
                    "eventStart": start.isoformat() if start else None,
                    "location": location_str,
                    "accepted": accepted,
                    "declined": declined,
                    "group": group_name
                }
            }
        except Exception as e:
            # normalisering feil ignorert
            return None

    def _parse_timestamp(self, ts: str) -> datetime:
        """Parser ISO-timestamp fra Spond til datetime."""
        if not ts:
            return datetime.now(timezone.utc)
        try:
            # Fjern Z og legg til UTC
            ts = ts.replace("Z", "+00:00")
            return datetime.fromisoformat(ts)
        except Exception:
            return datetime.now(timezone.utc)

    def _format_relative_time(self, dt: datetime) -> str:
        """Formaterer dato/tid som absolutt verdi, f.eks. '7. apr 14:32'."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%-d. %b %H:%M")
