import asyncio
from datetime import datetime, timezone, timedelta
from spond import spond as spond_lib


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
            await s.login()
            
            results = []
            now = datetime.now(timezone.utc)
            history_from = since if since else datetime(2026, 1, 1, tzinfo=timezone.utc)

            # ── Hent gruppe-meldinger ─────────────────────────────────────────
            try:
                chats = await s.get_messages()
                if chats:
                    print(f"Spond: fant {len(chats)} chatter")
                    seen_chat_ids = set()
                    # Filtrer bort arrangement-notifikasjonschatter
                    SKIP_NAMES = {"ready idrettsforening", "ukjent", ""}
                    for chat in chats:
                        chat_id = chat.get("id", "")
                        chat_name = chat.get("name", "") or "ukjent"
                        if not chat_id or chat_id in seen_chat_ids:
                            continue
                        seen_chat_ids.add(chat_id)
                        # Hopp over generiske arrangement-chatter
                        if chat_name.lower() in SKIP_NAMES:
                            continue
                        # Hent meldinger per chat via direkte API-kall
                        try:
                            url = f"{s._chat_url}/chats/{chat_id}/messages/?max=50"
                            async with s.clientsession.get(
                                url, headers={"auth": s._auth}
                            ) as r:
                                chat_msgs = await r.json()
                            print(f"  Chat '{chat_name}': {len(chat_msgs) if chat_msgs else 0} meldinger")
                            if chat_msgs:
                                for msg in chat_msgs:
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
                        except Exception as e:
                            print(f"  Feil ved henting av meldinger fra '{chat_name}': {e}")
                else:
                    print("Spond: ingen chatter funnet")
            except Exception as e:
                print(f"Kunne ikke hente meldinger: {e}")

            # ── Hent arrangementer (historisk + fremtid) ──────────────────────
            try:
                future = now + timedelta(days=90)
                raw_events = await s.get_events(
                    min_end=history_from,
                    max_start=future,
                    include_scheduled=True
                )
                if raw_events:
                    for event in raw_events:
                        normalized = self._normalize_event(event)
                        if normalized:
                            results.append(normalized)
            except Exception as e:
                print(f"Kunne ikke hente arrangementer: {e}")

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
            msg_num  = msg.get("msgNum", "")
            chat_id  = msg.get("chatId", chat.get("id", ""))
            msg_id   = f"{chat_id}-{msg_num}" if msg_num else (msg.get("identifier") or msg.get("id", ""))

            chat_name = chat.get("name", "") or ""
            timestamp_str = msg.get("timestamp", "")
            timestamp = self._parse_timestamp(timestamp_str)

            if msg_type == "CLUB_MESSAGE":
                # Klubbmelding fra idrettslag
                club_info = msg.get("clubMessage", {}) or {}
                source_label = "Melding"
                category = chat_name or "Spond"
                title = text[:80] + ("..." if len(text) > 80 else "")
                sender_name = club_info.get("clubName", chat_name)

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

            return {
                "id": f"spond-msg-{msg_id}",
                "source": "spond",
                "sourceLabel": source_label,
                "category": category,
                "title": title,
                "tldr": text[:120] + ("..." if len(text) > 120 else ""),
                "body": text,
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
            print(f"Feil ved normalisering av melding: {e}")
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

            group_name = event.get("groupName", "") or event.get("group", {}).get("name", "") or "idrett"
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
            print(f"Feil ved normalisering av arrangement: {e}")
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
