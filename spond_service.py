import asyncio
from datetime import datetime, timezone, timedelta
from spond import spond as spond_lib


class SpondService:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    async def fetch_all(self) -> list[dict]:
        """Henter meldinger og arrangementer fra Spond og normaliserer dem."""
        s = spond_lib.Spond(username=self.username, password=self.password)
        
        try:
            await s.login()
            
            results = []
            now = datetime.now(timezone.utc)
            history_from = datetime(2026, 1, 1, tzinfo=timezone.utc)

            # ── Hent gruppe-meldinger ─────────────────────────────────────────
            try:
                raw_messages = await s.get_messages()
                if raw_messages:
                    print(f"Spond: fant {len(raw_messages)} chatter")
                    for chat in raw_messages:
                        chat_name = chat.get("name", "ukjent")
                        msgs_in_chat = chat.get("messages", [])
                        print(f"  Chat '{chat_name}': {len(msgs_in_chat)} meldinger")
                        for msg in msgs_in_chat:
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

            msg_id = msg.get("identifier") or msg.get("id", "")
            sender = msg.get("actor", {})
            sender_name = f"{sender.get('firstName', '')} {sender.get('lastName', '')}".strip()
            
            group_name = chat.get("name", "Spond-gruppe")
            
            timestamp_str = msg.get("timestamp", "")
            timestamp = self._parse_timestamp(timestamp_str)
            
            return {
                "id": f"spond-msg-{msg_id}",
                "source": "spond",
                "sourceLabel": "Spond",
                "category": group_name or "idrett",
                "title": f"Melding fra {sender_name}" if sender_name else group_name,
                "tldr": text[:120] + ("..." if len(text) > 120 else ""),
                "body": text,
                "time": self._format_relative_time(timestamp),
                "timestamp": timestamp.isoformat(),
                "priority": "medium",
                "status": "unread",
                "meta": {
                    "group": group_name,
                    "sender": sender_name
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
                "sourceLabel": "Spond",
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
