import asyncio
import logging
from datetime import datetime, timedelta, timezone, date
from typing import Optional

logger = logging.getLogger(__name__)

ICLOUD_URL = "https://caldav.icloud.com"


async def fetch_calendar_events(
    url: str,
    username: str,
    password: str,
    calendar_filter: str = "",
    days_ahead: int = 14,
    start_date: Optional[datetime] = None
) -> list[dict]:
    """Henter kalenderoppføringer fra CalDAV (iCloud).

    Som standard hentes oppføringer fra "nå" og fremover (days_ahead dager). Send inn
    start_date (en tidssone-bevisst datetime) for å hente fra et fast tidspunkt i stedet -
    f.eks. midnatt mandag denne uken, slik at hele uken vises i ukesoversikten, inkludert
    aktiviteter tidligere i dag/uken som allerede er ferdig.
    """
    return await asyncio.to_thread(
        _fetch_sync, url, username, password, calendar_filter, days_ahead, start_date
    )


def _fetch_sync(url, username, password, calendar_filter, days_ahead, start_date=None):
    try:
        import caldav
        from icalendar import Calendar as iCal

        client = caldav.DAVClient(url=url, username=username, password=password)
        principal = client.principal()
        calendars = principal.calendars()

        # Filtrer kalendere hvis ønskelig
        filter_list = [f.strip().lower() for f in calendar_filter.split(",") if f.strip()]
        if filter_list:
            calendars = [c for c in calendars if any(
                f in (c.name or "").lower() for f in filter_list
            )]

        now = start_date if start_date is not None else datetime.now(timezone.utc)
        end = now + timedelta(days=days_ahead)

        events = []
        for cal in calendars:
            cal_name = cal.name or "Kalender"
            try:
                results = cal.search(
                    start=now,
                    end=end,
                    event=True,
                    expand=True
                )
                for item in results:
                    try:
                        ical = iCal.from_ical(item.data)
                        for component in ical.walk():
                            if component.name != "VEVENT":
                                continue
                            summary = str(component.get("SUMMARY", "")).strip()
                            if not summary:
                                continue

                            dtstart = component.get("DTSTART")
                            dtend = component.get("DTEND")
                            location = str(component.get("LOCATION", "")).strip()
                            description = str(component.get("DESCRIPTION", "")).strip()

                            start_dt = _to_datetime(dtstart.dt if dtstart else None)
                            end_dt = _to_datetime(dtend.dt if dtend else None)

                            if not start_dt:
                                continue

                            # Formater dato
                            days_no = ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"]
                            months_no = ["januar","februar","mars","april","mai","juni",
                                        "juli","august","september","oktober","november","desember"]

                            if isinstance(dtstart.dt, date) and not isinstance(dtstart.dt, datetime):
                                # Heldagsarrangement
                                time_str = f"{days_no[start_dt.weekday()]} {start_dt.day}. {months_no[start_dt.month-1]}"
                            else:
                                time_str = (
                                    f"{days_no[start_dt.weekday()]} {start_dt.day}. "
                                    f"{months_no[start_dt.month-1]} kl. "
                                    f"{start_dt.strftime('%H:%M')}"
                                )

                            event = {
                                "summary": summary,
                                "calendar": cal_name,
                                "start": start_dt.isoformat(),
                                "time_str": time_str,
                                "location": location,
                                "description": description[:200] if description else ""
                            }
                            events.append(event)
                    except Exception as e:
                        logger.debug(f"Feil ved parsing av event: {e}")
                        continue
            except Exception as e:
                logger.warning(f"Feil ved henting fra kalender '{cal_name}': {e}")
                continue

        # Sorter på startdato
        events.sort(key=lambda e: e["start"])
        return events

    except Exception as e:
        logger.error(f"CalDAV-feil: {e}")
        raise


def _to_datetime(dt) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    if isinstance(dt, date):
        return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return None


def format_events_for_ai(events: list[dict]) -> str:
    """Formater kalenderoppføringer som tekst for AI-kontekst."""
    if not events:
        return "Ingen kommende kalenderoppføringer."

    lines = []
    for e in events:
        line = f"- {e['time_str']}: {e['summary']}"
        if e.get("location"):
            line += f" ({e['location']})"
        if e.get("calendar"):
            line += f" [Kalender: {e['calendar']}]"
        if e.get("description"):
            line += f"\n  {e['description']}"
        lines.append(line)

    return "\n".join(lines)
