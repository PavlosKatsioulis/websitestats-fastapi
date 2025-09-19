# google_calendar.py
import os
from datetime import datetime, date, timedelta
from typing import Optional, Union

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

CALENDAR_ID   = os.getenv("GCAL_CALENDAR_ID")
TIMEZONE      = os.getenv("GCAL_TIMEZONE", "Europe/Athens")
DEFAULT_HOUR  = int(os.getenv("GCAL_DEFAULT_HOUR", "9"))
DURATION_H    = float(os.getenv("GCAL_DEFAULT_DURATION_HOURS", "2"))

CLIENT_ID     = os.getenv("GCAL_CLIENT_ID")
CLIENT_SECRET = os.getenv("GCAL_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("GCAL_REFRESH_TOKEN")

def _service():
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
    return build("calendar", "v3", credentials=creds, cache_discovery=False)

def _normalize_date_str(d: Union[str, date, datetime]) -> str:
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d")
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    s = str(d)
    return s[:10]

def _event_body(*, name: str, date_value: Union[str, date, datetime], location: str = "", description: str = ""):
    # normalize input to 'YYYY-MM-DD'
    date_str = _normalize_date_str(date_value)
    start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=DEFAULT_HOUR, minute=0, second=0, microsecond=0)
    end_dt   = start_dt + timedelta(hours=DURATION_H)
    return {
        "summary": f"Εγκατάσταση — {name}",
        "location": location or "",
        "description": description or "",
        "start": {"dateTime": start_dt.isoformat(), "timeZone": TIMEZONE},
        "end":   {"dateTime": end_dt.isoformat(),   "timeZone": TIMEZONE},
    }

def upsert_installation_event(
    *,
    company_name: str,
    probable_installation_date: Union[str, date, datetime],
    offer_link: Optional[str] = None,
    notes: Optional[str] = None,
    address: Optional[str] = None,
    existing_event_id: Optional[str] = None,
) -> str:

    """Create or update a Calendar event."""
    svc = _service()
    desc_parts = []
    if offer_link: desc_parts.append(f"Προσφορά: {offer_link}")
    if notes:      desc_parts.append(f"Σημειώσεις:\n{notes}")
    description = "\n\n".join(desc_parts) if desc_parts else ""
    body = _event_body(
        name=company_name,
        date_value=probable_installation_date,
        location=address or "",
        description=description,
    )
    if existing_event_id:
        ev = svc.events().patch(calendarId=CALENDAR_ID, eventId=existing_event_id, body=body).execute()
        return ev["id"]
    ev = svc.events().insert(calendarId=CALENDAR_ID, body=body).execute()
    return ev["id"]

def delete_installation_event(event_id: str):
    if not event_id:
        return
    svc = _service()
    try:
        svc.events().delete(calendarId=CALENDAR_ID, eventId=event_id).execute()
    except Exception:
        pass
