from __future__ import annotations

import hashlib
from dataclasses import dataclass


def _uid_part(value: str | None) -> str:
    if value is None:
        return ""
    text = " ".join(str(value).replace("\xa0", " ").split()).strip()
    if text.casefold() in {"nan", "n/a", "none", "null", "nat"}:
        return ""
    return text


def _stored_value(value: str | None) -> str | None:
    text = _uid_part(value)
    return text or None


@dataclass
class MeetingRecord:
    source: str
    source_page_url: str
    source_record_id: str | None
    scraped_at_utc: str
    weekday_is: str
    weekday_order: int
    start_time: str | None
    end_time: str | None
    time_display: str
    meeting_name: str | None
    subtitle: str | None
    fellowship: str | None
    format: str | None
    location_text: str | None
    venue_text: str | None
    zoom_url: str | None
    zoom_meeting_id: str | None
    zoom_passcode: str | None
    gender_restriction: str | None
    access_restriction: str | None
    recurrence_hint: str | None
    notes: str | None
    tags_json: str
    raw_json: str

    @property
    def source_uid(self) -> str:
        key = "|".join(
            [
                self.source,
                self.weekday_is,
                _uid_part(self.time_display),
                _uid_part(self.meeting_name),
                _uid_part(self.location_text),
                _uid_part(self.venue_text),
                _uid_part(self.zoom_meeting_id),
            ]
        )
        return hashlib.sha1(key.encode("utf-8")).hexdigest()

    def to_row(self) -> dict[str, str | None | int]:
        return {
            "source_uid": self.source_uid,
            "source": self.source,
            "source_page_url": self.source_page_url,
            "source_record_id": self.source_record_id,
            "scraped_at_utc": self.scraped_at_utc,
            "weekday_is": self.weekday_is,
            "weekday_order": self.weekday_order,
            "start_time": _stored_value(self.start_time),
            "end_time": _stored_value(self.end_time),
            "time_display": _uid_part(self.time_display),
            "meeting_name": _stored_value(self.meeting_name),
            "subtitle": _stored_value(self.subtitle),
            "fellowship": _stored_value(self.fellowship),
            "format": _stored_value(self.format),
            "location_text": _stored_value(self.location_text),
            "venue_text": _stored_value(self.venue_text),
            "zoom_url": _stored_value(self.zoom_url),
            "zoom_meeting_id": _stored_value(self.zoom_meeting_id),
            "zoom_passcode": _stored_value(self.zoom_passcode),
            "gender_restriction": _stored_value(self.gender_restriction),
            "access_restriction": _stored_value(self.access_restriction),
            "recurrence_hint": _stored_value(self.recurrence_hint),
            "notes": _stored_value(self.notes),
            "tags_json": self.tags_json,
            "raw_json": self.raw_json,
        }
