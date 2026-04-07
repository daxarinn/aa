from __future__ import annotations

import hashlib
from dataclasses import dataclass

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
                self.time_display or "",
                self.meeting_name or "",
                self.location_text or "",
                self.venue_text or "",
                self.zoom_meeting_id or "",
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
            "start_time": self.start_time,
            "end_time": self.end_time,
            "time_display": self.time_display,
            "meeting_name": self.meeting_name,
            "subtitle": self.subtitle,
            "fellowship": self.fellowship,
            "format": self.format,
            "location_text": self.location_text,
            "venue_text": self.venue_text,
            "zoom_url": self.zoom_url,
            "zoom_meeting_id": self.zoom_meeting_id,
            "zoom_passcode": self.zoom_passcode,
            "gender_restriction": self.gender_restriction,
            "access_restriction": self.access_restriction,
            "recurrence_hint": self.recurrence_hint,
            "notes": self.notes,
            "tags_json": self.tags_json,
            "raw_json": self.raw_json,
        }
