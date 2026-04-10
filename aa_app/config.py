from __future__ import annotations

from datetime import timedelta
from pathlib import Path

AA_ALL_MEETINGS_URL = "https://aa.is/aa-fundir/allir-fundir"
AA_ALL_MEETINGS_ALL_URL = f"{AA_ALL_MEETINGS_URL}?limit=0"
GULA_ALL_MEETINGS_URL = "https://gula.is/fundarskra/allir-fundir?view=all"
CODA_MEETINGS_URL = "https://coda.is/fundir/"
ALANON_MEETINGS_URL = "https://al-anon.is/alanon/fundaskra/"
TWELVE_STEP_HOUSE_MEETINGS_URL = "https://12sporahusid.is/meetings/?d=any"

AA_DAY_PAGES = [
    ("Mánudagur", "https://aa.is/aa-fundir/manudagur"),
    ("Þriðjudagur", "https://aa.is/aa-fundir/thridjudagur"),
    ("Miðvikudagur", "https://aa.is/aa-fundir/midvikudagur"),
    ("Fimmtudagur", "https://aa.is/aa-fundir/fimmtudagur"),
    ("Föstudagur", "https://aa.is/aa-fundir/fostudagur"),
    ("Laugardagur", "https://aa.is/aa-fundir/laugardagur"),
    ("Sunnudagur", "https://aa.is/aa-fundir/sunnudagur"),
]

FJAR_DAY_PAGES = [
    ("Mánudagur", "https://www.fjarfundir.org/manudagur/"),
    ("Þriðjudagur", "https://www.fjarfundir.org/thridjudagur/"),
    ("Miðvikudagur", "https://www.fjarfundir.org/midvikudagur/"),
    ("Fimmtudagur", "https://www.fjarfundir.org/fimmtudagur/"),
    ("Föstudagur", "https://www.fjarfundir.org/"),
    ("Laugardagur", "https://www.fjarfundir.org/laugardagur/"),
    ("Sunnudagur", "https://www.fjarfundir.org/sunnudagur/"),
]

WEEKDAY_ORDER = {
    "Mánudagur": 1,
    "Þriðjudagur": 2,
    "Miðvikudagur": 3,
    "Fimmtudagur": 4,
    "Föstudagur": 5,
    "Laugardagur": 6,
    "Sunnudagur": 7,
}

DEFAULT_DB_PATH = Path("data/meetings.sqlite")
DEFAULT_CSV_PATH = Path("exports/meetings_latest.csv")
FILTERS_COOKIE_NAME = "aa_filters"
FAVORITES_COOKIE_NAME = "aa_favorites"
CLIENT_COOKIE_NAME = "aa_client_id"
DEFAULT_CALENDAR_EVENT_DURATION = timedelta(hours=1)
ICAL_BYDAY_BY_ORDER = {
    1: "MO",
    2: "TU",
    3: "WE",
    4: "TH",
    5: "FR",
    6: "SA",
    7: "SU",
}
SIZE_BIN_OPTIONS = [
    {"value": "2-9", "label": "2-9", "midpoint": 5.5},
    {"value": "10-19", "label": "10-19", "midpoint": 14.5},
    {"value": "20-39", "label": "20-39", "midpoint": 29.5},
    {"value": "40+", "label": "40+", "midpoint": 45.0},
]
SIZE_BIN_VALUES = {item["value"] for item in SIZE_BIN_OPTIONS}
CHURCH_LOCATION_ICON_KEY = "__church_locations__"
SOURCE_PRIORITIES = {
    "al-anon.is": 5,
    "coda.is": 4,
    "fjarfundir.org": 3,
    "12sporahusid.is": 3,
    "gula.is": 2,
    "aa.is": 1,
}


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meetings (
    source_uid TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_page_url TEXT NOT NULL,
    source_record_id TEXT,
    scraped_at_utc TEXT NOT NULL,
    weekday_is TEXT NOT NULL,
    weekday_order INTEGER NOT NULL,
    start_time TEXT,
    end_time TEXT,
    time_display TEXT NOT NULL,
    meeting_name TEXT,
    subtitle TEXT,
    fellowship TEXT,
    format TEXT,
    location_text TEXT,
    venue_text TEXT,
    zoom_url TEXT,
    zoom_meeting_id TEXT,
    zoom_passcode TEXT,
    gender_restriction TEXT,
    access_restriction TEXT,
    recurrence_hint TEXT,
    notes TEXT,
    tags_json TEXT,
    raw_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    run_id TEXT PRIMARY KEY,
    scraped_at_utc TEXT NOT NULL,
    record_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS location_aliases (
    alias_location_text TEXT PRIMARY KEY,
    canonical_location_text TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS location_metadata (
    canonical_location_text TEXT PRIMARY KEY,
    nickname TEXT,
    icon_emoji TEXT,
    icon_bg_color TEXT,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS manual_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_kind TEXT NOT NULL,
    title TEXT NOT NULL,
    weekday_is TEXT NOT NULL,
    weekday_order INTEGER NOT NULL,
    start_time TEXT,
    end_time TEXT,
    time_display TEXT NOT NULL,
    subtitle TEXT,
    location_text TEXT,
    venue_text TEXT,
    notes TEXT,
    source_page_url TEXT,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meeting_size_reports (
    source_uid TEXT NOT NULL,
    client_id TEXT NOT NULL,
    size_bin TEXT NOT NULL,
    reported_at_utc TEXT NOT NULL,
    PRIMARY KEY (source_uid, client_id)
);

CREATE TABLE IF NOT EXISTS meeting_merges (
    duplicate_source_uid TEXT PRIMARY KEY,
    canonical_source_uid TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_meeting_merges_canonical
    ON meeting_merges (canonical_source_uid);

CREATE TABLE IF NOT EXISTS client_visits (
    visit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL,
    visited_at_utc TEXT NOT NULL,
    path TEXT NOT NULL,
    query_string TEXT
);

CREATE TABLE IF NOT EXISTS favorite_calendar_subscriptions (
    subscription_token TEXT PRIMARY KEY,
    client_id TEXT NOT NULL UNIQUE,
    favorite_ids_json TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);
"""
