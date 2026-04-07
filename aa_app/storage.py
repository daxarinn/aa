from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .config import DEFAULT_CALENDAR_EVENT_DURATION, ICAL_BYDAY_BY_ORDER, SCHEMA_SQL, SIZE_BIN_VALUES, WEEKDAY_ORDER
from .models import MeetingRecord
from .parsing import (
    clean_html_lines,
    normalize_space,
    pad_time,
    parse_clock_time,
    sanitize_rows_for_render,
    size_bin_from_average,
    truncate_text,
)

def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_schema(db_path: Path) -> None:
    ensure_parent_dir(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()


def write_snapshot(db_path: Path, records: list[MeetingRecord], scraped_at_utc: str) -> None:
    ensure_schema(db_path)
    rows = [record.to_row() for record in records]

    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM meetings")
        conn.executemany(
            """
            INSERT INTO meetings (
                source_uid, source, source_page_url, source_record_id, scraped_at_utc,
                weekday_is, weekday_order, start_time, end_time, time_display,
                meeting_name, subtitle, fellowship, format, location_text, venue_text,
                zoom_url, zoom_meeting_id, zoom_passcode, gender_restriction,
                access_restriction, recurrence_hint, notes, tags_json, raw_json
            ) VALUES (
                :source_uid, :source, :source_page_url, :source_record_id, :scraped_at_utc,
                :weekday_is, :weekday_order, :start_time, :end_time, :time_display,
                :meeting_name, :subtitle, :fellowship, :format, :location_text, :venue_text,
                :zoom_url, :zoom_meeting_id, :zoom_passcode, :gender_restriction,
                :access_restriction, :recurrence_hint, :notes, :tags_json, :raw_json
            )
            """,
            rows,
        )
        run_id = hashlib.sha1(scraped_at_utc.encode("utf-8")).hexdigest()
        conn.execute(
            "INSERT OR REPLACE INTO scrape_runs (run_id, scraped_at_utc, record_count) VALUES (?, ?, ?)",
            (run_id, scraped_at_utc, len(rows)),
        )
        conn.commit()


def load_dataframe(db_path: Path) -> pd.DataFrame:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            """
            WITH unified AS (
                SELECT
                    m.source_uid,
                    m.source,
                    m.weekday_is,
                    m.weekday_order,
                    m.start_time,
                    m.end_time,
                    m.time_display,
                    m.meeting_name,
                    m.subtitle,
                    m.fellowship,
                    m.format,
                    m.location_text,
                    m.venue_text,
                    m.zoom_meeting_id,
                    m.zoom_passcode,
                    m.zoom_url,
                    m.gender_restriction,
                    m.access_restriction,
                    m.recurrence_hint,
                    m.notes,
                    m.source_record_id,
                    m.source_page_url,
                    m.raw_json,
                    m.scraped_at_utc
                FROM meetings m
                UNION ALL
                SELECT
                    'manual:' || me.event_id AS source_uid,
                    'kirkja' AS source,
                    me.weekday_is,
                    me.weekday_order,
                    me.start_time,
                    me.end_time,
                    me.time_display,
                    me.title AS meeting_name,
                    me.subtitle,
                    'Kirkja' AS fellowship,
                    'Staðfundur' AS format,
                    me.location_text,
                    me.venue_text,
                    NULL AS zoom_meeting_id,
                    NULL AS zoom_passcode,
                    NULL AS zoom_url,
                    'Blandaður' AS gender_restriction,
                    'Opinn' AS access_restriction,
                    NULL AS recurrence_hint,
                    me.notes,
                    'manual:' || me.event_id AS source_record_id,
                    COALESCE(me.source_page_url, '') AS source_page_url,
                    json_object(
                        'event_kind', me.event_kind,
                        'title', me.title,
                        'subtitle', me.subtitle,
                        'location_text', me.location_text,
                        'venue_text', me.venue_text,
                        'notes', me.notes
                    ) AS raw_json,
                    me.updated_at_utc AS scraped_at_utc
                FROM manual_events me
            ),
            size_reports AS (
                SELECT
                    source_uid,
                    COUNT(*) AS size_report_count,
                    AVG(
                        CASE size_bin
                            WHEN '2-9' THEN 5.5
                            WHEN '10-19' THEN 14.5
                            WHEN '20-39' THEN 29.5
                            WHEN '40+' THEN 45.0
                            ELSE NULL
                        END
                    ) AS avg_size_value
                FROM meeting_size_reports
                GROUP BY source_uid
            )
            SELECT
                u.source_uid,
                u.source,
                u.weekday_is,
                u.weekday_order,
                u.start_time,
                u.end_time,
                u.time_display,
                u.meeting_name,
                u.subtitle,
                u.fellowship,
                u.format,
                u.location_text,
                COALESCE(la.canonical_location_text, u.location_text) AS canonical_location_text,
                lm.nickname AS location_nickname,
                CASE WHEN la.alias_location_text IS NULL THEN 0 ELSE 1 END AS has_location_mapping,
                u.venue_text,
                u.zoom_meeting_id,
                u.zoom_passcode,
                u.zoom_url,
                COALESCE(NULLIF(TRIM(u.gender_restriction), ''), 'Blandaður') AS gender_restriction,
                u.access_restriction,
                u.recurrence_hint,
                u.notes,
                u.source_record_id,
                u.source_page_url,
                u.raw_json,
                u.scraped_at_utc,
                sr.size_report_count,
                sr.avg_size_value
            FROM unified u
            LEFT JOIN location_aliases la
                ON la.alias_location_text = u.location_text
            LEFT JOIN location_metadata lm
                ON lm.canonical_location_text = COALESCE(la.canonical_location_text, u.location_text)
            LEFT JOIN size_reports sr
                ON sr.source_uid = u.source_uid
            ORDER BY weekday_order, start_time, time_display, source, meeting_name
            """,
            conn,
        )
    if not df.empty:
        locators: list[str] = []
        excerpts: list[str] = []
        for row in df.itertuples(index=False):
            locator, excerpt = build_source_context(
                source=str(row.source),
                source_record_id=getattr(row, "source_record_id", None),
                raw_json=getattr(row, "raw_json", None),
            )
            locators.append(locator)
            excerpts.append(excerpt)
        df["source_locator"] = locators
        df["source_excerpt"] = excerpts
        df["fellowship_display"] = df["fellowship"].fillna("").astype(str).str.strip()
        df.loc[df["fellowship_display"] == "", "fellowship_display"] = "Óskráð félag"
        df["avg_size_bin"] = df["avg_size_value"].apply(size_bin_from_average)
    return df


def export_csv(df: pd.DataFrame, csv_path: Path) -> None:
    ensure_parent_dir(csv_path)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def maybe_copy_to_clipboard(df: pd.DataFrame) -> None:
    df.to_clipboard(index=False, excel=True)

def summarize_dataframe(df: pd.DataFrame) -> str:
    source_counts = df.groupby("source").size().sort_index()
    weekday_counts = (
        df.groupby(["weekday_order", "weekday_is"])
        .size()
        .reset_index(name="count")
        .sort_values("weekday_order")
    )
    lines = [f"Fundir alls: {len(df)}", "Eftir source:"]
    lines.extend(f"  - {source}: {count}" for source, count in source_counts.items())
    lines.append("Eftir vikudegi:")
    lines.extend(f"  - {row.weekday_is}: {row.count}" for row in weekday_counts.itertuples())
    return "\n".join(lines)

def normalized_favorite_ids(values: object) -> list[str]:
    if not isinstance(values, list):
        return []
    seen: set[str] = set()
    favorite_ids: list[str] = []
    for value in values:
        clean_value = normalize_space(value)
        if not clean_value or clean_value in seen:
            continue
        seen.add(clean_value)
        favorite_ids.append(clean_value)
    return favorite_ids

def calendar_anchor_date_for_weekday(weekday_order: object | None) -> date | None:
    try:
        clean_weekday_order = int(weekday_order or 0)
    except (TypeError, ValueError):
        return None
    if clean_weekday_order not in range(1, 8):
        return None
    local_today = datetime.now(ZoneInfo("Atlantic/Reykjavik")).date()
    week_start = local_today - timedelta(days=local_today.isoweekday() - 1)
    return week_start + timedelta(days=clean_weekday_order - 1)


def parse_clock_time(value: object | None) -> tuple[int, int] | None:
    match = re.match(r"^(\d{1,2})[:.](\d{2})$", normalize_space(value))
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour not in range(0, 24) or minute not in range(0, 60):
        return None
    return hour, minute


def iso_to_ical_utc(value: object | None) -> str | None:
    clean_value = normalize_space(value)
    if not clean_value:
        return None
    try:
        parsed = datetime.fromisoformat(clean_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ical_escape(value: object | None) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    return (
        text.replace("\\", "\\\\")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "\\n")
        .replace(";", r"\;")
        .replace(",", r"\,")
    )


def fold_ical_line(line: str) -> str:
    if len(line) <= 75:
        return line
    chunks = [line[index:index + 73] for index in range(0, len(line), 73)]
    return "\r\n ".join(chunks)


def build_calendar_location(row: dict[str, object]) -> str:
    candidates = [
        row.get("location_nickname"),
        row.get("canonical_location_text"),
        row.get("location_text"),
        row.get("venue_text"),
    ]
    for candidate in candidates:
        clean_candidate = normalize_space(candidate)
        if clean_candidate:
            return clean_candidate
    if normalize_space(row.get("format")) == "Fjarfundur":
        return "Á netinu"
    return ""


def build_calendar_description(row: dict[str, object]) -> str:
    title = normalize_space(row.get("meeting_name_display") or row.get("meeting_name"))
    summary = normalize_space(row.get("summary_display"))
    lines: list[str] = []
    if summary and summary.casefold() != title.casefold():
        lines.append(summary)
    for label, value in [
        ("Undirtitill", row.get("subtitle")),
        ("Félag", row.get("fellowship_display") or row.get("fellowship")),
        ("Format", row.get("format")),
        ("Aðgangur", row.get("access_restriction")),
        ("Kyn", row.get("gender_restriction")),
        ("Glósur", row.get("notes")),
        ("Zoom ID", row.get("zoom_meeting_id")),
        ("Passcode", row.get("zoom_passcode")),
        ("Uppruni", row.get("source_page_url")),
    ]:
        clean_value = normalize_space(value)
        if clean_value:
            lines.append(f"{label}: {clean_value}")
    return "\n".join(lines)


def build_calendar_event_bounds(row: dict[str, object]) -> tuple[datetime, datetime] | None:
    weekday_date = calendar_anchor_date_for_weekday(row.get("weekday_order"))
    if weekday_date is None:
        return None
    start_parts = parse_clock_time(row.get("start_time")) or parse_clock_time(row.get("time_display"))
    if start_parts is None:
        return None
    start_dt = datetime(
        weekday_date.year,
        weekday_date.month,
        weekday_date.day,
        start_parts[0],
        start_parts[1],
        tzinfo=timezone.utc,
    )
    end_parts = parse_clock_time(row.get("end_time"))
    if end_parts is None:
        return start_dt, start_dt + DEFAULT_CALENDAR_EVENT_DURATION
    end_dt = datetime(
        weekday_date.year,
        weekday_date.month,
        weekday_date.day,
        end_parts[0],
        end_parts[1],
        tzinfo=timezone.utc,
    )
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


def build_favorites_calendar_ics(
    rows: list[dict[str, object]],
    *,
    calendar_name: str,
    calendar_url: str | None = None,
) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//AA Fundaskra//Favorites Calendar//IS",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ical_escape(calendar_name)}",
        "X-PUBLISHED-TTL:PT6H",
        "REFRESH-INTERVAL;VALUE=DURATION:PT6H",
    ]
    if calendar_url:
        lines.append(f"URL:{ical_escape(calendar_url)}")

    for row in rows:
        try:
            weekday_code = ICAL_BYDAY_BY_ORDER.get(int(row.get("weekday_order") or 0))
        except (TypeError, ValueError):
            weekday_code = None
        event_bounds = build_calendar_event_bounds(row)
        source_uid = normalize_space(row.get("source_uid"))
        if not weekday_code or event_bounds is None or not source_uid:
            continue
        start_dt, end_dt = event_bounds
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{ical_escape(source_uid)}@aa-fundaskra",
                f"DTSTAMP:{timestamp}",
                f"LAST-MODIFIED:{iso_to_ical_utc(row.get('scraped_at_utc')) or timestamp}",
                f"SUMMARY:{ical_escape(row.get('meeting_name_display') or row.get('meeting_name') or 'AA fundur')}",
                f"DTSTART:{start_dt.strftime('%Y%m%dT%H%M%SZ')}",
                f"DTEND:{end_dt.strftime('%Y%m%dT%H%M%SZ')}",
                f"RRULE:FREQ=WEEKLY;BYDAY={weekday_code}",
                "STATUS:CONFIRMED",
                "TRANSP:OPAQUE",
            ]
        )
        location = build_calendar_location(row)
        if location:
            lines.append(f"LOCATION:{ical_escape(location)}")
        description = build_calendar_description(row)
        if description:
            lines.append(f"DESCRIPTION:{ical_escape(description)}")
        source_url = normalize_space(row.get("source_page_url"))
        if source_url and re.match(r"^https?://", source_url, re.I):
            lines.append(f"URL:{ical_escape(source_url)}")
        lines.append("END:VEVENT")

    lines.append("END:VCALENDAR")
    return "\r\n".join(fold_ical_line(line) for line in lines) + "\r\n"

def load_favorite_calendar_rows(db_path: Path, favorite_ids: list[str]) -> list[dict[str, object]]:
    clean_favorite_ids = normalized_favorite_ids(favorite_ids)
    if not clean_favorite_ids:
        return []
    df = load_dataframe(db_path)
    if df.empty:
        return []
    filtered = df[df["source_uid"].fillna("").astype(str).isin(set(clean_favorite_ids))].copy()
    if filtered.empty:
        return []
    filtered = filtered.sort_values(
        by=["weekday_order", "start_time", "time_display", "meeting_name", "source_uid"],
        na_position="last",
    )
    return sanitize_rows_for_render(filtered.to_dict(orient="records"))


def upsert_favorite_calendar_subscription(db_path: Path, client_id: str, favorite_ids: list[str]) -> str:
    ensure_schema(db_path)
    clean_client_id = normalize_space(client_id)
    if not clean_client_id:
        raise ValueError("client_id vantar")
    favorite_ids_json = json.dumps(normalized_favorite_ids(favorite_ids), ensure_ascii=False, separators=(",", ":"))
    updated_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with sqlite3.connect(db_path) as conn:
        existing_row = conn.execute(
            "SELECT subscription_token FROM favorite_calendar_subscriptions WHERE client_id = ?",
            (clean_client_id,),
        ).fetchone()
        if existing_row and existing_row[0]:
            token = str(existing_row[0])
            conn.execute(
                """
                UPDATE favorite_calendar_subscriptions
                SET favorite_ids_json = ?, updated_at_utc = ?
                WHERE client_id = ?
                """,
                (favorite_ids_json, updated_at_utc, clean_client_id),
            )
            conn.commit()
            return token

        for _attempt in range(5):
            token = secrets.token_urlsafe(24)
            try:
                conn.execute(
                    """
                    INSERT INTO favorite_calendar_subscriptions (
                        subscription_token, client_id, favorite_ids_json, updated_at_utc
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (token, clean_client_id, favorite_ids_json, updated_at_utc),
                )
                conn.commit()
                return token
            except sqlite3.IntegrityError:
                continue
    raise RuntimeError("Tókst ekki að búa til calendar token")


def load_favorite_calendar_subscription(db_path: Path, subscription_token: str) -> dict[str, object] | None:
    ensure_schema(db_path)
    clean_token = normalize_space(subscription_token)
    if not clean_token:
        return None
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT client_id, favorite_ids_json, updated_at_utc
            FROM favorite_calendar_subscriptions
            WHERE subscription_token = ?
            """,
            (clean_token,),
        ).fetchone()
    if not row:
        return None
    try:
        favorite_ids_payload = json.loads(row[1])
    except (TypeError, json.JSONDecodeError):
        favorite_ids_payload = []
    return {
        "client_id": normalize_space(row[0]),
        "favorite_ids": normalized_favorite_ids(favorite_ids_payload),
        "updated_at_utc": normalize_space(row[2]),
    }

def normalize_location_token(token: str) -> str:
    token = token.casefold()
    token = token.replace("br.", "braut")
    token = token.replace("veg.", "vegur")
    token = token.replace("st.", "stígur")
    token = token.replace(" ", "")
    token = re.sub(r"[^0-9a-záðéíóúýþæö]", "", token)
    replacements = {
        "götu": "gata",
        "gotu": "gata",
        "stíg": "stígur",
        "stig": "stigur",
        "vegi": "vegur",
        "veg": "vegur",
        "brautar": "braut",
    }
    for old, new in replacements.items():
        if token.endswith(old):
            token = token[: -len(old)] + new
            break
    return token


def normalized_location_key(location_text: str) -> str:
    parts = [normalize_location_token(part) for part in normalize_space(location_text).split()]
    parts = [part for part in parts if part]
    return " ".join(parts)


def save_location_mapping(db_path: Path, alias_location_text: str, canonical_location_text: str) -> None:
    ensure_schema(db_path)
    alias = normalize_space(alias_location_text)
    canonical = normalize_space(canonical_location_text)
    with sqlite3.connect(db_path) as conn:
        if not canonical or canonical == alias:
            conn.execute("DELETE FROM location_aliases WHERE alias_location_text = ?", (alias,))
        else:
            conn.execute(
                """
                INSERT INTO location_aliases (alias_location_text, canonical_location_text, updated_at_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(alias_location_text) DO UPDATE SET
                    canonical_location_text = excluded.canonical_location_text,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (alias, canonical, datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
            )
        conn.commit()


def save_location_nickname(db_path: Path, canonical_location_text: str, nickname: str) -> None:
    ensure_schema(db_path)
    canonical = normalize_space(canonical_location_text)
    nickname_value = normalize_space(nickname)
    with sqlite3.connect(db_path) as conn:
        if not canonical:
            return
        if not nickname_value:
            conn.execute("DELETE FROM location_metadata WHERE canonical_location_text = ?", (canonical,))
        else:
            conn.execute(
                """
                INSERT INTO location_metadata (canonical_location_text, nickname, updated_at_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(canonical_location_text) DO UPDATE SET
                    nickname = excluded.nickname,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (canonical, nickname_value, datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
            )
        conn.commit()


def save_manual_event(
    db_path: Path,
    *,
    event_id: str | None,
    event_kind: str,
    title: str,
    weekday_is: str,
    start_time: str,
    end_time: str,
    subtitle: str,
    location_text: str,
    venue_text: str,
    notes: str,
    source_page_url: str,
) -> None:
    ensure_schema(db_path)
    clean_kind = normalize_space(event_kind) or "church"
    clean_title = normalize_space(title)
    clean_weekday = normalize_space(weekday_is)
    if not clean_title or clean_weekday not in WEEKDAY_ORDER:
        return

    padded_start = pad_time(start_time)
    padded_end = pad_time(end_time)
    time_display = padded_start or "Ótímasett"
    if padded_start and padded_end:
        time_display = f"{padded_start}-{padded_end}"

    payload = (
        clean_kind,
        clean_title,
        clean_weekday,
        WEEKDAY_ORDER[clean_weekday],
        padded_start,
        padded_end,
        time_display,
        normalize_space(subtitle) or None,
        normalize_space(location_text) or None,
        normalize_space(venue_text) or None,
        normalize_space(notes) or None,
        normalize_space(source_page_url) or None,
        datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    )

    with sqlite3.connect(db_path) as conn:
        if event_id and str(event_id).strip().isdigit():
            conn.execute(
                """
                UPDATE manual_events
                SET event_kind = ?, title = ?, weekday_is = ?, weekday_order = ?, start_time = ?, end_time = ?,
                    time_display = ?, subtitle = ?, location_text = ?, venue_text = ?, notes = ?, source_page_url = ?, updated_at_utc = ?
                WHERE event_id = ?
                """,
                payload + (int(event_id),),
            )
        else:
            conn.execute(
                """
                INSERT INTO manual_events (
                    event_kind, title, weekday_is, weekday_order, start_time, end_time, time_display,
                    subtitle, location_text, venue_text, notes, source_page_url, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
        conn.commit()


def save_meeting_size_report(db_path: Path, source_uid: str, client_id: str, size_bin: str) -> None:
    ensure_schema(db_path)
    clean_source_uid = normalize_space(source_uid)
    clean_client_id = normalize_space(client_id)
    clean_size_bin = normalize_space(size_bin)
    if not clean_source_uid or not clean_client_id or clean_size_bin not in SIZE_BIN_VALUES:
        return
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO meeting_size_reports (source_uid, client_id, size_bin, reported_at_utc)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_uid, client_id) DO UPDATE SET
                size_bin = excluded.size_bin,
                reported_at_utc = excluded.reported_at_utc
            """,
            (
                clean_source_uid,
                clean_client_id,
                clean_size_bin,
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            ),
        )
        conn.commit()


def delete_manual_event(db_path: Path, event_id: str | None) -> None:
    ensure_schema(db_path)
    if not event_id or not str(event_id).strip().isdigit():
        return
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM manual_events WHERE event_id = ?", (int(event_id),))
        conn.commit()


def load_manual_events(db_path: Path) -> list[dict[str, object]]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                event_id,
                event_kind,
                title,
                weekday_is,
                start_time,
                end_time,
                time_display,
                subtitle,
                location_text,
                venue_text,
                notes,
                source_page_url,
                updated_at_utc
            FROM manual_events
            ORDER BY weekday_order, start_time, time_display, title
            """
        ).fetchall()
    return [dict(row) for row in rows]


def load_user_size_reports(db_path: Path, client_id: str | None) -> dict[str, str]:
    ensure_schema(db_path)
    clean_client_id = normalize_space(client_id)
    if not clean_client_id:
        return {}
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT source_uid, size_bin FROM meeting_size_reports WHERE client_id = ?",
            (clean_client_id,),
        ).fetchall()
    return {str(source_uid): str(size_bin) for source_uid, size_bin in rows if source_uid and size_bin}


def log_client_visit(db_path: Path, client_id: str, path: str, query_string: str) -> None:
    ensure_schema(db_path)
    clean_client_id = normalize_space(client_id)
    if not clean_client_id:
        return
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO client_visits (client_id, visited_at_utc, path, query_string)
            VALUES (?, ?, ?, ?)
            """,
            (
                clean_client_id,
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                normalize_space(path) or "/",
                normalize_space(query_string) or None,
            ),
        )
        conn.commit()


def load_visit_summary(db_path: Path) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, int]]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        summary_rows = conn.execute(
            """
            SELECT
                client_id,
                COUNT(*) AS visit_count,
                MIN(visited_at_utc) AS first_seen_utc,
                MAX(visited_at_utc) AS last_seen_utc
            FROM client_visits
            GROUP BY client_id
            ORDER BY last_seen_utc DESC, visit_count DESC
            LIMIT 200
            """
        ).fetchall()
        recent_rows = conn.execute(
            """
            SELECT client_id, visited_at_utc, path, query_string
            FROM client_visits
            ORDER BY visited_at_utc DESC, visit_id DESC
            LIMIT 200
            """
        ).fetchall()
        totals_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_visits,
                COUNT(DISTINCT client_id) AS unique_clients
            FROM client_visits
            """
        ).fetchone()

    summary = [dict(row) for row in summary_rows]
    recent = [dict(row) for row in recent_rows]
    totals = dict(totals_row or {"total_visits": 0, "unique_clients": 0})
    return summary, recent, totals


def build_location_review_rows(df: pd.DataFrame, query: str) -> list[dict[str, object]]:
    if df.empty:
        return []

    working = df.copy()
    if query:
        mask = (
            working["location_text"].fillna("").str.contains(query, case=False, regex=False)
            | working["canonical_location_text"].fillna("").str.contains(query, case=False, regex=False)
            | working["venue_text"].fillna("").str.contains(query, case=False, regex=False)
        )
        working = working[mask]

    rows: list[dict[str, object]] = []
    grouped = (
        working.groupby(
            ["location_text", "canonical_location_text", "location_nickname", "has_location_mapping"],
            dropna=False,
        )
        .agg(
            meeting_count=("location_text", "size"),
            venues=("venue_text", lambda s: sorted({value for value in s.dropna() if value})[:3]),
            names=("meeting_name", lambda s: sorted({value for value in s.dropna() if value})[:3]),
            sources=("source", lambda s: sorted({value for value in s.dropna() if value})),
        )
        .reset_index()
    )

    for item in grouped.to_dict(orient="records"):
        item["normalized_key"] = normalized_location_key(str(item["location_text"]))
        item["suggested_canonical"] = str(item["canonical_location_text"] or item["location_text"])
        item["location_nickname"] = "" if pd.isna(item["location_nickname"]) else str(item["location_nickname"])
        rows.append(item)

    rows.sort(key=lambda item: (-int(item["meeting_count"]), str(item["location_text"])))
    return rows


def build_location_clusters(location_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    clusters: dict[str, list[dict[str, object]]] = {}
    for row in location_rows:
        key = str(row["normalized_key"])
        clusters.setdefault(key, []).append(row)

    items: list[dict[str, object]] = []
    for key, rows in clusters.items():
        if len(rows) < 2:
            continue
        canonical_candidates = sorted(
            {str(row["canonical_location_text"] or row["location_text"]) for row in rows if row["location_text"]},
            key=lambda value: (len(value), value),
        )
        items.append(
            {
                "normalized_key": key,
                "rows": sorted(rows, key=lambda row: (-int(row["meeting_count"]), str(row["location_text"]))),
                "suggested_canonical": canonical_candidates[0] if canonical_candidates else "",
            }
        )

    items.sort(key=lambda item: (-sum(int(row["meeting_count"]) for row in item["rows"]), item["normalized_key"]))
    return items


def build_source_context(source: str, source_record_id: str | None, raw_json: str | None) -> tuple[str, str]:
    locator = normalize_space(source_record_id) or ""
    excerpt_parts: list[str] = []

    try:
        payload = json.loads(raw_json) if raw_json else {}
    except json.JSONDecodeError:
        payload = {}

    if source == "aa.is":
        location_text = payload.get("location_text")
        place_text = payload.get("place_text")
        description_lines = payload.get("description_lines") or []
        excerpt_parts.extend(
            [
                truncate_text(location_text, 90) or "",
                truncate_text(place_text, 110) or "",
                truncate_text(" | ".join(description_lines[:2]), 140) or "",
            ]
        )
    elif source == "fjarfundir.org":
        description_fragment = payload.get("description_fragment", "")
        zoom_fragment = payload.get("zoom_fragment", "")
        description_lines = clean_html_lines(description_fragment)
        zoom_lines = clean_html_lines(zoom_fragment)
        excerpt_parts.extend(
            [
                truncate_text(" | ".join(description_lines[:3]), 150) or "",
                truncate_text(" | ".join(zoom_lines[:2]), 110) or "",
            ]
        )
    elif source == "gula.is":
        excerpt_parts.extend(
            [
                truncate_text(payload.get("fellowship"), 40) or "",
                truncate_text(payload.get("meeting_name"), 90) or "",
                truncate_text(payload.get("room"), 24) or "",
                truncate_text(payload.get("recurrence_hint"), 60) or "",
            ]
        )
    elif source == "coda.is":
        description_lines = payload.get("description_lines") or []
        excerpt_parts.extend(
            [
                truncate_text(" | ".join(description_lines[:2]), 150) or "",
                truncate_text(payload.get("location_tail"), 90) or "",
            ]
        )
    elif source == "al-anon.is":
        rows = payload.get("rows") or []
        excerpt_parts.extend(
            [
                truncate_text(" | ".join(normalize_space(item.get("text")) for item in rows[:3]), 170) or "",
            ]
        )
    elif source == "12sporahusid.is":
        excerpt_parts.extend(
            [
                truncate_text(payload.get("fellowship"), 40) or "",
                truncate_text(payload.get("meeting_name"), 90) or "",
                truncate_text(payload.get("room"), 16) or "",
            ]
        )

    excerpt = " | ".join([part for part in excerpt_parts if part])
    return locator, excerpt
