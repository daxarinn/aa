from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import json
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .config import (
    AA_ALL_MEETINGS_ALL_URL,
    AA_ALL_MEETINGS_URL,
    AA_DAY_PAGES,
    ALANON_MEETINGS_URL,
    CODA_MEETINGS_URL,
    FJAR_DAY_PAGES,
    GULA_ALL_MEETINGS_URL,
    TWELVE_STEP_HOUSE_MEETINGS_URL,
    WEEKDAY_ORDER,
)
from .models import MeetingRecord
from .parsing import (
    classify_format,
    clean_html_lines,
    extract_first_link,
    extract_urls,
    extract_zoom_details,
    infer_restrictions_from_texts,
    looks_like_address,
    make_session,
    normalize_space,
    normalize_zoom_meeting_id,
    normalize_zoom_url,
    pad_time,
    parse_alanon_headline,
    parse_restrictions,
    parse_time_range,
    split_gender_suffix,
    split_recurrence_hint,
    strip_zoom_meta,
)
from .storage import export_csv, load_dataframe, maybe_copy_to_clipboard, write_snapshot

def source_priority(record: MeetingRecord) -> int:
    priorities = {
        "al-anon.is": 5,
        "coda.is": 4,
        "fjarfundir.org": 3,
        "12sporahusid.is": 3,
        "gula.is": 2,
        "aa.is": 1,
    }
    return priorities.get(record.source, 0)


def source_merge_score(record: MeetingRecord) -> tuple[int, int]:
    populated_fields = sum(
        1
        for value in [
            record.meeting_name,
            record.subtitle,
            record.location_text,
            record.venue_text,
            record.zoom_url,
            record.zoom_meeting_id,
            record.zoom_passcode,
            record.notes,
            record.recurrence_hint,
        ]
        if normalize_space(value)
    )
    return (source_priority(record), populated_fields)


def record_overlap_keys(record: MeetingRecord) -> set[tuple[str, ...]]:
    keys = set(remote_overlap_keys(record))
    day_key = normalize_space(record.weekday_is)
    time_key = pad_time(record.start_time or record.time_display) or normalize_space(record.time_display)
    fellowship_key = normalize_space(record.fellowship).casefold()
    if not day_key or not time_key or not fellowship_key:
        return keys

    location_key = normalized_location_key(normalize_space(record.location_text))
    venue_key = normalized_location_key(normalize_space(record.venue_text))
    meeting_key = normalize_space(record.meeting_name).casefold()

    if location_key and venue_key:
        keys.add(("venue", day_key, time_key, fellowship_key, location_key, venue_key))
    if meeting_key and location_key:
        keys.add(("meeting_location", day_key, time_key, fellowship_key, meeting_key, location_key))
    if meeting_key and venue_key:
        keys.add(("meeting_venue", day_key, time_key, fellowship_key, meeting_key, venue_key))
    return keys


def extract_aa_description(tag) -> tuple[str | None, str | None, str | None, str | None, list[str]]:
    lines = [normalize_space(line) for line in tag.get_text("\n").splitlines()]
    lines = [line for line in lines if line]

    meeting_name = lines[0] if lines else None
    subtitle = None
    recurrence_parts: list[str] = []

    span_tags = [normalize_space(span.get_text(" ", strip=True)) for span in tag.find_all("span")]
    gender, access, tags = parse_restrictions(span_tags + lines[1:])

    for line in lines[1:]:
        lowered = line.casefold()
        if line in span_tags:
            continue
        if any(token in lowered for token in ["karlar", "konur", "blandað", "blandaður", "opinn", "lokaður", "lokað"]):
            continue
        if "alla daga" in lowered or "virka daga" in lowered or "mán" in lowered or "þri" in lowered:
            recurrence_parts.append(line)
        elif not subtitle:
            subtitle = line

    deduped_tags = sorted({tag for tag in tags if tag})
    if recurrence_parts:
        deduped_tags.append(" | ".join(recurrence_parts))
    return meeting_name, subtitle, gender, access, deduped_tags


def extract_fjar_description(lines: list[str]) -> tuple[str | None, str | None, str | None, str | None, str | None, str | None, list[str]]:
    meeting_name = lines[0] if lines else None
    subtitle = None
    recurrence_parts: list[str] = []
    note_parts: list[str] = []

    gender, access, tags = parse_restrictions(lines[1:])

    for line in lines[1:]:
        lowered = line.casefold()
        if any(token in lowered for token in ["karlar", "konur", "blandað", "blandaður", "opinn", "lokaður", "lokað"]):
            continue
        if line.startswith("▸") or "alla daga" in lowered or "virka" in lowered or "laugard" in lowered or "sunnud" in lowered:
            recurrence_parts.append(line)
        elif "kaffispjall" in lowered or "opnar" in lowered or "anonymous" in lowered:
            note_parts.append(line)
        elif not subtitle:
            subtitle = line
        else:
            note_parts.append(line)

    recurrence_hint = " | ".join(recurrence_parts) if recurrence_parts else None
    notes = " | ".join(note_parts) if note_parts else None
    deduped_tags = sorted({tag for tag in tags if tag})
    return meeting_name, subtitle, gender, access, recurrence_hint, notes, deduped_tags


def extract_fjar_dynamic_times(page_html: str, scraped_at_utc: str) -> dict[str, str]:
    dynamic_times: dict[str, str] = {}
    try:
        reference_utc = datetime.fromisoformat(scraped_at_utc)
    except ValueError:
        reference_utc = datetime.now(timezone.utc)

    pattern = re.compile(
        r'getElementById\("(?P<id>meeting-time-[^"]+)"\).*?'
        r'toLocaleString\("en-US",\s*\{\s*timeZone:\s*"(?P<source_tz>[^"]+)"\s*\}\).*?'
        r'setHours\((?P<hour>\d+),\s*(?P<minute>\d+),\s*0,\s*0\)',
        re.S,
    )

    for match in pattern.finditer(page_html):
        div_id = match.group("id")
        source_tz = match.group("source_tz")
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        try:
            source_now = reference_utc.astimezone(ZoneInfo(source_tz))
            meeting_source = source_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            meeting_iceland = meeting_source.astimezone(ZoneInfo("Atlantic/Reykjavik"))
            dynamic_times[div_id] = meeting_iceland.strftime("%H:%M")
        except Exception:
            continue

    return dynamic_times


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def parse_aa_row(
    cells,
    scraped_at_utc: str,
    source_page_url: str,
    source_record_id: str,
    weekday_is: str,
) -> MeetingRecord:
    time_display = normalize_space(cells[0].get_text(" ", strip=True))
    start_time, end_time = parse_time_range(time_display)
    location_text = normalize_space(cells[1].get_text(" ", strip=True))
    place_text = normalize_space(cells[2].get_text("\n", strip=True))
    description_tag = cells[3]

    meeting_name, subtitle, gender, access, tags = extract_aa_description(description_tag)
    venue_text = strip_zoom_meta(place_text) or None
    inline_urls = extract_urls(place_text)
    zoom_url = inline_urls[0] if inline_urls else None
    zoom_meeting_id, zoom_passcode = extract_zoom_details(place_text)
    meeting_format = classify_format(location_text, place_text, zoom_url)

    raw_payload = {
        "time_display": time_display,
        "location_text": location_text,
        "place_text": place_text,
        "description_lines": clean_html_lines(str(description_tag)),
        "source_record_id": source_record_id,
    }

    return MeetingRecord(
        source="aa.is",
        source_page_url=source_page_url,
        source_record_id=source_record_id,
        scraped_at_utc=scraped_at_utc,
        weekday_is=weekday_is,
        weekday_order=WEEKDAY_ORDER[weekday_is],
        start_time=start_time,
        end_time=end_time,
        time_display=time_display,
        meeting_name=meeting_name,
        subtitle=subtitle,
        fellowship="AA",
        format=meeting_format,
        location_text=location_text,
        venue_text=venue_text,
        zoom_url=zoom_url,
        zoom_meeting_id=zoom_meeting_id,
        zoom_passcode=zoom_passcode,
        gender_restriction=gender,
        access_restriction=access,
        recurrence_hint=None,
        notes=None,
        tags_json=json.dumps(tags, ensure_ascii=False),
        raw_json=json.dumps(raw_payload, ensure_ascii=False),
    )


def scrape_aa_all_meetings(session: requests.Session, scraped_at_utc: str) -> list[MeetingRecord]:
    soup = BeautifulSoup(fetch_html(session, AA_ALL_MEETINGS_ALL_URL), "html.parser")
    tbody = soup.find("tbody")
    if not tbody:
        raise RuntimeError("Engin tafla fannst á aa.is/allir-fundir?limit=0")

    records: list[MeetingRecord] = []

    for index, row in enumerate(tbody.find_all("tr"), start=1):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        weekday_is = normalize_space(cells[4].get_text(" ", strip=True))
        if weekday_is not in WEEKDAY_ORDER:
            continue
        records.append(
            parse_aa_row(
                cells=cells,
                scraped_at_utc=scraped_at_utc,
                source_page_url=AA_ALL_MEETINGS_ALL_URL,
                source_record_id=f"allir-fundir:{index}",
                weekday_is=weekday_is,
            )
        )

    if not records:
        raise RuntimeError("Engin gögn fundust á aa.is/allir-fundir")

    return records


def scrape_aa_day_pages(session: requests.Session, scraped_at_utc: str) -> list[MeetingRecord]:
    records: list[MeetingRecord] = []

    for weekday_is, url in AA_DAY_PAGES:
        html_text = fetch_html(session, url)
        soup = BeautifulSoup(html_text, "html.parser")
        tbody = soup.find("tbody")
        if not tbody:
            raise RuntimeError(f"Fant ekki fundartöflu á {url}")

        for index, row in enumerate(tbody.find_all("tr"), start=1):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            records.append(
                parse_aa_row(
                    cells=cells,
                    scraped_at_utc=scraped_at_utc,
                    source_page_url=url,
                    source_record_id=f"{weekday_is}-{index}",
                    weekday_is=weekday_is,
                )
            )

    return records


def dedupe_records(records: list[MeetingRecord]) -> list[MeetingRecord]:
    deduped: dict[str, MeetingRecord] = {}
    for record in records:
        deduped[record.source_uid] = record
    return list(deduped.values())


def remote_overlap_keys(record: MeetingRecord) -> set[tuple[str, ...]]:
    if record.format != "Fjarfundur":
        return set()

    keys: set[tuple[str, ...]] = set()
    day_key = record.weekday_is or ""
    time_key = record.start_time or record.time_display or ""

    meeting_id = normalize_zoom_meeting_id(record.zoom_meeting_id)
    if meeting_id:
        keys.add(("zoom_id", day_key, time_key, meeting_id))

    zoom_url = normalize_zoom_url(record.zoom_url)
    if zoom_url:
        keys.add(("zoom_url", day_key, time_key, zoom_url))

    return keys


def aa_remote_dedupe_key(record: MeetingRecord) -> tuple[str, ...] | None:
    if record.source != "aa.is" or record.format != "Fjarfundur":
        return None

    day_key = record.weekday_is or ""
    time_key = record.start_time or record.time_display or ""
    meeting_id = normalize_zoom_meeting_id(record.zoom_meeting_id)
    if meeting_id:
        return ("zoom_id", day_key, time_key, meeting_id)

    zoom_url = normalize_zoom_url(record.zoom_url)
    if zoom_url:
        return ("zoom_url", day_key, time_key, zoom_url)

    return None


def record_preference_score(record: MeetingRecord) -> tuple[int, int, int, int]:
    location_is_zoom = 1 if normalize_space(record.location_text).casefold() == "zoom" else 0
    populated_fields = sum(
        1
        for value in [
            record.meeting_name,
            record.subtitle,
            record.zoom_meeting_id,
            record.zoom_passcode,
            record.zoom_url,
            record.notes,
            record.recurrence_hint,
            record.venue_text,
        ]
        if normalize_space(value)
    )
    has_zoom_id = 1 if normalize_zoom_meeting_id(record.zoom_meeting_id) else 0
    has_zoom_url = 1 if normalize_zoom_url(record.zoom_url) else 0
    return (location_is_zoom, has_zoom_id, has_zoom_url, populated_fields)


def merge_meeting_records(preferred: MeetingRecord, other: MeetingRecord) -> MeetingRecord:
    def choose(primary: str | None, secondary: str | None) -> str | None:
        return primary if normalize_space(primary) else secondary

    return MeetingRecord(
        source=preferred.source,
        source_page_url=choose(preferred.source_page_url, other.source_page_url) or preferred.source_page_url,
        source_record_id=choose(preferred.source_record_id, other.source_record_id),
        scraped_at_utc=preferred.scraped_at_utc,
        weekday_is=preferred.weekday_is,
        weekday_order=preferred.weekday_order,
        start_time=choose(preferred.start_time, other.start_time),
        end_time=choose(preferred.end_time, other.end_time),
        time_display=choose(preferred.time_display, other.time_display) or preferred.time_display,
        meeting_name=choose(preferred.meeting_name, other.meeting_name),
        subtitle=choose(preferred.subtitle, other.subtitle),
        fellowship=choose(preferred.fellowship, other.fellowship),
        format=choose(preferred.format, other.format),
        location_text=choose(preferred.location_text, other.location_text),
        venue_text=choose(preferred.venue_text, other.venue_text),
        zoom_url=choose(preferred.zoom_url, other.zoom_url),
        zoom_meeting_id=choose(preferred.zoom_meeting_id, other.zoom_meeting_id),
        zoom_passcode=choose(preferred.zoom_passcode, other.zoom_passcode),
        gender_restriction=choose(preferred.gender_restriction, other.gender_restriction),
        access_restriction=choose(preferred.access_restriction, other.access_restriction),
        recurrence_hint=choose(preferred.recurrence_hint, other.recurrence_hint),
        notes=choose(preferred.notes, other.notes),
        tags_json=choose(preferred.tags_json, other.tags_json) or preferred.tags_json,
        raw_json=choose(preferred.raw_json, other.raw_json) or preferred.raw_json,
    )


def dedupe_aa_remote_variants(records: list[MeetingRecord]) -> list[MeetingRecord]:
    deduped: dict[tuple[str, ...], MeetingRecord] = {}
    output: list[MeetingRecord] = []

    for record in records:
        key = aa_remote_dedupe_key(record)
        if key is None:
            output.append(record)
            continue

        existing = deduped.get(key)
        if existing is None:
            deduped[key] = record
            continue

        preferred, secondary = (
            (record, existing)
            if record_preference_score(record) > record_preference_score(existing)
            else (existing, record)
        )
        deduped[key] = merge_meeting_records(preferred, secondary)

    output.extend(deduped.values())
    return output


def merge_remote_meetings(
    aa_records: list[MeetingRecord],
    fjar_records: list[MeetingRecord],
) -> list[MeetingRecord]:
    preferred_remote_keys: set[tuple[str, ...]] = set()
    for record in fjar_records:
        preferred_remote_keys.update(remote_overlap_keys(record))

    merged = list(fjar_records)
    dropped = 0
    for record in aa_records:
        keys = remote_overlap_keys(record)
        if record.source == "aa.is" and keys and keys & preferred_remote_keys:
            dropped += 1
            continue
        merged.append(record)

    return merged


def dedupe_preferred_source_records(records: list[MeetingRecord]) -> list[MeetingRecord]:
    kept: list[MeetingRecord] = []
    key_to_index: dict[tuple[str, ...], int] = {}

    for record in sorted(records, key=source_merge_score, reverse=True):
        keys = record_overlap_keys(record)
        existing_indexes = {key_to_index[key] for key in keys if key in key_to_index}
        if not existing_indexes:
            kept.append(record)
            record_index = len(kept) - 1
        else:
            record_index = min(existing_indexes)
            kept[record_index] = merge_meeting_records(kept[record_index], record)

        for key in record_overlap_keys(kept[record_index]):
            key_to_index[key] = record_index

    return dedupe_records(kept)


def scrape_aa(session: requests.Session, scraped_at_utc: str) -> list[MeetingRecord]:
    try:
        return dedupe_records(scrape_aa_all_meetings(session, scraped_at_utc))
    except Exception:
        return dedupe_records(scrape_aa_day_pages(session, scraped_at_utc))


def extract_fjar_ajax_url(page_html: str) -> str:
    anchor = "ninja_table_instance_0'] = "
    start = page_html.find(anchor)
    if start == -1:
        raise RuntimeError("NinjaTables config fannst ekki á fjarfundir.org")

    start += len(anchor)
    brace_depth = 0
    end = None

    for index, char in enumerate(page_html[start:], start):
        if char == "{":
            brace_depth += 1
        elif char == "}":
            brace_depth -= 1
            if brace_depth == 0:
                end = index + 1
                break

    if end is None:
        raise RuntimeError("Gat ekki lesið NinjaTables config á fjarfundir.org")

    config = json.loads(page_html[start:end])
    return config["init_config"]["data_request_url"]


def scrape_fjarfundir(session: requests.Session, scraped_at_utc: str) -> list[MeetingRecord]:
    records: list[MeetingRecord] = []

    for weekday_is, url in FJAR_DAY_PAGES:
        page_html = fetch_html(session, url)
        dynamic_times = extract_fjar_dynamic_times(page_html, scraped_at_utc)
        ajax_url = extract_fjar_ajax_url(page_html)
        ajax_response = session.get(ajax_url, timeout=30)
        ajax_response.raise_for_status()
        rows = ajax_response.json()

        for row in rows:
            values = row.get("value", {})
            source_record_id = str(values.get("___id___") or "")
            keys = [key for key in values.keys() if key != "___id___"]
            if len(keys) < 4:
                continue

            time_fragment = values[keys[0]]
            fellowship_fragment = values[keys[1]]
            description_fragment = values[keys[2]]
            zoom_fragment = values[keys[3]]

            time_display = normalize_space(" ".join(clean_html_lines(time_fragment)))
            fellowship_lines = clean_html_lines(fellowship_fragment)
            fellowship = fellowship_lines[0] if fellowship_lines else None

            description_lines = clean_html_lines(description_fragment)
            meeting_name, subtitle, gender, access, recurrence_hint, notes, tags = extract_fjar_description(description_lines)
            if not time_display:
                div_match = re.search(r'id="([^"]+)"', time_fragment or "")
                if div_match:
                    time_display = dynamic_times.get(div_match.group(1), "")
            if not time_display and subtitle:
                time_display = subtitle.strip()
            start_time, end_time = parse_time_range(time_display or subtitle or "")

            zoom_lines = clean_html_lines(zoom_fragment)
            zoom_text = " ".join(zoom_lines)
            zoom_url = extract_first_link(zoom_fragment)
            zoom_meeting_id, zoom_passcode = extract_zoom_details(zoom_text)

            raw_payload = {
                "time_fragment": time_fragment,
                "fellowship_fragment": fellowship_fragment,
                "description_fragment": description_fragment,
                "zoom_fragment": zoom_fragment,
                "source_record_id": source_record_id,
            }

            records.append(
                MeetingRecord(
                    source="fjarfundir.org",
                    source_page_url=url,
                    source_record_id=source_record_id or None,
                    scraped_at_utc=scraped_at_utc,
                    weekday_is=weekday_is,
                    weekday_order=WEEKDAY_ORDER[weekday_is],
                    start_time=start_time,
                    end_time=end_time,
                    time_display=time_display,
                    meeting_name=meeting_name,
                    subtitle=subtitle,
                    fellowship=fellowship,
                    format="Fjarfundur",
                    location_text="Zoom",
                    venue_text=None,
                    zoom_url=zoom_url,
                    zoom_meeting_id=zoom_meeting_id,
                    zoom_passcode=zoom_passcode,
                    gender_restriction=gender,
                    access_restriction=access,
                    recurrence_hint=recurrence_hint,
                    notes=notes,
                    tags_json=json.dumps(tags, ensure_ascii=False),
                    raw_json=json.dumps(raw_payload, ensure_ascii=False),
                )
            )

    return records


def scrape_gula(session: requests.Session, scraped_at_utc: str) -> list[MeetingRecord]:
    soup = BeautifulSoup(fetch_html(session, GULA_ALL_MEETINGS_URL), "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Engin tafla fannst á gula.is")

    records: list[MeetingRecord] = []
    for index, row in enumerate(table.find_all("tr")[1:], start=1):
        cells = [normalize_space(value) for value in row.stripped_strings]
        if len(cells) < 5:
            continue

        fellowship = cells[0]
        meeting_name, recurrence_hint = split_recurrence_hint(cells[1])
        room = cells[2]
        weekday_is = cells[3]
        time_display = pad_time(cells[4]) or cells[4]
        if weekday_is not in WEEKDAY_ORDER or not fellowship or not time_display:
            continue

        start_time, end_time = parse_time_range(time_display)
        gender, access = infer_restrictions_from_texts(meeting_name, recurrence_hint)
        venue_text = f"Gula húsið, salur {room}" if room else "Gula húsið"
        raw_payload = {
            "fellowship": fellowship,
            "meeting_name": meeting_name,
            "room": room,
            "weekday_is": weekday_is,
            "time_display": time_display,
            "recurrence_hint": recurrence_hint,
            "source_record_id": f"gula-{index}",
        }
        records.append(
            MeetingRecord(
                source="gula.is",
                source_page_url=GULA_ALL_MEETINGS_URL,
                source_record_id=f"gula-{index}",
                scraped_at_utc=scraped_at_utc,
                weekday_is=weekday_is,
                weekday_order=WEEKDAY_ORDER[weekday_is],
                start_time=start_time,
                end_time=end_time,
                time_display=time_display,
                meeting_name=meeting_name,
                subtitle=None,
                fellowship=fellowship,
                format="Staðfundur",
                location_text="Reykjavík Tjarnargata 20",
                venue_text=venue_text,
                zoom_url=None,
                zoom_meeting_id=None,
                zoom_passcode=None,
                gender_restriction=gender,
                access_restriction=access,
                recurrence_hint=recurrence_hint,
                notes=None,
                tags_json=json.dumps([value for value in [room, recurrence_hint] if value], ensure_ascii=False),
                raw_json=json.dumps(raw_payload, ensure_ascii=False),
            )
        )
    return records


def scrape_coda(session: requests.Session, scraped_at_utc: str) -> list[MeetingRecord]:
    soup = BeautifulSoup(fetch_html(session, CODA_MEETINGS_URL), "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("Engar töflur fundust á coda.is/fundir")

    records: list[MeetingRecord] = []
    main_table = tables[0]
    for index, row in enumerate(main_table.find_all("tr")[1:], start=1):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        weekday_is = normalize_space(cells[0].get_text(" ", strip=True))
        raw_time_display = normalize_space(cells[1].get_text(" ", strip=True))
        time_display = pad_time(raw_time_display) or raw_time_display
        if weekday_is not in WEEKDAY_ORDER or not time_display:
            continue

        description_lines = clean_html_lines(str(cells[2]))
        if not description_lines:
            continue
        meeting_name = description_lines[0]
        detail_lines = description_lines[1:]
        location_tail = normalize_space(cells[3].get_text(" ", strip=True))
        links = [link.get("href", "").strip() for link in row.find_all("a", href=True)]
        zoom_url = next((link for link in links if "zoom.us/" in link.lower()), None)
        map_url = next((link for link in links if "google.com/maps" in link.lower()), None)
        docs_url = next((link for link in links if link not in {zoom_url, map_url}), None)
        zoom_meeting_id, zoom_passcode = extract_zoom_details(" ".join(description_lines))
        explicit_start_time = next(
            (
                pad_time(match.group(1))
                for line in detail_lines
                for match in [re.search(r"fundur\s+hefst\s+(\d{1,2}[:.]\d{2})", line, re.IGNORECASE)]
                if match
            ),
            None,
        )

        gender, access = infer_restrictions_from_texts(meeting_name, *detail_lines)
        recurrence_hint = None
        notes_parts: list[str] = []
        if zoom_url:
            for line in detail_lines:
                if "opnar" in line.casefold() or "fundur hefst" in line.casefold():
                    notes_parts.append(line)
                else:
                    notes_parts.append(line)

        venue_text: str | None = None
        location_text: str | None = None
        subtitle: str | None = None

        if zoom_url:
            meeting_name, recurrence_hint = split_recurrence_hint(meeting_name)
            subtitle = detail_lines[0] if detail_lines else None
            location_text = "Zoom"
            venue_text = None
            meeting_format = "Fjarfundur"
            if explicit_start_time:
                time_display = explicit_start_time
        else:
            meeting_name, recurrence_hint = split_recurrence_hint(meeting_name)
            if re.search(r"\d", meeting_name):
                detail_lines = [meeting_name] + detail_lines
                meeting_name = None
            venue_bits = detail_lines[:]
            if location_tail:
                location_text = location_tail
            if venue_bits:
                venue_text = ", ".join(venue_bits)
            elif location_tail:
                venue_text = None
            if not location_text and venue_text:
                location_text = venue_text
            meeting_format = "Staðfundur"

        start_time, end_time = parse_time_range(time_display)
        notes = " | ".join([part for part in notes_parts if part]) or None
        raw_payload = {
            "weekday_is": weekday_is,
            "time_display": time_display,
            "description_lines": description_lines,
            "location_tail": location_tail,
            "zoom_url": zoom_url,
            "map_url": map_url,
            "docs_url": docs_url,
            "source_record_id": f"coda-{index}",
        }

        records.append(
            MeetingRecord(
                source="coda.is",
                source_page_url=CODA_MEETINGS_URL,
                source_record_id=f"coda-{index}",
                scraped_at_utc=scraped_at_utc,
                weekday_is=weekday_is,
                weekday_order=WEEKDAY_ORDER[weekday_is],
                start_time=start_time,
                end_time=end_time,
                time_display=time_display,
                meeting_name=meeting_name,
                subtitle=subtitle,
                fellowship="CODA",
                format=meeting_format,
                location_text=location_text,
                venue_text=venue_text,
                zoom_url=zoom_url,
                zoom_meeting_id=zoom_meeting_id,
                zoom_passcode=zoom_passcode,
                gender_restriction=gender,
                access_restriction=access,
                recurrence_hint=recurrence_hint,
                notes=notes,
                tags_json=json.dumps([value for value in [docs_url, map_url] if value], ensure_ascii=False),
                raw_json=json.dumps(raw_payload, ensure_ascii=False),
            )
        )

    return records


def scrape_alanon(session: requests.Session, scraped_at_utc: str) -> list[MeetingRecord]:
    soup = BeautifulSoup(fetch_html(session, ALANON_MEETINGS_URL), "html.parser")
    article = soup.find("article")
    groups = article.select(".x-accordion-group") if article else []
    if not groups:
        raise RuntimeError("Fann ekki dagskipta fundaskrá á al-anon.is")

    day_lookup = {day.casefold(): day for day in WEEKDAY_ORDER}
    records: list[MeetingRecord] = []

    for group_index, group in enumerate(groups, start=1):
        heading = group.find(class_="x-accordion-toggle")
        weekday_is = day_lookup.get(normalize_space(heading.get_text(" ", strip=True)).casefold()) if heading else None
        if not weekday_is:
            continue
        table = group.find("table")
        if table is None:
            continue

        current_rows: list[dict[str, object]] = []
        current_time: str | None = None
        previous_location_text: str | None = None
        previous_venue_text: str | None = None
        meeting_counter = 0

        def finalize_current() -> None:
            nonlocal current_rows, current_time, previous_location_text, previous_venue_text, meeting_counter
            if not current_time or not current_rows:
                current_rows = []
                current_time = None
                return

            meeting_counter += 1
            first_text = normalize_space(current_rows[0].get("text"))
            if " á sama stað" in first_text.casefold():
                first_text = re.sub(r"\s+á sama stað\.?$", "", first_text, flags=re.IGNORECASE).strip()
                inherit_previous_place = True
            else:
                inherit_previous_place = False
            meeting_name, location_text, venue_text, forced_format = parse_alanon_headline(first_text)
            links = [
                str(link).strip()
                for row in current_rows
                for link in row.get("links", [])
                if normalize_space(link)
            ]
            zoom_url = next((link for link in links if "zoom.us/" in link.lower()), None)
            zoom_meeting_id, zoom_passcode = extract_zoom_details(
                " ".join(normalize_space(row.get("text")) for row in current_rows)
            )

            recurrence_parts: list[str] = []
            note_parts: list[str] = []
            subtitle: str | None = None

            for extra_row in current_rows[1:]:
                line = normalize_space(extra_row.get("text"))
                if not line:
                    continue
                lowered = line.casefold()
                if lowered in {"á sama stað", "á sama stað."}:
                    inherit_previous_place = True
                    continue
                if line.startswith("http://") or line.startswith("https://"):
                    continue
                if not zoom_url and any(url for url in extra_row.get("links", []) if "zoom.us/" in str(url).lower()):
                    zoom_url = next(url for url in extra_row.get("links", []) if "zoom.us/" in str(url).lower())
                if "fundur id" in lowered or "lykilorð" in lowered or "facebook messenger" in lowered:
                    note_parts.append(line)
                    continue
                if "sporafundur" in lowered or "erfðavenjufundur" in lowered or "samviskufundir" in lowered or "þjónustuhugtakafundur" in lowered or "slagorðafundur" in lowered:
                    recurrence_parts.append(line)
                    continue
                if not venue_text and looks_like_address(line):
                    venue_text = line
                    continue
                if not subtitle and not looks_like_address(line):
                    subtitle = line
                    continue
                note_parts.append(line)

            if inherit_previous_place:
                location_text = location_text or previous_location_text
                venue_text = venue_text or previous_venue_text

            if meeting_name and meeting_name.casefold().startswith("nýliðafund") and inherit_previous_place:
                subtitle = subtitle or "Á sama stað"
            if not location_text and venue_text:
                if "zoom" in venue_text.casefold():
                    location_text = "Zoom"
                else:
                    first_part = normalize_space(venue_text.split(",", 1)[0])
                    if first_part and not looks_like_address(first_part):
                        location_text = first_part

            meeting_format = forced_format or ("Fjarfundur" if (zoom_url or "netfundur" in first_text.casefold() or any("zoom" in normalize_space(row.get("text")).casefold() for row in current_rows)) else "Staðfundur")
            if meeting_format == "Fjarfundur" and not location_text:
                location_text = "Zoom"
            if meeting_format == "Fjarfundur" and venue_text and venue_text.startswith("http"):
                venue_text = None
            if subtitle and subtitle.casefold() == normalize_space(meeting_name).casefold():
                subtitle = None

            gender, access = infer_restrictions_from_texts(meeting_name, subtitle, venue_text, " ".join(note_parts))
            recurrence_hint = " | ".join(recurrence_parts) if recurrence_parts else None
            notes = " | ".join(note_parts) if note_parts else None
            start_time, end_time = parse_time_range(current_time)
            raw_payload = {
                "weekday_is": weekday_is,
                "time_display": current_time,
                "rows": current_rows,
                "source_record_id": f"alanon-{group_index}-{meeting_counter}",
            }

            record = MeetingRecord(
                source="al-anon.is",
                source_page_url=ALANON_MEETINGS_URL,
                source_record_id=f"alanon-{group_index}-{meeting_counter}",
                scraped_at_utc=scraped_at_utc,
                weekday_is=weekday_is,
                weekday_order=WEEKDAY_ORDER[weekday_is],
                start_time=start_time,
                end_time=end_time,
                time_display=current_time,
                meeting_name=meeting_name,
                subtitle=subtitle,
                fellowship="Al-Anon",
                format=meeting_format,
                location_text=location_text,
                venue_text=venue_text,
                zoom_url=zoom_url,
                zoom_meeting_id=zoom_meeting_id,
                zoom_passcode=zoom_passcode,
                gender_restriction=gender,
                access_restriction=access,
                recurrence_hint=recurrence_hint,
                notes=notes,
                tags_json=json.dumps([], ensure_ascii=False),
                raw_json=json.dumps(raw_payload, ensure_ascii=False),
            )
            records.append(record)
            if meeting_format != "Fjarfundur":
                previous_location_text = record.location_text or previous_location_text
                previous_venue_text = record.venue_text or previous_venue_text
            current_rows = []
            current_time = None

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            time_value = pad_time(cells[0].get_text(" ", strip=True))
            row_text = normalize_space(cells[1].get_text(" ", strip=True).strip('"'))
            row_links = [link.get("href", "").strip() for link in cells[1].find_all("a", href=True)]
            if time_value:
                finalize_current()
                current_time = time_value
                current_rows = [{"text": row_text, "links": row_links}]
            elif current_rows and (row_text or row_links):
                current_rows.append({"text": row_text, "links": row_links})

        finalize_current()

    return records


def scrape_twelve_step_house(session: requests.Session, scraped_at_utc: str) -> list[MeetingRecord]:
    soup = BeautifulSoup(fetch_html(session, TWELVE_STEP_HOUSE_MEETINGS_URL), "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Engin tafla fannst á 12sporahusid.is/meetings")

    records: list[MeetingRecord] = []
    for index, row in enumerate(table.find_all("tr")[1:], start=1):
        cells = [normalize_space(cell.get_text(" ", strip=True)) for cell in row.find_all("td")]
        if len(cells) < 5:
            continue
        fellowship = cells[0]
        raw_name = cells[1]
        weekday_is = cells[2]
        raw_time_display = cells[3]
        room = cells[4]
        if weekday_is not in WEEKDAY_ORDER:
            continue

        time_display = pad_time(raw_time_display) or raw_time_display
        if not fellowship:
            if raw_name == "Alateen":
                fellowship = "Alateen"
            else:
                fellowship = normalize_space(re.split(r"\s*[–-]\s*", raw_name, maxsplit=1)[0])
        meeting_name, suffix_gender = split_gender_suffix(raw_name)
        inferred_gender, inferred_access = infer_restrictions_from_texts(raw_name)
        gender = suffix_gender or inferred_gender
        venue_text = f"12 Sporahús Alanó Holtagörðum, 2. hæð (Salur {room})" if room else "12 Sporahús Alanó Holtagörðum, 2. hæð"
        start_time, end_time = parse_time_range(time_display)
        raw_payload = {
            "fellowship": fellowship,
            "meeting_name": raw_name,
            "weekday_is": weekday_is,
            "time_display": time_display,
            "room": room,
            "source_record_id": f"12sporahusid-{index}",
        }
        records.append(
            MeetingRecord(
                source="12sporahusid.is",
                source_page_url=TWELVE_STEP_HOUSE_MEETINGS_URL,
                source_record_id=f"12sporahusid-{index}",
                scraped_at_utc=scraped_at_utc,
                weekday_is=weekday_is,
                weekday_order=WEEKDAY_ORDER[weekday_is],
                start_time=start_time,
                end_time=end_time,
                time_display=time_display,
                meeting_name=meeting_name,
                subtitle=None,
                fellowship=fellowship,
                format="Staðfundur",
                location_text="Reykjavík Holtavegur 10",
                venue_text=venue_text,
                zoom_url=None,
                zoom_meeting_id=None,
                zoom_passcode=None,
                gender_restriction=gender,
                access_restriction=inferred_access,
                recurrence_hint=None,
                notes=None,
                tags_json=json.dumps([value for value in [room] if value], ensure_ascii=False),
                raw_json=json.dumps(raw_payload, ensure_ascii=False),
            )
        )

    return records

def scrape_all(db_path: Path, csv_path: Path, copy_to_clipboard: bool) -> pd.DataFrame:
    session = make_session()
    scraped_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    aa_records = dedupe_aa_remote_variants(scrape_aa(session, scraped_at_utc))
    fjar_records = scrape_fjarfundir(session, scraped_at_utc)
    gula_records = scrape_gula(session, scraped_at_utc)
    coda_records = scrape_coda(session, scraped_at_utc)
    alanon_records = scrape_alanon(session, scraped_at_utc)
    twelve_step_house_records = scrape_twelve_step_house(session, scraped_at_utc)
    records = dedupe_preferred_source_records(
        merge_remote_meetings(aa_records, fjar_records)
        + gula_records
        + coda_records
        + alanon_records
        + twelve_step_house_records
    )
    write_snapshot(db_path, records, scraped_at_utc)
    df = load_dataframe(db_path)
    export_csv(df, csv_path)
    if copy_to_clipboard:
        maybe_copy_to_clipboard(df)
    return df
