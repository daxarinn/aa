from __future__ import annotations

import html
import json
import re
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .config import SIZE_BIN_OPTIONS, WEEKDAY_ORDER

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def normalize_space(value: object | None) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    text = value if isinstance(value, str) else str(value)
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def truncate_text(value: str | None, max_length: int = 180) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "…"


def clean_display_value(value: object | None, *, allow_placeholder: bool = False) -> object | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = normalize_space(value)
        if not text:
            return None
        if not allow_placeholder and text.casefold() in {"nan", "n/a", "none", "null", "nat"}:
            return None
        return text
    return value


def size_bin_midpoint(size_bin: str | None) -> float | None:
    normalized = normalize_space(size_bin)
    for item in SIZE_BIN_OPTIONS:
        if item["value"] == normalized:
            return float(item["midpoint"])
    return None


def size_bin_from_average(avg_size_value: float | None) -> str | None:
    if avg_size_value is None or pd.isna(avg_size_value):
        return None
    if avg_size_value < 10:
        return "2-9"
    if avg_size_value < 20:
        return "10-19"
    if avg_size_value < 40:
        return "20-39"
    return "40+"


def build_size_display(avg_size_bin: object | None, report_count: object | None) -> str | None:
    label = normalize_space(avg_size_bin)
    if not label:
        return None
    try:
        count = int(report_count or 0)
    except (TypeError, ValueError):
        count = 0
    if count > 0:
        suffix = "svar" if count == 1 else "svör"
        return f"Stærð {label} ({count} {suffix})"
    return f"Stærð {label}"


def is_meeting_live_now(row: dict[str, object], now_dt: datetime | None = None) -> bool:
    current_dt = now_dt or datetime.now(ZoneInfo("Atlantic/Reykjavik"))
    try:
        weekday_order = int(row.get("weekday_order") or 0)
    except (TypeError, ValueError):
        return False
    if weekday_order != current_dt.isoweekday():
        return False
    start_parts = parse_clock_time(row.get("start_time"))
    if start_parts is None:
        return False
    start_minutes = (start_parts[0] * 60) + start_parts[1]
    now_minutes = (current_dt.hour * 60) + current_dt.minute
    return start_minutes <= now_minutes < (start_minutes + 60)


def sanitize_rows_for_render(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    sanitized_rows: list[dict[str, object]] = []
    current_dt = datetime.now(ZoneInfo("Atlantic/Reykjavik"))
    for row in rows:
        clean_row: dict[str, object] = {}
        for key, value in row.items():
            clean_row[key] = clean_display_value(value)
        clean_row["meeting_name_display"] = build_meeting_name_display(clean_row)
        zoom_url = normalize_space(clean_row.get("zoom_url"))
        if zoom_url and not re.match(r"^https?://", zoom_url, re.I):
            clean_row["zoom_url"] = None
        clean_row["summary_display"] = build_summary_display(clean_row)
        clean_row["size_display"] = build_size_display(clean_row.get("avg_size_bin"), clean_row.get("size_report_count"))
        clean_row["is_live_now"] = is_meeting_live_now(clean_row, current_dt)
        sanitized_rows.append(clean_row)
    return sanitized_rows


def extract_place_name(value: object | None) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    if text.casefold() == "zoom":
        return "Zoom"
    first_part = text.split(",")[0].strip()
    tokens = first_part.split()
    if not tokens:
        return None
    stop_suffixes = (
        "gata",
        "götu",
        "vegur",
        "vegi",
        "veg",
        "stígur",
        "stíg",
        "stigur",
        "braut",
        "brautar",
        "torg",
        "hús",
        "hus",
    )
    collected: list[str] = []
    for token in tokens:
        lowered = token.casefold()
        if any(char.isdigit() for char in lowered):
            break
        if lowered.endswith(stop_suffixes):
            break
        collected.append(token)
    if collected:
        return " ".join(collected)
    return tokens[0]


def build_meeting_name_display(row: dict[str, object]) -> str:
    meeting_name = normalize_space(row.get("meeting_name"))
    if meeting_name:
        return meeting_name
    location_nickname = normalize_space(row.get("location_nickname"))
    if location_nickname:
        return location_nickname
    fallback_title = build_unnamed_meeting_fallback(row)
    if fallback_title:
        return fallback_title
    return "Ónefndur fundur"


def build_unnamed_meeting_fallback(row: dict[str, object]) -> str | None:
    fellowship_name = normalize_space(row.get("fellowship_display"))
    if fellowship_name and fellowship_name != "Óskráð félag":
        return f"{fellowship_name} fundur"
    for candidate in [
        row.get("canonical_location_text"),
        row.get("location_text"),
        row.get("venue_text"),
    ]:
        place_name = extract_place_name(candidate)
        if place_name:
            return place_name
    return None


def build_venue_summary(value: object | None) -> str | None:
    text = clean_display_value(value)
    if not text:
        return None
    first_part = normalize_space(text.split(",")[0])
    if not first_part:
        return None
    if first_part.casefold().startswith(
        (
            "gengið inn",
            "gengid inn",
            "inngangur",
            "vinstri hlið",
            "vinstri hlid",
            "hægra megin",
            "haegra megin",
            "norðanmegin",
            "nordanmegin",
            "sunnanmegin",
            "austanmegin",
            "vestanmegin",
            "2. hæð",
            "2. haed",
            "1. hæð",
            "1. haed",
        )
    ):
        return None
    return first_part


def build_summary_display(row: dict[str, object]) -> str:
    title_fallback = None
    if not normalize_space(row.get("meeting_name")) and normalize_space(row.get("location_nickname")):
        title_fallback = build_unnamed_meeting_fallback(row)
    candidates = [
        title_fallback,
        clean_display_value(row.get("location_nickname")),
        extract_place_name(row.get("canonical_location_text")),
        extract_place_name(row.get("location_text")),
        clean_display_value(row.get("location_text")),
        build_venue_summary(row.get("venue_text")),
        clean_display_value(row.get("subtitle")),
        clean_display_value(row.get("fellowship_display")),
    ]
    seen: set[str] = set()
    for candidate in candidates:
        text = normalize_space(candidate)
        if not text:
            continue
        if text.casefold() == "zoom":
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        return text
    return "Nánari upplýsingar"


def current_iceland_weekday() -> str:
    today_order = datetime.now(ZoneInfo("Atlantic/Reykjavik")).isoweekday()
    for day_name, day_order in WEEKDAY_ORDER.items():
        if day_order == today_order:
            return day_name
    return next(iter(WEEKDAY_ORDER))


def format_scraped_at_short(value: object | None) -> str:
    text = normalize_space(value)
    if not text:
        return "óþekkt"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    return parsed.astimezone(ZoneInfo("Atlantic/Reykjavik")).strftime("%Y-%m-%d %H:%M")


def capital_region_mask(df: pd.DataFrame) -> pd.Series:
    combined = (
        df["canonical_location_text"].fillna("").astype(str)
        + " "
        + df["location_text"].fillna("").astype(str)
        + " "
        + df["venue_text"].fillna("").astype(str)
    ).str.casefold()
    patterns = [
        "reykjav",
        "kópavog",
        "kopavog",
        "hafnarfj",
        "garðab",
        "gardab",
        "mosfellsb",
        "seltjarnarn",
        "álftanes",
        "alftanes",
    ]
    mask = pd.Series(False, index=df.index)
    for pattern in patterns:
        mask = mask | combined.str.contains(pattern, regex=False)
    return mask


def pad_time(time_value: str | None) -> str | None:
    if not time_value:
        return None
    normalized = normalize_space(time_value).replace(".", ":")
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", normalized)
    if not match:
        return None
    return f"{int(match.group(1)):02d}:{match.group(2)}"


def parse_time_range(text: str) -> tuple[str | None, str | None]:
    matches = re.findall(r"(\d{1,2}[:.]\d{2})", text or "")
    if not matches:
        return None, None
    start_time = pad_time(matches[0])
    end_time = pad_time(matches[1]) if len(matches) > 1 else None
    return start_time, end_time


def clean_html_lines(fragment: str) -> list[str]:
    soup = BeautifulSoup(fragment or "", "html.parser")
    text = soup.get_text("\n")
    lines = [normalize_space(html.unescape(line)) for line in text.splitlines()]
    return [line for line in lines if line]


def extract_first_link(fragment: str) -> str | None:
    soup = BeautifulSoup(fragment or "", "html.parser")
    link = soup.find("a", href=True)
    return link["href"].strip() if link else None


def extract_urls(text: str) -> list[str]:
    return re.findall(r"https?://[^\s<>\"]+", text or "")


def strip_zoom_meta(text: str) -> str:
    value = normalize_space(text)
    value = re.sub(r"https?://\S+", "", value)
    value = re.sub(r"(Meeting ID|ID):\s*[0-9 ]+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"Pass(?:code)?\s*:\s*[^ ]+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\(\s*opnar[^)]*\)", "", value, flags=re.IGNORECASE)
    value = value.replace("+", " ")
    return normalize_space(value)


def extract_zoom_details(text: str) -> tuple[str | None, str | None]:
    meeting_id_match = re.search(r"(?:Meeting ID|ID):\s*([0-9 ]+)", text or "", re.IGNORECASE)
    passcode_match = re.search(r"(?:Pass(?:code)?|Lykilorð)\s*:\s*([A-Za-z0-9]+)", text or "", re.IGNORECASE)
    meeting_id = normalize_space(meeting_id_match.group(1)) if meeting_id_match else None
    passcode = normalize_space(passcode_match.group(1)) if passcode_match else None
    return meeting_id, passcode


def normalize_zoom_meeting_id(meeting_id: str | None) -> str:
    return re.sub(r"\D+", "", meeting_id or "")


def normalize_zoom_url(url: str | None) -> str:
    value = normalize_space(url).lower()
    value = re.sub(r"[?#].*$", "", value)
    return value.rstrip("/")


def parse_restrictions(lines: Iterable[str]) -> tuple[str | None, str | None, list[str]]:
    gender = "Blandaður"
    access = "Lokaður"
    tags: list[str] = []

    for raw_line in lines:
        line = normalize_space(raw_line.strip(" -–—"))
        if not line:
            continue

        lowered = line.casefold()

        if "karlar" in lowered:
            gender = "Karlar"
        elif "konur" in lowered:
            gender = "Konur"
        elif "blanda" in lowered:
            gender = "Blandaður"

        if "lokað" in lowered or "lokaður" in lowered or "lokuð" in lowered:
            access = "Lokaður"
        elif "opinn" in lowered or "opin" in lowered or "opið" in lowered:
            access = "Opinn"

        tags.append(line)

    return gender, access, tags


def classify_format(location_text: str | None, venue_text: str | None, zoom_url: str | None) -> str:
    haystack = " ".join(filter(None, [location_text, venue_text, zoom_url])).casefold()
    if "zoom" in haystack or zoom_url:
        return "Fjarfundur"
    return "Staðfundur"


def infer_restrictions_from_texts(*texts: object) -> tuple[str | None, str | None]:
    gender: str | None = None
    access: str | None = "Lokaður"
    combined = " ".join(normalize_space(text) for text in texts if normalize_space(text))
    lowered = combined.casefold()
    if not lowered:
        return None, access
    if "karla" in lowered or "karlar" in lowered:
        gender = "Karlar"
    elif "kvenna" in lowered or "konur" in lowered or "kvk" in lowered:
        gender = "Konur"
    elif "blanda" in lowered:
        gender = "Blandaður"

    if "opinn" in lowered or "opin" in lowered or "opið" in lowered:
        access = "Opinn"
    elif "lokað" in lowered or "lokaður" in lowered or "lokuð" in lowered:
        access = "Lokaður"
    return gender, access


def split_recurrence_hint(value: object | None) -> tuple[str | None, str | None]:
    text = normalize_space(value)
    if not text:
        return None, None
    match = re.search(r"\(([^)]*(?:mánuð|mánað|vika|viku|fyrsta|annar|þriðja|fjórða)[^)]*)\)", text, re.IGNORECASE)
    if not match:
        return text, None
    recurrence_hint = normalize_space(match.group(1))
    cleaned = normalize_space((text[:match.start()] + " " + text[match.end():]).strip())
    return cleaned or None, recurrence_hint or None


ALANON_PLACE_HINTS = {
    "reykjavík",
    "kópavogur",
    "hafnarfjörður",
    "ísafjörður",
    "mosfellsbær",
    "egilsstaðir",
    "selfoss",
    "vestmannaeyjar",
    "akranes",
    "reykjanesbær",
    "akureyri",
    "osló",
    "netfundur",
}


def looks_like_address(text: object | None) -> bool:
    value = normalize_space(text)
    lowered = value.casefold()
    if not lowered:
        return False
    if re.search(r"\d", lowered):
        return True
    return any(
        token in lowered
        for token in [
            "gata",
            "götu",
            "vegur",
            "vegi",
            "veg",
            "hús",
            "húsið",
            "kirkja",
            "kirkj",
            "salur",
            "hæð",
            "holtagörðum",
            "gula húsið",
            "hvíta húsið",
        ]
    )


def parse_alanon_headline(text: str) -> tuple[str | None, str | None, str | None, str | None]:
    clean_text = normalize_space(text.strip(' "'))
    if not clean_text:
        return None, None, None, None

    if clean_text.casefold().startswith("netfundur"):
        remainder = re.sub(r"^netfundur\s*[-–—]?\s*", "", clean_text, flags=re.IGNORECASE).strip(' "')
        return remainder or clean_text, "Zoom", None, "Fjarfundur"

    if clean_text.casefold() in ALANON_PLACE_HINTS:
        return None, clean_text, None, None

    for place_hint in sorted(ALANON_PLACE_HINTS, key=len, reverse=True):
        lowered = clean_text.casefold()
        if lowered.startswith(f"{place_hint} "):
            remainder = normalize_space(clean_text[len(place_hint):])
            if looks_like_address(remainder):
                return None, place_hint.title(), remainder, None
            return remainder.lstrip("–- ").strip(' "') or None, place_hint.title(), None, None

    dash_match = re.match(r"^(?P<left>.+?)\s*[–-]\s*(?P<right>.+)$", clean_text)
    if dash_match:
        left = normalize_space(dash_match.group("left"))
        right = normalize_space(dash_match.group("right")).lstrip("–- ").strip(' "')
        if left.casefold() in ALANON_PLACE_HINTS:
            return right or None, left, None, None
        if looks_like_address(right):
            return left, None, right, None

    for place_hint in sorted(ALANON_PLACE_HINTS, key=len, reverse=True):
        if clean_text.casefold().endswith(f" {place_hint}"):
            name = normalize_space(clean_text[: -len(place_hint)])
            return name or None, place_hint.title(), None, None

    if looks_like_address(clean_text):
        return None, None, clean_text, None
    return clean_text, None, None, None


def split_gender_suffix(text: object | None) -> tuple[str | None, str | None]:
    clean_text = normalize_space(text)
    if not clean_text:
        return None, None
    match = re.match(r"^(?P<name>.+?)\s*[–-]\s*(?P<suffix>Karlar|Konur)$", clean_text, re.IGNORECASE)
    if not match:
        return clean_text, None
    gender = "Karlar" if match.group("suffix").casefold().startswith("kar") else "Konur"
    return normalize_space(match.group("name")), gender

def parse_clock_time(value: object | None) -> tuple[int, int] | None:
    match = re.match(r"^(\d{1,2})[:.](\d{2})$", normalize_space(value))
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour not in range(0, 24) or minute not in range(0, 60):
        return None
    return hour, minute
