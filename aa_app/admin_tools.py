from __future__ import annotations

from difflib import SequenceMatcher

import pandas as pd

from .parsing import normalize_space, normalize_zoom_meeting_id, normalize_zoom_url, sanitize_rows_for_render
from .storage import normalized_location_key


def _match_text(value: object | None) -> str:
    return normalize_space(value).casefold()


def _similarity_ratio(left: object | None, right: object | None) -> float:
    left_text = _match_text(left)
    right_text = _match_text(right)
    if not left_text or not right_text:
        return 0.0
    return SequenceMatcher(None, left_text, right_text).ratio()


def _parse_minutes(value: object | None) -> int | None:
    text = normalize_space(value)
    if not text or ":" not in text:
        return None
    hour_text, minute_text = text.split(":", 1)
    try:
        hour = int(hour_text)
        minute = int(minute_text)
    except ValueError:
        return None
    if hour not in range(0, 24) or minute not in range(0, 60):
        return None
    return (hour * 60) + minute


def _display_location(row: dict[str, object]) -> str:
    for candidate in [
        row.get("location_nickname"),
        row.get("canonical_location_text"),
        row.get("location_text"),
        row.get("venue_text"),
    ]:
        text = normalize_space(candidate)
        if text:
            return text
    return ""


def _build_duplicate_candidate(left: dict[str, object], right: dict[str, object]) -> dict[str, object] | None:
    if normalize_space(left.get("source")) == normalize_space(right.get("source")):
        return None

    try:
        left_weekday_order = int(left.get("weekday_order") or 0)
        right_weekday_order = int(right.get("weekday_order") or 0)
    except (TypeError, ValueError):
        return None
    if left_weekday_order != right_weekday_order or left_weekday_order not in range(1, 8):
        return None

    left_minutes = _parse_minutes(left.get("start_time"))
    right_minutes = _parse_minutes(right.get("start_time"))
    time_gap_minutes: int | None = None
    if left_minutes is not None and right_minutes is not None:
        time_gap_minutes = abs(left_minutes - right_minutes)
        if time_gap_minutes > 15:
            return None
    elif normalize_space(left.get("time_display")) != normalize_space(right.get("time_display")):
        return None

    left_name = left.get("meeting_name_display") or left.get("meeting_name")
    right_name = right.get("meeting_name_display") or right.get("meeting_name")
    name_similarity = _similarity_ratio(left_name, right_name)

    left_location = normalize_space(left.get("canonical_location_text") or left.get("location_text"))
    right_location = normalize_space(right.get("canonical_location_text") or right.get("location_text"))
    left_location_key = normalized_location_key(left_location) if left_location else ""
    right_location_key = normalized_location_key(right_location) if right_location else ""
    location_key_match = bool(left_location_key and left_location_key == right_location_key)
    location_similarity = _similarity_ratio(left_location, right_location)
    venue_similarity = _similarity_ratio(left.get("venue_text"), right.get("venue_text"))

    left_fellowship = left.get("fellowship_display") or left.get("fellowship")
    right_fellowship = right.get("fellowship_display") or right.get("fellowship")
    fellowship_match = bool(_match_text(left_fellowship) and _match_text(left_fellowship) == _match_text(right_fellowship))
    format_match = _match_text(left.get("format")) == _match_text(right.get("format"))

    left_zoom = normalize_zoom_meeting_id(left.get("zoom_meeting_id")) or normalize_zoom_url(left.get("zoom_url"))
    right_zoom = normalize_zoom_meeting_id(right.get("zoom_meeting_id")) or normalize_zoom_url(right.get("zoom_url"))
    zoom_match = bool(left_zoom and left_zoom == right_zoom)

    include = False
    if zoom_match:
        include = True
    elif location_key_match and name_similarity >= 0.68:
        include = True
    elif venue_similarity >= 0.90 and name_similarity >= 0.72:
        include = True
    elif location_similarity >= 0.96 and venue_similarity >= 0.75 and name_similarity >= 0.62:
        include = True

    if not include:
        return None

    reasons: list[str] = []
    if zoom_match:
        reasons.append("Sama Zoom auðkenni eða slóð")
    if time_gap_minutes in {None, 0}:
        reasons.append("Sami tími")
    elif time_gap_minutes is not None:
        reasons.append(f"Tími innan {time_gap_minutes} mínútna")
    if location_key_match:
        reasons.append("Sami staður eftir location mapping")
    elif location_similarity >= 0.96:
        reasons.append("Staðsetning mjög lík")
    if venue_similarity >= 0.90:
        reasons.append("Venue texti mjög líkur")
    if name_similarity >= 0.85:
        reasons.append("Heiti mjög lík")
    elif name_similarity >= 0.68:
        reasons.append("Heiti lík")
    if fellowship_match:
        reasons.append("Sama félag")
    if format_match:
        reasons.append("Sama format")

    score = 0.32
    if zoom_match:
        score += 0.25
    if location_key_match:
        score += 0.17
    if fellowship_match:
        score += 0.08
    if format_match:
        score += 0.05
    score += 0.05 if time_gap_minutes in {None, 0} else 0.03
    score += 0.20 * name_similarity
    score += 0.08 * max(location_similarity, venue_similarity)
    score = min(0.99, score)

    if score >= 0.90:
        confidence_label = "Mjög lík tvítekning"
    elif score >= 0.84:
        confidence_label = "Lík tvítekning"
    else:
        confidence_label = "Veikari vísbending"

    return {
        "score": round(score, 3),
        "confidence_label": confidence_label,
        "match_reasons": reasons,
        "weekday_is": normalize_space(left.get("weekday_is")),
        "time_display": normalize_space(left.get("time_display") or right.get("time_display")),
        "left": {
            "source_uid": normalize_space(left.get("source_uid")),
            "source": normalize_space(left.get("source")),
            "meeting_name_display": normalize_space(left_name),
            "fellowship_display": normalize_space(left_fellowship),
            "format": normalize_space(left.get("format")),
            "location_display": _display_location(left),
            "venue_text": normalize_space(left.get("venue_text")),
            "source_page_url": normalize_space(left.get("source_page_url")),
            "source_locator": normalize_space(left.get("source_locator")),
            "source_excerpt": normalize_space(left.get("source_excerpt")),
        },
        "right": {
            "source_uid": normalize_space(right.get("source_uid")),
            "source": normalize_space(right.get("source")),
            "meeting_name_display": normalize_space(right_name),
            "fellowship_display": normalize_space(right_fellowship),
            "format": normalize_space(right.get("format")),
            "location_display": _display_location(right),
            "venue_text": normalize_space(right.get("venue_text")),
            "source_page_url": normalize_space(right.get("source_page_url")),
            "source_locator": normalize_space(right.get("source_locator")),
            "source_excerpt": normalize_space(right.get("source_excerpt")),
        },
    }


def build_duplicate_review_rows(df: pd.DataFrame, *, max_pairs: int = 80) -> list[dict[str, object]]:
    if df.empty:
        return []

    working = df[df["source"].fillna("").astype(str) != "kirkja"].copy()
    if working.empty:
        return []

    rows = sanitize_rows_for_render(working.to_dict(orient="records"))
    results: list[dict[str, object]] = []

    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            candidate = _build_duplicate_candidate(left, right)
            if candidate is not None:
                results.append(candidate)

    results.sort(
        key=lambda item: (
            -float(item.get("score", 0)),
            normalize_space(item.get("weekday_is")),
            normalize_space(item.get("time_display")),
            normalize_space(item.get("left", {}).get("meeting_name_display")),
            normalize_space(item.get("right", {}).get("meeting_name_display")),
        )
    )
    return results[:max_pairs]
