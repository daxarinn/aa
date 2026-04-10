from __future__ import annotations

import re
from difflib import SequenceMatcher

import pandas as pd

from .config import SOURCE_PRIORITIES
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


def _is_generic_name(value: object | None) -> bool:
    text = _match_text(value)
    return not text or text in {
        "aa fundur",
        "al-anon fundur",
        "ónefndur fundur",
        "onefndur fundur",
        "fundur",
    }


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


def _display_name(row: dict[str, object]) -> str:
    for candidate in [
        row.get("meeting_name_display"),
        row.get("meeting_name"),
        row.get("location_nickname"),
        row.get("canonical_location_text"),
        row.get("location_text"),
        row.get("venue_text"),
        row.get("source_uid"),
    ]:
        text = normalize_space(candidate)
        if text:
            return text
    return "Ónefndur fundur"


def _source_priority_for_row(row: dict[str, object]) -> int:
    return SOURCE_PRIORITIES.get(normalize_space(row.get("source")), 0)


def _field_display(value: object | None) -> str:
    return normalize_space(value) or "óskráð"


def _explicit_room_key(value: object | None) -> str:
    text = _match_text(value)
    if not text:
        return ""
    match = re.search(r"\b(?:salur|sal|room)\s*([a-z0-9]+)\b", text)
    if match:
        return match.group(1)
    match = re.search(r"\((?:salur|sal|room)\s*([a-z0-9]+)\)", text)
    if match:
        return match.group(1)
    return ""


def _zoom_identity(row: dict[str, object]) -> str:
    meeting_id = normalize_zoom_meeting_id(row.get("zoom_meeting_id"))
    if meeting_id:
        return f"id:{meeting_id}"
    zoom_url = normalize_zoom_url(row.get("zoom_url"))
    if zoom_url:
        return f"url:{zoom_url}"
    for field in ["venue_text", "notes", "source_excerpt"]:
        text = normalize_space(row.get(field))
        if not text:
            continue
        match = re.search(r"(?:meeting\s*id|zoom\s*id|fundarn[uú]mer)\D*([\d\s]{8,})", text, flags=re.IGNORECASE)
        if match:
            parsed_id = normalize_zoom_meeting_id(match.group(1))
            if parsed_id:
                return f"id:{parsed_id}"
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
        if time_gap_minutes > 30:
            return None
    elif normalize_space(left.get("time_display")) != normalize_space(right.get("time_display")):
        return None

    left_name = left.get("meeting_name_display") or left.get("meeting_name")
    right_name = right.get("meeting_name_display") or right.get("meeting_name")
    name_similarity = _similarity_ratio(left_name, right_name)
    generic_name_match = _is_generic_name(left_name) and _is_generic_name(right_name)

    left_location = normalize_space(left.get("canonical_location_text") or left.get("location_text"))
    right_location = normalize_space(right.get("canonical_location_text") or right.get("location_text"))
    left_location_key = normalized_location_key(left_location) if left_location else ""
    right_location_key = normalized_location_key(right_location) if right_location else ""
    location_key_match = bool(left_location_key and left_location_key == right_location_key)
    location_similarity = _similarity_ratio(left_location, right_location)
    venue_similarity = _similarity_ratio(left.get("venue_text"), right.get("venue_text"))
    location_or_venue_similarity = max(location_similarity, venue_similarity)
    left_room = _explicit_room_key(left.get("venue_text"))
    right_room = _explicit_room_key(right.get("venue_text"))
    explicit_room_mismatch = bool(left_room and right_room and left_room != right_room)

    left_fellowship = left.get("fellowship_display") or left.get("fellowship")
    right_fellowship = right.get("fellowship_display") or right.get("fellowship")
    fellowship_match = bool(_match_text(left_fellowship) and _match_text(left_fellowship) == _match_text(right_fellowship))
    format_match = _match_text(left.get("format")) == _match_text(right.get("format"))

    left_zoom = _zoom_identity(left)
    right_zoom = _zoom_identity(right)
    zoom_match = bool(left_zoom and left_zoom == right_zoom)
    both_remote = _match_text(left.get("format")) == "fjarfundur" and _match_text(right.get("format")) == "fjarfundur"
    if both_remote and left_zoom and right_zoom and left_zoom != right_zoom:
        return None
    left_gender = _match_text(left.get("gender_restriction"))
    right_gender = _match_text(right.get("gender_restriction"))
    explicit_genders = {"blandaður", "karlar", "konur"}
    gender_conflict = left_gender in explicit_genders and right_gender in explicit_genders and left_gender != right_gender
    if gender_conflict and not zoom_match and name_similarity < 0.85:
        return None

    include = False
    if zoom_match:
        include = True
    elif location_key_match and (name_similarity >= 0.42 or generic_name_match or fellowship_match):
        include = True
    elif venue_similarity >= 0.78 and (name_similarity >= 0.45 or generic_name_match):
        include = True
    elif location_similarity >= 0.88 and (venue_similarity >= 0.58 or name_similarity >= 0.50 or generic_name_match):
        include = True
    elif explicit_room_mismatch and location_key_match and name_similarity >= 0.72 and fellowship_match:
        include = True
    elif time_gap_minutes in {None, 0} and location_or_venue_similarity >= 0.82 and fellowship_match:
        include = True
    elif time_gap_minutes in {None, 0} and name_similarity >= 0.88 and fellowship_match and format_match:
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
    elif location_similarity >= 0.88:
        reasons.append("Staðsetning lík")
    if venue_similarity >= 0.90:
        reasons.append("Venue texti mjög líkur")
    elif venue_similarity >= 0.78:
        reasons.append("Venue texti líkur")
    elif explicit_room_mismatch:
        reasons.append("Ólíkur skráður salur")
    if name_similarity >= 0.85:
        reasons.append("Heiti mjög lík")
    elif name_similarity >= 0.42:
        reasons.append("Heiti lík")
    elif generic_name_match:
        reasons.append("Bæði heiti eru ósértæk")
    if fellowship_match:
        reasons.append("Sama félag")
    if format_match:
        reasons.append("Sama format")

    score = 0.28
    if zoom_match:
        score += 0.25
    if location_key_match:
        score += 0.20
    if fellowship_match:
        score += 0.08
    if format_match:
        score += 0.05
    if generic_name_match:
        score += 0.04
    if explicit_room_mismatch and location_key_match and fellowship_match:
        score += 0.07
    score += 0.08 if time_gap_minutes in {None, 0} else 0.05
    score += 0.20 * name_similarity
    score += 0.15 * location_or_venue_similarity
    score = min(0.99, score)

    if score >= 0.90:
        confidence_label = "Mjög lík tvítekning"
    elif score >= 0.80:
        confidence_label = "Lík tvítekning"
    else:
        confidence_label = "Veikari vísbending"
    score_value = round(score, 3)

    return {
        "score": score_value,
        "confidence_label": confidence_label,
        "match_reasons": reasons,
        "weekday_is": normalize_space(left.get("weekday_is")),
        "time_display": normalize_space(left.get("time_display") or right.get("time_display")),
        "sort_score": score_value + (0.04 if venue_similarity >= 0.90 else 0) + (0.03 if name_similarity >= 0.85 else 0),
        "left": {
            "source_uid": normalize_space(left.get("source_uid")),
            "source": normalize_space(left.get("source")),
            "meeting_name_display": _display_name(left),
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
            "meeting_name_display": _display_name(right),
            "fellowship_display": normalize_space(right_fellowship),
            "format": normalize_space(right.get("format")),
            "location_display": _display_location(right),
            "venue_text": normalize_space(right.get("venue_text")),
            "source_page_url": normalize_space(right.get("source_page_url")),
            "source_locator": normalize_space(right.get("source_locator")),
            "source_excerpt": normalize_space(right.get("source_excerpt")),
        },
    }


def build_duplicate_review_rows(df: pd.DataFrame, *, max_pairs: int = 160) -> list[dict[str, object]]:
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


def _disagreement_key(row: dict[str, object]) -> tuple[str, str, str, str, str] | None:
    day_key = normalize_space(row.get("weekday_is"))
    time_key = normalize_space(row.get("start_time") or row.get("time_display"))
    fellowship_key = _match_text(row.get("fellowship_display") or row.get("fellowship"))
    name_key = _match_text(row.get("meeting_name_display") or row.get("meeting_name"))
    location_text = normalize_space(row.get("canonical_location_text") or row.get("location_text"))
    location_key = normalized_location_key(location_text)
    if not day_key or not time_key or not fellowship_key or not name_key or not location_key:
        return None
    return (day_key, time_key, fellowship_key, name_key, location_key)


def _field_values_differ(rows: list[dict[str, object]], field: str) -> bool:
    return len({normalize_space(row.get(field)) for row in rows}) > 1


def build_source_disagreement_options(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    working = df[df["source"].fillna("").astype(str) != "kirkja"].copy()
    if working.empty:
        return []
    counts = working["source"].fillna("").astype(str).str.strip()
    output: list[dict[str, object]] = []
    for source, count in counts[counts != ""].value_counts().items():
        output.append(
            {
                "source": source,
                "count": int(count),
                "priority": SOURCE_PRIORITIES.get(source, 0),
            }
        )
    output.sort(key=lambda item: (-int(item.get("priority") or 0), normalize_space(item.get("source"))))
    return output


def _report_value(row: dict[str, object], field: str) -> str:
    if field == "meeting_name_display":
        return _display_name(row)
    if field == "location_display":
        return _display_location(row)
    if field == "time_display":
        return normalize_space(row.get("start_time") or row.get("time_display")) or _field_display(None)
    return _field_display(row.get(field))


def _report_values_differ(left: dict[str, object], right: dict[str, object], field: str) -> bool:
    return _report_value(left, field).casefold() != _report_value(right, field).casefold()


def _source_report_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "source": normalize_space(row.get("source")),
        "priority": _source_priority_for_row(row),
        "source_uid": normalize_space(row.get("source_uid")),
        "meeting_name_display": _display_name(row),
        "weekday_is": normalize_space(row.get("weekday_is")),
        "time_display": normalize_space(row.get("time_display") or row.get("start_time")),
        "gender_restriction": _field_display(row.get("gender_restriction")),
        "access_restriction": _field_display(row.get("access_restriction")),
        "format": _field_display(row.get("format")),
        "location_display": _display_location(row),
        "venue_text": _field_display(row.get("venue_text")),
        "zoom_meeting_id": _field_display(row.get("zoom_meeting_id")),
        "zoom_url": normalize_space(row.get("zoom_url")),
        "zoom_passcode": _field_display(row.get("zoom_passcode")),
        "source_page_url": normalize_space(row.get("source_page_url")),
        "source_locator": normalize_space(row.get("source_locator")),
        "source_excerpt": normalize_space(row.get("source_excerpt")),
    }


def build_source_disagreement_rows(
    df: pd.DataFrame,
    *,
    selected_source: str = "",
    max_groups: int = 160,
) -> list[dict[str, object]]:
    if df.empty:
        return []

    working = df[df["source"].fillna("").astype(str) != "kirkja"].copy()
    if working.empty:
        return []

    selected_source = normalize_space(selected_source)
    rows = sanitize_rows_for_render(working.to_dict(orient="records"))
    fields = [
        ("meeting_name_display", "Heiti"),
        ("time_display", "Tími"),
        ("gender_restriction", "Kyn"),
        ("access_restriction", "Aðgangur"),
        ("format", "Form"),
        ("location_display", "Staður"),
        ("venue_text", "Salur/venue"),
        ("zoom_meeting_id", "Zoom ID"),
        ("zoom_url", "Zoom slóð"),
        ("zoom_passcode", "Zoom lykilorð"),
    ]
    output: list[dict[str, object]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            candidate = _build_duplicate_candidate(left, right)
            if candidate is None:
                continue
            if float(candidate.get("score") or 0) < 0.80:
                continue
            left_source = normalize_space(left.get("source"))
            right_source = normalize_space(right.get("source"))
            if selected_source and selected_source not in {left_source, right_source}:
                continue
            if selected_source:
                target, comparison = (left, right) if left_source == selected_source else (right, left)
            elif _source_priority_for_row(left) < _source_priority_for_row(right):
                target, comparison = left, right
            else:
                target, comparison = right, left

            target_uid = normalize_space(target.get("source_uid"))
            comparison_uid = normalize_space(comparison.get("source_uid"))
            pair_key = tuple(sorted([target_uid, comparison_uid]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            disagreements: list[dict[str, str]] = []
            for field, label in fields:
                if _report_values_differ(target, comparison, field):
                    disagreements.append(
                        {
                            "field": field,
                            "label": label,
                            "target_value": _report_value(target, field),
                            "comparison_value": _report_value(comparison, field),
                        }
                    )

            if not disagreements:
                continue

            target_report = _source_report_row(target)
            comparison_report = _source_report_row(comparison)
            sort_score = float(candidate.get("sort_score") or candidate.get("score") or 0)
            output.append(
                {
                    "weekday_is": target_report["weekday_is"],
                    "time_display": target_report["time_display"],
                    "meeting_name": target_report["meeting_name_display"],
                    "location_display": target_report["location_display"],
                    "target": target_report,
                    "comparison": comparison_report,
                    "preferred_source": comparison_report["source"]
                    if int(comparison_report.get("priority") or 0) >= int(target_report.get("priority") or 0)
                    else target_report["source"],
                    "score": candidate["score"],
                    "sort_score": sort_score,
                    "confidence_label": candidate["confidence_label"],
                    "match_reasons": candidate["match_reasons"],
                    "disagreements": disagreements,
                }
            )

    best_by_target: dict[str, dict[str, object]] = {}
    for item in output:
        target_uid = normalize_space(item.get("target", {}).get("source_uid"))
        if not target_uid:
            continue
        existing = best_by_target.get(target_uid)
        if existing is None or float(item.get("sort_score") or 0) > float(existing.get("sort_score") or 0):
            best_by_target[target_uid] = item
    output = list(best_by_target.values())

    output.sort(
        key=lambda item: (
            normalize_space(item.get("target", {}).get("source")),
            normalize_space(item.get("weekday_is")),
            normalize_space(item.get("time_display")),
            normalize_space(item.get("meeting_name")),
            -float(item.get("score") or 0),
        )
    )
    return output[:max_groups]
