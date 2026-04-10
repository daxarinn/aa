from __future__ import annotations

import csv
import hashlib
import hmac
import io
import json
import os
import secrets
import socket
from pathlib import Path
from urllib.parse import unquote, urlencode

import pandas as pd
from flask import Flask, Response, redirect, request, session

from .admin_tools import build_duplicate_review_rows, build_source_disagreement_options, build_source_disagreement_rows
from .config import (
    AA_DAY_PAGES,
    CHURCH_LOCATION_ICON_KEY,
    CLIENT_COOKIE_NAME,
    FAVORITES_COOKIE_NAME,
    FILTERS_COOKIE_NAME,
    SIZE_BIN_OPTIONS,
    WEEKDAY_ORDER,
)
from .parsing import capital_region_mask, current_iceland_weekday, format_scraped_at_short, normalize_space, sanitize_rows_for_render
from .storage import (
    build_favorites_calendar_ics,
    build_location_clusters,
    build_location_review_rows,
    delete_meeting_merge,
    delete_manual_event,
    load_dataframe,
    load_favorite_calendar_rows,
    load_favorite_calendar_subscription,
    load_location_metadata,
    load_manual_events,
    load_meeting_merges,
    load_user_size_reports,
    load_visit_summary,
    log_client_visit,
    normalized_favorite_ids,
    save_location_mapping,
    save_location_nickname,
    save_manual_event,
    save_meeting_merge,
    save_meeting_size_report,
    upsert_favorite_calendar_subscription,
)
from .templates import CARD_TEMPLATE

def distinct_values(df: pd.DataFrame, column: str, max_items: int | None = None) -> list[str]:
    values = [value for value in df[column].dropna().astype(str).tolist() if value.strip()]
    ordered = sorted(set(values))
    if max_items is not None:
        return ordered[:max_items]
    return ordered


def build_query_string(filters: dict[str, str], overrides: dict[str, str] | None = None, exclude: set[str] | None = None) -> str:
    params = dict(filters)
    if overrides:
        params.update(overrides)
    if exclude:
        for key in exclude:
            params.pop(key, None)
    return urlencode({key: value for key, value in params.items() if value})


def build_adjacent_weekday_queries(filters: dict[str, str]) -> tuple[str | None, str | None]:
    week_days = [day for day, _ in AA_DAY_PAGES]
    current_weekday = normalize_space(filters.get("weekday"))
    if current_weekday not in week_days:
        return None, None
    current_index = week_days.index(current_weekday)
    previous_weekday = week_days[(current_index - 1) % len(week_days)]
    next_weekday = week_days[(current_index + 1) % len(week_days)]
    previous_query = build_query_string(filters, overrides={"weekday": previous_weekday, "view": "week"})
    next_query = build_query_string(filters, overrides={"weekday": next_weekday, "view": "week"})
    return previous_query, next_query


def read_json_cookie(name: str) -> dict[str, object] | list[object] | None:
    raw_value = request.cookies.get(name)
    if not raw_value:
        return None
    try:
        return json.loads(unquote(raw_value))
    except json.JSONDecodeError:
        return None

def read_json_cookie(name: str) -> dict[str, object] | list[object] | None:
    raw_value = request.cookies.get(name)
    if not raw_value:
        return None
    try:
        return json.loads(unquote(raw_value))
    except json.JSONDecodeError:
        return None


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


def favorite_ids_from_cookie() -> list[str]:
    return normalized_favorite_ids(read_json_cookie(FAVORITES_COOKIE_NAME) or [])

def request_filters() -> dict[str, str]:
    clear_filters = request.args.get("clear_filters", "").strip() == "1"
    saved_filters = {} if clear_filters else (read_json_cookie(FILTERS_COOKIE_NAME) or {})
    default_weekday = current_iceland_weekday()

    def resolve_filter_value(name: str) -> str:
        if name in request.args:
            return request.args.get(name, "").strip()
        if name == "weekday":
            return default_weekday
        if isinstance(saved_filters, dict):
            if name in saved_filters:
                return normalize_space(saved_filters.get(name))
        return ""

    include_church_value = ""
    if "include_church" in request.args:
        include_church_value = request.args.get("include_church", "").strip()
    elif isinstance(saved_filters, dict) and "include_church" in saved_filters:
        include_church_value = normalize_space(saved_filters.get("include_church"))
    else:
        include_church_value = "1"

    return {
        "view": request.args.get("view", "week").strip() or "week",
        "weekday": resolve_filter_value("weekday"),
        "fellowship": resolve_filter_value("fellowship"),
        "format": resolve_filter_value("format"),
        "gender_restriction": resolve_filter_value("gender_restriction"),
        "access_restriction": resolve_filter_value("access_restriction"),
        "canonical_location": resolve_filter_value("canonical_location"),
        "region": resolve_filter_value("region"),
        "include_church": "1" if include_church_value in {"1", "true", "on", "yes"} else "",
        "time_from": resolve_filter_value("time_from"),
        "time_to": resolve_filter_value("time_to"),
        "favorites_only": "1" if resolve_filter_value("favorites_only") in {"1", "true", "on", "yes"} else "",
    }


def build_filter_options(df: pd.DataFrame) -> dict[str, list[str] | list[dict[str, str]]]:
    nickname_values = sorted(
        {
            normalize_space(value)
            for value in df["location_nickname"].fillna("").tolist()
            if normalize_space(value)
        },
        key=lambda value: value.casefold(),
    )
    nickname_locations = [{"value": "__zoom__", "label": "Zoom"}]
    nickname_locations.extend(
        {"value": value, "label": value}
        for value in nickname_values
        if value.casefold() != "zoom"
    )

    return {
        "weekday_is": [day for day, _ in AA_DAY_PAGES],
        "fellowship": distinct_values(df, "fellowship"),
        "format": distinct_values(df, "format"),
        "size_bins": [{"value": item["value"], "label": item["label"]} for item in SIZE_BIN_OPTIONS],
        "region_options": [
            {"value": "capital", "label": "Höfuðborgarsvæðið"},
            {"value": "rural", "label": "Landsbyggðin"},
        ],
        "gender_filter_options": [
            {"value": "Blandaður", "label": "Bara blandaðir"},
            {"value": "Karlar", "label": "Bara karla"},
            {"value": "Konur", "label": "Bara kvenna"},
            {"value": "Blandaður+Karlar", "label": "Blandaðir + karla"},
            {"value": "Blandaður+Konur", "label": "Blandaðir + kvenna"},
        ],
        "access_restriction": distinct_values(df, "access_restriction"),
        "nickname_locations": nickname_locations,
    }

def build_week_view(rows: list[dict[str, str | None]], week_days: list[str]) -> list[dict[str, object]]:
    slots: dict[tuple[int, str], dict[str, object]] = {}

    for row in rows:
        weekday_is = normalize_space(row.get("weekday_is"))
        if weekday_is not in week_days:
            continue

        start_time = normalize_space(row.get("start_time"))
        time_display = normalize_space(row.get("time_display")) or "Ótímasett"
        if start_time:
            slot_key = (0, start_time)
            time_label = start_time
        else:
            slot_key = (1, time_display)
            time_label = time_display

        if slot_key not in slots:
            slots[slot_key] = {
                "time_label": time_label,
                "cells": {day: [] for day in week_days},
            }

        slots[slot_key]["cells"][weekday_is].append(row)

    ordered_slots: list[dict[str, object]] = []
    for slot_key in sorted(slots.keys(), key=lambda item: item):
        slot = slots[slot_key]
        recurrence_keys: dict[str, dict[str, object]] = {}
        for day in week_days:
            for item in slot["cells"][day]:
                recurrence_key = "|".join(
                    [
                        normalize_space(item.get("meeting_name_display") or item.get("meeting_name")),
                        normalize_space(item.get("fellowship_display") or item.get("fellowship")),
                        normalize_space(item.get("location_nickname")),
                        normalize_space(item.get("canonical_location_text") or item.get("location_text")),
                        normalize_space(item.get("venue_text")),
                        normalize_space(item.get("zoom_meeting_id")),
                        normalize_space(item.get("format")),
                    ]
                )
                info = recurrence_keys.setdefault(
                    recurrence_key,
                    {
                        "days": set(),
                        "name": normalize_space(item.get("meeting_name_display") or item.get("meeting_name")),
                        "location": normalize_space(item.get("location_nickname") or item.get("canonical_location_text") or item.get("location_text")),
                    },
                )
                info["days"].add(day)

        ordered_keys = [
            item[0]
            for item in sorted(
                recurrence_keys.items(),
                key=lambda entry: (
                    -len(entry[1]["days"]),
                    entry[1]["name"].casefold(),
                    entry[1]["location"].casefold(),
                    entry[0].casefold(),
                ),
            )
        ]
        key_positions = {value: index for index, value in enumerate(ordered_keys)}
        ordered_cells = []
        for day in week_days:
            cell_rows = slot["cells"][day]
            cell_rows.sort(
                key=lambda item: (
                    key_positions.get(
                        "|".join(
                            [
                                normalize_space(item.get("meeting_name_display") or item.get("meeting_name")),
                                normalize_space(item.get("fellowship_display") or item.get("fellowship")),
                                normalize_space(item.get("location_nickname")),
                                normalize_space(item.get("canonical_location_text") or item.get("location_text")),
                                normalize_space(item.get("venue_text")),
                                normalize_space(item.get("zoom_meeting_id")),
                                normalize_space(item.get("format")),
                            ]
                        ),
                        9999,
                    ),
                    normalize_space(item.get("meeting_name_display") or item.get("meeting_name")),
                    normalize_space(item.get("location_nickname") or item.get("canonical_location_text") or item.get("location_text")),
                )
            )
            ordered_cells.append(cell_rows)
        total_card_count = sum(len(cell_rows) for cell_rows in ordered_cells)
        ordered_slots.append(
            {
                "time_label": slot["time_label"],
                "cells": ordered_cells,
                "is_compact": total_card_count <= 1,
            }
        )

    return ordered_slots


def detect_local_ipv4_addresses() -> list[str]:
    addresses: set[str] = set()
    hostname = socket.gethostname()
    try:
        for info in socket.getaddrinfo(hostname, None, family=socket.AF_INET):
            addr = info[4][0]
            if addr and not addr.startswith("127."):
                addresses.add(addr)
    except socket.gaierror:
        pass

    preferred = sorted(addresses, key=lambda addr: (not addr.startswith("10."), not addr.startswith("192.168."), addr))
    return preferred


def admin_password() -> str:
    return os.environ.get("AA_ADMIN_PASSWORD", "fundaskra")


def admin_secret_key(db_path: Path) -> str:
    configured = os.environ.get("AA_SECRET_KEY")
    if configured:
        return configured
    return hashlib.sha256(f"aa:{db_path.resolve()}".encode("utf-8")).hexdigest()


def build_app(db_path: Path) -> Flask:
    app = Flask(__name__)
    app.secret_key = admin_secret_key(db_path)

    def is_admin_authenticated() -> bool:
        return bool(session.get("aa_admin_ok"))

    def admin_redirect_target(default_section: str = "analytics") -> str:
        return f"/admin?admin_section={default_section}"

    def filter_df(df: pd.DataFrame, filters: dict[str, str]) -> pd.DataFrame:
        exact_map = {
            "weekday": "weekday_is",
            "fellowship": "fellowship",
            "format": "format",
            "access_restriction": "access_restriction",
            "canonical_location": "canonical_location_text",
        }

        for filter_name, column in exact_map.items():
            value = filters[filter_name]
            if value:
                if filter_name == "canonical_location":
                    if value == "__zoom__":
                        df = df[df["format"].fillna("") == "Fjarfundur"]
                    else:
                        mask = (
                            df["location_nickname"].fillna("").astype(str) == value
                        ) | (
                            df[column].fillna("").astype(str) == value
                        )
                        df = df[mask]
                else:
                    df = df[df[column].fillna("") == value]

        if not filters["include_church"]:
            df = df[df["source"].fillna("") != "kirkja"]

        if filters["gender_restriction"]:
            gender_filter_map = {
                "Blandaður": {"Blandaður"},
                "Karlar": {"Karlar"},
                "Konur": {"Konur"},
                "Blandaður+Karlar": {"Blandaður", "Karlar"},
                "Blandaður+Konur": {"Blandaður", "Konur"},
            }
            allowed = gender_filter_map.get(filters["gender_restriction"], {filters["gender_restriction"]})
            df = df[df["gender_restriction"].fillna("").isin(allowed)]

        if filters["region"]:
            remote_mask = df["format"].fillna("") == "Fjarfundur"
            capital_mask = capital_region_mask(df)
            if filters["region"] == "capital":
                df = df[remote_mask | capital_mask]
            elif filters["region"] == "rural":
                df = df[remote_mask | ~capital_mask]

        if filters["favorites_only"]:
            df = df[df["is_favorite"]]

        if filters["time_from"]:
            df = df[df["start_time"].fillna("") >= filters["time_from"]]

        if filters["time_to"]:
            df = df[df["start_time"].fillna("") <= filters["time_to"]]

        return df

    def build_single_day_week_views(source_df: pd.DataFrame, filters: dict[str, str]) -> list[dict[str, object]]:
        week_days = [day for day, _ in AA_DAY_PAGES]
        current_weekday = normalize_space(filters.get("weekday"))
        if current_weekday not in week_days:
            return []

        all_days_filters = dict(filters)
        all_days_filters["weekday"] = ""
        filtered = filter_df(source_df, all_days_filters)
        row_dicts = sanitize_rows_for_render(filtered.to_dict(orient="records"))
        views: list[dict[str, object]] = []
        for index, day in enumerate(week_days):
            previous_day = week_days[(index - 1) % len(week_days)]
            next_day = week_days[(index + 1) % len(week_days)]
            views.append(
                {
                    "day": day,
                    "weekday_order": WEEKDAY_ORDER[day],
                    "slots": build_week_view(row_dicts, [day]),
                    "query_string": build_query_string(filters, overrides={"weekday": day, "view": "week"}),
                    "previous_query_string": build_query_string(filters, overrides={"weekday": previous_day, "view": "week"}),
                    "next_query_string": build_query_string(filters, overrides={"weekday": next_day, "view": "week"}),
                }
            )
        return views

    def render_page(*, admin_mode: bool) -> Response:
        df = load_dataframe(db_path)
        favorite_cookie = read_json_cookie(FAVORITES_COOKIE_NAME) or []
        favorite_ids = {
            str(value).strip()
            for value in favorite_cookie
            if isinstance(value, str) and str(value).strip()
        } if isinstance(favorite_cookie, list) else set()
        df["is_favorite"] = df["source_uid"].fillna("").astype(str).isin(favorite_ids)
        client_id = normalize_space(request.cookies.get(CLIENT_COOKIE_NAME))
        user_size_reports = load_user_size_reports(db_path, client_id)
        df["current_size_bin"] = df["source_uid"].fillna("").astype(str).map(user_size_reports).fillna("")
        filters = request_filters()
        admin_section = request.args.get("admin_section", "analytics").strip() or "analytics"
        if admin_section not in {"analytics", "locations", "duplicates", "disagreements", "church"}:
            admin_section = "analytics"
        selected_disagreement_source = normalize_space(request.args.get("disagreement_source", ""))
        if admin_mode:
            filters["view"] = "admin"
        elif filters["view"] in {"locations", "church", "admin"}:
            filters["view"] = "week"
        effective_client_id = client_id or secrets.token_hex(16)
        if not admin_mode:
            log_client_visit(db_path, effective_client_id, request.path, request.query_string.decode("utf-8", errors="ignore"))
        filtered = filter_df(df, filters)
        row_dicts = sanitize_rows_for_render(filtered.to_dict(orient="records"))
        displayed_week_days = [filters["weekday"]] if filters["weekday"] in [day for day, _ in AA_DAY_PAGES] else [day for day, _ in AA_DAY_PAGES]
        displayed_week_day_orders = [WEEKDAY_ORDER[day] for day in displayed_week_days]
        single_day_week_views = build_single_day_week_views(df, filters) if filters["view"] == "week" and len(displayed_week_days) == 1 else []
        scraped_at = df["scraped_at_utc"].iloc[0] if not df.empty else "N/A"
        options = build_filter_options(df)
        source_counts = (
            filtered.groupby("source")
            .size()
            .sort_index()
            .reset_index(name="count")
            .itertuples(index=False, name=None)
        )
        source_disagreement_options = build_source_disagreement_options(df) if admin_mode else []
        valid_disagreement_sources = {
            normalize_space(item.get("source"))
            for item in source_disagreement_options
        }
        if selected_disagreement_source not in valid_disagreement_sources:
            selected_disagreement_source = ""
        csv_query_string = build_query_string(filters, exclude={"view"})
        list_query_string = build_query_string(filters, overrides={"view": "list"})
        week_query_string = build_query_string(filters, overrides={"view": "week"})
        previous_weekday_query_string, next_weekday_query_string = build_adjacent_weekday_queries(filters)
        admin_query_string = build_query_string(filters, overrides={"admin_section": admin_section}, exclude={"view"})
        locations_query_string = build_query_string(filters, overrides={"admin_section": "locations"}, exclude={"view"})
        duplicates_query_string = build_query_string(filters, overrides={"admin_section": "duplicates"}, exclude={"view"})
        disagreements_query_string = build_query_string(
            filters,
            overrides={"admin_section": "disagreements", "disagreement_source": selected_disagreement_source},
            exclude={"view"},
        )
        disagreements_csv_query_string = urlencode(
            {"disagreement_source": selected_disagreement_source}
            if selected_disagreement_source
            else {}
        )
        church_query_string = build_query_string(filters, overrides={"admin_section": "church"}, exclude={"view"})
        current_query_string = request.query_string.decode("utf-8", errors="ignore").strip()
        location_rows = build_location_review_rows(df[df["source"].fillna("") != "kirkja"], "")
        location_clusters = build_location_clusters(location_rows)
        church_location_icon = load_location_metadata(db_path, CHURCH_LOCATION_ICON_KEY) if admin_mode and admin_section == "locations" else {}
        duplicate_review_rows = build_duplicate_review_rows(df, max_pairs=160) if admin_mode and admin_section == "duplicates" else []
        duplicate_merge_rows = load_meeting_merges(db_path) if admin_mode and admin_section == "duplicates" else []
        source_disagreement_rows = (
            build_source_disagreement_rows(df, selected_source=selected_disagreement_source, max_groups=240)
            if admin_mode and admin_section == "disagreements"
            else []
        )
        manual_events = load_manual_events(db_path)
        visit_summary_rows, recent_visit_rows, visit_totals = load_visit_summary(db_path)
        mapped_location_rows = [
            row
            for row in location_rows
            if int(row.get("has_location_mapping", 0))
            or normalize_space(row.get("location_nickname"))
            or normalize_space(row.get("location_icon_emoji"))
            or normalize_space(row.get("location_icon_bg_color"))
        ]
        edit_event_id = request.args.get("edit_event_id", "").strip()
        church_edit_event = next(
            (item for item in manual_events if str(item.get("event_id")) == edit_event_id),
            None,
        ) if edit_event_id else None
        response = Response(
            app.jinja_env.from_string(CARD_TEMPLATE).render(
                rows=row_dicts,
                week_slots=build_week_view(row_dicts, displayed_week_days),
                week_days=displayed_week_days,
                week_day_orders=displayed_week_day_orders,
                single_day_week_views=single_day_week_views,
                week_day_count=len(displayed_week_days),
                total_count=len(filtered),
                scraped_at=format_scraped_at_short(scraped_at),
                options=options,
                filters=filters,
                source_counts=list(source_counts),
                csv_query_string=csv_query_string,
                list_query_string=list_query_string,
                week_query_string=week_query_string,
                previous_weekday_query_string=previous_weekday_query_string,
                next_weekday_query_string=next_weekday_query_string,
                admin_query_string=admin_query_string,
                locations_query_string=locations_query_string,
                duplicates_query_string=duplicates_query_string,
                disagreements_query_string=disagreements_query_string,
                disagreements_csv_query_string=disagreements_csv_query_string,
                church_query_string=church_query_string,
                current_query_string=current_query_string,
                default_weekday=current_iceland_weekday(),
                filters_cookie_name=FILTERS_COOKIE_NAME,
                favorites_cookie_name=FAVORITES_COOKIE_NAME,
                client_cookie_name=CLIENT_COOKIE_NAME,
                location_rows=location_rows,
                location_clusters=location_clusters,
                church_location_icon=church_location_icon,
                church_location_icon_key=CHURCH_LOCATION_ICON_KEY,
                duplicate_review_rows=duplicate_review_rows,
                duplicate_merge_rows=duplicate_merge_rows,
                source_disagreement_options=source_disagreement_options,
                selected_disagreement_source=selected_disagreement_source,
                source_disagreement_rows=source_disagreement_rows,
                mapped_location_rows=mapped_location_rows,
                manual_events=manual_events,
                visit_summary_rows=visit_summary_rows,
                recent_visit_rows=recent_visit_rows,
                visit_totals=visit_totals,
                church_edit_event=church_edit_event,
                admin_authenticated=is_admin_authenticated(),
                admin_section=admin_section,
            ),
            mimetype="text/html",
        )
        if not client_id:
            response.set_cookie(CLIENT_COOKIE_NAME, effective_client_id, max_age=365 * 24 * 60 * 60, samesite="Lax")
        return response

    @app.get("/")
    def index():
        return render_page(admin_mode=False)

    @app.get("/admin")
    def admin_index():
        return render_page(admin_mode=True)

    @app.get("/admin/disagreements.csv")
    def admin_disagreements_csv():
        if not is_admin_authenticated():
            return redirect(admin_redirect_target("disagreements"))
        selected_source = normalize_space(request.args.get("disagreement_source", ""))
        df = load_dataframe(db_path)
        valid_sources = {
            normalize_space(item.get("source"))
            for item in build_source_disagreement_options(df)
        }
        if selected_source not in valid_sources:
            selected_source = ""
        rows = build_source_disagreement_rows(df, selected_source=selected_source, max_groups=5000)
        output = io.StringIO()
        fieldnames = [
            "report_source",
            "comparison_source",
            "weekday",
            "time",
            "report_meeting",
            "comparison_meeting",
            "field",
            "report_value",
            "comparison_value",
            "match_score",
            "match_reasons",
            "report_source_url",
            "comparison_source_url",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for item in rows:
            target = item.get("target", {})
            comparison = item.get("comparison", {})
            for diff in item.get("disagreements", []):
                writer.writerow(
                    {
                        "report_source": target.get("source", ""),
                        "comparison_source": comparison.get("source", ""),
                        "weekday": target.get("weekday_is", ""),
                        "time": target.get("time_display", ""),
                        "report_meeting": target.get("meeting_name_display", ""),
                        "comparison_meeting": comparison.get("meeting_name_display", ""),
                        "field": diff.get("label", ""),
                        "report_value": diff.get("target_value", ""),
                        "comparison_value": diff.get("comparison_value", ""),
                        "match_score": item.get("score", ""),
                        "match_reasons": " | ".join(item.get("match_reasons", [])),
                        "report_source_url": target.get("source_page_url", ""),
                        "comparison_source_url": comparison.get("source_page_url", ""),
                    }
                )
        filename_source = selected_source.replace(".", "-") if selected_source else "all"
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=source_disagreements_{filename_source}.csv"},
        )

    @app.get("/csv")
    def csv_download():
        df = load_dataframe(db_path)
        favorite_cookie = read_json_cookie(FAVORITES_COOKIE_NAME) or []
        favorite_ids = {
            str(value).strip()
            for value in favorite_cookie
            if isinstance(value, str) and str(value).strip()
        } if isinstance(favorite_cookie, list) else set()
        df["is_favorite"] = df["source_uid"].fillna("").astype(str).isin(favorite_ids)
        filtered = filter_df(df, request_filters())
        csv_text = filtered.to_csv(index=False)
        return Response(
            csv_text,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=meetings_filtered.csv"},
        )

    @app.get("/favorites.ics")
    def favorites_calendar_download():
        subscription_token = request.args.get("token", "").strip()
        favorite_ids: list[str]
        calendar_name = "AA uppáhaldsfundir"
        headers = {"Cache-Control": "no-store"}
        if subscription_token:
            subscription = load_favorite_calendar_subscription(db_path, subscription_token)
            if subscription is None:
                return Response("Calendar fannst ekki", status=404, mimetype="text/plain")
            favorite_ids = list(subscription.get("favorite_ids", []))
            calendar_url = request.url_root.rstrip("/") + f"/favorites.ics?{urlencode({'token': subscription_token})}"
            headers["Content-Disposition"] = "inline; filename=aa-favorites-feed.ics"
        else:
            favorite_ids = favorite_ids_from_cookie()
            calendar_url = None
            headers["Content-Disposition"] = "attachment; filename=aa-favorites.ics"
            headers["Vary"] = "Cookie"
        rows = load_favorite_calendar_rows(db_path, favorite_ids)
        ical_text = build_favorites_calendar_ics(rows, calendar_name=calendar_name, calendar_url=calendar_url)
        return Response(ical_text, content_type="text/calendar; charset=utf-8", headers=headers)

    @app.get("/favorites-calendar-url")
    def favorites_calendar_url():
        favorite_ids = favorite_ids_from_cookie()
        if not favorite_ids:
            return Response(
                json.dumps({"error": "no-favorites"}, ensure_ascii=False),
                status=400,
                mimetype="application/json",
                headers={"Cache-Control": "no-store", "Vary": "Cookie"},
            )
        client_id = normalize_space(request.cookies.get(CLIENT_COOKIE_NAME))
        effective_client_id = client_id or secrets.token_hex(16)
        subscription_token = upsert_favorite_calendar_subscription(db_path, effective_client_id, favorite_ids)
        calendar_url = request.url_root.rstrip("/") + f"/favorites.ics?{urlencode({'token': subscription_token})}"
        response = Response(
            json.dumps({"url": calendar_url, "count": len(favorite_ids)}, ensure_ascii=False),
            mimetype="application/json",
            headers={"Cache-Control": "no-store", "Vary": "Cookie"},
        )
        if not client_id:
            response.set_cookie(CLIENT_COOKIE_NAME, effective_client_id, max_age=365 * 24 * 60 * 60, samesite="Lax")
        return response

    @app.post("/admin/login")
    def admin_login():
        password = request.form.get("password", "")
        redirect_query = request.form.get("redirect_query", "").strip()
        if hmac.compare_digest(password, admin_password()):
            session["aa_admin_ok"] = True
        target = "/admin" + (f"?{redirect_query}" if redirect_query else "?admin_section=analytics")
        return redirect(target)

    @app.post("/admin/logout")
    def admin_logout():
        session.pop("aa_admin_ok", None)
        return redirect(admin_redirect_target("locations"))

    @app.post("/size-report")
    def size_report():
        client_id = normalize_space(request.cookies.get(CLIENT_COOKIE_NAME))
        save_meeting_size_report(
            db_path,
            request.form.get("source_uid", ""),
            client_id,
            request.form.get("size_bin", ""),
        )
        redirect_query = request.form.get("redirect_query", "").strip()
        target = "/" + (f"?{redirect_query}" if redirect_query else "")
        return redirect(target)

    @app.post("/locations/map")
    def location_map():
        if not is_admin_authenticated():
            return redirect(admin_redirect_target("locations"))
        alias_location_text = request.form.get("alias_location_text", "").strip()
        canonical_location_text = request.form.get("canonical_location_text", "").strip()
        redirect_query = request.form.get("redirect_query", "").strip()
        if alias_location_text:
            save_location_mapping(db_path, alias_location_text, canonical_location_text)
        target = "/admin" + (f"?{redirect_query}" if redirect_query else "?admin_section=locations")
        return redirect(target)

    @app.post("/locations/nickname")
    def location_nickname():
        if not is_admin_authenticated():
            return redirect(admin_redirect_target("locations"))
        canonical_location_text = request.form.get("canonical_location_text", "").strip()
        nickname = request.form.get("nickname", "").strip()
        icon_emoji = request.form.get("icon_emoji", "").strip()
        icon_bg_color = request.form.get("icon_bg_color", "").strip() if request.form.get("icon_has_bg") == "1" else ""
        redirect_query = request.form.get("redirect_query", "").strip()
        if canonical_location_text:
            save_location_nickname(db_path, canonical_location_text, nickname, icon_emoji, icon_bg_color)
        target = "/admin" + (f"?{redirect_query}" if redirect_query else "?admin_section=locations")
        return redirect(target)

    @app.post("/duplicates/merge")
    def duplicate_merge():
        if not is_admin_authenticated():
            return redirect(admin_redirect_target("duplicates"))
        save_meeting_merge(
            db_path,
            canonical_source_uid=request.form.get("canonical_source_uid", ""),
            duplicate_source_uid=request.form.get("duplicate_source_uid", ""),
        )
        redirect_query = request.form.get("redirect_query", "").strip()
        target = "/admin" + (f"?{redirect_query}" if redirect_query else "?admin_section=duplicates")
        return redirect(target)

    @app.post("/duplicates/unmerge")
    def duplicate_unmerge():
        if not is_admin_authenticated():
            return redirect(admin_redirect_target("duplicates"))
        delete_meeting_merge(db_path, request.form.get("duplicate_source_uid", ""))
        redirect_query = request.form.get("redirect_query", "").strip()
        target = "/admin" + (f"?{redirect_query}" if redirect_query else "?admin_section=duplicates")
        return redirect(target)

    @app.post("/church/save")
    def church_save():
        if not is_admin_authenticated():
            return redirect(admin_redirect_target("church"))
        save_manual_event(
            db_path,
            event_id=request.form.get("event_id"),
            event_kind="church",
            title=request.form.get("title", ""),
            weekday_is=request.form.get("weekday_is", ""),
            start_time=request.form.get("start_time", ""),
            end_time=request.form.get("end_time", ""),
            subtitle=request.form.get("subtitle", ""),
            location_text=request.form.get("location_text", ""),
            venue_text=request.form.get("venue_text", ""),
            notes=request.form.get("notes", ""),
            source_page_url=request.form.get("source_page_url", ""),
        )
        redirect_query = request.form.get("redirect_query", "").strip()
        target = "/admin" + (f"?{redirect_query}" if redirect_query else "?admin_section=church")
        return redirect(target)

    @app.post("/church/delete")
    def church_delete():
        if not is_admin_authenticated():
            return redirect(admin_redirect_target("church"))
        delete_manual_event(db_path, request.form.get("event_id"))
        redirect_query = request.form.get("redirect_query", "").strip()
        target = "/admin" + (f"?{redirect_query}" if redirect_query else "?admin_section=church")
        return redirect(target)

    return app
