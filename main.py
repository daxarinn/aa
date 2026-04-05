from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode
import socket
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, Response, redirect, request


AA_ALL_MEETINGS_URL = "https://aa.is/aa-fundir/allir-fundir"
AA_ALL_MEETINGS_ALL_URL = f"{AA_ALL_MEETINGS_URL}?limit=0"

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
    updated_at_utc TEXT NOT NULL
);
"""


CARD_TEMPLATE = """
<!doctype html>
<html lang="is">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AA fundaskrá snapshot</title>
  <style>
    :root {
      --card: #fffdf8;
      --ink: #1e293b;
      --muted: #64748b;
      --accent: #0f766e;
      --border: #d9d3c7;
      --pill: #e8f4f1;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff7d6 0, transparent 30%),
        linear-gradient(180deg, #f3eee4 0%, #f7f4ee 100%);
    }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 20px 14px 28px; }
    .hero {
      background: rgba(255,255,255,0.75);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      backdrop-filter: blur(8px);
      box-shadow: 0 16px 40px rgba(15, 23, 42, 0.08);
    }
    h1 { font-size: clamp(1.8rem, 4vw, 3rem); margin: 0 0 8px; line-height: 1.05; }
    .meta { color: var(--muted); font-size: 0.95rem; margin: 0; }
    .filters {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
      margin-top: 14px;
    }
    .filter-field {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .filter-field label {
      font-size: 0.82rem;
      color: var(--muted);
    }
    .filters input, .filters select, .filters button, .filters a {
      width: 100%;
      border-radius: 12px;
      border: 1px solid var(--border);
      padding: 10px 12px;
      font: inherit;
      text-decoration: none;
      color: inherit;
      background: white;
    }
    .filters button { background: var(--accent); color: white; border-color: var(--accent); cursor: pointer; }
    .filters a { display: inline-flex; align-items: center; justify-content: center; }
    .filter-actions {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 10px;
      margin-top: 10px;
    }
    .view-switch {
      display: inline-flex;
      gap: 8px;
      margin-top: 14px;
      flex-wrap: wrap;
    }
    .view-switch a {
      text-decoration: none;
      border: 1px solid var(--border);
      color: var(--ink);
      background: white;
      padding: 8px 12px;
      border-radius: 999px;
      font-size: 0.92rem;
    }
    .view-switch a.active {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }
    .summary { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .summary span {
      padding: 7px 10px;
      border-radius: 999px;
      background: var(--pill);
      color: #0f172a;
      font-size: 0.9rem;
    }
    .grid { display: grid; gap: 12px; margin-top: 16px; }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 15px;
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
    }
    .card.is-remote,
    .slot-card.is-remote {
      background: #dff3e2;
      border-color: #86bf92;
      box-shadow: inset 2px 0 0 #2f8f46, 0 12px 28px rgba(15, 23, 42, 0.05);
    }
    .card.is-remote.is-women,
    .slot-card.is-remote.is-women {
      background: linear-gradient(135deg, #ffd7e3 0 50%, #dff3e2 50% 100%);
      border-color: #c5b5ba;
      box-shadow: inset 2px 0 0 #c83d6f, inset -2px 0 0 #2f8f46, 0 12px 28px rgba(15, 23, 42, 0.05);
    }
    .card.is-remote.is-men,
    .slot-card.is-remote.is-men {
      background: linear-gradient(135deg, #d9ebff 0 50%, #dff3e2 50% 100%);
      border-color: #b7c7c1;
      box-shadow: inset 2px 0 0 #2f72c8, inset -2px 0 0 #2f8f46, 0 12px 28px rgba(15, 23, 42, 0.05);
    }
    .card.is-women,
    .slot-card.is-women {
      background: #ffdbe7;
      border-color: #de8daa;
      box-shadow: inset 2px 0 0 #c83d6f, 0 12px 28px rgba(15, 23, 42, 0.05);
    }
    .card.is-men,
    .slot-card.is-men {
      background: #dcecff;
      border-color: #8fb2de;
      box-shadow: inset 2px 0 0 #2f72c8, 0 12px 28px rgba(15, 23, 42, 0.05);
    }
    .topline { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 8px; }
    .time { font-weight: 700; font-size: 1.05rem; color: var(--accent); }
    .pill {
      font-size: 0.82rem;
      color: #0f172a;
      background: #eef6f5;
      border: 1px solid #d7ebe7;
      padding: 5px 8px;
      border-radius: 999px;
    }
    h2 { margin: 0 0 6px; font-size: 1.2rem; }
    .line { margin: 4px 0; color: var(--muted); font-size: 0.96rem; }
    .line strong { color: var(--ink); }
    .zoom { margin-top: 8px; padding-top: 8px; border-top: 1px dashed var(--border); font-size: 0.92rem; }
    .zoom a { color: var(--accent); }
    .provenance {
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px dashed var(--border);
      font-size: 0.86rem;
      color: var(--muted);
    }
    .provenance a,
    .slot-provenance a {
      color: var(--accent);
    }
    .empty-state {
      margin-top: 16px;
      background: rgba(255,255,255,0.7);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      color: var(--muted);
    }
    .week-shell {
      margin-top: 16px;
      overflow-x: auto;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: rgba(255,255,255,0.55);
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
    }
    .week-board {
      min-width: 980px;
      display: grid;
      grid-template-columns: 88px repeat(7, minmax(120px, 1fr));
      position: relative;
    }
    .week-board.single-day {
      min-width: 0;
      grid-template-columns: 88px minmax(0, 1fr);
    }
    .week-head {
      position: sticky;
      top: 0;
      z-index: 2;
      background: #f8f5ee;
      border-bottom: 1px solid var(--border);
      padding: 12px 10px;
      font-size: 0.88rem;
      font-weight: 700;
    }
    .time-cell {
      padding: 12px 10px;
      border-right: 1px solid var(--border);
      border-bottom: 1px solid var(--border);
      color: var(--accent);
      font-weight: 700;
      font-size: 0.9rem;
      background: rgba(248,245,238,0.75);
    }
    .week-cell {
      min-height: 92px;
      padding: 8px;
      border-bottom: 1px solid var(--border);
      border-right: 1px solid var(--border);
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .slot-card {
      background: white;
      border: 1px solid #e7e0d2;
      border-radius: 14px;
      padding: 8px 10px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.04);
      position: relative;
      cursor: pointer;
      outline: none;
    }
    .slot-title {
      margin: 0;
      font-size: 0.9rem;
      line-height: 1.2;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .slot-summary {
      margin: 2px 0 0;
      color: var(--muted);
      font-size: 0.78rem;
      line-height: 1.25;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .slot-meta {
      margin: 0;
      color: var(--muted);
      font-size: 0.82rem;
      line-height: 1.35;
    }
    .slot-tooltip-pills {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 8px;
    }
    .slot-tooltip-pills span {
      font-size: 0.75rem;
      background: #eef6f5;
      border: 1px solid #d7ebe7;
      border-radius: 999px;
      padding: 4px 7px;
    }
    .slot-link {
      display: inline-block;
      margin-top: 6px;
      color: var(--accent);
      font-size: 0.82rem;
      text-decoration: none;
    }
    .slot-provenance {
      margin-top: 6px;
      font-size: 0.76rem;
      color: var(--muted);
      line-height: 1.35;
    }
    .slot-tooltip {
      display: none;
      position: absolute;
      left: 10px;
      top: calc(100% + 8px);
      width: min(290px, calc(100vw - 48px));
      background: rgba(255,255,255,0.98);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 20px 40px rgba(15, 23, 42, 0.18);
      z-index: 20;
      cursor: default;
    }
    .slot-card:hover .slot-tooltip,
    .slot-card:focus .slot-tooltip,
    .slot-card:focus-within .slot-tooltip {
      display: block;
    }
    .slot-tooltip::before {
      content: "";
      position: absolute;
      top: -8px;
      left: 18px;
      border-left: 8px solid transparent;
      border-right: 8px solid transparent;
      border-bottom: 8px solid rgba(255,255,255,0.98);
    }
    .now-line {
      position: absolute;
      left: 88px;
      right: 0;
      height: 2px;
      background: #d62828;
      box-shadow: 0 0 0 1px rgba(255,255,255,0.35);
      z-index: 12;
      pointer-events: none;
      display: none;
    }
    .now-line.visible {
      display: block;
    }
    .now-line-label {
      position: absolute;
      top: -12px;
      right: 10px;
      background: #d62828;
      color: white;
      font-size: 0.72rem;
      line-height: 1;
      padding: 4px 7px;
      border-radius: 999px;
      box-shadow: 0 6px 14px rgba(214, 40, 40, 0.22);
    }
    .mapping-section {
      margin-top: 16px;
      display: grid;
      gap: 14px;
    }
    .mapping-card {
      background: rgba(255,255,255,0.82);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
    }
    .mapping-card h3 {
      margin: 0 0 10px;
      font-size: 1.05rem;
    }
    .mapping-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
      font-size: 0.92rem;
    }
    .mapping-table th,
    .mapping-table td {
      text-align: left;
      padding: 10px 8px;
      border-top: 1px solid #ece5d8;
      vertical-align: top;
    }
    .mapping-form {
      display: grid;
      grid-template-columns: minmax(180px, 1fr) 120px;
      gap: 8px;
      align-items: start;
    }
    .mapping-form input,
    .mapping-form button {
      border-radius: 10px;
      border: 1px solid var(--border);
      padding: 9px 10px;
      font: inherit;
      background: white;
    }
    .mapping-form button {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
      cursor: pointer;
    }
    .mapping-meta {
      color: var(--muted);
      font-size: 0.84rem;
      line-height: 1.4;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>AA fundaskrá snapshot</h1>
      <p class="meta">Fundir af aa.is og fjarfundir.org. Síðast sótt: {{ scraped_at }}</p>
      <form class="filters" method="get">
        <input type="hidden" name="view" value="{{ filters["view"] }}">
        <div class="filter-field">
          <label for="weekday">Vikudagur</label>
          <select id="weekday" name="weekday">
            <option value="">Allir dagar</option>
            {% for value in options["weekday_is"] %}
            <option value="{{ value }}" {% if filters["weekday"] == value %}selected{% endif %}>{{ value }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="filter-field">
          <label for="fellowship">Félag</label>
          <select id="fellowship" name="fellowship">
            <option value="">Öll félög</option>
            {% for value in options["fellowship"] %}
            <option value="{{ value }}" {% if filters["fellowship"] == value %}selected{% endif %}>{{ value }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="filter-field">
          <label for="format">Format</label>
          <select id="format" name="format">
            <option value="">Öll format</option>
            {% for value in options["format"] %}
            <option value="{{ value }}" {% if filters["format"] == value %}selected{% endif %}>{{ value }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="filter-field">
          <label for="gender_restriction">Kyn</label>
          <select id="gender_restriction" name="gender_restriction">
            <option value="">Öll kynskilyrði</option>
            {% for item in options["gender_filter_options"] %}
            <option value="{{ item["value"] }}" {% if filters["gender_restriction"] == item["value"] %}selected{% endif %}>{{ item["label"] }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="filter-field">
          <label for="access_restriction">Aðgangur</label>
          <select id="access_restriction" name="access_restriction">
            <option value="">Allur aðgangur</option>
            {% for value in options["access_restriction"] %}
            <option value="{{ value }}" {% if filters["access_restriction"] == value %}selected{% endif %}>{{ value }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="filter-field">
          <label for="canonical_location">Staður með gælunafni</label>
          <select id="canonical_location" name="canonical_location">
            <option value="">Allir slíkir staðir</option>
            {% for item in options["nickname_locations"] %}
            <option value="{{ item["value"] }}" {% if filters["canonical_location"] == item["value"] %}selected{% endif %}>{{ item["label"] }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="filter-field">
          <label for="time_from">Frá tíma</label>
          <input id="time_from" type="time" name="time_from" value="{{ filters["time_from"] }}">
        </div>
        <div class="filter-field">
          <label for="time_to">Til tíma</label>
          <input id="time_to" type="time" name="time_to" value="{{ filters["time_to"] }}">
        </div>
        <div class="filter-actions">
          <button type="submit">Sía</button>
          <a href="/csv?{{ csv_query_string }}">CSV</a>
          <a href="/">Hreinsa</a>
        </div>
      </form>
      <div class="summary">
        <span>{{ total_count }} fundir</span>
        {% for item in source_counts %}
        <span>{{ item[0] }}: {{ item[1] }}</span>
        {% endfor %}
      </div>
      <div class="view-switch">
        <a href="/?{{ list_query_string }}" class="{% if filters["view"] != "week" %}active{% endif %}">Línuleg sýn</a>
        <a href="/?{{ week_query_string }}" class="{% if filters["view"] == "week" %}active{% endif %}">Vikusýn</a>
        <a href="/?{{ locations_query_string }}" class="{% if filters["view"] == "locations" %}active{% endif %}">Staðamöppun</a>
      </div>
    </section>
    {% if total_count == 0 %}
    <section class="empty-state">
      Engir fundir pössuðu við valdar síur.
    </section>
    {% elif filters["view"] == "locations" %}
    <section class="mapping-section">
      <article class="mapping-card">
        <h3>Tillögur að samruna</h3>
        <p class="mapping-meta">Hér sjást staðsetningar sem líta út fyrir að vera sama heimilisfang með mismunandi rithætti. Vistaðu canonical heiti fyrir hvert alias. Ef canonical er tómt eða sama og alias, er mapping fjarlægt.</p>
        {% if location_clusters %}
        {% for cluster in location_clusters %}
        <table class="mapping-table">
          <thead>
            <tr>
              <th colspan="5">Lykill: {{ cluster["normalized_key"] }}</th>
            </tr>
            <tr>
              <th>Alias</th>
              <th>Fjöldi</th>
              <th>Dæmi</th>
              <th>Canonical</th>
              <th>Gælunafn</th>
            </tr>
          </thead>
          <tbody>
            {% for row in cluster["rows"] %}
            <tr>
              <td>{{ row["location_text"] }}</td>
              <td>{{ row["meeting_count"] }}</td>
              <td class="mapping-meta">
                {% if row["venues"] %}{{ row["venues"]|join(" | ") }}<br>{% endif %}
                {% if row["names"] %}{{ row["names"]|join(" | ") }}{% endif %}
              </td>
              <td>
                <form class="mapping-form" method="post" action="/locations/map">
                  <input type="hidden" name="alias_location_text" value="{{ row["location_text"] }}">
                  <input type="hidden" name="redirect_query" value="{{ locations_query_string }}">
                  <input type="text" name="canonical_location_text" value="{{ row["canonical_location_text"] or cluster["suggested_canonical"] }}">
                  <button type="submit">Vista</button>
                </form>
              </td>
              <td>
                <form class="mapping-form" method="post" action="/locations/nickname">
                  <input type="hidden" name="canonical_location_text" value="{{ row["canonical_location_text"] or cluster["suggested_canonical"] }}">
                  <input type="hidden" name="redirect_query" value="{{ locations_query_string }}">
                  <input type="text" name="nickname" value="{{ row["location_nickname"] }}" placeholder="Gula húsið">
                  <button type="submit">Vista</button>
                </form>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% endfor %}
        {% else %}
        <p class="mapping-meta">Engar augljósar tillögur fundust með núverandi síum.</p>
        {% endif %}
      </article>

      <article class="mapping-card">
        <h3>Allar staðsetningar</h3>
        <table class="mapping-table">
          <thead>
            <tr>
              <th>Alias</th>
              <th>Fjöldi</th>
              <th>Núverandi canonical</th>
              <th>Vista canonical</th>
              <th>Gælunafn</th>
            </tr>
          </thead>
          <tbody>
            {% for row in location_rows %}
            <tr>
              <td>
                {{ row["location_text"] }}
                <div class="mapping-meta">{{ row["normalized_key"] }}</div>
              </td>
              <td>{{ row["meeting_count"] }}</td>
              <td class="mapping-meta">
                {{ row["canonical_location_text"] }}
                {% if row["venues"] %}<br>{{ row["venues"]|join(" | ") }}{% endif %}
              </td>
              <td>
                <form class="mapping-form" method="post" action="/locations/map">
                  <input type="hidden" name="alias_location_text" value="{{ row["location_text"] }}">
                  <input type="hidden" name="redirect_query" value="{{ locations_query_string }}">
                  <input type="text" name="canonical_location_text" value="{{ row["canonical_location_text"] }}">
                  <button type="submit">Vista</button>
                </form>
              </td>
              <td>
                <form class="mapping-form" method="post" action="/locations/nickname">
                  <input type="hidden" name="canonical_location_text" value="{{ row["canonical_location_text"] }}">
                  <input type="hidden" name="redirect_query" value="{{ locations_query_string }}">
                  <input type="text" name="nickname" value="{{ row["location_nickname"] }}" placeholder="Holtagarðar">
                  <button type="submit">Vista</button>
                </form>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </article>
    </section>
    {% elif filters["view"] == "week" %}
    <section class="week-shell">
      <div class="week-board{% if week_day_count == 1 %} single-day{% endif %}" data-week-days="{{ week_days|join('|') }}" data-weekday-orders="{{ week_day_orders|join('|') }}">
        <div class="now-line" id="nowLine">
          <span class="now-line-label" id="nowLineLabel">Núna</span>
        </div>
        <div class="week-head">Tími</div>
        {% for day in week_days %}
        <div class="week-head">{{ day }}</div>
        {% endfor %}
        {% for slot in week_slots %}
        <div class="time-cell" data-time-label="{{ slot["time_label"] }}">{{ slot["time_label"] }}</div>
        {% for cell in slot["cells"] %}
        <div class="week-cell">
          {% for row in cell %}
          <article class="slot-card{% if row["format"] == "Fjarfundur" %} is-remote{% endif %}{% if row["gender_restriction"] == "Konur" %} is-women{% elif row["gender_restriction"] == "Karlar" %} is-men{% endif %}" tabindex="0">
            <h3 class="slot-title">{{ row["meeting_name"] or "Ónefndur fundur" }}</h3>
            <p class="slot-summary">
              {% if row["location_nickname"] %}{{ row["location_nickname"] }}
              {% elif row["location_text"] %}{{ row["location_text"] }}
              {% else %}{{ row["fellowship_display"] }}{% endif %}
            </p>
            <div class="slot-tooltip">
              {% if row["subtitle"] %}<p class="slot-meta">{{ row["subtitle"] }}</p>{% endif %}
              {% if row["location_nickname"] %}<p class="slot-meta"><strong>{{ row["location_nickname"] }}</strong></p>{% endif %}
              {% if row["location_text"] %}<p class="slot-meta">{{ row["location_text"] }}</p>{% endif %}
              {% if row["venue_text"] %}<p class="slot-meta">{{ row["venue_text"] }}</p>{% endif %}
              <div class="slot-tooltip-pills">
                <span>{{ row["source"] }}</span>
                <span>{{ row["fellowship_display"] }}</span>
                {% if row["format"] %}<span>{{ row["format"] }}</span>{% endif %}
                {% if row["gender_restriction"] %}<span>{{ row["gender_restriction"] }}</span>{% endif %}
                {% if row["access_restriction"] %}<span>{{ row["access_restriction"] }}</span>{% endif %}
              </div>
              {% if row["zoom_url"] %}<a class="slot-link" href="{{ row["zoom_url"] }}" target="_blank" rel="noreferrer">Opna fund</a>{% endif %}
              <div class="slot-provenance">
                <a href="{{ row["source_page_url"] }}" target="_blank" rel="noreferrer">Upprunasíða</a>
                {% if row["source_locator"] %}<span> · {{ row["source_locator"] }}</span>{% endif %}
              </div>
              {% if row["source_excerpt"] %}<p class="slot-provenance">{{ row["source_excerpt"] }}</p>{% endif %}
            </div>
          </article>
          {% endfor %}
        </div>
        {% endfor %}
        {% endfor %}
      </div>
    </section>
    {% else %}
    <section class="grid">
      {% for row in rows %}
      <article class="card{% if row["format"] == "Fjarfundur" %} is-remote{% endif %}{% if row["gender_restriction"] == "Konur" %} is-women{% elif row["gender_restriction"] == "Karlar" %} is-men{% endif %}">
        <div class="topline">
          <span class="time">{{ row["weekday_is"] }} {{ row["time_display"] }}</span>
          <span class="pill">{{ row["source"] }}</span>
          <span class="pill">{{ row["fellowship_display"] }}</span>
          {% if row["gender_restriction"] %}<span class="pill">{{ row["gender_restriction"] }}</span>{% endif %}
          {% if row["access_restriction"] %}<span class="pill">{{ row["access_restriction"] }}</span>{% endif %}
          {% if row["format"] %}<span class="pill">{{ row["format"] }}</span>{% endif %}
        </div>
        <h2>{{ row["meeting_name"] or "Ónefndur fundur" }}</h2>
        {% if row["subtitle"] %}<p class="line"><strong>Undirlína:</strong> {{ row["subtitle"] }}</p>{% endif %}
        {% if row["location_nickname"] %}<p class="line"><strong>Gælunafn:</strong> {{ row["location_nickname"] }}</p>{% endif %}
        {% if row["location_text"] %}<p class="line"><strong>Staðsetning:</strong> {{ row["location_text"] }}</p>{% endif %}
        {% if row["venue_text"] %}<p class="line"><strong>Staður:</strong> {{ row["venue_text"] }}</p>{% endif %}
        {% if row["recurrence_hint"] %}<p class="line"><strong>Regluleiki:</strong> {{ row["recurrence_hint"] }}</p>{% endif %}
        {% if row["notes"] %}<p class="line"><strong>Glósur:</strong> {{ row["notes"] }}</p>{% endif %}
        {% if row["zoom_meeting_id"] or row["zoom_url"] %}
        <div class="zoom">
          {% if row["zoom_meeting_id"] %}<div><strong>Zoom ID:</strong> {{ row["zoom_meeting_id"] }}</div>{% endif %}
          {% if row["zoom_passcode"] %}<div><strong>Passcode:</strong> {{ row["zoom_passcode"] }}</div>{% endif %}
          {% if row["zoom_url"] %}<div><a href="{{ row["zoom_url"] }}" target="_blank" rel="noreferrer">Opna fund</a></div>{% endif %}
        </div>
        {% endif %}
        <div class="provenance">
          <div><strong>Uppruni:</strong> <a href="{{ row["source_page_url"] }}" target="_blank" rel="noreferrer">Opna upprunasíðu</a>{% if row["source_locator"] %} · {{ row["source_locator"] }}{% endif %}</div>
          {% if row["source_excerpt"] %}<div><strong>Raw:</strong> {{ row["source_excerpt"] }}</div>{% endif %}
        </div>
      </article>
      {% endfor %}
    </section>
    {% endif %}
  </div>
</body>
<script>
(function() {
  const board = document.querySelector('.week-board');
  const line = document.getElementById('nowLine');
  const label = document.getElementById('nowLineLabel');
  if (!board || !line || !label) return;

  const parseMinutes = (value) => {
    const match = /^(\\d{1,2}):(\\d{2})$/.exec((value || '').trim());
    if (!match) return null;
    return Number(match[1]) * 60 + Number(match[2]);
  };

  const visibleDayOrders = (board.dataset.weekdayOrders || '')
    .split('|')
    .map((item) => Number(item.trim()))
    .filter((value) => !Number.isNaN(value));

  const formatter = new Intl.DateTimeFormat('is-IS', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'Atlantic/Reykjavik'
  });
  const weekdayFormatter = new Intl.DateTimeFormat('en-US', {
    weekday: 'short',
    timeZone: 'Atlantic/Reykjavik'
  });
  const weekdayMap = { Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6, Sun: 7 };

  function updateNowLine() {
    const parts = formatter.formatToParts(new Date());
    const hour = Number(parts.find((part) => part.type === 'hour')?.value || '0');
    const minute = Number(parts.find((part) => part.type === 'minute')?.value || '0');
    const weekdayOrder = weekdayMap[weekdayFormatter.format(new Date())] || 0;
    const nowMinutes = (hour * 60) + minute;

    if (visibleDayOrders.length === 1 && visibleDayOrders[0] !== weekdayOrder) {
      line.classList.remove('visible');
      return;
    }

    const timeCells = Array.from(board.querySelectorAll('.time-cell'))
      .map((cell) => ({
        minutes: parseMinutes(cell.dataset.timeLabel || cell.textContent),
        top: cell.offsetTop,
        height: cell.offsetHeight
      }))
      .filter((item) => item.minutes !== null)
      .sort((a, b) => a.minutes - b.minutes);

    if (!timeCells.length) {
      line.classList.remove('visible');
      return;
    }

    let top = timeCells[0].top;
    if (nowMinutes <= timeCells[0].minutes) {
      top = timeCells[0].top;
    } else {
      for (let index = 0; index < timeCells.length; index += 1) {
        const current = timeCells[index];
        const next = timeCells[index + 1];
        if (!next) {
          top = current.top + current.height;
          break;
        }
        if (nowMinutes >= current.minutes && nowMinutes < next.minutes) {
          const ratio = (nowMinutes - current.minutes) / (next.minutes - current.minutes);
          top = current.top + ((next.top - current.top) * ratio);
          break;
        }
      }
    }

    line.style.top = `${top}px`;
    label.textContent = `Núna ${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
    line.classList.add('visible');
  }

  updateNowLine();
  window.addEventListener('resize', updateNowLine);
  setInterval(updateNowLine, 30000);
})();
</script>
</html>
"""


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


def normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def truncate_text(value: str | None, max_length: int = 180) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "…"


def pad_time(time_value: str | None) -> str | None:
    if not time_value:
        return None
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", time_value.strip())
    if not match:
        return None
    return f"{int(match.group(1)):02d}:{match.group(2)}"


def parse_time_range(text: str) -> tuple[str | None, str | None]:
    matches = re.findall(r"(\d{1,2}:\d{2})", text or "")
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
    passcode_match = re.search(r"Pass(?:code)?\s*:\s*([A-Za-z0-9]+)", text or "", re.IGNORECASE)
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
            SELECT
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
                COALESCE(la.canonical_location_text, m.location_text) AS canonical_location_text,
                lm.nickname AS location_nickname,
                CASE WHEN la.alias_location_text IS NULL THEN 0 ELSE 1 END AS has_location_mapping,
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
            LEFT JOIN location_aliases la
                ON la.alias_location_text = m.location_text
            LEFT JOIN location_metadata lm
                ON lm.canonical_location_text = COALESCE(la.canonical_location_text, m.location_text)
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
    return df


def export_csv(df: pd.DataFrame, csv_path: Path) -> None:
    ensure_parent_dir(csv_path)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def maybe_copy_to_clipboard(df: pd.DataFrame) -> None:
    df.to_clipboard(index=False, excel=True)


def scrape_all(db_path: Path, csv_path: Path, copy_to_clipboard: bool) -> pd.DataFrame:
    session = make_session()
    scraped_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    aa_records = dedupe_aa_remote_variants(scrape_aa(session, scraped_at_utc))
    fjar_records = scrape_fjarfundir(session, scraped_at_utc)
    records = merge_remote_meetings(aa_records, fjar_records)
    write_snapshot(db_path, records, scraped_at_utc)
    df = load_dataframe(db_path)
    export_csv(df, csv_path)
    if copy_to_clipboard:
        maybe_copy_to_clipboard(df)
    return df


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


def request_filters() -> dict[str, str]:
    return {
        "view": request.args.get("view", "list").strip() or "list",
        "weekday": request.args.get("weekday", "").strip(),
        "fellowship": request.args.get("fellowship", "").strip(),
        "format": request.args.get("format", "").strip(),
        "gender_restriction": request.args.get("gender_restriction", "").strip(),
        "access_restriction": request.args.get("access_restriction", "").strip(),
        "canonical_location": request.args.get("canonical_location", "").strip(),
        "time_from": request.args.get("time_from", "").strip(),
        "time_to": request.args.get("time_to", "").strip(),
    }


def build_filter_options(df: pd.DataFrame) -> dict[str, list[str] | list[dict[str, str]]]:
    nickname_rows = (
        df.loc[df["location_nickname"].fillna("").str.strip() != "", ["canonical_location_text", "location_nickname"]]
        .drop_duplicates()
        .sort_values(["location_nickname", "canonical_location_text"], kind="stable")
    )
    nickname_locations = [
        {
            "value": str(row["canonical_location_text"]),
            "label": (
                str(row["location_nickname"])
                if str(row["location_nickname"]) == str(row["canonical_location_text"])
                else f'{row["location_nickname"]} ({row["canonical_location_text"]})'
            ),
        }
        for _, row in nickname_rows.iterrows()
    ]

    return {
        "weekday_is": [day for day, _ in AA_DAY_PAGES],
        "fellowship": distinct_values(df, "fellowship"),
        "format": distinct_values(df, "format"),
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

    excerpt = " | ".join([part for part in excerpt_parts if part])
    return locator, excerpt


def build_week_view(rows: list[dict[str, str | None]], week_days: list[str]) -> list[dict[str, object]]:
    slots: dict[tuple[int, str], dict[str, object]] = {}

    for row in rows:
        start_time = row.get("start_time") or ""
        time_display = row.get("time_display") or "Ótímasett"
        if start_time:
            slot_key = (0, start_time)
            time_label = start_time
        else:
            slot_key = (1, str(time_display))
            time_label = str(time_display)

        if slot_key not in slots:
            slots[slot_key] = {
                "time_label": time_label,
                "cells": {day: [] for day in week_days},
            }

        slots[slot_key]["cells"][str(row["weekday_is"])].append(row)

    ordered_slots: list[dict[str, object]] = []
    for slot_key in sorted(slots.keys(), key=lambda item: item):
        slot = slots[slot_key]
        ordered_cells = []
        for day in week_days:
            cell_rows = slot["cells"][day]
            cell_rows.sort(key=lambda item: ((item.get("meeting_name") or ""), (item.get("location_text") or "")))
            ordered_cells.append(cell_rows)
        ordered_slots.append(
            {
                "time_label": slot["time_label"],
                "cells": ordered_cells,
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


def build_app(db_path: Path) -> Flask:
    app = Flask(__name__)

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
                df = df[df[column].fillna("") == value]

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

        if filters["time_from"]:
            df = df[df["start_time"].fillna("") >= filters["time_from"]]

        if filters["time_to"]:
            df = df[df["start_time"].fillna("") <= filters["time_to"]]

        return df

    @app.get("/")
    def index():
        df = load_dataframe(db_path)
        filters = request_filters()
        filtered = filter_df(df, filters)
        row_dicts = filtered.to_dict(orient="records")
        displayed_week_days = [filters["weekday"]] if filters["weekday"] in [day for day, _ in AA_DAY_PAGES] else [day for day, _ in AA_DAY_PAGES]
        displayed_week_day_orders = [WEEKDAY_ORDER[day] for day in displayed_week_days]
        scraped_at = df["scraped_at_utc"].iloc[0] if not df.empty else "N/A"
        options = build_filter_options(df)
        source_counts = (
            filtered.groupby("source")
            .size()
            .sort_index()
            .reset_index(name="count")
            .itertuples(index=False, name=None)
        )
        csv_query_string = build_query_string(filters, exclude={"view"})
        list_query_string = build_query_string(filters, overrides={"view": "list"})
        week_query_string = build_query_string(filters, overrides={"view": "week"})
        locations_query_string = build_query_string(filters, overrides={"view": "locations"})
        location_rows = build_location_review_rows(filtered, "")
        location_clusters = build_location_clusters(location_rows)
        return Response(
            app.jinja_env.from_string(CARD_TEMPLATE).render(
                rows=row_dicts,
                week_slots=build_week_view(row_dicts, displayed_week_days),
                week_days=displayed_week_days,
                week_day_orders=displayed_week_day_orders,
                week_day_count=len(displayed_week_days),
                total_count=len(filtered),
                scraped_at=scraped_at,
                options=options,
                filters=filters,
                source_counts=list(source_counts),
                csv_query_string=csv_query_string,
                list_query_string=list_query_string,
                week_query_string=week_query_string,
                locations_query_string=locations_query_string,
                location_rows=location_rows,
                location_clusters=location_clusters,
            ),
            mimetype="text/html",
        )

    @app.get("/csv")
    def csv_download():
        df = load_dataframe(db_path)
        filtered = filter_df(df, request_filters())
        csv_text = filtered.to_csv(index=False)
        return Response(
            csv_text,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=meetings_filtered.csv"},
        )

    @app.post("/locations/map")
    def location_map():
        alias_location_text = request.form.get("alias_location_text", "").strip()
        canonical_location_text = request.form.get("canonical_location_text", "").strip()
        redirect_query = request.form.get("redirect_query", "").strip()
        if alias_location_text:
            save_location_mapping(db_path, alias_location_text, canonical_location_text)
        target = "/" + (f"?{redirect_query}" if redirect_query else "?view=locations")
        return redirect(target)

    @app.post("/locations/nickname")
    def location_nickname():
        canonical_location_text = request.form.get("canonical_location_text", "").strip()
        nickname = request.form.get("nickname", "").strip()
        redirect_query = request.form.get("redirect_query", "").strip()
        if canonical_location_text:
            save_location_nickname(db_path, canonical_location_text, nickname)
        target = "/" + (f"?{redirect_query}" if redirect_query else "?view=locations")
        return redirect(target)

    return app


def cmd_scrape(args: argparse.Namespace) -> int:
    df = scrape_all(args.db, args.csv, args.copy)
    print(summarize_dataframe(df))
    print(f"SQLite: {args.db}")
    print(f"CSV: {args.csv}")
    if args.copy:
        print("Clipboard: copied")
    return 0


def cmd_preview(args: argparse.Namespace) -> int:
    if not args.db.exists():
        raise FileNotFoundError(f"Gagnagrunnur fannst ekki: {args.db}")

    df = load_dataframe(args.db)
    if args.copy:
        maybe_copy_to_clipboard(df)

    print(summarize_dataframe(df))
    print()
    preview_columns = [
        "source",
        "weekday_is",
        "time_display",
        "meeting_name",
        "subtitle",
        "fellowship",
        "format",
        "gender_restriction",
        "access_restriction",
        "location_text",
        "canonical_location_text",
        "location_nickname",
        "venue_text",
        "zoom_meeting_id",
        "source_record_id",
        "source_page_url",
        "source_locator",
        "source_excerpt",
    ]
    print(df[preview_columns].head(args.limit).to_string(index=False))
    if args.copy:
        print("\nClipboard: copied")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    if not args.db.exists():
        scrape_all(args.db, args.csv, copy_to_clipboard=False)
    app = build_app(args.db)
    print(f"Serving on http://{args.host}:{args.port}")
    if args.host == "0.0.0.0":
        for addr in detect_local_ipv4_addresses():
            print(f"Try in browser: http://{addr}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scraper fyrir AA fundaskrár")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape_parser = subparsers.add_parser("scrape", help="Sækir lifandi gögn og skrifar snapshot")
    scrape_parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    scrape_parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH)
    scrape_parser.add_argument("--copy", action="store_true")
    scrape_parser.set_defaults(func=cmd_scrape)

    preview_parser = subparsers.add_parser("preview", help="Sýnir yfirlit úr núverandi SQLite snapshot")
    preview_parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    preview_parser.add_argument("--copy", action="store_true")
    preview_parser.add_argument("--limit", type=int, default=30)
    preview_parser.set_defaults(func=cmd_preview)

    serve_parser = subparsers.add_parser("serve", help="Keyrir einfalt Flask-yfirlit")
    serve_parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    serve_parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH)
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=5000)
    serve_parser.set_defaults(func=cmd_serve)

    return parser


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = build_parser()
    args = parser.parse_args(["scrape"] if len(sys.argv) == 1 else None)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
