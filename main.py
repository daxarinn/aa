from __future__ import annotations

import argparse
import hmac
import hashlib
import html
import json
import os
import re
import secrets
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urlencode
import socket
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, Response, redirect, request, session


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


CARD_TEMPLATE = """
<!doctype html>
<html lang="is">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Fundaskrá</title>
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
    .wrap { max-width: 1100px; margin: 0 auto; padding: 12px 10px 18px; }
    .hero {
      background: rgba(255,255,255,0.75);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 12px;
      backdrop-filter: blur(8px);
      box-shadow: 0 16px 40px rgba(15, 23, 42, 0.08);
    }
    h1 { font-size: clamp(1.6rem, 3.2vw, 2.4rem); margin: 0 0 6px; line-height: 1.05; }
    .meta { color: var(--muted); font-size: 0.95rem; margin: 0; }
    .filters {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 8px;
      margin-top: 10px;
    }
    .filter-field {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .filter-field label {
      font-size: 0.82rem;
      color: var(--muted);
    }
    .filters input, .filters select, .filters button, .filters a {
      width: 100%;
      border-radius: 12px;
      border: 1px solid var(--border);
      padding: 8px 10px;
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
      gap: 8px;
      margin-top: 6px;
    }
    .filter-panel-header {
      margin-top: 10px;
    }
    .filter-toggle {
      border-radius: 999px;
      border: 1px solid var(--border);
      padding: 7px 11px;
      font: inherit;
      background: white;
      color: var(--ink);
      cursor: pointer;
    }
    .filters.is-collapsed {
      display: none;
    }
    .view-switch {
      display: inline-flex;
      gap: 6px;
      margin-top: 10px;
      flex-wrap: wrap;
    }
    .view-switch a {
      text-decoration: none;
      border: 1px solid var(--border);
      color: var(--ink);
      background: white;
      padding: 7px 10px;
      border-radius: 999px;
      font-size: 0.92rem;
    }
    .view-switch a.active {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }
    .calendar-tools {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
      align-items: center;
    }
    .calendar-tools a,
    .calendar-tools button {
      text-decoration: none;
      border: 1px solid var(--border);
      color: var(--ink);
      background: white;
      padding: 7px 10px;
      border-radius: 999px;
      font: inherit;
      font-size: 0.92rem;
      cursor: pointer;
    }
    .calendar-tools a.is-disabled,
    .calendar-tools button.is-disabled,
    .calendar-tools button:disabled {
      opacity: 0.55;
      pointer-events: none;
      cursor: not-allowed;
    }
    .calendar-status {
      color: var(--muted);
      font-size: 0.88rem;
      min-height: 1.2em;
    }
    .calendar-status.is-error {
      color: #991b1b;
    }
    .summary { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
    .summary span {
      padding: 5px 8px;
      border-radius: 999px;
      background: var(--pill);
      color: #0f172a;
      font-size: 0.9rem;
    }
    .grid { display: grid; gap: 8px; margin-top: 12px; }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 11px;
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
      position: relative;
    }
    .card.is-favorite,
    .slot-card.is-favorite {
      outline: 2px solid #d49f24;
      outline-offset: 1px;
      box-shadow: inset 0 0 0 1px rgba(212, 159, 36, 0.18), 0 12px 28px rgba(15, 23, 42, 0.05);
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
    .topline { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin-bottom: 6px; }
    .time { font-weight: 700; font-size: 1.05rem; color: var(--accent); }
    .pill {
      font-size: 0.82rem;
      color: #0f172a;
      background: #eef6f5;
      border: 1px solid #d7ebe7;
      padding: 4px 7px;
      border-radius: 999px;
    }
    h2 { margin: 0 0 6px; font-size: 1.2rem; }
    .line { margin: 3px 0; color: var(--muted); font-size: 0.93rem; }
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
      margin-top: 12px;
      background: rgba(255,255,255,0.7);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      color: var(--muted);
    }
    .week-shell {
      margin-top: 16px;
      border: 1px solid var(--border);
      border-radius: 16px;
      background: rgba(255,255,255,0.55);
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
      overflow: visible;
    }
    .week-scroll {
      overflow-x: auto;
      overflow-y: visible;
      border-radius: 16px;
    }
    .week-board {
      min-width: 920px;
      display: grid;
      grid-template-columns: 36px repeat(7, minmax(120px, 1fr));
      position: relative;
    }
    .week-board.single-day {
      min-width: 0;
      grid-template-columns: 36px minmax(0, 1fr);
    }
    .week-head {
      position: sticky;
      top: 0;
      z-index: 4;
      background: #f8f5ee;
      border-bottom: 1px solid var(--border);
      padding: 7px 6px;
      font-size: 0.88rem;
      font-weight: 700;
    }
    .week-board > .week-head:first-child {
      left: 0;
      z-index: 6;
      border-right: 1px solid var(--border);
      text-align: center;
    }
    .time-cell {
      position: sticky;
      left: 0;
      z-index: 3;
      padding: 4px 1px;
      border-right: 1px solid var(--border);
      border-bottom: 1px solid var(--border);
      color: var(--accent);
      font-weight: 700;
      font-size: 0.68rem;
      background: rgba(248,245,238,0.75);
      display: flex;
      align-items: center;
      justify-content: center;
      text-align: center;
    }
    .time-cell-text,
    .week-time-head {
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      white-space: nowrap;
      line-height: 1;
    }
    .week-cell {
      min-height: 80px;
      padding: 6px;
      border-bottom: 1px solid var(--border);
      border-right: 1px solid var(--border);
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .slot-card {
      background: white;
      border: 1px solid #e7e0d2;
      border-radius: 14px;
      padding: 7px 32px 7px 8px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.04);
      position: relative;
      cursor: pointer;
      outline: none;
    }
    .favorite-toggle {
      position: absolute;
      top: 8px;
      right: 8px;
      width: 28px;
      height: 28px;
      border-radius: 999px;
      border: 1px solid #d8d2c6;
      background: rgba(255,255,255,0.92);
      color: #8a7f68;
      font-size: 0.95rem;
      line-height: 1;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      z-index: 3;
    }
    .favorite-toggle.is-active {
      color: #9c6b00;
      background: #fff2bf;
      border-color: #d4a43d;
    }
    .favorite-toggle:hover {
      border-color: #c8b27d;
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
    @media (max-width: 720px) {
      .week-board {
        min-width: 700px;
        grid-template-columns: 30px repeat(7, minmax(104px, 1fr));
      }
      .week-board.single-day {
        min-width: 0;
        grid-template-columns: 30px minmax(0, 1fr);
      }
      .week-head {
        padding: 6px 4px;
        font-size: 0.8rem;
      }
      .time-cell {
        font-size: 0.62rem;
        padding: 3px 0;
      }
      .week-cell {
        padding: 4px;
        gap: 4px;
      }
      .slot-card {
        padding: 6px 28px 6px 6px;
      }
    }
    .slot-meta {
      margin: 0;
      color: var(--muted);
      font-size: 0.82rem;
      line-height: 1.35;
    }
    .slot-tooltip-title {
      margin: 0 0 6px;
      font-size: 0.95rem;
      line-height: 1.25;
      color: var(--ink);
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
      margin-top: 4px;
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
      width: min(280px, calc(100vw - 18px));
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
    .slot-card:focus-within .slot-tooltip,
    .slot-card.is-open .slot-tooltip {
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
    .admin-login {
      max-width: 420px;
      margin-top: 12px;
      display: grid;
      gap: 10px;
    }
    .admin-nav {
      display: inline-flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 4px;
    }
    .size-form {
      margin-top: 6px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 6px;
      align-items: center;
      max-width: 280px;
    }
    .size-form select,
    .size-form button {
      border-radius: 8px;
      border: 1px solid var(--border);
      padding: 5px 7px;
      font: inherit;
      background: white;
      font-size: 0.84rem;
    }
    .size-form button {
      background: #f8f5ee;
      color: var(--muted);
      border-color: #ddd5c4;
      cursor: pointer;
    }
    .size-note {
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 0.84rem;
    }
    .size-help {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 0.78rem;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Fundaskrá</h1>
      <p class="meta">AA, fjarfundir og kirkjusamkomur. Uppfært {{ scraped_at }}</p>
      <div class="filter-panel-header">
        <button type="button" class="filter-toggle" id="filterToggle" aria-expanded="false">Sýna síur</button>
      </div>
      <form class="filters" method="get" id="filtersForm">
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
          <label for="region">Svæði</label>
          <select id="region" name="region">
            <option value="">Allt landið</option>
            {% for item in options["region_options"] %}
            <option value="{{ item["value"] }}" {% if filters["region"] == item["value"] %}selected{% endif %}>{{ item["label"] }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="filter-field">
          <label for="favorites_only">Uppáhaldsfundir</label>
          <select id="favorites_only" name="favorites_only">
            <option value="">Sýna alla</option>
            <option value="1" {% if filters["favorites_only"] == "1" %}selected{% endif %}>Bara uppáhalds</option>
          </select>
        </div>
        <div class="filter-field">
          <label for="include_church">Kirkjusamkomur</label>
          <select id="include_church" name="include_church">
            <option value="">Fela</option>
            <option value="1" {% if filters["include_church"] == "1" %}selected{% endif %}>Sýna með fundum</option>
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
          <a href="/?clear_filters=1" id="clearFiltersLink">Hreinsa</a>
        </div>
      </form>
      <div class="summary">
        <span>{{ total_count }} fundir</span>
        {% for item in source_counts %}
        <span>{{ item[0] }}: {{ item[1] }}</span>
        {% endfor %}
      </div>
      <div class="view-switch">
        <a href="/?{{ list_query_string }}" class="{% if filters["view"] == "list" %}active{% endif %}">Línuleg sýn</a>
        <a href="/?{{ week_query_string }}" class="{% if filters["view"] == "week" %}active{% endif %}">Vikusýn</a>
      </div>
      <div class="calendar-tools">
        <a href="/favorites.ics" id="favoritesCalendarDownload">Dagatal (.ics)</a>
        <button type="button" id="favoritesCalendarSubscribe">Afrita áskriftarslóð</button>
        <span class="calendar-status" id="favoritesCalendarStatus" aria-live="polite"></span>
      </div>
    </section>
    {% if filters["view"] == "admin" %}
    {% if not admin_authenticated %}
    <section class="mapping-section">
      <article class="mapping-card">
        <h3>Admin</h3>
        <p class="mapping-meta">Skráðu þig inn til að breyta staðamöppunum og kirkjusamkomum.</p>
        <form class="admin-login" method="post" action="/admin/login">
          <input type="hidden" name="redirect_query" value="{{ admin_query_string }}">
          <input type="password" name="password" placeholder="Lykilorð">
          <button type="submit">Innskrá</button>
        </form>
      </article>
    </section>
    {% elif admin_section == "locations" %}
    <section class="mapping-section">
      <article class="mapping-card">
        <h3>Admin</h3>
        <div class="admin-nav">
          <a href="/admin?{{ admin_query_string }}">Yfirlit</a>
          <a href="/admin?{{ locations_query_string }}">Staðamöppun</a>
          <a href="/admin?{{ church_query_string }}">Kirkjuskráning</a>
          <form method="post" action="/admin/logout">
            <button type="submit">Útskrá</button>
          </form>
        </div>
      </article>
    </section>
    <section class="mapping-section">
      <article class="mapping-card">
        <h3>Skráðar staðamappanir</h3>
        {% if mapped_location_rows %}
        <table class="mapping-table">
          <thead>
            <tr>
              <th>Alias</th>
              <th>Canonical</th>
              <th>Gælunafn</th>
              <th>Aðgerðir</th>
            </tr>
          </thead>
          <tbody>
            {% for row in mapped_location_rows %}
            <tr>
              <td>
                {{ row["location_text"] }}
                <div class="mapping-meta">{{ row["normalized_key"] }}</div>
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
              <td class="mapping-meta">
                <form method="post" action="/locations/map">
                  <input type="hidden" name="alias_location_text" value="{{ row["location_text"] }}">
                  <input type="hidden" name="canonical_location_text" value="">
                  <input type="hidden" name="redirect_query" value="{{ locations_query_string }}">
                  <button type="submit">Hreinsa mapping</button>
                </form>
                <form method="post" action="/locations/nickname">
                  <input type="hidden" name="canonical_location_text" value="{{ row["canonical_location_text"] }}">
                  <input type="hidden" name="nickname" value="">
                  <input type="hidden" name="redirect_query" value="{{ locations_query_string }}">
                  <button type="submit">Hreinsa gælunafn</button>
                </form>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
        <p class="mapping-meta">Engar vistaðar staðamappanir eða gælunöfn hafa verið skráð enn.</p>
        {% endif %}
      </article>
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
    {% elif admin_section == "analytics" %}
    <section class="mapping-section">
      <article class="mapping-card">
        <h3>Admin</h3>
        <div class="admin-nav">
          <a href="/admin?{{ admin_query_string }}">Yfirlit</a>
          <a href="/admin?{{ locations_query_string }}">Staðamöppun</a>
          <a href="/admin?{{ church_query_string }}">Kirkjuskráning</a>
          <form method="post" action="/admin/logout">
            <button type="submit">Útskrá</button>
          </form>
        </div>
      </article>
      <article class="mapping-card">
        <h3>Heimsóknir</h3>
        <div class="summary">
          <span>Samtals heimsóknir: {{ visit_totals["total_visits"] }}</span>
          <span>Einstök client_id: {{ visit_totals["unique_clients"] }}</span>
        </div>
        {% if visit_summary_rows %}
        <table class="mapping-table">
          <thead>
            <tr>
              <th>client_id</th>
              <th>Fjöldi</th>
              <th>Fyrst séð</th>
              <th>Síðast séð</th>
            </tr>
          </thead>
          <tbody>
            {% for row in visit_summary_rows %}
            <tr>
              <td><code>{{ row["client_id"] }}</code></td>
              <td>{{ row["visit_count"] }}</td>
              <td class="mapping-meta">{{ row["first_seen_utc"] }}</td>
              <td class="mapping-meta">{{ row["last_seen_utc"] }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
        <p class="mapping-meta">Engar heimsóknir hafa verið skráðar enn.</p>
        {% endif %}
      </article>
      <article class="mapping-card">
        <h3>Nýjustu opnanir</h3>
        {% if recent_visit_rows %}
        <table class="mapping-table">
          <thead>
            <tr>
              <th>Tími</th>
              <th>client_id</th>
              <th>Slóð</th>
              <th>Query</th>
            </tr>
          </thead>
          <tbody>
            {% for row in recent_visit_rows %}
            <tr>
              <td class="mapping-meta">{{ row["visited_at_utc"] }}</td>
              <td><code>{{ row["client_id"] }}</code></td>
              <td>{{ row["path"] }}</td>
              <td class="mapping-meta">{{ row["query_string"] or "" }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
        <p class="mapping-meta">Engar nýlegar opnanir hafa verið skráðar enn.</p>
        {% endif %}
      </article>
    </section>
    {% else %}
    <section class="mapping-section">
      <article class="mapping-card">
        <h3>Admin</h3>
        <div class="admin-nav">
          <a href="/admin?{{ admin_query_string }}">Yfirlit</a>
          <a href="/admin?{{ locations_query_string }}">Staðamöppun</a>
          <a href="/admin?{{ church_query_string }}">Kirkjuskráning</a>
          <form method="post" action="/admin/logout">
            <button type="submit">Útskrá</button>
          </form>
        </div>
      </article>
    </section>
    <section class="mapping-section">
      <article class="mapping-card">
        <h3>{% if church_edit_event %}Breyta kirkjusamkomu{% else %}Ný kirkjusamkoma{% endif %}</h3>
        <p class="mapping-meta">Handfærðar samkomur sem hægt er að kveikja á í venjulegu fundayfirliti með filter.</p>
        <form class="filters" method="post" action="/church/save">
          {% if church_edit_event %}<input type="hidden" name="event_id" value="{{ church_edit_event["event_id"] }}">{% endif %}
          <input type="hidden" name="redirect_query" value="{{ church_query_string }}">
          <div class="filter-field">
            <label for="church_title">Heiti</label>
            <input id="church_title" type="text" name="title" placeholder="Messa, samvera..." value="{{ church_edit_event["title"] if church_edit_event else "" }}">
          </div>
          <div class="filter-field">
            <label for="church_weekday">Vikudagur</label>
            <select id="church_weekday" name="weekday_is">
              {% for value in options["weekday_is"] %}
              <option value="{{ value }}" {% if church_edit_event and church_edit_event["weekday_is"] == value %}selected{% endif %}>{{ value }}</option>
              {% endfor %}
            </select>
          </div>
          <div class="filter-field">
            <label for="church_start_time">Frá tími</label>
            <input id="church_start_time" type="time" name="start_time" value="{{ church_edit_event["start_time"] if church_edit_event else "" }}">
          </div>
          <div class="filter-field">
            <label for="church_end_time">Til tíma</label>
            <input id="church_end_time" type="time" name="end_time" value="{{ church_edit_event["end_time"] if church_edit_event else "" }}">
          </div>
          <div class="filter-field">
            <label for="church_subtitle">Undirlína</label>
            <input id="church_subtitle" type="text" name="subtitle" placeholder="Prestur, efni..." value="{{ church_edit_event["subtitle"] if church_edit_event else "" }}">
          </div>
          <div class="filter-field">
            <label for="church_location_text">Staðsetning</label>
            <input id="church_location_text" type="text" name="location_text" placeholder="Reykjavík..." value="{{ church_edit_event["location_text"] if church_edit_event else "" }}">
          </div>
          <div class="filter-field">
            <label for="church_venue_text">Staður</label>
            <input id="church_venue_text" type="text" name="venue_text" placeholder="Hallgrímskirkja..." value="{{ church_edit_event["venue_text"] if church_edit_event else "" }}">
          </div>
          <div class="filter-field">
            <label for="church_notes">Glósur</label>
            <input id="church_notes" type="text" name="notes" placeholder="Sálmar, kaffi..." value="{{ church_edit_event["notes"] if church_edit_event else "" }}">
          </div>
          <div class="filter-field">
            <label for="church_source_page_url">Slóð</label>
            <input id="church_source_page_url" type="url" name="source_page_url" placeholder="https://..." value="{{ church_edit_event["source_page_url"] if church_edit_event else "" }}">
          </div>
          <div class="filter-actions">
            <button type="submit">{% if church_edit_event %}Uppfæra kirkjusamkomu{% else %}Vista kirkjusamkomu{% endif %}</button>
            {% if church_edit_event %}<a href="/admin?{{ church_query_string }}">Hætta við</a>{% endif %}
          </div>
        </form>
      </article>
      <article class="mapping-card">
        <h3>Skráðar kirkjusamkomur</h3>
        {% if manual_events %}
        <table class="mapping-table">
          <thead>
            <tr>
              <th>Heiti</th>
              <th>Tími</th>
              <th>Staður</th>
              <th>Glósur</th>
              <th>Aðgerð</th>
            </tr>
          </thead>
          <tbody>
            {% for item in manual_events %}
            <tr>
              <td>
                {{ item["title"] }}
                {% if item["subtitle"] %}<div class="mapping-meta">{{ item["subtitle"] }}</div>{% endif %}
              </td>
              <td>{{ item["weekday_is"] }} {{ item["time_display"] }}</td>
              <td>
                {% if item["venue_text"] %}{{ item["venue_text"] }}{% endif %}
                {% if item["location_text"] %}<div class="mapping-meta">{{ item["location_text"] }}</div>{% endif %}
              </td>
              <td class="mapping-meta">
                {% if item["notes"] %}{{ item["notes"] }}{% endif %}
                {% if item["source_page_url"] %}<br><a href="{{ item["source_page_url"] }}" target="_blank" rel="noreferrer">Slóð</a>{% endif %}
              </td>
              <td>
                <a href="/admin?admin_section=church&edit_event_id={{ item["event_id"] }}">Breyta</a>
                <form method="post" action="/church/delete">
                  <input type="hidden" name="event_id" value="{{ item["event_id"] }}">
                  <input type="hidden" name="redirect_query" value="{{ church_query_string }}">
                  <button type="submit">Eyða</button>
                </form>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
        <p class="mapping-meta">Engar kirkjusamkomur hafa verið skráðar enn.</p>
        {% endif %}
      </article>
    </section>
    {% endif %}
    {% elif total_count == 0 %}
    <section class="empty-state">
      Engir fundir pössuðu við valdar síur.
    </section>
    {% elif filters["view"] == "week" %}
    <section class="week-shell">
      <div class="week-scroll" data-scroll-restore="week-shell">
      <div class="week-board{% if week_day_count == 1 %} single-day{% endif %}" data-week-days="{{ week_days|join('|') }}" data-weekday-orders="{{ week_day_orders|join('|') }}">
        <div class="now-line" id="nowLine">
          <span class="now-line-label" id="nowLineLabel">Núna</span>
        </div>
        <div class="week-head"><span class="week-time-head">Tími</span></div>
        {% for day in week_days %}
        <div class="week-head">{{ day }}</div>
        {% endfor %}
        {% for slot in week_slots %}
        <div class="time-cell" data-time-label="{{ slot["time_label"] }}"><span class="time-cell-text">{{ slot["time_label"] }}</span></div>
        {% for cell in slot["cells"] %}
        <div class="week-cell">
          {% for row in cell %}
          <article class="slot-card{% if row["format"] == "Fjarfundur" %} is-remote{% endif %}{% if row["gender_restriction"] == "Konur" %} is-women{% elif row["gender_restriction"] == "Karlar" %} is-men{% endif %}{% if row["is_favorite"] %} is-favorite{% endif %}" tabindex="0" data-meeting-id="{{ row["source_uid"] }}">
            <button class="favorite-toggle{% if row["is_favorite"] %} is-active{% endif %}" type="button" data-meeting-id="{{ row["source_uid"] }}" aria-label="Setja fund í uppáhald">★</button>
            <h3 class="slot-title">{{ row["meeting_name_display"] }}</h3>
            <p class="slot-summary">{{ row["summary_display"] }}</p>
            <div class="slot-tooltip">
              <p class="slot-tooltip-title">{{ row["meeting_name_display"] }}</p>
              {% if row["subtitle"] %}<p class="slot-meta">{{ row["subtitle"] }}</p>{% endif %}
              {% if row["size_display"] %}<p class="size-note">{{ row["size_display"] }}</p>{% endif %}
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
      </div>
    </section>
    {% else %}
    <section class="grid">
      {% for row in rows %}
      <article class="card{% if row["format"] == "Fjarfundur" %} is-remote{% endif %}{% if row["gender_restriction"] == "Konur" %} is-women{% elif row["gender_restriction"] == "Karlar" %} is-men{% endif %}{% if row["is_favorite"] %} is-favorite{% endif %}" data-meeting-id="{{ row["source_uid"] }}">
        <button class="favorite-toggle{% if row["is_favorite"] %} is-active{% endif %}" type="button" data-meeting-id="{{ row["source_uid"] }}" aria-label="Setja fund í uppáhald">★</button>
        <div class="topline">
          <span class="time">{{ row["weekday_is"] }} {{ row["time_display"] }}</span>
          <span class="pill">{{ row["source"] }}</span>
          <span class="pill">{{ row["fellowship_display"] }}</span>
          {% if row["gender_restriction"] %}<span class="pill">{{ row["gender_restriction"] }}</span>{% endif %}
          {% if row["access_restriction"] %}<span class="pill">{{ row["access_restriction"] }}</span>{% endif %}
          {% if row["format"] %}<span class="pill">{{ row["format"] }}</span>{% endif %}
        </div>
        <h2>{{ row["meeting_name_display"] }}</h2>
        {% if row["size_display"] %}<p class="size-note">{{ row["size_display"] }}</p>{% endif %}
        {% if row["subtitle"] %}<p class="line"><strong>Undirlína:</strong> {{ row["subtitle"] }}</p>{% endif %}
        {% if row["location_nickname"] %}<p class="line"><strong>Gælunafn:</strong> {{ row["location_nickname"] }}</p>{% endif %}
        {% if row["location_text"] %}<p class="line"><strong>Staðsetning:</strong> {{ row["location_text"] }}</p>{% endif %}
        {% if row["venue_text"] %}<p class="line"><strong>Staður:</strong> {{ row["venue_text"] }}</p>{% endif %}
        {% if row["recurrence_hint"] %}<p class="line"><strong>Regluleiki:</strong> {{ row["recurrence_hint"] }}</p>{% endif %}
        {% if row["notes"] %}<p class="line"><strong>Glósur:</strong> {{ row["notes"] }}</p>{% endif %}
        <form class="size-form" method="post" action="/size-report">
          <input type="hidden" name="source_uid" value="{{ row["source_uid"] }}">
          <input type="hidden" name="redirect_query" value="{{ current_query_string }}">
          <select name="size_bin">
            <option value="">Breyta fundarstærð</option>
            {% for item in options["size_bins"] %}
            <option value="{{ item["value"] }}" {% if row["current_size_bin"] == item["value"] %}selected{% endif %}>{{ item["label"] }}</option>
            {% endfor %}
          </select>
          <button type="submit">Vista</button>
        </form>
        <p class="size-help">Aðeins til upplýsinga. Ný innsending uppfærir meðaltalið þegar fleiri svara.</p>
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
  const filtersCookieName = "{{ filters_cookie_name }}";
  const favoritesCookieName = "{{ favorites_cookie_name }}";
  const clientCookieName = "{{ client_cookie_name }}";
  const defaultWeekday = "{{ default_weekday }}";
  const maxFavorites = 200;

  const getCookie = (name) => {
    const encoded = `${encodeURIComponent(name)}=`;
    return document.cookie
      .split('; ')
      .find((part) => part.startsWith(encoded))
      ?.slice(encoded.length) || '';
  };

  const setCookie = (name, value, days = 365) => {
    const expires = new Date(Date.now() + (days * 24 * 60 * 60 * 1000)).toUTCString();
    document.cookie = `${encodeURIComponent(name)}=${encodeURIComponent(value)}; expires=${expires}; path=/; SameSite=Lax`;
  };

  const deleteCookie = (name) => {
    document.cookie = `${encodeURIComponent(name)}=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/; SameSite=Lax`;
  };

  const parseJsonCookie = (name, fallback) => {
    const raw = getCookie(name);
    if (!raw) return fallback;
    try {
      return JSON.parse(decodeURIComponent(raw));
    } catch (_error) {
      return fallback;
    }
  };

  if (!getCookie(clientCookieName)) {
    const randomValue = window.crypto && window.crypto.randomUUID
      ? window.crypto.randomUUID()
      : `client-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setCookie(clientCookieName, randomValue, 365);
  }

  const favoriteSet = new Set(
    parseJsonCookie(favoritesCookieName, [])
      .filter((value) => typeof value === 'string' && value.trim())
      .slice(0, maxFavorites)
  );
  const favoritesCalendarDownload = document.getElementById('favoritesCalendarDownload');
  const favoritesCalendarSubscribe = document.getElementById('favoritesCalendarSubscribe');
  const favoritesCalendarStatus = document.getElementById('favoritesCalendarStatus');

  const setCalendarStatus = (message, isError = false) => {
    if (!favoritesCalendarStatus) return;
    favoritesCalendarStatus.textContent = message || '';
    favoritesCalendarStatus.classList.toggle('is-error', Boolean(message) && isError);
  };

  const updateCalendarActions = () => {
    const hasFavorites = favoriteSet.size > 0;
    if (favoritesCalendarDownload) {
      favoritesCalendarDownload.classList.toggle('is-disabled', !hasFavorites);
      favoritesCalendarDownload.setAttribute('aria-disabled', hasFavorites ? 'false' : 'true');
      favoritesCalendarDownload.setAttribute('title', hasFavorites ? 'Sækja dagatal fyrir uppáhaldsfundi' : 'Veldu fyrst uppáhaldsfundi');
    }
    if (favoritesCalendarSubscribe) {
      favoritesCalendarSubscribe.disabled = !hasFavorites;
      favoritesCalendarSubscribe.classList.toggle('is-disabled', !hasFavorites);
      favoritesCalendarSubscribe.setAttribute('title', hasFavorites ? 'Afrita áskriftarslóð fyrir uppáhaldsdagatal' : 'Veldu fyrst uppáhaldsfundi');
    }
    if (!hasFavorites) {
      setCalendarStatus('Veldu fyrst uppáhaldsfundi til að búa til dagatal.');
    } else if ((favoritesCalendarStatus?.textContent || '').startsWith('Veldu fyrst')) {
      setCalendarStatus('');
    }
  };

  const copyText = async (value) => {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(value);
      return;
    }
    const helper = document.createElement('textarea');
    helper.value = value;
    helper.setAttribute('readonly', 'readonly');
    helper.style.position = 'fixed';
    helper.style.top = '-999px';
    document.body.appendChild(helper);
    helper.select();
    document.execCommand('copy');
    helper.remove();
  };

  const syncFavoriteButtons = () => {
    document.querySelectorAll('[data-meeting-id]').forEach((node) => {
      const meetingId = node.getAttribute('data-meeting-id') || '';
      if (!meetingId) return;
      const isFavorite = favoriteSet.has(meetingId);
      if (node.classList.contains('favorite-toggle')) {
        node.classList.toggle('is-active', isFavorite);
        node.setAttribute('aria-pressed', isFavorite ? 'true' : 'false');
        node.setAttribute('title', isFavorite ? 'Fjarlægja úr uppáhaldi' : 'Setja í uppáhald');
      }
      if (node.classList.contains('card') || node.classList.contains('slot-card')) {
        node.classList.toggle('is-favorite', isFavorite);
      }
    });
    updateCalendarActions();
  };

  const persistFavorites = () => {
    setCookie(favoritesCookieName, JSON.stringify(Array.from(favoriteSet).sort()), 365);
    syncFavoriteButtons();
  };

  document.querySelectorAll('.favorite-toggle').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      document.querySelectorAll('.slot-card.is-open').forEach((card) => {
        card.classList.remove('is-open');
      });
      const meetingId = button.getAttribute('data-meeting-id') || '';
      if (!meetingId) return;
      if (favoriteSet.has(meetingId)) {
        favoriteSet.delete(meetingId);
      } else if (favoriteSet.size < maxFavorites) {
        favoriteSet.add(meetingId);
      }
      persistFavorites();
    });
  });

  if (favoritesCalendarDownload) {
    favoritesCalendarDownload.addEventListener('click', (event) => {
      if (favoriteSet.size > 0) return;
      event.preventDefault();
      setCalendarStatus('Veldu fyrst uppáhaldsfundi til að búa til dagatal.');
    });
  }

  if (favoritesCalendarSubscribe) {
    favoritesCalendarSubscribe.addEventListener('click', async () => {
      if (!favoriteSet.size) {
        setCalendarStatus('Veldu fyrst uppáhaldsfundi til að búa til dagatal.');
        return;
      }
      favoritesCalendarSubscribe.disabled = true;
      setCalendarStatus('Bý til áskriftarslóð...');
      try {
        const response = await fetch('/favorites-calendar-url', {
          method: 'GET',
          credentials: 'same-origin',
          cache: 'no-store',
          headers: { Accept: 'application/json' }
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || !payload.url) {
          throw new Error(payload.error || 'calendar-url-failed');
        }
        await copyText(payload.url);
        setCalendarStatus('Áskriftarslóð afrituð. Ef þú breytir uppáhaldsfundum þarftu að afrita hana aftur.');
      } catch (_error) {
        setCalendarStatus('Tókst ekki að búa til eða afrita áskriftarslóð.', true);
      } finally {
        updateCalendarActions();
      }
    });
  }

  const filtersForm = document.getElementById('filtersForm');
  const clearFiltersLink = document.getElementById('clearFiltersLink');
  const filterToggle = document.getElementById('filterToggle');
  const setFiltersCollapsed = (collapsed) => {
    if (!filtersForm || !filterToggle) return;
    filtersForm.classList.toggle('is-collapsed', collapsed);
    filterToggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    filterToggle.textContent = collapsed ? 'Sýna síur' : 'Fela síur';
  };

  if (filtersForm && filterToggle) {
    setFiltersCollapsed(true);
    filterToggle.addEventListener('click', () => {
      setFiltersCollapsed(!filtersForm.classList.contains('is-collapsed'));
    });
  }

  if (filtersForm) {
    filtersForm.addEventListener('submit', () => {
      const payload = {};
      Array.from(new FormData(filtersForm).entries()).forEach(([key, value]) => {
        if (key === 'view') return;
        const normalizedValue = String(value || '').trim();
        if (key === 'weekday') {
          return;
        }
        if (key === 'include_church' && normalizedValue === '1') {
          return;
        }
        payload[key] = normalizedValue;
      });
      setCookie(filtersCookieName, JSON.stringify(payload), 365);
    });
  }
  if (clearFiltersLink) {
    clearFiltersLink.addEventListener('click', () => {
      deleteCookie(filtersCookieName);
    });
  }

  syncFavoriteButtons();
})();

(function() {
  const storageKey = `aa-scroll:${window.location.pathname}${window.location.search}`;
  const postRestoreKey = `aa-post-restore:${window.location.pathname}${window.location.search}`;
  const scrollShell = document.querySelector('[data-scroll-restore="week-shell"]');
  window.__aaScrollRestoreState = { restored: false };
  const navigationEntry = performance.getEntriesByType && performance.getEntriesByType('navigation')[0];
  const shouldRestore = navigationEntry && navigationEntry.type === 'back_forward';

  try {
    if ('scrollRestoration' in history) {
      history.scrollRestoration = 'manual';
    }
  } catch (_error) {
    // noop
  }

  const capturePosition = () => ({
    top: window.scrollY || window.pageYOffset || 0,
    left: scrollShell ? scrollShell.scrollLeft : 0,
  });

  const applyPosition = (saved) => {
    const top = Number(saved.top || 0);
    const left = Number(saved.left || 0);
    window.scrollTo(0, top);
    if (scrollShell) {
      scrollShell.scrollLeft = left;
    }
    window.__aaScrollRestoreState.restored = true;
  };

  const restore = () => {
    try {
      const postRaw = sessionStorage.getItem(postRestoreKey);
      if (postRaw) {
        sessionStorage.removeItem(postRestoreKey);
        applyPosition(JSON.parse(postRaw));
        return;
      }
      if (!shouldRestore) return;
      const raw = sessionStorage.getItem(storageKey);
      if (!raw) return;
      applyPosition(JSON.parse(raw));
    } catch (_error) {
      // noop
    }
  };

  const persist = () => {
    sessionStorage.setItem(storageKey, JSON.stringify(capturePosition()));
  };

  document.querySelectorAll('form.size-form').forEach((form) => {
    form.addEventListener('submit', () => {
      sessionStorage.setItem(postRestoreKey, JSON.stringify(capturePosition()));
    });
  });

  window.addEventListener('pagehide', persist);
  window.addEventListener('beforeunload', persist);
  requestAnimationFrame(() => requestAnimationFrame(restore));
})();

(function() {
  const cards = Array.from(document.querySelectorAll('.slot-card'));
  const weekShell = document.querySelector('.week-scroll');
  if (!cards.length) return;

  const closeCards = (exceptCard = null) => {
    cards.forEach((card) => {
      if (card !== exceptCard) {
        card.classList.remove('is-open');
      }
    });
  };

  const positionTooltip = (card) => {
    const tooltip = card.querySelector('.slot-tooltip');
    if (!tooltip) return;

    tooltip.style.left = '10px';
    tooltip.style.right = 'auto';
    tooltip.style.top = 'calc(100% + 8px)';
    tooltip.style.bottom = 'auto';
    tooltip.style.transform = 'translateX(0)';

    const viewportPadding = 10;
    const boundaryRect = weekShell ? weekShell.getBoundingClientRect() : {
      left: viewportPadding,
      right: window.innerWidth - viewportPadding,
      top: viewportPadding,
      bottom: window.innerHeight - viewportPadding,
    };
    let rect = tooltip.getBoundingClientRect();
    let shiftX = 0;
    if (rect.right > boundaryRect.right - viewportPadding) {
      shiftX -= rect.right - (boundaryRect.right - viewportPadding);
    }
    if (rect.left + shiftX < boundaryRect.left + viewportPadding) {
      shiftX += (boundaryRect.left + viewportPadding) - (rect.left + shiftX);
    }
    tooltip.style.transform = `translateX(${shiftX}px)`;

    rect = tooltip.getBoundingClientRect();
    const cardRect = card.getBoundingClientRect();
    if (rect.bottom > boundaryRect.bottom - viewportPadding && cardRect.top - rect.height - 8 >= boundaryRect.top + viewportPadding) {
      tooltip.style.top = 'auto';
      tooltip.style.bottom = 'calc(100% + 8px)';
    }
  };

  cards.forEach((card) => {
    card.addEventListener('click', (event) => {
      if (event.target.closest('a, button')) return;
      const willOpen = !card.classList.contains('is-open');
      closeCards(card);
      card.classList.toggle('is-open', willOpen);
      if (willOpen) {
        positionTooltip(card);
      }
    });

    card.addEventListener('mouseenter', () => positionTooltip(card));
    card.addEventListener('focusin', () => {
      card.classList.add('is-open');
      positionTooltip(card);
    });
  });

  document.addEventListener('click', (event) => {
    if (!event.target.closest('.slot-card')) {
      closeCards();
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeCards();
    }
  });

  window.addEventListener('resize', () => {
    cards.filter((card) => card.classList.contains('is-open')).forEach(positionTooltip);
  });
})();

(function() {
  const board = document.querySelector('.week-board');
  const line = document.getElementById('nowLine');
  const label = document.getElementById('nowLineLabel');
  if (!board || !line || !label) return;
  let initialAutoScrollDone = false;

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

  const attemptInitialAutoScroll = () => {
    if (
      initialAutoScrollDone
      || (window.__aaScrollRestoreState && window.__aaScrollRestoreState.restored)
      || !line.classList.contains('visible')
    ) {
      return;
    }

    const currentWeekdayOrder = weekdayMap[weekdayFormatter.format(new Date())] || 0;
    if (visibleDayOrders.length !== 1 || visibleDayOrders[0] !== currentWeekdayOrder) {
      return;
    }

    const lineRect = line.getBoundingClientRect();
    const absoluteTop = window.scrollY + lineRect.top;
    const viewportOffset = Math.max(72, Math.round(window.innerHeight * 0.18));
    const targetTop = Math.max(0, absoluteTop - viewportOffset);
    initialAutoScrollDone = true;
    window.scrollTo(0, targetTop);
  };

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

    attemptInitialAutoScroll();
  }

  updateNowLine();
  requestAnimationFrame(() => requestAnimationFrame(attemptInitialAutoScroll));
  window.setTimeout(attemptInitialAutoScroll, 250);
  window.setTimeout(attemptInitialAutoScroll, 900);
  window.addEventListener('load', attemptInitialAutoScroll);
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


def sanitize_rows_for_render(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    sanitized_rows: list[dict[str, object]] = []
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
                u.gender_restriction,
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
    match = re.match(r"^(\d{1,2}):(\d{2})$", normalize_space(value))
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

    excerpt = " | ".join([part for part in excerpt_parts if part])
    return locator, excerpt


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
        if admin_section not in {"analytics", "locations", "church"}:
            admin_section = "analytics"
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
        admin_query_string = build_query_string(filters, overrides={"admin_section": admin_section}, exclude={"view"})
        locations_query_string = build_query_string(filters, overrides={"admin_section": "locations"}, exclude={"view"})
        church_query_string = build_query_string(filters, overrides={"admin_section": "church"}, exclude={"view"})
        current_query_string = request.query_string.decode("utf-8", errors="ignore").strip()
        location_rows = build_location_review_rows(df[df["source"].fillna("") != "kirkja"], "")
        location_clusters = build_location_clusters(location_rows)
        manual_events = load_manual_events(db_path)
        visit_summary_rows, recent_visit_rows, visit_totals = load_visit_summary(db_path)
        mapped_location_rows = [
            row for row in location_rows if int(row.get("has_location_mapping", 0)) or normalize_space(row.get("location_nickname"))
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
                week_day_count=len(displayed_week_days),
                total_count=len(filtered),
                scraped_at=format_scraped_at_short(scraped_at),
                options=options,
                filters=filters,
                source_counts=list(source_counts),
                csv_query_string=csv_query_string,
                list_query_string=list_query_string,
                week_query_string=week_query_string,
                admin_query_string=admin_query_string,
                locations_query_string=locations_query_string,
                church_query_string=church_query_string,
                current_query_string=current_query_string,
                default_weekday=current_iceland_weekday(),
                filters_cookie_name=FILTERS_COOKIE_NAME,
                favorites_cookie_name=FAVORITES_COOKIE_NAME,
                client_cookie_name=CLIENT_COOKIE_NAME,
                location_rows=location_rows,
                location_clusters=location_clusters,
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
        redirect_query = request.form.get("redirect_query", "").strip()
        if canonical_location_text:
            save_location_nickname(db_path, canonical_location_text, nickname)
        target = "/admin" + (f"?{redirect_query}" if redirect_query else "?admin_section=locations")
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
