from __future__ import annotations

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
      --week-time-column-width: 36px;
      --week-tooltip-tailroom: 220px;
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
    .hero-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }
    h1 { font-size: clamp(1.6rem, 3.2vw, 2.4rem); margin: 0 0 6px; line-height: 1.05; }
    h1 a {
      color: inherit;
      text-decoration: none;
    }
    h1 a:hover,
    h1 a:focus-visible {
      text-decoration: underline;
    }
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
      margin-top: 0;
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
    .filter-panel {
      margin-top: 10px;
    }
    .filter-panel.is-collapsed {
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
    .pill.is-live-now {
      background: #fef3c7;
      border-color: #f3d27a;
      color: #7c2d12;
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
      grid-template-columns: var(--week-time-column-width) repeat(7, minmax(120px, 1fr));
      position: relative;
      padding-bottom: var(--week-tooltip-tailroom);
    }
    .week-board.single-day {
      min-width: 0;
      grid-template-columns: var(--week-time-column-width) minmax(0, 1fr);
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
    .week-day-head {
      display: flex;
      align-items: center;
      justify-content: center;
      text-align: center;
      font-size: 1rem;
      line-height: 1.15;
      min-height: 44px;
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
    .time-cell-text {
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      white-space: nowrap;
      line-height: 1;
    }
    .week-cell {
      min-height: 80px;
      padding: 5px;
      border-bottom: 1px solid var(--border);
      border-right: 1px solid var(--border);
      display: flex;
      flex-direction: column;
      gap: 5px;
    }
    .week-cell.is-compact {
      min-height: 64px;
    }
    .slot-card {
      background: white;
      border: 1px solid #e7e0d2;
      border-radius: 14px;
      padding: 7px 40px 7px 8px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.04);
      position: relative;
      overflow: visible;
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
      z-index: 15;
    }
    .favorite-toggle.is-active {
      color: #9c6b00;
      background: #fff2bf;
      border-color: #d4a43d;
    }
    .favorite-toggle:hover {
      border-color: #c8b27d;
    }
    .slot-title-row {
      display: flex;
      align-items: center;
      gap: 6px;
      min-width: 0;
      position: relative;
      z-index: 15;
    }
    .slot-title {
      margin: 0;
      font-size: 0.9rem;
      line-height: 1.2;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      min-width: 0;
      flex: 1 1 auto;
    }
    .live-dot {
      position: relative;
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: #dc2626;
      box-shadow: 0 0 0 0 rgba(220, 38, 38, 0.45);
      flex: 0 0 auto;
      z-index: 15;
    }
    .live-dot.is-blinking {
      animation: live-dot-pulse 1.35s ease-out infinite;
    }
    @keyframes live-dot-pulse {
      0% {
        opacity: 0.95;
        transform: scale(0.92);
        box-shadow: 0 0 0 0 rgba(220, 38, 38, 0.45);
      }
      55% {
        opacity: 1;
        transform: scale(1);
        box-shadow: 0 0 0 5px rgba(220, 38, 38, 0.08);
      }
      100% {
        opacity: 0.5;
        transform: scale(0.92);
        box-shadow: 0 0 0 0 rgba(220, 38, 38, 0);
      }
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
      .hero-top {
        align-items: stretch;
      }
      :root {
        --week-time-column-width: 30px;
        --week-tooltip-tailroom: 180px;
      }
      .week-board {
        min-width: 700px;
        grid-template-columns: var(--week-time-column-width) repeat(7, minmax(104px, 1fr));
      }
      .week-board.single-day {
        min-width: 0;
        grid-template-columns: var(--week-time-column-width) minmax(0, 1fr);
      }
      .week-head {
        padding: 6px 4px;
        font-size: 0.8rem;
      }
      .week-day-head {
        font-size: 0.92rem;
        min-height: 40px;
      }
      .time-cell {
        font-size: 0.62rem;
        padding: 3px 0;
      }
      .week-cell {
        min-height: 72px;
        padding: 3px;
        gap: 3px;
      }
      .week-cell.is-compact {
        min-height: 56px;
      }
      .slot-card {
        padding: 6px 36px 6px 6px;
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
      left: calc(var(--week-time-column-width) + 1px);
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
    .now-line-status {
      position: absolute;
      top: 8px;
      right: 8px;
      z-index: 13;
      pointer-events: none;
      display: none;
    }
    .now-line-status.visible {
      display: block;
    }
    .now-line-label {
      position: static;
      display: inline-block;
      background: #d62828;
      color: white;
      font-size: 0.72rem;
      line-height: 1;
      padding: 4px 7px;
      border-radius: 999px;
      box-shadow: 0 6px 14px rgba(214, 40, 40, 0.22);
      white-space: nowrap;
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
    .match-reasons {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 6px;
    }
    .match-reasons span {
      padding: 4px 7px;
      border-radius: 999px;
      background: var(--pill);
      color: #0f172a;
      font-size: 0.8rem;
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
      <div class="hero-top">
        <h1><a href="/">Fundaskrá</a></h1>
        <div class="filter-panel-header">
          <button type="button" class="filter-toggle" id="filterToggle" aria-expanded="false">Síur og meira</button>
        </div>
      </div>
      <div class="filter-panel" id="filterPanel">
      <p class="meta">AA, fjarfundir og kirkjusamkomur. Uppfært {{ scraped_at }}</p>
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
        <button type="button" id="favoritesCodeCopy">Afrita favorites-kóða</button>
        <button type="button" id="favoritesCodeImport">Líma favorites-kóða</button>
        <span class="calendar-status" id="favoritesCalendarStatus" aria-live="polite"></span>
      </div>
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
          <a href="/admin?{{ duplicates_query_string }}">Tvítektir</a>
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
          <a href="/admin?{{ duplicates_query_string }}">Tvítektir</a>
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
    {% elif admin_section == "duplicates" %}
    <section class="mapping-section">
      <article class="mapping-card">
        <h3>Admin</h3>
        <div class="admin-nav">
          <a href="/admin?{{ admin_query_string }}">Yfirlit</a>
          <a href="/admin?{{ locations_query_string }}">Staðamöppun</a>
          <a href="/admin?{{ duplicates_query_string }}">Tvítektir</a>
          <a href="/admin?{{ church_query_string }}">Kirkjuskráning</a>
          <form method="post" action="/admin/logout">
            <button type="submit">Útskrá</button>
          </form>
        </div>
      </article>
      <article class="mapping-card">
        <h3>Mögulegar tvítektir á fundum</h3>
        <p class="mapping-meta">Þessi listi sýnir sterkustu fuzzy vísbendingarnar um að sami fundur sé skráður frá fleiri en einni síðu. Leitað er að sama vikudegi, sama eða mjög nálægum tíma og svipuðu heiti ásamt sama stað eða sama Zoom auðkenni.</p>
        <div class="summary">
          <span>{{ duplicate_review_rows|length }} pör fundust</span>
          <span>Aðeins mismunandi source-ar</span>
          <span>Hámark 80 pör</span>
        </div>
        {% if duplicate_review_rows %}
        <table class="mapping-table">
          <thead>
            <tr>
              <th>Líkur</th>
              <th>Ástæður</th>
              <th>Fundur A</th>
              <th>Fundur B</th>
            </tr>
          </thead>
          <tbody>
            {% for item in duplicate_review_rows %}
            <tr>
              <td>
                <strong>{{ item["confidence_label"] }}</strong>
                <div class="mapping-meta">Score {{ "%.3f"|format(item["score"]) }}</div>
                <div class="mapping-meta">{{ item["weekday_is"] }} {{ item["time_display"] }}</div>
              </td>
              <td>
                <div class="match-reasons">
                  {% for reason in item["match_reasons"] %}
                  <span>{{ reason }}</span>
                  {% endfor %}
                </div>
              </td>
              <td>
                <strong>{{ item["left"]["meeting_name_display"] }}</strong>
                <div class="mapping-meta">{{ item["left"]["source"] }} · {{ item["left"]["fellowship_display"] }}</div>
                {% if item["left"]["location_display"] %}<div class="mapping-meta">{{ item["left"]["location_display"] }}</div>{% endif %}
                {% if item["left"]["venue_text"] %}<div class="mapping-meta">{{ item["left"]["venue_text"] }}</div>{% endif %}
                <div class="mapping-meta">
                  <a href="{{ item["left"]["source_page_url"] }}" target="_blank" rel="noreferrer">Upprunasíða</a>
                  {% if item["left"]["source_locator"] %} · {{ item["left"]["source_locator"] }}{% endif %}
                </div>
                {% if item["left"]["source_excerpt"] %}<div class="mapping-meta">{{ item["left"]["source_excerpt"] }}</div>{% endif %}
              </td>
              <td>
                <strong>{{ item["right"]["meeting_name_display"] }}</strong>
                <div class="mapping-meta">{{ item["right"]["source"] }} · {{ item["right"]["fellowship_display"] }}</div>
                {% if item["right"]["location_display"] %}<div class="mapping-meta">{{ item["right"]["location_display"] }}</div>{% endif %}
                {% if item["right"]["venue_text"] %}<div class="mapping-meta">{{ item["right"]["venue_text"] }}</div>{% endif %}
                <div class="mapping-meta">
                  <a href="{{ item["right"]["source_page_url"] }}" target="_blank" rel="noreferrer">Upprunasíða</a>
                  {% if item["right"]["source_locator"] %} · {{ item["right"]["source_locator"] }}{% endif %}
                </div>
                {% if item["right"]["source_excerpt"] %}<div class="mapping-meta">{{ item["right"]["source_excerpt"] }}</div>{% endif %}
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
        <p class="mapping-meta">Engin sterk duplicate-pör fundust með núverandi heuristics.</p>
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
          <a href="/admin?{{ duplicates_query_string }}">Tvítektir</a>
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
        <div class="now-line-status" id="nowLineStatus">
          <span class="now-line-label" id="nowLineLabel">Núna</span>
        </div>
        <div class="now-line" id="nowLine">
        </div>
        <div class="week-head" aria-hidden="true"></div>
        {% for day in week_days %}
        <div class="week-head week-day-head">{{ day }}</div>
        {% endfor %}
        {% for slot in week_slots %}
        <div class="time-cell" data-time-label="{{ slot["time_label"] }}"><span class="time-cell-text">{{ slot["time_label"] }}</span></div>
        {% for cell in slot["cells"] %}
        <div class="week-cell{% if slot["is_compact"] %} is-compact{% endif %}">
          {% for row in cell %}
          <article class="slot-card{% if row["format"] == "Fjarfundur" %} is-remote{% endif %}{% if row["gender_restriction"] == "Konur" %} is-women{% elif row["gender_restriction"] == "Karlar" %} is-men{% endif %}{% if row["is_favorite"] %} is-favorite{% endif %}" tabindex="0" data-meeting-id="{{ row["source_uid"] }}">
            <button class="favorite-toggle{% if row["is_favorite"] %} is-active{% endif %}" type="button" data-meeting-id="{{ row["source_uid"] }}" aria-label="Setja fund í uppáhald">★</button>
            <div class="slot-title-row">
              <h3 class="slot-title">{{ row["meeting_name_display"] }}</h3>
              {% if row["is_live_now"] %}<span class="live-dot is-blinking" title="Í gangi núna" aria-hidden="true"></span>{% endif %}
            </div>
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
          {% if row["is_live_now"] %}<span class="pill is-live-now">Í gangi núna</span>{% endif %}
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
  const favoritesCodeCopy = document.getElementById('favoritesCodeCopy');
  const favoritesCodeImport = document.getElementById('favoritesCodeImport');
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
    if (favoritesCodeCopy) {
      favoritesCodeCopy.disabled = !hasFavorites;
      favoritesCodeCopy.classList.toggle('is-disabled', !hasFavorites);
      favoritesCodeCopy.setAttribute('title', hasFavorites ? 'Afrita kóða til að flytja uppáhaldsfundi í annan browser' : 'Veldu fyrst uppáhaldsfundi');
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

  const encodeBase64Url = (value) => {
    const bytes = new TextEncoder().encode(value);
    let binary = '';
    bytes.forEach((byte) => {
      binary += String.fromCharCode(byte);
    });
    return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/g, '');
  };

  const decodeBase64Url = (value) => {
    const normalized = String(value || '').trim().replace(/-/g, '+').replace(/_/g, '/');
    const padded = normalized + '==='.slice((normalized.length + 3) % 4);
    const binary = atob(padded);
    const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
    return new TextDecoder().decode(bytes);
  };

  const buildFavoritesTransferCode = () => {
    const payload = JSON.stringify(Array.from(favoriteSet).sort());
    return `fav1.${encodeBase64Url(payload)}`;
  };

  const parseFavoritesTransferCode = (value) => {
    const raw = String(value || '').trim();
    if (!raw) {
      throw new Error('empty-code');
    }
    const [prefix, encoded] = raw.split('.', 2);
    if (prefix !== 'fav1' || !encoded) {
      throw new Error('invalid-prefix');
    }
    const parsed = JSON.parse(decodeBase64Url(encoded));
    if (!Array.isArray(parsed)) {
      throw new Error('invalid-payload');
    }
    return parsed
      .filter((item) => typeof item === 'string' && item.trim())
      .map((item) => item.trim())
      .filter((item, index, items) => items.indexOf(item) === index)
      .slice(0, maxFavorites);
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

  if (favoritesCodeCopy) {
    favoritesCodeCopy.addEventListener('click', async () => {
      if (!favoriteSet.size) {
        setCalendarStatus('Veldu fyrst uppáhaldsfundi til að afrita favorites-kóða.');
        return;
      }
      try {
        await copyText(buildFavoritesTransferCode());
        setCalendarStatus('Favorites-kóði afritaður. Límdu hann inn í öðrum browser.');
      } catch (_error) {
        setCalendarStatus('Tókst ekki að afrita favorites-kóða.', true);
      }
    });
  }

  if (favoritesCodeImport) {
    favoritesCodeImport.addEventListener('click', () => {
      const entered = window.prompt('Límdu inn favorites-kóðann hér:');
      if (entered === null) return;
      try {
        const imported = parseFavoritesTransferCode(entered);
        favoriteSet.clear();
        imported.forEach((meetingId) => favoriteSet.add(meetingId));
        persistFavorites();
        window.location.reload();
      } catch (_error) {
        setCalendarStatus('Favorites-kóðinn var ekki gildur.', true);
      }
    });
  }

  const filtersForm = document.getElementById('filtersForm');
  const filterPanel = document.getElementById('filterPanel');
  const clearFiltersLink = document.getElementById('clearFiltersLink');
  const filterToggle = document.getElementById('filterToggle');
  const setFiltersCollapsed = (collapsed) => {
    if (!filterPanel || !filterToggle) return;
    filterPanel.classList.toggle('is-collapsed', collapsed);
    filterToggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    filterToggle.textContent = collapsed ? 'Síur og meira' : 'Fela síur og meira';
  };

  if (filterPanel && filterToggle) {
    setFiltersCollapsed(true);
    filterToggle.addEventListener('click', () => {
      setFiltersCollapsed(!filterPanel.classList.contains('is-collapsed'));
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
  const status = document.getElementById('nowLineStatus');
  const label = document.getElementById('nowLineLabel');
  if (!board || !line || !status || !label) return;
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
      status.classList.remove('visible');
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
      status.classList.remove('visible');
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
    status.classList.add('visible');

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
