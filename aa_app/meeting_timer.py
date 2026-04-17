from __future__ import annotations

import json

from flask import Blueprint, Flask, render_template_string

DEFAULT_PRESETS = [
    {
        "id": "chair-15",
        "label": "Leiðari 15",
        "duration_minutes": 15,
        "warning_minutes": 5,
    },
    {
        "id": "chair-10",
        "label": "Leiðari 10",
        "duration_minutes": 10,
        "warning_minutes": 5,
    },
    {
        "id": "share-3",
        "label": "Tjáning 3",
        "duration_minutes": 3,
        "warning_minutes": 0,
    },
]

TIMER_TEMPLATE = """
<!doctype html>
<html lang="is">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>{{ page_title }}</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #050607;
      --panel: #090c0f;
      --panel-2: #0d1114;
      --line: #12171b;
      --text-soft: #454e57;
      --text-mid: #606a74;
      --text-strong: #bcc6ce;
      --text-hot: #f2f5f7;
      --bar-track: #0f1316;
      --bar-fill: #232b31;
      --bar-final: #7d8a96;
      --bar-expired: #f4f7fa;
      --button: #0d1114;
      --button-active: #151b20;
      --button-text: #87919b;
      --danger: #ffffff;
      --shadow: 0 18px 48px rgba(0, 0, 0, 0.38);
    }
    * { box-sizing: border-box; }
    html, body { min-height: 100%; }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top center, rgba(255, 255, 255, 0.03), transparent 28%),
        linear-gradient(180deg, #07090b 0%, var(--bg) 100%);
      color: var(--text-soft);
    }
    body[data-phase="final"] {
      color: var(--text-strong);
    }
    body[data-phase="expired"] {
      color: var(--text-hot);
    }
    .app {
      min-height: 100vh;
      max-width: 540px;
      margin: 0 auto;
      padding:
        max(10px, env(safe-area-inset-top))
        12px
        max(12px, env(safe-area-inset-bottom))
        12px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .topbar a,
    .topbar button,
    summary {
      color: var(--text-soft);
      background: transparent;
      border: 0;
      padding: 0;
      font: inherit;
      text-decoration: none;
      cursor: pointer;
    }
    .topbar a:hover,
    .topbar a:focus-visible,
    .topbar button:hover,
    .topbar button:focus-visible,
    summary:hover,
    summary:focus-visible {
      color: var(--text-soft);
    }
    .hero {
      border: 1px solid var(--line);
      border-radius: 28px;
      background: linear-gradient(180deg, rgba(18, 22, 26, 0.98), rgba(10, 13, 16, 0.98));
      box-shadow: var(--shadow);
      padding: 14px;
    }
    .label {
      margin: 0;
      color: var(--text-soft);
      font-size: 0.9rem;
      text-transform: uppercase;
      letter-spacing: 0.16em;
    }
    .title-row {
      margin-top: 6px;
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 8px;
    }
    .title-row h1 {
      margin: 0;
      color: #56606a;
      font-size: clamp(1.45rem, 7vw, 2rem);
      line-height: 1;
    }
    .phase {
      color: #4c555f;
      font-size: 0.95rem;
      white-space: nowrap;
    }
    .status {
      min-height: 1.3em;
      text-align: center;
      color: #4b545d;
      font-size: 0.95rem;
    }
    body[data-phase="final"] .status,
    body[data-phase="expired"] .status {
      color: var(--text-strong);
    }
    .bar-wrap {
      margin-top: 12px;
      height: min(21vh, 124px);
      min-height: 78px;
      border-radius: 24px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, #080c0f 0%, var(--bar-track) 100%);
      overflow: hidden;
      position: relative;
    }
    .bar-fill {
      position: absolute;
      inset: 0 auto 0 0;
      width: 0;
      background: linear-gradient(90deg, #1b2228 0%, var(--bar-fill) 100%);
      transition: width 180ms linear;
    }
    .bar-wrap.has-active-frame {
      border-color: #3f5b46;
      box-shadow: 0 0 0 3px rgba(82, 130, 94, 0.22), 0 0 0 7px rgba(82, 130, 94, 0.06);
    }
    .bar-wrap.has-reminder-frame {
      border-color: #7a5a31;
      box-shadow: 0 0 0 3px rgba(255, 157, 47, 0.34), 0 0 0 7px rgba(255, 157, 47, 0.08);
    }
    .bar-wrap.has-reminder-frame.is-final-frame {
      animation: reminder-frame-blink 0.8s steps(2, end) infinite;
    }
    .bar-wrap.is-expired-frame {
      border-color: #ff4b4b;
      box-shadow: 0 0 0 4px rgba(255, 75, 75, 0.98), 0 0 0 9px rgba(255, 75, 75, 0.28);
      animation: expired-frame-blink 0.6s steps(2, end) infinite;
    }
    .bar-fill.is-final {
      background: linear-gradient(90deg, #4f5963 0%, var(--bar-final) 100%);
      animation: final-blink 0.8s linear infinite;
    }
    .bar-fill.is-expired {
      width: 100% !important;
      background: linear-gradient(90deg, #bfc9d2 0%, var(--bar-expired) 100%);
      animation: expired-blink 0.7s linear infinite;
    }
    .bar-overlay {
      position: absolute;
      inset: 0;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      gap: 6px;
      pointer-events: none;
    }
    .bar-time {
      color: #7c8791;
      font-size: clamp(4rem, 21vw, 6.5rem);
      line-height: 0.86;
      letter-spacing: -0.08em;
      font-variant-numeric: tabular-nums;
      position: relative;
      z-index: 1;
    }
    .bar-label {
      color: rgba(255, 255, 255, 0.14);
      font-size: clamp(0.9rem, 4vw, 1.1rem);
      text-transform: uppercase;
      letter-spacing: 0.18em;
      position: relative;
      z-index: 2;
    }
    body[data-phase="warning"] .bar-label {
      color: rgba(255, 176, 74, 0.42);
    }
    body[data-phase="final"] .bar-time {
      color: var(--text-strong);
    }
    body[data-phase="expired"] .bar-time {
      color: var(--text-hot);
      animation: expired-text 0.45s steps(2, end) infinite;
      opacity: 0.62;
    }
    body[data-phase="final"] .bar-label,
    body[data-phase="expired"] .bar-label {
      color: rgba(255, 93, 93, 0.82);
    }
    body[data-phase="expired"] .bar-label {
      position: absolute;
      left: 50%;
      top: 50%;
      transform: translate(-50%, -50%);
      font-size: clamp(2.8rem, 14vw, 4.9rem);
      font-weight: 700;
      letter-spacing: -0.04em;
      text-transform: none;
      line-height: 0.9;
      color: rgba(255, 93, 93, 0.94);
      text-shadow: 0 0 18px rgba(255, 93, 93, 0.18);
      white-space: nowrap;
    }
    .meta {
      margin-top: 8px;
      display: flex;
      justify-content: space-between;
      gap: 8px;
      color: #4b545d;
      font-size: 0.82rem;
    }
    .preset-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }
    .preset-button,
    .control-button,
    .adjust-button,
    .tool-button,
    .settings-button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: linear-gradient(180deg, #0d1114 0%, #0b0f12 100%);
      color: var(--button-text);
      font: inherit;
      padding: 12px 11px;
      cursor: pointer;
    }
    .preset-button.is-active,
    .tool-button.is-active {
      background: var(--button-active);
      border-color: #2a333c;
      color: #aab4bd;
    }
    .control-button.primary {
      background: linear-gradient(180deg, #13241a 0%, #0d1711 100%);
      border-color: #6cab7b;
      box-shadow: inset 0 0 0 1px rgba(150, 211, 166, 0.22);
      color: #c9ddcf;
    }
    .preset-button {
      padding: 13px 9px;
      min-height: 74px;
    }
    .preset-name {
      display: block;
      font-size: 1rem;
      margin-bottom: 6px;
      color: inherit;
    }
    .preset-detail {
      display: block;
      color: var(--text-soft);
      font-size: 0.82rem;
      line-height: 1.35;
    }
    .controls,
    .adjustments,
    .tools {
      display: grid;
      gap: 8px;
    }
    .controls {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .adjustments {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .tools {
      grid-template-columns: 1fr;
    }
    .panel {
      border: 1px solid var(--line);
      border-radius: 24px;
      background: linear-gradient(180deg, rgba(10, 13, 16, 0.98), rgba(8, 10, 13, 0.98));
      box-shadow: var(--shadow);
      padding: 12px;
    }
    .panel h2 {
      margin: 0 0 10px;
      font-size: 0.95rem;
      color: #56606a;
    }
    details {
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(8, 11, 13, 0.98);
      overflow: hidden;
    }
    summary {
      list-style: none;
      padding: 12px;
      color: #56606a;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    summary::-webkit-details-marker { display: none; }
    .settings {
      padding: 0 12px 12px;
      display: grid;
      gap: 10px;
    }
    .settings-card {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 10px;
      background: rgba(11, 14, 17, 0.98);
    }
    .settings-card strong {
      display: block;
      color: #59636d;
      margin-bottom: 10px;
      font-size: 0.96rem;
    }
    .settings-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .settings-field {
      display: grid;
      gap: 5px;
      color: #4b545d;
      font-size: 0.82rem;
    }
    .settings-field input {
      width: 100%;
      min-width: 0;
      border: 1px solid #1a2025;
      border-radius: 14px;
      background: #0b0f12;
      color: #aab4bd;
      padding: 11px 12px;
      font: inherit;
    }
    .settings-actions {
      margin-top: 10px;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .settings-actions.single {
      grid-template-columns: 1fr;
    }
    .footer-note {
      color: #4a535c;
      font-size: 0.82rem;
      line-height: 1.4;
    }
    @media (max-width: 560px) {
      .app {
        gap: 8px;
      }
      .hero {
        padding: 12px;
      }
      .bar-wrap {
        height: min(18vh, 104px);
        min-height: 72px;
      }
      .bar-time {
        font-size: clamp(3.3rem, 18vw, 5.1rem);
      }
      .meta {
        flex-wrap: wrap;
        row-gap: 4px;
      }
      .panel {
        padding: 10px;
      }
      .preset-button {
        min-height: 68px;
      }
    }
    @keyframes reminder-frame-blink {
      0%, 49% {
        box-shadow: 0 0 0 4px rgba(255, 157, 47, 0.98), 0 0 0 9px rgba(255, 157, 47, 0.28);
        border-color: #ffb458;
      }
      50%, 100% {
        box-shadow: 0 0 0 4px rgba(255, 157, 47, 0.22), 0 0 0 9px rgba(255, 157, 47, 0.08);
        border-color: #7f5d31;
      }
    }
    @keyframes expired-frame-blink {
      0%, 49% {
        box-shadow: 0 0 0 4px rgba(255, 75, 75, 0.98), 0 0 0 9px rgba(255, 75, 75, 0.32);
        border-color: #ff7c7c;
      }
      50%, 100% {
        box-shadow: 0 0 0 4px rgba(255, 75, 75, 0.22), 0 0 0 9px rgba(255, 75, 75, 0.08);
        border-color: #7f3232;
      }
    }
    @keyframes final-blink {
      0%, 100% { opacity: 0.48; }
      50% { opacity: 1; }
    }
    @keyframes expired-blink {
      0%, 100% { opacity: 0.58; }
      50% { opacity: 1; }
    }
    @keyframes expired-text {
      0%, 100% { opacity: 0.65; }
      50% { opacity: 1; }
    }
  </style>
</head>
<body data-phase="idle">
  <main class="app">
    <div class="topbar">
      <a href="{{ home_href }}">{{ home_label }}</a>
      <button type="button" id="fullscreenButton">Fullscreen</button>
    </div>

    <section class="hero">
      <p class="label">Fundastund</p>
      <div class="title-row">
        <h1 id="activeLabel">Leiðari 15</h1>
        <div class="phase" id="phaseLabel">Tilbúið</div>
      </div>
      <div class="status" id="statusText">Veldu forstillingu og ýttu á Byrja.</div>
      <div class="bar-wrap" id="progressWrap" aria-hidden="true">
        <div class="bar-fill" id="progressFill"></div>
        <div class="bar-overlay">
          <div class="bar-time" id="countdown">15:00</div>
          <div class="bar-label" id="barOverlay">Tímataka</div>
        </div>
      </div>
      <div class="meta">
        <span id="warningMeta">Áminning 5 mín</span>
        <span id="totalMeta">Gefið 00:20</span>
        <span id="elapsedMeta">Liðið 00:00</span>
        <span id="deadlineMeta">Ekki hafið</span>
      </div>
    </section>

    <section class="panel">
      <h2>Forstillingar</h2>
      <div class="preset-grid" id="presetGrid"></div>
    </section>

    <section class="panel">
      <h2>Stjórnun</h2>
      <div class="controls">
        <button type="button" class="control-button primary" id="startButton">Byrja</button>
        <button type="button" class="control-button" id="resetButton">Endurstilla</button>
      </div>
      <div class="adjustments" style="margin-top:10px;">
        <button type="button" class="adjust-button" id="plusOneButton">+1 mín</button>
        <button type="button" class="adjust-button" id="plusFiveButton">+5 mín</button>
        <button type="button" class="adjust-button" id="plusTenButton">+10 mín</button>
      </div>
      <div class="tools" style="margin-top:10px;">
        <button type="button" class="tool-button is-active" id="wakeLockButton">Skjár vakandi</button>
      </div>
    </section>

    <details>
      <summary>
        <span>Stillingar</span>
        <span id="settingsHint">Forstillingar og custom tímar</span>
      </summary>
      <div class="settings">
        <div class="settings-card" id="presetSettings"></div>

        <div class="settings-card">
          <strong>Custom tími</strong>
          <div class="settings-grid">
            <label class="settings-field">
              Mínútur
              <input type="number" id="customDurationInput" min="0" max="180" step="1" value="5" inputmode="numeric">
            </label>
            <label class="settings-field">
              Sekúndur
              <input type="number" id="customDurationSecondsInput" min="0" max="59" step="1" value="0" inputmode="numeric">
            </label>
            <label class="settings-field">
              Áminning mín
              <input type="number" id="customWarningInput" min="0" max="180" step="1" value="1" inputmode="numeric">
            </label>
            <label class="settings-field">
              Áminning sek
              <input type="number" id="customWarningSecondsInput" min="0" max="59" step="1" value="0" inputmode="numeric">
            </label>
          </div>
          <div class="settings-actions single">
            <button type="button" class="settings-button" id="applyCustomButton">Nota custom tíma</button>
          </div>
        </div>

        <div class="settings-card">
          <strong>Verkfæri</strong>
          <div class="settings-actions single">
            <button type="button" class="settings-button" id="resetPresetsButton">Endurheimta sjálfgefnar forstillingar</button>
          </div>
        </div>

        <div class="footer-note">
          Timerinn reynir að nota Screen Wake Lock API meðan hann er í gangi. Fullscreen er í boði ef síminn leyfir það.
        </div>
      </div>
    </details>
  </main>

  <script>
    const defaultPresets = {{ presets_json | safe }};

    (() => {
      const STORAGE_KEY = 'aa-meeting-timer-presets-v3';
      const elements = {
        body: document.body,
        activeLabel: document.getElementById('activeLabel'),
        phaseLabel: document.getElementById('phaseLabel'),
        countdown: document.getElementById('countdown'),
        statusText: document.getElementById('statusText'),
        progressWrap: document.getElementById('progressWrap'),
        progressFill: document.getElementById('progressFill'),
        barOverlay: document.getElementById('barOverlay'),
        warningMeta: document.getElementById('warningMeta'),
        totalMeta: document.getElementById('totalMeta'),
        elapsedMeta: document.getElementById('elapsedMeta'),
        deadlineMeta: document.getElementById('deadlineMeta'),
        presetGrid: document.getElementById('presetGrid'),
        presetSettings: document.getElementById('presetSettings'),
        settingsHint: document.getElementById('settingsHint'),
        startButton: document.getElementById('startButton'),
        resetButton: document.getElementById('resetButton'),
        plusOneButton: document.getElementById('plusOneButton'),
        plusFiveButton: document.getElementById('plusFiveButton'),
        plusTenButton: document.getElementById('plusTenButton'),
        wakeLockButton: document.getElementById('wakeLockButton'),
        fullscreenButton: document.getElementById('fullscreenButton'),
        customDurationInput: document.getElementById('customDurationInput'),
        customDurationSecondsInput: document.getElementById('customDurationSecondsInput'),
        customWarningInput: document.getElementById('customWarningInput'),
        customWarningSecondsInput: document.getElementById('customWarningSecondsInput'),
        applyCustomButton: document.getElementById('applyCustomButton'),
        resetPresetsButton: document.getElementById('resetPresetsButton'),
      };

      const timeFormatter = new Intl.DateTimeFormat('is-IS', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZone: 'Atlantic/Reykjavik',
      });

      function clampInteger(value, fallback, min, max) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) {
          return fallback;
        }
        return Math.max(min, Math.min(max, Math.round(parsed)));
      }

      function readMinuteSecondPair(minutesValue, secondsValue, fallbackMinutes, fallbackSeconds, minTotalSeconds, maxTotalSeconds) {
        const minutes = clampInteger(minutesValue, fallbackMinutes, 0, Math.floor(maxTotalSeconds / 60));
        const seconds = clampInteger(secondsValue, fallbackSeconds, 0, 59);
        return Math.max(minTotalSeconds, Math.min(maxTotalSeconds, (minutes * 60) + seconds));
      }

      function normalizePreset(rawPreset, fallbackPreset) {
        const duration = clampInteger(
          rawPreset?.duration_minutes,
          fallbackPreset.duration_minutes,
          1,
          180,
        );
        const warning = clampInteger(
          rawPreset?.warning_minutes,
          fallbackPreset.warning_minutes,
          0,
          duration,
        );
        return {
          id: fallbackPreset.id,
          label: fallbackPreset.label,
          duration_minutes: duration,
          warning_minutes: warning,
        };
      }

      function loadPresetConfigs() {
        try {
          const raw = window.localStorage.getItem(STORAGE_KEY);
          const parsed = raw ? JSON.parse(raw) : [];
          const byId = new Map(Array.isArray(parsed) ? parsed.map((item) => [String(item.id || ''), item]) : []);
          return defaultPresets.map((fallback) => normalizePreset(byId.get(fallback.id), fallback));
        } catch (_error) {
          return defaultPresets.map((preset) => normalizePreset(preset, preset));
        }
      }

      let presets = loadPresetConfigs();

      function savePresetConfigs() {
        try {
          window.localStorage.setItem(STORAGE_KEY, JSON.stringify(presets));
        } catch (_error) {
          // noop
        }
      }

      const initialTestSeconds = 20;
      const initialTestReminderSeconds = 15;

      const state = {
        selectedPresetId: '',
        label: 'Prufa 20 sek',
        totalSeconds: initialTestSeconds,
        remainingSeconds: initialTestSeconds,
        warningSeconds: initialTestReminderSeconds,
        deadlineAt: null,
        timerId: null,
        wakeLockEnabled: true,
        wakeLock: null,
        wakeLockSupported: 'wakeLock' in navigator,
      };

      elements.customDurationInput.value = '0';
      elements.customDurationSecondsInput.value = String(initialTestSeconds);
      elements.customWarningInput.value = '0';
      elements.customWarningSecondsInput.value = String(initialTestReminderSeconds);

      function getSelectedPreset() {
        return presets.find((item) => item.id === state.selectedPresetId) || null;
      }

      function formatClock(totalSeconds) {
        const absoluteSeconds = Math.abs(totalSeconds);
        const hours = Math.floor(absoluteSeconds / 3600);
        const minutes = Math.floor((absoluteSeconds % 3600) / 60);
        const seconds = absoluteSeconds % 60;
        const prefix = totalSeconds < 0 ? '-' : '';
        if (hours > 0) {
          return `${prefix}${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
        }
        return `${prefix}${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
      }

      function countdownDisplaySeconds() {
        if (state.remainingSeconds < 0) {
          return state.remainingSeconds + 1;
        }
        return state.remainingSeconds;
      }

      function formatMinutesLabel(minutes) {
        return `${minutes} mín`;
      }

      function currentPhase() {
        if (state.remainingSeconds < 0) {
          return 'expired';
        }
        if (
          state.warningSeconds > 0
          && state.remainingSeconds <= state.warningSeconds
          && state.remainingSeconds > 10
          && (state.deadlineAt !== null || state.remainingSeconds !== state.totalSeconds)
        ) {
          return 'warning';
        }
        if (state.remainingSeconds <= 10 && (state.deadlineAt !== null || state.remainingSeconds !== state.totalSeconds)) {
          return 'final';
        }
        return 'idle';
      }

      function deadlineText() {
        if (state.deadlineAt === null) {
          return 'Ekki hafið';
        }
        const deadline = new Date(state.deadlineAt);
        if (state.remainingSeconds >= 0) {
          return `Endar ${timeFormatter.format(deadline)}`;
        }
        return `Fór yfir ${timeFormatter.format(deadline)}`;
      }

      function elapsedSinceStartSeconds() {
        return Math.max(0, state.totalSeconds - state.remainingSeconds);
      }

      function statusText() {
        if (state.remainingSeconds < 0) {
          return 'Tími liðinn';
        }
        if (state.deadlineAt === null && state.remainingSeconds === state.totalSeconds) {
          return 'Veldu forstillingu og ýttu á Byrja.';
        }
        if (state.deadlineAt === null) {
          return 'Timer í pásu';
        }
        if (state.remainingSeconds <= 10) {
          return 'Lokaniðurtalning';
        }
        if (state.warningSeconds > 0 && state.remainingSeconds <= state.warningSeconds) {
          return 'Áminning';
        }
        return 'Tímataka í gangi';
      }

      function phaseLabelText() {
        if (state.remainingSeconds < 0) return 'Útrunnið';
        if (state.deadlineAt === null && state.remainingSeconds === state.totalSeconds) return 'Tilbúið';
        if (state.deadlineAt === null) return 'Í pásu';
        if (state.warningSeconds > 0 && state.remainingSeconds <= state.warningSeconds && state.remainingSeconds > 10) return 'Áminning';
        if (state.remainingSeconds <= 10) return '10 sek eftir';
        return 'Í gangi';
      }

      function renderPresetButtons() {
        elements.presetGrid.innerHTML = presets.map((preset) => `
          <button type="button" class="preset-button${preset.id === state.selectedPresetId ? ' is-active' : ''}" data-preset-id="${preset.id}">
            <span class="preset-name">${preset.label}</span>
            <span class="preset-detail">${preset.duration_minutes} mín · áminn. ${preset.warning_minutes} mín</span>
          </button>
        `).join('');
      }

      function renderPresetSettings() {
        elements.presetSettings.innerHTML = presets.map((preset) => `
          <div class="settings-card" data-preset-id="${preset.id}">
            <strong>${preset.label}</strong>
            <div class="settings-grid">
              <label class="settings-field">
                Mínútur
                <input type="number" name="duration_minutes" min="1" max="180" step="1" value="${preset.duration_minutes}" inputmode="numeric">
              </label>
              <label class="settings-field">
                Áminning
                <input type="number" name="warning_minutes" min="0" max="180" step="1" value="${preset.warning_minutes}" inputmode="numeric">
              </label>
            </div>
            <div class="settings-actions">
              <button type="button" class="settings-button" data-save-preset-id="${preset.id}">Vista</button>
              <button type="button" class="settings-button" data-use-preset-id="${preset.id}">Nota</button>
            </div>
          </div>
        `).join('');
      }

      function syncProgressBar() {
        const phase = currentPhase();
        const percent = state.totalSeconds > 0
          ? Math.max(0, Math.min(100, ((state.totalSeconds - Math.max(state.remainingSeconds, 0)) / state.totalSeconds) * 100))
          : 0;
        const hasStarted = state.deadlineAt !== null || state.remainingSeconds !== state.totalSeconds;
        const reminderFrameActive = state.warningSeconds > 0
          && state.remainingSeconds <= state.warningSeconds
          && state.remainingSeconds >= 0
          && hasStarted;
        elements.progressFill.style.width = `${phase === 'expired' ? 100 : percent}%`;
        elements.progressFill.classList.toggle('is-final', phase === 'final');
        elements.progressFill.classList.toggle('is-expired', phase === 'expired');
        elements.progressWrap.classList.toggle('has-active-frame', hasStarted && !reminderFrameActive && phase !== 'expired');
        elements.progressWrap.classList.toggle('has-reminder-frame', reminderFrameActive);
        elements.progressWrap.classList.toggle('is-final-frame', phase === 'final');
        elements.progressWrap.classList.toggle('is-expired-frame', phase === 'expired');
      }

      function syncButtons() {
        const running = state.deadlineAt !== null;
        const paused = state.deadlineAt === null && state.remainingSeconds !== state.totalSeconds;
        elements.startButton.disabled = false;
        elements.startButton.textContent = running
          ? 'Pása'
          : paused
            ? 'Halda áfram'
            : 'Byrja';
        elements.resetButton.disabled = false;
      }

      function render() {
        const phase = currentPhase();
        const countdownSeconds = countdownDisplaySeconds();
        elements.body.dataset.phase = phase;
        elements.activeLabel.textContent = state.label;
        elements.phaseLabel.textContent = phaseLabelText();
        elements.countdown.textContent = formatClock(countdownSeconds);
        elements.statusText.textContent = statusText();
        elements.warningMeta.textContent = `Áminning ${state.warningSeconds > 0 ? formatClock(state.warningSeconds) : 'engin'}`;
        elements.totalMeta.textContent = `Gefið ${formatClock(state.totalSeconds)}`;
        elements.elapsedMeta.textContent = `Liðið ${formatClock(elapsedSinceStartSeconds())}`;
        elements.deadlineMeta.textContent = deadlineText();
        elements.barOverlay.textContent = phase === 'expired'
          ? 'Tími'
          : phase === 'final'
            ? 'Lokasekúndur'
            : phase === 'warning'
              ? 'Áminning'
              : 'Tímataka';
        elements.settingsHint.textContent = getSelectedPreset()
          ? `${getSelectedPreset().label} · ${getSelectedPreset().duration_minutes} mín`
          : 'Custom tími';
        syncProgressBar();
        syncButtons();
        renderPresetButtons();
        document.title = phase === 'expired'
          ? `YFIRTÍMI ${formatClock(Math.abs(countdownSeconds))} | {{ page_title }}`
          : `${formatClock(countdownSeconds)} | ${state.label}`;
      }

      function stopTicker() {
        if (state.timerId !== null) {
          window.clearInterval(state.timerId);
          state.timerId = null;
        }
      }

      function computeRemainingSeconds() {
        if (state.deadlineAt === null) {
          return state.remainingSeconds;
        }
        const deltaMs = state.deadlineAt - Date.now();
        if (deltaMs >= 0) {
          return Math.ceil(deltaMs / 1000);
        }
        return -Math.floor(Math.abs(deltaMs) / 1000);
      }

      function tick() {
        state.remainingSeconds = computeRemainingSeconds();
        render();
      }

      async function acquireWakeLock() {
        if (!state.wakeLockEnabled) {
          return;
        }
        if (!state.wakeLockSupported) {
          elements.wakeLockButton.textContent = 'Wake Lock ekki stutt';
          elements.wakeLockButton.classList.remove('is-active');
          return;
        }
        try {
          if (state.wakeLock) {
            return;
          }
          state.wakeLock = await navigator.wakeLock.request('screen');
          state.wakeLock.addEventListener('release', () => {
            state.wakeLock = null;
            if (state.wakeLockEnabled) {
              elements.wakeLockButton.textContent = 'Skjár vakandi';
            }
          });
          elements.wakeLockButton.textContent = 'Skjár vakandi';
          elements.wakeLockButton.classList.add('is-active');
        } catch (_error) {
          elements.wakeLockButton.textContent = 'Wake Lock mistókst';
          elements.wakeLockButton.classList.remove('is-active');
        }
      }

      async function releaseWakeLock() {
        if (!state.wakeLock) {
          return;
        }
        try {
          await state.wakeLock.release();
        } catch (_error) {
          // noop
        }
        state.wakeLock = null;
      }

      function selectPreset(presetId) {
        const preset = presets.find((item) => item.id === presetId);
        if (!preset) {
          return;
        }
        stopTicker();
        state.selectedPresetId = preset.id;
        state.label = preset.label;
        state.totalSeconds = preset.duration_minutes * 60;
        state.remainingSeconds = state.totalSeconds;
        state.warningSeconds = preset.warning_minutes * 60;
        state.deadlineAt = null;
        render();
      }

      function applyCustomTimer() {
        const durationSeconds = readMinuteSecondPair(
          elements.customDurationInput.value,
          elements.customDurationSecondsInput.value,
          5,
          0,
          1,
          180 * 60,
        );
        const warningSeconds = readMinuteSecondPair(
          elements.customWarningInput.value,
          elements.customWarningSecondsInput.value,
          0,
          0,
          0,
          durationSeconds,
        );
        stopTicker();
        state.selectedPresetId = '';
        state.label = `Custom ${formatClock(durationSeconds)}`;
        state.totalSeconds = durationSeconds;
        state.remainingSeconds = state.totalSeconds;
        state.warningSeconds = warningSeconds;
        state.deadlineAt = null;
        render();
      }

      function updatePreset(presetId) {
        const wrapper = elements.presetSettings.querySelector(`[data-preset-id="${presetId}"]`);
        if (!wrapper) {
          return;
        }
        const durationMinutes = clampInteger(wrapper.querySelector('[name="duration_minutes"]').value, 5, 1, 180);
        const warningMinutes = clampInteger(wrapper.querySelector('[name="warning_minutes"]').value, 0, 0, durationMinutes);
        presets = presets.map((preset) => (
          preset.id === presetId
            ? { ...preset, duration_minutes: durationMinutes, warning_minutes: warningMinutes }
            : preset
        ));
        savePresetConfigs();
        renderPresetSettings();
        if (state.selectedPresetId === presetId) {
          selectPreset(presetId);
          return;
        }
        renderPresetButtons();
      }

      async function startTimer() {
        if (state.deadlineAt !== null) {
          return;
        }
        state.deadlineAt = Date.now() + (state.remainingSeconds * 1000);
        stopTicker();
        state.timerId = window.setInterval(tick, 250);
        await acquireWakeLock();
        tick();
      }

      async function pauseTimer() {
        if (state.deadlineAt === null) {
          return;
        }
        state.remainingSeconds = computeRemainingSeconds();
        state.deadlineAt = null;
        stopTicker();
        await releaseWakeLock();
        render();
      }

      async function resetTimer() {
        state.deadlineAt = null;
        stopTicker();
        state.remainingSeconds = state.totalSeconds;
        await releaseWakeLock();
        render();
      }

      async function handlePrimaryButton() {
        if (state.deadlineAt !== null) {
          await pauseTimer();
          return;
        }
        await startTimer();
      }

      function addMinutes(minutes) {
        const delta = minutes * 60;
        state.remainingSeconds += delta;
        state.totalSeconds += delta;
        if (state.deadlineAt !== null) {
          state.deadlineAt += delta * 1000;
        }
        render();
      }

      async function toggleFullscreen() {
        try {
          if (document.fullscreenElement) {
            await document.exitFullscreen();
          } else {
            await document.documentElement.requestFullscreen();
          }
        } catch (_error) {
          // noop
        }
      }

      elements.presetGrid.addEventListener('click', (event) => {
        const button = event.target.closest('[data-preset-id]');
        if (!button) return;
        selectPreset(button.dataset.presetId);
      });

      elements.presetSettings.addEventListener('click', (event) => {
        const saveButton = event.target.closest('[data-save-preset-id]');
        if (saveButton) {
          updatePreset(saveButton.dataset.savePresetId);
          return;
        }
        const useButton = event.target.closest('[data-use-preset-id]');
        if (useButton) {
          updatePreset(useButton.dataset.usePresetId);
          selectPreset(useButton.dataset.usePresetId);
        }
      });

      elements.applyCustomButton.addEventListener('click', applyCustomTimer);
      elements.resetPresetsButton.addEventListener('click', () => {
        presets = defaultPresets.map((preset) => ({ ...preset }));
        savePresetConfigs();
        renderPresetSettings();
        if (state.selectedPresetId) {
          selectPreset(state.selectedPresetId);
          return;
        }
        renderPresetButtons();
      });

      elements.startButton.addEventListener('click', () => { handlePrimaryButton(); });
      elements.resetButton.addEventListener('click', () => { resetTimer(); });
      elements.plusOneButton.addEventListener('click', () => { addMinutes(1); });
      elements.plusFiveButton.addEventListener('click', () => { addMinutes(5); });
      elements.plusTenButton.addEventListener('click', () => { addMinutes(10); });
      elements.fullscreenButton.addEventListener('click', () => { toggleFullscreen(); });

      elements.wakeLockButton.addEventListener('click', async () => {
        state.wakeLockEnabled = !state.wakeLockEnabled;
        if (state.wakeLockEnabled) {
          elements.wakeLockButton.textContent = 'Skjár vakandi';
          elements.wakeLockButton.classList.add('is-active');
          if (state.deadlineAt !== null) {
            await acquireWakeLock();
          }
        } else {
          await releaseWakeLock();
          elements.wakeLockButton.textContent = 'Skjár má sofa';
          elements.wakeLockButton.classList.remove('is-active');
        }
      });

      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible' && state.deadlineAt !== null) {
          acquireWakeLock();
        }
      });

      window.addEventListener('keydown', (event) => {
        if (event.code === 'Space') {
          event.preventDefault();
          handlePrimaryButton();
        }
      });

      renderPresetButtons();
      renderPresetSettings();
      render();
    })();
  </script>
</body>
</html>
"""


def create_meeting_timer_blueprint(
    *,
    name: str = "meeting_timer",
    home_href: str = "/",
    home_label: str = "Til baka í fundaskrá",
    page_title: str = "Fundastund",
) -> Blueprint:
    blueprint = Blueprint(name, __name__)

    @blueprint.get("")
    @blueprint.get("/")
    def timer_index() -> str:
        return render_template_string(
            TIMER_TEMPLATE,
            home_href=home_href,
            home_label=home_label,
            page_title=page_title,
            presets_json=json.dumps(DEFAULT_PRESETS, ensure_ascii=False),
        )

    return blueprint


def register_meeting_timer(
    app: Flask,
    *,
    url_prefix: str = "/timer",
    home_href: str = "/",
    home_label: str = "Til baka í fundaskrá",
    page_title: str = "Fundastund",
) -> None:
    app.register_blueprint(
        create_meeting_timer_blueprint(
            home_href=home_href,
            home_label=home_label,
            page_title=page_title,
        ),
        url_prefix=url_prefix,
    )
