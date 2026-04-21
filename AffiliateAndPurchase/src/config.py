"""
Shared configuration: environment variables, constants, analysis windows.

Report mode is selected via REPORT_MODE env var (or override at call site):
    oneoff — fixed historical investigation windows (default)
    pulse  — rolling 7 UTC days (Problem A only)
    deep   — rolling 28 UTC days (Problem A full-period; Problem B with 7d maturity buffer)
"""

import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv()

# ── Paths ────────────────────────────────────────────────────────────────────

CACHE_DIR = Path("./cache")
CACHE_DIR.mkdir(exist_ok=True)

# ── MongoDB ──────────────────────────────────────────────────────────────────

SSH_HOST   = os.getenv("MONGO_SSH_HOST")
SSH_USER   = os.getenv("MONGO_SSH_USER")
DB_HOST    = os.getenv("MONGO_DB_HOST")
DB_PORT    = int(os.getenv("MONGO_DB_PORT", 27017))
LOCAL_PORT = int(os.getenv("MONGO_LOCAL_PORT", 27018))
DB_NAME    = os.getenv("MONGO_DB_NAME")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASSWORD")
AUTH_DB    = os.getenv("MONGO_AUTH_DB", "admin")

# ── Mixpanel ─────────────────────────────────────────────────────────────────

MP_ACCOUNT = os.getenv("MIXPANEL_SERVICE_ACCOUNT")
MP_SECRET  = os.getenv("MIXPANEL_SECRET")
MP_PROJECT = os.getenv("MIXPANEL_PROJECT_ID")
MP_BASE    = os.getenv("MIXPANEL_BASE_URL")

# ── Report mode ──────────────────────────────────────────────────────────────

REPORT_MODE = os.getenv("REPORT_MODE", "oneoff").lower()
if REPORT_MODE not in {"oneoff", "pulse", "deep"}:
    raise ValueError(f"Unknown REPORT_MODE={REPORT_MODE!r}; expected oneoff|pulse|deep")

PROBLEM_B_ENABLED = REPORT_MODE != "pulse"

# ── Analysis windows (UTC) ───────────────────────────────────────────────────

def _last_complete_utc_day() -> datetime:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=1)


def _end_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=23, minute=59, second=59)


def _derive_windows(mode: str):
    if mode == "oneoff":
        a_start = datetime(2026, 3,  6,  0,  0,  0, tzinfo=timezone.utc)
        a_end   = datetime(2026, 4,  2, 23, 59, 59, tzinfo=timezone.utc)
        b_start = datetime(2026, 2, 27,  0,  0,  0, tzinfo=timezone.utc)
        b_end   = datetime(2026, 3, 26, 23, 59, 59, tzinfo=timezone.utc)
    elif mode == "pulse":
        last_day = _last_complete_utc_day()
        a_end = _end_of_day(last_day)
        a_start = last_day - timedelta(days=6)
        b_start = b_end = None
    elif mode == "deep":
        last_day = _last_complete_utc_day()
        a_end = _end_of_day(last_day)
        a_start = last_day - timedelta(days=27)
        maturity = last_day - timedelta(days=7)
        b_end = _end_of_day(maturity)
        b_start = maturity - timedelta(days=27)
    return a_start, a_end, b_start, b_end


A_START, A_END, B_START, B_END = _derive_windows(REPORT_MODE)

# 72h lookback for Problem B event reconstruction
B_LOOKBACK_START = (B_START - timedelta(hours=72)) if B_START is not None else None

# ── Report identity & cache suffix ───────────────────────────────────────────

def _fmt_date(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%d") if dt else "NA"


REPORT_ID = (
    f"{REPORT_MODE}_{_fmt_date(A_START)}_to_{_fmt_date(A_END)}"
    if A_START is not None else f"{REPORT_MODE}"
)

# Cache files are namespaced by mode+period so pulse/deep/oneoff don't collide
CACHE_SUFFIX = f"{REPORT_MODE}__{_fmt_date(A_START)}__{_fmt_date(A_END)}"

# ── AliHelper-owned Global sk whitelist ──────────────────────────────────────

OUR_SKS = {"_c36PoUEj", "_d6jWDbY", "_AnTGXs", "_olPBn9X", "_dVh6yw5"}

# ── AliHelper EPN cabinet id (CIS only) ──────────────────────────────────────

EPN_CABINET_ID = "7685"
EPN_SUFFIX = f"_{EPN_CABINET_ID}"

# ── CIS countries (user-level region; per-event CIS is URL-domain-based) ─────

CIS_COUNTRIES = {"RU", "BY", "KZ", "UZ", "AZ", "AM", "GE", "KG", "MD", "TJ", "TM"}

# ── Browser / redirect lineage (UA-fallback only) ────────────────────────────
# `build_app` is authoritative when present; see src/utils.py:lineage_from_client

AUTO_REDIRECT_BROWSERS = {"firefox", "edge"}
DOGI_BROWSERS = {"chrome", "yandex", "opera"}

# ── Attribution & matching parameters ────────────────────────────────────────

PROXY_RETURN_WINDOW_S = 120       # CIS proxy return window (seconds after Affiliate Click)
ATTRIBUTION_WINDOW_H  = 72        # Problem B 72h lookback window
MATCH_WINDOW_S        = 10 * 60   # Purchase matching: 10 minutes
MP_TZ_OFFSET_H        = 3         # Mixpanel project timezone offset (Europe/Moscow = UTC+3)
SESSION_GAP_S         = 30 * 60   # Session boundary: 30 minutes of inactivity

# Affiliate Click data coverage start
AC_COVERAGE_START_UTC = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)

# Coverage of new fields (added mid-April 2026)
NEW_FIELDS_COVERAGE_START_UTC = datetime(2026, 4, 14, 0, 0, 0, tzinfo=timezone.utc)
