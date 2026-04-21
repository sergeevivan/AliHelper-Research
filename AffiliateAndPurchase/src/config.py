"""
Shared configuration: environment variables, constants, analysis windows.
"""

import os
from pathlib import Path
from datetime import datetime, timezone

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

# ── Analysis windows (UTC) ──────────────────────────────────────────────────

# Problem A — Missing Affiliate Click (full March 2026)
A_START = datetime(2026, 3,  1,  0,  0,  0, tzinfo=timezone.utc)
A_END   = datetime(2026, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

# Problem B — Purchase Completed without Purchase (March minus last 7 days for mature cohort)
B_START = datetime(2026, 3,  1,  0,  0,  0, tzinfo=timezone.utc)
B_END   = datetime(2026, 3, 24, 23, 59, 59, tzinfo=timezone.utc)

# Problem B — 72h lookback requires events starting 3 days before B_START
B_LOOKBACK_START = datetime(2026, 2, 26,  0,  0,  0, tzinfo=timezone.utc)

# ── AliHelper-owned Global sk whitelist ──────────────────────────────────────

OUR_SKS = {"_c36PoUEj", "_d6jWDbY", "_AnTGXs", "_olPBn9X", "_dVh6yw5"}

# ── CIS countries (by actual affiliate routing — UA is Global/Portals) ──────

CIS_COUNTRIES = {"RU", "BY", "KZ", "UZ", "AZ", "AM", "GE", "KG", "MD", "TJ", "TM"}

# ── Browser / redirect lineage ──────────────────────────────────────────────

AUTO_REDIRECT_BROWSERS = {"firefox", "edge"}

# ── Attribution & matching parameters ────────────────────────────────────────

PROXY_RETURN_WINDOW_S = 120       # CIS proxy return window (seconds after Affiliate Click)
ATTRIBUTION_WINDOW_H  = 72        # Problem B 72h lookback window
MATCH_WINDOW_S        = 10 * 60   # Purchase matching: 10 minutes
MP_TZ_OFFSET_H        = 3         # Mixpanel project timezone offset (Europe/Moscow = UTC+3)

# Affiliate Click data coverage start
AC_COVERAGE_START_UTC = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
