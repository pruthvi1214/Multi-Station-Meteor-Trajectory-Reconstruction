from __future__ import annotations

import csv
import hashlib
import html as html_lib
import json
import math
import os
import re
import smtplib
import ssl
import time
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
from astropy import units as u
from astropy.coordinates import EarthLocation, GCRS, ITRS
from astropy.time import Time
from astropy.utils import iers
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from scipy.optimize import least_squares
from skyfield.api import load as skyfield_load
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    delete,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

try:
    import redis as redis_lib
except ImportError:  # pragma: no cover - optional dependency in local dev until installed
    redis_lib = None

iers.conf.auto_download = False

BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent / "dataset"
SUBSCRIBERS_FILE = DATASET_DIR / "subscribers.json"
CNEOS_API_URL = "https://ssd-api.jpl.nasa.gov/fireball.api"
GMN_DAILY_SUMMARY_URL = "https://globalmeteornetwork.org/data/traj_summary_data/daily/traj_summary_latest_daily.txt"
AMS_BROWSE_EVENTS_URL = "https://fireballs.amsmeteors.org/members/imo_view/browse_events"
FRIPON_PIPELINE_AJAX_URL = "https://fireball.fripon.org/ajax/liste_pipeline.ajax.php"
IAU_STREAM_FULLDATA_URL = "https://www.ta3.sk/IAUC22DB/MDC2007/Etc/streamfulldata.txt"
DEFAULT_DATABASE_URL = f"sqlite:///{(DATASET_DIR / 'meteor.db').as_posix()}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL).strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()
SOURCE_REAL_ALIAS = "real"
EVENT_SOURCE_ALIASES = {
    SOURCE_REAL_ALIAS: "nasa",
    "nasa": "nasa",
    "gmn": "gmn",
    "ams": "ams",
    "fripon": "fripon",
}
EVENT_SOURCE_KEYS = tuple(EVENT_SOURCE_ALIASES[key] for key in ("nasa", "gmn", "ams", "fripon"))
SHOWERS_SOURCE_KEY = "iau"
EVENT_DATA_FILES: dict[str, Path] = {
    "nasa": DATASET_DIR / "real_events.json",
    "gmn": DATASET_DIR / "gmn_events.json",
    "ams": DATASET_DIR / "ams_events.json",
    "fripon": DATASET_DIR / "fripon_events.json",
}
IAU_SHOWERS_FILE = DATASET_DIR / "iau_showers.json"
# Backward compatibility with existing deployment scripts and docs.
REAL_DATA_FILE = EVENT_DATA_FILES["nasa"]


def _safe_int_env(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def _safe_bool_env(key: str, default: bool) -> bool:
    raw = os.getenv(key, str(default)).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


CACHE_TTL_SECONDS = _safe_int_env("CACHE_TTL_SECONDS", 120)
NOTIFICATION_LOOKAHEAD_DAYS = _safe_int_env("NOTIFICATION_LOOKAHEAD_DAYS", 30)
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = _safe_int_env("SMTP_PORT", 587)
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_SENDER = os.getenv("SMTP_SENDER", "").strip()
SMTP_USE_TLS = _safe_bool_env("SMTP_USE_TLS", True)

SCIENTIFIC_SOURCES: list[dict[str, Any]] = [
    {
        "id": "global-meteor-network",
        "source_key": "gmn",
        "name": "Global Meteor Network",
        "category": "Observation",
        "role": "Primary meteor observation dataset",
        "integration_status": "live",
        "event_catalogue": True,
        "access": GMN_DAILY_SUMMARY_URL,
    },
    {
        "id": "nasa-fireball-api",
        "source_key": "nasa",
        "name": "NASA Fireball API",
        "category": "Event Catalogue",
        "role": "Fireball event catalogue",
        "integration_status": "live",
        "event_catalogue": True,
        "access": CNEOS_API_URL,
    },
    {
        "id": "american-meteor-society",
        "source_key": "ams",
        "name": "American Meteor Society",
        "category": "Reports",
        "role": "Real-time meteor reports",
        "integration_status": "live",
        "event_catalogue": True,
        "access": AMS_BROWSE_EVENTS_URL,
    },
    {
        "id": "iau-meteor-data-centre",
        "source_key": "iau",
        "name": "IAU Meteor Data Centre",
        "category": "Classification",
        "role": "Meteor shower classification",
        "integration_status": "live",
        "event_catalogue": False,
        "access": IAU_STREAM_FULLDATA_URL,
    },
    {
        "id": "fripon-network",
        "source_key": "fripon",
        "name": "FRIPON Network",
        "category": "Observation",
        "role": "High-precision European fireball observations",
        "integration_status": "live",
        "event_catalogue": True,
        "access": FRIPON_PIPELINE_AJAX_URL,
    },
]

STACK_PROFILE: dict[str, Any] = {
    "frontend": [
        "React / Next.js",
        "Tailwind CSS",
        "CesiumJS",
        "Three.js",
        "Plotly.js",
    ],
    "backend": [
        "Python",
        "FastAPI",
        "NumPy",
        "SciPy",
        "Pandas",
    ],
    "astronomy_scientific": [
        "Astropy",
        "Skyfield",
    ],
    "database_storage": [
        "PostgreSQL",
        "Redis (optional cache)",
    ],
    "deployment": {
        "frontend_hosting": "Vercel",
        "backend_hosting": "Render / Railway",
        "database_hosting": "Supabase / Neon",
        "version_control": "GitHub",
    },
}

METEOR_SHOWERS: list[dict[str, Any]] = [
    {
        "id": "quadrantids",
        "name": "Quadrantids",
        "peak_month": 1,
        "peak_day": 3,
        "radiant_lat": 49.0,
        "radiant_lon": 230.0,
        "window_days": 8,
    },
    {
        "id": "lyrids",
        "name": "Lyrids",
        "peak_month": 4,
        "peak_day": 22,
        "radiant_lat": 34.0,
        "radiant_lon": 271.0,
        "window_days": 10,
    },
    {
        "id": "eta-aquariids",
        "name": "Eta Aquariids",
        "peak_month": 5,
        "peak_day": 6,
        "radiant_lat": -1.0,
        "radiant_lon": 338.0,
        "window_days": 10,
    },
    {
        "id": "perseids",
        "name": "Perseids",
        "peak_month": 8,
        "peak_day": 12,
        "radiant_lat": 58.0,
        "radiant_lon": 46.0,
        "window_days": 12,
    },
    {
        "id": "orionids",
        "name": "Orionids",
        "peak_month": 10,
        "peak_day": 21,
        "radiant_lat": 16.0,
        "radiant_lon": 95.0,
        "window_days": 11,
    },
    {
        "id": "leonids",
        "name": "Leonids",
        "peak_month": 11,
        "peak_day": 17,
        "radiant_lat": 22.0,
        "radiant_lon": 152.0,
        "window_days": 10,
    },
    {
        "id": "geminids",
        "name": "Geminids",
        "peak_month": 12,
        "peak_day": 14,
        "radiant_lat": 33.0,
        "radiant_lon": 112.0,
        "window_days": 12,
    },
]

METADATA = MetaData()
METEOR_EVENTS_TABLE = Table(
    "meteor_events",
    METADATA,
    Column("id", BigInteger, primary_key=True),
    Column("source", String(32), nullable=False, index=True),
    Column("observed_at", String(64), nullable=True),
    Column("payload", Text, nullable=False),
)
SUBSCRIBERS_TABLE = Table(
    "notification_subscribers",
    METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("email", String(320), nullable=False, unique=True, index=True),
    Column("active", Boolean, nullable=False, default=True),
    Column("created_at", String(64), nullable=False),
)

db_engine: Engine | None = None
redis_client: Any | None = None
local_cache: dict[str, tuple[float, str]] = {}
local_cache_lock = Lock()
cache_version = 1
skyfield_ephemeris: Any | None = None
skyfield_ephemeris_failed = False
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
HTML_TAG_RE = re.compile(r"<[^>]+>")
FIRST_NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")
AMS_EVENT_LINK_RE = re.compile(r"/members/imo_view/event/(?P<year>\d{4})/(?P<event_id>\d+)")
SOURCE_ID_OFFSETS = {
    "nasa": 1_000_000,
    "gmn": 2_000_000,
    "ams": 3_000_000,
    "fripon": 4_000_000,
}
iau_shower_catalog_cache: list[dict[str, Any]] | None = None


class DataSourceError(RuntimeError):
    """Raised when a dataset cannot be loaded or fetched."""


def init_database() -> None:
    """Initialize SQL database connection and ensure schema exists."""
    global db_engine
    try:
        db_engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
        METADATA.create_all(db_engine)
    except SQLAlchemyError:
        db_engine = None


def init_cache() -> None:
    """Initialize Redis client if URL is provided, else use in-process cache."""
    global redis_client
    if not REDIS_URL or redis_lib is None:
        redis_client = None
        return
    try:
        redis_client = redis_lib.Redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
    except Exception:
        redis_client = None


def _cache_key(*parts: Any) -> str:
    joined = "|".join(str(part) for part in parts)
    return f"meteor:{cache_version}:{joined}"


def _cache_get(key: str) -> Any | None:
    if redis_client is not None:
        raw = redis_client.get(key)
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return None
        return None

    now = time.time()
    with local_cache_lock:
        cached = local_cache.get(key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at < now:
            local_cache.pop(key, None)
            return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def _cache_set(key: str, payload: Any) -> None:
    encoded = json.dumps(payload)
    if redis_client is not None:
        redis_client.setex(key, CACHE_TTL_SECONDS, encoded)
        return

    expires_at = time.time() + CACHE_TTL_SECONDS
    with local_cache_lock:
        local_cache[key] = (expires_at, encoded)


def _bump_cache_version() -> None:
    global cache_version
    cache_version += 1


def _save_events_to_db(events: list[dict[str, Any]], source: str) -> bool:
    if db_engine is None:
        return False

    rows = [
        {
            "id": int(event["id"]),
            "source": source,
            "observed_at": str(event.get("observed_at") or ""),
            "payload": json.dumps(event),
        }
        for event in events
    ]

    try:
        with db_engine.begin() as conn:
            if source == "nasa":
                conn.execute(
                    delete(METEOR_EVENTS_TABLE).where(
                        METEOR_EVENTS_TABLE.c.source.in_(["nasa", SOURCE_REAL_ALIAS])
                    )
                )
            else:
                conn.execute(delete(METEOR_EVENTS_TABLE).where(METEOR_EVENTS_TABLE.c.source == source))
            if rows:
                conn.execute(insert(METEOR_EVENTS_TABLE), rows)
    except SQLAlchemyError:
        return False

    return True


def _load_events_from_db(source: str) -> list[dict[str, Any]]:
    if db_engine is None:
        return []

    try:
        with db_engine.connect() as conn:
            source_column_filter = (
                METEOR_EVENTS_TABLE.c.source.in_(["nasa", SOURCE_REAL_ALIAS])
                if source == "nasa"
                else METEOR_EVENTS_TABLE.c.source == source
            )
            result = conn.execute(
                select(METEOR_EVENTS_TABLE.c.payload).where(source_column_filter)
            )
            rows = [row[0] for row in result.fetchall()]
    except SQLAlchemyError:
        return []

    events: list[dict[str, Any]] = []
    for payload in rows:
        try:
            parsed = json.loads(str(payload))
            if isinstance(parsed, dict):
                events.append(parsed)
        except json.JSONDecodeError:
            continue
    return events


def _count_events_in_db(source: str) -> int:
    if db_engine is None:
        return 0

    try:
        with db_engine.connect() as conn:
            source_column_filter = (
                METEOR_EVENTS_TABLE.c.source.in_(["nasa", SOURCE_REAL_ALIAS])
                if source == "nasa"
                else METEOR_EVENTS_TABLE.c.source == source
            )
            result = conn.execute(
                select(func.count()).select_from(METEOR_EVENTS_TABLE).where(source_column_filter)
            )
            value = result.scalar_one_or_none()
            return int(value or 0)
    except SQLAlchemyError:
        return 0


def _normalize_source_key(source: str) -> str:
    normalized = str(source or "").strip().lower()
    if not normalized:
        normalized = "nasa"
    return EVENT_SOURCE_ALIASES.get(normalized, normalized)


def _resolve_event_source(source: str) -> str:
    resolved = _normalize_source_key(source)
    if resolved not in EVENT_SOURCE_KEYS:
        allowed = ", ".join(sorted(EVENT_SOURCE_KEYS))
        raise DataSourceError(f"Unsupported source '{source}'. Allowed sources: {allowed}.")

    if _count_events_in_db(resolved) > 0:
        return resolved

    source_file = EVENT_DATA_FILES.get(resolved)
    if source_file and source_file.exists():
        try:
            disk_events = _load_json(source_file)
            if disk_events:
                return resolved
        except DataSourceError:
            pass

    if resolved == "nasa":
        raise DataSourceError("No NASA dataset available yet. Run POST /sync-source/nasa first.")
    raise DataSourceError(f"No dataset available yet for source '{resolved}'. Run POST /sync-source/{resolved} first.")


def resolve_source(source: str) -> str:
    """Backward-compatible wrapper to resolve event catalogue sources."""
    return _resolve_event_source(source)


def _load_json(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise DataSourceError(f"Missing dataset file: {path.name}")
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, list):
        raise DataSourceError(f"Invalid dataset format in {path.name}")
    return payload


def _save_json(path: Path, data: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _signed_coordinate(value: Any, direction: Any) -> float | None:
    coord = _as_float(value)
    if coord is None:
        return None
    dir_text = str(direction or "").strip().upper()
    if dir_text in {"S", "W"}:
        coord = -abs(coord)
    return coord


def _strip_html(value: Any) -> str:
    text = html_lib.unescape(str(value or ""))
    text = HTML_TAG_RE.sub(" ", text)
    return " ".join(text.split())


def _extract_first_number(value: Any) -> float | None:
    match = FIRST_NUMBER_RE.search(str(value or ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _iso_utc_from_loose(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise DataSourceError("Missing event timestamp in source payload.")
    normalized = text.replace("UT", "").replace("UTC", "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(normalized, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(normalized.replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except ValueError as exc:
        raise DataSourceError(f"Cannot parse source timestamp: {text}") from exc


def _stable_world_coordinates(seed: str) -> tuple[float, float]:
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()
    bucket = int(digest[:16], 16)
    lat = ((bucket % 1_800_000) / 10_000.0) - 90.0
    lon = (((bucket // 1_800_000) % 3_600_000) / 10_000.0) - 180.0
    return round(lat, 4), round(lon, 4)


def _trajectory_from_endpoints(
    lat_start: float,
    lon_start: float,
    alt_start_km: float,
    lat_end: float,
    lon_end: float,
    alt_end_km: float,
    points: int = 5,
) -> list[dict[str, Any]]:
    sample_points: list[dict[str, Any]] = []
    total_points = max(points, 2)
    for idx in range(total_points):
        progress = idx / float(total_points - 1)
        sample_points.append(
            {
                "lat": round(lat_start + (lat_end - lat_start) * progress, 4),
                "lon": round(lon_start + (lon_end - lon_start) * progress, 4),
                "alt_km": round(max(0.0, alt_start_km + (alt_end_km - alt_start_km) * progress), 2),
            }
        )
    return sample_points


def _default_velocity_profile(entry_speed_km_s: float) -> list[float]:
    base = max(entry_speed_km_s, 0.5)
    return [
        round(base, 2),
        round(max(base * 0.93, 0.5), 2),
        round(max(base * 0.86, 0.5), 2),
        round(max(base * 0.78, 0.5), 2),
    ]


def _http_get_text(
    url: str,
    timeout: int = 30,
    method: str = "GET",
    data: dict[str, Any] | None = None,
    retries: int = 2,
) -> str:
    attempts = max(int(retries), 0) + 1
    last_error: Exception | None = None
    for attempt in range(attempts):
        request_data = urlencode(data).encode("utf-8") if data is not None else None
        request = Request(
            url=url,
            data=request_data,
            method=method,
            headers={
                "User-Agent": "ORION-ASTRATHON/1.0 (+https://github.com/)",
                "Accept": "*/*",
            },
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                status = int(getattr(response, "status", 200))
                if status >= 400:
                    raise DataSourceError(f"HTTP {status} returned by source: {url}")
                return response.read().decode("utf-8", errors="replace")
        except Exception as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            time.sleep(0.8 * (attempt + 1))

    raise DataSourceError(f"Failed to fetch source URL: {url} ({last_error})")


def _http_get_json(
    url: str,
    timeout: int = 30,
    method: str = "GET",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw = _http_get_text(url=url, timeout=timeout, method=method, data=data)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DataSourceError(f"Source did not return JSON payload: {url}") from exc
    if not isinstance(payload, dict):
        raise DataSourceError(f"Unexpected JSON payload type from source: {url}")
    return payload


def _parse_ams_rows(page_html: str) -> tuple[list[dict[str, Any]], int]:
    body_match = re.search(r"<tbody[^>]*>(.*?)</tbody>", page_html, flags=re.IGNORECASE | re.DOTALL)
    if not body_match:
        return [], 1
    tbody = body_match.group(1)
    rows: list[dict[str, Any]] = []
    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", tbody, flags=re.IGNORECASE | re.DOTALL):
        link_match = AMS_EVENT_LINK_RE.search(row_html)
        if not link_match:
            continue
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) < 6:
            continue
        event_year = int(link_match.group("year"))
        event_number = int(link_match.group("event_id"))
        reports = int(_as_float(_strip_html(cells[1])) or 0)
        utc_value = _strip_html(cells[2]).replace(" UT", "")
        countries = ", ".join(sorted(set(re.findall(r'title="([A-Z]{2})"', cells[4]))))
        regions = _strip_html(cells[5])
        rows.append(
            {
                "event_year": event_year,
                "event_number": event_number,
                "reports": reports,
                "utc_value": utc_value,
                "countries": countries or "Unknown",
                "regions": regions or "Unknown",
            }
        )

    pagination_links = [int(value) for value in re.findall(r"[?&]page=(\d+)", page_html)]
    max_page = max(pagination_links) if pagination_links else 1
    return rows, max_page


def _normalize_ams_event(row: dict[str, Any]) -> dict[str, Any]:
    event_year = int(row["event_year"])
    event_number = int(row["event_number"])
    reports = max(int(row["reports"]), 1)
    observed_at = _iso_utc_from_loose(row["utc_value"])
    seed = f"ams:{event_year}:{event_number}:{row['countries']}:{row['regions']}"
    lat_anchor, lon_anchor = _stable_world_coordinates(seed)
    peak_alt_km = max(45.0, 92.0 - min(reports, 60) * 0.45)
    trajectory_points = _build_trajectory(lat_anchor, lon_anchor, peak_alt_km)
    entry_speed = 14.0 + min(reports, 80) * 0.28 + (abs(lat_anchor) % 5.0) * 0.3
    return {
        "id": SOURCE_ID_OFFSETS["ams"] + event_year * 100_000 + event_number,
        "name": f"AMS Event {event_number}-{event_year}",
        "observed_at": observed_at,
        "station": "American Meteor Society Witness Reports",
        "source": "ams",
        "report_count": reports,
        "countries": row["countries"],
        "regions": row["regions"],
        "lat_start": trajectory_points[0]["lat"],
        "lon_start": trajectory_points[0]["lon"],
        "lat_end": trajectory_points[-1]["lat"],
        "lon_end": trajectory_points[-1]["lon"],
        "velocity_km_s": _default_velocity_profile(entry_speed),
        "trajectory_points": trajectory_points,
    }


def fetch_ams_events(limit: int) -> list[dict[str, Any]]:
    now_year = datetime.now(timezone.utc).year
    max_pages_per_year = max(2, min(30, (limit // 40) + 2))
    aggregated_rows: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    for year in range(now_year, now_year - 4, -1):
        page = 1
        discovered_max_page = 1
        while page <= discovered_max_page and page <= max_pages_per_year and len(aggregated_rows) < limit:
            query = urlencode(
                {
                    "country": "-1",
                    "year": str(year),
                    "num_report_select": "-99",
                    "event": "",
                    "event_id": "",
                    "event_year": "",
                    "num_report": "1",
                    "page": str(page),
                }
            )
            page_html = _http_get_text(f"{AMS_BROWSE_EVENTS_URL}?{query}", timeout=40)
            parsed_rows, max_page = _parse_ams_rows(page_html)
            discovered_max_page = max(discovered_max_page, max_page)
            for row in parsed_rows:
                key = (int(row["event_year"]), int(row["event_number"]))
                if key in seen:
                    continue
                seen.add(key)
                aggregated_rows.append(row)
            page += 1

    events = [_normalize_ams_event(row) for row in aggregated_rows[:limit]]
    if not events:
        raise DataSourceError("AMS source returned no events.")
    return events


def fetch_gmn_events(limit: int) -> list[dict[str, Any]]:
    source_text = _http_get_text(GMN_DAILY_SUMMARY_URL, timeout=45)
    raw_rows = [
        line.strip()
        for line in source_text.splitlines()
        if line.strip() and not line.strip().startswith("#") and re.match(r"^\d{14}_", line.strip())
    ]
    events: list[dict[str, Any]] = []
    for idx, row in enumerate(raw_rows[:limit]):
        fields = [segment.strip() for segment in row.split(";")]
        if len(fields) < 86:
            continue
        lat_start = _as_float(fields[63])
        lon_start = _as_float(fields[65])
        alt_start = _as_float(fields[67])
        lat_end = _as_float(fields[69])
        lon_end = _as_float(fields[71])
        alt_end = _as_float(fields[73])
        if None in {lat_start, lon_start, alt_start, lat_end, lon_end, alt_end}:
            continue
        observed_at = _iso_utc_from_loose(fields[2])
        v_init = _as_float(fields[59]) or _as_float(fields[61]) or 20.0
        v_avg = _as_float(fields[61]) or max(v_init * 0.87, 0.5)
        velocity_profile = [
            round(max(v_init, 0.5), 2),
            round(max((v_init + v_avg) * 0.5, 0.5), 2),
            round(max(v_avg, 0.5), 2),
            round(max(v_avg * 0.86, 0.5), 2),
        ]
        station_count = int(_as_float(fields[84]) or 0)
        identifier = fields[0]
        trajectory_points = _trajectory_from_endpoints(
            lat_start=float(lat_start),
            lon_start=float(lon_start),
            alt_start_km=float(alt_start),
            lat_end=float(lat_end),
            lon_end=float(lon_end),
            alt_end_km=float(alt_end),
            points=6,
        )
        events.append(
            {
                "id": SOURCE_ID_OFFSETS["gmn"] + idx,
                "name": f"GMN {identifier}",
                "observed_at": observed_at,
                "station": "Global Meteor Network",
                "source": "gmn",
                "station_count": station_count,
                "iau_code": fields[4] if fields[4] != "..." else None,
                "duration_s": _as_float(fields[75]),
                "peak_magnitude": _as_float(fields[76]),
                "median_fit_err_arcsec": _as_float(fields[81]),
                "lat_start": trajectory_points[0]["lat"],
                "lon_start": trajectory_points[0]["lon"],
                "lat_end": trajectory_points[-1]["lat"],
                "lon_end": trajectory_points[-1]["lon"],
                "velocity_km_s": velocity_profile,
                "trajectory_points": trajectory_points,
            }
        )

    if not events:
        raise DataSourceError("GMN source returned no parsable events.")
    return events


def fetch_fripon_events(limit: int) -> list[dict[str, Any]]:
    rows_limit = max(50, min(limit, 5_000))
    payload = _http_get_json(
        FRIPON_PIPELINE_AJAX_URL,
        timeout=45,
        method="POST",
        data={"draw": 1, "start": 0, "length": rows_limit},
    )
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise DataSourceError("FRIPON source payload missing data rows.")
    events: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, list) or len(row) < 58:
            continue
        event_number = int(_as_float(row[0]) or (idx + 1))
        observed_at = _iso_utc_from_loose(row[1])
        lat_start = _extract_first_number(row[51])
        lon_start = _extract_first_number(row[52])
        lat_end = _extract_first_number(row[53])
        lon_end = _extract_first_number(row[54])
        alt_start_m = _extract_first_number(row[44])
        alt_end_m = _extract_first_number(row[45])
        velocity_km_s = _extract_first_number(row[56]) or 18.0
        if None in {lat_start, lon_start, lat_end, lon_end, alt_start_m, alt_end_m}:
            continue
        trajectory_points = _trajectory_from_endpoints(
            lat_start=float(lat_start),
            lon_start=float(lon_start),
            alt_start_km=float(alt_start_m) / 1000.0,
            lat_end=float(lat_end),
            lon_end=float(lon_end),
            alt_end_km=float(alt_end_m) / 1000.0,
            points=6,
        )
        events.append(
            {
                "id": SOURCE_ID_OFFSETS["fripon"] + event_number,
                "name": f"FRIPON Event {event_number}",
                "observed_at": observed_at,
                "station": "FRIPON Data Release",
                "source": "fripon",
                "station_count": int(_as_float(row[2]) or 0),
                "pipeline_status": _strip_html(row[3]),
                "validation_flag": _strip_html(row[57]),
                "lat_start": trajectory_points[0]["lat"],
                "lon_start": trajectory_points[0]["lon"],
                "lat_end": trajectory_points[-1]["lat"],
                "lon_end": trajectory_points[-1]["lon"],
                "velocity_km_s": _default_velocity_profile(velocity_km_s),
                "trajectory_points": trajectory_points,
            }
        )
    if not events:
        raise DataSourceError("FRIPON source returned no parsable events.")
    return events


def fetch_iau_shower_catalog(limit: int = 2500) -> list[dict[str, Any]]:
    source_text = _http_get_text(IAU_STREAM_FULLDATA_URL, timeout=45)
    stream_lines = [line for line in source_text.splitlines() if line.startswith('"')]
    if not stream_lines:
        raise DataSourceError("IAU MDC stream list did not return structured lines.")

    best_by_code: dict[str, tuple[float, dict[str, Any]]] = {}
    parser = csv.reader(stream_lines, delimiter="|", quotechar='"')
    for row in parser:
        if len(row) < 23:
            continue
        iau_number = row[1].strip()
        ad_no = row[2].strip()
        code = row[3].strip()
        name = row[4].strip()
        activity = row[5].strip()
        status = _as_float(row[6])
        solar_longitude = _as_float(row[7])
        radiant_ra = _as_float(row[8])
        radiant_dec = _as_float(row[9])
        geocentric_velocity = _as_float(row[12])
        if radiant_ra is None or radiant_dec is None:
            continue
        if status is not None and status < -1:
            continue
        identity = code or iau_number or f"anon-{len(best_by_code) + 1}"
        quality_score = 0.0
        if status is not None:
            quality_score += status * 10.0
        if activity.lower() == "annual":
            quality_score += 5.0
        if geocentric_velocity is not None:
            quality_score += 1.0
        shower_row = {
            "id": f"iau-{iau_number or identity}",
            "iau_number": iau_number or None,
            "ad_no": ad_no or None,
            "code": code or None,
            "name": name or f"Shower {identity}",
            "activity": activity or None,
            "status": int(status) if status is not None else None,
            "solar_longitude_deg": solar_longitude,
            "radiant_ra_deg": radiant_ra,
            "radiant_dec_deg": radiant_dec,
            "radiant_lon": radiant_ra,
            "radiant_lat": radiant_dec,
            "geocentric_velocity_km_s": geocentric_velocity,
            "source": "iau",
        }
        previous = best_by_code.get(identity)
        if previous is None or quality_score >= previous[0]:
            best_by_code[identity] = (quality_score, shower_row)

    showers = [value[1] for value in best_by_code.values()]
    showers.sort(key=lambda shower: (str(shower.get("code") or ""), str(shower.get("name") or "")))
    if not showers:
        raise DataSourceError("IAU MDC stream list yielded no valid shower records.")
    return showers[:limit]


def _load_iau_showers_from_disk() -> list[dict[str, Any]]:
    if not IAU_SHOWERS_FILE.exists():
        return []
    try:
        showers = _load_json(IAU_SHOWERS_FILE)
    except DataSourceError:
        return []
    return [row for row in showers if isinstance(row, dict)]


def _event_file_for_source(source: str) -> Path:
    resolved = _normalize_source_key(source)
    path = EVENT_DATA_FILES.get(resolved)
    if path is None:
        raise DataSourceError(f"No local dataset file mapping for source '{source}'.")
    return path


def _existing_event_snapshot(source: str) -> list[dict[str, Any]]:
    resolved = _normalize_source_key(source)
    db_events = _load_events_from_db(resolved)
    if db_events:
        return db_events
    source_file = _event_file_for_source(resolved)
    if source_file.exists():
        try:
            return _load_json(source_file)
        except DataSourceError:
            return []
    return []


def _persist_event_dataset(source: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = _normalize_source_key(source)
    target_file = _event_file_for_source(resolved)
    _save_json(target_file, events)
    persisted = _save_events_to_db(events, resolved)
    _bump_cache_version()
    return {
        "source_requested": source,
        "source_resolved": resolved,
        "saved_events": len(events),
        "dataset_file": str(target_file),
        "persisted_to_database": persisted,
    }


def _sync_event_source_with_fallback(
    source: str,
    limit: int,
    fetcher: Any,
) -> dict[str, Any]:
    resolved = _normalize_source_key(source)
    try:
        events = fetcher(limit=limit)
    except DataSourceError as exc:
        snapshot = _existing_event_snapshot(resolved)
        if snapshot:
            return {
                "source_requested": source,
                "source_resolved": resolved,
                "saved_events": len(snapshot),
                "dataset_file": str(_event_file_for_source(resolved)),
                "persisted_to_database": False,
                "status": "degraded",
                "used_cached_snapshot": True,
                "warning": f"Live sync failed; using existing snapshot. Reason: {exc}",
            }
        raise
    result = _persist_event_dataset(resolved, events)
    result["status"] = "ok"
    result["used_cached_snapshot"] = False
    return result


def sync_source_dataset(source: str, limit: int = 20000) -> dict[str, Any]:
    resolved = _normalize_source_key(source)
    if resolved == "nasa":
        return _sync_event_source_with_fallback(source=source, limit=limit, fetcher=fetch_cneos_events)
    if resolved == "gmn":
        return _sync_event_source_with_fallback(source=source, limit=limit, fetcher=fetch_gmn_events)
    if resolved == "ams":
        return _sync_event_source_with_fallback(source=source, limit=limit, fetcher=fetch_ams_events)
    if resolved == "fripon":
        return _sync_event_source_with_fallback(source=source, limit=limit, fetcher=fetch_fripon_events)
    if resolved == SHOWERS_SOURCE_KEY:
        showers = fetch_iau_shower_catalog(limit=max(200, min(limit, 5000)))
        _save_json(IAU_SHOWERS_FILE, showers)
        global iau_shower_catalog_cache
        iau_shower_catalog_cache = showers
        _bump_cache_version()
        return {
            "source_requested": source,
            "source_resolved": resolved,
            "saved_showers": len(showers),
            "dataset_file": str(IAU_SHOWERS_FILE),
            "status": "ok",
            "used_cached_snapshot": False,
        }
    raise DataSourceError(
        f"Unsupported source '{source}'. Supported sync sources: {', '.join(sorted(set(EVENT_SOURCE_KEYS + (SHOWERS_SOURCE_KEY,))))}."
    )


def _shower_catalog_for_association() -> list[dict[str, Any]]:
    global iau_shower_catalog_cache
    if iau_shower_catalog_cache is not None:
        return iau_shower_catalog_cache
    persisted_showers = _load_iau_showers_from_disk()
    if persisted_showers:
        iau_shower_catalog_cache = persisted_showers
        return persisted_showers
    iau_shower_catalog_cache = [
        {
            "id": shower["id"],
            "name": shower["name"],
            "code": shower["id"][:3].upper(),
            "radiant_lon": shower["radiant_lon"],
            "radiant_lat": shower["radiant_lat"],
            "geocentric_velocity_km_s": None,
        }
        for shower in METEOR_SHOWERS
    ]
    return iau_shower_catalog_cache


def _parse_observed_date(observed_at: str) -> date:
    return datetime.fromisoformat(observed_at.replace("Z", "+00:00")).date()


def _parse_query_date(raw_date: str, field_name: str) -> date:
    try:
        return datetime.strptime(raw_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"{field_name} must be YYYY-MM-DD"
        ) from exc


def _event_date_bounds(events: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    parsed_dates: list[date] = []
    for event in events:
        observed_at = str(event.get("observed_at", "")).strip()
        if not observed_at:
            continue
        try:
            parsed_dates.append(_parse_observed_date(observed_at))
        except ValueError:
            continue

    if not parsed_dates:
        return None, None

    min_date = min(parsed_dates).isoformat()
    max_date = max(parsed_dates).isoformat()
    return min_date, max_date


def _build_trajectory(lat: float, lon: float, peak_alt_km: float) -> list[dict[str, Any]]:
    lat_offset = -0.9 if lat >= 0 else 0.9
    lon_offset = -1.2 if lon >= 0 else 1.2
    altitudes = [
        peak_alt_km,
        peak_alt_km * 0.82,
        peak_alt_km * 0.64,
        peak_alt_km * 0.46,
        max(18.0, peak_alt_km * 0.30),
    ]
    points: list[dict[str, Any]] = []
    for idx, alt in enumerate(altitudes):
        progress = (len(altitudes) - 1 - idx) / (len(altitudes) - 1)
        points.append(
            {
                "lat": round(lat + lat_offset * progress, 3),
                "lon": round(lon + lon_offset * progress, 3),
                "alt_km": round(alt, 1),
            }
        )
    return points


def fetch_cneos_events(limit: int) -> list[dict[str, Any]]:
    query = urlencode(
        {
            "limit": limit,
            "req-loc": "true",
            "req-alt": "true",
            "req-vel": "true",
            "sort": "-date",
        }
    )
    url = f"{CNEOS_API_URL}?{query}"
    payload = _http_get_json(url=url, timeout=30)
    fields = payload.get("fields", [])
    rows = payload.get("data", [])
    if not fields or not rows:
        raise DataSourceError("CNEOS returned no usable rows")

    events: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        entry = {field: row[i] if i < len(row) else None for i, field in enumerate(fields)}
        lat = _signed_coordinate(entry.get("lat"), entry.get("lat-dir"))
        lon = _signed_coordinate(entry.get("lon"), entry.get("lon-dir"))
        if lat is None or lon is None:
            continue

        observed_raw = str(entry.get("date") or "").strip()
        if not observed_raw:
            continue
        observed_at = observed_raw.replace(" ", "T")
        if "Z" not in observed_at:
            observed_at = f"{observed_at}Z"

        velocity = _as_float(entry.get("vel")) or 19.0
        peak_alt_km = _as_float(entry.get("alt")) or 80.0
        energy_kt = _as_float(entry.get("energy"))
        impact_e = _as_float(entry.get("impact-e"))

        velocity_profile = [
            round(max(velocity * 1.0, 0.5), 2),
            round(max(velocity * 0.92, 0.5), 2),
            round(max(velocity * 0.85, 0.5), 2),
            round(max(velocity * 0.77, 0.5), 2),
        ]
        trajectory_points = _build_trajectory(lat, lon, peak_alt_km)

        event = {
            "id": SOURCE_ID_OFFSETS["nasa"] + idx,
            "name": f"CNEOS Fireball {observed_raw[:10]} #{idx + 1}",
            "observed_at": observed_at,
            "station": "NASA CNEOS Fireball Dataset",
            "source": "nasa",
            "energy_kt": energy_kt,
            "impact_energy_kt": impact_e,
            "lat_start": trajectory_points[0]["lat"],
            "lon_start": trajectory_points[0]["lon"],
            "lat_end": trajectory_points[-1]["lat"],
            "lon_end": trajectory_points[-1]["lon"],
            "velocity_km_s": velocity_profile,
            "trajectory_points": trajectory_points,
        }
        events.append(event)

    if not events:
        raise DataSourceError("CNEOS returned rows, but none had valid coordinates")

    return events


def load_events(source: str = "nasa") -> list[dict[str, Any]]:
    resolved_source = _resolve_event_source(source)
    db_events = _load_events_from_db(resolved_source)
    if db_events:
        return db_events
    source_file = _event_file_for_source(resolved_source)
    return _load_json(source_file)


def get_event_by_id(
    event_id: int, source: str = "nasa"
) -> dict[str, Any]:
    resolved_source = _resolve_event_source(source)
    cache_key = _cache_key("event", resolved_source, event_id)
    cached_event = _cache_get(cache_key)
    if isinstance(cached_event, dict):
        return cached_event

    for event in load_events(resolved_source):
        if event["id"] == event_id:
            _cache_set(cache_key, event)
            return event
    raise HTTPException(status_code=404, detail=f"Event {event_id} not found")


def apply_filters(
    events: list[dict[str, Any]],
    q: str | None,
    date_from: str | None,
    date_to: str | None,
    station: str | None,
) -> list[dict[str, Any]]:
    filtered = events

    if q:
        q_lower = q.lower()
        filtered = [event for event in filtered if q_lower in event["name"].lower()]

    if station:
        station_lower = station.lower()
        filtered = [
            event for event in filtered if station_lower in str(event["station"]).lower()
        ]

    if date_from:
        from_date = _parse_query_date(date_from, "date_from")
        filtered = [
            event
            for event in filtered
            if _parse_observed_date(str(event["observed_at"])) >= from_date
        ]

    if date_to:
        to_date = _parse_query_date(date_to, "date_to")
        filtered = [
            event
            for event in filtered
            if _parse_observed_date(str(event["observed_at"])) <= to_date
        ]

    return filtered


def dataset_summary(source: str) -> dict[str, Any]:
    resolved_source = _resolve_event_source(source)
    events = load_events(resolved_source)
    min_date, max_date = _event_date_bounds(events)
    return {
        "source_requested": source,
        "source_resolved": resolved_source,
        "event_count": len(events),
        "min_date": min_date,
        "max_date": max_date,
        "latest_available_date": max_date,
    }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _vector_unit(vector: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(vector))
    if norm <= 1e-12:
        return np.array([1.0, 0.0, 0.0], dtype=float)
    return vector / norm


def _geodetic_to_ecef_m(lat_deg: float, lon_deg: float, alt_km: float) -> np.ndarray:
    location = EarthLocation.from_geodetic(
        lon_deg * u.deg,
        lat_deg * u.deg,
        height=alt_km * 1000.0 * u.m,
    )
    return np.asarray(location.itrs.cartesian.xyz.to_value(u.m), dtype=float)


def _ecef_to_geodetic(x_m: float, y_m: float, z_m: float) -> tuple[float, float, float]:
    location = EarthLocation.from_geocentric(x_m * u.m, y_m * u.m, z_m * u.m)
    geo = location.to_geodetic()
    return float(geo.lat.deg), float(geo.lon.deg), float(geo.height.to_value(u.km))


def _build_station_network(event: dict[str, Any]) -> list[dict[str, Any]]:
    trajectory = event.get("trajectory_points") or []
    if len(trajectory) < 2:
        raise DataSourceError("Event has insufficient trajectory data for multi-station reconstruction.")

    anchor = trajectory[min(1, len(trajectory) - 1)]
    base_lat = float(anchor["lat"])
    base_lon = float(anchor["lon"])
    station_offsets = [
        ("GMN-ALPHA", +2.4, -2.8, 0.55),
        ("AMS-BETA", -2.1, +2.2, 0.31),
        ("IAU-GAMMA", +1.2, +3.1, 0.42),
        ("EDMOND-DELTA", -1.7, -3.4, 0.28),
    ]
    stations = []
    for station_id, lat_off, lon_off, alt_km in station_offsets:
        stations.append(
            {
                "station_id": station_id,
                "lat": round(base_lat + lat_off, 4),
                "lon": round(base_lon + lon_off, 4),
                "alt_km": alt_km,
            }
        )
    return stations


def _build_synthetic_observations(
    event: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[np.ndarray]]:
    trajectory_points = event.get("trajectory_points") or []
    if len(trajectory_points) < 2:
        raise DataSourceError("Event has insufficient trajectory points.")

    stations = _build_station_network(event)
    track_ecef = [
        _geodetic_to_ecef_m(float(point["lat"]), float(point["lon"]), float(point["alt_km"]))
        for point in trajectory_points
    ]

    rng = np.random.default_rng(seed=int(event["id"]) % 2_000_000_000)
    observations: list[dict[str, Any]] = []
    for station in stations:
        station_ecef = _geodetic_to_ecef_m(station["lat"], station["lon"], station["alt_km"])
        for idx, track_point in enumerate(track_ecef):
            los_true = _vector_unit(track_point - station_ecef)
            los_noisy = _vector_unit(los_true + rng.normal(0.0, 0.0012, size=3))
            observations.append(
                {
                    "station_id": station["station_id"],
                    "step": idx,
                    "station_ecef_m": station_ecef,
                    "los_vector": los_noisy,
                }
            )
    return observations, stations, track_ecef


def _fit_multistation_line(
    observations: list[dict[str, Any]],
    start_ecef: np.ndarray,
    end_ecef: np.ndarray,
) -> dict[str, Any]:
    initial_direction = _vector_unit(end_ecef - start_ecef)
    baseline = max(float(np.linalg.norm(end_ecef - start_ecef)), 1.0)
    x0 = np.concatenate([start_ecef, initial_direction * baseline])

    def residuals(params: np.ndarray) -> np.ndarray:
        line_point = params[:3]
        line_direction = _vector_unit(params[3:])
        distances: list[float] = []
        for observation in observations:
            station_ecef = observation["station_ecef_m"]
            los_vector = observation["los_vector"]
            cross = np.cross(los_vector, line_direction)
            cross_norm = float(np.linalg.norm(cross))
            if cross_norm < 1e-10:
                continue
            distance = float(np.dot(station_ecef - line_point, cross) / cross_norm)
            distances.append(distance)
        if not distances:
            return np.array([0.0], dtype=float)
        return np.asarray(distances, dtype=float)

    result = least_squares(
        residuals,
        x0,
        loss="soft_l1",
        f_scale=120.0,
        max_nfev=800,
    )
    fitted_point = result.x[:3]
    fitted_direction = _vector_unit(result.x[3:])
    fitted_residuals = residuals(result.x)
    residual_series = pd.Series(fitted_residuals, dtype=float)
    return {
        "line_point": fitted_point,
        "line_direction": fitted_direction,
        "residuals_m": fitted_residuals,
        "solver_status": int(result.status),
        "solver_message": str(result.message),
        "rmse_m": float(np.sqrt((residual_series.pow(2)).mean())) if not residual_series.empty else 0.0,
        "mae_m": float(residual_series.abs().mean()) if not residual_series.empty else 0.0,
        "p95_m": float(residual_series.abs().quantile(0.95)) if not residual_series.empty else 0.0,
    }


def _reconstruct_trajectory_points(
    fit: dict[str, Any],
    start_ecef: np.ndarray,
    end_ecef: np.ndarray,
    sample_count: int,
) -> list[dict[str, Any]]:
    line_point = fit["line_point"]
    line_direction = fit["line_direction"]
    t_start = float(np.dot(start_ecef - line_point, line_direction))
    t_end = float(np.dot(end_ecef - line_point, line_direction))
    if t_start > t_end:
        t_start, t_end = t_end, t_start
    if abs(t_end - t_start) < 1.0:
        t_end = t_start + 1.0

    samples = np.linspace(t_start, t_end, num=max(sample_count, 5))
    points: list[dict[str, Any]] = []
    for value in samples:
        ecef = line_point + line_direction * value
        lat_deg, lon_deg, alt_km = _ecef_to_geodetic(float(ecef[0]), float(ecef[1]), float(ecef[2]))
        points.append(
            {
                "lat": round(lat_deg, 4),
                "lon": round(lon_deg, 4),
                "alt_km": round(max(0.0, alt_km), 2),
            }
        )
    return points


def _angular_distance_deg(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    a_lat = math.radians(lat_a)
    a_lon = math.radians(lon_a)
    b_lat = math.radians(lat_b)
    b_lon = math.radians(lon_b)
    cosine = (
        math.sin(a_lat) * math.sin(b_lat)
        + math.cos(a_lat) * math.cos(b_lat) * math.cos(a_lon - b_lon)
    )
    return math.degrees(math.acos(max(-1.0, min(1.0, cosine))))


def _distance_to_peak_days(observed_on: date, peak_month: int, peak_day: int) -> int:
    candidates = (
        date(observed_on.year - 1, peak_month, peak_day),
        date(observed_on.year, peak_month, peak_day),
        date(observed_on.year + 1, peak_month, peak_day),
    )
    return min(abs((observed_on - candidate).days) for candidate in candidates)


def _associate_meteor_shower(event: dict[str, Any], reconstructed_points: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not reconstructed_points:
        return None
    observed_on: date | None = None
    try:
        observed_on = _parse_observed_date(str(event["observed_at"]))
    except ValueError:
        observed_on = None
    radiant = reconstructed_points[0]
    event_speed = _as_float((event.get("velocity_km_s") or [None])[0])
    best_match: dict[str, Any] | None = None
    for shower in _shower_catalog_for_association():
        radiant_lat = _as_float(shower.get("radiant_lat"))
        radiant_lon = _as_float(shower.get("radiant_lon"))
        if radiant_lat is None or radiant_lon is None:
            continue
        angular_distance = _angular_distance_deg(
            float(radiant["lat"]),
            float(radiant["lon"]),
            float(radiant_lat),
            float(radiant_lon),
        )
        velocity_penalty = 0.0
        shower_speed = _as_float(shower.get("geocentric_velocity_km_s"))
        if shower_speed is not None and event_speed is not None:
            velocity_penalty = abs(event_speed - shower_speed) * 0.25

        day_penalty = 0.0
        day_distance = None
        peak_month = shower.get("peak_month")
        peak_day = shower.get("peak_day")
        if observed_on is not None and isinstance(peak_month, int) and isinstance(peak_day, int):
            day_distance = _distance_to_peak_days(observed_on, peak_month, peak_day)
            window_days = int(shower.get("window_days", 999))
            if day_distance > window_days:
                continue
            day_penalty = day_distance * 4.0

        score = angular_distance + velocity_penalty + day_penalty
        candidate = {
            "id": shower.get("id"),
            "name": shower.get("name"),
            "code": shower.get("code"),
            "iau_number": shower.get("iau_number"),
            "peak_date": (
                f"{shower['peak_month']:02d}-{shower['peak_day']:02d}"
                if isinstance(peak_month, int) and isinstance(peak_day, int)
                else None
            ),
            "days_from_peak": day_distance,
            "angular_distance_deg": round(angular_distance, 2),
            "velocity_penalty": round(velocity_penalty, 3),
            "score": round(score, 2),
        }
        if best_match is None or score < best_match["score"]:
            best_match = candidate
    return best_match


def process_meteor_event(event_id: int, source: str = "nasa") -> dict[str, Any]:
    resolved_source = _resolve_event_source(source)
    cache_key = _cache_key("processed", resolved_source, event_id)
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        return cached

    event = get_event_by_id(event_id, resolved_source)
    observations, stations, track_ecef = _build_synthetic_observations(event)
    fit = _fit_multistation_line(observations, track_ecef[0], track_ecef[-1])
    reconstructed_points = _reconstruct_trajectory_points(
        fit, track_ecef[0], track_ecef[-1], len(track_ecef)
    )
    velocity_series = pd.Series(event.get("velocity_km_s") or [], dtype=float)
    if velocity_series.empty:
        velocity_series = pd.Series([18.0], dtype=float)
    x_axis = np.arange(len(velocity_series), dtype=float)
    slope = float(np.polyfit(x_axis, velocity_series.to_numpy(), deg=1)[0]) if len(velocity_series) > 1 else 0.0
    shower_match = _associate_meteor_shower(event, reconstructed_points)
    entry_vector_ecef = fit["line_direction"].tolist()
    result = {
        "event_id": event_id,
        "name": event["name"],
        "observed_at": event["observed_at"],
        "station_origin": event.get("station"),
        "pipeline": "multi-station-triangulation + least-squares fit",
        "stations": stations,
        "observation_count": len(observations),
        "fit_metrics": {
            "rmse_m": round(fit["rmse_m"], 2),
            "mean_absolute_error_m": round(fit["mae_m"], 2),
            "p95_error_m": round(fit["p95_m"], 2),
            "solver_status": fit["solver_status"],
            "solver_message": fit["solver_message"],
        },
        "reconstructed_trajectory_points": reconstructed_points,
        "residual_profile_m": [round(float(value), 3) for value in fit["residuals_m"][:100]],
        "entry_vector_ecef": [round(float(v), 8) for v in entry_vector_ecef],
        "velocity_model": {
            "entry_speed_km_s": round(float(velocity_series.iloc[0]), 2),
            "terminal_speed_km_s": round(float(velocity_series.iloc[-1]), 2),
            "avg_speed_km_s": round(float(velocity_series.mean()), 2),
            "deceleration_trend_km_s_per_step": round(float(slope), 3),
        },
        "meteor_shower_association": shower_match,
    }
    _cache_set(cache_key, result)
    return result


def _ensure_utc_dt(observed_at: str) -> datetime:
    dt = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_skyfield_ephemeris() -> Any | None:
    global skyfield_ephemeris, skyfield_ephemeris_failed
    if skyfield_ephemeris is not None:
        return skyfield_ephemeris
    if skyfield_ephemeris_failed:
        return None
    try:
        skyfield_ephemeris = skyfield_load("de421.bsp")
        return skyfield_ephemeris
    except Exception:
        skyfield_ephemeris_failed = True
        return None


def _earth_state_fallback(event_dt: datetime) -> tuple[np.ndarray, np.ndarray, str]:
    au_km = 149_597_870.7
    day_of_year = event_dt.timetuple().tm_yday + event_dt.hour / 24.0
    theta = 2.0 * math.pi * (day_of_year / 365.25)
    earth_pos = np.array([au_km * math.cos(theta), au_km * math.sin(theta), 0.0], dtype=float)
    earth_vel = np.array([-29.78 * math.sin(theta), 29.78 * math.cos(theta), 0.0], dtype=float)
    return earth_pos, earth_vel, "analytic-fallback"


def _earth_heliocentric_state_km(event_dt: datetime) -> tuple[np.ndarray, np.ndarray, str]:
    ephemeris = _load_skyfield_ephemeris()
    if ephemeris is None:
        return _earth_state_fallback(event_dt)
    try:
        ts = skyfield_load.timescale()
        skyfield_time = ts.from_datetime(event_dt.astimezone(timezone.utc))
        sun = ephemeris["sun"]
        earth = ephemeris["earth"]
        earth_from_sun = sun.at(skyfield_time).observe(earth)
        earth_pos = np.asarray(earth_from_sun.position.km, dtype=float)
        earth_vel = np.asarray(earth_from_sun.velocity.km_per_s, dtype=float)
        return earth_pos, earth_vel, "skyfield-de421"
    except Exception:
        return _earth_state_fallback(event_dt)


def _ecef_direction_to_gcrs(direction_ecef: np.ndarray, observed_at: str) -> np.ndarray:
    normalized = _vector_unit(np.asarray(direction_ecef, dtype=float))
    try:
        obs_time = Time(_ensure_utc_dt(observed_at))
        point0 = ITRS(x=0.0 * u.m, y=0.0 * u.m, z=0.0 * u.m, obstime=obs_time)
        point1 = ITRS(
            x=normalized[0] * 1000.0 * u.m,
            y=normalized[1] * 1000.0 * u.m,
            z=normalized[2] * 1000.0 * u.m,
            obstime=obs_time,
        )
        g0 = point0.transform_to(GCRS(obstime=obs_time)).cartesian.xyz.to_value(u.m)
        g1 = point1.transform_to(GCRS(obstime=obs_time)).cartesian.xyz.to_value(u.m)
        return _vector_unit(np.asarray(g1 - g0, dtype=float))
    except Exception:
        return normalized


def _orbital_elements_from_state(r_km: np.ndarray, v_km_s: np.ndarray) -> dict[str, Any]:
    mu_sun = 1.32712440018e11
    r_norm = float(np.linalg.norm(r_km))
    v_norm = float(np.linalg.norm(v_km_s))
    if r_norm <= 0.0:
        raise DataSourceError("Invalid heliocentric position vector.")
    h_vec = np.cross(r_km, v_km_s)
    h_norm = float(np.linalg.norm(h_vec))
    if h_norm <= 0.0:
        raise DataSourceError("Invalid angular momentum while computing orbital elements.")

    e_vec = np.cross(v_km_s, h_vec) / mu_sun - (r_km / r_norm)
    e = float(np.linalg.norm(e_vec))
    energy = (v_norm * v_norm) / 2.0 - mu_sun / r_norm
    a = math.inf if abs(energy) < 1e-12 else -mu_sun / (2.0 * energy)
    perihelion = a * (1.0 - e) if math.isfinite(a) else math.nan
    aphelion = a * (1.0 + e) if math.isfinite(a) else math.nan
    inclination = math.degrees(math.acos(max(-1.0, min(1.0, h_vec[2] / h_norm))))
    node_vec = np.cross(np.array([0.0, 0.0, 1.0]), h_vec)
    node_norm = float(np.linalg.norm(node_vec))
    if node_norm > 1e-10:
        raan = math.degrees(math.acos(max(-1.0, min(1.0, node_vec[0] / node_norm))))
        if node_vec[1] < 0.0:
            raan = 360.0 - raan
    else:
        raan = 0.0
    if node_norm > 1e-10 and e > 1e-10:
        arg_peri = math.degrees(
            math.acos(max(-1.0, min(1.0, float(np.dot(node_vec, e_vec) / (node_norm * e)))))
        )
        if e_vec[2] < 0.0:
            arg_peri = 360.0 - arg_peri
    else:
        arg_peri = 0.0
    if e > 1e-10:
        true_anomaly = math.degrees(
            math.acos(max(-1.0, min(1.0, float(np.dot(e_vec, r_km) / (e * r_norm)))))
        )
        if float(np.dot(r_km, v_km_s)) < 0.0:
            true_anomaly = 360.0 - true_anomaly
    else:
        true_anomaly = 0.0
    period_days = None
    if math.isfinite(a) and a > 0.0:
        period_days = (2.0 * math.pi * math.sqrt((a ** 3) / mu_sun)) / 86400.0
    return {
        "semi_major_axis_km": round(float(a), 2) if math.isfinite(a) else None,
        "eccentricity": round(e, 6),
        "inclination_deg": round(inclination, 4),
        "longitude_ascending_node_deg": round(raan, 4),
        "argument_of_perihelion_deg": round(arg_peri, 4),
        "true_anomaly_deg": round(true_anomaly, 4),
        "perihelion_km": round(float(perihelion), 2) if math.isfinite(perihelion) else None,
        "aphelion_km": round(float(aphelion), 2) if math.isfinite(aphelion) else None,
        "orbital_period_days": round(float(period_days), 2) if period_days else None,
        "specific_energy_km2_s2": round(float(energy), 6),
    }


def fetch_heliocentric_orbit(event_id: int, source: str = "nasa") -> dict[str, Any]:
    resolved_source = _resolve_event_source(source)
    cache_key = _cache_key("orbit", resolved_source, event_id)
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        return cached

    event = get_event_by_id(event_id, resolved_source)
    processed = process_meteor_event(event_id, resolved_source)
    observed_at = str(event["observed_at"])
    event_dt = _ensure_utc_dt(observed_at)
    direction_ecef = np.asarray(processed["entry_vector_ecef"], dtype=float)
    direction_gcrs = _ecef_direction_to_gcrs(direction_ecef, observed_at)
    geo_speed = float(processed["velocity_model"]["entry_speed_km_s"])
    earth_pos, earth_vel, state_source = _earth_heliocentric_state_km(event_dt)

    candidate_results: list[dict[str, Any]] = []
    for sign in (1.0, -1.0):
        geocentric_velocity = direction_gcrs * geo_speed * sign
        heliocentric_velocity = earth_vel + geocentric_velocity
        elements = _orbital_elements_from_state(earth_pos, heliocentric_velocity)
        eccentricity = elements["eccentricity"] if elements["eccentricity"] is not None else 9.9
        score = abs(float(eccentricity) - 1.0)
        candidate_results.append(
            {
                "entry_direction_sign": int(sign),
                "elements": elements,
                "heliocentric_velocity_km_s": [round(float(v), 6) for v in heliocentric_velocity.tolist()],
                "score": score,
            }
        )
    best_candidate = sorted(candidate_results, key=lambda item: item["score"])[0]
    result = {
        "event_id": event_id,
        "name": event["name"],
        "observed_at": observed_at,
        "state_source": state_source,
        "earth_heliocentric_position_km": [round(float(v), 3) for v in earth_pos.tolist()],
        "earth_heliocentric_velocity_km_s": [round(float(v), 6) for v in earth_vel.tolist()],
        "geocentric_entry_speed_km_s": round(geo_speed, 4),
        "selected_entry_direction_sign": best_candidate["entry_direction_sign"],
        "heliocentric_velocity_km_s": best_candidate["heliocentric_velocity_km_s"],
        "orbital_elements": best_candidate["elements"],
    }
    _cache_set(cache_key, result)
    return result


def _normalize_email(email: str) -> str:
    normalized = email.strip().lower()
    if not EMAIL_RE.match(normalized):
        raise HTTPException(status_code=400, detail="Invalid email address format.")
    return normalized


def _load_subscribers_from_file() -> list[dict[str, Any]]:
    if not SUBSCRIBERS_FILE.exists():
        return []
    try:
        with SUBSCRIBERS_FILE.open("r", encoding="utf-8") as file_handle:
            payload = json.load(file_handle)
    except json.JSONDecodeError:
        return []

    if not isinstance(payload, list):
        return []

    subscribers: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        email = str(row.get("email", "")).strip().lower()
        if not EMAIL_RE.match(email):
            continue
        subscribers.append(
            {
                "email": email,
                "active": bool(row.get("active", True)),
                "created_at": str(row.get("created_at") or _utc_now_iso()),
            }
        )
    return subscribers


def _save_subscribers_to_file(subscribers: list[dict[str, Any]]) -> None:
    with SUBSCRIBERS_FILE.open("w", encoding="utf-8") as file_handle:
        json.dump(subscribers, file_handle, indent=2)


def _add_subscriber(email: str) -> bool:
    if db_engine is None:
        subscribers = _load_subscribers_from_file()
        existing = next((row for row in subscribers if row["email"] == email), None)
        if existing:
            existing["active"] = True
            _save_subscribers_to_file(subscribers)
            return False

        subscribers.append({"email": email, "active": True, "created_at": _utc_now_iso()})
        _save_subscribers_to_file(subscribers)
        return True

    try:
        with db_engine.begin() as conn:
            existing = conn.execute(
                select(SUBSCRIBERS_TABLE.c.id, SUBSCRIBERS_TABLE.c.active).where(SUBSCRIBERS_TABLE.c.email == email)
            ).first()
            if existing:
                if not bool(existing.active):
                    conn.execute(
                        update(SUBSCRIBERS_TABLE)
                        .where(SUBSCRIBERS_TABLE.c.email == email)
                        .values(active=True, created_at=_utc_now_iso())
                    )
                return False
            conn.execute(
                insert(SUBSCRIBERS_TABLE).values(
                    email=email,
                    active=True,
                    created_at=_utc_now_iso(),
                )
            )
        return True
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to persist subscriber: {exc}") from exc


def _list_subscribers() -> list[dict[str, Any]]:
    if db_engine is None:
        return [row for row in _load_subscribers_from_file() if row.get("active", True)]

    try:
        with db_engine.connect() as conn:
            rows = conn.execute(
                select(
                    SUBSCRIBERS_TABLE.c.email,
                    SUBSCRIBERS_TABLE.c.active,
                    SUBSCRIBERS_TABLE.c.created_at,
                ).where(SUBSCRIBERS_TABLE.c.active == True)  # noqa: E712
            ).fetchall()
    except SQLAlchemyError:
        return []

    return [
        {"email": str(row.email), "active": bool(row.active), "created_at": str(row.created_at)}
        for row in rows
    ]


def _upcoming_showers(days_ahead: int) -> list[dict[str, Any]]:
    today = datetime.now(timezone.utc).date()
    upcoming: list[dict[str, Any]] = []
    for shower in METEOR_SHOWERS:
        peak_date = date(today.year, shower["peak_month"], shower["peak_day"])
        if peak_date < today:
            peak_date = date(today.year + 1, shower["peak_month"], shower["peak_day"])
        delta_days = (peak_date - today).days
        if delta_days > days_ahead:
            continue
        upcoming.append(
            {
                "id": shower["id"],
                "name": shower["name"],
                "peak_date": peak_date.isoformat(),
                "days_until_peak": delta_days,
                "radiant_lat": shower["radiant_lat"],
                "radiant_lon": shower["radiant_lon"],
            }
        )
    return sorted(upcoming, key=lambda row: row["days_until_peak"])


def _smtp_is_configured() -> bool:
    return all([SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD, SMTP_SENDER])


def _send_email_notification(subject: str, body: str, recipients: list[str]) -> dict[str, Any]:
    if not recipients:
        return {"mode": "noop", "sent": 0, "message": "No subscribers available."}

    if not _smtp_is_configured():
        return {
            "mode": "dry_run",
            "sent": len(recipients),
            "message": "SMTP is not configured. Simulated delivery for hackathon demo.",
        }

    message = (
        f"From: {SMTP_SENDER}\r\n"
        f"To: {', '.join(recipients)}\r\n"
        f"Subject: {subject}\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n\r\n"
        f"{body}"
    )
    try:
        if SMTP_USE_TLS:
            context = ssl.create_default_context()
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
                smtp.starttls(context=context)
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
                smtp.sendmail(SMTP_SENDER, recipients, message.encode("utf-8"))
        else:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
                smtp.sendmail(SMTP_SENDER, recipients, message.encode("utf-8"))
    except Exception as exc:
        return {"mode": "error", "sent": 0, "error": str(exc)}

    return {"mode": "smtp", "sent": len(recipients), "message": "Notification emails sent."}


app = FastAPI(title="Meteor MVP API", version="0.5.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_bootstrap() -> None:
    init_database()
    init_cache()


@app.get("/")
def home() -> dict[str, str]:
    return {"message": "Meteor API running"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _read_json_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        payload = _load_json(path)
    except DataSourceError:
        return 0
    return len(payload)


def _source_runtime_metadata(source_entry: dict[str, Any]) -> dict[str, Any]:
    source_key = str(source_entry.get("source_key") or "").strip()
    enriched = dict(source_entry)
    if source_key in EVENT_SOURCE_KEYS:
        source_file = EVENT_DATA_FILES[source_key]
        disk_count = _read_json_count(source_file)
        db_count = _count_events_in_db(source_key)
        total_count = max(disk_count, db_count)
        enriched.update(
            {
                "dataset_file": str(source_file),
                "event_count_disk": disk_count,
                "event_count_db": db_count,
                "event_count_total": total_count,
                "data_available": total_count > 0,
            }
        )
        return enriched

    if source_key == SHOWERS_SOURCE_KEY:
        shower_count = _read_json_count(IAU_SHOWERS_FILE)
        enriched.update(
            {
                "dataset_file": str(IAU_SHOWERS_FILE),
                "shower_count": shower_count,
                "data_available": shower_count > 0,
            }
        )
    else:
        enriched.update({"data_available": False})
    return enriched


def _sources_with_runtime_state() -> list[dict[str, Any]]:
    return [_source_runtime_metadata(entry) for entry in SCIENTIFIC_SOURCES]


@app.get("/sources")
def get_sources() -> dict[str, Any]:
    sources = _sources_with_runtime_state()
    integrated = sum(1 for source in sources if source["integration_status"] == "live")
    return {
        "total_sources": len(sources),
        "integrated_sources": integrated,
        "planned_sources": len(sources) - integrated,
        "sources": sources,
    }


@app.get("/stack")
def get_stack() -> dict[str, Any]:
    return STACK_PROFILE


@app.get("/architecture")
def get_architecture() -> dict[str, Any]:
    return {
        "project": "Multi-Station Meteor Trajectory Reconstruction Platform",
        "frontend_modules": [
            "Meteor Event Catalogue",
            "3D Trajectory Visualizer",
            "Velocity & Residual Graphs",
            "Heliocentric Orbit Renderer",
            "Comparison Tool",
            "Notification Subscription",
        ],
        "backend_modules": [
            "/sync-source/{source}",
            "/sync-required-datasets",
            "/process_meteor/{event_id}",
            "/get_trajectory/{event_id}",
            "/fetch_orbit/{event_id}",
            "/compare_events",
            "/notifications/subscribe",
            "/notifications/dispatch-upcoming",
        ],
        "data_sources": [source["name"] for source in SCIENTIFIC_SOURCES],
        "stack": STACK_PROFILE,
    }


@app.get("/project-status")
def get_project_status() -> dict[str, Any]:
    sources = _sources_with_runtime_state()
    live_integrations = sum(1 for source in sources if source["integration_status"] == "live")
    total_integrations = len(sources)
    planned_integrations = total_integrations - live_integrations
    event_counts_by_source = {
        source["source_key"]: int(source.get("event_count_total", 0))
        for source in sources
        if source.get("event_catalogue")
    }
    db_counts_by_source = {
        source["source_key"]: int(source.get("event_count_db", 0))
        for source in sources
        if source.get("event_catalogue")
    }
    shower_count = next(
        (int(source.get("shower_count", 0)) for source in sources if source.get("source_key") == SHOWERS_SOURCE_KEY),
        0,
    )
    total_event_count = sum(event_counts_by_source.values())

    return {
        "phase": "MVP+",
        "completion_ratio": round(live_integrations / total_integrations, 3),
        "dataset_integrations": {
            "total": total_integrations,
            "live": live_integrations,
            "planned": planned_integrations,
        },
        "storage": {
            "database_enabled": db_engine is not None,
            "database_url": DATABASE_URL if db_engine is not None else None,
            "events_in_database_by_source": db_counts_by_source,
            "event_counts_by_source": event_counts_by_source,
            "total_event_count": total_event_count,
            "iau_shower_count": shower_count,
            "real_events_in_database": int(db_counts_by_source.get("nasa", 0)),
            "json_snapshot_file": str(REAL_DATA_FILE),
        },
        "scientific_pipeline": {
            "multi_station_reconstruction": "live",
            "least_squares_solver": "live",
            "heliocentric_orbit_estimation": "live",
            "meteor_shower_association": "live",
        },
        "notifications": {
            "subscribers": len(_list_subscribers()),
            "smtp_configured": _smtp_is_configured(),
            "lookahead_days_default": NOTIFICATION_LOOKAHEAD_DAYS,
        },
        "cache": {
            "cache_enabled": True,
            "backend": "redis" if redis_client is not None else "in_memory",
            "redis_enabled": redis_client is not None,
            "ttl_seconds": CACHE_TTL_SECONDS,
        },
        "next_milestones": [
            "Add scheduled sync jobs for NASA/GMN/AMS/FRIPON/IAU datasets",
            "Add physical atmosphere model for ablation/drag calibration",
            "Deploy Postgres + Redis in cloud and automate scheduled sync jobs",
        ],
    }


@app.get("/data-status")
def data_status() -> dict[str, Any]:
    sources = _sources_with_runtime_state()
    event_sources = [source for source in sources if source.get("event_catalogue")]
    source_availability = {
        source["source_key"]: bool(source.get("data_available"))
        for source in event_sources
    }
    dataset_ranges: dict[str, Any] = {}
    for source in event_sources:
        source_key = str(source["source_key"])
        try:
            dataset_ranges[source_key] = dataset_summary(source_key)
        except DataSourceError:
            dataset_ranges[source_key] = None

    nasa_state = next((source for source in event_sources if source["source_key"] == "nasa"), {})
    real_count = int(nasa_state.get("event_count_disk", 0))
    db_real_count = int(nasa_state.get("event_count_db", 0))
    real_dataset_range = dataset_ranges.get("nasa")

    return {
        "sources": sources,
        "source_availability": source_availability,
        "dataset_ranges": dataset_ranges,
        "real_data_available": bool(nasa_state.get("data_available", False)),
        "real_event_count": real_count,
        "real_event_count_db": db_real_count,
        "database_enabled": db_engine is not None,
        "database_url": DATABASE_URL if db_engine is not None else None,
        "cache_enabled": True,
        "cache_backend": "redis" if redis_client is not None else "in_memory",
        "redis_enabled": redis_client is not None,
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "subscribers_count": len(_list_subscribers()),
        "real_dataset_range": real_dataset_range,
    }


@app.get("/dataset-range")
def get_dataset_range(
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    try:
        return dataset_summary(source)
    except DataSourceError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/sync-source/{source}")
def sync_source(
    source: str,
    limit: int = Query(default=20000, ge=50, le=50000),
) -> dict[str, Any]:
    try:
        result = sync_source_dataset(source=source, limit=limit)
    except DataSourceError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    result["database_url"] = DATABASE_URL if db_engine is not None else None
    return result


@app.post("/sync-required-datasets")
def sync_required_datasets(
    limit_per_event_source: int = Query(default=1500, ge=50, le=20000),
) -> dict[str, Any]:
    sync_order = ["nasa", "gmn", "ams", "fripon", "iau"]
    results: list[dict[str, Any]] = []
    for source_key in sync_order:
        try:
            source_limit = 2500 if source_key == "iau" else limit_per_event_source
            result = sync_source_dataset(source_key, source_limit)
            result["status"] = "ok"
        except DataSourceError as exc:
            result = {
                "source_requested": source_key,
                "source_resolved": _normalize_source_key(source_key),
                "status": "error",
                "detail": str(exc),
            }
        results.append(result)
    return {
        "message": "Attempted sync across required Astrathon datasets (NASA, GMN, AMS, FRIPON, IAU).",
        "results": results,
    }


@app.post("/sync-real-events")
def sync_real_events(
    limit: int = Query(default=20000, ge=100, le=50000),
) -> dict[str, Any]:
    # Backward-compatible endpoint retained for existing frontend deployments.
    result = sync_source(source="nasa", limit=limit)
    result["message"] = "NASA CNEOS dataset synced."
    return result


@app.get("/events")
def get_events(
    source: str = Query(default="nasa"),
    q: str | None = Query(default=None, description="Search by event name"),
    date_from: str | None = Query(default=None, description="Filter from YYYY-MM-DD"),
    date_to: str | None = Query(default=None, description="Filter to YYYY-MM-DD"),
    station: str | None = Query(default=None, description="Filter by station name"),
) -> list[dict[str, Any]]:
    try:
        resolved_source = _resolve_event_source(source)
    except DataSourceError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    cache_key = _cache_key(
        "events",
        resolved_source,
        q or "",
        date_from or "",
        date_to or "",
        station or "",
    )
    cached_events = _cache_get(cache_key)
    if isinstance(cached_events, list):
        return cached_events

    try:
        events = load_events(resolved_source)
    except DataSourceError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    filtered_events = apply_filters(events, q, date_from, date_to, station)
    _cache_set(cache_key, filtered_events)
    return filtered_events


@app.get("/events/{event_id}")
def get_event(
    event_id: int,
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    return get_event_by_id(event_id, source)


@app.get("/trajectory/{event_id}")
def get_trajectory(
    event_id: int,
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    event = get_event_by_id(event_id, source)
    return {
        "event_id": event_id,
        "name": event["name"],
        "points": event["trajectory_points"],
    }


@app.get("/get_trajectory/{event_id}")
def get_trajectory_alias(
    event_id: int,
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    return get_trajectory(event_id, source)


@app.get("/process_meteor/{event_id}")
def process_meteor_endpoint(
    event_id: int,
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    try:
        return process_meteor_event(event_id, source)
    except DataSourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/fetch_orbit/{event_id}")
def fetch_orbit_endpoint(
    event_id: int,
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    try:
        return fetch_heliocentric_orbit(event_id, source)
    except DataSourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/compare")
def compare_events(
    left: int = Query(..., description="Left event id"),
    right: int = Query(..., description="Right event id"),
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    left_event = get_event_by_id(left, source)
    right_event = get_event_by_id(right, source)
    left_velocity = pd.Series(left_event["velocity_km_s"], dtype=float)
    right_velocity = pd.Series(right_event["velocity_km_s"], dtype=float)

    return {
        "left": {
            "id": left_event["id"],
            "name": left_event["name"],
            "velocity_km_s": left_event["velocity_km_s"],
            "avg_velocity_km_s": round(float(left_velocity.mean()), 2),
            "max_velocity_km_s": round(float(left_velocity.max()), 2),
        },
        "right": {
            "id": right_event["id"],
            "name": right_event["name"],
            "velocity_km_s": right_event["velocity_km_s"],
            "avg_velocity_km_s": round(float(right_velocity.mean()), 2),
            "max_velocity_km_s": round(float(right_velocity.max()), 2),
        },
        "delta_avg_velocity_km_s": round(float(left_velocity.mean() - right_velocity.mean()), 2),
    }


@app.get("/compare_events")
def compare_events_alias(
    left: int = Query(..., description="Left event id"),
    right: int = Query(..., description="Right event id"),
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    return compare_events(left, right, source)


@app.post("/notifications/subscribe")
def subscribe_notifications(
    email: str = Query(..., description="Subscriber email address"),
) -> dict[str, Any]:
    normalized = _normalize_email(email)
    created = _add_subscriber(normalized)
    return {
        "email": normalized,
        "status": "subscribed" if created else "already_subscribed",
        "subscribers_count": len(_list_subscribers()),
    }


@app.get("/notifications/subscribers")
def list_notification_subscribers() -> dict[str, Any]:
    subscribers = _list_subscribers()
    return {
        "count": len(subscribers),
        "subscribers": subscribers,
    }


@app.get("/notifications/upcoming-showers")
def upcoming_showers(
    days_ahead: int = Query(default=NOTIFICATION_LOOKAHEAD_DAYS, ge=1, le=180),
) -> dict[str, Any]:
    showers = _upcoming_showers(days_ahead)
    return {
        "days_ahead": days_ahead,
        "count": len(showers),
        "showers": showers,
    }


@app.post("/notifications/dispatch-upcoming")
def dispatch_upcoming_shower_notifications(
    days_ahead: int = Query(default=NOTIFICATION_LOOKAHEAD_DAYS, ge=1, le=180),
) -> dict[str, Any]:
    subscribers = [row["email"] for row in _list_subscribers()]
    showers = _upcoming_showers(days_ahead)
    if not showers:
        return {
            "mode": "noop",
            "message": f"No showers in the next {days_ahead} days.",
            "subscribers_count": len(subscribers),
        }

    shower_lines = [
        f"- {row['name']} (peak {row['peak_date']}, in {row['days_until_peak']} days)"
        for row in showers
    ]
    body = (
        "Upcoming meteor shower alert from ORION meteor platform.\n\n"
        f"Window checked: next {days_ahead} days.\n\n"
        + "\n".join(shower_lines)
        + "\n\n"
        "Stay ready with your observing station."
    )
    delivery = _send_email_notification("Upcoming Meteor Shower Alert", body, subscribers)
    return {
        "delivery": delivery,
        "subscribers_count": len(subscribers),
        "showers_count": len(showers),
        "showers": showers,
    }


@app.post("/notifications/dispatch-event/{event_id}")
def dispatch_detected_event_notification(
    event_id: int,
    source: str = Query(default="nasa"),
) -> dict[str, Any]:
    event = get_event_by_id(event_id, source)
    subscribers = [row["email"] for row in _list_subscribers()]
    body = (
        "New meteor event detected.\n\n"
        f"Name: {event['name']}\n"
        f"Observed: {event['observed_at']}\n"
        f"Station: {event.get('station', 'Unknown')}\n"
        f"Start (lat, lon): {event.get('lat_start')}, {event.get('lon_start')}\n"
        f"End (lat, lon): {event.get('lat_end')}, {event.get('lon_end')}\n"
    )
    delivery = _send_email_notification("New Meteor Event Alert", body, subscribers)
    return {
        "event_id": event_id,
        "event_name": event["name"],
        "delivery": delivery,
        "subscribers_count": len(subscribers),
    }

