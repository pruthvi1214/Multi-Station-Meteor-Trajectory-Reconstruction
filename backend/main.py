from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Literal
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import BigInteger, Column, MetaData, String, Table, Text, create_engine, delete, func, insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

try:
    import redis as redis_lib
except ImportError:  # pragma: no cover - optional dependency in local dev until installed
    redis_lib = None

BASE_DIR = Path(__file__).resolve().parent
REAL_DATA_FILE = BASE_DIR.parent / "dataset" / "real_events.json"
CNEOS_API_URL = "https://ssd-api.jpl.nasa.gov/fireball.api"
DEFAULT_DATABASE_URL = f"sqlite:///{(BASE_DIR.parent / 'dataset' / 'meteor.db').as_posix()}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL).strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()


def _safe_int_env(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


CACHE_TTL_SECONDS = _safe_int_env("CACHE_TTL_SECONDS", 120)

SCIENTIFIC_SOURCES: list[dict[str, Any]] = [
    {
        "id": "global-meteor-network",
        "name": "Global Meteor Network",
        "category": "Observation",
        "role": "Primary meteor observation dataset",
        "integration_status": "planned",
        "access": "Community network exports/API",
    },
    {
        "id": "nasa-fireball-api",
        "name": "NASA Fireball API",
        "category": "Event Catalogue",
        "role": "Fireball event catalogue",
        "integration_status": "live",
        "access": CNEOS_API_URL,
    },
    {
        "id": "american-meteor-society",
        "name": "American Meteor Society",
        "category": "Reports",
        "role": "Real-time meteor reports",
        "integration_status": "planned",
        "access": "AMS fireball reports feed/API",
    },
    {
        "id": "iau-meteor-data-centre",
        "name": "IAU Meteor Data Centre",
        "category": "Classification",
        "role": "Meteor shower classification",
        "integration_status": "planned",
        "access": "IAU MDC datasets",
    },
    {
        "id": "jpl-horizons-api",
        "name": "JPL Horizons API",
        "category": "Orbital Mechanics",
        "role": "Planetary positions and orbit calculations",
        "integration_status": "planned",
        "access": "JPL Horizons API",
    },
    {
        "id": "sonotaco-meteor-orbit-db",
        "name": "SonotaCo Meteor Orbit Database",
        "category": "Reference Orbit Data",
        "role": "Reference meteor orbit dataset",
        "integration_status": "planned",
        "access": "SonotaCo dataset releases",
    },
    {
        "id": "edmond-database",
        "name": "EDMOND Database",
        "category": "Multi-station Observations",
        "role": "European multi-station meteor observations",
        "integration_status": "planned",
        "access": "EDMOND data publications",
    },
    {
        "id": "nasa-meteoroid-environment-office",
        "name": "NASA Meteoroid Environment Office Dataset",
        "category": "Environment Modelling",
        "role": "Meteoroid environment modelling",
        "integration_status": "planned",
        "access": "NASA MEO public resources",
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

METADATA = MetaData()
METEOR_EVENTS_TABLE = Table(
    "meteor_events",
    METADATA,
    Column("id", BigInteger, primary_key=True),
    Column("source", String(32), nullable=False, index=True),
    Column("observed_at", String(64), nullable=True),
    Column("payload", Text, nullable=False),
)

db_engine: Engine | None = None
redis_client: Any | None = None
local_cache: dict[str, tuple[float, str]] = {}
local_cache_lock = Lock()
cache_version = 1


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
            result = conn.execute(
                select(METEOR_EVENTS_TABLE.c.payload).where(METEOR_EVENTS_TABLE.c.source == source)
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
            result = conn.execute(
                select(func.count()).select_from(METEOR_EVENTS_TABLE).where(METEOR_EVENTS_TABLE.c.source == source)
            )
            value = result.scalar_one_or_none()
            return int(value or 0)
    except SQLAlchemyError:
        return 0


def resolve_source(source: Literal["real"]) -> Literal["real"]:
    if source != "real":
        raise DataSourceError("Only real source is supported.")

    if _count_events_in_db("real") > 0:
        return "real"

    if REAL_DATA_FILE.exists():
        try:
            real_events = _load_json(REAL_DATA_FILE)
            if real_events:
                return "real"
        except DataSourceError:
            pass

    raise DataSourceError("No real dataset available yet. Run POST /sync-real-events first.")


def _load_json(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise DataSourceError(f"Missing dataset file: {path.name}")
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, list):
        raise DataSourceError(f"Invalid dataset format in {path.name}")
    return payload


def _save_json(path: Path, data: list[dict[str, Any]]) -> None:
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
    try:
        with urlopen(url, timeout=30) as response:
            if response.status >= 400:
                raise DataSourceError(f"CNEOS returned HTTP {response.status}")
            payload = json.loads(response.read().decode("utf-8"))
    except URLError as exc:
        raise DataSourceError(f"Failed to fetch CNEOS data: {exc}") from exc
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
            "id": 1_000_000 + idx,
            "name": f"CNEOS Fireball {observed_raw[:10]} #{idx + 1}",
            "observed_at": observed_at,
            "station": "NASA CNEOS Fireball Dataset",
            "source": "real",
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


def load_events(source: Literal["real"] = "real") -> list[dict[str, Any]]:
    resolve_source(source)
    db_events = _load_events_from_db("real")
    if db_events:
        return db_events

    return _load_json(REAL_DATA_FILE)


def get_event_by_id(
    event_id: int, source: Literal["real"] = "real"
) -> dict[str, Any]:
    cache_key = _cache_key("event", source, event_id)
    cached_event = _cache_get(cache_key)
    if isinstance(cached_event, dict):
        return cached_event

    for event in load_events(source):
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


def dataset_summary(source: Literal["real"]) -> dict[str, Any]:
    resolved_source = resolve_source(source)
    events = load_events(source)
    min_date, max_date = _event_date_bounds(events)
    return {
        "source_requested": source,
        "source_resolved": resolved_source,
        "event_count": len(events),
        "min_date": min_date,
        "max_date": max_date,
        "latest_available_date": max_date,
    }


app = FastAPI(title="Meteor MVP API", version="0.3.0")

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


@app.get("/sources")
def get_sources() -> dict[str, Any]:
    integrated = sum(1 for source in SCIENTIFIC_SOURCES if source["integration_status"] == "live")
    return {
        "total_sources": len(SCIENTIFIC_SOURCES),
        "integrated_sources": integrated,
        "planned_sources": len(SCIENTIFIC_SOURCES) - integrated,
        "sources": SCIENTIFIC_SOURCES,
    }


@app.get("/stack")
def get_stack() -> dict[str, Any]:
    return STACK_PROFILE


@app.get("/project-status")
def get_project_status() -> dict[str, Any]:
    live_integrations = sum(
        1 for source in SCIENTIFIC_SOURCES if source["integration_status"] == "live"
    )
    total_integrations = len(SCIENTIFIC_SOURCES)
    planned_integrations = total_integrations - live_integrations

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
            "real_events_in_database": _count_events_in_db("real"),
            "json_snapshot_file": str(REAL_DATA_FILE),
        },
        "cache": {
            "cache_enabled": True,
            "backend": "redis" if redis_client is not None else "in_memory",
            "redis_enabled": redis_client is not None,
            "ttl_seconds": CACHE_TTL_SECONDS,
        },
        "next_milestones": [
            "Integrate additional live sources (GMN/AMS/IAU/EDMOND/SonotaCo)",
            "Add orbit reconstruction pipeline using Astropy + Skyfield",
            "Deploy Postgres + Redis in cloud and automate scheduled sync jobs",
        ],
    }


@app.get("/data-status")
def data_status() -> dict[str, Any]:
    real_exists = REAL_DATA_FILE.exists()
    real_count = 0
    if real_exists:
        try:
            real_count = len(_load_json(REAL_DATA_FILE))
        except DataSourceError:
            real_count = 0

    db_real_count = _count_events_in_db("real")
    real_dataset_range: dict[str, Any] | None = None
    try:
        real_dataset_range = dataset_summary("real")
    except DataSourceError:
        real_dataset_range = None

    return {
        "real_data_available": (real_exists and real_count > 0) or db_real_count > 0,
        "real_event_count": real_count,
        "real_event_count_db": db_real_count,
        "database_enabled": db_engine is not None,
        "database_url": DATABASE_URL if db_engine is not None else None,
        "cache_enabled": True,
        "cache_backend": "redis" if redis_client is not None else "in_memory",
        "redis_enabled": redis_client is not None,
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "real_dataset_range": real_dataset_range,
    }


@app.get("/dataset-range")
def get_dataset_range(
    source: Literal["real"] = Query(default="real"),
) -> dict[str, Any]:
    try:
        return dataset_summary(source)
    except DataSourceError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/sync-real-events")
def sync_real_events(
    limit: int = Query(default=20000, ge=100, le=50000),
) -> dict[str, Any]:
    try:
        events = fetch_cneos_events(limit)
        _save_json(REAL_DATA_FILE, events)
        persisted_to_db = _save_events_to_db(events, "real")
        _bump_cache_version()
    except DataSourceError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {
        "message": "Real NASA CNEOS dataset synced (live-only mode)",
        "saved_events": len(events),
        "dataset_file": str(REAL_DATA_FILE),
        "persisted_to_database": persisted_to_db,
        "database_url": DATABASE_URL if persisted_to_db else None,
    }


@app.get("/events")
def get_events(
    source: Literal["real"] = Query(default="real"),
    q: str | None = Query(default=None, description="Search by event name"),
    date_from: str | None = Query(default=None, description="Filter from YYYY-MM-DD"),
    date_to: str | None = Query(default=None, description="Filter to YYYY-MM-DD"),
    station: str | None = Query(default=None, description="Filter by station name"),
) -> list[dict[str, Any]]:
    cache_key = _cache_key(
        "events",
        source,
        q or "",
        date_from or "",
        date_to or "",
        station or "",
    )
    cached_events = _cache_get(cache_key)
    if isinstance(cached_events, list):
        return cached_events

    try:
        events = load_events(source)
    except DataSourceError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    filtered_events = apply_filters(events, q, date_from, date_to, station)
    _cache_set(cache_key, filtered_events)
    return filtered_events


@app.get("/events/{event_id}")
def get_event(
    event_id: int,
    source: Literal["real"] = Query(default="real"),
) -> dict[str, Any]:
    return get_event_by_id(event_id, source)


@app.get("/trajectory/{event_id}")
def get_trajectory(
    event_id: int,
    source: Literal["real"] = Query(default="real"),
) -> dict[str, Any]:
    event = get_event_by_id(event_id, source)
    return {
        "event_id": event_id,
        "name": event["name"],
        "points": event["trajectory_points"],
    }


@app.get("/compare")
def compare_events(
    left: int = Query(..., description="Left event id"),
    right: int = Query(..., description="Right event id"),
    source: Literal["real"] = Query(default="real"),
) -> dict[str, Any]:
    left_event = get_event_by_id(left, source)
    right_event = get_event_by_id(right, source)

    return {
        "left": {
            "id": left_event["id"],
            "name": left_event["name"],
            "velocity_km_s": left_event["velocity_km_s"],
            "avg_velocity_km_s": round(
                sum(left_event["velocity_km_s"]) / len(left_event["velocity_km_s"]), 2
            ),
        },
        "right": {
            "id": right_event["id"],
            "name": right_event["name"],
            "velocity_km_s": right_event["velocity_km_s"],
            "avg_velocity_km_s": round(
                sum(right_event["velocity_km_s"]) / len(right_event["velocity_km_s"]), 2
            ),
        },
    }
