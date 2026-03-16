import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import Globe from "react-globe.gl";
import Plot from "react-plotly.js";

const API_BASE = import.meta.env.VITE_API_URL || "http://127.0.0.1:8000";
const API_TIMEOUT_MS = 60000;
const apiClient = axios.create({ baseURL: API_BASE, timeout: API_TIMEOUT_MS });
const INITIAL_FILTERS = { q: "", dateFrom: "", dateTo: "", station: "" };
const FALLBACK_SOURCES = [
  {
    id: "global-meteor-network",
    source_key: "gmn",
    name: "Global Meteor Network",
    category: "Observation",
    role: "Primary meteor observation dataset",
    integration_status: "live",
    event_catalogue: true,
  },
  {
    id: "nasa-fireball-api",
    source_key: "nasa",
    name: "NASA Fireball API",
    category: "Event Catalogue",
    role: "Fireball event catalogue",
    integration_status: "live",
    event_catalogue: true,
  },
  {
    id: "american-meteor-society",
    source_key: "ams",
    name: "American Meteor Society",
    category: "Reports",
    role: "Real-time meteor reports",
    integration_status: "live",
    event_catalogue: true,
  },
  {
    id: "iau-meteor-data-centre",
    source_key: "iau",
    name: "IAU Meteor Data Centre",
    category: "Classification",
    role: "Meteor shower classification",
    integration_status: "live",
    event_catalogue: false,
  },
  {
    id: "fripon-network",
    source_key: "fripon",
    name: "FRIPON Network",
    category: "Observation",
    role: "High-precision European fireball observations",
    integration_status: "live",
    event_catalogue: true,
  },
];

const FALLBACK_STACK = {
  frontend: ["React / Next.js", "Tailwind CSS", "CesiumJS", "Three.js", "Plotly.js"],
  backend: ["Python", "FastAPI", "NumPy", "SciPy", "Pandas"],
  astronomy_scientific: ["Astropy", "Skyfield"],
  database_storage: ["PostgreSQL", "Redis (optional cache)"],
  deployment: {
    frontend_hosting: "Vercel",
    backend_hosting: "Render / Railway",
    database_hosting: "Supabase / Neon",
    version_control: "GitHub",
  },
};

const FALLBACK_PROJECT_STATUS = {
  phase: "MVP+",
  dataset_integrations: { total: 5, live: 5, planned: 0 },
  storage: { database_enabled: false, real_events_in_database: 0 },
  cache: { cache_enabled: true, backend: "in_memory", ttl_seconds: 120 },
};

const FALLBACK_ARCHITECTURE = {
  frontend_modules: [
    "Meteor Event Catalogue",
    "3D Trajectory Visualizer",
    "Velocity & Residual Graphs",
    "Heliocentric Orbit Renderer",
    "Comparison Tool",
    "Notification Subscription",
  ],
  backend_modules: [
    "/sync-source/{source}",
    "/sync-required-datasets",
    "/process_meteor/{event_id}",
    "/get_trajectory/{event_id}",
    "/fetch_orbit/{event_id}",
    "/compare_events",
    "/notifications/subscribe",
  ],
};

const normalizeEvents = (rawEvents) => {
  if (!Array.isArray(rawEvents)) return [];
  return rawEvents.filter(
    (event) =>
      event &&
      typeof event.id !== "undefined" &&
      event.observed_at &&
      Array.isArray(event.velocity_km_s) &&
      Array.isArray(event.trajectory_points),
  );
};

const safeDateLabel = (value) => {
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? "Unknown date" : dt.toISOString().split("T")[0];
};

const safeDateTimeLabel = (value) => {
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? "Unknown" : dt.toLocaleString();
};

function App() {
  const [events, setEvents] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [compareId, setCompareId] = useState(null);
  const [filters, setFilters] = useState(INITIAL_FILTERS);
  const [sourceMode, setSourceMode] = useState("nasa");
  const [datasetRange, setDatasetRange] = useState(null);
  const [rangeWarning, setRangeWarning] = useState("");
  const [compareSummary, setCompareSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [compareLoading, setCompareLoading] = useState(false);
  const [syncLoading, setSyncLoading] = useState(false);
  const [syncMessage, setSyncMessage] = useState("");
  const [sources, setSources] = useState(FALLBACK_SOURCES);
  const [stackProfile, setStackProfile] = useState(FALLBACK_STACK);
  const [projectStatus, setProjectStatus] = useState(FALLBACK_PROJECT_STATUS);
  const [architecture, setArchitecture] = useState(FALLBACK_ARCHITECTURE);
  const [processResult, setProcessResult] = useState(null);
  const [processLoading, setProcessLoading] = useState(false);
  const [orbitResult, setOrbitResult] = useState(null);
  const [orbitLoading, setOrbitLoading] = useState(false);
  const [analysisMessage, setAnalysisMessage] = useState("");
  const [notificationEmail, setNotificationEmail] = useState("");
  const [notificationMessage, setNotificationMessage] = useState("");
  const [notificationLoading, setNotificationLoading] = useState(false);
  const [dispatchLoading, setDispatchLoading] = useState(false);
  const [error, setError] = useState("");
  const [globeSize, setGlobeSize] = useState({ width: 760, height: 420 });
  const [refreshTick, setRefreshTick] = useState(0);
  const [pointerTilt, setPointerTilt] = useState({ x: 0, y: 0 });

  const handlePointerMove = (event) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const px = ((event.clientX - rect.left) / Math.max(rect.width, 1) - 0.5) * 16;
    const py = ((event.clientY - rect.top) / Math.max(rect.height, 1) - 0.5) * 16;
    setPointerTilt({ x: -py, y: px });
  };

  const handlePointerLeave = () => {
    setPointerTilt({ x: 0, y: 0 });
  };

  useEffect(() => {
    const fetchProjectMetadata = async () => {
      try {
        const [sourceResponse, stackResponse, statusResponse, architectureResponse] = await Promise.all([
          apiClient.get("/sources"),
          apiClient.get("/stack"),
          apiClient.get("/project-status"),
          apiClient.get("/architecture"),
        ]);

        const remoteSources = sourceResponse?.data?.sources;
        if (Array.isArray(remoteSources) && remoteSources.length > 0) {
          setSources(remoteSources);
          const availableEventSources = remoteSources
            .filter((source) => source?.event_catalogue !== false)
            .map((source) => source?.source_key)
            .filter(Boolean);
          if (availableEventSources.length > 0 && !availableEventSources.includes(sourceMode)) {
            setSourceMode(availableEventSources[0]);
          }
        }

        const remoteStack = stackResponse?.data;
        if (remoteStack && typeof remoteStack === "object") {
          setStackProfile(remoteStack);
        }

        const remoteStatus = statusResponse?.data;
        if (remoteStatus && typeof remoteStatus === "object") {
          setProjectStatus(remoteStatus);
        }

        const remoteArchitecture = architectureResponse?.data;
        if (remoteArchitecture && typeof remoteArchitecture === "object") {
          setArchitecture(remoteArchitecture);
        }
      } catch (metadataError) {
        setSources(FALLBACK_SOURCES);
        setStackProfile(FALLBACK_STACK);
        setProjectStatus(FALLBACK_PROJECT_STATUS);
        setArchitecture(FALLBACK_ARCHITECTURE);
        setSourceMode("nasa");
      }
    };

    fetchProjectMetadata();
  }, []);

  useEffect(() => {
    const updateGlobeSize = () => {
      const viewportWidth = window.innerWidth;
      if (viewportWidth < 700) {
        setGlobeSize({ width: Math.max(viewportWidth - 52, 300), height: 290 });
        return;
      }
      if (viewportWidth < 1100) {
        setGlobeSize({ width: Math.max(viewportWidth - 90, 420), height: 360 });
        return;
      }
      setGlobeSize({ width: 760, height: 420 });
    };

    updateGlobeSize();
    window.addEventListener("resize", updateGlobeSize);
    return () => window.removeEventListener("resize", updateGlobeSize);
  }, []);

  useEffect(() => {
    const fetchDatasetRange = async () => {
      try {
        const response = await apiClient.get("/dataset-range", {
          params: { source: sourceMode },
        });
        const range = response.data;
        setDatasetRange(range);
        if (range?.latest_available_date) {
          setFilters((prev) => {
            const latest = range.latest_available_date;
            const min = range.min_date;
            const outOfRange = prev.dateTo && ((min && prev.dateTo < min) || prev.dateTo > latest);
            if (!prev.dateTo || outOfRange) {
              return { ...prev, dateTo: latest };
            }
            return prev;
          });
        }
      } catch (rangeError) {
        setDatasetRange(null);
      }
    };

    fetchDatasetRange();
  }, [sourceMode, refreshTick]);

  useEffect(() => {
    const fetchEvents = async () => {
      try {
        setError("");
        setLoading(true);
        const response = await apiClient.get("/events", {
          params: {
            source: sourceMode,
            q: filters.q || undefined,
            date_from: filters.dateFrom || undefined,
            date_to: filters.dateTo || undefined,
            station: filters.station || undefined,
          },
        });

        const fetched = normalizeEvents(response.data);
        setEvents(fetched);

        if (fetched.length === 0) {
          setSelectedId(null);
          setCompareId(null);
          return;
        }

        const selectedStillExists = fetched.some((event) => event.id === selectedId);
        const compareStillExists = fetched.some((event) => event.id === compareId);
        const nextSelectedId = selectedStillExists ? selectedId : fetched[0].id;
        const nextCompareId = compareStillExists
          ? compareId
          : fetched.find((event) => event.id !== nextSelectedId)?.id ?? fetched[0].id;

        setSelectedId(nextSelectedId);
        setCompareId(nextCompareId);
      } catch (fetchError) {
        setEvents([]);
        setSelectedId(null);
        setCompareId(null);
        const detail = fetchError?.response?.data?.detail;
        setError(
          detail || "No dataset available for this source. Run Sync Selected Source (or Sync Required Datasets).",
        );
      } finally {
        setLoading(false);
      }
    };

    fetchEvents();
  }, [filters.q, filters.dateFrom, filters.dateTo, filters.station, sourceMode, refreshTick]);

  useEffect(() => {
    if (!datasetRange?.min_date || !datasetRange?.max_date) {
      setRangeWarning("");
      return;
    }

    const warnings = [];
    const minDate = datasetRange.min_date;
    const maxDate = datasetRange.max_date;

    if (filters.dateFrom && filters.dateFrom < minDate) {
      warnings.push(`Date-from is before available data (${minDate}).`);
    }
    if (filters.dateFrom && filters.dateFrom > maxDate) {
      warnings.push(`Date-from is after latest available data (${maxDate}).`);
    }
    if (filters.dateTo && filters.dateTo < minDate) {
      warnings.push(`Date-to is before available data (${minDate}).`);
    }
    if (filters.dateTo && filters.dateTo > maxDate) {
      warnings.push(`Date-to is after latest available data (${maxDate}).`);
    }
    if (filters.dateFrom && filters.dateTo && filters.dateFrom > filters.dateTo) {
      warnings.push("Date-from must be earlier than date-to.");
    }

    setRangeWarning(warnings.join(" "));
  }, [filters.dateFrom, filters.dateTo, datasetRange]);

  useEffect(() => {
    const fetchCompareSummary = async () => {
      if (!selectedId || !compareId || selectedId === compareId) {
        setCompareSummary(null);
        return;
      }

      try {
        setCompareLoading(true);
        const response = await apiClient.get("/compare_events", {
          params: {
            left: selectedId,
            right: compareId,
            source: sourceMode,
          },
        });
        setCompareSummary(response.data);
      } catch (compareError) {
        setCompareSummary(null);
      } finally {
        setCompareLoading(false);
      }
    };

    fetchCompareSummary();
  }, [selectedId, compareId, sourceMode]);

  const selectedEvent = useMemo(
    () => events.find((event) => event.id === selectedId) || null,
    [events, selectedId],
  );

  const compareEvent = useMemo(
    () => events.find((event) => event.id === compareId) || null,
    [events, compareId],
  );

  useEffect(() => {
    setProcessResult(null);
    setOrbitResult(null);
    setAnalysisMessage("");
  }, [selectedId, sourceMode]);

  const arcData = useMemo(() => {
    if (!selectedEvent) return [];
    return [
      {
        startLat: selectedEvent.lat_start,
        startLng: selectedEvent.lon_start,
        endLat: selectedEvent.lat_end,
        endLng: selectedEvent.lon_end,
        color: "#ff6b6b",
      },
    ];
  }, [selectedEvent]);

  const pointData = useMemo(() => {
    if (!selectedEvent) return [];
    return selectedEvent.trajectory_points.map((point, index) => ({
      ...point,
      idx: index,
    }));
  }, [selectedEvent]);

  const velocityData = useMemo(() => {
    if (!selectedEvent) return [];
    const traces = [
      {
        x: selectedEvent.velocity_km_s.map((_, i) => i + 1),
        y: selectedEvent.velocity_km_s,
        type: "scatter",
        mode: "lines+markers",
        name: selectedEvent.name,
        line: { color: "#ff6b6b", width: 3 },
      },
    ];

    if (compareEvent && compareEvent.id !== selectedEvent.id) {
      traces.push({
        x: compareEvent.velocity_km_s.map((_, i) => i + 1),
        y: compareEvent.velocity_km_s,
        type: "scatter",
        mode: "lines+markers",
        name: compareEvent.name,
        line: { color: "#48dbfb", width: 3 },
      });
    }

    return traces;
  }, [selectedEvent, compareEvent]);

  const residualData = useMemo(() => {
    if (!Array.isArray(processResult?.residual_profile_m) || processResult.residual_profile_m.length === 0) {
      return [];
    }
    return [
      {
        x: processResult.residual_profile_m.map((_, index) => index + 1),
        y: processResult.residual_profile_m,
        type: "scatter",
        mode: "lines+markers",
        name: "Residuals (m)",
        line: { color: "#ffd078", width: 2 },
      },
    ];
  }, [processResult]);

  const dashboardStats = useMemo(() => {
    const total = events.length;
    const allVelocities = events.flatMap((event) => event.velocity_km_s);
    const peak = allVelocities.length ? Math.max(...allVelocities) : 0;
    return { total, peak };
  }, [events]);

  const sourceStats = useMemo(() => {
    const integrated = sources.filter((source) => source.integration_status === "live").length;
    return {
      total: sources.length,
      integrated,
      planned: Math.max(sources.length - integrated, 0),
    };
  }, [sources]);

  const eventCatalogueSources = useMemo(
    () =>
      sources.filter(
        (source) => source?.event_catalogue !== false && typeof source?.source_key === "string",
      ),
    [sources],
  );

  const activeSource = useMemo(
    () => eventCatalogueSources.find((source) => source.source_key === sourceMode) || null,
    [eventCatalogueSources, sourceMode],
  );

  const updateFilter = (key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  const clearFilters = () => {
    setFilters({
      ...INITIAL_FILTERS,
      dateTo: datasetRange?.latest_available_date || "",
    });
    setError("");
    setRangeWarning("");
  };

  const syncSelectedSource = async () => {
    if (!sourceMode) return;
    try {
      setSyncLoading(true);
      setSyncMessage("");
      const response = await apiClient.post(`/sync-source/${sourceMode}`, null, {
        params: { limit: 2000 },
      });
      const savedEvents = response?.data?.saved_events ?? 0;
      const sourceLabel = activeSource?.name || sourceMode.toUpperCase();
      setSyncMessage(`Synced ${savedEvents} events from ${sourceLabel}.`);
      setRefreshTick((prev) => prev + 1);
    } catch (syncError) {
      const detail = syncError?.response?.data?.detail;
      setSyncMessage(detail || "Source sync failed. Retry when backend/API is reachable.");
    } finally {
      setSyncLoading(false);
    }
  };

  const syncRequiredDatasets = async () => {
    try {
      setSyncLoading(true);
      setSyncMessage("");
      const response = await apiClient.post("/sync-required-datasets", null, {
        params: { limit_per_event_source: 1200 },
      });
      const results = Array.isArray(response?.data?.results) ? response.data.results : [];
      const successCount = results.filter((row) => row.status === "ok").length;
      setSyncMessage(`Sync completed for ${successCount}/${results.length} required datasets.`);
      setRefreshTick((prev) => prev + 1);
    } catch (syncError) {
      const detail = syncError?.response?.data?.detail;
      setSyncMessage(detail || "Required dataset sync failed.");
    } finally {
      setSyncLoading(false);
    }
  };

  const runMultiStationProcessing = async () => {
    if (!selectedId) return;
    try {
      setProcessLoading(true);
      setAnalysisMessage("");
      const response = await apiClient.get(`/process_meteor/${selectedId}`, {
        params: { source: sourceMode },
      });
      setProcessResult(response.data);
      setAnalysisMessage("Multi-station reconstruction complete.");
    } catch (processError) {
      const detail = processError?.response?.data?.detail;
      setProcessResult(null);
      setAnalysisMessage(detail || "Trajectory reconstruction failed.");
    } finally {
      setProcessLoading(false);
    }
  };

  const fetchOrbitModel = async () => {
    if (!selectedId) return;
    try {
      setOrbitLoading(true);
      const response = await apiClient.get(`/fetch_orbit/${selectedId}`, {
        params: { source: sourceMode },
      });
      setOrbitResult(response.data);
    } catch (orbitError) {
      const detail = orbitError?.response?.data?.detail;
      setOrbitResult(null);
      setAnalysisMessage(detail || "Heliocentric orbit calculation failed.");
    } finally {
      setOrbitLoading(false);
    }
  };

  const subscribeNotifications = async () => {
    if (!notificationEmail) return;
    try {
      setNotificationLoading(true);
      setNotificationMessage("");
      const response = await apiClient.post("/notifications/subscribe", null, {
        params: { email: notificationEmail },
      });
      setNotificationMessage(
        `${response.data.email} - ${response.data.status.replace("_", " ")}.`,
      );
      setNotificationEmail("");
    } catch (subscribeError) {
      const detail = subscribeError?.response?.data?.detail;
      setNotificationMessage(detail || "Subscription failed.");
    } finally {
      setNotificationLoading(false);
    }
  };

  const dispatchUpcomingAlerts = async () => {
    try {
      setDispatchLoading(true);
      const response = await apiClient.post("/notifications/dispatch-upcoming", null, {
        params: { days_ahead: 45 },
      });
      const sent = response?.data?.delivery?.sent ?? 0;
      const mode = response?.data?.delivery?.mode ?? "noop";
      setNotificationMessage(`Upcoming alerts dispatched in ${mode} mode (sent: ${sent}).`);
    } catch (dispatchError) {
      const detail = dispatchError?.response?.data?.detail;
      setNotificationMessage(detail || "Failed to dispatch upcoming alerts.");
    } finally {
      setDispatchLoading(false);
    }
  };

  const appShellStyle = {
    "--tilt-x": `${pointerTilt.x}deg`,
    "--tilt-y": `${pointerTilt.y}deg`,
  };

  return (
    <main
      className="app-shell"
      style={appShellStyle}
      onMouseMove={handlePointerMove}
      onMouseLeave={handlePointerLeave}
    >
      <header className="app-header rounded-2xl border border-cyan-300/20 bg-slate-900/40 p-4 shadow-2xl shadow-cyan-500/10">
        <h1>Meteor Trajectory Command Deck</h1>
        <p>Live multi-source mode: NASA + GMN + AMS + FRIPON event catalogues with IAU shower matching.</p>
      </header>

      {loading && <div className="state-banner loading">Loading meteor events...</div>}
      {error && <div className="state-banner error">{error}</div>}
      {rangeWarning && <div className="state-banner warning">{rangeWarning}</div>}

      {!loading && (
        <section className="layout-grid">
          <aside className="card panel">
            <h2>Event Catalogue</h2>

            <label className="field-label" htmlFor="source-mode">
              Data Source
            </label>
            <select
              id="source-mode"
              value={sourceMode}
              onChange={(e) => setSourceMode(e.target.value)}
              disabled={eventCatalogueSources.length === 0}
            >
              {eventCatalogueSources.map((source) => (
                <option key={source.source_key} value={source.source_key}>
                  {source.name}
                </option>
              ))}
            </select>

            <div className="analysis-actions">
              <button className="sync-btn" onClick={syncSelectedSource} disabled={syncLoading}>
                {syncLoading ? "Syncing..." : "Sync Selected Source"}
              </button>
              <button className="sync-btn secondary" onClick={syncRequiredDatasets} disabled={syncLoading}>
                {syncLoading ? "Syncing..." : "Sync Required Datasets"}
              </button>
            </div>
            {syncMessage && <div className="sync-note">{syncMessage}</div>}

            {datasetRange?.min_date && datasetRange?.max_date && (
              <div className="range-note">
                Range: {datasetRange.min_date} to {datasetRange.max_date}
                <br />
                Latest available: {datasetRange.latest_available_date}
              </div>
            )}

            <div className="project-note">
              <b>Build Status:</b> {projectStatus.phase || "MVP+"}
              <br />
              Sources live: {projectStatus.dataset_integrations?.live ?? 0}/
              {projectStatus.dataset_integrations?.total ?? 0}
              <br />
              Storage:{" "}
              {projectStatus.storage?.database_enabled
                ? `${
                    projectStatus.storage?.event_counts_by_source?.[sourceMode] ??
                    projectStatus.storage?.real_events_in_database ??
                    0
                  } events (${activeSource?.name || sourceMode})`
                : "JSON snapshot only"}
              <br />
              Cache: {projectStatus.cache?.backend || "in_memory"} (
              {projectStatus.cache?.ttl_seconds ?? 120}s)
            </div>

            <input
              className="search-box"
              type="text"
              placeholder="Search event name..."
              value={filters.q}
              onChange={(e) => updateFilter("q", e.target.value)}
            />

            <label className="field-label" htmlFor="date-from">
              Date From
            </label>
            <input
              id="date-from"
              className="search-box"
              type="date"
              value={filters.dateFrom}
              onChange={(e) => updateFilter("dateFrom", e.target.value)}
            />

            <label className="field-label" htmlFor="date-to">
              Date To
            </label>
            <input
              id="date-to"
              className="search-box"
              type="date"
              value={filters.dateTo}
              onChange={(e) => updateFilter("dateTo", e.target.value)}
            />

            <input
              className="search-box"
              type="text"
              placeholder="Filter by location/station..."
              value={filters.station}
              onChange={(e) => updateFilter("station", e.target.value)}
            />
            <button className="clear-btn" onClick={clearFilters}>
              Clear Filters
            </button>

            <div className="quick-stats">
              <div>
                <small>Visible Events</small>
                <strong>{dashboardStats.total}</strong>
              </div>
              <div>
                <small>Peak Velocity</small>
                <strong>{dashboardStats.peak.toFixed(1)} km/s</strong>
              </div>
            </div>

            <div className="event-list">
              {events.map((event) => (
                <button
                  key={event.id}
                  className={`event-btn ${selectedId === event.id ? "active" : ""}`}
                  onClick={() => setSelectedId(event.id)}
                >
                  <strong>{event.name}</strong>
                  <span>{safeDateLabel(event.observed_at)}</span>
                  <span>{event.station}</span>
                </button>
              ))}
            </div>
          </aside>

          <section className="card globe-wrap">
            <h2>3D Trajectory Visualizer</h2>
            <div className="globe-box">
              <Globe
                globeImageUrl="//unpkg.com/three-globe/example/img/earth-dark.jpg"
                backgroundImageUrl="//unpkg.com/three-globe/example/img/night-sky.png"
                arcsData={arcData}
                arcColor={(d) => d.color}
                arcDashLength={0.55}
                arcDashGap={0.2}
                arcDashAnimateTime={1800}
                arcStroke={0.9}
                pointsData={pointData}
                pointLat="lat"
                pointLng="lon"
                pointAltitude={(d) => d.alt_km / 300}
                pointRadius={0.22}
                pointColor={() => "#ffe66d"}
                width={globeSize.width}
                height={globeSize.height}
              />
            </div>
          </section>

          <section className="card info-panel">
            <h2>Event Details</h2>
            {selectedEvent ? (
              <>
                <p>
                  <b>Name:</b> {selectedEvent.name}
                </p>
                <p>
                  <b>Observed:</b> {safeDateTimeLabel(selectedEvent.observed_at)}
                </p>
                <p>
                  <b>Source:</b> {selectedEvent.source || "unknown"}
                </p>
                <p>
                  <b>Station:</b> {selectedEvent.station}
                </p>
                <p>
                  <b>Start:</b> {selectedEvent.lat_start}, {selectedEvent.lon_start}
                </p>
                <p>
                  <b>End:</b> {selectedEvent.lat_end}, {selectedEvent.lon_end}
                </p>

                <label htmlFor="compare-select">
                  <b>Compare With:</b>
                </label>
                <select
                  id="compare-select"
                  value={compareId ?? ""}
                  onChange={(e) => setCompareId(Number(e.target.value))}
                >
                  {events.map((event) => (
                    <option key={event.id} value={event.id}>
                      {event.name}
                    </option>
                  ))}
                </select>

                <div className="analysis-actions">
                  <button className="sync-btn" onClick={runMultiStationProcessing} disabled={processLoading}>
                    {processLoading ? "Processing..." : "Run /process_meteor"}
                  </button>
                  <button className="sync-btn secondary" onClick={fetchOrbitModel} disabled={orbitLoading}>
                    {orbitLoading ? "Fetching..." : "Run /fetch_orbit"}
                  </button>
                </div>
                {analysisMessage && <p className="analysis-note">{analysisMessage}</p>}

                {compareLoading && <p>Updating comparison...</p>}
                {!compareLoading && compareSummary && (
                  <div className="compare-summary">
                    <p>
                      <b>{compareSummary.left.name} avg:</b>{" "}
                      {compareSummary.left.avg_velocity_km_s} km/s
                    </p>
                    <p>
                      <b>{compareSummary.right.name} avg:</b>{" "}
                      {compareSummary.right.avg_velocity_km_s} km/s
                    </p>
                    <p>
                      <b>Delta avg:</b> {compareSummary.delta_avg_velocity_km_s ?? "n/a"} km/s
                    </p>
                  </div>
                )}
                {processResult?.fit_metrics && (
                  <div className="compare-summary">
                    <p>
                      <b>Reconstruction RMSE:</b> {processResult.fit_metrics.rmse_m} m
                    </p>
                    <p>
                      <b>P95 Error:</b> {processResult.fit_metrics.p95_error_m} m
                    </p>
                    <p>
                      <b>Meteor Shower:</b>{" "}
                      {processResult.meteor_shower_association?.name || "No clear association"}
                    </p>
                  </div>
                )}
                {orbitResult?.orbital_elements && (
                  <div className="compare-summary">
                    <p>
                      <b>Orbit e:</b> {orbitResult.orbital_elements.eccentricity}
                    </p>
                    <p>
                      <b>Inclination:</b> {orbitResult.orbital_elements.inclination_deg} deg
                    </p>
                    <p>
                      <b>Semi-major axis:</b>{" "}
                      {orbitResult.orbital_elements.semi_major_axis_km ?? "hyperbolic"} km
                    </p>
                  </div>
                )}
              </>
            ) : (
              <p>No events match your filters. Expand date range or clear filters.</p>
            )}
          </section>

          <section className="card chart-wrap">
            <h2>Velocity Profile (km/s)</h2>
            <Plot
              data={velocityData}
              layout={{
                paper_bgcolor: "rgba(0,0,0,0)",
                plot_bgcolor: "rgba(0,0,0,0)",
                font: { color: "#e6f1ff" },
                xaxis: { title: "Time Step", gridcolor: "#223047" },
                yaxis: { title: "Velocity (km/s)", gridcolor: "#223047" },
                margin: { t: 24, b: 50, l: 60, r: 24 },
                legend: { orientation: "h", y: 1.14 },
              }}
              config={{ displayModeBar: false, responsive: true }}
              style={{ width: "100%", height: "320px" }}
              useResizeHandler
            />
          </section>

          <section className="card chart-wrap">
            <h2>Residual & Orbit Diagnostics</h2>
            {residualData.length > 0 ? (
              <Plot
                data={residualData}
                layout={{
                  paper_bgcolor: "rgba(0,0,0,0)",
                  plot_bgcolor: "rgba(0,0,0,0)",
                  font: { color: "#e6f1ff" },
                  xaxis: { title: "Observation Index", gridcolor: "#223047" },
                  yaxis: { title: "Residual Error (m)", gridcolor: "#223047" },
                  margin: { t: 24, b: 50, l: 60, r: 24 },
                }}
                config={{ displayModeBar: false, responsive: true }}
                style={{ width: "100%", height: "280px" }}
                useResizeHandler
              />
            ) : (
              <p className="analysis-note">Run /process_meteor to view residual graphs.</p>
            )}
            {orbitResult?.orbital_elements && (
              <div className="orbit-grid">
                <div>
                  <small>Eccentricity</small>
                  <strong>{orbitResult.orbital_elements.eccentricity}</strong>
                </div>
                <div>
                  <small>Inclination</small>
                  <strong>{orbitResult.orbital_elements.inclination_deg} deg</strong>
                </div>
                <div>
                  <small>Perihelion</small>
                  <strong>{orbitResult.orbital_elements.perihelion_km ?? "n/a"} km</strong>
                </div>
                <div>
                  <small>Orbit Source</small>
                  <strong>{orbitResult.state_source}</strong>
                </div>
              </div>
            )}
          </section>

          <section className="card panel">
            <h2>Email Notifications</h2>
            <p className="analysis-note">
              Subscribe users and trigger meteor shower/event alerts from the dashboard.
            </p>
            <input
              className="search-box"
              type="email"
              placeholder="astro-user@example.com"
              value={notificationEmail}
              onChange={(e) => setNotificationEmail(e.target.value)}
            />
            <div className="analysis-actions">
              <button className="sync-btn" onClick={subscribeNotifications} disabled={notificationLoading}>
                {notificationLoading ? "Subscribing..." : "Subscribe"}
              </button>
              <button className="sync-btn secondary" onClick={dispatchUpcomingAlerts} disabled={dispatchLoading}>
                {dispatchLoading ? "Dispatching..." : "Dispatch Shower Alerts"}
              </button>
            </div>
            {notificationMessage && <div className="sync-note">{notificationMessage}</div>}
          </section>

          <section className="card stack-wrap">
            <h2>System Architecture</h2>
            <div className="stack-grid">
              <div>
                <h3>Frontend Modules</h3>
                <ul>
                  {(architecture.frontend_modules || []).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </div>
              <div>
                <h3>Backend APIs</h3>
                <ul>
                  {(architecture.backend_modules || []).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </div>
            </div>
          </section>

          <section className="card source-wrap">
            <h2>Scientific Dataset Registry</h2>
            <div className="quick-stats source-stats">
              <div>
                <small>Total Sources</small>
                <strong>{sourceStats.total}</strong>
              </div>
              <div>
                <small>Integrated</small>
                <strong>{sourceStats.integrated}</strong>
              </div>
              <div>
                <small>Planned</small>
                <strong>{sourceStats.planned}</strong>
              </div>
            </div>
            <div className="source-list">
              {sources.map((source) => (
                <article key={source.id} className="source-item">
                  <div className="source-top">
                    <strong>{source.name}</strong>
                    <span
                      className={`source-status ${
                        source.integration_status === "live" ? "live" : "planned"
                      }`}
                    >
                      {source.integration_status === "live" ? "Live" : "Planned"}
                    </span>
                  </div>
                  <p>{source.role}</p>
                  <small>{source.category}</small>
                </article>
              ))}
            </div>
          </section>

          <section className="card stack-wrap">
            <h2>Technology Stack</h2>
            <div className="stack-grid">
              <div>
                <h3>Frontend</h3>
                <ul>
                  {(stackProfile.frontend || []).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </div>
              <div>
                <h3>Backend</h3>
                <ul>
                  {(stackProfile.backend || []).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </div>
              <div>
                <h3>Astronomy Libraries</h3>
                <ul>
                  {(stackProfile.astronomy_scientific || []).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </div>
              <div>
                <h3>Data Layer</h3>
                <ul>
                  {(stackProfile.database_storage || []).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </div>
            </div>
            <div className="deploy-note">
              <p>
                <b>Frontend Hosting:</b> {stackProfile.deployment?.frontend_hosting || "Vercel"}
              </p>
              <p>
                <b>Backend Hosting:</b> {stackProfile.deployment?.backend_hosting || "Render / Railway"}
              </p>
              <p>
                <b>Database Hosting:</b> {stackProfile.deployment?.database_hosting || "Supabase / Neon"}
              </p>
              <p>
                <b>Version Control:</b> {stackProfile.deployment?.version_control || "GitHub"}
              </p>
            </div>
          </section>
        </section>
      )}
    </main>
  );
}

export default App;
