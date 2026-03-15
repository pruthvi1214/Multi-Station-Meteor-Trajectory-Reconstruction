# Meteor MVP Starter (24-Hour Hackathon Build)

This starter gives you a working end-to-end demo:

- FastAPI backend with fake-but-consistent meteor data
- React frontend with event catalogue
- 3D globe trajectory visualization
- Velocity comparison chart
- Scientific source registry + architecture stack endpoints

## Scientific Data Sources

- Global Meteor Network - Primary meteor observation dataset
- NASA Fireball API - Fireball event catalogue (integrated in current MVP)
- American Meteor Society - Real-time meteor reports
- IAU Meteor Data Centre - Meteor shower classification
- JPL Horizons API - Planetary positions and orbit calculations
- SonotaCo Meteor Orbit Database - Reference meteor orbit dataset
- EDMOND Database - European multi-station meteor observations
- NASA Meteoroid Environment Office Dataset - Meteoroid environment modelling

## Technology Stack

- Frontend: React / Next.js, Tailwind CSS, CesiumJS, Three.js, Plotly.js
- Backend: Python, FastAPI, NumPy, SciPy, Pandas
- Astronomy libraries: Astropy, Skyfield
- Database & cache: PostgreSQL, Redis (optional)
- Deployment: Vercel (frontend), Render or Railway (backend), Supabase or Neon (database), GitHub (version control)

## Project Structure

```text
meteor-project
|-- backend
|   |-- main.py
|   `-- requirements.txt
|-- dataset
|   `-- events.json
`-- frontend
    |-- src
    |   |-- App.jsx
    |   |-- App.css
    |   `-- main.jsx
    `-- package.json
```

## 1) Run Backend

```bash
cd backend
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload
```

Optional environment variables:

- `DATABASE_URL` (defaults to local SQLite at `dataset/meteor.db`; use PostgreSQL URL in cloud)
- `REDIS_URL` (optional Redis cache backend)
- `CACHE_TTL_SECONDS` (default `120`)

API endpoints:

- `GET /`
- `GET /health`
- `GET /sources`
- `GET /stack`
- `GET /project-status`
- `GET /data-status`
- `GET /dataset-range?source=real`
- `POST /sync-real-events?limit=20000`
- `GET /events?source=real&q=fireball&date_from=2024-01-01&date_to=2026-12-31&station=nasa`
- `GET /events/{event_id}`
- `GET /trajectory/{event_id}`
- `GET /compare?left=1&right=2`

To load real data the first time:

1. Start backend (`uvicorn main:app --reload`)
2. Open `http://127.0.0.1:8000/docs`
3. Run `POST /sync-real-events`

After sync, the frontend runs in `Real NASA Data (Live Only)` mode.
No mock/dummy fallback is used for event rendering.

UI behavior:

- Auto-detects date range from real dataset source
- Defaults `Date To` to latest available dataset date
- Warns when selected dates are outside available range

## 2) Run Frontend

Open a new terminal:

```bash
cd frontend
copy .env.example .env
npm.cmd install
npm.cmd run dev
```

Open the URL shown by Vite (usually `http://127.0.0.1:5173`).

## 3) What to Demo to Judges

1. Open event catalogue and pick a meteor event
2. Show 3D trajectory arc on globe
3. Show velocity profile chart
4. Switch compare event and show second curve

## 4) Fast Polishing Ideas

- Add one "reconstructed by multi-station model" badge in UI
- Add loading spinner for event fetch
- Add one screenshot slide for architecture + one for results
- Record a 60-second backup demo video

## 5) Quick Cloud Deploy (Optional)

- Backend (Render):
  - Use `render.yaml` from project root
  - API URL will look like `https://meteor-mvp-api.onrender.com`
- Frontend (Vercel):
  - Import `frontend` folder as project root
  - Add env var `VITE_API_URL=<your-render-url>`
  - `vercel.json` is already included for SPA routing
