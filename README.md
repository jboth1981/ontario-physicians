# Ontario Physician Search

A web application for searching Ontario physicians by name, specialty, language, and location. Data is sourced from the [College of Physicians and Surgeons of Ontario (CPSO)](https://register.cpso.on.ca/) public register.

Live at: `http://3.99.186.120`

## Architecture

- **Backend:** FastAPI with Jinja2 templates
- **Database:** SQLite with FTS5 (full-text search) and R-tree (spatial indexing)
- **Frontend:** Leaflet.js map with marker clustering, server-rendered HTML
- **Hosting:** AWS Lightsail (512MB instance), deployed via GitHub Actions
- **Geocoding:** Google Geocoding API for postal code/city coordinate lookup

## Data Pipeline

The data pipeline has three stages that run independently:

### 1. Discover (`discover.py`)

Finds every CPSO number in the registry by searching the CPSO API.

**Strategy:** Searches all 676 two-letter last name prefixes (Aa-Zz). The API does
substring matching, so `lastName="Sm"` matches Smith, Bhusumane, Al-Kassmy, etc. When
a search overflows (API returns `totalcount: -1`), it subdivides by first name initial
(A-Z), then extends the first name further if needed (Aa, Ab, ...).

This guarantees complete coverage: every doctor has a name, and every name contains
at least one two-letter substring that will be searched. The first-name subdivision
ensures doctors with short last names (Ng, Li, Wu) are not missed.

```bash
python3 discover.py              # Full run
python3 discover.py --resume     # Resume from saved progress
python3 discover.py --dry-run    # Preview without calling API
```

- Progress saved to `discover_progress.json` every 50 queries
- Handles Ctrl+C gracefully (saves progress before exiting)
- Outputs `discovered_cpso_numbers.txt` on completion
- Handles malformed JSON from the API (stray backslashes in address fields)
- Failed prefixes are tracked and reported; the output file is only written
  if all prefixes succeed

### 2. Scrape (`scraper.py`)

Fetches each doctor's full profile page from the CPSO website and parses it
into structured data (name, addresses, specialties, languages, etc.).

```bash
python3 scraper.py                          # Scrape default CPSO range
python3 scraper.py --start 18000 --end 290000  # Custom range
```

- Stores parsed data in `cpso_physicians.db`
- Retains raw HTML for re-parsing if needed
- Supports resume (tracks progress in the database)

### 3. Geocode (`geocode.py`)

Converts postal codes to lat/lng coordinates using the Google Geocoding API,
enabling distance-based search.

```bash
python3 geocode.py
```

- Backfills coordinates for addresses missing lat/lng
- Populates the R-tree spatial index after geocoding
- Requires `GOOGLE_GEOCODING_API_KEY` in `.env`

## Web Application

```bash
cd web
uvicorn app:app --reload
```

### Search features

- **Name search:** Full-text search across physician names
- **Specialty filter:** Multi-select from all known specialties
- **Language filter:** Sorted by frequency (top 5, then alphabetical)
- **Gender filter**
- **Location search:** Postal code, city name, or click-to-pick on map
- **Distance filter:** 10/25/50/100/200/500 km radius
- **Map:** Interactive Leaflet map with clustered markers. Click a cluster
  to see a scrollable list of physicians at that location.

## Deployment

### Code deployment (automatic)

Pushing to `main` triggers GitHub Actions, which rsyncs code to the server
and restarts the service. See `.github/workflows/deploy.yml`.

### Database deployment (manual)

The local database contains raw HTML (~8GB) that won't fit on the 512MB server.
Use the deploy script to create a stripped copy, validate it, and swap it into
production with automatic rollback on failure:

```bash
python3 deploy_db.py
```

See [DEPLOY.md](DEPLOY.md) for manual steps and important notes.

## Project Structure

```
config.py        — Constants, paths, API keys
db.py            — Database schema, connection, R-tree management
discover.py      — CPSO number discovery via API search
scraper.py       — Profile page scraper and parser
parser.py        — HTML parsing for physician detail pages
geocode.py       — Postal code geocoding via Google API
search.py        — CPSO search API client (used by scraper)
deploy_db.py     — Safe database deployment with validation
web/
  app.py         — FastAPI application
  routes.py      — Route handlers
  query.py       — Search query builder (FTS5 + R-tree)
  templates/     — Jinja2 templates
  static/        — CSS, JS, favicon
```
