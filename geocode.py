"""Batch geocode physician addresses by postal code using Google Geocoding API."""

import argparse
import logging
import signal
import sqlite3
import sys
import time

import requests

import config
import db

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

log = logging.getLogger("geocoder")
log.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(config.GEOCODE_LOG_PATH)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
)
log.addHandler(file_handler)

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.INFO)
stderr_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
)
log.addHandler(stderr_handler)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

shutdown_requested = False


def _handle_signal(signum, frame):
    global shutdown_requested
    log.info("Shutdown signal received (signal %d). Finishing current work...", signum)
    shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------


def geocode_postal_code(session, postal_code):
    """Call Google Geocoding API for a Canadian postal code.

    Returns (lat, lng, status) where status is 'ok', 'zero_results', or 'error'.
    """
    try:
        resp = session.get(
            config.GEOCODE_URL,
            params={
                "address": f"{postal_code}, Ontario, Canada",
                "key": config.GOOGLE_API_KEY,
                "components": "country:CA",
            },
            timeout=10,
        )
        data = resp.json()
        api_status = data.get("status")

        if api_status == "OK" and data.get("results"):
            location = data["results"][0]["geometry"]["location"]
            return location["lat"], location["lng"], "ok"

        if api_status == "ZERO_RESULTS":
            return None, None, "zero_results"

        log.warning(
            "Unexpected API status %r for postal code %s", api_status, postal_code
        )
        return None, None, "error"

    except Exception as e:
        log.error("Request failed for postal code %s: %s", postal_code, e)
        return None, None, "error"


def get_pending_postal_codes(conn, retry_errors=False):
    """Get distinct postal codes from addresses that aren't yet cached."""
    query = """
        SELECT DISTINCT a.postal_code
        FROM addresses a
        WHERE a.postal_code IS NOT NULL
          AND a.postal_code != ''
          AND a.postal_code NOT IN (
              SELECT gc.postal_code FROM geocode_cache gc
              WHERE gc.status IN ('ok', 'zero_results')
          )
    """
    if retry_errors:
        # Include postal codes with 'error' status for retry
        query = """
            SELECT DISTINCT a.postal_code
            FROM addresses a
            WHERE a.postal_code IS NOT NULL
              AND a.postal_code != ''
              AND a.postal_code NOT IN (
                  SELECT gc.postal_code FROM geocode_cache gc
                  WHERE gc.status IN ('ok', 'zero_results')
              )
        """
    else:
        query = """
            SELECT DISTINCT a.postal_code
            FROM addresses a
            WHERE a.postal_code IS NOT NULL
              AND a.postal_code != ''
              AND a.postal_code NOT IN (
                  SELECT gc.postal_code FROM geocode_cache gc
              )
        """

    return [row[0] for row in conn.execute(query)]


def run_geocoding(dry_run=False, retry_errors=False):
    """Main geocoding loop."""
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode=WAL")
    pending = get_pending_postal_codes(conn, retry_errors=retry_errors)

    log.info("Found %d postal codes to geocode", len(pending))

    if dry_run:
        for pc in pending[:20]:
            print(f"  {pc}")
        if len(pending) > 20:
            print(f"  ... and {len(pending) - 20} more")
        conn.close()
        return

    if not config.GOOGLE_API_KEY or config.GOOGLE_API_KEY == "your-api-key-here":
        log.error("GOOGLE_GEOCODING_API_KEY not set in .env")
        conn.close()
        sys.exit(1)

    session = requests.Session()
    geocoded = 0
    errors = 0
    pending_commits = 0

    for postal_code in pending:
        if shutdown_requested:
            log.info("Shutdown requested. Committing and exiting.")
            conn.commit()
            break

        lat, lng, status = geocode_postal_code(session, postal_code)

        # Write with retry — the scraper may hold a write lock
        for attempt in range(10):
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO geocode_cache
                       (postal_code, lat, lng, status) VALUES (?, ?, ?, ?)""",
                    (postal_code, lat, lng, status),
                )
                if status == "ok":
                    conn.execute(
                        "UPDATE addresses SET lat = ?, lng = ? WHERE postal_code = ?",
                        (lat, lng, postal_code),
                    )
                conn.commit()
                break
            except sqlite3.OperationalError:
                if attempt < 9:
                    time.sleep(3)
                else:
                    log.warning("Could not write after 10 retries for %s, skipping", postal_code)

        if status == "ok":
            geocoded += 1
            log.debug("Geocoded %s -> (%.6f, %.6f)", postal_code, lat, lng)
        else:
            errors += 1
            log.debug("Postal code %s: %s", postal_code, status)

        time.sleep(config.GEOCODE_DELAY)

    conn.commit()
    log.info(
        "Done. Geocoded %d postal codes. Errors/zero results: %d.", geocoded, errors
    )
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    arg_parser = argparse.ArgumentParser(
        description="Batch geocode physician addresses by postal code."
    )
    arg_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List postal codes to geocode without calling the API",
    )
    arg_parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry postal codes that previously returned errors",
    )
    args = arg_parser.parse_args()
    run_geocoding(dry_run=args.dry_run, retry_errors=args.retry_errors)


if __name__ == "__main__":
    main()
