"""Main scraper: iterates CPSO numbers, checks existence, fetches details."""

import argparse
import json
import logging
import random
import signal
import sys
import time

import requests

import config
import db
import parser as physician_parser

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

log = logging.getLogger("cpso_scraper")
log.setLevel(logging.DEBUG)

# File handler — DEBUG level
file_handler = logging.FileHandler(config.LOG_PATH)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
)
log.addHandler(file_handler)

# Stderr handler — INFO level
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
# HTTP helpers
# ---------------------------------------------------------------------------


def _make_session():
    """Create a requests.Session with default headers."""
    session = requests.Session()
    session.headers.update(config.DEFAULT_HEADERS)
    return session


def _delay():
    """Sleep for a random interval between requests."""
    time.sleep(random.uniform(config.MIN_DELAY, config.MAX_DELAY))


def _request_with_retry(session, method, url, retries_left=None, **kwargs):
    """Make an HTTP request with retry + exponential backoff.

    Returns a requests.Response on success, or None after exhausting retries.
    """
    if retries_left is None:
        retries_left = config.MAX_RETRIES

    backoff = config.BACKOFF_BASE

    for attempt in range(retries_left + 1):
        try:
            resp = session.request(method, url, timeout=30, **kwargs)

            if resp.status_code == 200:
                return resp

            if resp.status_code in (429, 403):
                log.warning(
                    "Got %d from %s — backing off %ds (attempt %d)",
                    resp.status_code,
                    url,
                    backoff,
                    attempt + 1,
                )
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code >= 500:
                log.warning(
                    "Got %d from %s — retrying in %ds (attempt %d)",
                    resp.status_code,
                    url,
                    backoff,
                    attempt + 1,
                )
                time.sleep(backoff)
                backoff *= 2
                continue

            # Other 4xx — don't retry
            log.error("Got %d from %s — not retrying", resp.status_code, url)
            return None

        except requests.RequestException as e:
            log.warning(
                "Request error for %s: %s — retrying in %ds (attempt %d)",
                url,
                e,
                backoff,
                attempt + 1,
            )
            time.sleep(backoff)
            backoff *= 2

    log.error("Exhausted retries for %s", url)
    return None


# ---------------------------------------------------------------------------
# Core scraping functions
# ---------------------------------------------------------------------------


def check_exists(session, cpso_number):
    """Check if a CPSO number exists via the search API.

    Returns True if found, False if not found, None on error.
    """
    resp = _request_with_retry(
        session,
        "POST",
        config.SEARCH_URL,
        headers=config.SEARCH_HEADERS,
        data={"cpsoNumber": str(cpso_number)},
    )
    if resp is None:
        return None

    try:
        result = json.loads(resp.text)
        total = result.get("totalcount", 0)
        return total > 0
    except (json.JSONDecodeError, KeyError) as e:
        log.error("Failed to parse search response for %d: %s", cpso_number, e)
        return None


def fetch_detail_page(session, cpso_number):
    """Fetch the physician detail HTML page.

    Returns the HTML string on success, None on error.
    """
    url = f"{config.DETAIL_URL}?cpsonum={cpso_number}"
    resp = _request_with_retry(session, "GET", url)
    if resp is None:
        return None
    return resp.text


# ---------------------------------------------------------------------------
# Reparse mode
# ---------------------------------------------------------------------------


def reparse(conn):
    """Re-extract data from stored raw HTML without re-downloading."""
    cursor = conn.execute(
        "SELECT cpso_number, raw_html FROM physicians WHERE raw_html IS NOT NULL"
    )
    count = 0
    for row in cursor:
        cpso_number = row["cpso_number"]
        raw_html = row["raw_html"]
        try:
            data = physician_parser.parse_physician_page(raw_html, cpso_number)
            # Keep raw_html as-is
            db.insert_physician(conn, data)
            db.update_fts_for_physician(conn, cpso_number)
            count += 1
            if count % 100 == 0:
                conn.commit()
                log.info("Reparsed %d physicians so far...", count)
        except Exception as e:
            log.error("Failed to reparse CPSO# %d: %s", cpso_number, e)

    conn.commit()
    log.info("Reparse complete. Processed %d physicians.", count)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def scrape_range(start, end):
    """Scrape CPSO numbers from start to end (inclusive)."""
    conn = db.get_connection()
    session = _make_session()

    # Load already-processed numbers for resume
    done = db.get_scraped_numbers(conn)
    total_range = end - start + 1
    already_done = sum(1 for n in range(start, end + 1) if n in done)
    log.info(
        "Scraping CPSO# %d-%d (%d numbers, %d already done)",
        start,
        end,
        total_range,
        already_done,
    )

    checked = 0
    found = 0
    errors = 0
    pending_commits = 0

    for cpso_number in range(start, end + 1):
        if shutdown_requested:
            log.info("Shutdown requested. Committing and exiting.")
            conn.commit()
            break

        if cpso_number in done:
            continue

        checked += 1

        # Phase 1: existence check
        exists = check_exists(session, cpso_number)
        _delay()

        if exists is None:
            # Error — mark and move on
            db.mark_error(conn, cpso_number)
            errors += 1
            pending_commits += 1
            log.debug("CPSO# %d: error during existence check", cpso_number)

        elif not exists:
            # Not found
            db.mark_not_found(conn, cpso_number)
            pending_commits += 1
            log.debug("CPSO# %d: not found", cpso_number)

        else:
            # Phase 2: fetch and parse detail page
            html = fetch_detail_page(session, cpso_number)
            _delay()

            if html is None:
                db.mark_error(conn, cpso_number)
                errors += 1
                pending_commits += 1
                log.warning("CPSO# %d: failed to fetch detail page", cpso_number)
            else:
                try:
                    data = physician_parser.parse_physician_page(html, cpso_number)
                    db.insert_physician(conn, data)
                    db.update_fts_for_physician(conn, cpso_number)
                    found += 1
                    pending_commits += 1
                    log.debug(
                        "CPSO# %d: scraped — %s",
                        cpso_number,
                        data.get("full_name", "?"),
                    )
                except Exception as e:
                    db.mark_error(conn, cpso_number)
                    errors += 1
                    pending_commits += 1
                    log.error("CPSO# %d: parse error: %s", cpso_number, e)

        # Batch commit
        if pending_commits >= config.BATCH_SIZE:
            conn.commit()
            pending_commits = 0

        # Progress logging
        if checked % config.PROGRESS_INTERVAL == 0:
            log.info(
                "Progress: checked %d / %d | found %d | errors %d | current CPSO# %d",
                checked + already_done,
                total_range,
                found,
                errors,
                cpso_number,
            )

    # Final commit
    conn.commit()
    log.info(
        "Done. Checked %d new numbers. Found %d physicians. Errors: %d.",
        checked,
        found,
        errors,
    )
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    arg_parser = argparse.ArgumentParser(
        description="Scrape the CPSO physician register."
    )
    arg_parser.add_argument(
        "--start",
        type=int,
        default=config.DEFAULT_START,
        help=f"First CPSO number to check (default: {config.DEFAULT_START})",
    )
    arg_parser.add_argument(
        "--end",
        type=int,
        default=config.DEFAULT_END,
        help=f"Last CPSO number to check (default: {config.DEFAULT_END})",
    )
    arg_parser.add_argument(
        "--reparse",
        action="store_true",
        help="Re-extract data from stored HTML without re-downloading",
    )
    arg_parser.add_argument(
        "--rebuild-fts",
        action="store_true",
        help="Rebuild the full-text search index from existing data",
    )

    args = arg_parser.parse_args()

    if args.reparse:
        conn = db.get_connection()
        reparse(conn)
        conn.close()
    elif args.rebuild_fts:
        conn = db.get_connection()
        log.info("Rebuilding FTS index...")
        db.rebuild_fts(conn)
        conn.commit()
        conn.close()
        log.info("FTS rebuild complete.")
    else:
        scrape_range(args.start, args.end)


if __name__ == "__main__":
    main()
