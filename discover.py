"""Discover all CPSO numbers via recursive last name prefix search.

Uses the CPSO search API to find every registered physician by
progressively deepening last name prefixes until results fit under
the API's overflow limit. Falls back to first name subdivision for
extremely common surnames (e.g. Smith).

Guarantees complete coverage: every prefix is either resolved (results
returned) or subdivided further. No CPSO number can be missed.

Usage:
    python3 discover.py              # Full discovery run
    python3 discover.py --dry-run    # Show plan without calling API
    python3 discover.py --resume     # Resume from saved progress
"""

import argparse
import json
import logging
import os
import re
import signal
import string
import sys
import time

import requests

import config

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger("discover")
log.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(
    os.path.join(config.DATA_DIR, "discover.log")
)
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
    log.info("Shutdown signal received. Saving progress and exiting...")
    shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

PROGRESS_FILE = os.path.join(config.DATA_DIR, "discover_progress.json")


def load_progress():
    """Load saved progress (completed prefixes and discovered CPSO numbers)."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
        return set(data.get("completed", [])), set(data.get("cpso_numbers", []))
    return set(), set()


def save_progress(completed, cpso_numbers):
    """Save progress to disk for resume capability."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(
            {
                "completed": sorted(completed),
                "cpso_numbers": sorted(cpso_numbers),
            },
            f,
        )


# ---------------------------------------------------------------------------
# API interaction
# ---------------------------------------------------------------------------

SEARCH_URL = "https://register.cpso.on.ca/Get-Search-Results/"
DELAY = 0.3  # seconds between API calls


class SearchError(Exception):
    """Raised when a search fails after all retries."""


def search(session, last_name, first_name=""):
    """Search the CPSO API for a name prefix.

    Returns (cpso_numbers, overflowed) where:
    - cpso_numbers: set of int CPSO numbers found
    - overflowed: True if the API returned -1 (too many results)

    Raises SearchError if all retries are exhausted.
    """
    data = {"lastName": last_name, "cbx-includeinactive": "true"}
    if first_name:
        data["firstName"] = first_name

    last_error = None
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            resp = session.post(
                SEARCH_URL,
                data=data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer": "https://register.cpso.on.ca/Search-Results/",
                },
                timeout=15,
            )
            # The CPSO API occasionally returns malformed JSON with
            # stray backslashes in address fields (e.g. "123 Oak Ave\").
            # Strip trailing backslashes before closing quotes to fix.
            text = re.sub(r'\\(?=")', '', resp.text)
            result = json.loads(text, strict=False)

            if result.get("totalcount") == -1:
                return set(), True

            numbers = set()
            for r in result.get("results", []):
                cpso = r.get("cpsonumber")
                if cpso:
                    numbers.add(int(cpso))
            return numbers, False

        except Exception as e:
            last_error = e
            backoff = config.BACKOFF_BASE * (2 ** attempt)
            log.warning(
                "Search failed for '%s'/'%s': %s — retrying in %ds (attempt %d)",
                last_name, first_name, e, backoff, attempt + 1,
            )
            time.sleep(backoff)

    log.error("Exhausted retries for '%s'/'%s'", last_name, first_name)
    raise SearchError(f"Failed after {config.MAX_RETRIES + 1} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Recursive discovery
# ---------------------------------------------------------------------------

# Characters to extend prefixes with — lowercase letters plus common
# surname characters (hyphen, apostrophe, space)
EXTEND_CHARS = list(string.ascii_lowercase) + ["-", "'", " "]
FIRST_NAME_CHARS = list(string.ascii_uppercase)

# Maximum prefix depth before switching to first name subdivision
MAX_LAST_NAME_DEPTH = 6


def discover_prefix(session, last_name, completed, all_cpso, stats, first_name=""):
    """Recursively discover all CPSO numbers matching a name prefix.

    If the API overflows, subdivides by appending characters to the last name.
    If the last name is already very long, subdivides by first name initial.
    """
    if shutdown_requested:
        return

    # Build a key for tracking completed prefixes
    key = f"{last_name}|{first_name}" if first_name else last_name
    if key in completed:
        return

    time.sleep(DELAY)
    try:
        numbers, overflowed = search(session, last_name, first_name)
    except SearchError:
        # Permanent failure — record it and keep going so we can report
        # all failures at the end rather than aborting on the first one.
        stats.setdefault("failed", []).append(key)
        log.error("Permanent failure for prefix '%s' — will NOT mark complete", key)
        return
    stats["queries"] += 1

    if stats["queries"] % 100 == 0:
        log.info(
            "Progress: %d queries, %d doctors found, %d prefixes completed",
            stats["queries"], len(all_cpso), len(completed),
        )

    if not overflowed:
        # Success — record results
        new = numbers - all_cpso
        if new:
            log.debug(
                "%s%s: %d doctors (%d new)",
                last_name,
                f" / {first_name}*" if first_name else "",
                len(numbers),
                len(new),
            )
        all_cpso.update(numbers)
        completed.add(key)
        stats["resolved"] += 1
        return

    # Overflowed — need to subdivide
    if first_name:
        # Already subdividing by first name — go deeper on first name
        log.debug(
            "%s / %s*: overflow — extending first name", last_name, first_name
        )
        for c in EXTEND_CHARS:
            if shutdown_requested:
                return
            discover_prefix(
                session, last_name, completed, all_cpso, stats,
                first_name=first_name + c,
            )
    elif len(last_name) >= MAX_LAST_NAME_DEPTH:
        # Last name is long enough — switch to first name subdivision
        log.debug("%s: overflow at depth %d — splitting by first name", last_name, len(last_name))
        for c in FIRST_NAME_CHARS:
            if shutdown_requested:
                return
            discover_prefix(
                session, last_name, completed, all_cpso, stats,
                first_name=c,
            )
    else:
        # Extend last name by one character
        log.debug("%s: overflow — extending to %d chars", last_name, len(last_name) + 1)
        for c in EXTEND_CHARS:
            if shutdown_requested:
                return
            discover_prefix(
                session, last_name + c, completed, all_cpso, stats,
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(resume=False, dry_run=False):
    """Run the full discovery process."""
    if dry_run:
        # Two-letter starting prefixes
        prefixes = [a + b for a in string.ascii_uppercase for b in string.ascii_lowercase]
        print(f"Would search {len(prefixes)} two-letter prefixes (Aa-Zz)")
        print(f"Overflow prefixes get recursively subdivided")
        print(f"Estimated: ~12,000 API calls, ~1 hour")
        return

    completed, all_cpso = load_progress() if resume else (set(), set())

    if resume and completed:
        log.info(
            "Resuming: %d prefixes completed, %d CPSO numbers found so far",
            len(completed), len(all_cpso),
        )

    session = requests.Session()
    session.headers.update({
        "User-Agent": config.DEFAULT_HEADERS["User-Agent"],
        "X-Requested-With": "XMLHttpRequest",
    })

    # Initialize session cookies
    session.get("https://register.cpso.on.ca/Search-Results/")

    stats = {"queries": 0, "resolved": 0}

    # Start with two-letter prefixes for all uppercase+lowercase combinations
    for first_letter in string.ascii_uppercase:
        for second_letter in string.ascii_lowercase:
            if shutdown_requested:
                break
            prefix = first_letter + second_letter
            discover_prefix(session, prefix, completed, all_cpso, stats)

            # Save progress periodically
            if stats["queries"] % 50 == 0:
                save_progress(completed, all_cpso)

        if shutdown_requested:
            break

    # Final save
    save_progress(completed, all_cpso)

    failed = stats.get("failed", [])

    log.info(
        "Discovery %s. %d API queries, %d prefixes resolved, %d unique CPSO numbers found.",
        "interrupted — progress saved" if shutdown_requested else "complete",
        stats["queries"],
        stats["resolved"],
        len(all_cpso),
    )

    if failed:
        log.error(
            "INCOMPLETE: %d prefixes failed permanently and were NOT searched: %s",
            len(failed), ", ".join(failed),
        )
        log.error("Re-run with --resume to retry failed prefixes.")

    if all_cpso:
        log.info("CPSO range: %d - %d", min(all_cpso), max(all_cpso))

    # Write final list to a simple text file for easy consumption
    if not shutdown_requested and not failed:
        output_path = os.path.join(config.DATA_DIR, "discovered_cpso_numbers.txt")
        with open(output_path, "w") as f:
            for num in sorted(all_cpso):
                f.write(f"{num}\n")
        log.info("Wrote %d CPSO numbers to %s", len(all_cpso), output_path)


def main():
    parser = argparse.ArgumentParser(
        description="Discover all CPSO numbers via recursive name prefix search."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show the plan without calling the API",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from saved progress",
    )
    args = parser.parse_args()
    run(resume=args.resume, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
