"""Constants and configuration for the CPSO physician register scraper."""

import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# URLs
BASE_URL = "https://register.cpso.on.ca"
SEARCH_URL = f"{BASE_URL}/Get-Search-Results/"
DETAIL_URL = f"{BASE_URL}/physician-info/"

# CPSO number range (known valid range with ~29% density)
DEFAULT_START = 50000
DEFAULT_END = 155000

# Pacing
MIN_DELAY = 2.0
MAX_DELAY = 3.5

# Retry / backoff
MAX_RETRIES = 3
BACKOFF_BASE = 5  # seconds; doubles each retry: 5, 10, 20, 40, 80

# Batch commit size
BATCH_SIZE = 10

# Progress logging interval
PROGRESS_INTERVAL = 100

# Paths
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DATA_DIR, "cpso_physicians.db")
LOG_PATH = os.path.join(DATA_DIR, "scraper.log")

# HTTP headers
# Google Geocoding
GOOGLE_API_KEY = os.environ.get("GOOGLE_GEOCODING_API_KEY", "")
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
GEOCODE_LOG_PATH = os.path.join(DATA_DIR, "geocode.log")
GEOCODE_BATCH_SIZE = 50
GEOCODE_DELAY = 0.1  # 100ms between API calls (10 QPS)

# HTTP headers
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

SEARCH_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": f"{BASE_URL}/Search-Results/",
}
