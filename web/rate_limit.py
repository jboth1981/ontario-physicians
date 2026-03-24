"""Simple in-memory rate limiter by IP address."""

import time
from collections import defaultdict

from fastapi import Request
from fastapi.responses import HTMLResponse

# Max requests per IP within the time window
MAX_REQUESTS = 30
WINDOW_SECONDS = 60

_requests: dict[str, list[float]] = defaultdict(list)


def is_rate_limited(request: Request) -> bool:
    """Check if the client IP has exceeded the rate limit."""
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    cutoff = now - WINDOW_SECONDS

    # Prune old entries
    _requests[ip] = [t for t in _requests[ip] if t > cutoff]

    if len(_requests[ip]) >= MAX_REQUESTS:
        return True

    _requests[ip].append(now)
    return False


RATE_LIMIT_RESPONSE = HTMLResponse(
    "<h1>Too many requests</h1><p>Please wait a minute and try again.</p>",
    status_code=429,
)
