"""Postal code geocoding with cache for search-time lookups."""

import sqlite3

import httpx

import config


async def geocode_postal_code(conn: sqlite3.Connection, postal_code: str):
    """Look up lat/lng for a postal code. Uses cache, falls back to Google API.

    Returns (lat, lng) or (None, None).
    """
    postal_code = postal_code.strip().upper()

    # Check cache
    row = conn.execute(
        "SELECT lat, lng, status FROM geocode_cache WHERE postal_code = ?",
        (postal_code,),
    ).fetchone()

    if row:
        if row["status"] == "ok":
            return row["lat"], row["lng"]
        return None, None

    # Cache miss — call Google API
    if not config.GOOGLE_API_KEY or config.GOOGLE_API_KEY == "your-api-key-here":
        return None, None

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
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
            lat, lng = location["lat"], location["lng"]
            conn.execute(
                """INSERT OR REPLACE INTO geocode_cache
                   (postal_code, lat, lng, status) VALUES (?, ?, ?, 'ok')""",
                (postal_code, lat, lng),
            )
            conn.commit()
            return lat, lng

        status = "zero_results" if api_status == "ZERO_RESULTS" else "error"
        conn.execute(
            """INSERT OR REPLACE INTO geocode_cache
               (postal_code, lat, lng, status) VALUES (?, NULL, NULL, ?)""",
            (postal_code, status),
        )
        conn.commit()
        return None, None

    except Exception:
        return None, None
