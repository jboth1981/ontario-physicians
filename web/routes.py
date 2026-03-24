"""Route definitions for the physician search web app."""

import sqlite3

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse

import config
from web.app import templates
from web.geocoding import geocode_postal_code
from web.query import get_filter_options, search_physicians
from web.rate_limit import RATE_LIMIT_RESPONSE, is_rate_limited

router = APIRouter()


def get_db():
    conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@router.get("/", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query(default="", description="Search keyword"),
    postal_code: str = Query(default="", description="Postal code"),
    specialty: str = Query(default="", description="Filter by specialty"),
    gender: str = Query(default="", description="Filter by gender"),
    language: str = Query(default="", description="Filter by language"),
    active_only: str = Query(default="on", description="Show only active physicians"),
    conn: sqlite3.Connection = Depends(get_db),
):
    if is_rate_limited(request):
        return RATE_LIMIT_RESPONSE

    results = []
    user_lat = None
    user_lng = None
    error = None
    is_active_only = active_only == "on"

    filter_options = get_filter_options(conn)

    # Build FTS query: use keyword if provided, otherwise use specialty name
    fts_query = q.strip()
    if not fts_query and specialty:
        fts_query = specialty
    has_search = fts_query and postal_code.strip()

    if has_search:
        user_lat, user_lng = await geocode_postal_code(conn, postal_code)

        if user_lat is None:
            error = f"Could not locate postal code \"{postal_code}\". Please check and try again."
        else:
            try:
                results = search_physicians(
                    conn,
                    fts_query,
                    user_lat,
                    user_lng,
                    specialty=specialty,
                    gender=gender,
                    language=language,
                    active_only=is_active_only,
                )
            except Exception:
                error = "Search failed. Try simplifying your search terms."

        if not results and not error:
            error = f"No results found for \"{q or specialty}\"."

    return templates.TemplateResponse(
        request,
        "search.html",
        {
            "q": q,
            "postal_code": postal_code,
            "specialty": specialty,
            "gender": gender,
            "language": language,
            "active_only": is_active_only,
            "results": results,
            "user_lat": user_lat,
            "user_lng": user_lng,
            "error": error,
            "filter_options": filter_options,
        },
    )
