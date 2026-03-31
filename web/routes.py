"""Route definitions for the physician search web app."""

import math
import sqlite3
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse

import config
from web.app import templates
from web.geocoding import geocode_postal_code
from web.query import get_filter_options, search_physicians
from web.rate_limit import RATE_LIMIT_RESPONSE, is_rate_limited

router = APIRouter()

MAX_RESULTS = 100
PER_PAGE = 10


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
    lat: str = Query(default="", description="Latitude from map click"),
    lng: str = Query(default="", description="Longitude from map click"),
    specialty: list[str] = Query(default=[], description="Filter by specialty"),
    gender: str = Query(default="", description="Filter by gender"),
    language: str = Query(default="", description="Filter by language"),
    active_only: str = Query(default="on", description="Show only active physicians"),
    distance: str = Query(default="", description="Max distance in km"),
    page: int = Query(default=1, ge=1, description="Page number"),
    conn: sqlite3.Connection = Depends(get_db),
):
    if is_rate_limited(request):
        return RATE_LIMIT_RESPONSE

    all_results = []
    page_results = []
    user_lat = None
    user_lng = None
    error = None
    is_active_only = active_only == "on"
    max_distance_km = float(distance) if distance else 0
    total_found = 0
    total_pages = 1
    prev_url = None
    next_url = None

    filter_options = get_filter_options(conn)

    # Filter out empty strings from specialty list
    specialties = [s for s in specialty if s]

    # Accept direct lat/lng from map click, or geocode from postal code
    has_location = False
    if lat and lng:
        try:
            user_lat = float(lat)
            user_lng = float(lng)
            has_location = True
        except ValueError:
            pass

    has_search = (q.strip() or specialties) and (has_location or postal_code.strip())

    if has_search:
        if not has_location:
            user_lat, user_lng = await geocode_postal_code(conn, postal_code)

        if user_lat is None:
            error = f"Could not locate \"{postal_code}\". Please check and try again."
        else:
            try:
                all_results, total_found = search_physicians(
                    conn,
                    user_lat,
                    user_lng,
                    keyword=q.strip(),
                    specialties=specialties,
                    gender=gender,
                    language=language,
                    active_only=is_active_only,
                    max_distance_km=max_distance_km,
                    max_results=MAX_RESULTS,
                )
            except Exception:
                error = "Search failed. Try simplifying your search terms."

        if not all_results and not error:
            error = f"No results found for \"{q or ', '.join(specialties)}\"."

    # Pagination
    if all_results:
        total_pages = math.ceil(len(all_results) / PER_PAGE)
        page = min(page, total_pages)
        start = (page - 1) * PER_PAGE
        page_results = all_results[start:start + PER_PAGE]

        if total_pages > 1:
            base_params = [
                (k, v) for k, v in request.query_params.multi_items()
                if k != "page"
            ]
            if page > 1:
                prev_url = "/?" + urlencode(base_params + [("page", str(page - 1))])
            if page < total_pages:
                next_url = "/?" + urlencode(base_params + [("page", str(page + 1))])

    return templates.TemplateResponse(
        request,
        "search.html",
        {
            "q": q,
            "postal_code": postal_code,
            "specialties": specialties,
            "gender": gender,
            "language": language,
            "active_only": is_active_only,
            "distance": distance,
            "results": all_results,
            "page_results": page_results,
            "page": page,
            "total_pages": total_pages,
            "total_found": total_found,
            "per_page": PER_PAGE,
            "user_lat": user_lat,
            "user_lng": user_lng,
            "error": error,
            "filter_options": filter_options,
            "prev_url": prev_url,
            "next_url": next_url,
        },
    )
