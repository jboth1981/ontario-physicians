"""FTS search + haversine distance ranking for physician lookup."""

import math
import sqlite3


def haversine_km(lat1, lng1, lat2, lng2):
    """Compute the great-circle distance between two points in km."""
    R = 6371.0
    lat1, lng1, lat2, lng2 = map(math.radians, (lat1, lng1, lat2, lng2))
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_filter_options(conn: sqlite3.Connection):
    """Fetch distinct values for filter dropdowns."""
    specialties = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT specialty_name FROM specialties "
            "WHERE specialty_name IS NOT NULL ORDER BY specialty_name"
        )
    ]

    langs_raw = conn.execute(
        "SELECT DISTINCT languages FROM physicians "
        "WHERE languages IS NOT NULL AND languages != ''"
    ).fetchall()
    lang_set = set()
    for row in langs_raw:
        for lang in row[0].split(","):
            lang = lang.strip()
            if lang:
                lang_set.add(lang)
    languages = sorted(lang_set)

    genders = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT gender FROM physicians "
            "WHERE gender IS NOT NULL AND gender != 'No Information Available' "
            "ORDER BY gender"
        )
    ]

    return {
        "specialties": specialties,
        "languages": languages,
        "genders": genders,
    }


def search_physicians(
    conn: sqlite3.Connection,
    query: str,
    user_lat: float,
    user_lng: float,
    limit: int = 10,
    specialty: str = "",
    gender: str = "",
    language: str = "",
    active_only: bool = True,
):
    """Search for physicians matching query + filters, ranked by distance from user.

    Returns a list of dicts with physician info + distance_km.
    """
    # FTS search — over-fetch to allow distance re-ranking and post-filtering
    cursor = conn.execute(
        """
        SELECT p.cpso_number, p.full_name, p.registration_status, p.registration_class,
               p.gender, p.languages, p.medical_school,
               f.specialties, f.hospitals,
               a.name AS practice_name, a.street, a.city, a.province, a.postal_code,
               a.phone, a.lat, a.lng
        FROM physicians_fts f
        JOIN physicians p ON p.cpso_number = CAST(f.cpso_number AS INTEGER)
        LEFT JOIN addresses a ON a.cpso_number = p.cpso_number
        WHERE physicians_fts MATCH ?
        ORDER BY rank
        LIMIT 500
        """,
        (query,),
    )

    rows = cursor.fetchall()

    # Group by physician — keep only the nearest address per physician
    physicians = {}
    for row in rows:
        cpso = row["cpso_number"]

        # Apply filters before distance calc
        if active_only and row["registration_status"] != "Active":
            continue
        if gender and row["gender"] != gender:
            continue
        if language and (not row["languages"] or language.upper() not in row["languages"].upper()):
            continue
        if specialty and (not row["specialties"] or specialty.lower() not in row["specialties"].lower()):
            continue

        lat = row["lat"]
        lng = row["lng"]

        if lat is not None and lng is not None:
            distance = haversine_km(user_lat, user_lng, lat, lng)
        else:
            distance = float("inf")

        if cpso not in physicians or distance < physicians[cpso]["distance_km"]:
            physicians[cpso] = {
                "cpso_number": cpso,
                "full_name": row["full_name"],
                "registration_status": row["registration_status"],
                "registration_class": row["registration_class"],
                "gender": row["gender"],
                "languages": row["languages"],
                "medical_school": row["medical_school"],
                "specialties": row["specialties"],
                "hospitals": row["hospitals"],
                "practice_name": row["practice_name"],
                "street": row["street"],
                "city": row["city"],
                "province": row["province"],
                "postal_code": row["postal_code"],
                "phone": row["phone"],
                "lat": lat,
                "lng": lng,
                "distance_km": distance,
            }

    # Sort by distance, take top N
    sorted_results = sorted(physicians.values(), key=lambda x: x["distance_km"])
    results = sorted_results[:limit]

    # Round distances for display
    for r in results:
        if r["distance_km"] != float("inf"):
            r["distance_km"] = round(r["distance_km"], 1)
        else:
            r["distance_km"] = None

    return results
