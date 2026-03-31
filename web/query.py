"""FTS search + haversine distance ranking for physician lookup."""

import math
import sqlite3


def bounding_box(lat, lng, km):
    """Compute a lat/lng bounding box around a point.

    Returns (min_lat, max_lat, min_lng, max_lng).
    Conservative approximation — box is slightly larger than the circle.
    """
    dlat = km / 111.32
    dlng = km / (111.32 * math.cos(math.radians(lat)))
    return (lat - dlat, lat + dlat, lng - dlng, lng + dlng)


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
    lang_counts: dict[str, int] = {}
    for row in langs_raw:
        for lang in row[0].split(","):
            lang = lang.strip()
            if lang:
                lang_counts[lang] = lang_counts.get(lang, 0) + 1
    by_freq = sorted(lang_counts, key=lambda l: lang_counts[l], reverse=True)
    top = by_freq[:5]
    rest = sorted(l for l in by_freq[5:])
    languages = top + rest

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
    user_lat: float,
    user_lng: float,
    keyword: str = "",
    specialties: list[str] | None = None,
    gender: str = "",
    language: str = "",
    active_only: bool = True,
    max_distance_km: float = 0,
    max_results: int = 100,
):
    """Search for physicians matching filters, ranked by distance from user.

    Builds a single SQL query with all filters applied in the database.
    Returns (results, total_found) where results is capped at max_results
    and total_found is the count before capping.
    """
    select_fields = """\
        p.cpso_number, p.full_name, p.registration_status, p.registration_class,
        p.gender, p.languages, p.medical_school,
        (SELECT GROUP_CONCAT(s2.specialty_name, ', ')
         FROM specialties s2 WHERE s2.cpso_number = p.cpso_number
        ) AS specialties,
        (SELECT GROUP_CONCAT(
            COALESCE(h.hospital_name, '') || ' ' || COALESCE(h.hospital_location, ''),
            ' | '
        ) FROM hospital_privileges h WHERE h.cpso_number = p.cpso_number
        ) AS hospitals,
        a.name AS practice_name, a.street, a.city, a.province, a.postal_code,
        a.phone, a.lat, a.lng"""

    joins = ["LEFT JOIN addresses a ON a.cpso_number = p.cpso_number"]
    conditions = ["a.lat IS NOT NULL"]
    params = []

    # Keyword search via FTS (no row limit — filters handle narrowing)
    if keyword:
        words = keyword.split()
        safe_query = " ".join('"' + w.replace('"', '""') + '"' for w in words if w)
        from_clause = (
            "physicians_fts f "
            "JOIN physicians p ON p.cpso_number = CAST(f.cpso_number AS INTEGER)"
        )
        conditions.append("physicians_fts MATCH ?")
        params.append(safe_query)
    else:
        from_clause = "physicians p"

    # Specialty filter via direct JOIN (no FTS needed)
    if specialties:
        placeholders = ",".join("?" for _ in specialties)
        joins.append("JOIN specialties s ON s.cpso_number = p.cpso_number")
        conditions.append(f"s.specialty_name IN ({placeholders})")
        params.extend(specialties)

    if active_only:
        conditions.append("p.registration_status = 'Active'")
    if gender:
        conditions.append("p.gender = ?")
        params.append(gender)
    if language:
        conditions.append("p.languages LIKE ?")
        params.append(f"%{language}%")

    if max_distance_km:
        min_lat, max_lat, min_lng, max_lng = bounding_box(user_lat, user_lng, max_distance_km)
        joins.append("JOIN addresses_rtree rt ON rt.id = a.id")
        conditions.append("rt.min_lat >= ? AND rt.max_lat <= ?")
        params.extend([min_lat, max_lat])
        conditions.append("rt.min_lng >= ? AND rt.max_lng <= ?")
        params.extend([min_lng, max_lng])

    join_clause = "\n".join(joins)
    where_clause = " AND ".join(conditions)

    sql = f"SELECT {select_fields}\nFROM {from_clause}\n{join_clause}\nWHERE {where_clause}"
    rows = conn.execute(sql, params).fetchall()

    # Compute distance and deduplicate (keep nearest address per physician)
    physicians = {}
    for row in rows:
        cpso = row["cpso_number"]
        lat = row["lat"]
        lng = row["lng"]

        if lat is not None and lng is not None:
            distance = haversine_km(user_lat, user_lng, lat, lng)
        else:
            distance = float("inf")

        if max_distance_km and distance > max_distance_km:
            continue

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

    total_found = len(physicians)

    # Sort by distance, cap at max_results
    sorted_results = sorted(physicians.values(), key=lambda x: x["distance_km"])
    results = sorted_results[:max_results]

    # Round distances for display
    for r in results:
        if r["distance_km"] != float("inf"):
            r["distance_km"] = round(r["distance_km"], 1)
        else:
            r["distance_km"] = None

    return results, total_found
