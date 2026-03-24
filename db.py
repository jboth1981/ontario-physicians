"""SQLite schema, insert helpers, and FTS indexing for the CPSO scraper."""

import sqlite3

from config import DB_PATH

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS physicians (
    cpso_number     INTEGER PRIMARY KEY,
    full_name       TEXT,
    first_name      TEXT,
    last_name       TEXT,
    gender          TEXT,
    languages       TEXT,
    former_name     TEXT,
    member_status   TEXT,
    registration_status TEXT,
    status_date     TEXT,
    registration_class TEXT,
    medical_school  TEXT,
    graduation_year TEXT,
    raw_html        TEXT,
    scraped_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS addresses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpso_number     INTEGER NOT NULL REFERENCES physicians(cpso_number),
    address_type    TEXT,  -- 'primary' or 'additional'
    name            TEXT,
    street          TEXT,
    city            TEXT,
    province        TEXT,
    postal_code     TEXT,
    phone           TEXT,
    phone_ext       TEXT,
    fax             TEXT,
    email           TEXT
);

CREATE TABLE IF NOT EXISTS specialties (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpso_number     INTEGER NOT NULL REFERENCES physicians(cpso_number),
    specialty_name  TEXT,
    certifying_body TEXT,
    effective_date  TEXT
);

CREATE TABLE IF NOT EXISTS hospital_privileges (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpso_number     INTEGER NOT NULL REFERENCES physicians(cpso_number),
    hospital_name   TEXT,
    hospital_location TEXT
);

CREATE TABLE IF NOT EXISTS professional_corporations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpso_number     INTEGER NOT NULL REFERENCES physicians(cpso_number),
    corp_name       TEXT,
    corp_status     TEXT,
    end_date        TEXT,
    business_address TEXT,
    shareholders    TEXT
);

CREATE TABLE IF NOT EXISTS registration_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpso_number     INTEGER NOT NULL REFERENCES physicians(cpso_number),
    details         TEXT,
    effective_date  TEXT
);

CREATE TABLE IF NOT EXISTS practice_conditions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpso_number     INTEGER NOT NULL REFERENCES physicians(cpso_number),
    condition_text  TEXT
);

CREATE TABLE IF NOT EXISTS public_notifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpso_number     INTEGER NOT NULL REFERENCES physicians(cpso_number),
    notification_type TEXT,  -- 'current_referral' or 'past_finding'
    summary         TEXT
);

CREATE TABLE IF NOT EXISTS scrape_progress (
    cpso_number     INTEGER PRIMARY KEY,
    status          TEXT NOT NULL,  -- 'scraped', 'not_found', 'error'
    checked_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_addresses_cpso ON addresses(cpso_number);
CREATE INDEX IF NOT EXISTS idx_specialties_cpso ON specialties(cpso_number);
CREATE INDEX IF NOT EXISTS idx_hospital_privileges_cpso ON hospital_privileges(cpso_number);
CREATE INDEX IF NOT EXISTS idx_professional_corporations_cpso ON professional_corporations(cpso_number);
CREATE INDEX IF NOT EXISTS idx_registration_history_cpso ON registration_history(cpso_number);
CREATE INDEX IF NOT EXISTS idx_practice_conditions_cpso ON practice_conditions(cpso_number);
CREATE INDEX IF NOT EXISTS idx_public_notifications_cpso ON public_notifications(cpso_number);
CREATE INDEX IF NOT EXISTS idx_scrape_progress_status ON scrape_progress(status);

CREATE TABLE IF NOT EXISTS geocode_cache (
    postal_code TEXT PRIMARY KEY,
    lat         REAL,
    lng         REAL,
    status      TEXT NOT NULL,  -- 'ok', 'zero_results', 'error'
    geocoded_at TEXT DEFAULT (datetime('now'))
);
"""

FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS physicians_fts USING fts5(
    cpso_number UNINDEXED,
    full_name,
    gender,
    languages,
    former_name,
    registration_status,
    registration_class,
    medical_school,
    addresses,
    specialties,
    hospitals,
    corporations,
    registration_history,
    practice_conditions,
    public_notifications,
    tokenize='porter unicode61'
);
"""


def _migrate_schema(conn):
    """Add lat/lng columns to addresses if they don't exist yet."""
    columns = {row[1] for row in conn.execute("PRAGMA table_info(addresses)")}
    if "lat" not in columns:
        conn.execute("ALTER TABLE addresses ADD COLUMN lat REAL")
        conn.execute("ALTER TABLE addresses ADD COLUMN lng REAL")
        conn.commit()


def get_connection(db_path=None):
    """Return a new SQLite connection with the schema initialized."""
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.executescript(FTS_SCHEMA)
    _migrate_schema(conn)
    return conn


def get_scraped_numbers(conn):
    """Return the set of CPSO numbers already processed (any status)."""
    cursor = conn.execute("SELECT cpso_number FROM scrape_progress")
    return {row[0] for row in cursor}


def mark_not_found(conn, cpso_number):
    """Record that a CPSO number has no matching physician."""
    conn.execute(
        "INSERT OR REPLACE INTO scrape_progress (cpso_number, status) VALUES (?, 'not_found')",
        (cpso_number,),
    )


def mark_error(conn, cpso_number):
    """Record that scraping a CPSO number failed after retries."""
    conn.execute(
        "INSERT OR REPLACE INTO scrape_progress (cpso_number, status) VALUES (?, 'error')",
        (cpso_number,),
    )


def insert_physician(conn, data):
    """Insert a fully parsed physician record into all tables.

    `data` is a dict returned by parser.parse_physician_page().
    """
    cpso = data["cpso_number"]

    conn.execute(
        """INSERT OR REPLACE INTO physicians
           (cpso_number, full_name, first_name, last_name, gender, languages,
            former_name, member_status, registration_status, status_date,
            registration_class, medical_school, graduation_year, raw_html)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            cpso,
            data.get("full_name"),
            data.get("first_name"),
            data.get("last_name"),
            data.get("gender"),
            data.get("languages"),
            data.get("former_name"),
            data.get("member_status"),
            data.get("registration_status"),
            data.get("status_date"),
            data.get("registration_class"),
            data.get("medical_school"),
            data.get("graduation_year"),
            data.get("raw_html"),
        ),
    )

    # Clear old child records before re-inserting (idempotent on re-scrape)
    for table in (
        "addresses",
        "specialties",
        "hospital_privileges",
        "professional_corporations",
        "registration_history",
        "practice_conditions",
        "public_notifications",
    ):
        conn.execute(f"DELETE FROM {table} WHERE cpso_number = ?", (cpso,))

    for addr in data.get("addresses", []):
        conn.execute(
            """INSERT INTO addresses
               (cpso_number, address_type, name, street, city, province,
                postal_code, phone, phone_ext, fax, email)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cpso,
                addr.get("address_type"),
                addr.get("name"),
                addr.get("street"),
                addr.get("city"),
                addr.get("province"),
                addr.get("postal_code"),
                addr.get("phone"),
                addr.get("phone_ext"),
                addr.get("fax"),
                addr.get("email"),
            ),
        )

    for spec in data.get("specialties", []):
        conn.execute(
            """INSERT INTO specialties
               (cpso_number, specialty_name, certifying_body, effective_date)
               VALUES (?, ?, ?, ?)""",
            (
                cpso,
                spec.get("specialty_name"),
                spec.get("certifying_body"),
                spec.get("effective_date"),
            ),
        )

    for hosp in data.get("hospital_privileges", []):
        conn.execute(
            """INSERT INTO hospital_privileges
               (cpso_number, hospital_name, hospital_location)
               VALUES (?, ?, ?)""",
            (cpso, hosp.get("hospital_name"), hosp.get("hospital_location")),
        )

    for corp in data.get("professional_corporations", []):
        conn.execute(
            """INSERT INTO professional_corporations
               (cpso_number, corp_name, corp_status, end_date,
                business_address, shareholders)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                cpso,
                corp.get("corp_name"),
                corp.get("corp_status"),
                corp.get("end_date"),
                corp.get("business_address"),
                corp.get("shareholders"),
            ),
        )

    for hist in data.get("registration_history", []):
        conn.execute(
            """INSERT INTO registration_history
               (cpso_number, details, effective_date)
               VALUES (?, ?, ?)""",
            (cpso, hist.get("details"), hist.get("effective_date")),
        )

    for cond in data.get("practice_conditions", []):
        conn.execute(
            """INSERT INTO practice_conditions (cpso_number, condition_text)
               VALUES (?, ?)""",
            (cpso, cond),
        )

    for notif in data.get("public_notifications", []):
        conn.execute(
            """INSERT INTO public_notifications
               (cpso_number, notification_type, summary)
               VALUES (?, ?, ?)""",
            (cpso, notif.get("notification_type"), notif.get("summary")),
        )

    conn.execute(
        "INSERT OR REPLACE INTO scrape_progress (cpso_number, status) VALUES (?, 'scraped')",
        (cpso,),
    )


def rebuild_fts(conn):
    """Rebuild the FTS index from all physician data."""
    conn.execute("DELETE FROM physicians_fts")

    conn.executescript("""
        INSERT INTO physicians_fts (
            cpso_number, full_name, gender, languages, former_name,
            registration_status, registration_class, medical_school,
            addresses, specialties, hospitals, corporations,
            registration_history, practice_conditions, public_notifications
        )
        SELECT
            p.cpso_number,
            COALESCE(p.full_name, ''),
            COALESCE(p.gender, ''),
            COALESCE(p.languages, ''),
            COALESCE(p.former_name, ''),
            COALESCE(p.registration_status, ''),
            COALESCE(p.registration_class, ''),
            COALESCE(p.medical_school, ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(a.name, '') || ' ' ||
                    COALESCE(a.street, '') || ' ' ||
                    COALESCE(a.city, '') || ' ' ||
                    COALESCE(a.province, '') || ' ' ||
                    COALESCE(a.postal_code, '') || ' ' ||
                    COALESCE(a.phone, '') || ' ' ||
                    COALESCE(a.email, ''),
                    ' | '
                ) FROM addresses a WHERE a.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(s.specialty_name, '') || ' ' ||
                    COALESCE(s.certifying_body, ''),
                    ' | '
                ) FROM specialties s WHERE s.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(h.hospital_name, '') || ' ' ||
                    COALESCE(h.hospital_location, ''),
                    ' | '
                ) FROM hospital_privileges h WHERE h.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(c.corp_name, '') || ' ' ||
                    COALESCE(c.corp_status, '') || ' ' ||
                    COALESCE(c.shareholders, ''),
                    ' | '
                ) FROM professional_corporations c WHERE c.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(r.details, ''),
                    ' | '
                ) FROM registration_history r WHERE r.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(pc.condition_text, ''),
                    ' | '
                ) FROM practice_conditions pc WHERE pc.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(pn.notification_type, '') || ' ' ||
                    COALESCE(pn.summary, ''),
                    ' | '
                ) FROM public_notifications pn WHERE pn.cpso_number = p.cpso_number
            ), '')
        FROM physicians p;
    """)


def update_fts_for_physician(conn, cpso_number):
    """Update the FTS entry for a single physician (after insert/update)."""
    conn.execute(
        "DELETE FROM physicians_fts WHERE cpso_number = ?",
        (str(cpso_number),),
    )

    conn.execute(
        """
        INSERT INTO physicians_fts (
            cpso_number, full_name, gender, languages, former_name,
            registration_status, registration_class, medical_school,
            addresses, specialties, hospitals, corporations,
            registration_history, practice_conditions, public_notifications
        )
        SELECT
            p.cpso_number,
            COALESCE(p.full_name, ''),
            COALESCE(p.gender, ''),
            COALESCE(p.languages, ''),
            COALESCE(p.former_name, ''),
            COALESCE(p.registration_status, ''),
            COALESCE(p.registration_class, ''),
            COALESCE(p.medical_school, ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(a.name, '') || ' ' ||
                    COALESCE(a.street, '') || ' ' ||
                    COALESCE(a.city, '') || ' ' ||
                    COALESCE(a.province, '') || ' ' ||
                    COALESCE(a.postal_code, '') || ' ' ||
                    COALESCE(a.phone, '') || ' ' ||
                    COALESCE(a.email, ''),
                    ' | '
                ) FROM addresses a WHERE a.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(s.specialty_name, '') || ' ' ||
                    COALESCE(s.certifying_body, ''),
                    ' | '
                ) FROM specialties s WHERE s.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(h.hospital_name, '') || ' ' ||
                    COALESCE(h.hospital_location, ''),
                    ' | '
                ) FROM hospital_privileges h WHERE h.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(c.corp_name, '') || ' ' ||
                    COALESCE(c.corp_status, '') || ' ' ||
                    COALESCE(c.shareholders, ''),
                    ' | '
                ) FROM professional_corporations c WHERE c.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(r.details, ''),
                    ' | '
                ) FROM registration_history r WHERE r.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(pc.condition_text, ''),
                    ' | '
                ) FROM practice_conditions pc WHERE pc.cpso_number = p.cpso_number
            ), ''),
            COALESCE((
                SELECT GROUP_CONCAT(
                    COALESCE(pn.notification_type, '') || ' ' ||
                    COALESCE(pn.summary, ''),
                    ' | '
                ) FROM public_notifications pn WHERE pn.cpso_number = p.cpso_number
            ), '')
        FROM physicians p
        WHERE p.cpso_number = ?
        """,
        (cpso_number,),
    )
