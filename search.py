"""CLI keyword search across the CPSO physician database."""

import argparse
import sys

import db


def search(query, limit=20):
    """Search the FTS index and return matching physicians with details."""
    conn = db.get_connection()

    # FTS5 query — wrap each word in quotes for exact matching,
    # or pass through as-is to allow FTS5 syntax (AND, OR, NEAR, etc.)
    cursor = conn.execute(
        """
        SELECT
            f.cpso_number,
            p.full_name,
            p.registration_status,
            p.registration_class,
            p.gender,
            p.languages,
            p.medical_school,
            f.addresses,
            f.specialties,
            f.hospitals,
            rank
        FROM physicians_fts f
        JOIN physicians p ON p.cpso_number = CAST(f.cpso_number AS INTEGER)
        WHERE physicians_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (query, limit),
    )

    results = cursor.fetchall()
    conn.close()
    return results


def format_result(row):
    """Format a single search result for display."""
    lines = []
    lines.append(f"  CPSO# {row['cpso_number']}  |  {row['full_name'] or '?'}")
    lines.append(
        f"  Status: {row['registration_status'] or '?'}  |  "
        f"Class: {row['registration_class'] or '?'}"
    )

    if row["gender"] or row["languages"]:
        parts = []
        if row["gender"]:
            parts.append(f"Gender: {row['gender']}")
        if row["languages"]:
            parts.append(f"Languages: {row['languages']}")
        lines.append(f"  {' | '.join(parts)}")

    if row["medical_school"]:
        lines.append(f"  School: {row['medical_school']}")

    if row["specialties"]:
        lines.append(f"  Specialties: {row['specialties']}")

    if row["addresses"]:
        # Show a condensed version
        addrs = row["addresses"]
        if len(addrs) > 120:
            addrs = addrs[:120] + "..."
        lines.append(f"  Address: {addrs}")

    if row["hospitals"]:
        hosps = row["hospitals"]
        if len(hosps) > 120:
            hosps = hosps[:120] + "..."
        lines.append(f"  Hospitals: {hosps}")

    return "\n".join(lines)


def main():
    arg_parser = argparse.ArgumentParser(
        description="Search the CPSO physician database by keyword."
    )
    arg_parser.add_argument(
        "query",
        nargs="+",
        help='Search terms (e.g., "family medicine toronto")',
    )
    arg_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum results to show (default: 20)",
    )
    arg_parser.add_argument(
        "--rebuild-fts",
        action="store_true",
        help="Rebuild the FTS index before searching",
    )

    args = arg_parser.parse_args()
    query = " ".join(args.query)

    if args.rebuild_fts:
        print("Rebuilding FTS index...", file=sys.stderr)
        conn = db.get_connection()
        db.rebuild_fts(conn)
        conn.commit()
        conn.close()
        print("Done.", file=sys.stderr)

    results = search(query, limit=args.limit)

    if not results:
        print(f'No results for "{query}".')
        sys.exit(0)

    print(f'Found {len(results)} result(s) for "{query}":\n')
    for i, row in enumerate(results, 1):
        print(f"[{i}]")
        print(format_result(row))
        print()


if __name__ == "__main__":
    main()
