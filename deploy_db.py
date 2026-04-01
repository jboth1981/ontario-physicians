"""Safe database deployment to production.

Creates a stripped copy of the local database (no raw_html), validates it,
uploads to the server alongside the existing DB, then atomically swaps them.
Keeps the previous production DB as a backup for easy rollback.
"""

import shutil
import sqlite3
import subprocess
import sys

SERVER = "ubuntu@3.99.186.120"
SSH_KEY = "~/Downloads/LightsailDefaultKey-ca-central-1.pem"
REMOTE_DIR = "/opt/ontario_physicians"
REMOTE_DB = f"{REMOTE_DIR}/cpso_physicians.db"
SERVICE = "ontario-physicians"

LOCAL_DB = "cpso_physicians.db"
STAGING_DB = "/tmp/cpso_deploy.db"


def ssh(cmd):
    """Run a command on the server via SSH."""
    result = subprocess.run(
        ["ssh", "-i", SSH_KEY, SERVER, cmd],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        print(f"SSH command failed: {cmd}")
        print(result.stderr)
        sys.exit(1)
    return result.stdout.strip()


def scp(local, remote):
    """Copy a file to the server."""
    result = subprocess.run(
        ["scp", "-i", SSH_KEY, local, f"{SERVER}:{remote}"],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"SCP failed: {local} -> {remote}")
        print(result.stderr)
        sys.exit(1)


def step(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def main():
    # Step 1: Create stripped copy
    step("1. Creating stripped database copy")
    print(f"   Copying {LOCAL_DB} -> {STAGING_DB}")
    shutil.copy2(LOCAL_DB, STAGING_DB)

    conn = sqlite3.connect(STAGING_DB, isolation_level=None)
    conn.execute("UPDATE physicians SET raw_html = NULL")
    conn.execute("VACUUM")
    conn.close()

    size_mb = shutil.os.path.getsize(STAGING_DB) / (1024 * 1024)
    print(f"   Stripped DB size: {size_mb:.1f} MB")

    if size_mb > 400:
        print(f"   ERROR: DB is {size_mb:.0f} MB — too large for 512MB server.")
        print("   raw_html may not have been stripped. Aborting.")
        sys.exit(1)

    # Step 2: Validate the stripped DB
    step("2. Validating stripped database")
    conn = sqlite3.connect(STAGING_DB)
    conn.row_factory = sqlite3.Row

    checks = {
        "physicians": conn.execute("SELECT COUNT(*) FROM physicians").fetchone()[0],
        "addresses": conn.execute("SELECT COUNT(*) FROM addresses").fetchone()[0],
        "geocoded": conn.execute("SELECT COUNT(*) FROM addresses WHERE lat IS NOT NULL").fetchone()[0],
        "specialties": conn.execute("SELECT COUNT(*) FROM specialties").fetchone()[0],
    }

    for label, count in checks.items():
        print(f"   {label}: {count:,}")

    if checks["physicians"] < 30000:
        print(f"   ERROR: Only {checks['physicians']} physicians — expected 30k+. Aborting.")
        conn.close()
        sys.exit(1)

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        print(f"   ERROR: Integrity check failed: {integrity}")
        conn.close()
        sys.exit(1)
    print("   Integrity check: ok")

    # Step 3: Rebuild R-tree in the stripped copy
    step("3. Rebuilding R-tree spatial index")
    conn.execute("DELETE FROM addresses_rtree")
    conn.execute(
        "INSERT INTO addresses_rtree (id, min_lat, max_lat, min_lng, max_lng) "
        "SELECT id, lat, lat, lng, lng "
        "FROM addresses WHERE lat IS NOT NULL AND lng IS NOT NULL"
    )
    conn.commit()
    rtree_count = conn.execute("SELECT COUNT(*) FROM addresses_rtree").fetchone()[0]
    print(f"   R-tree entries: {rtree_count:,}")
    conn.close()

    # Step 4: Upload to staging path on server
    step("4. Uploading to server (staging path)")
    remote_staging = f"{REMOTE_DIR}/cpso_physicians_new.db"
    scp(STAGING_DB, remote_staging)
    print("   Upload complete")

    # Step 5: Backup current production DB and swap
    step("5. Backing up current DB and swapping")
    ssh(f"cp {REMOTE_DB} {REMOTE_DIR}/cpso_physicians_backup.db")
    print("   Backup created: cpso_physicians_backup.db")

    ssh(f"mv {remote_staging} {REMOTE_DB}")
    print("   Swapped new DB into place")

    # Step 6: Restart and verify
    step("6. Restarting service")
    ssh(f"sudo systemctl restart {SERVICE}")

    status = ssh(f"systemctl is-active {SERVICE}")
    if status == "active":
        print("   Service is running")
    else:
        print(f"   WARNING: Service status is '{status}'")
        print("   Rolling back...")
        ssh(f"mv {REMOTE_DIR}/cpso_physicians_backup.db {REMOTE_DB}")
        ssh(f"sudo systemctl restart {SERVICE}")
        print("   Rolled back to previous database")
        sys.exit(1)

    step("Done!")
    print("   Database deployed successfully.")
    print(f"   Rollback available: {REMOTE_DIR}/cpso_physicians_backup.db")
    print()


if __name__ == "__main__":
    main()
