# Deploying to Production

## Overview

The local database (`cpso_physicians.db`) contains raw HTML from every scraped
physician page, which makes it 8GB+. Production runs on a 512MB Lightsail
instance, so we **never push the full database**. Instead, we create a stripped
copy that removes `raw_html` and vacuum it down to ~190MB.

## Prerequisites

- SSH key: `~/Downloads/LightsailDefaultKey-ca-central-1.pem`
- Server: `ubuntu@3.99.186.120`
- App path on server: `/opt/ontario_physicians`

## Automated deployment

```bash
python3 deploy_db.py
```

This script handles everything in one command:

1. Creates a stripped copy (nulls `raw_html`, vacuums)
2. Validates the stripped DB (row counts, integrity check)
3. Rebuilds the R-tree spatial index
4. Uploads to the server alongside the existing DB
5. Backs up the current production DB
6. Swaps the new DB into place
7. Restarts the service and verifies it's running
8. **Rolls back automatically** if the service fails to start

The previous production DB is kept as `cpso_physicians_backup.db` on the server.

## Manual steps (reference)

If you need to deploy manually (e.g., the script fails partway through):

### 1. Create a stripped copy

```bash
python3 -c "
import sqlite3, shutil
shutil.copy('cpso_physicians.db', '/tmp/cpso_deploy.db')
conn = sqlite3.connect('/tmp/cpso_deploy.db', isolation_level=None)
conn.execute('UPDATE physicians SET raw_html = NULL')
conn.execute('VACUUM')
conn.close()
print('Done.')
"
```

### 2. Upload to the server

```bash
scp -i ~/Downloads/LightsailDefaultKey-ca-central-1.pem \
    /tmp/cpso_deploy.db \
    ubuntu@3.99.186.120:/opt/ontario_physicians/cpso_physicians.db
```

### 3. Restart the service

```bash
ssh -i ~/Downloads/LightsailDefaultKey-ca-central-1.pem \
    ubuntu@3.99.186.120 \
    "sudo systemctl restart ontario-physicians"
```

### 4. Verify

```bash
ssh -i ~/Downloads/LightsailDefaultKey-ca-central-1.pem \
    ubuntu@3.99.186.120 \
    "sudo systemctl status ontario-physicians --no-pager"
```

Should show `active (running)`.

## Important notes

- **Never upload the full database.** The raw HTML alone is 8GB+ and will
  exhaust memory on the 512MB instance.
- **Geocoder vs scraper conflict:** You can't run `geocode.py` while
  `scraper.py` is running (SQLite write lock). Send `SIGINT` to the scraper
  first, run the geocoder, then resume the scraper.
- The scraper supports resume — it tracks progress in the database, so
  restarting it will pick up where it left off.
