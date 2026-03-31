# Deploying to Production

## Overview

The local database (`cpso_physicians.db`) contains raw HTML from every scraped
physician page, which makes it 8GB+. Production runs on a 512MB Lightsail
instance, so we **never push the full database**. Instead, we create a stripped
copy that removes `raw_html` and vacuum it down to ~120MB.

## Prerequisites

- SSH key: `~/Downloads/LightsailDefaultKey-ca-central-1.pem`
- Server: `ubuntu@3.99.186.120`
- App path on server: `/opt/ontario_physicians`

## Steps

### 1. Create a stripped copy of the database

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

This copies the DB to `/tmp`, nulls out `raw_html`, and vacuums to reclaim
space. The original database is untouched.

**Safe to run while the scraper is running** — it copies the file first,
so the scraper's writes don't conflict.

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
- The scraper supports resume — it tracks progress in `scrape_progress`, so
  restarting it will pick up where it left off.
