#!/usr/bin/env python3
"""Push UCLA contacts CSV to Supabase college_contacts table with deduplication."""

import csv
import json
import os
import urllib.request
import urllib.error
import ssl
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
TABLE = 'college_contacts'

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


def supabase_upsert(rows, batch_size=50):
    """Upsert rows to Supabase using the REST API. On conflict (email), skip."""
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",  # Skip duplicates
    }

    total = len(rows)
    inserted = 0
    errors = 0

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        data = json.dumps(batch).encode('utf-8')

        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        try:
            resp = urllib.request.urlopen(req, context=SSL_CTX)
            status = resp.getcode()
            if status in (200, 201):
                inserted += len(batch)
            else:
                body = resp.read().decode()
                print(f"  Batch {i//batch_size + 1}: status {status} — {body[:200]}")
                errors += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  Batch {i//batch_size + 1}: HTTP {e.code} — {body[:200]}")
            errors += len(batch)
        except Exception as e:
            print(f"  Batch {i//batch_size + 1}: Error — {e}")
            errors += len(batch)

        if (i // batch_size + 1) % 5 == 0:
            print(f"  Progress: {i + len(batch)}/{total}")

    return inserted, errors


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base, "ucla_master.csv")

    # Load CSV
    rows = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "email": row["email"].lower().strip(),
                "name": row.get("name", "").strip() or None,
                "title": row.get("title", "").strip() or None,
                "department": row.get("department", "").strip() or None,
                "role": row.get("role", "student").strip(),
                "university": "UCLA",
                "source_url": row.get("source_url", "").strip() or None,
                "segment": "grad_student" if "@g.ucla.edu" in row["email"] else (
                    "greek_life" if "@gmail.com" in row["email"] else
                    "staff" if row.get("role") in ("staff", "faculty", "coach") else
                    "student"
                ),
            })

    print(f"Loaded {len(rows)} contacts from CSV")
    print(f"  Students: {sum(1 for r in rows if r['role'] == 'student')}")
    print(f"  Staff: {sum(1 for r in rows if r['role'] == 'staff')}")
    print(f"  Faculty: {sum(1 for r in rows if r['role'] == 'faculty')}")
    print(f"  Coaches: {sum(1 for r in rows if r['role'] == 'coach')}")
    print(f"  Student orgs: {sum(1 for r in rows if r['role'] == 'student_org')}")

    print(f"\nPushing to Supabase ({TABLE})...")
    inserted, errors = supabase_upsert(rows)
    print(f"\nDone! Inserted: {inserted}, Errors/Skipped: {errors}")


if __name__ == "__main__":
    main()
