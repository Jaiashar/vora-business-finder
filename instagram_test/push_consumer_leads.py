#!/usr/bin/env python3
"""
Push consumer leads (fitness influencers + wearable users) to Supabase.

Usage:
  python push_consumer_leads.py fitness     # push fitness influencer leads
  python push_consumer_leads.py wearable    # push wearable user leads
  python push_consumer_leads.py all         # push both
"""

import csv
import json
import os
import sys
import urllib.request
import urllib.error
import ssl

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass

def load_env_manual():
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    env = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                key, val = line.split('=', 1)
                env[key.strip()] = val.strip()
    return env

ENV = load_env_manual()
SUPABASE_URL = os.getenv('SUPABASE_URL') or ENV.get('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY') or ENV.get('SUPABASE_KEY')
TABLE = 'consumer_leads'

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


def supabase_upsert(rows, batch_size=50):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=email"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
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
                print(f"  Batch {i // batch_size + 1}: status {status} — {body[:200]}")
                errors += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  Batch {i // batch_size + 1}: HTTP {e.code} — {body[:200]}")
            errors += len(batch)
        except Exception as e:
            print(f"  Batch {i // batch_size + 1}: Error — {e}")
            errors += len(batch)

        if (i // batch_size + 1) % 10 == 0:
            print(f"  Progress: {i + len(batch)}/{total}")

    return inserted, errors


def load_csv(csv_path, default_category):
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row.get("email", "").lower().strip()
            if not email or '@' not in email:
                continue

            tags_raw = row.get("tags", "")
            tags = [t.strip() for t in tags_raw.split(",") if t.strip()] if tags_raw else []

            category = row.get("category", default_category).strip()
            valid_categories = {'fitness_influencer', 'wearable_user', 'fitness_wearable', 'wellness', 'athlete'}
            if category not in valid_categories:
                category = default_category

            followers = row.get("followers", "0")
            try:
                followers = int(followers)
            except (ValueError, TypeError):
                followers = 0

            rows.append({
                "email": email,
                "name": row.get("name", "").strip() or None,
                "ig_username": row.get("ig_username", "").strip() or None,
                "followers": followers,
                "platform": row.get("platform", "instagram").strip(),
                "category": category,
                "tags": tags or None,
                "bio": (row.get("bio", "") or "")[:500].strip() or None,
                "external_url": row.get("external_url", "").strip() or None,
                "email_source": row.get("email_source", "").strip() or None,
                "is_business": row.get("is_business", "").lower() in ('true', '1', 'yes'),
                "business_category": row.get("business_category", "").strip() or None,
            })

    return rows


def main():
    base = os.path.dirname(os.path.abspath(__file__))

    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    files_to_push = []

    if mode in ("fitness", "all"):
        fitness_csv = os.path.join(base, "fitness_influencer_leads.csv")
        if os.path.exists(fitness_csv):
            files_to_push.append(("Fitness Influencers", fitness_csv, "fitness_influencer"))
        else:
            print(f"  Skipping fitness — {fitness_csv} not found")

    if mode in ("wearable", "all"):
        wearable_csv = os.path.join(base, "wearable_user_leads.csv")
        if os.path.exists(wearable_csv):
            files_to_push.append(("Wearable Users", wearable_csv, "wearable_user"))
        else:
            print(f"  Skipping wearable — {wearable_csv} not found")

    if not files_to_push:
        print("No CSV files found to push. Run the scrapers first.")
        print("  python fitness_influencer_scraper.py")
        print("  python wearable_user_scraper.py")
        return

    total_inserted = 0
    total_errors = 0

    for label, csv_path, default_cat in files_to_push:
        print(f"\n{'=' * 50}")
        print(f"  {label}: {csv_path}")
        print(f"{'=' * 50}")

        rows = load_csv(csv_path, default_cat)
        print(f"  Loaded {len(rows)} rows")

        if not rows:
            print("  No valid rows, skipping.")
            continue

        # Stats
        categories = {}
        for r in rows:
            cat = r.get("category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1
        for cat, count in sorted(categories.items()):
            print(f"    {cat}: {count}")

        if default_cat == "wearable_user":
            device_counts = {}
            for r in rows:
                for tag in (r.get("tags") or []):
                    device_counts[tag] = device_counts.get(tag, 0) + 1
            if device_counts:
                print("  Devices:")
                for dev, cnt in sorted(device_counts.items(), key=lambda x: -x[1]):
                    print(f"    {dev}: {cnt}")

        print(f"\n  Pushing to Supabase ({TABLE})...")
        inserted, errors = supabase_upsert(rows)
        total_inserted += inserted
        total_errors += errors
        print(f"  Done: {inserted} inserted, {errors} errors/skipped")

    print(f"\n{'=' * 50}")
    print(f"  TOTAL: {total_inserted} inserted, {total_errors} errors/skipped")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
