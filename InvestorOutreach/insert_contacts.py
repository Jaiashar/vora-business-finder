#!/usr/bin/env python3
"""
Insert investor contacts from contacts.json into Supabase investor_contacts table.

Usage:
    python insert_contacts.py              # Insert all contacts
    python insert_contacts.py --dry-run    # Show what would be inserted without inserting
"""

import argparse
import json
import os
import sys
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in ../.env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def load_contacts():
    contacts_path = os.path.join(os.path.dirname(__file__), 'contacts.json')
    with open(contacts_path, 'r') as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description='Insert investor contacts into Supabase')
    parser.add_argument('--dry-run', action='store_true', help='Show contacts without inserting')
    args = parser.parse_args()

    contacts = load_contacts()

    print(f"\n{'=' * 60}")
    print(f"  INVESTOR CONTACTS — {'DRY RUN' if args.dry_run else 'INSERT'}")
    print(f"  {len(contacts)} contacts from contacts.json")
    print(f"{'=' * 60}\n")

    for i, c in enumerate(contacts):
        print(f"  [{i+1}] {c['first_name']} {c['last_name']}")
        print(f"      Email: {c['email']}")
        print(f"      Company: {c.get('company') or '—'}")
        print(f"      Type: {c.get('investor_type') or '—'}")
        print(f"      Referred by: {c.get('referred_by') or '—'}")
        print()

    if args.dry_run:
        print("  DRY RUN — no changes made.\n")
        return

    supabase = get_supabase()

    inserted = 0
    skipped = 0
    failed = 0

    for c in contacts:
        row = {k: v for k, v in c.items() if v is not None}

        try:
            supabase.table('investor_contacts').upsert(
                row, on_conflict='email'
            ).execute()
            inserted += 1
            print(f"  ✓ {c['first_name']} {c['last_name']} ({c['email']})")
        except Exception as e:
            error_msg = str(e)
            if 'duplicate' in error_msg.lower() or '23505' in error_msg:
                skipped += 1
                print(f"  ⊘ Skipped (already exists): {c['email']}")
            else:
                failed += 1
                print(f"  ✗ Failed: {c['email']} — {error_msg}")

    print(f"\n{'=' * 60}")
    print(f"  DONE: {inserted} inserted, {skipped} skipped, {failed} failed")
    print(f"{'=' * 60}\n")


if __name__ == '__main__':
    main()
