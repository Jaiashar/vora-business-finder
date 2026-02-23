#!/usr/bin/env python3
"""
Stage 2: Email Pattern Generation

Takes discovered investor names + company domains from investor_leads
and generates candidate email patterns for each.

Usage:
    python generate_emails.py                       # All leads with no candidates yet
    python generate_emails.py --company "Accel"     # Single company
    python generate_emails.py --all                 # Regenerate for all leads
    python generate_emails.py --dry-run             # Preview without saving
"""

import argparse
import json
import os
import re
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


# ── Email Pattern Generation ────────────────────────────────────────────────

def normalize_name_part(name):
    """Lowercase, remove non-alpha characters."""
    if not name:
        return ''
    return re.sub(r'[^a-z]', '', name.lower())


def generate_patterns(first_name, last_name, domain):
    """
    Generate candidate email patterns from a name and domain.
    Returns a list of email strings.
    """
    if not domain:
        return []

    first = normalize_name_part(first_name)
    last = normalize_name_part(last_name)

    if not first:
        return []

    patterns = []

    # Always include first-name based patterns
    patterns.append(f"{first}@{domain}")

    if last:
        patterns.append(f"{first}.{last}@{domain}")
        patterns.append(f"{first}{last}@{domain}")
        patterns.append(f"{first[0]}{last}@{domain}")
        patterns.append(f"{first}{last[0]}@{domain}")
        patterns.append(f"{first}_{last}@{domain}")
        patterns.append(f"{first}-{last}@{domain}")
        patterns.append(f"{last}@{domain}")
        patterns.append(f"{first[0]}.{last}@{domain}")

    return patterns


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Stage 2: Email Pattern Generation')
    parser.add_argument('--company', type=str, help='Generate for a specific company only')
    parser.add_argument('--all', action='store_true',
                        help='Regenerate for ALL leads (not just empty)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview patterns without saving')
    args = parser.parse_args()

    supabase = get_supabase()

    # Fetch leads that need email generation
    query = supabase.table('investor_leads').select('*')

    if args.company:
        query = query.ilike('company', f'%{args.company}%')

    if not args.all:
        query = query.eq('candidate_emails', '[]')

    # Paginate
    all_leads = []
    page_size = 1000
    offset = 0
    while True:
        response = query.range(offset, offset + page_size - 1).execute()
        all_leads.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size

    print(f"\n{'=' * 65}")
    print(f"  EMAIL PATTERN GENERATION - Stage 2")
    print(f"  Leads to process: {len(all_leads)}")
    print(f"  {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"{'=' * 65}\n")

    if not all_leads:
        print("  No leads to process. Run discover_names.py first.\n")
        return

    generated = 0
    skipped = 0
    total_patterns = 0

    for lead in all_leads:
        first = lead.get('first_name', '')
        last = lead.get('last_name', '')
        domain = lead.get('domain', '')
        full_name = lead.get('full_name', '')
        company = lead.get('company', '?')

        if not domain:
            skipped += 1
            continue

        patterns = generate_patterns(first, last, domain)

        if not patterns:
            skipped += 1
            continue

        if args.dry_run:
            print(f"  {full_name} @ {company} ({domain})")
            for p in patterns:
                print(f"    {p}")
            print()
        else:
            try:
                supabase.table('investor_leads').update({
                    'candidate_emails': patterns,
                }).eq('id', lead['id']).execute()
                generated += 1
                total_patterns += len(patterns)
            except Exception as e:
                print(f"  ERROR updating {full_name}: {str(e)[:60]}")

    print(f"  {'=' * 55}")
    if args.dry_run:
        print(f"  DRY RUN: Would generate patterns for {len(all_leads) - skipped} leads")
    else:
        print(f"  Generated: {generated} leads ({total_patterns} total candidate emails)")
    print(f"  Skipped (no domain or name): {skipped}")
    print(f"  {'=' * 55}\n")


if __name__ == '__main__':
    main()
