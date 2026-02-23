#!/usr/bin/env python3
"""
Investor Email Discovery Pipeline - Orchestrator

Runs the 3-stage pipeline and manages data flow.
All data lives in the investor_leads table.

Usage:
    python pipeline.py discover                  # Stage 1: Find names
    python pipeline.py generate                  # Stage 2: Generate email patterns
    python pipeline.py verify                    # Stage 3: SMTP verification
    python pipeline.py import-external           # Import from AngelList, Angelmatch, GitHub, Bing
    python pipeline.py run-all                   # Run stages 1-3 in sequence
    python pipeline.py stats                     # Show pipeline progress
    python pipeline.py export                    # Export verified leads to CSV

Options work with all stage commands:
    python pipeline.py discover --firm "Accel"   # Single firm
    python pipeline.py run-all --dry-run         # Preview entire pipeline
    python pipeline.py verify --batch-size 20    # Limit verification batch
    python pipeline.py discover --extra-queries  # Include general angel searches
"""

import argparse
import csv
import json
import os
import subprocess
import sys

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_PYTHON = os.path.join(SCRIPT_DIR, '.venv', 'bin', 'python3')

if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = sys.executable


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in ../.env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def run_script(script_name, extra_args=None):
    """Run a pipeline stage script."""
    script_path = os.path.join(SCRIPT_DIR, script_name)
    cmd = [VENV_PYTHON, script_path]
    if extra_args:
        cmd.extend(extra_args)

    print(f"\n  Running: {' '.join(cmd)}\n", flush=True)
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    result = subprocess.run(cmd, cwd=SCRIPT_DIR, env=env)
    return result.returncode


def fetch_all(supabase, table, filters=None):
    """Fetch all rows from a table with optional filters."""
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        query = supabase.table(table).select('*')
        if filters:
            for col, val in filters.items():
                query = query.eq(col, val)
        response = query.range(offset, offset + page_size - 1).execute()
        all_rows.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size
    return all_rows


# ── Stats ────────────────────────────────────────────────────────────────────

def show_stats():
    """Show pipeline progress stats."""
    supabase = get_supabase()
    leads = fetch_all(supabase, 'investor_leads')

    total = len(leads)
    if total == 0:
        print("\n  Pipeline is empty. Run: python pipeline.py discover\n")
        return

    # Count by status
    status_counts = {}
    for lead in leads:
        status = lead.get('verification_status', 'pending')
        status_counts[status] = status_counts.get(status, 0) + 1

    # Count with candidate emails
    has_candidates = sum(1 for l in leads if l.get('candidate_emails') and l['candidate_emails'] != [])
    has_verified = sum(1 for l in leads if l.get('verified_email'))
    emailed = sum(1 for l in leads if l.get('outreach_status') == 'emailed')

    # Count by company
    company_counts = {}
    for lead in leads:
        company = lead.get('company', 'Unknown')
        company_counts[company] = company_counts.get(company, 0) + 1

    # Count by source
    source_counts = {}
    for lead in leads:
        source = lead.get('source', 'unknown')
        source_counts[source] = source_counts.get(source, 0) + 1

    print(f"\n{'=' * 65}")
    print(f"  INVESTOR PIPELINE STATUS")
    print(f"{'=' * 65}\n")

    print(f"  Total leads discovered:     {total}")
    print(f"  With email candidates:      {has_candidates}")
    print(f"  With verified/best email:   {has_verified}")
    print(f"  Emailed:                    {emailed}")

    # Count by outreach status
    outreach_counts = {}
    for lead in leads:
        status = lead.get('outreach_status', 'pending')
        outreach_counts[status] = outreach_counts.get(status, 0) + 1

    print(f"\n  Verification Status:")
    for status in ['pending', 'verified', 'failed', 'catch_all', 'unverifiable']:
        count = status_counts.get(status, 0)
        bar = '#' * min(count, 40)
        print(f"    {status:15s} {count:4d}  {bar}")

    print(f"\n  Outreach Status:")
    for status in ['pending', 'emailed', 'responded', 'meeting', 'passed']:
        count = outreach_counts.get(status, 0)
        if count > 0:
            bar = '#' * min(count, 40)
            print(f"    {status:15s} {count:4d}  {bar}")

    print(f"\n  By Source:")
    for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"    {source:20s} {count:4d}")

    print(f"\n  By Company (top 15):")
    for company, count in sorted(company_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"    {company:30s} {count:4d}")

    print()


def import_external():
    """Run the external data import script."""
    run_script('import_external_leads.py')


# ── Export ───────────────────────────────────────────────────────────────────

def export_leads(output_file='investor_leads_export.csv'):
    """Export all leads with emails to CSV."""
    supabase = get_supabase()
    leads = fetch_all(supabase, 'investor_leads')

    # Filter to leads with some email
    with_email = [l for l in leads if l.get('verified_email')]

    output_path = os.path.join(SCRIPT_DIR, output_file)
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'email', 'first_name', 'last_name', 'full_name',
            'company', 'domain', 'investor_type', 'source',
            'verification_status', 'linkedin_url',
        ])
        for l in with_email:
            writer.writerow([
                l.get('verified_email', ''),
                l.get('first_name', ''),
                l.get('last_name', ''),
                l.get('full_name', ''),
                l.get('company', ''),
                l.get('domain', ''),
                l.get('investor_type', ''),
                l.get('source', ''),
                l.get('verification_status', ''),
                l.get('linkedin_url', ''),
            ])

    print(f"\n  Exported {len(with_email)} leads to {output_path}\n")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Investor Email Discovery Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  discover          Stage 1: Find investor names via Bing + Playwright
  generate          Stage 2: Generate email pattern candidates
  verify            Stage 3: Verify emails via SMTP
  import-external   Import from AngelList, Angelmatch, GitHub, Bing/Crunchbase
  run-all           Run stages 1-3 in sequence
  stats             Show pipeline progress
  export            Export verified leads to CSV
        """,
    )
    parser.add_argument('command', choices=[
        'discover', 'generate', 'verify', 'import-external', 'run-all', 'stats', 'export',
    ])
    parser.add_argument('--firm', type=str, help='Process a single firm')
    parser.add_argument('--company', type=str, help='Filter by company (for generate/verify)')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    parser.add_argument('--batch-size', type=int, default=100, help='Verify batch size')
    parser.add_argument('--source', choices=['bing', 'playwright', 'both'], default='both',
                        help='Discovery source for discover command')
    parser.add_argument('--extra-queries', action='store_true',
                        help='Include general angel investor queries')
    parser.add_argument('--skip-catch-all', action='store_true',
                        help='Skip catch-all detection during verification')
    args = parser.parse_args()

    if args.command == 'stats':
        show_stats()
        return

    if args.command == 'export':
        export_leads()
        return

    if args.command == 'discover':
        extra = ['--source', args.source]
        if args.firm:
            extra.extend(['--firm', args.firm])
        if args.dry_run:
            extra.append('--dry-run')
        if args.extra_queries:
            extra.append('--extra-queries')
        run_script('discover_names.py', extra)

    elif args.command == 'generate':
        extra = []
        if args.company:
            extra.extend(['--company', args.company])
        if args.dry_run:
            extra.append('--dry-run')
        run_script('generate_emails.py', extra)

    elif args.command == 'verify':
        extra = ['--batch-size', str(args.batch_size)]
        if args.company:
            extra.extend(['--company', args.company])
        if args.dry_run:
            extra.append('--dry-run')
        if args.skip_catch_all:
            extra.append('--skip-catch-all')
        run_script('verify_emails.py', extra)

    elif args.command == 'import-external':
        import_external()

    elif args.command == 'run-all':
        print(f"\n{'=' * 65}")
        print(f"  RUNNING FULL PIPELINE")
        print(f"{'=' * 65}")

        # Stage 1: Discover
        print(f"\n  >>>  STAGE 1: Name Discovery")
        extra = ['--source', args.source]
        if args.firm:
            extra.extend(['--firm', args.firm])
        if args.dry_run:
            extra.append('--dry-run')
        if args.extra_queries:
            extra.append('--extra-queries')
        rc = run_script('discover_names.py', extra)
        if rc != 0:
            print(f"\n  Stage 1 failed (exit {rc}). Stopping.\n")
            return

        # Stage 2: Generate
        print(f"\n  >>>  STAGE 2: Email Pattern Generation")
        extra = []
        if args.company or args.firm:
            extra.extend(['--company', args.company or args.firm])
        if args.dry_run:
            extra.append('--dry-run')
        rc = run_script('generate_emails.py', extra)
        if rc != 0:
            print(f"\n  Stage 2 failed (exit {rc}). Stopping.\n")
            return

        # Stage 3: Verify
        print(f"\n  >>>  STAGE 3: SMTP Verification")
        extra = ['--batch-size', str(args.batch_size)]
        if args.company or args.firm:
            extra.extend(['--company', args.company or args.firm])
        if args.dry_run:
            extra.append('--dry-run')
        if args.skip_catch_all:
            extra.append('--skip-catch-all')
        rc = run_script('verify_emails.py', extra)
        if rc != 0:
            print(f"\n  Stage 3 failed (exit {rc}).\n")

        # Show final stats
        show_stats()


if __name__ == '__main__':
    main()
