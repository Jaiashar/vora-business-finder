#!/usr/bin/env python3
"""
Stage 1: Investor Name Discovery

Discovers investor names from two sources:
  A) Bing LinkedIn Dorking (via ddgs library)
  B) Playwright Team Page Scraping (JS-rendered firm websites)

Stores results in Supabase investor_leads table.

Usage:
    python discover_names.py --source bing              # Bing LinkedIn dorking only
    python discover_names.py --source playwright         # Team page scraping only
    python discover_names.py --source both               # Both sources
    python discover_names.py --firm "Khosla Ventures"    # Single firm only
    python discover_names.py --dry-run                   # Preview without saving
    python discover_names.py --extra-queries             # Also search for general angel investors
"""

import argparse
import json
import os
import re
import sys
import time

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

NAME_SUFFIXES = {
    'md', 'phd', 'mba', 'jd', 'ms', 'ma', 'dds', 'do', 'msc', 'bsc',
    'jr', 'sr', 'ii', 'iii', 'iv', 'cfa', 'cpa', 'esq', 'pe', 'rn',
    'pmp', 'mpp', 'mph', 'mha', 'facs', 'facp',
}

# ── Supabase ─────────────────────────────────────────────────────────────────

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in ../.env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_lead(supabase, lead, dry_run=False):
    """Upsert a lead into investor_leads. Returns 'inserted', 'exists', or 'error'."""
    if dry_run:
        return 'dry_run'
    try:
        supabase.table('investor_leads').upsert(
            lead,
            on_conflict='full_name,company',
        ).execute()
        return 'inserted'
    except Exception as e:
        err = str(e)
        if 'duplicate' in err.lower() or '23505' in err:
            return 'exists'
        return f'error: {err[:80]}'


# ── Name Parsing ─────────────────────────────────────────────────────────────

def clean_name(raw_name):
    """
    Parse a raw name string into (first_name, last_name, full_name).
    Handles suffixes, parenthetical names, commas.
    """
    if not raw_name or not raw_name.strip():
        return None, None, None

    name = raw_name.strip()

    # Remove parenthetical content like "(Hill)" or "(she/her)"
    name = re.sub(r'\([^)]*\)', '', name)

    # Remove common title prefixes
    name = re.sub(r'^(Dr\.?|Prof\.?|Mr\.?|Mrs\.?|Ms\.?)\s+', '', name, flags=re.I)

    # Split by comma and take just the name part (before any title)
    if ',' in name:
        parts = name.split(',')
        name = parts[0].strip()

    # Remove suffixes
    words = name.split()
    cleaned = []
    for w in words:
        w_lower = w.strip('.').lower()
        if w_lower not in NAME_SUFFIXES and not re.match(r'^[A-Z]\.$', w):
            cleaned.append(w)

    if not cleaned:
        return None, None, None

    full_name = ' '.join(cleaned).strip()

    # Handle single-word names
    if len(cleaned) == 1:
        return cleaned[0], '', full_name

    first_name = cleaned[0]
    last_name = cleaned[-1]

    return first_name, last_name, full_name


def parse_linkedin_name(url, title):
    """Extract a name from a LinkedIn search result."""
    name_from_title = ''
    if title:
        # LinkedIn titles are usually "First Last - Title | LinkedIn"
        name_from_title = title.split(' - ')[0].split(' | ')[0].strip()
        # Remove "..." from truncated titles
        name_from_title = name_from_title.rstrip('.')

    name_from_url = ''
    if 'linkedin.com/in/' in url:
        slug = url.split('linkedin.com/in/')[1].split('/')[0].split('?')[0]
        # Remove trailing hash IDs like "-485a811" at the end
        slug_clean = re.sub(r'-[a-f0-9]{6,}$', '', slug)
        name_from_url = slug_clean.replace('-', ' ').strip()

    # Prefer title-based name (more accurate), fall back to URL
    raw = name_from_title if name_from_title and len(name_from_title) > 2 else name_from_url
    return clean_name(raw)


# ── Source A: Bing LinkedIn Dorking ──────────────────────────────────────────

def discover_bing(firms, extra_queries=False):
    """Search Bing via ddgs for LinkedIn profiles of people at each firm."""
    from ddgs import DDGS

    all_leads = []
    ddgs = DDGS()

    for firm in firms:
        query = f'site:linkedin.com/in "{firm["linkedin_query"]}"'
        print(f"  Bing: {query}")

        try:
            results = ddgs.text(query, max_results=25, backend='bing')
        except Exception as e:
            print(f"    ERROR: {str(e)[:60]}")
            time.sleep(2)
            continue

        firm_count = 0
        for r in results:
            url = r.get('href', '')
            title = r.get('title', '')

            if 'linkedin.com/in/' not in url:
                continue

            first, last, full = parse_linkedin_name(url, title)
            if not full or len(full) < 3:
                continue

            lead = {
                'first_name': first,
                'last_name': last,
                'full_name': full,
                'company': firm['name'],
                'domain': firm['domain'],
                'linkedin_url': url.split('?')[0],
                'source': 'bing_linkedin',
                'investor_type': firm['type'],
            }
            all_leads.append(lead)
            firm_count += 1

        print(f"    {firm_count} profiles found")
        time.sleep(1.5)

    # Extra queries for general angel investors in health/AI
    if extra_queries:
        general_queries = [
            ('site:linkedin.com/in angel investor health tech', 'angel'),
            ('site:linkedin.com/in angel investor "digital health"', 'angel'),
            ('site:linkedin.com/in angel investor AI wellness', 'angel'),
            ('site:linkedin.com/in angel investor longevity wearable', 'angel'),
            ('site:linkedin.com/in angel investor "pre-seed" health', 'angel'),
        ]

        for query, inv_type in general_queries:
            print(f"  Bing: {query}")
            try:
                results = ddgs.text(query, max_results=20, backend='bing')
            except Exception as e:
                print(f"    ERROR: {str(e)[:60]}")
                time.sleep(2)
                continue

            count = 0
            for r in results:
                url = r.get('href', '')
                title = r.get('title', '')

                if 'linkedin.com/in/' not in url:
                    continue

                first, last, full = parse_linkedin_name(url, title)
                if not full or len(full) < 3:
                    continue

                # Try to extract company from LinkedIn title
                company = 'Independent'
                if ' at ' in title:
                    company = title.split(' at ')[-1].split('|')[0].strip()
                elif ' - ' in title:
                    parts = title.split(' - ')
                    if len(parts) >= 2:
                        company = parts[1].split('|')[0].strip()

                domain_guess = company.lower().replace(' ', '') + '.com' if company != 'Independent' else ''

                lead = {
                    'first_name': first,
                    'last_name': last,
                    'full_name': full,
                    'company': company,
                    'domain': domain_guess,
                    'linkedin_url': url.split('?')[0],
                    'source': 'bing_linkedin',
                    'investor_type': inv_type,
                    'title': title[:200] if title else None,
                }
                all_leads.append(lead)
                count += 1

            print(f"    {count} profiles found")
            time.sleep(1.5)

    return all_leads


# ── Source B: Playwright Team Pages ──────────────────────────────────────────

def discover_playwright(firms):
    """Scrape firm team pages using Playwright for JS rendering."""
    from playwright.sync_api import sync_playwright

    all_leads = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 800},
            locale='en-US',
        )
        page = context.new_page()

        for firm in firms:
            team_url = firm.get('team_url')
            if not team_url:
                continue

            print(f"  Playwright: {firm['name']} ({team_url})")

            try:
                page.goto(team_url, wait_until='networkidle', timeout=20000)
                time.sleep(1)

                # Scroll to trigger lazy loading
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)

                # Extract people names from common team page patterns
                names_found = set()
                selectors = [
                    '[class*="team"] [class*="name"]',
                    '[class*="member"] [class*="name"]',
                    '[class*="person"] [class*="name"]',
                    '[class*="people"] h3', '[class*="people"] h4',
                    '[class*="team"] h3', '[class*="team"] h4',
                    '[class*="staff"] h3', '[class*="staff"] h4',
                    '[class*="card"] h3', '[class*="card"] h4',
                    '[class*="grid"] h3', '[class*="grid"] h4',
                    'article h3', 'article h4',
                    '[class*="bio"] h3', '[class*="bio"] h4',
                    '[class*="partner"] h3', '[class*="partner"] h4',
                    '[data-testid*="name"]', '[data-testid*="person"]',
                ]

                for selector in selectors:
                    try:
                        elements = page.query_selector_all(selector)
                        for el in elements:
                            text = el.inner_text().strip()
                            if text and len(text) < 60 and '@' not in text:
                                # Basic heuristic: looks like a name (2-4 words, title-case-ish)
                                words = text.split()
                                if 1 <= len(words) <= 5:
                                    # Filter out obvious non-names
                                    lower = text.lower()
                                    skip_words = ['team', 'about', 'partner', 'member', 'people',
                                                  'company', 'program', 'resource', 'contact',
                                                  'overview', 'portfolio', 'news', 'blog',
                                                  'make something', 'view all', 'see more',
                                                  'read more', 'learn more', 'get', 'subscribe']
                                    if not any(sw in lower for sw in skip_words):
                                        names_found.add(text)
                    except Exception:
                        continue

                firm_count = 0
                for raw_name in names_found:
                    first, last, full = clean_name(raw_name)
                    if not full or len(full) < 3:
                        continue

                    lead = {
                        'first_name': first,
                        'last_name': last,
                        'full_name': full,
                        'company': firm['name'],
                        'domain': firm['domain'],
                        'source': 'playwright_team',
                        'investor_type': firm['type'],
                    }
                    all_leads.append(lead)
                    firm_count += 1

                print(f"    {firm_count} names extracted")

            except Exception as e:
                print(f"    ERROR: {str(e)[:70]}")

            time.sleep(0.5)

        browser.close()

    return all_leads


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Stage 1: Investor Name Discovery')
    parser.add_argument('--source', choices=['bing', 'playwright', 'both'], default='both',
                        help='Discovery source')
    parser.add_argument('--firm', type=str, help='Process a single firm by name')
    parser.add_argument('--firms-file', type=str, default='firms.json',
                        help='Path to firms config file')
    parser.add_argument('--extra-queries', action='store_true',
                        help='Include general angel investor queries (Bing only)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview leads without saving to Supabase')
    args = parser.parse_args()

    # Load firms
    firms_path = os.path.join(os.path.dirname(__file__), args.firms_file)
    with open(firms_path) as f:
        firms_data = json.load(f)

    firms = firms_data['firms']
    if args.firm:
        firms = [f for f in firms if f['name'].lower() == args.firm.lower()]
        if not firms:
            print(f"ERROR: Firm '{args.firm}' not found in {args.firms_file}")
            sys.exit(1)

    print(f"\n{'=' * 65}")
    print(f"  INVESTOR NAME DISCOVERY - Stage 1")
    print(f"  Source: {args.source} | Firms: {len(firms)}")
    print(f"  {'DRY RUN' if args.dry_run else 'LIVE (saving to Supabase)'}")
    print(f"{'=' * 65}\n")

    all_leads = []

    if args.source in ('bing', 'both'):
        print(f"  --- Bing LinkedIn Dorking ---\n")
        bing_leads = discover_bing(firms, extra_queries=args.extra_queries)
        all_leads.extend(bing_leads)
        print(f"\n  Bing total: {len(bing_leads)} leads\n")

    if args.source in ('playwright', 'both'):
        print(f"  --- Playwright Team Pages ---\n")
        pw_leads = discover_playwright(firms)
        all_leads.extend(pw_leads)
        print(f"\n  Playwright total: {len(pw_leads)} leads\n")

    # Dedupe by (full_name, company)
    seen = set()
    unique_leads = []
    for lead in all_leads:
        key = (lead['full_name'].lower(), (lead.get('company') or '').lower())
        if key not in seen:
            seen.add(key)
            unique_leads.append(lead)

    print(f"  {'=' * 55}")
    print(f"  TOTAL: {len(all_leads)} raw, {len(unique_leads)} unique leads")
    print(f"  {'=' * 55}\n")

    # Save to Supabase
    if not args.dry_run:
        supabase = get_supabase()

    inserted = 0
    existed = 0
    errors = 0

    for lead in unique_leads:
        if args.dry_run:
            print(f"  [DRY] {lead['full_name']} @ {lead.get('company', '?')} ({lead['source']})")
            continue

        result = upsert_lead(supabase, lead, dry_run=args.dry_run)
        if result == 'inserted':
            inserted += 1
        elif result == 'exists':
            existed += 1
        else:
            errors += 1
            if errors <= 5:
                print(f"  ERROR: {lead['full_name']} - {result}")

    if not args.dry_run:
        print(f"\n  Results:")
        print(f"    Inserted/updated: {inserted}")
        print(f"    Already existed:  {existed}")
        if errors:
            print(f"    Errors:           {errors}")
    else:
        print(f"\n  Dry run complete. {len(unique_leads)} leads would be saved.")

    # Summary by company
    from collections import Counter
    company_counts = Counter(l.get('company', '?') for l in unique_leads)
    print(f"\n  By company:")
    for company, count in company_counts.most_common():
        print(f"    {company}: {count}")

    print()


if __name__ == '__main__':
    main()
