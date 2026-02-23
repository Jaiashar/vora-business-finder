#!/usr/bin/env python3
"""
Import external investor data into investor_leads table.

Sources:
  A) AngelList GitHub dataset (400 investors with fund websites/domains)
  B) Angelmatch.io health investors (Playwright scrape)
  C) GitHub curated email lists (direct emails)
  D) Crunchbase health angel search via Bing

Usage:
    python import_external_leads.py                     # Import all sources
    python import_external_leads.py --source angellist   # Single source
    python import_external_leads.py --dry-run            # Preview without saving
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in ../.env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def extract_domain(url):
    if not url:
        return None
    try:
        if not url.startswith('http'):
            url = 'http://' + url
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        skip = {'angel.co', 'linkedin.com', 'twitter.com', 'facebook.com',
                'github.com', 'medium.com', 'wordpress.com', 'blogspot.com',
                'youtube.com', 'instagram.com', 'crunchbase.com', 'wellfound.com',
                'google.com', 'about.me', 'substack.com', 'x.com', ''}
        if domain in skip or '.' not in domain:
            return None
        return domain
    except Exception:
        return None


def parse_name(full_name):
    if not full_name or not full_name.strip():
        return None, None
    parts = full_name.strip().split()
    if len(parts) < 2:
        return full_name.strip(), ''
    return parts[0], parts[-1]


def upsert_lead(supabase, lead, dry_run=False):
    if dry_run:
        return 'dry_run'
    try:
        supabase.table('investor_leads').upsert(
            lead, on_conflict='full_name,company',
        ).execute()
        return 'inserted'
    except Exception as e:
        err = str(e)
        if 'duplicate' in err.lower() or '23505' in err:
            return 'exists'
        return f'error: {err[:80]}'


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE A: AngelList GitHub Dataset
# ══════════════════════════════════════════════════════════════════════════════

def import_angellist(supabase, dry_run=False):
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE A: AngelList GitHub Dataset")
    print(f"  {'═' * 55}\n")

    url = 'https://raw.githubusercontent.com/VirenMohindra/AngelList/master/investor_names.csv'
    resp = requests.get(url, timeout=20)
    if resp.status_code != 200:
        print(f"    Failed to fetch: {resp.status_code}")
        return 0

    reader = csv.DictReader(io.StringIO(resp.text))
    inserted = 0
    skipped = 0

    for row in reader:
        name = row.get('Name', '').strip()
        website = row.get('Website', '').strip()
        angellist_url = row.get('AngelList URL', '').strip()

        if not name or len(name) < 3:
            skipped += 1
            continue

        domain = extract_domain(website)
        first, last = parse_name(name)

        # Try to extract company from the website domain
        company = domain.split('.')[0].title() if domain else 'Independent'

        lead = {
            'first_name': first,
            'last_name': last,
            'full_name': name,
            'company': company,
            'domain': domain,
            'linkedin_url': angellist_url,
            'source': 'angellist_github',
            'investor_type': 'angel',
        }

        result = upsert_lead(supabase, lead, dry_run)
        if result in ('inserted', 'dry_run'):
            inserted += 1
        else:
            skipped += 1

    print(f"    Imported: {inserted}, Skipped: {skipped}")
    return inserted


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE B: Angelmatch.io Health Investors (Playwright)
# ══════════════════════════════════════════════════════════════════════════════

def import_angelmatch(supabase, dry_run=False):
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE B: Angelmatch.io Health Investors (Playwright)")
    print(f"  {'═' * 55}\n")

    from playwright.sync_api import sync_playwright

    categories = [
        ('health-care', 'health_vc'),
        ('wellness', 'health_vc'),
        ('fitness', 'health_vc'),
        ('digital-health', 'health_vc'),
        ('wearables', 'health_vc'),
        ('biotechnology', 'health_vc'),
        ('medical', 'health_vc'),
    ]

    inserted = 0
    seen_names = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')

        for category, inv_type in categories:
            url = f'https://angelmatch.io/investors/by-market/{category}'
            print(f"    Scraping {category}...", end=' ')

            try:
                page.goto(url, wait_until='networkidle', timeout=25000)
                time.sleep(2)

                # Scroll to load all content
                for _ in range(5):
                    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    time.sleep(1)

                content = page.content()
                soup = BeautifulSoup(content, 'html.parser')

                # Extract investor info from the page
                count = 0

                # Look for investor name elements (h2, h3, h4, links)
                for tag in soup.find_all(['h2', 'h3', 'h4', 'a']):
                    text = tag.get_text(strip=True)
                    words = text.split()

                    if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words if len(w) > 1):
                        name = text.strip()
                        if name.lower() in seen_names:
                            continue
                        if any(w in name.lower() for w in ['view', 'see', 'more', 'all', 'health',
                                                            'care', 'angel', 'investor', 'match']):
                            continue

                        seen_names.add(name.lower())
                        first, last = parse_name(name)

                        lead = {
                            'first_name': first,
                            'last_name': last,
                            'full_name': name,
                            'company': 'Independent',
                            'domain': None,
                            'source': 'angelmatch',
                            'investor_type': inv_type,
                        }

                        result = upsert_lead(supabase, lead, dry_run)
                        if result in ('inserted', 'dry_run'):
                            inserted += 1
                            count += 1

                # Also extract from table rows
                for tr in soup.find_all('tr'):
                    cells = tr.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        name_text = cells[0].get_text(strip=True)
                        words = name_text.split()
                        if 2 <= len(words) <= 4 and name_text.lower() not in seen_names:
                            seen_names.add(name_text.lower())
                            first, last = parse_name(name_text)
                            company = cells[1].get_text(strip=True)[:100] if len(cells) > 1 else 'Independent'

                            lead = {
                                'first_name': first,
                                'last_name': last,
                                'full_name': name_text,
                                'company': company or 'Independent',
                                'domain': None,
                                'source': 'angelmatch',
                                'investor_type': inv_type,
                            }

                            result = upsert_lead(supabase, lead, dry_run)
                            if result in ('inserted', 'dry_run'):
                                inserted += 1
                                count += 1

                print(f"{count} investors")

            except Exception as e:
                print(f"Error: {str(e)[:50]}")
            time.sleep(1)

        browser.close()

    print(f"    Total imported from Angelmatch: {inserted}")
    return inserted


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE C: GitHub Curated Email Lists
# ══════════════════════════════════════════════════════════════════════════════

def import_github_emails(supabase, dry_run=False):
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE C: GitHub Curated Email Lists")
    print(f"  {'═' * 55}\n")

    inserted = 0

    # swyxio/devtools-angels
    url = 'https://raw.githubusercontent.com/swyxio/devtools-angels/main/README.md'
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        # Parse markdown to extract name-email pairs
        lines = resp.text.split('\n')
        current_name = None
        for line in lines:
            # Look for markdown links with names
            name_match = re.search(r'\[([A-Z][a-z]+ [A-Z][a-z]+)\]', line)
            if name_match:
                current_name = name_match.group(1)

            emails = EMAIL_RE.findall(line)
            for email in emails:
                email = email.lower()
                if '@' in email and current_name:
                    first, last = parse_name(current_name)
                    domain = email.split('@')[1]
                    lead = {
                        'first_name': first,
                        'last_name': last,
                        'full_name': current_name,
                        'company': domain.split('.')[0].title(),
                        'domain': domain,
                        'verified_email': email,
                        'verification_status': 'verified',
                        'source': 'github_curated',
                        'investor_type': 'angel',
                    }
                    result = upsert_lead(supabase, lead, dry_run)
                    if result in ('inserted', 'dry_run'):
                        inserted += 1
                        print(f"    {current_name}: {email}")

        print(f"    swyxio/devtools-angels: {inserted} imported")

    # ishandutta2007/contact-angel-investors
    url = 'https://raw.githubusercontent.com/ishandutta2007/contact-angel-investors/master/email-list.csv'
    resp = requests.get(url, timeout=10)
    gh2_count = 0
    if resp.status_code == 200:
        emails = list(set(EMAIL_RE.findall(resp.text)))
        for email in emails:
            email = email.lower()
            local_part = email.split('@')[0]
            domain = email.split('@')[1]

            # Try to parse a name from the local part
            name_parts = local_part.replace('.', ' ').replace('_', ' ').split()
            if len(name_parts) >= 2:
                first = name_parts[0].title()
                last = name_parts[-1].title()
                full_name = f"{first} {last}"
            else:
                first = local_part.title()
                last = ''
                full_name = first

            lead = {
                'first_name': first,
                'last_name': last,
                'full_name': full_name,
                'company': domain.split('.')[0].title(),
                'domain': domain,
                'verified_email': email,
                'verification_status': 'verified',
                'source': 'github_curated',
                'investor_type': 'angel',
            }
            result = upsert_lead(supabase, lead, dry_run)
            if result in ('inserted', 'dry_run'):
                gh2_count += 1
                inserted += 1

        print(f"    contact-angel-investors: {gh2_count} imported")

    print(f"    Total from GitHub: {inserted}")
    return inserted


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE D: Bing Crunchbase Health Angel Search
# ══════════════════════════════════════════════════════════════════════════════

def import_bing_crunchbase(supabase, dry_run=False):
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE D: Bing Crunchbase Health Angel Investors")
    print(f"  {'═' * 55}\n")

    from ddgs import DDGS

    queries = [
        ('site:crunchbase.com/person "angel investor" health', 'angel'),
        ('site:crunchbase.com/person "angel investor" wellness', 'angel'),
        ('site:crunchbase.com/person "angel investor" "digital health"', 'angel'),
        ('site:crunchbase.com/person "angel investor" fitness wearable', 'angel'),
        ('site:crunchbase.com/person "angel investor" longevity', 'angel'),
        ('site:crunchbase.com/person "angel investor" AI seed', 'angel'),
        ('site:crunchbase.com/person "angel investor" consumer health', 'angel'),
        ('site:crunchbase.com/person "angel investor" healthcare technology', 'angel'),
        ('site:crunchbase.com/person health investor seed pre-seed', 'angel'),
        ('site:crunchbase.com/person wellness investor angel', 'angel'),
    ]

    inserted = 0
    seen = set()
    ddgs = DDGS()

    for query, inv_type in queries:
        try:
            results = ddgs.text(query, max_results=25, backend='bing')
        except Exception as e:
            print(f"    Error: {str(e)[:50]}")
            time.sleep(2)
            continue

        count = 0
        for r in results:
            url = r.get('href', '')
            title = r.get('title', '')

            if 'crunchbase.com/person' not in url:
                continue

            name = title.split(' - ')[0].split(' | ')[0].strip()
            name = re.sub(r'\s*\(.*?\)', '', name).strip()

            if not name or len(name.split()) < 2 or name.lower() in seen:
                continue

            seen.add(name.lower())
            first, last = parse_name(name)

            lead = {
                'first_name': first,
                'last_name': last,
                'full_name': name,
                'company': 'Independent',
                'domain': None,
                'linkedin_url': url,
                'source': 'bing_crunchbase',
                'investor_type': inv_type,
            }

            result = upsert_lead(supabase, lead, dry_run)
            if result in ('inserted', 'dry_run'):
                inserted += 1
                count += 1

        print(f"    '{query[:50]}...': {count} new")
        time.sleep(1.5)

    print(f"    Total from Bing/Crunchbase: {inserted}")
    return inserted


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Import external investor data')
    parser.add_argument('--source', choices=['angellist', 'angelmatch', 'github', 'bing', 'all'],
                        default='all', help='Which source to import')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    args = parser.parse_args()

    supabase = None if args.dry_run else get_supabase()

    print(f"\n{'=' * 65}")
    print(f"  EXTERNAL INVESTOR DATA IMPORT")
    print(f"  Source: {args.source}")
    print(f"  {'DRY RUN' if args.dry_run else 'LIVE (saving to Supabase)'}")
    print(f"{'=' * 65}")

    total = 0

    if args.source in ('angellist', 'all'):
        total += import_angellist(supabase, args.dry_run)

    if args.source in ('angelmatch', 'all'):
        total += import_angelmatch(supabase, args.dry_run)

    if args.source in ('github', 'all'):
        total += import_github_emails(supabase, args.dry_run)

    if args.source in ('bing', 'all'):
        total += import_bing_crunchbase(supabase, args.dry_run)

    print(f"\n{'=' * 65}")
    print(f"  TOTAL IMPORTED: {total}")
    print(f"{'=' * 65}\n")


if __name__ == '__main__':
    main()
