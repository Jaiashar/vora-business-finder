#!/usr/bin/env python3
"""Test scraping angel investor sources for contact emails."""

import requests
from bs4 import BeautifulSoup
import re
import json
import time

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
JUNK_PREFIXES = {'info@', 'hello@', 'contact@', 'support@', 'admin@', 'press@',
                 'media@', 'jobs@', 'careers@', 'noreply@', 'no-reply@', 'webmaster@',
                 'team@', 'office@', 'general@', 'help@', 'feedback@', 'privacy@'}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

def is_junk_email(email):
    email_lower = email.lower()
    for prefix in JUNK_PREFIXES:
        if email_lower.startswith(prefix):
            return True
    junk_domains = {'example.com', 'sentry.io', 'w3.org', 'schema.org',
                    'wordpress.org', 'gravatar.com', 'wixpress.com',
                    'google.com', 'apple.com', 'facebook.com', 'twitter.com'}
    domain = email_lower.split('@')[1] if '@' in email_lower else ''
    if domain in junk_domains:
        return True
    if email_lower.endswith('.png') or email_lower.endswith('.jpg') or email_lower.endswith('.svg'):
        return True
    return False

def scrape_page(url, session):
    """Scrape a URL for emails and page content."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return set(), resp.status_code, len(resp.text), resp.url

        soup = BeautifulSoup(resp.text, 'html.parser')
        emails = set()

        # mailto links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if 'mailto:' in href:
                email = href.split('mailto:')[1].split('?')[0].strip()
                if EMAIL_RE.match(email) and not is_junk_email(email):
                    emails.add(email.lower())

        # regex on text
        for match in EMAIL_RE.findall(soup.get_text()):
            if not is_junk_email(match):
                emails.add(match.lower())

        # regex on raw HTML
        for match in EMAIL_RE.findall(resp.text):
            if not is_junk_email(match):
                emails.add(match.lower())

        return emails, resp.status_code, len(resp.text), resp.url

    except Exception as e:
        return set(), str(e), 0, url


# ─── Test Sources ────────────────────────────────────────────────────────────

ANGEL_SOURCES = {
    'Angel Directories & Lists': [
        {
            'name': 'AngelList / Wellfound Browse',
            'urls': [
                'https://wellfound.com/investors',
                'https://angel.co/investors',
            ],
            'notes': 'Major angel investor directory',
        },
        {
            'name': 'Crunchbase Angel Investors',
            'urls': [
                'https://www.crunchbase.com/discover/people/angel-investors',
                'https://www.crunchbase.com/lists/active-angel-investors',
            ],
            'notes': 'Crunchbase investor listings',
        },
        {
            'name': 'Signal by NFX',
            'urls': [
                'https://signal.nfx.com/investors',
            ],
            'notes': 'Investor search platform',
        },
    ],
    'Angel Networks & Groups': [
        {
            'name': 'Tech Coast Angels',
            'urls': [
                'https://www.techcoastangels.com/members/',
                'https://www.techcoastangels.com/about/',
            ],
            'notes': 'Large SoCal angel group',
        },
        {
            'name': 'Golden Seeds',
            'urls': [
                'https://goldenseeds.com/team/',
                'https://goldenseeds.com/about/',
            ],
            'notes': 'Angel network',
        },
        {
            'name': 'Band of Angels',
            'urls': [
                'https://www.bandangels.com/members',
                'https://www.bandangels.com/about',
            ],
            'notes': 'Silicon Valley angel group',
        },
        {
            'name': 'New York Angels',
            'urls': [
                'https://www.newyorkangels.com/members',
                'https://www.newyorkangels.com/about',
            ],
            'notes': 'NYC angel group',
        },
        {
            'name': 'Hustle Fund',
            'urls': [
                'https://www.hustlefund.vc/team',
                'https://www.hustlefund.vc/about',
            ],
            'notes': 'Pre-seed fund, angel-like',
        },
        {
            'name': 'Precursor Ventures',
            'urls': [
                'https://precursorvc.com/team/',
                'https://precursorvc.com/about/',
            ],
            'notes': 'Pre-seed fund',
        },
    ],
    'Accelerator Team Pages': [
        {
            'name': 'Y Combinator Partners',
            'urls': [
                'https://www.ycombinator.com/people',
            ],
            'notes': 'YC partners and team',
        },
        {
            'name': 'Techstars',
            'urls': [
                'https://www.techstars.com/the-line/team',
                'https://www.techstars.com/about',
            ],
            'notes': 'Major accelerator',
        },
        {
            'name': '500 Global',
            'urls': [
                'https://500.co/team',
                'https://500.co/about',
            ],
            'notes': 'Global accelerator',
        },
        {
            'name': 'Founders Inc',
            'urls': [
                'https://f.inc/team',
                'https://f.inc/about',
                'https://f.inc/',
            ],
            'notes': 'Already have Ruslan from here',
        },
    ],
    'Health/AI Focused Investors': [
        {
            'name': 'General Catalyst (health focus)',
            'urls': [
                'https://www.generalcatalyst.com/team',
                'https://www.generalcatalyst.com/about',
            ],
            'notes': 'Has health practice',
        },
        {
            'name': 'Andreessen Bio Fund',
            'urls': [
                'https://a16z.com/bio-health/',
            ],
            'notes': 'a16z bio/health team',
        },
        {
            'name': 'Rock Health',
            'urls': [
                'https://rockhealth.com/about/',
                'https://rockhealth.com/team/',
            ],
            'notes': 'Digital health focused VC',
        },
        {
            'name': 'Maveron',
            'urls': [
                'https://www.maveron.com/team/',
                'https://www.maveron.com/about/',
            ],
            'notes': 'Consumer focused, has health',
        },
        {
            'name': '7wireVentures',
            'urls': [
                'https://7wireventures.com/team/',
                'https://7wireventures.com/about/',
            ],
            'notes': 'Health tech focused',
        },
    ],
    'Email Pattern Discovery': [
        {
            'name': 'Hunter.io (homepage test)',
            'urls': [
                'https://hunter.io/',
            ],
            'notes': 'Testing if we can access Hunter without API key',
        },
    ],
}


def main():
    session = requests.Session()
    all_results = {}

    total_sources = sum(len(sources) for sources in ANGEL_SOURCES.values())

    print(f"\n{'=' * 70}")
    print(f"  ANGEL / INVESTOR SOURCE SCRAPING TEST")
    print(f"  Testing {total_sources} sources across {len(ANGEL_SOURCES)} categories")
    print(f"{'=' * 70}\n")

    for category, sources in ANGEL_SOURCES.items():
        print(f"\n  --- {category} ---\n")

        for source in sources:
            source_name = source['name']
            all_emails = set()

            for url in source['urls']:
                emails, status, size, final_url = scrape_page(url, session)
                all_emails.update(emails)

                redirected = f" -> {final_url}" if final_url != url else ""
                print(f"  {source_name}: {url}")
                print(f"    Status: {status}, Size: {size:,} bytes, Emails: {len(emails)}{redirected}")

                time.sleep(0.5)

            all_results[source_name] = {
                'category': category,
                'emails': list(all_emails),
                'email_count': len(all_emails),
                'notes': source['notes'],
                'urls_tested': source['urls'],
            }

            if all_emails:
                for e in list(all_emails)[:5]:
                    print(f"    -> {e}")
            print()

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY BY CATEGORY")
    print(f"{'=' * 70}")

    for category in ANGEL_SOURCES:
        print(f"\n  {category}:")
        for source_name, data in all_results.items():
            if data['category'] == category:
                count = data['email_count']
                status = f"{count} emails found" if count > 0 else "no emails (likely JS-rendered or requires auth)"
                print(f"    {source_name}: {status}")

    total_emails = sum(d['email_count'] for d in all_results.values())
    sources_with_emails = sum(1 for d in all_results.values() if d['email_count'] > 0)
    print(f"\n  TOTAL: {total_emails} emails from {sources_with_emails}/{total_sources} sources")

    # Save
    with open('test_angel_results.json', 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"  Detailed results saved to test_angel_results.json\n")


if __name__ == '__main__':
    main()
