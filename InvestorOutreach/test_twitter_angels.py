#!/usr/bin/env python3
"""Test scraping Twitter/X bios for angel investor emails."""

import requests
import re
import json
import time

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Known angel investors with Twitter/X handles
ANGEL_HANDLES = [
    'jasonfried',       # Jason Fried - Basecamp, angel
    'naval',            # Naval Ravikant - AngelList founder
    'garaborosz',       # Gara Borosz
    'hunterwalk',       # Hunter Walk - Homebrew
    'msuster',          # Mark Suster - Upfront Ventures
    'sacca',            # Chris Sacca - Lowercase Capital
    'jason',            # Jason Calacanis - angel investor
    'balaboreddy',      # angel investor
    'alexisohanian',    # Alexis Ohanian - Reddit founder, angel
    'rrhoover',         # Ryan Hoover - Product Hunt, angel
    'aaboreddy',        # angel investor
    'elaboreddy',       # angel investor
    'prestonpes',       # Preston Pesek - angel
    'davidtisch',       # David Tisch - BoxGroup
    'baboreddy',        # angel investor
    'joshuakushner',    # Joshua Kushner - Thrive Capital
    'semilshah',        # Semil Shah - Haystack
    'baboreddy',        # angel investor
    'susaboreddy',      # angel investor
    'nivi',             # Babak Nivi - AngelList co-founder
    'sama',             # Sam Altman
    'paulg',            # Paul Graham
    'elaboreddy',       # angel investor
]

# Nitter instances (public Twitter mirrors that don't require auth)
NITTER_INSTANCES = [
    'https://nitter.net',
    'https://nitter.privacydev.net',
    'https://nitter.poast.org',
    'https://xcancel.com',
]


def try_nitter(handle, session):
    """Try to fetch a Twitter profile via Nitter instances."""
    for instance in NITTER_INSTANCES:
        url = f"{instance}/{handle}"
        try:
            resp = session.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
            if resp.status_code == 200 and len(resp.text) > 5000:
                return resp.text, url, resp.status_code
        except Exception:
            continue
    return None, None, None


def try_twitter_direct(handle, session):
    """Try to fetch Twitter profile directly (usually blocked but worth trying)."""
    url = f"https://x.com/{handle}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        return resp.text, url, resp.status_code
    except Exception as e:
        return None, url, str(e)


def extract_from_page(html):
    """Extract emails and links from a page."""
    emails = set()
    links = set()

    if not html:
        return emails, links

    # Direct email regex
    for match in EMAIL_RE.findall(html):
        email = match.lower()
        if not any(email.endswith(d) for d in ['.png', '.jpg', '.svg', '.gif']):
            if not any(email.startswith(p) for p in ['noreply@', 'no-reply@', 'support@']):
                emails.add(email)

    # Extract links that might lead to personal sites
    link_re = re.compile(r'href=["\']?(https?://[^"\'>\s]+)', re.I)
    for link in link_re.findall(html):
        if any(d in link for d in ['linkedin.com', 'twitter.com', 'x.com', 'facebook.com',
                                    'instagram.com', 'nitter', 'github.com']):
            continue
        if any(d in link for d in ['.blog', 'substack.com', 'medium.com', 'about.me',
                                    'linktree', 'beacons.ai']):
            links.add(link)
        # Personal domains (short URLs that aren't social media)
        parts = link.split('/')
        if len(parts) >= 3:
            domain = parts[2]
            if domain.count('.') == 1 and len(domain) < 30:
                links.add(link)

    return emails, links


def main():
    session = requests.Session()

    # Dedupe handles
    unique_handles = list(dict.fromkeys(ANGEL_HANDLES))

    print(f"\n{'=' * 70}")
    print(f"  TWITTER/X ANGEL INVESTOR BIO SCRAPING TEST")
    print(f"  Testing {len(unique_handles)} handles via Nitter mirrors + direct X")
    print(f"{'=' * 70}\n")

    # First test which Nitter instances are alive
    print("  Testing Nitter instances...")
    alive_instances = []
    for instance in NITTER_INSTANCES:
        try:
            resp = session.get(f"{instance}/elonmusk", headers=HEADERS, timeout=8)
            if resp.status_code == 200 and len(resp.text) > 5000:
                alive_instances.append(instance)
                print(f"    {instance}: ALIVE ({len(resp.text):,} bytes)")
            else:
                print(f"    {instance}: DOWN (status {resp.status_code}, {len(resp.text)} bytes)")
        except Exception as e:
            print(f"    {instance}: DOWN ({str(e)[:50]})")
        time.sleep(0.5)

    print(f"\n  {len(alive_instances)} Nitter instances alive\n")

    # Also test direct X.com
    print("  Testing direct X.com access...")
    try:
        resp = session.get("https://x.com/naval", headers=HEADERS, timeout=10)
        print(f"    x.com: status {resp.status_code}, {len(resp.text):,} bytes")
        x_emails, _ = extract_from_page(resp.text)
        if x_emails:
            print(f"    Found emails in X.com page: {x_emails}")
        else:
            print(f"    No emails in direct X.com HTML (likely JS-rendered)")
    except Exception as e:
        print(f"    x.com: {str(e)[:60]}")

    print()

    # Now scrape each angel handle
    results = {}
    total_emails = 0

    for handle in unique_handles:
        print(f"  @{handle}")

        # Try Nitter first
        html, source_url, status = try_nitter(handle, session)
        source = "nitter"

        if not html:
            # Fall back to direct X
            html, source_url, status = try_twitter_direct(handle, session)
            source = "x.com"

        if html:
            emails, personal_links = extract_from_page(html)
            results[handle] = {
                'source': source,
                'source_url': source_url,
                'status': status,
                'emails': list(emails),
                'personal_links': list(personal_links)[:5],
                'page_size': len(html),
            }

            if emails:
                total_emails += len(emails)
                for e in emails:
                    print(f"    EMAIL: {e}")
            if personal_links:
                for l in list(personal_links)[:3]:
                    print(f"    LINK: {l}")
            if not emails and not personal_links:
                print(f"    No emails or links ({source}, {status})")
        else:
            results[handle] = {
                'source': 'failed',
                'emails': [],
                'personal_links': [],
            }
            print(f"    FAILED to fetch")

        time.sleep(0.5)

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY")
    print(f"{'=' * 70}")

    handles_with_emails = sum(1 for d in results.values() if d['emails'])
    handles_with_links = sum(1 for d in results.values() if d['personal_links'])

    print(f"  Total emails found: {total_emails}")
    print(f"  Handles with emails: {handles_with_emails}/{len(unique_handles)}")
    print(f"  Handles with personal links (can scrape for emails): {handles_with_links}/{len(unique_handles)}")
    print()

    with open('test_twitter_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"  Results saved to test_twitter_results.json\n")


if __name__ == '__main__':
    main()
