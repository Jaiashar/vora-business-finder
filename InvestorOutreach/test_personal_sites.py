#!/usr/bin/env python3
"""Test scraping personal websites/blogs of known angel investors for emails."""

import requests
from bs4 import BeautifulSoup
import re
import json
import time

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

JUNK_DOMAINS = {'example.com', 'sentry.io', 'w3.org', 'schema.org', 'google.com',
                'gstatic.com', 'wordpress.org', 'gravatar.com', 'wixpress.com',
                'cloudflare.com', 'facebook.com', 'twitter.com', 'x.com',
                'squarespace.com', 'github.com', 'linkedin.com'}


def is_useful_email(email):
    email = email.lower()
    domain = email.split('@')[1] if '@' in email else ''
    if domain in JUNK_DOMAINS:
        return False
    if email.endswith(('.png', '.jpg', '.svg', '.gif', '.css', '.js')):
        return False
    if email.startswith(('noreply@', 'no-reply@', 'support@', 'webmaster@')):
        return False
    return True


def scrape_site(base_url, session):
    """Scrape a personal site for emails, checking homepage + common pages."""
    emails = set()
    pages_checked = []

    # Pages to check
    paths = ['', '/contact', '/about', '/about-me', '/connect', '/investing']

    for path in paths:
        url = base_url.rstrip('/') + path
        try:
            resp = session.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                pages_checked.append({'url': url, 'status': resp.status_code, 'emails': 0})
                continue

            # mailto links
            soup = BeautifulSoup(resp.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a.get('href', '')
                if 'mailto:' in href:
                    email = href.split('mailto:')[1].split('?')[0].strip()
                    if EMAIL_RE.match(email) and is_useful_email(email):
                        emails.add(email.lower())

            # Regex on text
            for match in EMAIL_RE.findall(soup.get_text()):
                if is_useful_email(match):
                    emails.add(match.lower())

            # Regex on raw HTML
            for match in EMAIL_RE.findall(resp.text):
                if is_useful_email(match):
                    emails.add(match.lower())

            # Obfuscated
            for ob in re.findall(r'[\w.+-]+\s*\[?\s*(?:at|AT)\s*\]?\s*[\w.-]+\s*\[?\s*(?:dot|DOT)\s*\]?\s*\w+', soup.get_text()):
                cleaned = re.sub(r'\s*\[?\s*(?:at|AT)\s*\]?\s*', '@', ob)
                cleaned = re.sub(r'\s*\[?\s*(?:dot|DOT)\s*\]?\s*', '.', cleaned)
                if EMAIL_RE.match(cleaned) and is_useful_email(cleaned):
                    emails.add(cleaned.lower())

            pages_checked.append({'url': url, 'status': 200, 'emails': len(emails)})

        except Exception as e:
            pages_checked.append({'url': url, 'status': str(e)[:50], 'emails': 0})

        time.sleep(0.3)

    return emails, pages_checked


# Known angel investors with personal websites
ANGEL_SITES = [
    {'name': 'Jason Calacanis', 'url': 'https://calacanis.com', 'type': 'angel'},
    {'name': 'Naval Ravikant', 'url': 'https://nav.al', 'type': 'angel'},
    {'name': 'Hunter Walk', 'url': 'https://hunterwalk.com', 'type': 'angel'},
    {'name': 'Mark Suster', 'url': 'https://bothsidesofthetable.com', 'type': 'angel'},
    {'name': 'Semil Shah', 'url': 'https://semilshah.com', 'type': 'angel'},
    {'name': 'David Tisch', 'url': 'https://www.davidtisch.com', 'type': 'angel'},
    {'name': 'Ryan Hoover', 'url': 'https://ryanhoover.me', 'type': 'angel'},
    {'name': 'Sahil Lavingia', 'url': 'https://sahillavingia.com', 'type': 'angel'},
    {'name': 'Elad Gil', 'url': 'https://eladgil.com', 'type': 'angel'},
    {'name': 'Harry Stebbings', 'url': 'https://www.harrystebbing.com', 'type': 'angel'},
    {'name': 'Cindy Bi', 'url': 'https://www.cindybi.com', 'type': 'angel'},
    {'name': 'Alexis Ohanian', 'url': 'https://alexisohanian.com', 'type': 'angel'},
    {'name': 'Garry Tan', 'url': 'https://blog.garrytan.com', 'type': 'angel'},
    {'name': 'Paul Graham', 'url': 'https://paulgraham.com', 'type': 'angel'},
    {'name': 'Sam Altman', 'url': 'https://blog.samaltman.com', 'type': 'angel'},

    # Health-focused investors personal sites
    {'name': 'Rock Health Blog', 'url': 'https://rockhealth.com/insights/', 'type': 'health_vc'},
    {'name': 'Andreessen Bio', 'url': 'https://a16z.com/bio-health/', 'type': 'health_vc'},

    # Angel investor directories / about pages
    {'name': 'AngelList About', 'url': 'https://wellfound.com/about', 'type': 'directory'},
    {'name': 'Hustle Fund Blog', 'url': 'https://www.hustlefund.vc/blog', 'type': 'micro_vc'},
]


def main():
    session = requests.Session()

    print(f"\n{'=' * 70}")
    print(f"  PERSONAL WEBSITE/BLOG SCRAPING TEST")
    print(f"  Testing {len(ANGEL_SITES)} sites")
    print(f"{'=' * 70}\n")

    results = {}
    total_emails = 0

    for site in ANGEL_SITES:
        name = site['name']
        url = site['url']

        print(f"  {name} ({url})")

        emails, pages = scrape_site(url, session)
        total_emails += len(emails)

        results[name] = {
            'url': url,
            'type': site['type'],
            'emails': list(emails),
            'email_count': len(emails),
            'pages_checked': pages,
        }

        if emails:
            for e in emails:
                print(f"    EMAIL: {e}")
        else:
            # Report which pages were accessible
            accessible = sum(1 for p in pages if p['status'] == 200)
            print(f"    No emails ({accessible}/{len(pages)} pages accessible)")

        time.sleep(0.5)

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY")
    print(f"{'=' * 70}")

    sites_with_emails = sum(1 for d in results.values() if d['email_count'] > 0)
    print(f"  Sites with emails: {sites_with_emails}/{len(ANGEL_SITES)}")
    print(f"  Total emails: {total_emails}")
    print()

    if total_emails > 0:
        print(f"  All emails found:")
        for name, data in results.items():
            if data['emails']:
                for e in data['emails']:
                    print(f"    {name}: {e}")
    print()

    with open('test_personal_site_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"  Results saved to test_personal_site_results.json\n")


if __name__ == '__main__':
    main()
