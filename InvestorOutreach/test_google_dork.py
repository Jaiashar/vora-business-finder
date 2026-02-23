#!/usr/bin/env python3
"""Test Google dorking for angel investor emails."""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

JUNK_DOMAINS = {'example.com', 'sentry.io', 'w3.org', 'schema.org', 'google.com',
                'gstatic.com', 'googleapis.com', 'googleusercontent.com',
                'wordpress.org', 'gravatar.com', 'wixpress.com', 'squarespace.com',
                'cloudflare.com', 'github.com', 'twitter.com', 'facebook.com'}


def is_useful_email(email):
    email = email.lower()
    domain = email.split('@')[1] if '@' in email else ''
    if domain in JUNK_DOMAINS:
        return False
    if email.endswith(('.png', '.jpg', '.svg', '.gif', '.css', '.js')):
        return False
    if email.startswith(('noreply@', 'no-reply@', 'support@', 'info@', 'admin@',
                         'webmaster@', 'contact@', 'privacy@', 'help@')):
        return False
    return True


def google_search(query, session, num_results=10):
    """Search Google and return result URLs + snippets."""
    url = f"https://www.google.com/search?q={requests.utils.quote(query)}&num={num_results}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return [], resp.status_code

        soup = BeautifulSoup(resp.text, 'html.parser')
        results = []

        # Extract search result links
        for g in soup.find_all('div', class_='g'):
            link = g.find('a', href=True)
            snippet = g.find('span', class_='st') or g.find('div', class_=['VwiC3b', 'IsZvec'])
            if link:
                href = link.get('href', '')
                if href.startswith('http') and 'google.com' not in href:
                    results.append({
                        'url': href,
                        'snippet': snippet.get_text(strip=True) if snippet else '',
                    })

        # Also extract emails directly from search results page
        page_emails = set()
        for match in EMAIL_RE.findall(resp.text):
            if is_useful_email(match):
                page_emails.add(match.lower())

        return results, resp.status_code, page_emails

    except Exception as e:
        return [], str(e), set()


def scrape_url_for_emails(url, session):
    """Scrape a single URL for email addresses."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return set(), resp.status_code

        emails = set()
        for match in EMAIL_RE.findall(resp.text):
            if is_useful_email(match):
                emails.add(match.lower())

        return emails, resp.status_code
    except Exception:
        return set(), 'error'


# Google dork queries targeting angel investors
DORK_QUERIES = [
    # Direct email searches
    '"angel investor" "health" "@gmail.com"',
    '"angel investor" "AI" "email" site:twitter.com',
    '"angel investor" "health tech" "reach me at"',
    '"angel investor" "pre-seed" "email me"',

    # Personal sites with contact info
    '"angel investor" "health" "contact" site:about.me',
    '"angel investor" inurl:contact "health" "@"',

    # AngelList / investor profiles
    'site:wellfound.com "angel investor" "health"',
    'site:crunchbase.com "angel investor" "health tech"',

    # Substack / newsletters by health investors
    'site:substack.com "angel investor" "health" "subscribe"',

    # Angel groups with member lists
    '"angel group" OR "angel network" "health" "members" "@"',

    # Specific health/AI angel searches
    '"invested in" "health" "angel" "@gmail.com" OR "@yahoo.com" OR "@outlook.com"',
    '"health tech investor" "email" -site:linkedin.com -site:facebook.com',
]


def main():
    session = requests.Session()

    print(f"\n{'=' * 70}")
    print(f"  GOOGLE DORKING TEST FOR ANGEL INVESTOR EMAILS")
    print(f"  Testing {len(DORK_QUERIES)} queries")
    print(f"{'=' * 70}\n")

    all_emails = set()
    all_urls = set()
    query_results = {}

    for i, query in enumerate(DORK_QUERIES):
        print(f"  [{i+1}/{len(DORK_QUERIES)}] {query[:70]}...")

        results, status, page_emails = google_search(query, session)

        if isinstance(status, int) and status == 429:
            print(f"    RATE LIMITED - waiting 30s")
            time.sleep(30)
            results, status, page_emails = google_search(query, session)

        query_data = {
            'query': query,
            'status': status,
            'num_results': len(results) if isinstance(results, list) else 0,
            'emails_from_serp': list(page_emails),
            'result_urls': [r['url'] for r in results] if isinstance(results, list) else [],
        }

        if page_emails:
            all_emails.update(page_emails)
            for e in page_emails:
                print(f"    SERP EMAIL: {e}")

        if isinstance(results, list) and results:
            print(f"    {len(results)} results found")
            # Scrape top 3 results for emails
            for r in results[:3]:
                url = r['url']
                if url not in all_urls:
                    all_urls.add(url)
                    emails, scrape_status = scrape_url_for_emails(url, session)
                    if emails:
                        all_emails.update(emails)
                        for e in emails:
                            print(f"    PAGE EMAIL ({url[:50]}...): {e}")
                    time.sleep(0.3)
        else:
            print(f"    Status: {status}, no results parsed")

        query_results[f"query_{i+1}"] = query_data
        time.sleep(random.uniform(3, 6))  # Be nice to Google

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Total unique emails found: {len(all_emails)}")
    print(f"  URLs scraped: {len(all_urls)}")
    print()
    if all_emails:
        print(f"  Emails:")
        for e in sorted(all_emails):
            print(f"    {e}")
    print()

    with open('test_google_dork_results.json', 'w') as f:
        json.dump({
            'emails': list(all_emails),
            'query_results': query_results,
        }, f, indent=2)
    print(f"  Results saved to test_google_dork_results.json\n")


if __name__ == '__main__':
    main()
