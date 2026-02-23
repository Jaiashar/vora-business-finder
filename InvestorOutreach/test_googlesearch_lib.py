#!/usr/bin/env python3
"""Test googlesearch-python library for finding angel investor emails via Google."""

from googlesearch import search
import requests
from bs4 import BeautifulSoup
import re
import json
import time

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}

JUNK_DOMAINS = {'example.com', 'sentry.io', 'w3.org', 'schema.org', 'google.com',
                'gstatic.com', 'googleapis.com', 'wordpress.org', 'gravatar.com',
                'wixpress.com', 'squarespace.com', 'cloudflare.com', 'github.com',
                'twitter.com', 'facebook.com', 'linkedin.com'}


def is_useful_email(email):
    email = email.lower()
    domain = email.split('@')[1] if '@' in email else ''
    if domain in JUNK_DOMAINS:
        return False
    if email.endswith(('.png', '.jpg', '.svg', '.gif', '.css', '.js')):
        return False
    if email.startswith(('noreply@', 'no-reply@', 'support@', 'webmaster@',
                         'privacy@', 'help@', 'abuse@')):
        return False
    return True


def scrape_url_for_emails(url, session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return set()
        emails = set()
        for match in EMAIL_RE.findall(resp.text):
            if is_useful_email(match):
                emails.add(match.lower())
        return emails
    except Exception:
        return set()


# Targeted Google dork queries
QUERIES = [
    # Angel investor personal emails
    '"angel investor" "health tech" email contact',
    '"angel investor" "pre-seed" "@gmail.com" health',
    'site:about.me angel investor health AI',
    'site:angel.co angel investor health wearable',

    # Angel groups with contact info
    '"angel group" OR "angel network" health tech members contact',
    '"angel investor" "reach me at" OR "email me" health AI',

    # Specific health/wellness investor searches
    '"health tech" investor contact email pre-seed wearable',
    'angel investor "digital health" "longevity" email',

    # Investor personal sites / substacks
    'site:substack.com investor "health tech" contact',
    '"invested in" "health" "wellness" angel investor contact',
]


def main():
    session = requests.Session()

    print(f"\n{'=' * 70}")
    print(f"  GOOGLESEARCH-PYTHON LIBRARY TEST")
    print(f"  Testing {len(QUERIES)} queries")
    print(f"{'=' * 70}\n")

    all_emails = set()
    all_urls_scraped = set()
    query_results = {}

    for i, query in enumerate(QUERIES):
        print(f"  [{i+1}/{len(QUERIES)}] {query[:65]}...")

        try:
            # googlesearch-python handles SERP parsing
            urls = list(search(query, num_results=10, sleep_interval=2))
            print(f"    {len(urls)} URLs returned")

            query_emails = set()
            for url in urls[:5]:  # scrape top 5
                if url not in all_urls_scraped:
                    all_urls_scraped.add(url)
                    emails = scrape_url_for_emails(url, session)
                    if emails:
                        query_emails.update(emails)
                        for e in emails:
                            print(f"    EMAIL ({url[:50]}...): {e}")
                    time.sleep(0.3)

            all_emails.update(query_emails)
            query_results[f"query_{i+1}"] = {
                'query': query,
                'urls': urls,
                'emails_found': list(query_emails),
            }

        except Exception as e:
            print(f"    ERROR: {str(e)[:60]}")
            query_results[f"query_{i+1}"] = {
                'query': query,
                'error': str(e)[:100],
            }

        time.sleep(3)  # between queries

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Total unique emails: {len(all_emails)}")
    print(f"  Total URLs scraped: {len(all_urls_scraped)}")
    if all_emails:
        print(f"\n  All emails:")
        for e in sorted(all_emails):
            print(f"    {e}")
    print()

    with open('test_googlesearch_results.json', 'w') as f:
        json.dump({
            'total_emails': len(all_emails),
            'emails': list(all_emails),
            'query_results': query_results,
        }, f, indent=2)
    print(f"  Results saved to test_googlesearch_results.json\n")


if __name__ == '__main__':
    main()
