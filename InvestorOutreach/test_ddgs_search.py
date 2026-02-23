#!/usr/bin/env python3
"""Test ddgs library for investor discovery using multiple search backends.
ddgs supports: DuckDuckGo, Bing, Brave, Google, Yandex, Yahoo"""

from ddgs import DDGS
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
                'wixpress.com', 'cloudflare.com', 'facebook.com', 'linkedin.com',
                'twitter.com', 'x.com'}


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


def extract_linkedin_names_from_results(results):
    """Extract names from LinkedIn URLs in search results."""
    names = []
    for r in results:
        url = r.get('href', r.get('link', ''))
        title = r.get('title', '')
        body = r.get('body', '')
        if 'linkedin.com/in/' in url:
            slug = url.split('linkedin.com/in/')[1].split('/')[0].split('?')[0]
            name_from_url = slug.replace('-', ' ').strip()
            name_from_title = title.split(' - ')[0].split(' | ')[0].strip() if title else ''
            names.append({
                'url': url,
                'name_from_url': name_from_url,
                'name_from_title': name_from_title,
                'full_title': title,
            })
    return names


# =====================
# QUERIES TO TEST
# =====================

# CrossLinked-style LinkedIn dorking
LINKEDIN_QUERIES = [
    'site:linkedin.com/in "7wire Ventures"',
    'site:linkedin.com/in "Rock Health"',
    'site:linkedin.com/in "Khosla Ventures" partner',
    'site:linkedin.com/in "Founders Fund"',
    'site:linkedin.com/in "General Catalyst" partner',
    'site:linkedin.com/in angel investor health tech',
    'site:linkedin.com/in angel investor "digital health"',
    'site:linkedin.com/in angel investor AI wellness',
]

# Email discovery
EMAIL_QUERIES = [
    'angel investor health tech email contact pre-seed',
    '"angel investor" "health" "@gmail.com" wearable',
    'angel investor digital health longevity contact',
    '"invested in" health AI angel personal email',
    'health tech angel investor portfolio contact about',
]

# Search backends to test
BACKENDS = ['duckduckgo', 'bing', 'brave']


def main():
    session = requests.Session()

    print(f"\n{'=' * 70}")
    print(f"  DDGS MULTI-ENGINE SEARCH TEST")
    print(f"  Backends: {', '.join(BACKENDS)}")
    print(f"{'=' * 70}\n")

    all_results = {}

    # =====================
    # TEST 1: LinkedIn Dorking per backend
    # =====================
    for backend in BACKENDS:
        print(f"\n  {'=' * 55}")
        print(f"  BACKEND: {backend.upper()}")
        print(f"  {'=' * 55}\n")

        ddgs = DDGS()
        backend_results = {}
        backend_linkedin_total = 0

        print(f"  --- LinkedIn Dorking ---\n")

        for query in LINKEDIN_QUERIES:
            print(f"  [{backend}] {query[:55]}...")
            try:
                results = ddgs.text(query, max_results=20, backend=backend)
                linkedin_names = extract_linkedin_names_from_results(results)
                backend_linkedin_total += len(linkedin_names)

                backend_results[query] = {
                    'num_results': len(results),
                    'linkedin_profiles': len(linkedin_names),
                    'names': [{'name': n['name_from_title'], 'url': n['url'][:80]} for n in linkedin_names[:10]],
                    'other_urls': [r.get('href', '')[:80] for r in results if 'linkedin.com/in/' not in r.get('href', '')][:5],
                }

                if linkedin_names:
                    for n in linkedin_names[:3]:
                        print(f"    LINKEDIN: {n['name_from_title']} ({n['name_from_url']})")
                    if len(linkedin_names) > 3:
                        print(f"    ... +{len(linkedin_names) - 3} more profiles")
                else:
                    print(f"    {len(results)} results, 0 LinkedIn profiles")

            except Exception as e:
                print(f"    ERROR: {str(e)[:60]}")
                backend_results[query] = {'error': str(e)[:100]}

            time.sleep(1)

        print(f"\n  [{backend}] Total LinkedIn names: {backend_linkedin_total}")

        # Email discovery
        print(f"\n  --- Email Discovery ---\n")
        backend_emails = set()

        for query in EMAIL_QUERIES[:3]:  # First 3 per backend
            print(f"  [{backend}] {query[:55]}...")
            try:
                results = ddgs.text(query, max_results=10, backend=backend)

                query_emails = set()
                # Check snippets for emails
                for r in results:
                    body = r.get('body', '')
                    for match in EMAIL_RE.findall(body):
                        if is_useful_email(match):
                            query_emails.add(match.lower())

                # Scrape top URLs
                for r in results[:3]:
                    url = r.get('href', '')
                    if url:
                        emails = scrape_url_for_emails(url, session)
                        query_emails.update(emails)
                    time.sleep(0.3)

                backend_emails.update(query_emails)
                backend_results[query] = {
                    'num_results': len(results),
                    'emails': list(query_emails),
                }

                if query_emails:
                    for e in query_emails:
                        print(f"    EMAIL: {e}")
                else:
                    print(f"    {len(results)} results, 0 emails")

            except Exception as e:
                print(f"    ERROR: {str(e)[:60]}")
                backend_results[query] = {'error': str(e)[:100]}

            time.sleep(1)

        print(f"\n  [{backend}] Total emails: {len(backend_emails)}")
        all_results[backend] = {
            'linkedin_names_total': backend_linkedin_total,
            'emails_total': len(backend_emails),
            'emails': list(backend_emails),
            'queries': backend_results,
        }

    # =====================
    # OVERALL SUMMARY
    # =====================
    print(f"\n{'=' * 70}")
    print(f"  OVERALL SUMMARY")
    print(f"{'=' * 70}")

    for backend, data in all_results.items():
        print(f"  {backend:15s} | LinkedIn names: {data['linkedin_names_total']:4d} | Emails: {data['emails_total']}")
        if data['emails']:
            for e in data['emails']:
                print(f"    {e}")

    best_backend = max(all_results.items(), key=lambda x: x[1]['linkedin_names_total'])
    print(f"\n  Best for LinkedIn names: {best_backend[0]} ({best_backend[1]['linkedin_names_total']} names)")

    best_email = max(all_results.items(), key=lambda x: x[1]['emails_total'])
    print(f"  Best for emails: {best_email[0]} ({best_email[1]['emails_total']} emails)")
    print()

    with open('test_ddgs_results.json', 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"  Results saved to test_ddgs_results.json\n")


if __name__ == '__main__':
    main()
