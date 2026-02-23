#!/usr/bin/env python3
"""Test multiple search engines for investor email discovery:
1. DuckDuckGo (no consent wall, simpler HTML)
2. Google via Playwright (renders JS, handles consent)
3. Bing via Playwright
Also test CrossLinked-style LinkedIn dorking via these engines."""

import requests
from bs4 import BeautifulSoup
import re
import json
import time

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

JUNK_DOMAINS = {'example.com', 'sentry.io', 'w3.org', 'schema.org', 'google.com',
                'gstatic.com', 'googleapis.com', 'wordpress.org', 'gravatar.com',
                'wixpress.com', 'cloudflare.com', 'facebook.com'}


def is_useful_email(email):
    email = email.lower()
    domain = email.split('@')[1] if '@' in email else ''
    if domain in JUNK_DOMAINS:
        return False
    if email.endswith(('.png', '.jpg', '.svg', '.gif', '.css', '.js')):
        return False
    if email.startswith(('noreply@', 'no-reply@', 'support@', 'webmaster@', 'privacy@')):
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


# =====================
# PART 1: DuckDuckGo
# =====================

def ddg_search(query, session, num_results=10):
    """Search DuckDuckGo HTML version."""
    url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return [], resp.status_code

        soup = BeautifulSoup(resp.text, 'html.parser')
        results = []

        for link in soup.find_all('a', class_='result__a'):
            href = link.get('href', '')
            title = link.get_text(strip=True)
            if href and href.startswith('http'):
                results.append({'url': href, 'title': title})
            if len(results) >= num_results:
                break

        # Also extract any URLs from result snippets
        for snippet in soup.find_all('a', class_='result__snippet'):
            href = snippet.get('href', '')
            if href and href.startswith('http'):
                results.append({'url': href, 'title': snippet.get_text(strip=True)[:80]})

        return results, resp.status_code
    except Exception as e:
        return [], str(e)


def extract_linkedin_names(results):
    """Extract names from LinkedIn URLs in search results."""
    names = []
    for r in results:
        url = r.get('url', '')
        title = r.get('title', '')
        if 'linkedin.com/in/' in url:
            # Extract name from URL path
            parts = url.split('linkedin.com/in/')[1].split('/')[0].split('?')[0]
            name_from_url = parts.replace('-', ' ').strip()
            # Also extract from title (usually "First Last - Title | LinkedIn")
            name_from_title = title.split(' - ')[0].split(' | ')[0].strip() if title else ''
            names.append({
                'url': url,
                'name_from_url': name_from_url,
                'name_from_title': name_from_title,
                'title': title,
            })
    return names


# =====================
# PART 2: Playwright Google/Bing
# =====================

def playwright_google_search(query, page):
    """Search Google using Playwright to handle JS rendering."""
    url = f"https://www.google.com/search?q={requests.utils.quote(query)}&num=20&hl=en"
    try:
        page.goto(url, wait_until='networkidle', timeout=15000)
        time.sleep(1)

        # Handle consent page if present
        try:
            consent = page.query_selector('button:has-text("Accept")')
            if consent:
                consent.click()
                time.sleep(1)
        except Exception:
            pass
        try:
            consent = page.query_selector('button:has-text("Accept all")')
            if consent:
                consent.click()
                time.sleep(1)
        except Exception:
            pass
        try:
            consent = page.query_selector('[id*="accept"]')
            if consent:
                consent.click()
                time.sleep(1)
        except Exception:
            pass

        # Extract search results
        results = []
        links = page.query_selector_all('a[href]')
        for link in links:
            href = link.get_attribute('href') or ''
            if href.startswith('http') and 'google.com' not in href:
                text = link.inner_text().strip()
                if text and len(text) > 3:
                    results.append({'url': href, 'title': text[:100]})

        # Dedupe
        seen = set()
        unique = []
        for r in results:
            if r['url'] not in seen:
                seen.add(r['url'])
                unique.append(r)

        return unique[:20], 200
    except Exception as e:
        return [], str(e)


# =====================
# TEST QUERIES
# =====================

# LinkedIn dorking queries (CrossLinked style)
LINKEDIN_DORK_QUERIES = [
    'site:linkedin.com/in "7wireVentures"',
    'site:linkedin.com/in "Rock Health"',
    'site:linkedin.com/in "Khosla Ventures"',
    'site:linkedin.com/in "General Catalyst" partner',
    'site:linkedin.com/in "Founders Fund"',
]

# Email discovery queries
EMAIL_QUERIES = [
    'angel investor "health tech" email contact',
    '"angel investor" "pre-seed" health wellness email',
    '"digital health" investor angel "reach out" email',
    'angel investor longevity wearable AI contact',
]


def main():
    session = requests.Session()

    print(f"\n{'=' * 70}")
    print(f"  MULTI-ENGINE SEARCH TEST")
    print(f"{'=' * 70}\n")

    all_results = {'duckduckgo': {}, 'playwright_google': {}}

    # =====================
    # PART 1: DuckDuckGo Tests
    # =====================
    print(f"  {'=' * 60}")
    print(f"  DUCKDUCKGO TESTS")
    print(f"  {'=' * 60}\n")

    # Test LinkedIn dorking
    print(f"  --- LinkedIn Dorking (CrossLinked-style) ---\n")
    total_linkedin_names = 0

    for query in LINKEDIN_DORK_QUERIES:
        print(f"  DDG: {query[:60]}...")
        results, status = ddg_search(query, session)
        names = extract_linkedin_names(results)
        total_linkedin_names += len(names)

        all_results['duckduckgo'][query] = {
            'status': status,
            'num_results': len(results),
            'linkedin_names': names[:10],
            'all_urls': [r['url'] for r in results[:10]],
        }

        if names:
            for n in names[:5]:
                print(f"    LINKEDIN: {n['name_from_title']} ({n['url'][:60]})")
        else:
            print(f"    {len(results)} results, {len(names)} LinkedIn profiles")

        time.sleep(1)

    print(f"\n  Total LinkedIn names from DDG: {total_linkedin_names}\n")

    # Test email queries
    print(f"  --- Email Discovery ---\n")
    ddg_emails = set()

    for query in EMAIL_QUERIES:
        print(f"  DDG: {query[:60]}...")
        results, status = ddg_search(query, session)

        query_emails = set()
        for r in results[:5]:
            emails = scrape_url_for_emails(r['url'], session)
            query_emails.update(emails)
            time.sleep(0.3)

        ddg_emails.update(query_emails)
        all_results['duckduckgo'][query] = {
            'status': status,
            'num_results': len(results),
            'emails': list(query_emails),
        }

        if query_emails:
            for e in query_emails:
                print(f"    EMAIL: {e}")
        else:
            print(f"    {len(results)} results, 0 emails")

        time.sleep(1)

    print(f"\n  Total emails from DDG: {len(ddg_emails)}")
    if ddg_emails:
        for e in sorted(ddg_emails):
            print(f"    {e}")
    print()

    # =====================
    # PART 2: Playwright Google Tests
    # =====================
    try:
        from playwright.sync_api import sync_playwright

        print(f"  {'=' * 60}")
        print(f"  PLAYWRIGHT GOOGLE TESTS")
        print(f"  {'=' * 60}\n")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-US',
            )
            page = context.new_page()

            # Test LinkedIn dorking
            print(f"  --- LinkedIn Dorking via Google (Playwright) ---\n")
            pw_linkedin_names = 0

            for query in LINKEDIN_DORK_QUERIES[:3]:  # Test first 3 only
                print(f"  Google: {query[:60]}...")
                results, status = playwright_google_search(query, page)
                names = extract_linkedin_names(results)
                pw_linkedin_names += len(names)

                all_results['playwright_google'][query] = {
                    'status': status,
                    'num_results': len(results),
                    'linkedin_names': [{'name': n['name_from_title'], 'url': n['url']} for n in names[:15]],
                }

                if names:
                    for n in names[:5]:
                        print(f"    LINKEDIN: {n['name_from_title']} ({n['url'][:60]})")
                    if len(names) > 5:
                        print(f"    ... and {len(names) - 5} more")
                else:
                    print(f"    {len(results)} results, {len(names)} LinkedIn profiles")

                time.sleep(2)

            print(f"\n  Total LinkedIn names from Playwright Google: {pw_linkedin_names}\n")

            # Test email queries
            print(f"  --- Email Discovery via Google (Playwright) ---\n")
            pw_emails = set()

            for query in EMAIL_QUERIES[:2]:  # Test first 2
                print(f"  Google: {query[:60]}...")
                results, status = playwright_google_search(query, page)

                query_emails = set()
                for r in results[:5]:
                    emails = scrape_url_for_emails(r['url'], session)
                    query_emails.update(emails)
                    time.sleep(0.3)

                pw_emails.update(query_emails)
                all_results['playwright_google'][query] = {
                    'status': status,
                    'num_results': len(results),
                    'emails': list(query_emails),
                    'urls': [r['url'] for r in results[:10]],
                }

                if query_emails:
                    for e in query_emails:
                        print(f"    EMAIL: {e}")
                else:
                    print(f"    {len(results)} results, 0 emails")

                time.sleep(2)

            print(f"\n  Total emails from Playwright Google: {len(pw_emails)}")
            if pw_emails:
                for e in sorted(pw_emails):
                    print(f"    {e}")

            browser.close()

    except ImportError:
        print("  Playwright not available, skipping Google tests")

    # =====================
    # SUMMARY
    # =====================
    print(f"\n{'=' * 70}")
    print(f"  OVERALL SUMMARY")
    print(f"{'=' * 70}")
    print(f"  DuckDuckGo LinkedIn names: {total_linkedin_names}")
    print(f"  DuckDuckGo emails: {len(ddg_emails)}")
    print(f"  Playwright Google LinkedIn names: {pw_linkedin_names if 'pw_linkedin_names' in dir() else 'N/A'}")
    print(f"  Playwright Google emails: {len(pw_emails) if 'pw_emails' in dir() else 'N/A'}")
    print()

    with open('test_search_engines_results.json', 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"  Results saved to test_search_engines_results.json\n")


if __name__ == '__main__':
    main()
