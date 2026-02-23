#!/usr/bin/env python3
"""Test Playwright on JS-heavy angel investor sites that requests couldn't render."""

import re
import json
import time

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

JUNK_PREFIXES = {'info@', 'hello@', 'contact@', 'support@', 'admin@', 'press@',
                 'media@', 'jobs@', 'careers@', 'noreply@', 'no-reply@', 'webmaster@',
                 'team@', 'office@', 'general@', 'help@', 'feedback@', 'privacy@'}
JUNK_DOMAINS = {'example.com', 'sentry.io', 'w3.org', 'schema.org',
                'wordpress.org', 'gravatar.com', 'wixpress.com', 'google.com',
                'cloudflare.com', 'gstatic.com', 'facebook.com'}


def is_useful_email(email):
    email = email.lower()
    for p in JUNK_PREFIXES:
        if email.startswith(p):
            return False
    domain = email.split('@')[1] if '@' in email else ''
    if domain in JUNK_DOMAINS:
        return False
    if email.endswith(('.png', '.jpg', '.svg', '.gif')):
        return False
    return True


# Sites that returned empty or JS-rendered content with requests
JS_HEAVY_SITES = [
    {
        'name': 'New York Angels - Members',
        'url': 'https://www.newyorkangels.com/members',
        'type': 'angel_network',
    },
    {
        'name': 'New York Angels - About',
        'url': 'https://www.newyorkangels.com/about',
        'type': 'angel_network',
    },
    {
        'name': 'Wellfound / AngelList Investors',
        'url': 'https://wellfound.com/investors',
        'type': 'directory',
    },
    {
        'name': 'Tech Coast Angels',
        'url': 'https://www.techcoastangels.com/members/',
        'type': 'angel_network',
    },
    {
        'name': 'Golden Seeds Team',
        'url': 'https://www.goldenseeds.com/',
        'type': 'angel_network',
    },
    {
        'name': 'Band of Angels Members',
        'url': 'https://www.bandangels.com/members',
        'type': 'angel_network',
    },
    {
        'name': 'Y Combinator People',
        'url': 'https://www.ycombinator.com/people',
        'type': 'accelerator',
    },
    {
        'name': 'Accel Team',
        'url': 'https://www.accel.com/team',
        'type': 'vc',
    },
    {
        'name': '7wireVentures Team (health focus)',
        'url': 'https://www.7wireventures.com/team/',
        'type': 'vc_health',
    },
    {
        'name': 'General Catalyst Team',
        'url': 'https://www.generalcatalyst.com/team',
        'type': 'vc',
    },
    {
        'name': 'Rock Health Team',
        'url': 'https://rockhealth.com/about/',
        'type': 'vc_health',
    },
    {
        'name': 'Signal by NFX Investors',
        'url': 'https://signal.nfx.com/investors',
        'type': 'directory',
    },
]


def extract_people_and_emails(page):
    """Extract emails and people names from a rendered page."""
    content = page.content()
    text = page.inner_text('body') if page.query_selector('body') else ''

    emails = set()
    for match in EMAIL_RE.findall(content):
        if is_useful_email(match):
            emails.add(match.lower())
    for match in EMAIL_RE.findall(text):
        if is_useful_email(match):
            emails.add(match.lower())

    # Extract mailto links
    mailto_links = page.query_selector_all('a[href^="mailto:"]')
    for link in mailto_links:
        href = link.get_attribute('href') or ''
        email = href.replace('mailto:', '').split('?')[0].strip()
        if EMAIL_RE.match(email) and is_useful_email(email):
            emails.add(email.lower())

    # Extract people names from common patterns
    people = []
    for selector in [
        '[class*="team"] [class*="name"]',
        '[class*="member"] [class*="name"]',
        '[class*="person"] [class*="name"]',
        '[class*="people"] h3', '[class*="people"] h4',
        '[class*="team"] h3', '[class*="team"] h4',
        '[class*="staff"] h3', '[class*="staff"] h4',
    ]:
        try:
            elements = page.query_selector_all(selector)
            for el in elements:
                name = el.inner_text().strip()
                if name and len(name) < 60 and '@' not in name:
                    people.append(name)
        except Exception:
            continue

    # Also try generic h3/h4 inside cards/grid items
    for selector in ['[class*="card"] h3', '[class*="grid"] h3', 'article h3',
                      '[class*="card"] h4', '[class*="grid"] h4']:
        try:
            elements = page.query_selector_all(selector)
            for el in elements:
                name = el.inner_text().strip()
                if name and len(name) < 60 and '@' not in name and len(name.split()) <= 4:
                    people.append(name)
        except Exception:
            continue

    return emails, list(set(people))


def main():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
        return

    print(f"\n{'=' * 70}")
    print(f"  PLAYWRIGHT JS-RENDERED SITE SCRAPING TEST")
    print(f"  Testing {len(JS_HEAVY_SITES)} sites")
    print(f"{'=' * 70}\n")

    results = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 800},
            locale='en-US',
        )
        page = context.new_page()

        for site in JS_HEAVY_SITES:
            print(f"  {site['name']}")
            print(f"    {site['url']}")

            try:
                page.goto(site['url'], wait_until='networkidle', timeout=20000)
                time.sleep(1)

                # Scroll to trigger lazy loading
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)

                content_size = len(page.content())
                emails, people = extract_people_and_emails(page)

                results[site['name']] = {
                    'url': site['url'],
                    'type': site['type'],
                    'content_size': content_size,
                    'emails': list(emails),
                    'people': people[:30],
                    'email_count': len(emails),
                    'people_count': len(people),
                }

                print(f"    Content: {content_size:,} bytes")
                print(f"    Emails: {len(emails)}, People: {len(people)}")

                if emails:
                    for e in list(emails)[:5]:
                        print(f"    -> EMAIL: {e}")
                if people:
                    for p_name in people[:5]:
                        print(f"    -> PERSON: {p_name}")

            except Exception as e:
                error_msg = str(e)[:80]
                print(f"    ERROR: {error_msg}")
                results[site['name']] = {
                    'url': site['url'],
                    'type': site['type'],
                    'error': error_msg,
                    'emails': [],
                    'people': [],
                }

            print()
            time.sleep(0.5)

        browser.close()

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY")
    print(f"{'=' * 70}")

    total_emails = 0
    total_people = 0
    for name, data in results.items():
        e_count = data.get('email_count', 0)
        p_count = data.get('people_count', 0)
        total_emails += e_count
        total_people += p_count
        status = f"{e_count} emails, {p_count} people" if not data.get('error') else f"ERROR"
        print(f"  {name}: {status}")

    print(f"\n  TOTAL: {total_emails} emails, {total_people} people names")
    print()

    with open('test_playwright_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"  Results saved to test_playwright_results.json\n")


if __name__ == '__main__':
    main()
