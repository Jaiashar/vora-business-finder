#!/usr/bin/env python3
"""
Comprehensive test of ALL strategies for finding angel investor emails.
Tests 6 different approaches and reports which ones actually yield results.

Strategy 1: Crunchbase - investors in health/wellness seed deals
Strategy 2: Conference speaker lists (HLTH, StartUp Health, etc.)
Strategy 3: SEC EDGAR Form D filings (public angel investor data)
Strategy 4: Wellfound/AngelList profiles via Playwright
Strategy 5: Substack health investor newsletters
Strategy 6: Scaled personal site discovery via Bing search
"""

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
                'wixpress.com', 'cloudflare.com', 'facebook.com', 'twitter.com',
                'linkedin.com', 'x.com', 'squarespace.com', 'github.com',
                'sentry-next.wixpress.com', 'googletagmanager.com', 'fbcdn.net'}


def is_personal_email(email):
    """Check if email looks like a real personal/business email."""
    email = email.lower()
    domain = email.split('@')[1] if '@' in email else ''
    if domain in JUNK_DOMAINS:
        return False
    if email.endswith(('.png', '.jpg', '.svg', '.gif', '.css', '.js', '.webp')):
        return False
    if email.startswith(('noreply@', 'no-reply@', 'support@', 'webmaster@',
                         'privacy@', 'help@', 'abuse@', 'info@', 'hello@',
                         'admin@', 'team@', 'press@', 'contact@', 'office@',
                         'general@', 'careers@', 'jobs@', 'media@', 'feedback@',
                         'billing@', 'sales@', 'legal@', 'security@')):
        return False
    return True


def scrape_emails_from_url(url, session):
    """Scrape a URL for email addresses."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
        if resp.status_code != 200:
            return set(), resp.status_code

        emails = set()

        # Regex on raw HTML
        for match in EMAIL_RE.findall(resp.text):
            if is_personal_email(match):
                emails.add(match.lower())

        # mailto links
        soup = BeautifulSoup(resp.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            if 'mailto:' in href:
                email = href.split('mailto:')[1].split('?')[0].strip()
                if EMAIL_RE.match(email) and is_personal_email(email):
                    emails.add(email.lower())

        # Obfuscated patterns
        text = soup.get_text()
        for ob in re.findall(r'[\w.+-]+\s*\[?\s*(?:at|AT)\s*\]?\s*[\w.-]+\s*\[?\s*(?:dot|DOT)\s*\]?\s*\w+', text):
            cleaned = re.sub(r'\s*\[?\s*(?:at|AT)\s*\]?\s*', '@', ob)
            cleaned = re.sub(r'\s*\[?\s*(?:dot|DOT)\s*\]?\s*', '.', cleaned)
            if EMAIL_RE.match(cleaned) and is_personal_email(cleaned):
                emails.add(cleaned.lower())

        return emails, 200
    except Exception as e:
        return set(), str(e)[:50]


def bing_search(query, max_results=20):
    """Search Bing via ddgs."""
    from ddgs import DDGS
    try:
        results = DDGS().text(query, max_results=max_results, backend='bing')
        return results
    except Exception as e:
        return []


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: Crunchbase - Health/Wellness Angel Investors
# ══════════════════════════════════════════════════════════════════════════════

def test_crunchbase(session):
    """Scrape Crunchbase for angel investors in health deals."""
    print(f"\n  {'─' * 55}")
    print(f"  STRATEGY 1: Crunchbase Health Angel Investors")
    print(f"  {'─' * 55}\n")

    urls = [
        'https://www.crunchbase.com/hub/health-angel-investors',
        'https://www.crunchbase.com/hub/wellness-angel-investors',
        'https://www.crunchbase.com/hub/digital-health-angel-investors',
        'https://www.crunchbase.com/hub/health-care-angel-investors',
        'https://www.crunchbase.com/hub/fitness-angel-investors',
    ]

    results = {'emails': [], 'names': [], 'pages_accessible': 0}

    for url in urls:
        emails, status = scrape_emails_from_url(url, session)
        if status == 200:
            results['pages_accessible'] += 1
        if emails:
            results['emails'].extend(emails)
        print(f"    {url.split('/')[-1]}: status={status}, emails={len(emails)}")
        time.sleep(1)

    # Also try Bing to find Crunchbase angel profiles
    print(f"\n    Bing search for Crunchbase angel profiles...")
    queries = [
        'site:crunchbase.com/person angel investor health',
        'site:crunchbase.com/person angel investor wellness wearable',
        'site:crunchbase.com angel investor "digital health" seed',
    ]
    for q in queries:
        bing_results = bing_search(q)
        for r in bing_results:
            title = r.get('title', '')
            url = r.get('href', '')
            if 'crunchbase.com/person' in url:
                name = title.split(' - ')[0].split(' | ')[0].strip()
                results['names'].append({'name': name, 'url': url})
        print(f"    Query '{q[:50]}...': {len(bing_results)} results, {sum(1 for r in bing_results if 'crunchbase.com/person' in r.get('href',''))} person profiles")
        time.sleep(1.5)

    print(f"\n    RESULT: {len(results['emails'])} emails, {len(results['names'])} investor profiles")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: Conference Speaker Lists
# ══════════════════════════════════════════════════════════════════════════════

def test_conferences(session):
    """Scrape health tech conference speaker pages for investor contact info."""
    print(f"\n  {'─' * 55}")
    print(f"  STRATEGY 2: Health Tech Conference Speakers")
    print(f"  {'─' * 55}\n")

    conferences = [
        {'name': 'HLTH Conference', 'urls': [
            'https://www.hlth.com/speakers',
            'https://www.hlth.com/2024speakers',
            'https://www.hlth.com/2025speakers',
        ]},
        {'name': 'StartUp Health Festival', 'urls': [
            'https://www.startuphealth.com/festival',
            'https://www.startuphealth.com/health-transformer-awards',
        ]},
        {'name': 'Health 2.0', 'urls': [
            'https://www.health2con.com/speakers/',
        ]},
        {'name': 'MedCity INVEST', 'urls': [
            'https://events.medcitynews.com/invest/speakers/',
        ]},
        {'name': 'Digital Health Summit', 'urls': [
            'https://www.digitalhealthsummit.com/speakers',
        ]},
        {'name': 'JP Morgan Healthcare Conference', 'urls': [
            'https://www.jpmorgan.com/about-us/events-conferences/health-care-conference',
        ]},
    ]

    results = {'emails': [], 'speakers_found': 0, 'pages_accessible': 0}

    for conf in conferences:
        print(f"    {conf['name']}:")
        for url in conf['urls']:
            emails, status = scrape_emails_from_url(url, session)
            if status == 200:
                results['pages_accessible'] += 1
            if emails:
                results['emails'].extend(emails)
                for e in emails:
                    print(f"      EMAIL: {e}")
            print(f"      {url[:60]}... status={status}, emails={len(emails)}")
            time.sleep(0.5)

    # Bing search for conference speakers with emails
    print(f"\n    Bing search for health investor conference speakers...")
    queries = [
        '"health tech" conference speaker investor angel email contact 2025',
        '"digital health" summit speaker investor email',
        '"HLTH" speaker investor angel 2024 2025',
        'health investor conference panelist email contact',
    ]
    for q in queries:
        bing_results = bing_search(q, 10)
        for r in bing_results[:3]:
            url = r.get('href', '')
            emails, _ = scrape_emails_from_url(url, session)
            if emails:
                results['emails'].extend(emails)
                for e in emails:
                    print(f"      EMAIL from {url[:50]}: {e}")
            time.sleep(0.5)
        print(f"    '{q[:50]}...': {len(bing_results)} results")
        time.sleep(1.5)

    print(f"\n    RESULT: {len(results['emails'])} emails, {results['pages_accessible']} pages accessible")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: SEC EDGAR Form D Filings
# ══════════════════════════════════════════════════════════════════════════════

def test_sec_edgar(session):
    """Search SEC EDGAR for Form D filings (angel/seed fundraising disclosures)."""
    print(f"\n  {'─' * 55}")
    print(f"  STRATEGY 3: SEC EDGAR Form D Filings")
    print(f"  {'─' * 55}\n")

    # EDGAR full-text search API
    queries = [
        'https://efts.sec.gov/LATEST/search-index?q=%22health%22%20%22angel%22&dateRange=custom&startdt=2024-01-01&enddt=2025-12-31&forms=D',
        'https://efts.sec.gov/LATEST/search-index?q=%22wellness%22%20%22seed%22&forms=D',
    ]

    # EDGAR XBRL company search
    edgar_urls = [
        'https://efts.sec.gov/LATEST/search-index?q=%22digital+health%22&forms=D&dateRange=custom&startdt=2024-01-01',
        'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=D&dateb=&owner=include&count=40&search_text=health+wellness&action=getcompany',
    ]

    # EDGAR full-text search (new API)
    base_url = 'https://efts.sec.gov/LATEST/search-index'
    search_url = 'https://efts.sec.gov/LATEST/search-index?q=%22health%22+%22angel%22&forms=D'

    results = {'filings': 0, 'names': [], 'emails': []}

    # Try the EDGAR full-text search
    try:
        resp = session.get(
            'https://efts.sec.gov/LATEST/search-index?q=%22digital+health%22&forms=D&dateRange=custom&startdt=2024-01-01',
            headers={**HEADERS, 'Accept': 'application/json'},
            timeout=15,
        )
        print(f"    EDGAR API search: status={resp.status_code}, size={len(resp.text):,}")
        if resp.status_code == 200:
            try:
                data = resp.json()
                hits = data.get('hits', {}).get('hits', [])
                results['filings'] = len(hits)
                print(f"    Found {len(hits)} Form D filings")
                for hit in hits[:5]:
                    source = hit.get('_source', {})
                    print(f"      {source.get('display_names', ['?'])[0]}: {source.get('file_description', '?')}")
            except Exception:
                print(f"    Could not parse JSON response")
    except Exception as e:
        print(f"    EDGAR API: {str(e)[:60]}")

    # Also try the traditional EDGAR search
    try:
        resp = session.get(
            'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=D&dateb=&owner=include&count=20&search_text=health+digital&action=getcompany',
            headers=HEADERS,
            timeout=15,
        )
        print(f"    EDGAR company search: status={resp.status_code}")
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            # Extract company names from results
            rows = soup.find_all('tr')
            company_count = 0
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    company_count += 1
            print(f"    Found {company_count} companies in EDGAR")
    except Exception as e:
        print(f"    EDGAR search: {str(e)[:60]}")

    # Bing for EDGAR Form D health investors
    print(f"\n    Bing search for SEC Form D health angel filings...")
    bing_results = bing_search('site:sec.gov "form D" "health" "angel" investor', 10)
    print(f"    {len(bing_results)} results")
    for r in bing_results[:3]:
        print(f"      {r.get('title', '')[:70]}")

    print(f"\n    RESULT: {results['filings']} filings found")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 4: Wellfound/AngelList via Playwright
# ══════════════════════════════════════════════════════════════════════════════

def test_wellfound(session):
    """Scrape Wellfound (AngelList) for angel investor profiles."""
    print(f"\n  {'─' * 55}")
    print(f"  STRATEGY 4: Wellfound/AngelList Angel Profiles")
    print(f"  {'─' * 55}\n")

    results = {'profiles': [], 'emails': [], 'pages_accessible': 0}

    # Bing search for Wellfound investor profiles
    queries = [
        'site:wellfound.com angel investor health',
        'site:wellfound.com angel investor "digital health" wellness',
        'site:wellfound.com/u angel investor health',
        'site:angel.co angel investor health wearable',
    ]

    for q in queries:
        bing_results = bing_search(q, 15)
        for r in bing_results:
            url = r.get('href', '')
            title = r.get('title', '')
            if '/u/' in url or 'angel.co' in url:
                results['profiles'].append({'name': title.split(' - ')[0].strip(), 'url': url})
        print(f"    '{q[:50]}...': {len(bing_results)} results, {sum(1 for r in bing_results if '/u/' in r.get('href',''))} profiles")
        time.sleep(1.5)

    # Try scraping a few wellfound profiles for emails
    unique_profiles = {p['url']: p for p in results['profiles']}.values()
    for profile in list(unique_profiles)[:5]:
        url = profile['url']
        emails, status = scrape_emails_from_url(url, session)
        if emails:
            results['emails'].extend(emails)
            for e in emails:
                print(f"      EMAIL ({profile['name']}): {e}")
        time.sleep(0.5)

    print(f"\n    RESULT: {len(list(unique_profiles))} profiles, {len(results['emails'])} emails")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 5: Substack Health Investor Newsletters
# ══════════════════════════════════════════════════════════════════════════════

def test_substack(session):
    """Find health/wellness investor Substack newsletters (reply-to = personal email)."""
    print(f"\n  {'─' * 55}")
    print(f"  STRATEGY 5: Substack Health Investor Newsletters")
    print(f"  {'─' * 55}\n")

    results = {'newsletters': [], 'emails': []}

    # Bing search for investor substacks
    queries = [
        'site:substack.com angel investor health wellness',
        'site:substack.com "health tech" investor newsletter',
        'site:substack.com "digital health" investor angel',
        'site:substack.com health investor "pre-seed" OR "seed" OR angel',
        'site:substack.com longevity investor wellness',
    ]

    for q in queries:
        bing_results = bing_search(q, 15)
        for r in bing_results:
            url = r.get('href', '')
            title = r.get('title', '')
            if 'substack.com' in url:
                results['newsletters'].append({'title': title, 'url': url})
        print(f"    '{q[:50]}...': {len(bing_results)} results")
        time.sleep(1.5)

    # Scrape newsletter pages for emails/about pages
    unique_subs = {n['url'].split('?')[0]: n for n in results['newsletters']}.values()
    for sub in list(unique_subs)[:10]:
        url = sub['url']
        # Try the about page for each substack
        base = url.split('.substack.com')[0] + '.substack.com' if '.substack.com' in url else url.rstrip('/')
        for page in [base + '/about', base]:
            emails, status = scrape_emails_from_url(page, session)
            if emails:
                results['emails'].extend(emails)
                for e in emails:
                    print(f"      EMAIL ({sub['title'][:40]}): {e}")
                break
            time.sleep(0.3)

    print(f"\n    RESULT: {len(list(unique_subs))} newsletters, {len(results['emails'])} emails")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 6: Scaled Personal Site Discovery
# ══════════════════════════════════════════════════════════════════════════════

def test_personal_sites_scaled(session):
    """Use Bing to find angel investor personal websites, then scrape for emails."""
    print(f"\n  {'─' * 55}")
    print(f"  STRATEGY 6: Personal Site Discovery (Scaled)")
    print(f"  {'─' * 55}\n")

    results = {'sites': [], 'emails': [], 'email_details': []}

    # Targeted queries to find personal sites of angel investors
    queries = [
        # Personal sites with contact pages
        '"angel investor" "health" contact email -linkedin -twitter -facebook',
        '"angel investor" "digital health" "about me" OR "contact" email',
        '"angel investor" "wellness" portfolio contact email',
        'angel investor health "reach out" OR "get in touch" email',
        'angel investor "pre-seed" health "my portfolio" email',

        # About.me profiles
        'site:about.me angel investor health',
        'site:about.me angel investor wellness AI',

        # Personal blogs/sites
        'angel investor health blog "email me" OR "reach me"',
        '"angel investor" "health tech" site:.com/about OR site:.com/contact',

        # Investor directories with emails
        '"angel investor" directory health email',
        '"angel investor" "digital health" list email contact',
        'angel investor longevity AI "contact me" email',
    ]

    all_urls_scraped = set()

    for q in queries:
        bing_results = bing_search(q, 15)
        print(f"    '{q[:55]}...'")
        print(f"      {len(bing_results)} results")

        for r in bing_results[:5]:
            url = r.get('href', '')
            title = r.get('title', '')

            # Skip social media and known non-useful sites
            skip = ['linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com',
                    'youtube.com', 'reddit.com', 'wikipedia.org', 'crunchbase.com']
            if any(s in url for s in skip):
                continue
            if url in all_urls_scraped:
                continue

            all_urls_scraped.add(url)
            emails, status = scrape_emails_from_url(url, session)
            if emails:
                for e in emails:
                    results['emails'].append(e)
                    results['email_details'].append({
                        'email': e,
                        'source_url': url,
                        'source_title': title[:100],
                        'query': q[:60],
                    })
                    print(f"      EMAIL: {e} (from {url[:50]})")
            time.sleep(0.3)

        time.sleep(1.5)

    # Dedupe
    unique_emails = list(set(results['emails']))
    results['emails'] = unique_emails

    print(f"\n    RESULT: {len(all_urls_scraped)} sites scraped, {len(unique_emails)} unique emails")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    session = requests.Session()

    print(f"\n{'=' * 65}")
    print(f"  ANGEL INVESTOR EMAIL DISCOVERY - STRATEGY TESTING")
    print(f"  Testing 6 different approaches")
    print(f"{'=' * 65}")

    all_results = {}

    # Run all strategies
    all_results['crunchbase'] = test_crunchbase(session)
    all_results['conferences'] = test_conferences(session)
    all_results['sec_edgar'] = test_sec_edgar(session)
    all_results['wellfound'] = test_wellfound(session)
    all_results['substack'] = test_substack(session)
    all_results['personal_sites'] = test_personal_sites_scaled(session)

    # ══════════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 65}")
    print(f"  STRATEGY COMPARISON")
    print(f"{'=' * 65}\n")

    total_emails = set()
    for name, data in all_results.items():
        emails = data.get('emails', [])
        unique = set(e if isinstance(e, str) else '' for e in emails)
        total_emails.update(unique)

        extras = ''
        if data.get('names'):
            extras += f", {len(data['names'])} profiles"
        if data.get('newsletters'):
            extras += f", {len(set(n['url'].split('?')[0] for n in data['newsletters']))} newsletters"
        if data.get('filings'):
            extras += f", {data['filings']} filings"
        if data.get('profiles'):
            extras += f", {len(set(p['url'] for p in data['profiles']))} profiles"

        verdict = 'WORKS' if len(unique) > 0 else 'NO EMAILS'
        print(f"  {name:20s} | {len(unique):3d} emails{extras}  [{verdict}]")

    total_emails.discard('')
    print(f"\n  TOTAL UNIQUE EMAILS: {len(total_emails)}")
    if total_emails:
        print(f"\n  All emails found:")
        for e in sorted(total_emails):
            print(f"    {e}")

    # Save results
    with open('test_angel_strategies_results.json', 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n  Results saved to test_angel_strategies_results.json\n")


if __name__ == '__main__':
    main()
