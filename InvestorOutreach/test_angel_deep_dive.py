#!/usr/bin/env python3
"""
Deep dive on the approaches that showed promise for angel emails:

1. Angel investor LIST ARTICLES (e.g. "top 50 health angel investors")
   -> Extract names -> Find personal sites -> Scrape emails
2. Crunchbase person profiles via Bing -> Extract names -> Find sites
3. About.me profiles (found btai@crv.com for Bill Tai)
4. Investor directory sites (Angel Capital Association, etc.)
5. Angel investor fund/group websites with team pages
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time

from ddgs import DDGS

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

JUNK_DOMAINS = {'example.com', 'sentry.io', 'w3.org', 'schema.org', 'google.com',
                'gstatic.com', 'googleapis.com', 'wordpress.org', 'gravatar.com',
                'wixpress.com', 'cloudflare.com', 'facebook.com', 'twitter.com',
                'linkedin.com', 'x.com', 'squarespace.com', 'github.com',
                'googletagmanager.com', 'fbcdn.net', 'yourdomain.com', 'disney.com',
                'domain.com', 'email.com'}


def is_personal_email(email):
    email = email.lower()
    domain = email.split('@')[1] if '@' in email else ''
    if domain in JUNK_DOMAINS:
        return False
    if email.endswith(('.png', '.jpg', '.svg', '.gif', '.css', '.js', '.webp')):
        return False
    if email.startswith(('noreply@', 'no-reply@', 'support@', 'webmaster@',
                         'privacy@', 'help@', 'abuse@', 'admin@', 'team@',
                         'press@', 'office@', 'general@', 'careers@', 'jobs@',
                         'media@', 'feedback@', 'billing@', 'sales@', 'legal@',
                         'security@', 'email_link', 'orders@', 'members@',
                         'providers@', 'user@', 'smartypants@', 'events@',
                         'ceo@', 'example@', 'test@')):
        return False
    if 'sentry' in domain or 'ingest' in domain:
        return False
    return True


def scrape_emails(url, session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
        if resp.status_code != 200:
            return set()
        emails = set()
        for match in EMAIL_RE.findall(resp.text):
            if is_personal_email(match):
                emails.add(match.lower())
        soup = BeautifulSoup(resp.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            if 'mailto:' in href:
                email = href.split('mailto:')[1].split('?')[0].strip()
                if EMAIL_RE.match(email) and is_personal_email(email):
                    emails.add(email.lower())
        return emails
    except Exception:
        return set()


def bing_search(query, max_results=20):
    try:
        return DDGS().text(query, max_results=max_results, backend='bing')
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
# APPROACH 1: Angel Investor List Articles -> Names -> Personal Sites -> Emails
# ══════════════════════════════════════════════════════════════════════════════

def test_list_articles(session):
    """Find "top angel investors" list articles, extract names, find their sites."""
    print(f"\n  {'═' * 55}")
    print(f"  APPROACH 1: Angel Investor List Articles")
    print(f"  {'═' * 55}\n")

    # Search for curated lists of angel investors
    queries = [
        '"angel investors" "health" list OR directory 2024 OR 2025',
        '"top angel investors" health wellness 2024 OR 2025',
        '"angel investors" "digital health" list names',
        '"best angel investors" "health tech" OR wellness OR fitness',
        'angel investors health AI longevity list portfolio',
        '"health angel investors" directory OR list OR top',
    ]

    all_articles = []
    all_emails = set()
    all_investor_names = []

    for q in queries:
        results = bing_search(q, 15)
        for r in results:
            url = r.get('href', '')
            title = r.get('title', '')
            if any(s in url for s in ['linkedin.com', 'twitter.com', 'facebook.com',
                                       'youtube.com', 'reddit.com']):
                continue
            all_articles.append({'url': url, 'title': title})
        print(f"    '{q[:55]}...' -> {len(results)} results")
        time.sleep(1.5)

    # Dedupe articles
    seen_urls = set()
    unique_articles = []
    for a in all_articles:
        if a['url'] not in seen_urls:
            seen_urls.add(a['url'])
            unique_articles.append(a)

    print(f"\n    Found {len(unique_articles)} unique articles. Scraping top 20...\n")

    # Scrape each article for emails and investor names
    for article in unique_articles[:20]:
        url = article['url']
        try:
            resp = session.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
            if resp.status_code != 200:
                continue

            # Extract emails
            emails = set()
            for match in EMAIL_RE.findall(resp.text):
                if is_personal_email(match):
                    emails.add(match.lower())

            soup = BeautifulSoup(resp.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a.get('href', '')
                if 'mailto:' in href:
                    email = href.split('mailto:')[1].split('?')[0].strip()
                    if EMAIL_RE.match(email) and is_personal_email(email):
                        emails.add(email.lower())

            if emails:
                all_emails.update(emails)
                print(f"    {article['title'][:50]}...")
                for e in emails:
                    print(f"      EMAIL: {e}")

        except Exception:
            pass
        time.sleep(0.5)

    print(f"\n    RESULT: {len(all_emails)} emails from list articles")
    return {'emails': list(all_emails)}


# ══════════════════════════════════════════════════════════════════════════════
# APPROACH 2: Crunchbase Names -> Personal Site Search -> Emails
# ══════════════════════════════════════════════════════════════════════════════

def test_crunchbase_to_personal(session):
    """Get angel investor names from Crunchbase via Bing, then find their personal sites."""
    print(f"\n  {'═' * 55}")
    print(f"  APPROACH 2: Crunchbase Names -> Personal Sites")
    print(f"  {'═' * 55}\n")

    # Get investor names from Crunchbase profiles
    queries = [
        'site:crunchbase.com/person "angel investor" health',
        'site:crunchbase.com/person "angel investor" wellness',
        'site:crunchbase.com/person "angel investor" "digital health"',
        'site:crunchbase.com/person "angel investor" fitness wearable',
        'site:crunchbase.com/person "angel investor" longevity',
        'site:crunchbase.com/person "angel investor" AI health',
    ]

    investors = []
    for q in queries:
        results = bing_search(q, 20)
        for r in results:
            url = r.get('href', '')
            title = r.get('title', '')
            if 'crunchbase.com/person' in url:
                name = title.split(' - ')[0].split(' | ')[0].strip()
                if name and len(name) > 2:
                    investors.append({'name': name, 'crunchbase_url': url})
        print(f"    '{q[:50]}...' -> {sum(1 for r in results if 'crunchbase.com/person' in r.get('href',''))} profiles")
        time.sleep(1.5)

    # Dedupe
    seen = set()
    unique_investors = []
    for inv in investors:
        if inv['name'].lower() not in seen:
            seen.add(inv['name'].lower())
            unique_investors.append(inv)

    print(f"\n    {len(unique_investors)} unique investor names. Searching for personal sites...\n")

    all_emails = set()
    email_details = []

    # For each investor, search for their personal site/email
    for inv in unique_investors[:30]:
        name = inv['name']
        search_q = f'"{name}" angel investor email OR contact site OR blog'
        results = bing_search(search_q, 5)

        for r in results[:3]:
            url = r.get('href', '')
            if any(s in url for s in ['linkedin.com', 'twitter.com', 'facebook.com',
                                       'crunchbase.com', 'wellfound.com']):
                continue
            emails = scrape_emails(url, session)
            if emails:
                all_emails.update(emails)
                for e in emails:
                    email_details.append({'name': name, 'email': e, 'source_url': url})
                    print(f"    {name}: {e} (from {url[:50]})")
            time.sleep(0.3)

        time.sleep(1)

    print(f"\n    RESULT: {len(all_emails)} emails from {len(unique_investors)} Crunchbase investors")
    return {'emails': list(all_emails), 'investors': unique_investors, 'email_details': email_details}


# ══════════════════════════════════════════════════════════════════════════════
# APPROACH 3: About.me Profiles at Scale
# ══════════════════════════════════════════════════════════════════════════════

def test_aboutme_scaled(session):
    """Find and scrape about.me profiles of angel investors using Playwright."""
    print(f"\n  {'═' * 55}")
    print(f"  APPROACH 3: About.me Angel Investor Profiles")
    print(f"  {'═' * 55}\n")

    queries = [
        'site:about.me angel investor health',
        'site:about.me angel investor wellness',
        'site:about.me angel investor AI',
        'site:about.me investor health tech',
        'site:about.me venture angel health',
        'site:about.me angel investor digital',
        'site:about.me investor wellness fitness',
        'site:about.me angel investor seed',
    ]

    profiles = []
    for q in queries:
        results = bing_search(q, 15)
        for r in results:
            url = r.get('href', '')
            title = r.get('title', '')
            if 'about.me/' in url:
                profiles.append({'url': url, 'title': title})
        print(f"    '{q[:45]}...' -> {sum(1 for r in results if 'about.me/' in r.get('href',''))} profiles")
        time.sleep(1.5)

    # Dedupe
    seen = set()
    unique = []
    for p in profiles:
        if p['url'] not in seen:
            seen.add(p['url'])
            unique.append(p)

    print(f"\n    {len(unique)} unique about.me profiles. Scraping with Playwright...\n")

    # About.me is JS-rendered, use Playwright
    all_emails = set()
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')

            for prof in unique[:25]:
                url = prof['url']
                try:
                    page.goto(url, wait_until='networkidle', timeout=12000)
                    content = page.content()
                    text = page.inner_text('body') if page.query_selector('body') else ''

                    emails = set()
                    for match in EMAIL_RE.findall(content + ' ' + text):
                        if is_personal_email(match):
                            emails.add(match.lower())

                    # Check mailto links
                    for link in page.query_selector_all('a[href^="mailto:"]'):
                        href = link.get_attribute('href') or ''
                        email = href.replace('mailto:', '').split('?')[0].strip()
                        if EMAIL_RE.match(email) and is_personal_email(email):
                            emails.add(email.lower())

                    if emails:
                        all_emails.update(emails)
                        for e in emails:
                            print(f"    {prof['title'][:40]}: {e}")
                    else:
                        # Check for personal website links
                        links = page.query_selector_all('a[href]')
                        for link in links:
                            href = link.get_attribute('href') or ''
                            if href.startswith('http') and 'about.me' not in href:
                                if not any(s in href for s in ['twitter.com', 'linkedin.com',
                                                                'facebook.com', 'instagram.com']):
                                    # Try scraping their personal site
                                    site_emails = scrape_emails(href, session)
                                    if site_emails:
                                        all_emails.update(site_emails)
                                        for e in site_emails:
                                            print(f"    {prof['title'][:40]}: {e} (from personal site {href[:40]})")
                                        break
                except Exception:
                    pass
                time.sleep(0.5)

            browser.close()
    except ImportError:
        print("    Playwright not available")

    print(f"\n    RESULT: {len(all_emails)} emails from {len(unique)} about.me profiles")
    return {'emails': list(all_emails), 'profiles': unique}


# ══════════════════════════════════════════════════════════════════════════════
# APPROACH 4: Angel Group / Network Websites
# ══════════════════════════════════════════════════════════════════════════════

def test_angel_groups(session):
    """Scrape angel group/network websites for member directories and contact info."""
    print(f"\n  {'═' * 55}")
    print(f"  APPROACH 4: Angel Group/Network Websites")
    print(f"  {'═' * 55}\n")

    # Known angel groups
    angel_groups = [
        {'name': 'Angel Capital Association', 'url': 'https://angelcapitalassociation.org/directory/'},
        {'name': 'Keiretsu Forum', 'url': 'https://keiretsuforum.com/chapters/'},
        {'name': 'Golden Seeds', 'url': 'https://www.goldenseeds.com/team'},
        {'name': 'Tech Coast Angels', 'url': 'https://www.techcoastangels.com/members/'},
        {'name': 'New York Angels', 'url': 'https://www.newyorkangels.com/about'},
        {'name': 'Houston Angel Network', 'url': 'https://www.houstonangelnetwork.org/team'},
        {'name': 'Atlanta Technology Angels', 'url': 'https://www.angelatlanta.com/'},
        {'name': 'Sand Hill Angels', 'url': 'https://www.sandhillangels.com/'},
        {'name': 'Life Science Angels', 'url': 'https://www.lifescienceangels.com/'},
        {'name': 'Desert Angels', 'url': 'https://www.desertangels.org/'},
        {'name': 'Alliance of Angels', 'url': 'https://www.allianceofangels.com/'},
        {'name': 'Robin Hood Ventures', 'url': 'https://www.robinhoodventures.com/members'},
        {'name': 'Pasadena Angels', 'url': 'https://www.pasadenaangels.com/'},
    ]

    all_emails = set()

    for group in angel_groups:
        emails = scrape_emails(group['url'], session)
        if emails:
            all_emails.update(emails)
            print(f"    {group['name']}: {len(emails)} emails")
            for e in list(emails)[:3]:
                print(f"      {e}")
        else:
            print(f"    {group['name']}: 0 emails")
        time.sleep(0.5)

    # Also search Bing for angel group contact pages
    print(f"\n    Bing search for angel group member contacts...")
    queries = [
        '"angel group" OR "angel network" members email health',
        '"angel investor group" directory contact email',
        'angel network team members email health tech',
    ]
    for q in queries:
        results = bing_search(q, 10)
        for r in results[:5]:
            url = r.get('href', '')
            if any(s in url for s in ['linkedin.com', 'twitter.com', 'facebook.com']):
                continue
            emails = scrape_emails(url, session)
            if emails:
                all_emails.update(emails)
                for e in emails:
                    print(f"      {e} (from {url[:50]})")
            time.sleep(0.3)
        print(f"    '{q[:50]}...' -> {len(results)} results")
        time.sleep(1.5)

    print(f"\n    RESULT: {len(all_emails)} emails from angel groups")
    return {'emails': list(all_emails)}


# ══════════════════════════════════════════════════════════════════════════════
# APPROACH 5: Micro-VC / Solo GP Fund Sites
# ══════════════════════════════════════════════════════════════════════════════

def test_micro_vc_sites(session):
    """Find micro-VC and solo GP funds focused on health - these often have contact emails."""
    print(f"\n  {'═' * 55}")
    print(f"  APPROACH 5: Micro-VC / Solo GP Health Fund Sites")
    print(f"  {'═' * 55}\n")

    queries = [
        '"solo gp" OR "micro vc" health angel fund email contact',
        '"solo gp" health wellness investor contact',
        '"pre-seed fund" health wellness angel email',
        '"micro fund" OR "micro vc" digital health email contact',
        '"angel fund" health wellness longevity contact',
        '"emerging manager" health tech fund email',
        '"health fund" angel OR seed email contact about',
        '"solo capitalist" health OR wellness email',
    ]

    all_emails = set()
    scraped_urls = set()

    for q in queries:
        results = bing_search(q, 15)
        for r in results[:5]:
            url = r.get('href', '')
            title = r.get('title', '')
            if any(s in url for s in ['linkedin.com', 'twitter.com', 'facebook.com',
                                       'crunchbase.com', 'medium.com']):
                continue
            if url in scraped_urls:
                continue
            scraped_urls.add(url)

            emails = scrape_emails(url, session)
            if emails:
                all_emails.update(emails)
                for e in emails:
                    print(f"    {e} (from {title[:40]}... {url[:40]})")

            # Also try /contact and /about pages
            base = url.rstrip('/')
            for suffix in ['/contact', '/about', '/team']:
                sub_url = base + suffix
                if sub_url not in scraped_urls:
                    scraped_urls.add(sub_url)
                    emails = scrape_emails(sub_url, session)
                    if emails:
                        all_emails.update(emails)
                        for e in emails:
                            print(f"    {e} (from {sub_url[:60]})")
                    time.sleep(0.3)

            time.sleep(0.3)

        print(f"    '{q[:55]}...' -> {len(results)} results")
        time.sleep(1.5)

    print(f"\n    RESULT: {len(all_emails)} emails from {len(scraped_urls)} micro-VC sites")
    return {'emails': list(all_emails)}


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    session = requests.Session()

    print(f"\n{'=' * 65}")
    print(f"  ANGEL INVESTOR DEEP DIVE - 5 Targeted Approaches")
    print(f"{'=' * 65}")

    all_results = {}

    all_results['list_articles'] = test_list_articles(session)
    all_results['crunchbase_to_personal'] = test_crunchbase_to_personal(session)
    all_results['aboutme'] = test_aboutme_scaled(session)
    all_results['angel_groups'] = test_angel_groups(session)
    all_results['micro_vc'] = test_micro_vc_sites(session)

    # Final summary
    print(f"\n{'=' * 65}")
    print(f"  DEEP DIVE RESULTS")
    print(f"{'=' * 65}\n")

    all_emails = set()
    for name, data in all_results.items():
        emails = set(data.get('emails', []))
        all_emails.update(emails)
        extras = ''
        if data.get('investors'):
            extras = f" ({len(data['investors'])} names discovered)"
        if data.get('profiles'):
            extras = f" ({len(data['profiles'])} profiles found)"
        print(f"  {name:25s} | {len(emails):3d} emails{extras}")

    print(f"\n  TOTAL UNIQUE EMAILS: {len(all_emails)}")
    if all_emails:
        print(f"\n  All emails:")
        for e in sorted(all_emails):
            print(f"    {e}")

    with open('test_angel_deep_dive_results.json', 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n  Saved to test_angel_deep_dive_results.json\n")


if __name__ == '__main__':
    main()
