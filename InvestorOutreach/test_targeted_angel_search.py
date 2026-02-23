#!/usr/bin/env python3
"""
TARGETED approach for angel investor emails:
1. Scrape curated "top angel investor" list articles to get NAMES
2. For each name, do a targeted Bing search for their personal email
3. Also check known high-yield sources (about.me, personal sites, blogs)

This is fundamentally different from broad scraping - we identify specific
people first, then hunt for their specific email.
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
                'domain.com', 'email.com', 'sentry-next.wixpress.com',
                'threads.net', 'mastodon.social', 'mastodon.world', 'hachyderm.io',
                'infosec.exchange', 'journa.host', 'indieweb.social', 'masto.ai',
                'saturation.social', 'wandering.shop', 'scicomm.xyz', 'mstdn.social',
                'krigskunst.social', 'texasobserver.social', 'bloomberg.net',
                'washpost.com', 'techcrunch.com', 'cnn.com', 'wsj.com', 'wired.com',
                'nytimes.com', 'flipboard.com', 'medium.com', 'substack.com',
                'producthunt.com', 'folk.app', 'tracxn.com', 'rho.co', 'mytablon.com',
                'excedr.com', 'campaignlake.com', 'esalesclub.com', 'fastercapital.com',
                'cambridgeinnovationinstitute.com', 'cambridgevip.com', 'healthtech.com',
                'epicmed.com', 'epic.com', 'leadiq.com', 'anglehealth.com', 'chcsolutions.com',
                'healthsourcemedicalsupply.com', 'streak.com', 'hustlefund.vc',
                'sonicxmedia.com', 'cybervillains.com', 'soreniverson.com', 'whoapp.ai',
                'getpliant.com', 'uw.edu', 'lifesciencenation.com', 'angelspartners.com',
                'angelcapitalassociation.org', 'sciencecenter.org', 'wtop.com',
                'crain.com', 'remoovit.com', 'rockhealth.org', 'citrineangels.com',
                'longevitylist.com', 'wisinvpartners.com'}


def is_real_investor_email(email):
    """Strict filter for actual personal investor emails."""
    email = email.lower().strip()
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
                         'ceo@', 'example@', 'test@', 'contact@', 'hello@',
                         'hi@', 'info@', 'reach@', 'name@', 'clientservice@')):
        return False

    return True


def scrape_emails(url, session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return set()
        emails = set()
        for match in EMAIL_RE.findall(resp.text):
            if is_real_investor_email(match):
                emails.add(match.lower())
        soup = BeautifulSoup(resp.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            if 'mailto:' in href:
                email = href.split('mailto:')[1].split('?')[0].strip()
                if EMAIL_RE.match(email) and is_real_investor_email(email):
                    emails.add(email.lower())
        return emails
    except Exception:
        return set()


def bing_search(query, max_results=10):
    try:
        return DDGS().text(query, max_results=max_results, backend='bing')
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: Build a MASSIVE list of angel investor names from curated articles
# ══════════════════════════════════════════════════════════════════════════════

def extract_names_from_article(url, session):
    """Extract potential investor names from a list article."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        names = []

        # Strategy: look for headings (h2, h3, h4) that contain person names
        for tag in soup.find_all(['h2', 'h3', 'h4']):
            text = tag.get_text(strip=True)
            # Check if it looks like a person name (2-4 words, starts with caps)
            words = text.split()
            if 2 <= len(words) <= 5:
                if all(w[0].isupper() for w in words if len(w) > 1):
                    # Filter out common non-name headings
                    skip_words = ['What', 'How', 'Why', 'The', 'Top', 'Best', 'Our',
                                  'About', 'Table', 'Related', 'Similar', 'More',
                                  'Summary', 'Conclusion', 'Introduction', 'Overview',
                                  'Angel', 'Investors', 'Investment', 'Key', 'FAQ',
                                  'Source', 'Share', 'Follow']
                    if words[0] not in skip_words and not any(c.isdigit() for c in text):
                        names.append(text)

        # Also look for bold text in list items
        for li in soup.find_all('li'):
            bold = li.find(['strong', 'b'])
            if bold:
                text = bold.get_text(strip=True)
                words = text.split()
                if 2 <= len(words) <= 4:
                    if all(w[0].isupper() for w in words if len(w) > 1):
                        skip = ['Angel', 'Investor', 'Fund', 'Capital', 'Venture',
                                'The', 'Total', 'Net', 'Worth']
                        if words[0] not in skip:
                            names.append(text)

        return list(set(names))
    except Exception:
        return []


def build_name_list(session):
    """Build a massive list of angel investor names from curated articles."""
    print(f"\n  {'═' * 55}")
    print(f"  PHASE 1: Building Angel Investor Name List")
    print(f"  {'═' * 55}\n")

    # Search for list articles
    queries = [
        '"top angel investors" health 2024 OR 2025',
        '"best angel investors" health wellness',
        '"angel investors" "digital health" list',
        '"top 50" OR "top 100" angel investors',
        '"angel investors" list "health tech"',
        '"notable angel investors" health OR wellness OR fitness',
        '"angel investors" "seed stage" health list',
        '"prolific angel investors" health',
        'famous angel investors health wellness longevity',
        '"angel investor" directory list names 2025',
        '"angel investor" health wearable seed pre-seed list',
        '"health tech angel investors" who invest',
    ]

    all_articles = set()
    for q in queries:
        results = bing_search(q, 10)
        for r in results:
            url = r.get('href', '')
            if any(s in url for s in ['linkedin.com', 'twitter.com', 'facebook.com', 'youtube.com', 'reddit.com']):
                continue
            all_articles.add(url)
        print(f"    '{q[:50]}...' -> {len(results)} results")
        time.sleep(1.5)

    print(f"\n    Scraping {len(all_articles)} articles for investor names...\n")

    all_names = {}
    for url in list(all_articles)[:30]:
        names = extract_names_from_article(url, session)
        for name in names:
            if name not in all_names:
                all_names[name] = url
        if names:
            print(f"    {url[:55]}... -> {len(names)} names")
        time.sleep(0.5)

    print(f"\n    Total unique names extracted: {len(all_names)}")
    if all_names:
        print(f"    Sample names:")
        for name in list(all_names.keys())[:20]:
            print(f"      {name}")

    return all_names


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: Targeted email search for each angel investor
# ══════════════════════════════════════════════════════════════════════════════

def search_person_email(name, session):
    """Search for a specific person's email address."""
    found_emails = set()
    sources = []

    # Query 1: Direct email search
    results = bing_search(f'"{name}" email angel investor', 5)
    for r in results[:3]:
        url = r.get('href', '')
        if any(s in url for s in ['linkedin.com', 'twitter.com', 'facebook.com']):
            continue
        emails = scrape_emails(url, session)
        if emails:
            found_emails.update(emails)
            sources.append(url)
        time.sleep(0.2)

    time.sleep(1)

    # Query 2: Personal website
    if not found_emails:
        results = bing_search(f'"{name}" personal website OR blog investor', 5)
        for r in results[:3]:
            url = r.get('href', '')
            if any(s in url for s in ['linkedin.com', 'twitter.com', 'facebook.com', 'crunchbase.com']):
                continue
            emails = scrape_emails(url, session)
            if emails:
                found_emails.update(emails)
                sources.append(url)
            time.sleep(0.2)
        time.sleep(1)

    return found_emails, sources


def search_emails_for_names(names_dict, session, max_names=50):
    """For each name, do targeted email search."""
    print(f"\n  {'═' * 55}")
    print(f"  PHASE 2: Targeted Email Search ({min(len(names_dict), max_names)} names)")
    print(f"  {'═' * 55}\n")

    results = []
    found_count = 0
    searched = 0

    for name in list(names_dict.keys())[:max_names]:
        searched += 1
        emails, sources = search_person_email(name, session)
        if emails:
            found_count += 1
            for email in emails:
                results.append({
                    'name': name,
                    'email': email,
                    'source': sources[0] if sources else 'unknown',
                    'article': names_dict[name],
                })
                print(f"    [{found_count}/{searched}] {name}: {email}")
        if searched % 10 == 0:
            print(f"    ... searched {searched}/{min(len(names_dict), max_names)}, found {found_count} with emails")

    hit_rate = (found_count / searched * 100) if searched > 0 else 0
    print(f"\n    RESULT: {found_count} / {searched} names had findable emails ({hit_rate:.1f}% hit rate)")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: Known high-value angel investors (manually curated seed list)
# ══════════════════════════════════════════════════════════════════════════════

def search_known_angels(session):
    """Search for emails of well-known health/wellness angel investors."""
    print(f"\n  {'═' * 55}")
    print(f"  PHASE 3: Known Health/Wellness Angels (Direct Search)")
    print(f"  {'═' * 55}\n")

    # These are angels known to invest in health/wellness/consumer from public sources
    known_angels = [
        'Esther Dyson',       # Longevity angel
        'Vinod Khosla',       # Health/AI angel
        'Jason Calacanis',    # Famous angel
        'Naval Ravikant',     # AngelList founder
        'Lachy Groom',        # Prolific angel
        'Elad Gil',           # Health tech angel
        'Nat Friedman',       # AI/tech angel
        'Julia Cheek',        # Health startup founder/angel
        'Bob Kocher',         # Healthcare investor
        'Bryan Johnson',      # Longevity focused
        'Cindy Bi',           # Health tech angel
        'Lisa Suennen',       # Health investor
        'Unity Stoakes',      # StartUp Health founder
        'Steven Krein',       # StartUp Health
        'Howard Lindzon',     # Angel investor
        'Jeff Clavier',       # SoftTech VC
        'Dave McClure',       # 500 Startups
        'Cyan Banister',      # Angel investor
        'Alexis Ohanian',     # Reddit/angel
        'Sahil Lavingia',     # Gumroad/angel
        'Li Jin',             # Creator economy angel
        'Hunter Walk',        # Homebrew/angel
        'Josh Buckley',       # Angel investor
        'Harry Stebbings',    # Podcast host/angel
        'Keith Rabois',       # Angel investor
        'Todd McKinnon',      # Health tech
        'Halle Tecco',        # Rock Health founder
        'Megan Zweig',        # Rock Health
        'Troy Carter',        # Angel investor
        'Mike Maples',        # Angel/Floodgate
    ]

    results = []
    found = 0

    for name in known_angels:
        emails, sources = search_person_email(name, session)
        if emails:
            found += 1
            for email in emails:
                results.append({'name': name, 'email': email, 'source': sources[0] if sources else 'unknown'})
                print(f"    {name}: {email}")
        time.sleep(0.5)

    print(f"\n    RESULT: {found}/{len(known_angels)} known angels had findable emails ({found/len(known_angels)*100:.0f}%)")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    session = requests.Session()

    print(f"\n{'=' * 65}")
    print(f"  TARGETED ANGEL INVESTOR EMAIL DISCOVERY")
    print(f"  Phase 1: Extract names from curated articles")
    print(f"  Phase 2: Search each name for their email")
    print(f"  Phase 3: Search known high-value angels")
    print(f"{'=' * 65}")

    # Phase 1: Build name list
    names = build_name_list(session)

    # Phase 2: Search names for emails
    article_results = search_emails_for_names(names, session, max_names=40)

    # Phase 3: Known angels
    known_results = search_known_angels(session)

    # Final summary
    print(f"\n{'=' * 65}")
    print(f"  FINAL RESULTS")
    print(f"{'=' * 65}\n")

    all_emails = {}
    for r in article_results + known_results:
        email = r['email']
        if email not in all_emails:
            all_emails[email] = r

    print(f"  Total unique emails found: {len(all_emails)}")
    print(f"  From article name extraction: {len(article_results)}")
    print(f"  From known angel search: {len(known_results)}")

    if all_emails:
        print(f"\n  All angel investor emails:")
        for email, data in sorted(all_emails.items()):
            print(f"    {data['name']:25s} | {email}")

    with open('test_targeted_angel_results.json', 'w') as f:
        json.dump({
            'names_found': len(names),
            'article_results': article_results,
            'known_results': known_results,
            'all_emails': [{'email': e, **d} for e, d in all_emails.items()],
        }, f, indent=2)
    print(f"\n  Saved to test_targeted_angel_results.json\n")


if __name__ == '__main__':
    main()
