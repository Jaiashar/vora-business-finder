#!/usr/bin/env python3
"""
Iowa Deep Supplement Scraper
Targets departments with <5 emails using deeper crawling strategies:
- College of Law: faculty directory, all subpages
- College of Public Health: profiles API, faculty pages
- Writers' Workshop: nonfiction program, MFA pages
- Daily Iowan: SNO sites, author pages
- Additional Hawkeye sports pages for athletics
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
from urllib.parse import urljoin

def log(msg):
    print(msg, flush=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/iowa_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/iowa_dept_emails.json'

ADMIN_PREFIXES = [
    'info@','admin@','office@','dept@','webmaster@','help@','support@','contact@',
    'registrar@','grad@','gradoffice@','department@','chair@','advising@','undergrad@',
    'dean@','reception@','main@','general@','staff@','gradadmit@','calendar@','events@',
    'news@','newsletter@','web@','marketing@','media@','communications@','hr@','hiring@',
    'jobs@','career@','alumni@','development@','giving@','feedback@','safety@','security@',
    'facilities@','it@','tech@','helpdesk@','library@','gradapp@','apply@','admissions@',
    'enrollment@','records@','bursar@','finaid@','housing@','dining@','parking@','police@',
    'noreply@','do-not-reply@','donotreply@','president@','provost@','gradschool@',
    'gradstudies@','law-admissions@','law-career@','law-library@','athletics@','compliance@',
    'titleix@','title-ix@','conduct@','counseling@','disability@','veterans@','international@',
    'multicultural@','equity@','clas@','coe@','cph@','tippie@','nursing@','ask-',
    'its-helpdesk@','its@','hawkeyesports@','uifoundation@','ui-usg@','usg-','usg@',
    'con-','nursing-','sociology@','sociology-','actuarial-science@','writers-workshop@',
    'pharmacy-','pharmacy@','education@','daily-iowan@','geology@','clas-','engineering@',
    'law-','uiowa-','daily-iowan-circ@',
]

def is_admin(email):
    el = email.lower()
    return any(el.startswith(p) for p in ADMIN_PREFIXES)

def extract_uiowa_emails(text):
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*uiowa\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        m = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*uiowa\.edu)', e)
        cleaned.add(m.group(1).lower() if m else e)
    return list(cleaned)

def extract_mailto(soup):
    emails = []
    for a in soup.find_all('a', href=True):
        if 'mailto:' in a['href'].lower():
            m = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uiowa\.edu)', a['href'], re.IGNORECASE)
            if m: emails.append(m.group(1).lower().strip())
    return list(set(emails))

def decode_cf_email(enc):
    try:
        r = int(enc[:2], 16)
        return ''.join(chr(int(enc[i:i+2], 16) ^ r) for i in range(2, len(enc), 2)).lower()
    except: return None

def extract_cf_emails(soup):
    emails = []
    for tag in soup.find_all(attrs={'data-cfemail': True}):
        d = decode_cf_email(tag.get('data-cfemail', ''))
        if d and '@' in d: emails.append(d)
    for a in soup.find_all('a', href=True):
        m = re.search(r'/email-protection#([a-f0-9]+)', a.get('href', ''))
        if m:
            d = decode_cf_email(m.group(1))
            if d and '@' in d: emails.append(d)
    return [e for e in set(emails) if 'uiowa.edu' in e]

def get_soup(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code == 200:
            return BeautifulSoup(r.text, 'html.parser'), r.url
        return None, None
    except: return None, None

SKIP = ['email','contact','phone','http','department','graduate','student',
        'people','faculty','office','read more','view profile','website',
        'lab','research','iowa','uiowa','university','search','page']

def get_name(soup, email):
    for a in soup.find_all('a', href=True):
        if email in a.get('href','').lower():
            p = a.parent
            for _ in range(6):
                if not p: break
                for t in p.find_all(['h2','h3','h4','h5','strong','b','a','span']):
                    tx = t.get_text(strip=True)
                    if tx and '@' not in tx and 2<len(tx)<80 and not any(x in tx.lower() for x in SKIP):
                        return tx
                p = p.parent
    for elem in soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE)):
        p = elem.parent
        for _ in range(6):
            if not p: break
            for t in p.find_all(['h2','h3','h4','h5','strong','b','a','span']):
                tx = t.get_text(strip=True)
                if tx and '@' not in tx and 2<len(tx)<80 and not any(x in tx.lower() for x in SKIP):
                    return tx
            p = p.parent
    return ""

def scrape_all_emails(url, dept, session, seen):
    """Scrape a URL for all uiowa emails, returning new results."""
    results = []
    soup, final = get_soup(url, session)
    if not soup:
        return results
    src = final or url
    text = soup.get_text(separator=' ', strip=True)
    all_e = list(set(extract_uiowa_emails(text) + extract_mailto(soup) + extract_cf_emails(soup)))
    for e in all_e:
        if e not in seen and not is_admin(e):
            seen.add(e)
            results.append({'email': e, 'name': get_name(soup, e), 'department': dept, 'source_url': src})
    return results


def deep_crawl(base_url, dept, session, seen, max_pages=100, link_filter=None):
    """Crawl a site depth-first looking for emails."""
    results = []
    visited = set()
    to_visit = [base_url]
    pages_visited = 0

    while to_visit and pages_visited < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)
        pages_visited += 1

        log(f"    Crawling: {url}")
        soup, final = get_soup(url, session)
        if not soup:
            continue
        src = final or url
        text = soup.get_text(separator=' ', strip=True)
        all_e = list(set(extract_uiowa_emails(text) + extract_mailto(soup) + extract_cf_emails(soup)))
        
        for e in all_e:
            if e not in seen and not is_admin(e):
                seen.add(e)
                results.append({'email': e, 'name': get_name(soup, e), 'department': dept, 'source_url': src})

        # Find more links to crawl
        for a in soup.find_all('a', href=True):
            href = a['href']
            full = urljoin(src, href)
            # Remove fragments
            full = full.split('#')[0]
            if full in visited:
                continue
            if link_filter and not link_filter(full):
                continue
            if full not in to_visit:
                to_visit.append(full)

        time.sleep(0.3)

    return results


def scrape_law_deep(session, seen):
    """Deep crawl College of Law for emails."""
    dept = "College of Law"
    results = []
    log(f"\n{'='*60}\nDeep Crawl: {dept}\n{'='*60}")

    # Try profiles API for law
    api_urls = [
        "https://law.uiowa.edu/people/faculty",
        "https://law.uiowa.edu/people/staff",
        "https://law.uiowa.edu/people/adjunct-faculty",
        "https://law.uiowa.edu/people/visiting-faculty",
        "https://law.uiowa.edu/faculty",
        "https://law.uiowa.edu/staff",
        "https://law.uiowa.edu/about/faculty-staff",
        "https://law.uiowa.edu/about/faculty",
        "https://law.uiowa.edu/about/staff",
        "https://law.uiowa.edu/about",
        "https://law.uiowa.edu/student-life",
        "https://law.uiowa.edu/student-life/student-organizations",
        "https://law.uiowa.edu/student-life/journals",
        "https://law.uiowa.edu/student-organizations",
        "https://law.uiowa.edu/research",
        "https://law.uiowa.edu/research/centers",
        "https://law.uiowa.edu/jd",
        "https://law.uiowa.edu/llm",
    ]

    for url in api_urls:
        log(f"  Trying: {url}")
        r = scrape_all_emails(url, dept, session, seen)
        results.extend(r)
        if r:
            log(f"    -> +{len(r)} emails")
        time.sleep(0.5)

    # Deep crawl law.uiowa.edu/people
    def law_filter(url):
        return 'law.uiowa.edu' in url and not any(x in url for x in ['.pdf', '.doc', 'login', 'apply'])

    deep = deep_crawl("https://law.uiowa.edu/people", dept, session, seen, max_pages=30, link_filter=law_filter)
    results.extend(deep)

    log(f"  TOTAL {dept}: {len(results)} new")
    return results


def scrape_public_health_deep(session, seen):
    """Deep crawl College of Public Health."""
    dept = "College of Public Health"
    results = []
    log(f"\n{'='*60}\nDeep Crawl: {dept}\n{'='*60}")

    urls = [
        "https://www.public-health.uiowa.edu/people/",
        "https://www.public-health.uiowa.edu/people/faculty/",
        "https://www.public-health.uiowa.edu/people/staff/",
        "https://www.public-health.uiowa.edu/people/students/",
        "https://www.public-health.uiowa.edu/people/researchers/",
        "https://www.public-health.uiowa.edu/directory/",
        "https://www.public-health.uiowa.edu/departments/",
        "https://www.public-health.uiowa.edu/departments/biostatistics/",
        "https://www.public-health.uiowa.edu/departments/community-behavioral-health/",
        "https://www.public-health.uiowa.edu/departments/epidemiology/",
        "https://www.public-health.uiowa.edu/departments/health-management-policy/",
        "https://www.public-health.uiowa.edu/departments/occupational-environmental-health/",
        "https://www.public-health.uiowa.edu/contact/",
        "https://www.public-health.uiowa.edu/about/",
        "https://cph.uiowa.edu/people",
        "https://cph.uiowa.edu/people/faculty",
        "https://cph.uiowa.edu/people/staff",
        "https://cph.uiowa.edu/people/students",
    ]

    # Also try profiles API
    # Try common API keys for CPH
    profile_api_urls = [
        "https://www.public-health.uiowa.edu/wp-json/wp/v2/people?per_page=100",
        "https://www.public-health.uiowa.edu/wp-json/wp/v2/users?per_page=100",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        r = scrape_all_emails(url, dept, session, seen)
        results.extend(r)
        if r:
            log(f"    -> +{len(r)} emails")
        time.sleep(0.5)

    # Try WP REST API
    for url in profile_api_urls:
        log(f"  Trying API: {url}")
        try:
            resp = session.get(url, headers={'Accept': 'application/json'}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                for item in data:
                    email = ''
                    name = ''
                    if isinstance(item, dict):
                        email = item.get('email', item.get('acf', {}).get('email', '')) if isinstance(item.get('acf'), dict) else item.get('email', '')
                        name = item.get('title', {}).get('rendered', '') if isinstance(item.get('title'), dict) else item.get('name', '')
                    if email and 'uiowa.edu' in email.lower():
                        e = email.lower().strip()
                        if e not in seen and not is_admin(e):
                            seen.add(e)
                            results.append({'email': e, 'name': name, 'department': dept, 'source_url': url})
                log(f"    -> API returned {len(data)} items")
        except Exception as ex:
            log(f"    -> API error: {ex}")
        time.sleep(0.5)

    # Deep crawl with link filtering
    def cph_filter(url):
        return ('public-health.uiowa.edu' in url or 'cph.uiowa.edu' in url) and \
               not any(x in url for x in ['.pdf', '.doc', 'login', 'apply', 'news', 'events', 'calendar'])

    deep = deep_crawl("https://www.public-health.uiowa.edu/people/", dept, session, seen, max_pages=40, link_filter=cph_filter)
    results.extend(deep)

    log(f"  TOTAL {dept}: {len(results)} new")
    return results


def scrape_writers_workshop(session, seen):
    """Scrape Iowa Writers' Workshop and Nonfiction Writing Program."""
    dept = "Iowa Writers' Workshop / Nonfiction Writing Program"
    results = []
    log(f"\n{'='*60}\nDeep Crawl: {dept}\n{'='*60}")

    urls = [
        "https://writersworkshop.uiowa.edu/",
        "https://writersworkshop.uiowa.edu/people",
        "https://writersworkshop.uiowa.edu/people/faculty",
        "https://writersworkshop.uiowa.edu/people/students",
        "https://writersworkshop.uiowa.edu/about",
        "https://writersworkshop.uiowa.edu/contact",
        "https://clas.uiowa.edu/nonfiction-writing-program/",
        "https://clas.uiowa.edu/nonfiction-writing-program/people",
        "https://clas.uiowa.edu/nonfiction-writing-program/people/faculty",
        "https://clas.uiowa.edu/nonfiction-writing-program/people/students",
        "https://clas.uiowa.edu/nonfiction-writing-program/about",
        "https://iwp.uiowa.edu/",
        "https://iwp.uiowa.edu/people",
        "https://iwp.uiowa.edu/about",
        "https://iwp.uiowa.edu/about/staff",
        "https://iwp.uiowa.edu/residency/current-residents",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        r = scrape_all_emails(url, dept, session, seen)
        results.extend(r)
        if r:
            log(f"    -> +{len(r)} emails")
        time.sleep(0.5)

    # Also try translation workshop
    translation_urls = [
        "https://translationworkshop.uiowa.edu/",
        "https://translationworkshop.uiowa.edu/people",
        "https://clas.uiowa.edu/spanish-portuguese/people",
        "https://clas.uiowa.edu/spanish-portuguese/people/graduate-students",
    ]

    for url in translation_urls:
        log(f"  Trying: {url}")
        r = scrape_all_emails(url, dept, session, seen)
        results.extend(r)
        if r:
            log(f"    -> +{len(r)} emails")
        time.sleep(0.5)

    log(f"  TOTAL {dept}: {len(results)} new")
    return results


def scrape_daily_iowan(session, seen):
    """Scrape Daily Iowan for staff emails."""
    dept = "The Daily Iowan (Student Newspaper)"
    results = []
    log(f"\n{'='*60}\nDeep Crawl: {dept}\n{'='*60}")

    urls = [
        "https://dailyiowan.com/contact/",
        "https://dailyiowan.com/staff/",
        "https://dailyiowan.com/about/",
        "https://dailyiowan.com/staff_name/di-staff/",
        "https://dailyiowan.com/staff-list/",
        "https://dailyiowan.com/editorial-board/",
        "https://dailyiowan.com/opinions/editorial/",
        "https://dailyiowan.com/advertise/",
        "https://www.dailyiowan.com/contact/",
        "https://www.dailyiowan.com/staff/",
        "https://www.dailyiowan.com/about/",
        "https://www.dailyiowan.com/advertise/",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        r = scrape_all_emails(url, dept, session, seen)
        results.extend(r)
        if r:
            log(f"    -> +{len(r)} emails")
        time.sleep(0.5)

    # Also try to find DI author pages
    log(f"  Crawling author pages...")
    soup, final = get_soup("https://dailyiowan.com/", session)
    if soup:
        author_links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            full = urljoin("https://dailyiowan.com/", href)
            if '/author/' in full and full not in author_links:
                author_links.add(full)
        log(f"    Found {len(author_links)} author links")
        for url in list(author_links)[:30]:
            r = scrape_all_emails(url, dept, session, seen)
            results.extend(r)
            time.sleep(0.3)

    log(f"  TOTAL {dept}: {len(results)} new")
    return results


def scrape_additional_athletics(session, seen):
    """Scrape additional athletics pages."""
    dept = "Hawkeyes Athletics (Staff)"
    results = []
    log(f"\n{'='*60}\nDeep Crawl: {dept}\n{'='*60}")

    # Try individual sport staff pages
    sport_urls = [
        "https://hawkeyesports.com/sports/football/coaches",
        "https://hawkeyesports.com/sports/mens-basketball/coaches",
        "https://hawkeyesports.com/sports/womens-basketball/coaches",
        "https://hawkeyesports.com/sports/baseball/coaches",
        "https://hawkeyesports.com/sports/softball/coaches",
        "https://hawkeyesports.com/sports/wrestling/coaches",
        "https://hawkeyesports.com/sports/track-and-field/coaches",
        "https://hawkeyesports.com/sports/volleyball/coaches",
        "https://hawkeyesports.com/sports/soccer/coaches",
        "https://hawkeyesports.com/sports/mens-tennis/coaches",
        "https://hawkeyesports.com/sports/womens-tennis/coaches",
        "https://hawkeyesports.com/sports/swimming-and-diving/coaches",
        "https://hawkeyesports.com/sports/rowing/coaches",
        "https://hawkeyesports.com/sports/gymnastics/coaches",
        "https://hawkeyesports.com/sports/field-hockey/coaches",
        "https://hawkeyesports.com/sports/golf/coaches",
        "https://hawkeyesports.com/sports/cross-country/coaches",
    ]

    for url in sport_urls:
        log(f"  Trying: {url}")
        r = scrape_all_emails(url, dept, session, seen)
        results.extend(r)
        if r:
            log(f"    -> +{len(r)} emails")
        time.sleep(0.3)

    log(f"  TOTAL {dept}: {len(results)} new")
    return results


def main():
    session = requests.Session()

    # Load existing
    existing = []
    existing_emails = set()
    try:
        with open(OUTPUT_CSV) as f:
            for row in csv.DictReader(f):
                existing.append(row)
                existing_emails.add(row['email'].lower().strip())
        log(f"Loaded {len(existing)} existing ({len(existing_emails)} unique)")
    except FileNotFoundError:
        log("Starting fresh")

    all_results = list(existing)
    seen = set(existing_emails)

    def add(dept_results):
        n = 0
        for r in dept_results:
            e = r['email'].lower().strip()
            if e and e not in seen:
                seen.add(e)
                all_results.append(r)
                n += 1
        return n

    log("="*70)
    log("IOWA DEEP SUPPLEMENT SCRAPER")
    log(f"Starting with {len(existing)} emails")
    log("="*70)

    # 1. College of Law
    try:
        n = add(scrape_law_deep(session, set(seen)))
        log(f"  => +{n} new (total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR Law: {e}")

    # 2. College of Public Health
    try:
        n = add(scrape_public_health_deep(session, set(seen)))
        log(f"  => +{n} new (total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR Public Health: {e}")

    # 3. Writers' Workshop
    try:
        n = add(scrape_writers_workshop(session, set(seen)))
        log(f"  => +{n} new (total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR Writers' Workshop: {e}")

    # 4. Daily Iowan
    try:
        n = add(scrape_daily_iowan(session, set(seen)))
        log(f"  => +{n} new (total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR Daily Iowan: {e}")

    # 5. Additional Athletics
    try:
        n = add(scrape_additional_athletics(session, set(seen)))
        log(f"  => +{n} new (total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR Athletics: {e}")

    # Save
    log(f"\n{'='*70}")
    log(f"TOTAL: {len(all_results)} ({len(all_results)-len(existing)} new)")
    log(f"{'='*70}")

    with open(OUTPUT_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['email','name','department','source_url'])
        w.writeheader()
        for r in sorted(all_results, key=lambda x: (x['department'], x['email'])):
            w.writerow(r)
    log(f"Saved {OUTPUT_CSV}")

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(all_results, f, indent=2)
    log(f"Saved {OUTPUT_JSON}")

    dc = {}
    for r in all_results:
        dc[r['department']] = dc.get(r['department'], 0) + 1
    log(f"\nBY DEPARTMENT:")
    for d, c in sorted(dc.items(), key=lambda x: -x[1]):
        log(f"  {d}: {c}")


if __name__ == '__main__':
    main()
