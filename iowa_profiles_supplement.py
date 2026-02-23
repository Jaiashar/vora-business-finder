#!/usr/bin/env python3
"""
Iowa Profiles API Supplement Scraper
Uses the profiles.uiowa.edu API to get emails from:
- Tippie College of Business (Economics, PhD students, etc.)
- College of Education
- College of Pharmacy

Also re-scrapes remaining weak departments.
Merges into existing iowa_dept_emails.csv.
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
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
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
    'law-','uiowa-',
]

def is_admin(email):
    el = email.lower()
    return any(el.startswith(p) for p in ADMIN_PREFIXES)


# ============================================================
# PROFILES API SCRAPER
# ============================================================

PROFILES_API = 'https://profiles.uiowa.edu/api'

PROFILE_CONFIGS = [
    {
        'api_key': 'ebab7021-4a40-45b5-83d5-26922855e68d',
        'site_name': 'Tippie College of Business',
        'department_map': {
            'Economics': 'Economics',
            'Tippie College of Business': 'Tippie College of Business (PhD)',
        },
        'type_filter': None,  # Get all types initially
    },
    {
        'api_key': '0f7ad038-cfa6-406d-ab27-c572dc435d59',
        'site_name': 'College of Education',
        'department_map': {
            None: 'College of Education',
        },
        'type_filter': None,
    },
    {
        'api_key': '4c2067bf-ed5b-47c5-a059-567709e3cf86',
        'site_name': 'College of Pharmacy',
        'department_map': {
            None: 'College of Pharmacy',
        },
        'type_filter': None,
    },
]


def scrape_profiles_api(config):
    """Scrape a profiles.uiowa.edu API endpoint."""
    api_key = config['api_key']
    site_name = config['site_name']
    dept_map = config['department_map']
    results = []
    seen_emails = set()

    log(f"\n{'='*60}")
    log(f"Profiles API: {site_name}")
    log(f"{'='*60}")

    # Get first page to determine total
    url = f"{PROFILES_API}/people?apiKey={api_key}&page=0&size=100"
    try:
        resp = requests.get(url, headers={'Accept': 'application/json'}, timeout=15)
        data = resp.json()
    except Exception as e:
        log(f"  Error: {e}")
        return results

    total = data['pager']['totalSize']
    pages_needed = (total + 99) // 100
    all_people = list(data.get('results', []))

    log(f"  Total profiles: {total}, pages: {pages_needed}")

    # Fetch remaining pages
    for pg in range(1, pages_needed):
        url = f"{PROFILES_API}/people?apiKey={api_key}&page={pg}&size=100"
        try:
            resp = requests.get(url, headers={'Accept': 'application/json'}, timeout=15)
            page_data = resp.json()
            all_people.extend(page_data.get('results', []))
        except Exception as e:
            log(f"  Error on page {pg}: {e}")
        time.sleep(0.3)

    log(f"  Fetched {len(all_people)} profiles")

    # Process people
    for person in all_people:
        email = person.get('email', '').lower().strip()
        if not email or 'uiowa.edu' not in email:
            continue
        if email in seen_emails or is_admin(email):
            continue

        seen_emails.add(email)
        first = person.get('firstName', '')
        last = person.get('lastName', '')
        name = f"{first} {last}".strip()
        cohort_name = person.get('cohortName', '')
        person_type = person.get('personType', '')

        # Map to department
        department = None
        for key, dept in dept_map.items():
            if key is None:
                department = dept
                break
            if key.lower() in cohort_name.lower():
                department = dept
                break

        if department is None:
            # Default to site name
            department = site_name

        # For Tippie, separate Economics from general
        if site_name == 'Tippie College of Business':
            if 'economics' in cohort_name.lower() or cohort_name == 'Economics':
                department = 'Economics'
            else:
                department = 'Tippie College of Business (PhD)'

        source_url = f"https://profiles.uiowa.edu/api/people?apiKey={api_key}"

        results.append({
            'email': email,
            'name': name,
            'department': department,
            'source_url': source_url,
        })

    # Summary
    dept_counts = {}
    for r in results:
        d = r['department']
        dept_counts[d] = dept_counts.get(d, 0) + 1

    log(f"  Extracted {len(results)} @uiowa.edu emails:")
    for d, c in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"    {d}: {c}")

    return results


# ============================================================
# STANDARD HTML SCRAPING FOR REMAINING DEPTS
# ============================================================

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
            if m:
                emails.append(m.group(1).lower().strip())
    return list(set(emails))

def get_soup(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code == 200:
            return BeautifulSoup(r.text, 'html.parser'), r.url
        return None, None
    except:
        return None, None

SKIP = ['email','contact','phone','http','department','graduate','student',
        'people','faculty','office','read more','view profile','website',
        'lab','research','iowa','uiowa','university']

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
    return ""

def scrape_html_urls(dept, urls, session):
    """Scrape a list of URLs for @uiowa.edu emails."""
    results = []
    seen = set()

    log(f"\n{'='*60}")
    log(f"HTML Scrape: {dept}")
    log(f"{'='*60}")

    for url in urls:
        log(f"  Trying: {url}")
        soup, final = get_soup(url, session)
        if not soup:
            log(f"    -> Failed")
            time.sleep(0.5)
            continue

        src = final or url
        text = soup.get_text(separator=' ', strip=True)
        emails = list(set(extract_uiowa_emails(text) + extract_mailto(soup)))

        for email in emails:
            if email not in seen and not is_admin(email):
                seen.add(email)
                name = get_name(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': dept,
                    'source_url': src,
                })

        # Check pagination
        for a in soup.find_all('a', href=True):
            full = urljoin(src, a['href'])
            if re.search(r'[?&]page=\d+', full):
                log(f"    Paginated: {full}")
                ps, _ = get_soup(full, session)
                if ps:
                    pg_text = ps.get_text(separator=' ', strip=True)
                    for email in set(extract_uiowa_emails(pg_text) + extract_mailto(ps)):
                        if email not in seen and not is_admin(email):
                            seen.add(email)
                            results.append({
                                'email': email,
                                'name': get_name(ps, email),
                                'department': dept,
                                'source_url': full,
                            })
                time.sleep(0.5)

        if results:
            log(f"    -> {len(results)} emails so far")
        time.sleep(0.5)

    log(f"  TOTAL {dept}: {len(results)}")
    return results


# ============================================================
# MAIN
# ============================================================

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
    log("IOWA PROFILES API SUPPLEMENT SCRAPER")
    log(f"Starting with {len(existing)} emails")
    log("="*70)

    # Phase 1: Profiles API
    log("\n\nPHASE 1: PROFILES API")
    log("="*70)

    for config in PROFILE_CONFIGS:
        try:
            results = scrape_profiles_api(config)
            n = add(results)
            log(f"  => +{n} new (total: {len(all_results)})")
        except Exception as e:
            log(f"  ERROR {config['site_name']}: {e}")
        time.sleep(1)

    # Phase 2: Remaining weak departments via HTML
    log("\n\nPHASE 2: HTML SCRAPING FOR REMAINING DEPTS")
    log("="*70)

    weak_depts = [
        ("College of Public Health", [
            "https://www.public-health.uiowa.edu/people/",
            "https://www.public-health.uiowa.edu/directory/",
            "https://www.public-health.uiowa.edu/",
            "https://cph.uiowa.edu/people",
            "https://cph.uiowa.edu/directory",
        ]),
        ("College of Law", [
            "https://law.uiowa.edu/student-organizations",
            "https://law.uiowa.edu/about/people",
            "https://law.uiowa.edu/people",
            "https://law.uiowa.edu/directory",
            "https://law.uiowa.edu/",
        ]),
        ("Iowa Writers' Workshop / Nonfiction Writing Program", [
            "https://writersworkshop.uiowa.edu/",
            "https://writersworkshop.uiowa.edu/people",
            "https://writersworkshop.uiowa.edu/people/students",
            "https://clas.uiowa.edu/nonfiction-writing-program/",
            "https://clas.uiowa.edu/nonfiction-writing-program/people",
        ]),
        ("Geography & Sustainability Sciences", [
            "https://sees.uiowa.edu/people",
            "https://sees.uiowa.edu/people?page=0",
            "https://sees.uiowa.edu/people?page=1",
            "https://sees.uiowa.edu/people?page=2",
            "https://sees.uiowa.edu/people?page=3",
            "https://sees.uiowa.edu/people?page=4",
        ]),
    ]

    for dept, urls in weak_depts:
        try:
            results = scrape_html_urls(dept, urls, session)
            n = add(results)
            log(f"  => +{n} new (total: {len(all_results)})")
        except Exception as e:
            log(f"  ERROR {dept}: {e}")
        time.sleep(1)

    # Save
    log(f"\n\n{'='*70}")
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

    # Summary
    dc = {}
    for r in all_results:
        dc[r['department']] = dc.get(r['department'], 0) + 1
    log(f"\nBY DEPARTMENT:")
    for d, c in sorted(dc.items(), key=lambda x: -x[1]):
        log(f"  {d}: {c}")


if __name__ == '__main__':
    main()
