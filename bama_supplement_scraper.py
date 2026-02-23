#!/usr/bin/env python3
"""
University of Alabama - Supplemental Scraper
Covers departments that failed in the initial run due to incorrect URL patterns.
UA uses varied URL structures across departments:
- Some use /graduate-student-directory/
- Some use /contact-us/graduate-students/
- Some use /faculty-and-staff-directory/graduate-students/
- Engineering lives under *.eng.ua.edu
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
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/bama_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/bama_dept_emails.json'

# Load existing results to merge
EXISTING_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/bama_dept_emails.csv'


# ============================================================
# CORRECTED DEPARTMENT CONFIGURATIONS
# ============================================================

# Arts & Sciences departments that failed with wrong URLs
CORRECTED_ARTS_SCIENCES = [
    # economics.ua.edu DNS doesn't resolve - skip economics (already under Culverhouse)
    {"department": "Political Science", "urls": [
        "https://psc.ua.edu/contact/current-graduate-students/",
        "https://psc.ua.edu/contact/directory/",
        "https://psc.ua.edu/contact/staff-directory/",
    ]},
    # sociology.ua.edu DNS doesn't resolve - skip
    {"department": "Psychology", "urls": [
        "https://psychology.ua.edu/graduate-student-directory/",
        "https://psychology.ua.edu/faculty-and-staff-directory/",
        "https://psychology.ua.edu/staff-directory/",
    ]},
    {"department": "History", "urls": [
        "https://history.ua.edu/contact-us/graduate-students/",
        "https://history.ua.edu/contact-us/faculty-and-staff-directory/",
    ]},
    {"department": "English", "urls": [
        "https://english.ua.edu/faculty-and-staff-directory/graduate-students/",
        "https://english.ua.edu/faculty-and-staff-directory/",
    ]},
    {"department": "Mathematics", "urls": [
        "https://math.ua.edu/graduate-program/graduate-student-directory/",
        "https://math.ua.edu/faculty-staff/",
    ]},
    {"department": "Physics", "urls": [
        "https://physics.ua.edu/graduate-directory/",
        "https://physics.ua.edu/department-directory/",
    ]},
    {"department": "Chemistry", "urls": [
        "https://chemistry.ua.edu/graduate-student-directory/",
        "https://chemistry.ua.edu/faculty-and-staff/",
    ]},
]

# Engineering departments under eng.ua.edu
CORRECTED_ENGINEERING = [
    {"department": "Electrical & Computer Engineering", "urls": [
        "https://ece.eng.ua.edu/faculty-staff/",
        "https://ece.eng.ua.edu/graduate/",
        "https://ece.eng.ua.edu/",
    ]},
    {"department": "Mechanical Engineering", "urls": [
        "https://me.eng.ua.edu/faculty-staff/",
        "https://me.eng.ua.edu/graduate/",
        "https://me.eng.ua.edu/",
    ]},
    {"department": "Civil, Construction & Environmental Engineering", "urls": [
        "https://cce.eng.ua.edu/faculty-staff/",
        "https://cce.eng.ua.edu/graduate/",
        "https://cce.eng.ua.edu/",
    ]},
    {"department": "Chemical & Biological Engineering", "urls": [
        "https://che.eng.ua.edu/faculty-staff/",
        "https://che.eng.ua.edu/graduate/",
        "https://che.eng.ua.edu/",
    ]},
    {"department": "Aerospace Engineering", "urls": [
        "https://aem.eng.ua.edu/faculty-staff/",
        "https://aem.eng.ua.edu/graduate/",
        "https://aem.eng.ua.edu/",
    ]},
    {"department": "Metallurgical & Materials Engineering", "urls": [
        "https://mte.eng.ua.edu/faculty-staff/",
        "https://mte.eng.ua.edu/graduate/",
        "https://mte.eng.ua.edu/",
    ]},
    {"department": "College of Engineering (Directory)", "urls": [
        "https://eng.ua.edu/faculty-staff/directory/",
        "https://eng.ua.edu/faculty-staff/",
    ]},
]

# Additional professional school pages
CORRECTED_PROFESSIONAL = [
    {"department": "Capstone College of Nursing", "urls": [
        "https://nursing.ua.edu/about-us/directory/",
        "https://nursing.ua.edu/about-us/faculty-divisions/",
        "https://nursing.ua.edu/contact-us/",
        "https://nursing.ua.edu/",
    ]},
    {"department": "School of Social Work", "urls": [
        "https://socialwork.ua.edu/about/contact-us/",
        "https://socialwork.ua.edu/faculty-staff-links/",
        "https://socialwork.ua.edu/",
    ]},
]


# ============================================================
# EMAIL EXTRACTION
# ============================================================

def extract_ua_emails(text):
    """Extract all @ua.edu and @crimson.ua.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:crimson\.)?ua\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:crimson\.)?ua\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract UA emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:crimson\.)?ua\.edu)',
                href, re.IGNORECASE
            )
            if match:
                emails.append(match.group(1).lower().strip())
    return list(set(emails))


def is_admin_email(email):
    """Filter out department/admin/generic emails."""
    admin_patterns = [
        'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
        'support@', 'contact@', 'registrar@', 'grad@', 'gradoffice@',
        'department@', 'chair@', 'advising@', 'undergrad@', 'dean@',
        'reception@', 'main@', 'general@', 'staff@', 'gradadmit@',
        'calendar@', 'events@', 'news@', 'newsletter@', 'web@',
        'marketing@', 'media@', 'communications@', 'hr@', 'hiring@',
        'jobs@', 'career@', 'alumni@', 'development@', 'giving@',
        'feedback@', 'safety@', 'security@', 'facilities@', 'it@',
        'tech@', 'helpdesk@', 'library@', 'gradapp@', 'apply@',
        'admissions@', 'enrollment@', 'records@', 'bursar@',
        'finaid@', 'housing@', 'dining@', 'parking@', 'police@',
        'noreply@', 'do-not-reply@', 'donotreply@',
        'uaadmissions@', 'uanews@', 'uapress@',
        'oira@', 'provost@', 'president@', 'chancellor@',
        'graduate@', 'testing@', 'counseling@', 'transit@',
        'engagement@', 'research@', 'compliance@', 'title-ix@',
        'sgawebmaster@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


def get_soup(url, session):
    """Fetch a page and return BeautifulSoup object."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        else:
            log(f"    HTTP {resp.status_code}")
            return None, None
    except Exception as e:
        log(f"    Error fetching {url}: {e}")
        return None, None


# ============================================================
# NAME EXTRACTION
# ============================================================

def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email on the page."""
    # Strategy 1: mailto link parent traversal
    for a_tag in soup.find_all('a', href=True):
        if email in a_tag.get('href', '').lower():
            parent = a_tag.parent
            for _ in range(6):
                if parent is None:
                    break
                for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a']):
                    tag_text = tag.get_text(strip=True)
                    if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                        if not any(x in tag_text.lower() for x in [
                            'email', 'contact', 'phone', 'http', 'department',
                            'graduate', 'student', 'people', 'faculty', 'office',
                            'read more', 'view profile', 'website', 'lab',
                            'skip to content', 'accessibility',
                        ]):
                            return tag_text
                parent = parent.parent

    # Strategy 2: Find email as visible text and look for nearby heading
    email_elements = soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE))
    for elem in email_elements:
        parent = elem.parent
        for _ in range(6):
            if parent is None:
                break
            name_tags = parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a'])
            for tag in name_tags:
                name = tag.get_text(strip=True)
                if name and '@' not in name and len(name) > 2 and len(name) < 80:
                    if not any(x in name.lower() for x in [
                        'email', 'contact', '@', 'student', 'people', 'phone',
                        'read more', 'department', 'faculty', 'office', 'http',
                        'skip to content', 'accessibility',
                    ]):
                        return name
            parent = parent.parent

    return ""


# ============================================================
# STRUCTURED EXTRACTION
# ============================================================

def extract_from_person_cards(soup, url, department):
    """Extract people from card/grid-based layouts."""
    results = []
    seen_emails = set()

    person_selectors = [
        '.views-row',
        '.view-content .views-row',
        '[class*="person"]',
        '[class*="profile"]',
        '[class*="people"]',
        '[class*="member"]',
        '[class*="student"]',
        '[class*="card"]',
        '[class*="directory"]',
        '.field-content',
        'article',
        '.person-row',
        'tr',
        '.vcard',
        '.wp-block-column',
        '.entry-content .wp-block-group',
        '[class*="grid-item"]',
        '[class*="team-member"]',
        '[class*="faculty"]',
        '[class*="hrz"]',
        '.hrz-directory-row',
        '.hrz-d-card',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_ua_emails(text)
                mailto_emails = extract_mailto_emails(card)
                all_emails = list(set(emails + mailto_emails))

                for email in all_emails:
                    if email in seen_emails or is_admin_email(email):
                        continue
                    seen_emails.add(email)

                    name = ""
                    for tag in card.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a']):
                        tag_text = tag.get_text(strip=True)
                        if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                            if not any(x in tag_text.lower() for x in [
                                'email', 'contact', 'phone', 'http', 'department',
                                'graduate', 'student', 'people', 'faculty', 'office',
                                'read more', 'view profile', 'skip to content',
                                'accessibility',
                            ]):
                                name = tag_text
                                break

                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': url,
                    })
        except Exception:
            continue

    return results


def find_pagination_urls(soup, base_url):
    """Find paginated page URLs."""
    pages = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)
        if re.search(r'[?&]page=\d+', full_url):
            pages.add(full_url)
        elif re.search(r'/page/\d+/?$', full_url):
            pages.add(full_url)
        elif re.search(r'[?&]sf_paged=\d+', full_url):
            pages.add(full_url)
    return sorted(pages)


def find_profile_links(soup, base_url):
    """Find links to individual profile pages."""
    profiles = []
    seen_urls = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)

        if full_url in seen_urls:
            continue
        if '#' in href and not href.startswith('http'):
            continue

        # Match profile-like URLs on ua.edu
        if re.search(r'/(people|directory|eng-directory|contact-us)/[\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'administration',
                    'directory', 'about', 'contact', 'staff',
                    'edit', 'request', 'skip', 'accessibility',
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session):
    """Scrape an individual profile page for UA email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_ua_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


# ============================================================
# MAIN DEPARTMENT SCRAPER
# ============================================================

def scrape_department(config, session):
    """Scrape a single department."""
    department = config['department']
    urls = config['urls']
    results = []
    seen_emails = set()
    all_pages_scraped = set()

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    for url in urls:
        if url in all_pages_scraped:
            continue

        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)

        if soup is None:
            log(f"    -> Failed")
            time.sleep(0.5)
            continue

        all_pages_scraped.add(url)
        if final_url:
            all_pages_scraped.add(final_url)

        page_text = soup.get_text(separator=' ', strip=True)
        log(f"    -> Page loaded (final URL: {final_url})")

        # Strategy 1: Extract from structured cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_ua_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': final_url or url,
                })

        # Strategy 3: Obfuscated emails
        obfuscated = re.findall(
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:crimson\.)?ua\.edu)',
            page_text, re.IGNORECASE
        )
        for prefix, domain in obfuscated:
            email = f"{prefix}@{domain}".lower()
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': '',
                    'department': department,
                    'source_url': final_url or url,
                })

        # Strategy 4: JavaScript emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_ua_emails(script.string)
                for email in script_emails:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': department,
                            'source_url': final_url or url,
                        })

        # Strategy 5: Pagination
        pagination_urls = find_pagination_urls(soup, final_url or url)
        if pagination_urls:
            log(f"    -> Found {len(pagination_urls)} additional pages")
            for page_url in pagination_urls:
                if page_url in all_pages_scraped:
                    continue
                all_pages_scraped.add(page_url)
                log(f"    Paginated: {page_url}")
                page_soup, page_final = get_soup(page_url, session)
                if page_soup is None:
                    continue

                pg_text = page_soup.get_text(separator=' ', strip=True)
                pg_text_emails = extract_ua_emails(pg_text)
                pg_mailto = extract_mailto_emails(page_soup)
                pg_all = list(set(pg_text_emails + pg_mailto))

                pg_cards = extract_from_person_cards(page_soup, page_url, department)
                for r in pg_cards:
                    if r['email'] not in seen_emails:
                        seen_emails.add(r['email'])
                        results.append(r)

                for email in pg_all:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        name = try_get_name_for_email(page_soup, email)
                        results.append({
                            'email': email,
                            'name': name,
                            'department': department,
                            'source_url': page_url,
                        })

                time.sleep(0.5)

        # Strategy 6: Profile links if few emails
        if len(results) < 3:
            profiles = find_profile_links(soup, final_url or url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting...")
                for profile in profiles[:60]:
                    pname = profile['name']
                    purl = profile['profile_url']
                    email = scrape_profile_page(purl, session)
                    if email and email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': pname,
                            'department': department,
                            'source_url': purl,
                        })
                    time.sleep(0.3)

        if len(results) > 0:
            log(f"    -> Found {len(results)} emails so far")

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    for r in results:
        log(f"    {r['email']:<45} | {r['name']}")

    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()

    # Load existing results
    existing_results = []
    existing_emails = set()
    try:
        with open(EXISTING_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_results.append(row)
                existing_emails.add(row['email'].lower().strip())
        log(f"Loaded {len(existing_results)} existing results ({len(existing_emails)} unique emails)")
    except Exception as e:
        log(f"No existing results to load: {e}")

    all_results = list(existing_results)
    global_seen_emails = set(existing_emails)

    def add_results(results):
        count = 0
        for r in results:
            email = r['email'].lower().strip()
            if email and email not in global_seen_emails:
                global_seen_emails.add(email)
                all_results.append(r)
                count += 1
        return count

    log("=" * 70)
    log("UNIVERSITY OF ALABAMA - SUPPLEMENTAL SCRAPER")
    log("Corrected URLs for zero-result departments")
    log("=" * 70)

    # ---- Corrected Arts & Sciences ----
    log("\n\nPHASE 1: CORRECTED ARTS & SCIENCES")
    log("=" * 70)

    for config in CORRECTED_ARTS_SCIENCES:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Corrected Engineering ----
    log("\n\nPHASE 2: CORRECTED ENGINEERING (eng.ua.edu)")
    log("=" * 70)

    for config in CORRECTED_ENGINEERING:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Corrected Professional Schools ----
    log("\n\nPHASE 3: CORRECTED PROFESSIONAL SCHOOLS")
    log("=" * 70)

    for config in CORRECTED_PROFESSIONAL:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Save merged results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique UA emails: {len(all_results)}")

    # Save CSV
    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: (x['department'], x['email'])):
            writer.writerow(r)
    log(f"\nSaved to {OUTPUT_CSV}")

    # Save JSON
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(all_results, f, indent=2)
    log(f"Saved to {OUTPUT_JSON}")

    # Summary by department
    log(f"\n{'=' * 70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")

    return all_results


if __name__ == '__main__':
    main()
