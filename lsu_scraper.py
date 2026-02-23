#!/usr/bin/env python3
"""
Louisiana State University (LSU) Email Scraper
Scrapes @lsu.edu emails from:
- Arts & Sciences (HSS + Science) graduate student directories
- Engineering department directories
- Professional school directories (Business, Law, Education, Manship)
- Athletics staff directory (lsusports.net)
- Student organizations (SGA, The Reveille)
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/lsu_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/lsu_dept_emails.json'


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_lsu_emails(text):
    """Extract all @lsu.edu emails from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*lsu\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number-prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*lsu\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract LSU emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*lsu\.edu)',
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
        'admission@', 'admissions@', 'gradschool@', 'finaid@',
        'testing@', 'counseling@', 'housing@', 'parking@', 'transit@',
        'police@', 'records@', 'bursar@', 'payroll@', 'noreply@',
        'do-not-reply@', 'donotreply@', 'enrollment@', 'lsusga@',
        'lsumanship@', 'lsulaw@', 'lsued@', 'lsueng@', 'lsubiz@',
        'dining@', 'athletics@', 'compliance@', 'sports@',
        'lsureveille@', 'gradstudies@',
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
            log(f"    HTTP {resp.status_code} for {url}")
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
                    ]):
                        return name
            parent = parent.parent

    return ""


# ============================================================
# STRUCTURED EXTRACTION: Person cards / grid layouts
# ============================================================

def extract_from_person_cards(soup, url, department):
    """Extract people from card/grid-based layouts (LSU Drupal/PHP sites)."""
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
        '.node--type-person',
        '.person-row',
        'tr',
        'li',
        '.vcard',
        '.grid-item',
        '.teaser',
        '.faculty-staff',
        '.people-listing',
        '.staff-listing',
        '.ppl-lister__card',
        '.ppl-card',
        '.people-grid__item',
        '.grad-student',
        '.listing-item',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_lsu_emails(text)
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
                                'read more', 'view profile',
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


# ============================================================
# PAGINATION HANDLING
# ============================================================

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
    return sorted(pages)


# ============================================================
# PROFILE LINK SCRAPING
# ============================================================

def find_profile_links(soup, base_url):
    """Find links to individual profile pages from a listing page."""
    profiles = []
    seen_urls = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)

        if full_url in seen_urls:
            continue
        if '#' in href and not href.startswith('http'):
            continue

        # Match profile-like URLs: /people/john-doe, /directory/john-doe
        if re.search(r'/(people|directory|grad[_-]?students?|faculty)/[\w-]+\.php$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'directory', 'about',
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

        # Also match /people/person-name style (no .php)
        if re.search(r'/(people|directory)/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'directory', 'about',
                ]):
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
    """Scrape an individual profile page for lsu.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_lsu_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# HSS = Humanities & Social Sciences
HSS_DEPARTMENTS = [
    {"department": "Economics", "urls": [
        "https://www.lsu.edu/hss/economics/people/graduate_students.php",
        "https://www.lsu.edu/hss/economics/people/",
        "https://www.lsu.edu/hss/economics/people/index.php",
    ]},
    {"department": "Political Science", "urls": [
        "https://www.lsu.edu/hss/polisci/people/graduate_students.php",
        "https://www.lsu.edu/hss/polisci/people/",
        "https://www.lsu.edu/hss/polisci/people/index.php",
    ]},
    {"department": "Sociology", "urls": [
        "https://www.lsu.edu/hss/sociology/people/graduate_students.php",
        "https://www.lsu.edu/hss/sociology/people/",
        "https://www.lsu.edu/hss/sociology/people/index.php",
    ]},
    {"department": "Psychology", "urls": [
        "https://www.lsu.edu/hss/psychology/people/graduate_students.php",
        "https://www.lsu.edu/hss/psychology/people/",
        "https://www.lsu.edu/hss/psychology/people/index.php",
    ]},
    {"department": "History", "urls": [
        "https://www.lsu.edu/hss/history/people/graduate_students.php",
        "https://www.lsu.edu/hss/history/people/",
        "https://www.lsu.edu/hss/history/people/index.php",
    ]},
    {"department": "English", "urls": [
        "https://www.lsu.edu/hss/english/people/graduate_students.php",
        "https://www.lsu.edu/hss/english/people/",
        "https://www.lsu.edu/hss/english/people/index.php",
    ]},
    {"department": "Philosophy", "urls": [
        "https://www.lsu.edu/hss/philosophy/people/graduate_students.php",
        "https://www.lsu.edu/hss/philosophy/people/",
        "https://www.lsu.edu/hss/philosophy/people/index.php",
    ]},
    {"department": "Geography", "urls": [
        "https://www.lsu.edu/hss/geography/people/graduate_students.php",
        "https://www.lsu.edu/hss/geography/people/",
        "https://www.lsu.edu/hss/geography/people/index.php",
    ]},
    {"department": "Communication", "urls": [
        "https://www.lsu.edu/hss/communication/people/graduate_students.php",
        "https://www.lsu.edu/hss/communication/people/",
        "https://www.lsu.edu/hss/communication/people/index.php",
    ]},
]

SCIENCE_DEPARTMENTS = [
    {"department": "Mathematics", "urls": [
        "https://www.lsu.edu/science/math/people/grad-students.php",
        "https://www.lsu.edu/science/math/people/",
        "https://www.lsu.edu/science/math/people/index.php",
    ]},
    {"department": "Physics", "urls": [
        "https://www.lsu.edu/science/physics/people/grad-students.php",
        "https://www.lsu.edu/science/physics/people/",
        "https://www.lsu.edu/science/physics/people/index.php",
    ]},
    {"department": "Chemistry", "urls": [
        "https://www.lsu.edu/science/chemistry/people/grad-students.php",
        "https://www.lsu.edu/science/chemistry/people/",
        "https://www.lsu.edu/science/chemistry/people/index.php",
    ]},
    {"department": "Geology & Geophysics", "urls": [
        "https://www.lsu.edu/science/geology/people/grad-students.php",
        "https://www.lsu.edu/science/geology/people/",
        "https://www.lsu.edu/science/geology/people/index.php",
    ]},
    {"department": "Biological Sciences", "urls": [
        "https://www.lsu.edu/science/biosci/people/grad-students.php",
        "https://www.lsu.edu/science/biosci/people/",
        "https://www.lsu.edu/science/biosci/people/index.php",
    ]},
]

ENGINEERING_DEPARTMENTS = [
    {"department": "Computer Science & Engineering", "urls": [
        "https://www.lsu.edu/eng/cse/people/grad-students.php",
        "https://www.lsu.edu/eng/cse/people/",
        "https://www.lsu.edu/eng/cse/people/index.php",
    ]},
    {"department": "Electrical & Computer Engineering", "urls": [
        "https://www.lsu.edu/eng/ece/people/grad-students.php",
        "https://www.lsu.edu/eng/ece/people/",
        "https://www.lsu.edu/eng/ece/people/index.php",
    ]},
    {"department": "Mechanical & Industrial Engineering", "urls": [
        "https://www.lsu.edu/eng/mie/people/grad-students.php",
        "https://www.lsu.edu/eng/mie/people/",
        "https://www.lsu.edu/eng/mie/people/index.php",
    ]},
    {"department": "Civil & Environmental Engineering", "urls": [
        "https://www.lsu.edu/eng/civil/people/grad-students.php",
        "https://www.lsu.edu/eng/civil/people/",
        "https://www.lsu.edu/eng/civil/people/index.php",
    ]},
    {"department": "Chemical Engineering", "urls": [
        "https://www.lsu.edu/eng/che/people/grad-students.php",
        "https://www.lsu.edu/eng/che/people/",
        "https://www.lsu.edu/eng/che/people/index.php",
    ]},
]

PROFESSIONAL_SCHOOLS = [
    {"department": "E.J. Ourso College of Business (PhD)", "urls": [
        "https://business.lsu.edu/phd",
        "https://business.lsu.edu/phd/",
        "https://business.lsu.edu/phd/students",
        "https://business.lsu.edu/phd/current-students",
        "https://business.lsu.edu/faculty-and-research/",
        "https://business.lsu.edu/people/",
        "https://business.lsu.edu/directory/",
    ]},
    {"department": "Paul M. Hebert Law Center", "urls": [
        "https://law.lsu.edu/",
        "https://law.lsu.edu/students/",
        "https://law.lsu.edu/student-organizations/",
        "https://law.lsu.edu/directory/",
        "https://law.lsu.edu/faculty/",
        "https://law.lsu.edu/about/",
    ]},
    {"department": "College of Education", "urls": [
        "https://education.lsu.edu/people/students",
        "https://education.lsu.edu/people/",
        "https://education.lsu.edu/directory/",
        "https://www.lsu.edu/education/people/",
    ]},
    {"department": "Manship School of Mass Communication", "urls": [
        "https://www.lsu.edu/manship/",
        "https://www.lsu.edu/manship/people/",
        "https://www.lsu.edu/manship/people/graduate-students.php",
        "https://www.lsu.edu/manship/people/graduate_students.php",
        "https://www.lsu.edu/manship/people/index.php",
        "https://www.lsu.edu/manship/directory/",
    ]},
]

ATHLETICS = [
    {"department": "Athletics (Staff)", "urls": [
        "https://lsusports.net/staff-directory/",
        "https://lsusports.net/staff/",
    ]},
]

STUDENT_ORGS = [
    {"department": "Student Government", "urls": [
        "https://www.lsu.edu/sg/",
        "https://www.lsu.edu/sg/about/",
        "https://www.lsu.edu/sg/branches/executive/",
        "https://www.lsu.edu/sg/branches/",
        "https://www.lsu.edu/sg/officers/",
        "https://www.lsu.edu/sg/leadership/",
        "https://www.lsu.edu/sg/contact/",
    ]},
    {"department": "The Reveille (Student Newspaper)", "urls": [
        "https://www.lsureveille.com/staff/",
        "https://www.lsureveille.com/contact/",
        "https://www.lsureveille.com/about/",
        "https://lsureveille.com/staff/",
        "https://lsureveille.com/contact/",
    ]},
]


# ============================================================
# MAIN DEPARTMENT SCRAPER
# ============================================================

def scrape_department(config, session):
    """Scrape a single department, trying multiple URLs and strategies."""
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
        successful_url = final_url or url

        # Strategy 1: Extract from structured cards
        card_results = extract_from_person_cards(soup, successful_url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_lsu_emails(page_text)
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
                    'source_url': successful_url,
                })

        # Strategy 3: Obfuscated emails
        obfuscated = re.findall(
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*lsu\.edu)',
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
                    'source_url': successful_url,
                })

        # Strategy 4: JavaScript-embedded emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_lsu_emails(script.string)
                for email in script_emails:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': department,
                            'source_url': successful_url,
                        })

        # Strategy 5: Follow pagination
        pagination_urls = find_pagination_urls(soup, successful_url)
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
                pg_text_emails = extract_lsu_emails(pg_text)
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

        # Strategy 6: If few emails, try visiting profile links
        if len(results) < 3:
            profiles = find_profile_links(soup, successful_url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting for emails...")
                for profile in profiles[:50]:
                    pname = profile['name']
                    purl = profile['profile_url']
                    email = scrape_profile_page(purl, session, department)
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
# ATHLETICS SCRAPER (lsusports.net)
# ============================================================

def scrape_athletics(session):
    """Scrape LSU Tigers athletics staff directory for @lsu.edu emails."""
    results = []
    seen_emails = set()
    department = "Athletics (Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://lsusports.net/staff-directory/",
        "https://lsusports.net/staff/",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_lsu_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} raw @lsu.edu emails on page")

        # Extract from structured cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails and not is_admin_email(r['email']):
                seen_emails.add(r['email'])
                results.append(r)

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

        # Check for paginated staff lists
        pagination_urls = find_pagination_urls(soup, final_url or url)
        for page_url in pagination_urls:
            log(f"    Paginated athletics: {page_url}")
            page_soup, _ = get_soup(page_url, session)
            if page_soup:
                pg_text = page_soup.get_text(separator=' ', strip=True)
                pg_emails = extract_lsu_emails(pg_text)
                pg_mailto = extract_mailto_emails(page_soup)
                for email in list(set(pg_emails + pg_mailto)):
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

        # Follow internal staff links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if ('staff' in href.lower() or 'directory' in href.lower()) and full_url != url:
                if 'lsusports.net' in full_url or 'lsu.edu' in full_url:
                    log(f"    Following staff link: {full_url}")
                    sub_soup, sub_url = get_soup(full_url, session)
                    if sub_soup:
                        sub_text = sub_soup.get_text(separator=' ', strip=True)
                        sub_emails = extract_lsu_emails(sub_text)
                        sub_mailto = extract_mailto_emails(sub_soup)
                        for email in list(set(sub_emails + sub_mailto)):
                            if email not in seen_emails and not is_admin_email(email):
                                seen_emails.add(email)
                                name = try_get_name_for_email(sub_soup, email)
                                results.append({
                                    'email': email,
                                    'name': name,
                                    'department': department,
                                    'source_url': sub_url or full_url,
                                })
                    time.sleep(0.3)

        if results:
            break
        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_results = []
    global_seen_emails = set()

    def add_results(dept_results):
        count = 0
        for r in dept_results:
            email = r['email'].lower().strip()
            if email and email not in global_seen_emails:
                global_seen_emails.add(email)
                all_results.append(r)
                count += 1
        return count

    log("=" * 70)
    log("LOUISIANA STATE UNIVERSITY (LSU) EMAIL SCRAPER")
    log("Domain: @lsu.edu")
    log("=" * 70)

    # ---- Phase 1: HSS (Humanities & Social Sciences) ----
    log("\n\nPHASE 1: HUMANITIES & SOCIAL SCIENCES (HSS)")
    log("=" * 70)

    for config in HSS_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: Science ----
    log("\n\nPHASE 2: COLLEGE OF SCIENCE")
    log("=" * 70)

    for config in SCIENCE_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 3: Engineering ----
    log("\n\nPHASE 3: COLLEGE OF ENGINEERING")
    log("=" * 70)

    for config in ENGINEERING_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 4: Professional Schools ----
    log("\n\nPHASE 4: PROFESSIONAL SCHOOLS")
    log("=" * 70)

    for config in PROFESSIONAL_SCHOOLS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 5: Athletics ----
    log("\n\nPHASE 5: ATHLETICS")
    log("=" * 70)

    try:
        results = scrape_athletics(session)
        n = add_results(results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 6: Student Organizations ----
    log("\n\nPHASE 6: STUDENT ORGANIZATIONS")
    log("=" * 70)

    for config in STUDENT_ORGS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @lsu.edu emails: {len(all_results)}")

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

    all_depts = set(
        c['department'] for group in [
            HSS_DEPARTMENTS, SCIENCE_DEPARTMENTS, ENGINEERING_DEPARTMENTS,
            PROFESSIONAL_SCHOOLS, ATHLETICS, STUDENT_ORGS,
        ]
        for c in group
    )
    depts_with_zero = all_depts - set(dept_counts.keys())
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in sorted(depts_with_zero):
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
