#!/usr/bin/env python3
"""
Auburn University Email Scraper
Scrapes @auburn.edu emails from:
- COSAM (College of Sciences and Mathematics) graduate student directories
- CLA (College of Liberal Arts) graduate student directories
- Samuel Ginn College of Engineering graduate student directories
- Professional school directories (Harbert Business, Education, Pharmacy, Nursing, Vet Med)
- Athletics staff directory (auburntigers.com)
- Student organizations (SGA, Auburn Plainsman)

Auburn uses several CMS patterns:
  - COSAM: auburn.edu/cosam/departments/DEPT/people/grad-students.htm
  - CLA: cla.auburn.edu/DEPT/people/graduate-students/
  - Engineering: eng.auburn.edu/DEPT/people/graduate-students.html
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/auburn_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/auburn_dept_emails.json'


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# COSAM - College of Sciences and Mathematics
# NOTE: COSAM has a custom 404 that returns HTTP 200 with title "Page Not Found"
# Correct URLs found by probing the actual site structure.
COSAM_DEPARTMENTS = [
    {"department": "Chemistry (COSAM)", "urls": [
        "https://www.auburn.edu/cosam/departments/chemistry/people/Graduate-Students.htm",
        "https://www.auburn.edu/cosam/departments/chemistry/people/index.htm",
    ]},
    {"department": "Physics (COSAM)", "urls": [
        # No dedicated grad student page found; faculty page has emails
        "https://www.auburn.edu/cosam/departments/physics/physics-faculty/index.htm",
    ]},
    {"department": "Mathematics & Statistics (COSAM)", "urls": [
        "https://www.auburn.edu/cosam/departments/math/students/grad/graduate-students.htm",
    ]},
    {"department": "Biological Sciences (COSAM)", "urls": [
        # Biology grad page only has a generic email; try faculty page
        "https://www.auburn.edu/cosam/departments/biology/graduate-student-current.htm",
        "https://www.auburn.edu/cosam/faculty/biology/index.htm",
    ]},
    {"department": "Geosciences (COSAM)", "urls": [
        "https://www.auburn.edu/cosam/departments/geosciences/geosciences-students/all_grads.htm",
    ]},
]

# CLA - College of Liberal Arts
# NOTE: CLA uses /our-people/ and Umbraco-style CMS; some depts renamed.
# Sociology -> sociology-anthropology-social-work; Psychology -> psychological-sciences
# Communication -> cmjn (Communication, Media, Journalism & Narrative)
CLA_DEPARTMENTS = [
    {"department": "Economics (CLA)", "urls": [
        "https://cla.auburn.edu/economics/graduate-program/phd-student-directory/",
        "https://cla.auburn.edu/economics/our-people/",
        "https://cla.auburn.edu/economics/graduate-program/",
        "https://cla.auburn.edu/economics/",
    ]},
    {"department": "Political Science (CLA)", "urls": [
        "https://cla.auburn.edu/polisci/our-people/",
        "https://cla.auburn.edu/polisci/our-people/graduate-students/",
        "https://cla.auburn.edu/polisci/",
    ]},
    {"department": "Sociology / Anthropology / Social Work (CLA)", "urls": [
        "https://cla.auburn.edu/sociology-anthropology-social-work/our-people/",
        "https://cla.auburn.edu/sociology-anthropology-social-work/sociology/graduate-degrees/",
        "https://cla.auburn.edu/sociology-anthropology-social-work/",
    ]},
    {"department": "Psychological Sciences (CLA)", "urls": [
        "https://cla.auburn.edu/psychological-sciences/our-people/",
        "https://cla.auburn.edu/psychological-sciences/graduate-studies/",
        "https://cla.auburn.edu/psychological-sciences/",
    ]},
    {"department": "History (CLA)", "urls": [
        "https://cla.auburn.edu/history/our-people/",
        "https://cla.auburn.edu/history/current-students/graduate/",
        "https://cla.auburn.edu/history/",
    ]},
    {"department": "English (CLA)", "urls": [
        "https://cla.auburn.edu/english/our-people/",
        "https://cla.auburn.edu/english/current-students/graduate/",
        "https://cla.auburn.edu/english/",
    ]},
    {"department": "Philosophy (CLA)", "urls": [
        "https://cla.auburn.edu/philosophy/our-people/",
        "https://cla.auburn.edu/philosophy/",
    ]},
    {"department": "Communication / Media / Journalism (CLA)", "urls": [
        "https://cla.auburn.edu/cmjn/our-people/",
        "https://cla.auburn.edu/cmjn/",
    ]},
]

# Samuel Ginn College of Engineering
ENGINEERING_DEPARTMENTS = [
    {"department": "Computer Science & Software Engineering", "urls": [
        "https://eng.auburn.edu/csse/people/graduate-students.html",
        "https://eng.auburn.edu/csse/people/graduate-students",
        "https://eng.auburn.edu/csse/people/",
        "https://www.eng.auburn.edu/csse/people/graduate-students.html",
    ]},
    {"department": "Electrical & Computer Engineering", "urls": [
        "https://eng.auburn.edu/ece/people/graduate-students.html",
        "https://eng.auburn.edu/ece/people/graduate-students",
        "https://eng.auburn.edu/ece/people/",
    ]},
    {"department": "Mechanical Engineering", "urls": [
        "https://eng.auburn.edu/me/people/graduate-students.html",
        "https://eng.auburn.edu/me/people/graduate-students",
        "https://eng.auburn.edu/me/people/",
    ]},
    {"department": "Civil & Environmental Engineering", "urls": [
        "https://eng.auburn.edu/civil/people/graduate-students.html",
        "https://eng.auburn.edu/civil/people/graduate-students",
        "https://eng.auburn.edu/civil/people/",
    ]},
    {"department": "Chemical Engineering", "urls": [
        "https://eng.auburn.edu/che/people/graduate-students.html",
        "https://eng.auburn.edu/che/people/graduate-students",
        "https://eng.auburn.edu/che/people/",
    ]},
    {"department": "Aerospace Engineering", "urls": [
        "https://eng.auburn.edu/aero/people/graduate-students.html",
        "https://eng.auburn.edu/aero/people/graduate-students",
        "https://eng.auburn.edu/aero/people/",
    ]},
    {"department": "Biosystems Engineering", "urls": [
        "https://eng.auburn.edu/bme/people/graduate-students.html",
        "https://eng.auburn.edu/bse/people/graduate-students.html",
        "https://eng.auburn.edu/bme/people/",
        "https://eng.auburn.edu/bse/people/",
    ]},
]

# Professional Schools
PROFESSIONAL_SCHOOLS = [
    {"department": "Harbert College of Business", "urls": [
        "https://harbert.auburn.edu/people/graduate-students/",
        "https://harbert.auburn.edu/people/students/",
        "https://harbert.auburn.edu/people/phd-students/",
        "https://harbert.auburn.edu/phd/students/",
        "https://harbert.auburn.edu/phd/",
        "https://harbert.auburn.edu/academics/graduate/phd/",
        "https://harbert.auburn.edu/directory/",
        "https://harbert.auburn.edu/",
    ]},
    {"department": "College of Education", "urls": [
        "https://education.auburn.edu/people/students",
        "https://education.auburn.edu/people/students/",
        "https://education.auburn.edu/people/graduate-students/",
        "https://education.auburn.edu/people/",
        "https://education.auburn.edu/directory/",
    ]},
    {"department": "Harrison College of Pharmacy", "urls": [
        "https://pharmacy.auburn.edu/people/students",
        "https://pharmacy.auburn.edu/people/students/",
        "https://pharmacy.auburn.edu/people/graduate-students/",
        "https://pharmacy.auburn.edu/people/",
        "https://pharmacy.auburn.edu/directory/",
    ]},
    {"department": "School of Nursing", "urls": [
        "https://nursing.auburn.edu/people/students",
        "https://nursing.auburn.edu/people/students/",
        "https://nursing.auburn.edu/people/graduate-students/",
        "https://nursing.auburn.edu/people/",
        "https://nursing.auburn.edu/directory/",
    ]},
    {"department": "College of Veterinary Medicine", "urls": [
        "https://vet.auburn.edu/people/students/",
        "https://vet.auburn.edu/people/graduate-students/",
        "https://vet.auburn.edu/education/graduate-students/",
        "https://vet.auburn.edu/directory/",
        "https://vet.auburn.edu/",
    ]},
]

# Athletics
ATHLETICS = [
    {"department": "Auburn Tigers Athletics (Staff)", "urls": [
        "https://auburntigers.com/staff-directory",
        "https://auburntigers.com/staff-directory/",
        "https://www.auburntigers.com/staff-directory",
        "https://www.auburntigers.com/staff-directory/",
    ]},
]

# Student Organizations
STUDENT_ORGS = [
    {"department": "Student Government Association", "urls": [
        "https://www.auburn.edu/student_info/student-government/",
        "https://auburn.campuslabs.com/engage/organization/sga",
        "https://www.auburn.edu/sga/",
        "https://www.auburn.edu/sga/officers/",
        "https://www.auburn.edu/sga/about/",
        "https://www.auburn.edu/sga/contact/",
        "https://www.auburn.edu/student_info/student-government/officers.php",
        "https://www.auburn.edu/student_info/student-government/about.php",
    ]},
    {"department": "The Auburn Plainsman (Student Newspaper)", "urls": [
        "https://www.theplainsman.com/staff/",
        "https://www.theplainsman.com/contact/",
        "https://www.theplainsman.com/about/",
        "https://www.theplainsman.com/",
        "https://theplainsman.com/staff/",
        "https://theplainsman.com/contact/",
        "https://theplainsman.com/about/",
    ]},
]


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_auburn_emails(text):
    """Extract all @auburn.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*auburn\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*auburn\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract auburn.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*auburn\.edu)',
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
        'provost@', 'president@', 'chancellor@',
        'graduate@', 'testing@', 'counseling@', 'transit@',
        'engagement@', 'research@', 'compliance@', 'title-ix@',
        'oithelp@', 'cosam@', 'cla@', 'eng@', 'harbert@',
        'education@', 'pharmacy@', 'nursing@', 'vetmed@',
        'athletics@', 'ticket@', 'tickets@', 'gameday@',
        'gradschool@', 'oit@', 'aub-opp@', 'aufamily@',
        'aero@', 'bme@', 'bse@', 'che@', 'civil@', 'csse@',
        'ece@', 'me@', 'eng@', 'cosam@', 'cla@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


def get_soup(url, session):
    """Fetch a page and return BeautifulSoup object.
    Detects COSAM and CLA custom 404 pages that return HTTP 200."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            # COSAM custom 404 detection (redirects to /cosam/404.htm)
            if '/cosam/404.htm' in resp.url:
                log(f"    -> Custom 404 (COSAM)")
                return None, None
            # CLA custom 404 detection
            title = soup.title.string.strip() if soup.title and soup.title.string else ""
            if 'page not found' in title.lower():
                log(f"    -> Custom 404 (title: Page Not Found)")
                return None, None
            return soup, resp.url
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
                            'read more', 'view profile', 'website', 'lab'
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
                        'read more', 'department', 'faculty', 'office', 'http'
                    ]):
                        return name
            parent = parent.parent

    return ""


# ============================================================
# STRUCTURED EXTRACTION: Person cards / grid layouts
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
        '.node--type-person',
        '.person-row',
        'tr',
        'li.leaf',
        '.vcard',
        '.grid-item',
        '.teaser',
        '.faculty-staff',
        '.people-listing',
        '[class*="grid-item"]',
        '[class*="team-member"]',
        '[class*="staff-listing"]',
        '[class*="faculty"]',
        '.wp-block-column',
        '.entry-content',
        '.bio',
        '.staff-bio',
        '.listing-item',
        'dl',  # definition lists used on some Auburn pages
        '.accordion-content',
        '.panel-body',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_auburn_emails(text)
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
                                'read more', 'view profile'
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

        # Match profile-like URLs on auburn.edu
        if re.search(r'/people/[\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'staff', 'administration',
                    'directory', 'about', 'contact', 'grad-students',
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
    """Scrape an individual profile page for auburn.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_auburn_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


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

        # Strategy 1: Extract from structured cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_auburn_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*auburn\.edu)',
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

        # Strategy 4: JavaScript-embedded emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_auburn_emails(script.string)
                for email in script_emails:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': department,
                            'source_url': final_url or url,
                        })

        # Strategy 5: Follow pagination
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
                pg_text_emails = extract_auburn_emails(pg_text)
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
            profiles = find_profile_links(soup, final_url or url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting for emails...")
                for profile in profiles[:60]:
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
# ATHLETICS SCRAPER (auburntigers.com)
# ============================================================

def scrape_athletics(session):
    """Scrape Auburn Tigers athletics staff directory for @auburn.edu emails."""
    results = []
    seen_emails = set()
    department = "Auburn Tigers Athletics (Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls_to_try = [
        "https://auburntigers.com/staff-directory",
        "https://auburntigers.com/staff-directory/",
        "https://www.auburntigers.com/staff-directory",
        "https://www.auburntigers.com/staff-directory/",
    ]

    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        log(f"    -> Page loaded (final URL: {final_url})")

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_auburn_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} raw Auburn emails")

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

        # Also check person cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Check script tags
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_auburn_emails(script.string)
                for email in script_emails:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': department,
                            'source_url': final_url or url,
                        })

        if results:
            break
        time.sleep(1)

    # Also try individual sport staff / coach pages
    sport_pages = [
        "https://auburntigers.com/sports/football/coaches",
        "https://auburntigers.com/sports/mens-basketball/coaches",
        "https://auburntigers.com/sports/womens-basketball/coaches",
        "https://auburntigers.com/sports/baseball/coaches",
        "https://auburntigers.com/sports/softball/coaches",
        "https://auburntigers.com/sports/mens-swimming-and-diving/coaches",
        "https://auburntigers.com/sports/womens-swimming-and-diving/coaches",
        "https://auburntigers.com/sports/track-and-field/coaches",
        "https://auburntigers.com/sports/womens-soccer/coaches",
        "https://auburntigers.com/sports/volleyball/coaches",
        "https://auburntigers.com/sports/womens-golf/coaches",
        "https://auburntigers.com/sports/mens-golf/coaches",
        "https://auburntigers.com/sports/gymnastics/coaches",
        "https://auburntigers.com/sports/equestrian/coaches",
        "https://auburntigers.com/sports/mens-tennis/coaches",
        "https://auburntigers.com/sports/womens-tennis/coaches",
    ]

    for url in sport_pages:
        log(f"  Trying sport page: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_auburn_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                sport_name = url.split('/sports/')[1].split('/')[0].replace('-', ' ').title() if '/sports/' in url else ""
                results.append({
                    'email': email,
                    'name': name,
                    'department': f"Athletics - {sport_name}" if sport_name else department,
                    'source_url': final_url or url,
                })

        time.sleep(0.5)

    log(f"  TOTAL Athletics: {len(results)} emails")
    return results


# ============================================================
# HARBERT BUSINESS SCHOOL - SPECIAL HANDLING
# ============================================================

def scrape_harbert(session):
    """Scrape Harbert College of Business - may have different layout."""
    results = []
    seen_emails = set()
    department = "Harbert College of Business"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://harbert.auburn.edu/people/graduate-students/",
        "https://harbert.auburn.edu/people/students/",
        "https://harbert.auburn.edu/people/phd-students/",
        "https://harbert.auburn.edu/phd/students/",
        "https://harbert.auburn.edu/phd/",
        "https://harbert.auburn.edu/academics/graduate/phd/",
        "https://harbert.auburn.edu/directory/",
        "https://harbert.auburn.edu/",
    ]

    # Also try sub-department pages
    harbert_subs = [
        "https://harbert.auburn.edu/academics/departments/accounting/",
        "https://harbert.auburn.edu/academics/departments/finance/",
        "https://harbert.auburn.edu/academics/departments/management/",
        "https://harbert.auburn.edu/academics/departments/marketing/",
        "https://harbert.auburn.edu/academics/departments/supply-chain-management/",
        "https://harbert.auburn.edu/academics/departments/systems-technology/",
    ]

    all_urls = urls + harbert_subs

    for url in all_urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_auburn_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        if not all_emails:
            log(f"    -> No emails found")
            profiles = find_profile_links(soup, final_url or url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links")
                for profile in profiles[:30]:
                    email = scrape_profile_page(profile['profile_url'], session, department)
                    if email and email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': profile['name'],
                            'department': department,
                            'source_url': profile['profile_url'],
                        })
                    time.sleep(0.3)
        else:
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

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# CLA DIRECTORY API SCRAPER (Umbraco-based)
# ============================================================

def scrape_cla_directory_api(session):
    """Scrape CLA directory via Umbraco API for graduate students.
    CLA uses a Vue.js frontend backed by /umbraco/api/Directory/GetPeople.
    Requires visiting the Directory page first to get session cookies."""
    results = []
    seen_emails = set()

    log(f"\n{'=' * 60}")
    log("CLA Directory API (Graduate Students)")
    log(f"{'=' * 60}")

    # Must visit Directory page first to establish cookies
    try:
        session.get('https://cla.auburn.edu/Directory', headers=HEADERS, timeout=15)
    except Exception:
        pass

    people_url = 'https://cla.auburn.edu/umbraco/api/Directory/GetPeople'
    api_headers = dict(HEADERS)
    api_headers.update({
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://cla.auburn.edu/Directory',
        'X-Requested-With': 'XMLHttpRequest',
    })

    for page in range(1, 50):  # safety limit
        params = {'page': page, 'category': 'Graduate Student'}
        try:
            resp = session.get(people_url, params=params, headers=api_headers, timeout=15)
            if resp.status_code != 200:
                log(f"  API page {page}: HTTP {resp.status_code}")
                break
            data = resp.json()
        except Exception as e:
            log(f"  API error on page {page}: {e}")
            break

        people = data.get('results', [])
        total_pages = int(data.get('totalPages', 0))
        log(f"  Page {page}/{total_pages}: {len(people)} results")

        for person in people:
            email = (person.get('email') or '').lower().strip()
            if not email or '@auburn.edu' not in email:
                continue
            if email in seen_emails or is_admin_email(email):
                continue

            seen_emails.add(email)
            name = (person.get('name') or '').strip()
            depts = person.get('department', [])
            dept_str = ', '.join(depts) if depts else 'CLA'

            results.append({
                'email': email,
                'name': name,
                'department': f"{dept_str} (CLA)",
                'source_url': f"https://cla.auburn.edu/Directory",
            })

        if page >= total_pages:
            break
        time.sleep(0.3)

    log(f"  CLA API total: {len(results)} graduate student emails")
    return results


# ============================================================
# ADDITIONAL SEARCH: General Auburn directory-style pages
# ============================================================

def scrape_additional_sources(session):
    """Try additional Auburn pages that may have graduate student emails."""
    results = []
    seen_emails = set()

    log(f"\n{'=' * 60}")
    log("Additional Auburn sources")
    log(f"{'=' * 60}")

    # General graduate school pages
    additional_urls = [
        ("Graduate School", "https://graduate.auburn.edu/"),
        ("Graduate School", "https://graduate.auburn.edu/current-students/"),
        ("Graduate School", "https://graduate.auburn.edu/prospective-students/"),
        # Forestry and Wildlife
        ("School of Forestry & Wildlife", "https://sfws.auburn.edu/people/graduate-students/"),
        ("School of Forestry & Wildlife", "https://sfws.auburn.edu/people/"),
        # Agriculture
        ("College of Agriculture", "https://agriculture.auburn.edu/people/graduate-students/"),
        ("College of Agriculture", "https://agriculture.auburn.edu/people/"),
        # Architecture
        ("College of Architecture", "https://cadc.auburn.edu/people/graduate-students/"),
        ("College of Architecture", "https://cadc.auburn.edu/people/"),
        # Human Sciences
        ("College of Human Sciences", "https://humsci.auburn.edu/people/graduate-students/"),
        ("College of Human Sciences", "https://humsci.auburn.edu/people/"),
    ]

    for dept, url in additional_urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_auburn_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': dept,
                    'source_url': final_url or url,
                })

        time.sleep(0.5)

    log(f"  Additional sources total: {len(results)} emails")
    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_results = []
    global_seen_emails = set()

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
    log("AUBURN UNIVERSITY EMAIL SCRAPER")
    log("Domain: @auburn.edu")
    log("=" * 70)

    # ---- Phase 1: COSAM (Sciences & Mathematics) ----
    log("\n\nPHASE 1: COSAM (College of Sciences and Mathematics)")
    log("=" * 70)

    for config in COSAM_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: CLA (Liberal Arts) via API ----
    log("\n\nPHASE 2: CLA (College of Liberal Arts) via Umbraco API")
    log("=" * 70)

    try:
        cla_results = scrape_cla_directory_api(session)
        n = add_results(cla_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping CLA API: {e}")

    # Also try CLA department pages directly (may catch some not in API)
    for config in CLA_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            if n > 0:
                log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 3: Engineering ----
    log("\n\nPHASE 3: SAMUEL GINN COLLEGE OF ENGINEERING")
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

    # Harbert Business - special handling
    try:
        harbert_results = scrape_harbert(session)
        n = add_results(harbert_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping Harbert: {e}")

    # Other professional schools
    for config in PROFESSIONAL_SCHOOLS:
        if 'Harbert' in config['department']:
            continue  # Already handled above
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
        athletics_results = scrape_athletics(session)
        n = add_results(athletics_results)
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

    # ---- Phase 7: Additional Sources ----
    log("\n\nPHASE 7: ADDITIONAL SOURCES")
    log("=" * 70)

    try:
        additional_results = scrape_additional_sources(session)
        n = add_results(additional_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping additional sources: {e}")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @auburn.edu emails: {len(all_results)}")

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

    all_dept_names = (
        [c['department'] for c in COSAM_DEPARTMENTS] +
        [c['department'] for c in CLA_DEPARTMENTS] +
        [c['department'] for c in ENGINEERING_DEPARTMENTS] +
        [c['department'] for c in PROFESSIONAL_SCHOOLS] +
        ["Auburn Tigers Athletics (Staff)"] +
        [c['department'] for c in STUDENT_ORGS]
    )
    depts_with_zero = [d for d in all_dept_names if d not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in sorted(set(depts_with_zero)):
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
