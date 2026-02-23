#!/usr/bin/env python3
"""
Penn State University Email Scraper
Scrapes @psu.edu emails from:
- Liberal Arts department graduate student directories
- Science department graduate student directories
- Engineering department directories
- Professional school directories
- Athletics staff directory
- Student organizations

PSU sites use a mix of Drupal CMS patterns. Liberal Arts departments use
la.psu.edu subdomains; Science uses science.psu.edu/dept; Engineering
uses dept.psu.edu with /people/graduate-students paths.
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/penn_state_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/penn_state_dept_emails.json'


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_psu_emails(text):
    """Extract all @psu.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*psu\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*psu\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract PSU emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*psu\.edu)',
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
        'do-not-reply@', 'donotreply@', 'la-help@', 'ist-help@',
        'recruitment@', 'graduate@', 'hhd@', 'educ@', 'smeal@',
        'law@', 'med@', 'ist@', 'cse@', 'engr@', 'science@',
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
    """Extract people from card/grid-based layouts (PSU Drupal & custom sites)."""
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
        '.person-listing',
        '.people-listing__person',
        '.staff-listing',
        '.view-people',
        '.view-directory',
        '.people-grid-item',
        '.people-list-item',
        '.directory-listing',
        '.media',
        '.content-card',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_psu_emails(text)
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

        # Match profile-like URLs on psu.edu
        if re.search(r'/(people|directory|person|users?)/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'directory', 'about'
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
    """Scrape an individual profile page for psu.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_psu_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# Liberal Arts departments (la.psu.edu subdomains)
LIBERAL_ARTS_DEPARTMENTS = [
    {"department": "Economics", "urls": [
        "https://econ.la.psu.edu/people/graduate-students/",
        "https://econ.la.psu.edu/people/",
        "https://econ.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Political Science", "urls": [
        "https://polisci.la.psu.edu/people/graduate-students/",
        "https://polisci.la.psu.edu/people/",
        "https://polisci.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Sociology", "urls": [
        "https://sociology.la.psu.edu/people/graduate-students/",
        "https://sociology.la.psu.edu/people/",
        "https://sociology.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Psychology", "urls": [
        "https://psych.la.psu.edu/people/graduate-students/",
        "https://psych.la.psu.edu/people/",
        "https://psych.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "History", "urls": [
        "https://history.la.psu.edu/people/graduate-students/",
        "https://history.la.psu.edu/people/",
        "https://history.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "English", "urls": [
        "https://english.la.psu.edu/people/graduate-students/",
        "https://english.la.psu.edu/people/",
        "https://english.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Philosophy", "urls": [
        "https://philosophy.la.psu.edu/people/graduate-students/",
        "https://philosophy.la.psu.edu/people/",
        "https://philosophy.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Linguistics", "urls": [
        "https://linguistics.la.psu.edu/people/graduate-students/",
        "https://linguistics.la.psu.edu/people/",
        "https://linguistics.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Anthropology", "urls": [
        "https://anthro.la.psu.edu/people/graduate-students/",
        "https://anthro.la.psu.edu/people/",
        "https://anthro.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Classics & Ancient Mediterranean Studies", "urls": [
        "https://classics.la.psu.edu/people/graduate-students/",
        "https://classics.la.psu.edu/people/",
        "https://classics.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Communication Arts & Sciences", "urls": [
        "https://comm.la.psu.edu/people/graduate-students/",
        "https://comm.la.psu.edu/people/",
        "https://comm.la.psu.edu/directory/graduate-students/",
    ]},
    {"department": "Geography", "urls": [
        "https://geog.la.psu.edu/people/graduate-students/",
        "https://geog.la.psu.edu/people/",
        "https://geog.la.psu.edu/directory/graduate-students/",
    ]},
]

# Science departments - Eberly College of Science (science.psu.edu)
# person_type=48 = Graduate Students; department IDs:
# 16=Physics, 14=Chemistry, 11=Biology, 15=Mathematics, 17=Statistics
# 10=Astronomy, 12=Biochemistry
SCIENCE_DEPARTMENTS = [
    {"department": "Physics", "urls": [
        "https://science.psu.edu/people?person_type=48&department=16&items_per_page=200",
        "https://science.psu.edu/people?person_type=47&department=16&items_per_page=200",
    ]},
    {"department": "Chemistry", "urls": [
        "https://science.psu.edu/people?person_type=48&department=14&items_per_page=200",
        "https://science.psu.edu/people?person_type=47&department=14&items_per_page=200",
    ]},
    {"department": "Biology", "urls": [
        "https://science.psu.edu/people?person_type=48&department=11&items_per_page=200",
        "https://science.psu.edu/people?person_type=47&department=11&items_per_page=200",
    ]},
    {"department": "Mathematics", "urls": [
        "https://science.psu.edu/people?person_type=48&department=15&items_per_page=200",
        "https://science.psu.edu/people?person_type=47&department=15&items_per_page=200",
    ]},
    {"department": "Statistics", "urls": [
        "https://science.psu.edu/people?person_type=48&department=17&items_per_page=200",
        "https://science.psu.edu/people?person_type=47&department=17&items_per_page=200",
    ]},
    {"department": "Astronomy & Astrophysics", "urls": [
        "https://science.psu.edu/people?person_type=48&department=10&items_per_page=200",
        "https://science.psu.edu/people?person_type=47&department=10&items_per_page=200",
    ]},
    {"department": "Biochemistry & Molecular Biology", "urls": [
        "https://science.psu.edu/people?person_type=48&department=12&items_per_page=200",
        "https://science.psu.edu/people?person_type=47&department=12&items_per_page=200",
    ]},
    {"department": "Earth Sciences / Geosciences", "urls": [
        "https://www.geosc.psu.edu/people/graduate-students",
        "https://www.geosc.psu.edu/people",
        "https://www.geosc.psu.edu/academic-programs/graduate-program/current-graduate-students",
    ]},
]

# Engineering departments (use .aspx patterns from PSU College of Engineering sites)
ENGINEERING_DEPARTMENTS = [
    {"department": "Computer Science & Engineering (PhD)", "urls": [
        "https://www.eecs.psu.edu/departments/cse-phd-student-list.aspx",
        "https://www.eecs.psu.edu/departments/cse-masters-student-list.aspx",
        "https://www.eecs.psu.edu/departments/cse-faculty-list.aspx",
    ]},
    {"department": "Electrical Engineering (PhD)", "urls": [
        "https://www.eecs.psu.edu/departments/ee-phd-student-list.aspx",
        "https://www.eecs.psu.edu/departments/ee-masters-student-list.aspx",
        "https://www.eecs.psu.edu/departments/ee-faculty-list.aspx",
    ]},
    {"department": "Mechanical Engineering", "urls": [
        "https://www.me.psu.edu/department/faculty-staff-list.aspx",
        "https://www.me.psu.edu/department/faculty-list.aspx",
        "https://www.me.psu.edu/department/staff-list.aspx",
    ]},
    {"department": "Civil & Environmental Engineering", "urls": [
        "https://www.cee.psu.edu/department/faculty-staff-list.aspx",
        "https://www.cee.psu.edu/department/faculty-list.aspx",
        "https://www.cee.psu.edu/department/staff-list.aspx",
    ]},
    {"department": "Biomedical Engineering", "urls": [
        "https://www.bme.psu.edu/department/faculty-staff-list.aspx",
        "https://www.bme.psu.edu/department/faculty-list.aspx",
        "https://www.bme.psu.edu/department/staff-list.aspx",
    ]},
    {"department": "Materials Science & Engineering", "urls": [
        "https://www.matse.psu.edu/people",
        "https://www.matse.psu.edu/directory",
        "https://www.matse.psu.edu/people/faculty",
        "https://www.matse.psu.edu/people/staff",
    ]},
    {"department": "Chemical Engineering", "urls": [
        "https://www.che.psu.edu/department/graduate-student-list.aspx",
        "https://www.che.psu.edu/department/faculty-staff-list.aspx",
        "https://www.che.psu.edu/department/faculty-list.aspx",
        "https://www.che.psu.edu/department/postdoc-list.aspx",
    ]},
    {"department": "Aerospace Engineering", "urls": [
        "https://www.aero.psu.edu/department/faculty-staff-list.aspx",
        "https://www.aero.psu.edu/department/faculty-list.aspx",
        "https://www.aero.psu.edu/department/staff-list.aspx",
    ]},
]

# Professional Schools
PROFESSIONAL_SCHOOLS = [
    {"department": "Smeal College of Business (PhD)", "urls": [
        "https://www.smeal.psu.edu/phd",
        "https://www.smeal.psu.edu/phd/students",
        "https://www.smeal.psu.edu/phd/current-students",
        "https://www.smeal.psu.edu/directory/phd-students",
        "https://www.smeal.psu.edu/directory/graduate-students",
        "https://www.smeal.psu.edu/directory",
    ]},
    {"department": "Penn State Law", "urls": [
        "https://pennstatelaw.psu.edu/",
        "https://pennstatelaw.psu.edu/practice-skills/student-organizations",
        "https://pennstatelaw.psu.edu/student-life/student-organizations",
        "https://pennstatelaw.psu.edu/about/contact",
        "https://pennstatelaw.psu.edu/people",
        "https://pennstatelaw.psu.edu/faculty",
    ]},
    {"department": "College of Medicine", "urls": [
        "https://www.med.psu.edu/",
        "https://www.med.psu.edu/student-directory",
        "https://www.med.psu.edu/directory",
        "https://med.psu.edu/graduate-students",
        "https://med.psu.edu/biomedical-sciences/students",
        "https://med.psu.edu/people",
    ]},
    {"department": "College of Education", "urls": [
        "https://ed.psu.edu/directory",
        "https://ed.psu.edu/directory?search_api_fulltext=&field_person_type=All",
        "https://ed.psu.edu/people",
        "https://ed.psu.edu/",
    ]},
    {"department": "Health & Human Development", "urls": [
        "https://hhd.psu.edu/directory",
        "https://hhd.psu.edu/contact",
        "https://hhd.psu.edu/people",
        "https://hhd.psu.edu/",
    ]},
    {"department": "School of Information Sciences & Technology", "urls": [
        "https://ist.psu.edu/directory",
        "https://ist.psu.edu/directory/students",
        "https://ist.psu.edu/directory/graduate",
        "https://ist.psu.edu/people",
        "https://ist.psu.edu/",
    ]},
]

# Athletics
ATHLETICS = [
    {"department": "Athletics (Nittany Lions Staff)", "urls": [
        "https://gopsusports.com/staff-directory",
        "https://gopsusports.com/sports/2017/6/16/staff-html.aspx",
    ]},
]

# Student Organizations
STUDENT_ORGS = [
    {"department": "UPUA (Undergrad Association)", "urls": [
        "https://sites.psu.edu/upua/",
        "https://sites.psu.edu/upua/about/",
        "https://sites.psu.edu/upua/leadership/",
        "https://sites.psu.edu/upua/executive-board/",
        "https://sites.psu.edu/upua/contact/",
        "https://upua.psu.edu/",
        "https://upua.psu.edu/about/",
    ]},
    {"department": "The Daily Collegian (Student Newspaper)", "urls": [
        "https://www.collegian.psu.edu/site/staff.html",
        "https://www.collegian.psu.edu/site/about.html",
        "https://www.collegian.psu.edu/site/contact.html",
        "https://www.collegian.psu.edu/",
        "https://www.psucollegian.com/staff/",
        "https://www.psucollegian.com/about/",
        "https://www.psucollegian.com/contact/",
        "https://www.psucollegian.com/",
    ]},
    {"department": "Graduate and Professional Student Association", "urls": [
        "https://gpsa.psu.edu/",
        "https://gpsa.psu.edu/about/",
        "https://gpsa.psu.edu/leadership/",
        "https://gpsa.psu.edu/officers/",
        "https://sites.psu.edu/gpsa/",
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
        text_emails = extract_psu_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*psu\.edu)',
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
                script_emails = extract_psu_emails(script.string)
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
                pg_text_emails = extract_psu_emails(pg_text)
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
# ATHLETICS-SPECIFIC SCRAPER
# ============================================================

def scrape_athletics(session):
    """Scrape Penn State athletics staff directory for @psu.edu emails."""
    results = []
    seen_emails = set()
    department = "Athletics (Nittany Lions Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://gopsusports.com/staff-directory",
        "https://gopsusports.com/sports/2017/6/16/staff-html.aspx",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_psu_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} raw PSU emails on page")

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
                pg_emails = extract_psu_emails(pg_text)
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

        # Follow staff sub-links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if ('staff' in href.lower() or 'directory' in href.lower()) and full_url != url and 'psu' in full_url:
                if full_url not in [u for u in urls]:
                    log(f"    Following staff link: {full_url}")
                    sub_soup, sub_url = get_soup(full_url, session)
                    if sub_soup:
                        sub_text = sub_soup.get_text(separator=' ', strip=True)
                        sub_emails = extract_psu_emails(sub_text)
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
# SMEAL BUSINESS SCHOOL - SPECIAL HANDLING
# ============================================================

def scrape_smeal_phd(session):
    """Scrape Smeal College of Business PhD students - may have special layout."""
    results = []
    seen_emails = set()
    department = "Smeal College of Business (PhD)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    # Main PhD page and sub-department pages
    urls = [
        "https://www.smeal.psu.edu/phd",
        "https://www.smeal.psu.edu/phd/students",
        "https://www.smeal.psu.edu/phd/current-students",
        "https://www.smeal.psu.edu/phd/student-directory",
        "https://www.smeal.psu.edu/directory/?role=phd",
        "https://www.smeal.psu.edu/directory/graduate-students",
        # Sub-department PhD pages
        "https://www.smeal.psu.edu/accounting/phd/students",
        "https://www.smeal.psu.edu/accounting/phd",
        "https://www.smeal.psu.edu/finance/phd/students",
        "https://www.smeal.psu.edu/finance/phd",
        "https://www.smeal.psu.edu/marketing/phd/students",
        "https://www.smeal.psu.edu/marketing/phd",
        "https://www.smeal.psu.edu/management/phd/students",
        "https://www.smeal.psu.edu/management/phd",
        "https://www.smeal.psu.edu/scm/phd/students",
        "https://www.smeal.psu.edu/scm/phd",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_psu_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        if not all_emails:
            log(f"    -> No emails found")
            # Try profile links
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
            # Extract from cards first
            card_results = extract_from_person_cards(soup, final_url or url, department)
            for r in card_results:
                if r['email'] not in seen_emails:
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
    log("PENN STATE UNIVERSITY EMAIL SCRAPER")
    log("Domain: @psu.edu")
    log("=" * 70)

    # ---- Phase 1: Liberal Arts ----
    log("\n\nPHASE 1: LIBERAL ARTS DEPARTMENTS")
    log("=" * 70)

    for config in LIBERAL_ARTS_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: Science ----
    log("\n\nPHASE 2: SCIENCE DEPARTMENTS")
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
    log("\n\nPHASE 3: ENGINEERING DEPARTMENTS")
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

    # Smeal Business - special handling
    try:
        smeal_results = scrape_smeal_phd(session)
        n = add_results(smeal_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping Smeal: {e}")

    # Other professional schools (skip Smeal since handled above)
    for config in PROFESSIONAL_SCHOOLS:
        if 'Smeal' in config['department']:
            continue
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

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @psu.edu emails: {len(all_results)}")

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
        c['department']
        for group in [LIBERAL_ARTS_DEPARTMENTS, SCIENCE_DEPARTMENTS,
                      ENGINEERING_DEPARTMENTS, PROFESSIONAL_SCHOOLS,
                      ATHLETICS, STUDENT_ORGS]
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
