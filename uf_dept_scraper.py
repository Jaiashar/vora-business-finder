#!/usr/bin/env python3
"""
University of Florida Graduate Student Email Scraper
Scrapes @ufl.edu emails from department people/graduate-student pages,
professional schools, athletics, and student organizations.
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

# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# CLAS - College of Liberal Arts & Sciences
CLAS_DEPARTMENTS = [
    {
        "department": "Economics",
        "urls": [
            "https://economics.ufl.edu/people/graduate-students/",
            "https://economics.ufl.edu/people/phd-students/",
            "https://economics.ufl.edu/people/",
        ],
    },
    {
        "department": "Political Science",
        "urls": [
            "https://polisci.ufl.edu/people/graduate-students/",
            "https://polisci.ufl.edu/people/phd-students/",
            "https://polisci.ufl.edu/people/",
        ],
    },
    {
        "department": "Sociology",
        "urls": [
            "https://sociology.ufl.edu/people/graduate-students/",
            "https://sociology.ufl.edu/people/phd-students/",
            "https://sociology.ufl.edu/people/",
        ],
    },
    {
        "department": "Psychology",
        "urls": [
            "https://psych.ufl.edu/people/graduate-students/",
            "https://psych.ufl.edu/people/phd-students/",
            "https://psych.ufl.edu/people/",
        ],
    },
    {
        "department": "History",
        "urls": [
            "https://history.ufl.edu/people/graduate-students/",
            "https://history.ufl.edu/people/phd-students/",
            "https://history.ufl.edu/people/",
        ],
    },
    {
        "department": "English",
        "urls": [
            "https://english.ufl.edu/people/graduate-students/",
            "https://english.ufl.edu/people/phd-students/",
            "https://english.ufl.edu/people/",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.ufl.edu/people/graduate-students/",
            "https://philosophy.ufl.edu/people/phd-students/",
            "https://philosophy.ufl.edu/people/",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://lin.ufl.edu/people/graduate-students/",
            "https://lin.ufl.edu/people/phd-students/",
            "https://lin.ufl.edu/people/",
        ],
    },
    {
        "department": "Mathematics",
        "urls": [
            "https://math.ufl.edu/people/graduate-students/",
            "https://math.ufl.edu/people/phd-students/",
            "https://math.ufl.edu/people/",
        ],
    },
    {
        "department": "Statistics",
        "urls": [
            "https://stat.ufl.edu/people/graduate-students/",
            "https://stat.ufl.edu/people/phd-students/",
            "https://stat.ufl.edu/people/",
        ],
    },
    {
        "department": "Physics",
        "urls": [
            "https://phys.ufl.edu/people/graduate-students/",
            "https://phys.ufl.edu/people/phd-students/",
            "https://phys.ufl.edu/people/",
        ],
    },
    {
        "department": "Chemistry",
        "urls": [
            "https://chem.ufl.edu/people/graduate-students/",
            "https://chem.ufl.edu/people/phd-students/",
            "https://chem.ufl.edu/people/",
        ],
    },
    {
        "department": "Geological Sciences",
        "urls": [
            "https://geology.ufl.edu/people/graduate-students/",
            "https://geology.ufl.edu/people/phd-students/",
            "https://geology.ufl.edu/people/",
        ],
    },
    {
        "department": "Biology",
        "urls": [
            "https://biology.ufl.edu/people/graduate-students/",
            "https://biology.ufl.edu/people/phd-students/",
            "https://biology.ufl.edu/people/",
        ],
    },
    {
        "department": "Zoology",
        "urls": [
            "https://zoo.ufl.edu/people/graduate-students/",
            "https://zoo.ufl.edu/people/phd-students/",
            "https://zoo.ufl.edu/people/",
        ],
    },
    {
        "department": "Botany",
        "urls": [
            "https://botany.ufl.edu/people/graduate-students/",
            "https://botany.ufl.edu/people/phd-students/",
            "https://botany.ufl.edu/people/",
        ],
    },
    {
        "department": "Anthropology",
        "urls": [
            "https://anthro.ufl.edu/people/graduate-students/",
            "https://anthro.ufl.edu/people/phd-students/",
            "https://anthro.ufl.edu/people/",
        ],
    },
    {
        "department": "Classics",
        "urls": [
            "https://classics.ufl.edu/people/graduate-students/",
            "https://classics.ufl.edu/people/phd-students/",
            "https://classics.ufl.edu/people/",
        ],
    },
    {
        "department": "Religion",
        "urls": [
            "https://religion.ufl.edu/people/graduate-students/",
            "https://religion.ufl.edu/people/phd-students/",
            "https://religion.ufl.edu/people/",
        ],
    },
]

# Herbert Wertheim College of Engineering
ENGINEERING_DEPARTMENTS = [
    {
        "department": "Electrical & Computer Engineering",
        "urls": [
            "https://www.ece.ufl.edu/people/graduate-students/",
            "https://www.ece.ufl.edu/people/phd-students/",
            "https://www.ece.ufl.edu/people/",
            "https://ece.ufl.edu/people/graduate-students/",
        ],
    },
    {
        "department": "Computer & Information Science & Engineering",
        "urls": [
            "https://www.cise.ufl.edu/people/graduate-students/",
            "https://www.cise.ufl.edu/people/phd-students/",
            "https://www.cise.ufl.edu/people/",
            "https://cise.ufl.edu/people/graduate-students/",
        ],
    },
    {
        "department": "Mechanical & Aerospace Engineering",
        "urls": [
            "https://mae.ufl.edu/people/graduate-students/",
            "https://mae.ufl.edu/people/phd-students/",
            "https://mae.ufl.edu/people/",
        ],
    },
    {
        "department": "Biomedical Engineering",
        "urls": [
            "https://bme.ufl.edu/people/graduate-students/",
            "https://bme.ufl.edu/people/phd-students/",
            "https://bme.ufl.edu/people/",
        ],
    },
    {
        "department": "Environmental Engineering Sciences",
        "urls": [
            "https://essie.ufl.edu/people/graduate-students/",
            "https://essie.ufl.edu/people/phd-students/",
            "https://essie.ufl.edu/people/",
        ],
    },
    {
        "department": "Materials Science & Engineering",
        "urls": [
            "https://mse.ufl.edu/people/graduate-students/",
            "https://mse.ufl.edu/people/phd-students/",
            "https://mse.ufl.edu/people/",
        ],
    },
    {
        "department": "Chemical Engineering",
        "urls": [
            "https://che.ufl.edu/people/graduate-students/",
            "https://che.ufl.edu/people/phd-students/",
            "https://che.ufl.edu/people/",
        ],
    },
]

# Professional Schools
PROFESSIONAL_SCHOOLS = [
    {
        "department": "Warrington College of Business (PhD)",
        "urls": [
            "https://warrington.ufl.edu/phd/",
            "https://warrington.ufl.edu/phd/current-students/",
            "https://warrington.ufl.edu/directory/?role=doctoral-student",
            "https://warrington.ufl.edu/directory/?role=phd-student",
        ],
    },
    {
        "department": "College of Education",
        "urls": [
            "https://education.ufl.edu/people/students/",
            "https://education.ufl.edu/people/graduate-students/",
            "https://education.ufl.edu/people/",
        ],
    },
    {
        "department": "College of Journalism & Communications",
        "urls": [
            "https://journalism.ufl.edu/people/students/",
            "https://journalism.ufl.edu/people/graduate-students/",
            "https://journalism.ufl.edu/people/",
        ],
    },
    {
        "department": "College of Pharmacy",
        "urls": [
            "https://pharmacy.ufl.edu/people/students/",
            "https://pharmacy.ufl.edu/people/graduate-students/",
            "https://pharmacy.ufl.edu/people/",
        ],
    },
    {
        "department": "Public Health & Health Professions",
        "urls": [
            "https://phhp.ufl.edu/people/",
            "https://phhp.ufl.edu/people/students/",
            "https://phhp.ufl.edu/people/graduate-students/",
        ],
    },
    {
        "department": "College of Nursing",
        "urls": [
            "https://nursing.ufl.edu/people/students/",
            "https://nursing.ufl.edu/people/graduate-students/",
            "https://nursing.ufl.edu/people/",
        ],
    },
    {
        "department": "Design, Construction & Planning",
        "urls": [
            "https://dcp.ufl.edu/people/students/",
            "https://dcp.ufl.edu/people/graduate-students/",
            "https://dcp.ufl.edu/people/",
        ],
    },
    {
        "department": "College of the Arts",
        "urls": [
            "https://arts.ufl.edu/people/students/",
            "https://arts.ufl.edu/people/graduate-students/",
            "https://arts.ufl.edu/people/",
        ],
    },
    {
        "department": "Levin College of Law",
        "urls": [
            "https://www.law.ufl.edu/law/student-organizations",
            "https://www.law.ufl.edu/student-organizations",
            "https://law.ufl.edu/student-organizations",
            "https://law.ufl.edu/areas-of-study/student-organizations/",
        ],
    },
    {
        "department": "College of Veterinary Medicine",
        "urls": [
            "https://vet.ufl.edu/",
            "https://vet.ufl.edu/about/people/",
            "https://vet.ufl.edu/education/graduate-students/",
        ],
    },
]

# Athletics
ATHLETICS_PAGES = [
    {
        "department": "Florida Gators Athletics",
        "urls": [
            "https://floridagators.com/staff-directory",
            "https://floridagators.com/sports/2017/6/16/staff-directory.aspx",
        ],
    },
]

# Student organizations and misc
STUDENT_ORG_PAGES = [
    {
        "department": "Student Government",
        "urls": [
            "https://sg.ufl.edu/",
            "https://sg.ufl.edu/about/executive-cabinet/",
            "https://sg.ufl.edu/about/",
            "https://sg.ufl.edu/executive-cabinet/",
        ],
    },
    {
        "department": "Independent Florida Alligator (Student Newspaper)",
        "urls": [
            "https://www.alligator.org/staff/",
            "https://www.alligator.org/contact/",
        ],
    },
]

ALL_DEPARTMENTS = (
    CLAS_DEPARTMENTS
    + ENGINEERING_DEPARTMENTS
    + PROFESSIONAL_SCHOOLS
    + ATHLETICS_PAGES
    + STUDENT_ORG_PAGES
)

# ============================================================
# EMAIL EXTRACTION
# ============================================================

def extract_ufl_emails(text):
    """Extract all @ufl.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*ufl\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup):
    """Extract @ufl.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*ufl\.edu)',
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
        'admission@', 'admissions@', 'records@', 'enrollment@',
        'financial@', 'compliance@', 'title-ix@', 'titleix@',
        'president@', 'provost@', 'uflib@', 'uf-', 'gatorlink@',
        'housing@', 'parking@', 'police@', 'conduct@', 'ehs@',
        'health@', 'counseling@', 'disability@', 'veterans@',
        'international@', 'grad-', 'uaa@', 'ufaa@',
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
                for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b']):
                    tag_text = tag.get_text(strip=True)
                    if tag_text and '@' not in tag_text and len(tag_text) > 2:
                        if not any(x in tag_text.lower() for x in [
                            'email', 'contact', 'phone', 'http', 'department',
                            'graduate', 'student', 'people', 'faculty', 'office'
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
# STRUCTURED EXTRACTION: Card/grid-based people listings
# ============================================================

def extract_from_person_cards(soup, url, department):
    """Extract people from card/grid-based layouts (common in UF Drupal sites)."""
    results = []
    seen_emails = set()

    # Selectors common in UF department sites
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
        '.user-info',
        '.person-listing',
        '.bio-block',
        '.staff-member',
        '.team-member',
        '.people-item',
        '.faculty-card',
        '.person-card',
        '.people-card',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_ufl_emails(text)
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

        # Match profile-like URLs
        if re.search(r'/people/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'staff', 'back'
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session):
    """Scrape an individual profile page for ufl.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_ufl_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


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
# MAIN DEPARTMENT SCRAPER
# ============================================================

def scrape_department(config, session):
    """Scrape a single department, trying multiple URLs and strategies."""
    department = config['department']
    urls = config['urls']
    results = []
    seen_emails = set()
    successful_url = None
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
            continue

        all_pages_scraped.add(url)
        if final_url:
            all_pages_scraped.add(final_url)

        page_text = soup.get_text(separator=' ', strip=True)

        # Check if this looks like a people/student listing page
        has_people = any(kw in page_text.lower() for kw in [
            'graduate student', 'grad student', 'phd student',
            'doctoral student', '@ufl.edu', 'email',
            'ph.d.', 'master', 'research assistant',
        ])

        if not has_people and len(extract_ufl_emails(page_text)) == 0:
            log(f"    -> No student/email content detected, trying next URL")
            time.sleep(0.5)
            continue

        log(f"    -> Page loaded (final URL: {final_url})")
        successful_url = final_url or url

        # Strategy 1: Extract from structured cards
        card_results = extract_from_person_cards(soup, successful_url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_ufl_emails(page_text)
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

        # Strategy 3: Check for obfuscated emails
        obfuscated = re.findall(
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*ufl\.edu)',
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

        # Strategy 4: Check JavaScript for emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_ufl_emails(script.string)
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
                pg_text_emails = extract_ufl_emails(pg_text)
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

        # Strategy 6: If few emails found, try profile links
        if len(results) < 3:
            profiles = find_profile_links(soup, successful_url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting for emails...")
                for i, profile in enumerate(profiles[:60]):
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
        log(f"    {r['email']:<40} | {r['name']}")

    return results


# ============================================================
# ATHLETICS SCRAPER (floridagators.com)
# ============================================================

def scrape_athletics(session, seen_emails):
    """Scrape Florida Gators athletics staff directory for @ufl.edu emails."""
    results = []

    urls = [
        "https://floridagators.com/staff-directory",
        "https://floridagators.com/sports/2017/6/16/staff-directory.aspx",
    ]

    log(f"\n{'=' * 60}")
    log(f"Athletics: Florida Gators Staff Directory")
    log(f"{'=' * 60}")

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_ufl_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'Florida Gators Athletics',
                    'source_url': final_url or url,
                })

        # Also check for athlete/staff profile links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if '/staff/' in href.lower() or '/coaches/' in href.lower():
                pass  # We already got what we need from the directory

        if results:
            log(f"    -> Found {len(results)} emails")
            break

        time.sleep(1)

    return results


# ============================================================
# UF DIRECTORY SEARCH (directory.ufl.edu)
# ============================================================

def scrape_uf_directory_search(session, seen_emails):
    """Try UF's people directory for additional grad student emails."""
    results = []

    # UF has a public directory at directory.ufl.edu but it requires
    # search terms. We'll try department-based searches.
    search_terms = [
        "graduate student economics",
        "graduate student political science",
        "graduate student computer science",
        "graduate student physics",
        "graduate student chemistry",
        "graduate student mathematics",
        "graduate student biology",
        "graduate student engineering",
        "graduate student psychology",
    ]

    log(f"\n{'=' * 60}")
    log(f"UF Directory Search")
    log(f"{'=' * 60}")

    for term in search_terms:
        url = f"https://directory.ufl.edu/search/?q={term.replace(' ', '+')}"
        log(f"  Searching: {term}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_ufl_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        count = 0
        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': f"UF Directory ({term})",
                    'source_url': final_url or url,
                })
                count += 1

        log(f"    -> {count} new emails")
        time.sleep(1)

    return results


# ============================================================
# RESEARCH LAB PAGES
# ============================================================

def scrape_research_labs(session, seen_emails):
    """Scrape UF research lab pages for graduate student emails."""
    results = []

    LAB_URLS = [
        # AI / ML / Data Science labs
        ("https://www.cise.ufl.edu/research/", "CISE Research Labs"),
        ("https://www.cise.ufl.edu/research/ai/", "CISE AI Lab"),
        ("https://www.cise.ufl.edu/~daisyw/", "Daisy Wang Lab"),
        ("https://www.cise.ufl.edu/~mythai/", "My Thai Lab"),

        # Engineering research labs
        ("https://mae.ufl.edu/research/", "MAE Research"),
        ("https://bme.ufl.edu/research/", "BME Research"),
        ("https://ece.ufl.edu/research/", "ECE Research"),
        ("https://essie.ufl.edu/research/", "ESSIE Research"),

        # Science research
        ("https://phys.ufl.edu/research/", "Physics Research"),
        ("https://chem.ufl.edu/research/", "Chemistry Research"),
        ("https://biology.ufl.edu/research/", "Biology Research"),

        # Interdisciplinary centers
        ("https://www.eng.ufl.edu/ai/", "UF AI Initiative"),
        ("https://informatics.research.ufl.edu/", "UF Informatics"),
    ]

    log(f"\n{'=' * 60}")
    log(f"Research Labs")
    log(f"{'=' * 60}")

    for url, lab_name in LAB_URLS:
        log(f"  Scraping: {lab_name} ({url})")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_ufl_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)

        # Check JavaScript
        script_emails = []
        for script in soup.find_all('script'):
            if script.string:
                script_emails.extend(extract_ufl_emails(script.string))

        all_emails = list(set(text_emails + mailto_emails + script_emails))
        count = 0

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': lab_name,
                    'source_url': final_url or url,
                })
                count += 1

        if count:
            log(f"    -> {count} new emails")
        time.sleep(0.5)

    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_results = []
    global_seen_emails = set()

    log("=" * 70)
    log("UNIVERSITY OF FLORIDA - GRADUATE STUDENT EMAIL SCRAPER")
    log("=" * 70)
    log(f"Scraping {len(ALL_DEPARTMENTS)} department/org configurations...")

    # ---- Phase 1: All departments (CLAS, Engineering, Professional Schools) ----
    log("\n\nPHASE 1: DEPARTMENT PAGES")
    log("=" * 70)

    for config in ALL_DEPARTMENTS:
        try:
            dept_results = scrape_department(config, session)
            for r in dept_results:
                email = r['email'].lower().strip()
                if email and email not in global_seen_emails:
                    global_seen_emails.add(email)
                    all_results.append(r)
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    log(f"\nAfter Phase 1: {len(all_results)} unique emails")

    # ---- Phase 2: Research Labs ----
    log("\n\nPHASE 2: RESEARCH LAB PAGES")
    log("=" * 70)

    lab_results = scrape_research_labs(session, global_seen_emails)
    all_results.extend(lab_results)
    log(f"\nAfter Phase 2: {len(all_results)} unique emails")

    # ---- Phase 3: Athletics ----
    log("\n\nPHASE 3: ATHLETICS")
    log("=" * 70)

    athletics_results = scrape_athletics(session, global_seen_emails)
    all_results.extend(athletics_results)
    log(f"\nAfter Phase 3: {len(all_results)} unique emails")

    # ---- Phase 4: UF Directory Search ----
    log("\n\nPHASE 4: UF DIRECTORY SEARCH")
    log("=" * 70)

    dir_results = scrape_uf_directory_search(session, global_seen_emails)
    all_results.extend(dir_results)
    log(f"\nAfter Phase 4: {len(all_results)} unique emails")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @ufl.edu emails: {len(all_results)}")

    # Save CSV
    output_csv = 'uf_dept_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: (x['department'], x['email'])):
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")

    # Save JSON
    output_json = 'uf_dept_emails.json'
    with open(output_json, 'w') as f:
        json.dump(all_results, f, indent=2)
    log(f"Saved to {output_json}")

    # Print summary by department
    log(f"\n{'=' * 70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")

    all_dept_names = set(c['department'] for c in ALL_DEPARTMENTS)
    depts_with_zero = all_dept_names - set(dept_counts.keys())
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in sorted(depts_with_zero):
            log(f"  - {d}")

    # Print sample emails
    log(f"\n{'=' * 70}")
    log("SAMPLE EMAILS (first 50):")
    log(f"{'=' * 70}")
    for r in sorted(all_results, key=lambda x: x['email'])[:50]:
        name_str = f" ({r['name']})" if r['name'] else ""
        log(f"  {r['email']}{name_str} - {r['department']}")

    return all_results


if __name__ == '__main__':
    main()
