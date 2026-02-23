#!/usr/bin/env python3
"""
University of Iowa Email Scraper
Scrapes @uiowa.edu emails from:
- CLAS (Arts & Sciences) department graduate student directories
- Engineering department directories
- Professional school directories (Tippie Business, Law, Education, Public Health, Pharmacy, Nursing, Writers' Workshop)
- Athletics staff directory (hawkeyesports.com)
- Student organizations (UISG, Daily Iowan)

Iowa departments typically use a Drupal-based CMS with /people/graduate-students paths.
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/iowa_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/iowa_dept_emails.json'


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# Arts & Sciences (College of Liberal Arts & Sciences - CLAS)
CLAS_DEPARTMENTS = [
    {
        "department": "Economics",
        "urls": [
            "https://economics.uiowa.edu/people/graduate-students",
            "https://economics.uiowa.edu/people/students",
            "https://economics.uiowa.edu/people",
        ],
    },
    {
        "department": "Political Science",
        "urls": [
            "https://polisci.uiowa.edu/people/graduate-students",
            "https://polisci.uiowa.edu/people/students",
            "https://polisci.uiowa.edu/people",
        ],
    },
    {
        "department": "Sociology",
        "urls": [
            "https://sociology.uiowa.edu/people/graduate-students",
            "https://sociology.uiowa.edu/people/students",
            "https://sociology.uiowa.edu/people",
        ],
    },
    {
        "department": "Psychology",
        "urls": [
            "https://psychology.uiowa.edu/people/graduate-students",
            "https://psychology.uiowa.edu/people/students",
            "https://psychology.uiowa.edu/people",
        ],
    },
    {
        "department": "History",
        "urls": [
            "https://history.uiowa.edu/people/graduate-students",
            "https://history.uiowa.edu/people/students",
            "https://history.uiowa.edu/people",
        ],
    },
    {
        "department": "English",
        "urls": [
            "https://english.uiowa.edu/people/graduate-students",
            "https://english.uiowa.edu/people/students",
            "https://english.uiowa.edu/people",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.uiowa.edu/people/graduate-students",
            "https://philosophy.uiowa.edu/people/students",
            "https://philosophy.uiowa.edu/people",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://linguistics.uiowa.edu/people/graduate-students",
            "https://linguistics.uiowa.edu/people/students",
            "https://linguistics.uiowa.edu/people",
        ],
    },
    {
        "department": "Mathematics",
        "urls": [
            "https://math.uiowa.edu/people/graduate-students",
            "https://math.uiowa.edu/people/students",
            "https://math.uiowa.edu/people",
        ],
    },
    {
        "department": "Statistics & Actuarial Science",
        "urls": [
            "https://stat.uiowa.edu/people/graduate-students",
            "https://stat.uiowa.edu/people/students",
            "https://stat.uiowa.edu/people",
        ],
    },
    {
        "department": "Physics & Astronomy",
        "urls": [
            "https://physics.uiowa.edu/people/graduate-students",
            "https://physics.uiowa.edu/people/students",
            "https://physics.uiowa.edu/people",
        ],
    },
    {
        "department": "Chemistry",
        "urls": [
            "https://chem.uiowa.edu/people/graduate-students",
            "https://chem.uiowa.edu/people/students",
            "https://chem.uiowa.edu/people",
        ],
    },
    {
        "department": "Earth & Environmental Sciences",
        "urls": [
            "https://geoscience.uiowa.edu/people/graduate-students",
            "https://geoscience.uiowa.edu/people/students",
            "https://geoscience.uiowa.edu/people",
        ],
    },
    {
        "department": "Biology",
        "urls": [
            "https://biology.uiowa.edu/people/graduate-students",
            "https://biology.uiowa.edu/people/students",
            "https://biology.uiowa.edu/people",
        ],
    },
    {
        "department": "Anthropology",
        "urls": [
            "https://anthropology.uiowa.edu/people/graduate-students",
            "https://anthropology.uiowa.edu/people/students",
            "https://anthropology.uiowa.edu/people",
        ],
    },
    {
        "department": "Geography & Sustainability Sciences",
        "urls": [
            "https://geography.uiowa.edu/people/graduate-students",
            "https://geography.uiowa.edu/people/students",
            "https://geography.uiowa.edu/people",
        ],
    },
    {
        "department": "Journalism & Mass Communication",
        "urls": [
            "https://journalism.uiowa.edu/people/graduate-students",
            "https://journalism.uiowa.edu/people/students",
            "https://journalism.uiowa.edu/people",
        ],
    },
    {
        "department": "Communication Studies",
        "urls": [
            "https://communication-studies.uiowa.edu/people/graduate-students",
            "https://communication-studies.uiowa.edu/people/students",
            "https://communication-studies.uiowa.edu/people",
        ],
    },
    {
        "department": "Music",
        "urls": [
            "https://music.uiowa.edu/people/graduate-students",
            "https://music.uiowa.edu/people/students",
            "https://music.uiowa.edu/people",
        ],
    },
]

# Engineering
ENGINEERING = [
    {
        "department": "Computer Science",
        "urls": [
            "https://cs.uiowa.edu/people/graduate-students",
            "https://cs.uiowa.edu/people/students",
            "https://cs.uiowa.edu/people",
        ],
    },
    {
        "department": "Electrical & Computer Engineering",
        "urls": [
            "https://ece.uiowa.edu/people/graduate-students",
            "https://ece.uiowa.edu/people/students",
            "https://ece.uiowa.edu/people",
        ],
    },
    {
        "department": "Mechanical Engineering",
        "urls": [
            "https://me.uiowa.edu/people/graduate-students",
            "https://me.uiowa.edu/people/students",
            "https://me.uiowa.edu/people",
        ],
    },
    {
        "department": "Civil & Environmental Engineering",
        "urls": [
            "https://cee.uiowa.edu/people/graduate-students",
            "https://cee.uiowa.edu/people/students",
            "https://cee.uiowa.edu/people",
        ],
    },
    {
        "department": "Biomedical Engineering",
        "urls": [
            "https://bme.uiowa.edu/people/graduate-students",
            "https://bme.uiowa.edu/people/students",
            "https://bme.uiowa.edu/people",
        ],
    },
    {
        "department": "Chemical & Biochemical Engineering",
        "urls": [
            "https://che.uiowa.edu/people/graduate-students",
            "https://che.uiowa.edu/people/students",
            "https://che.uiowa.edu/people",
        ],
    },
]

# Professional Schools
PROFESSIONAL = [
    {
        "department": "Tippie College of Business (PhD)",
        "urls": [
            "https://tippie.uiowa.edu/phd",
            "https://tippie.uiowa.edu/phd/students",
            "https://tippie.uiowa.edu/phd/current-students",
            "https://tippie.uiowa.edu/people/students",
            "https://tippie.uiowa.edu/people/graduate-students",
            "https://tippie.uiowa.edu/people/phd-students",
            "https://tippie.uiowa.edu/people",
        ],
    },
    {
        "department": "College of Law",
        "urls": [
            "https://law.uiowa.edu/student-organizations",
            "https://law.uiowa.edu/students",
            "https://law.uiowa.edu/about/people",
            "https://law.uiowa.edu/people",
            "https://law.uiowa.edu/student-life/student-organizations",
            "https://law.uiowa.edu/",
        ],
    },
    {
        "department": "College of Education",
        "urls": [
            "https://education.uiowa.edu/people/students",
            "https://education.uiowa.edu/people/graduate-students",
            "https://education.uiowa.edu/people",
        ],
    },
    {
        "department": "College of Public Health",
        "urls": [
            "https://www.public-health.uiowa.edu/people/",
            "https://www.public-health.uiowa.edu/people/students/",
            "https://www.public-health.uiowa.edu/people/graduate-students/",
            "https://www.public-health.uiowa.edu/directory/",
            "https://www.public-health.uiowa.edu/",
        ],
    },
    {
        "department": "College of Pharmacy",
        "urls": [
            "https://pharmacy.uiowa.edu/people/students",
            "https://pharmacy.uiowa.edu/people/graduate-students",
            "https://pharmacy.uiowa.edu/people",
        ],
    },
    {
        "department": "College of Nursing",
        "urls": [
            "https://nursing.uiowa.edu/people/students",
            "https://nursing.uiowa.edu/people/graduate-students",
            "https://nursing.uiowa.edu/people",
        ],
    },
    {
        "department": "Iowa Writers' Workshop / Nonfiction Writing Program",
        "urls": [
            "https://clas.uiowa.edu/nonfiction-writing-program/",
            "https://clas.uiowa.edu/nonfiction-writing-program/people",
            "https://writersworkshop.uiowa.edu/",
            "https://writersworkshop.uiowa.edu/people",
            "https://writersworkshop.uiowa.edu/people/students",
        ],
    },
]

# Athletics
ATHLETICS = [
    {
        "department": "Hawkeyes Athletics (Staff)",
        "urls": [
            "https://hawkeyesports.com/staff-directory/",
            "https://hawkeyesports.com/staff/",
            "https://hawkeyesports.com/sports/2017/6/16/staff-html.aspx",
        ],
    },
]

# Student Organizations
STUDENT_ORGS = [
    {
        "department": "University of Iowa Student Government (UISG)",
        "urls": [
            "https://uisg.uiowa.edu/",
            "https://uisg.uiowa.edu/about/",
            "https://uisg.uiowa.edu/executive-branch/",
            "https://uisg.uiowa.edu/leadership/",
            "https://uisg.uiowa.edu/officers/",
            "https://uisg.uiowa.edu/contact/",
            "https://uisg.uiowa.edu/executive/",
            "https://uisg.uiowa.edu/branches/",
        ],
    },
    {
        "department": "The Daily Iowan (Student Newspaper)",
        "urls": [
            "https://dailyiowan.com/staff/",
            "https://dailyiowan.com/about/",
            "https://dailyiowan.com/contact/",
            "https://www.dailyiowan.com/staff/",
            "https://www.dailyiowan.com/about/",
            "https://www.dailyiowan.com/contact/",
        ],
    },
]


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_uiowa_emails(text):
    """Extract all @uiowa.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*uiowa\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*uiowa\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract @uiowa.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uiowa\.edu)',
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
        'president@', 'provost@', 'gradschool@', 'gradstudies@',
        'law-admissions@', 'law-career@',
        'athletics@', 'compliance@', 'titleix@', 'title-ix@',
        'conduct@', 'counseling@', 'disability@', 'veterans@',
        'international@', 'multicultural@', 'equity@',
        'uisg@', 'sga@', 'clas@', 'coe@', 'cph@',
        'tippie@', 'pharmacy@', 'nursing@', 'education@',
        'ask-', 'its-helpdesk@', 'its@',
        'hawkeyesports@', 'uifoundation@',
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
                for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a', 'span']):
                    tag_text = tag.get_text(strip=True)
                    if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                        if not any(x in tag_text.lower() for x in [
                            'email', 'contact', 'phone', 'http', 'department',
                            'graduate', 'student', 'people', 'faculty', 'office',
                            'read more', 'view profile', 'website', 'lab',
                            'research', 'iowa', 'uiowa',
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
            name_tags = parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a', 'span'])
            for tag in name_tags:
                name = tag.get_text(strip=True)
                if name and '@' not in name and len(name) > 2 and len(name) < 80:
                    if not any(x in name.lower() for x in [
                        'email', 'contact', '@', 'student', 'people', 'phone',
                        'read more', 'department', 'faculty', 'office', 'http',
                        'website', 'iowa', 'uiowa',
                    ]):
                        return name
            parent = parent.parent

    return ""


# ============================================================
# STRUCTURED EXTRACTION: Person cards / grid layouts
# ============================================================

def extract_from_person_cards(soup, url, department):
    """Extract people from card/grid-based layouts (Iowa Drupal sites)."""
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
        '.bio-block',
        '.staff-member',
        '.team-member',
        '.people-item',
        '.faculty-card',
        '.person-card',
        '.people-card',
        '.people-listing__person',
        '.view-people .view-content > div',
        '.view-directory .view-content > div',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_uiowa_emails(text)
                mailto_emails = extract_mailto_emails(card)
                all_emails = list(set(emails + mailto_emails))

                for email in all_emails:
                    if email in seen_emails or is_admin_email(email):
                        continue
                    seen_emails.add(email)

                    name = ""
                    for tag in card.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a', 'span']):
                        tag_text = tag.get_text(strip=True)
                        if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                            if not any(x in tag_text.lower() for x in [
                                'email', 'contact', 'phone', 'http', 'department',
                                'graduate', 'student', 'people', 'faculty', 'office',
                                'read more', 'view profile', 'research', 'website',
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

        # Match profile-like URLs on uiowa.edu
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
    """Scrape an individual profile page for uiowa.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_uiowa_emails(text)
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

        successful_url = final_url or url

        # Strategy 1: Extract from structured cards
        card_results = extract_from_person_cards(soup, successful_url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_uiowa_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*uiowa\.edu)',
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
                script_emails = extract_uiowa_emails(script.string)
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
                pg_text_emails = extract_uiowa_emails(pg_text)
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
# ATHLETICS SCRAPER (hawkeyesports.com)
# ============================================================

def scrape_athletics(session):
    """Scrape Hawkeyes athletics staff directory for @uiowa.edu emails."""
    results = []
    seen_emails = set()
    department = "Hawkeyes Athletics (Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://hawkeyesports.com/staff-directory/",
        "https://hawkeyesports.com/staff/",
        "https://hawkeyesports.com/sports/2017/6/16/staff-html.aspx",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_uiowa_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} Iowa emails on page")

        # Try to associate names with emails from the staff directory
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if 'mailto:' in href.lower():
                match = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uiowa\.edu)', href, re.IGNORECASE)
                if match:
                    email = match.group(1).lower().strip()
                    if email in seen_emails or is_admin_email(email):
                        continue
                    name = ""
                    parent = a_tag.parent
                    for _ in range(5):
                        if parent is None:
                            break
                        for link in parent.find_all('a', href=True):
                            link_href = link.get('href', '')
                            if 'staff-directory/' in link_href or 'coaches/' in link_href:
                                name_text = link.get_text(strip=True)
                                if name_text and '@' not in name_text and len(name_text) > 2:
                                    name = name_text
                                    break
                        for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'span']):
                            tag_text = tag.get_text(strip=True)
                            if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                                if not any(x in tag_text.lower() for x in ['email', 'phone', 'contact', 'http']):
                                    name = tag_text
                                    break
                        if name:
                            break
                        parent = parent.parent

                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': final_url or url,
                    })

        # Catch remaining emails not in mailto links
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
        for pg_url in pagination_urls:
            log(f"    Paginated athletics: {pg_url}")
            pg_soup, _ = get_soup(pg_url, session)
            if pg_soup:
                pg_text = pg_soup.get_text(separator=' ', strip=True)
                pg_emails = extract_uiowa_emails(pg_text)
                pg_mailto = extract_mailto_emails(pg_soup)
                for email in list(set(pg_emails + pg_mailto)):
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        name = try_get_name_for_email(pg_soup, email)
                        results.append({
                            'email': email,
                            'name': name,
                            'department': department,
                            'source_url': pg_url,
                        })
            time.sleep(0.5)

        # Follow sub-page staff links
        visited_sub = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if full_url in visited_sub or full_url == url or full_url in urls:
                continue
            if re.search(r'/staff-directory/[\w-]+/\d+', full_url):
                continue  # Skip individual profiles
            if 'staff' in href.lower() and ('hawkeyesports.com' in full_url or 'uiowa.edu' in full_url):
                visited_sub.add(full_url)
                log(f"    Following staff link: {full_url}")
                sub_soup, sub_url = get_soup(full_url, session)
                if sub_soup:
                    sub_text = sub_soup.get_text(separator=' ', strip=True)
                    sub_emails = extract_uiowa_emails(sub_text)
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
# TIPPIE BUSINESS - SPECIAL HANDLING
# ============================================================

def scrape_tippie_phd(session):
    """Scrape Tippie College of Business PhD students — may have different layout."""
    results = []
    seen_emails = set()
    department = "Tippie College of Business (PhD)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://tippie.uiowa.edu/phd",
        "https://tippie.uiowa.edu/phd/students",
        "https://tippie.uiowa.edu/phd/current-students",
        "https://tippie.uiowa.edu/phd/student-directory",
        "https://tippie.uiowa.edu/people/students",
        "https://tippie.uiowa.edu/people/graduate-students",
        "https://tippie.uiowa.edu/people/phd-students",
    ]

    # Sub-department PhD pages
    tippie_depts = [
        "https://tippie.uiowa.edu/accounting/phd",
        "https://tippie.uiowa.edu/finance/phd",
        "https://tippie.uiowa.edu/management-organizations/phd",
        "https://tippie.uiowa.edu/marketing/phd",
        "https://tippie.uiowa.edu/economics/phd",
        "https://tippie.uiowa.edu/management-sciences/phd",
        "https://tippie.uiowa.edu/business-analytics-information-systems/phd",
    ]

    all_urls = urls + tippie_depts

    for url in all_urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_uiowa_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        if not all_emails:
            log(f"    -> No emails found")
            profiles = find_profile_links(soup, final_url or url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links")
                for profile in profiles[:30]:
                    email = scrape_profile_page(profile['profile_url'], session)
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

        # Also look for cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

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
    log("UNIVERSITY OF IOWA EMAIL SCRAPER")
    log("Domain: @uiowa.edu")
    log("=" * 70)

    # ---- Phase 1: CLAS (Arts & Sciences) ----
    log("\n\nPHASE 1: COLLEGE OF LIBERAL ARTS & SCIENCES (CLAS)")
    log("=" * 70)

    for config in CLAS_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: Engineering ----
    log("\n\nPHASE 2: COLLEGE OF ENGINEERING")
    log("=" * 70)

    for config in ENGINEERING:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 3: Professional Schools ----
    log("\n\nPHASE 3: PROFESSIONAL SCHOOLS")
    log("=" * 70)

    # Tippie Business - special handling
    try:
        tippie_results = scrape_tippie_phd(session)
        n = add_results(tippie_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping Tippie: {e}")

    # Other professional schools
    for config in PROFESSIONAL:
        if 'Tippie' in config['department']:
            continue  # Already handled above
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 4: Athletics ----
    log("\n\nPHASE 4: ATHLETICS")
    log("=" * 70)

    try:
        athletics_results = scrape_athletics(session)
        n = add_results(athletics_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 5: Student Organizations ----
    log("\n\nPHASE 5: STUDENT ORGANIZATIONS")
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
    log(f"Total unique @uiowa.edu emails: {len(all_results)}")

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

    all_depts = (
        [c['department'] for c in CLAS_DEPARTMENTS]
        + [c['department'] for c in ENGINEERING]
        + [c['department'] for c in PROFESSIONAL]
        + [c['department'] for c in ATHLETICS]
        + [c['department'] for c in STUDENT_ORGS]
    )
    depts_with_zero = [d for d in all_depts if d not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in depts_with_zero:
            log(f"  - {d}")

    # Sample emails
    log(f"\n{'=' * 70}")
    log("SAMPLE EMAILS (first 50):")
    log(f"{'=' * 70}")
    for r in sorted(all_results, key=lambda x: x['email'])[:50]:
        name_str = f" ({r['name']})" if r['name'] else ""
        log(f"  {r['email']}{name_str} - {r['department']}")

    return all_results


if __name__ == '__main__':
    main()
