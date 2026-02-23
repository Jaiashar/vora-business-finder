#!/usr/bin/env python3
"""
University of Oregon Email Scraper
Scrapes @uoregon.edu emails from:
- Arts & Sciences department graduate student directories
- Professional school directories (Business, Law, Education, Journalism, Architecture)
- Athletics staff directory (goducks.com)
- Student organizations (ASUO, Oregon Daily Emerald)
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/oregon_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/oregon_dept_emails.json'


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# Arts & Sciences departments - /people/graduate-students pattern
ARTS_SCIENCES = [
    {
        "department": "Economics",
        "urls": [
            "https://economics.uoregon.edu/people/graduate-students/",
            "https://economics.uoregon.edu/people/graduate-students",
            "https://economics.uoregon.edu/people/",
            "https://economics.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Political Science",
        "urls": [
            "https://polisci.uoregon.edu/people/graduate-students/",
            "https://polisci.uoregon.edu/people/graduate-students",
            "https://polisci.uoregon.edu/people/",
            "https://polisci.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Sociology",
        "urls": [
            "https://sociology.uoregon.edu/people/graduate-students/",
            "https://sociology.uoregon.edu/people/graduate-students",
            "https://sociology.uoregon.edu/people/",
            "https://sociology.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Psychology",
        "urls": [
            "https://psychology.uoregon.edu/people/graduate-students/",
            "https://psychology.uoregon.edu/people/graduate-students",
            "https://psychology.uoregon.edu/people/",
            "https://psychology.uoregon.edu/directory/",
        ],
    },
    {
        "department": "History",
        "urls": [
            "https://history.uoregon.edu/people/graduate-students/",
            "https://history.uoregon.edu/people/graduate-students",
            "https://history.uoregon.edu/people/",
            "https://history.uoregon.edu/directory/",
        ],
    },
    {
        "department": "English",
        "urls": [
            "https://english.uoregon.edu/people/graduate-students/",
            "https://english.uoregon.edu/people/graduate-students",
            "https://english.uoregon.edu/people/",
            "https://english.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.uoregon.edu/people/graduate-students/",
            "https://philosophy.uoregon.edu/people/graduate-students",
            "https://philosophy.uoregon.edu/people/",
            "https://philosophy.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://linguistics.uoregon.edu/people/graduate-students/",
            "https://linguistics.uoregon.edu/people/graduate-students",
            "https://linguistics.uoregon.edu/people/",
            "https://linguistics.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Mathematics",
        "urls": [
            "https://math.uoregon.edu/people/graduate-students/",
            "https://math.uoregon.edu/people/graduate-students",
            "https://math.uoregon.edu/people/",
            "https://math.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Physics",
        "urls": [
            "https://physics.uoregon.edu/people/graduate-students/",
            "https://physics.uoregon.edu/people/graduate-students",
            "https://physics.uoregon.edu/people/",
            "https://physics.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Chemistry",
        "urls": [
            "https://chemistry.uoregon.edu/people/graduate-students/",
            "https://chemistry.uoregon.edu/people/graduate-students",
            "https://chemistry.uoregon.edu/people/",
            "https://chemistry.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Earth Sciences",
        "urls": [
            "https://earthsciences.uoregon.edu/people/graduate-students/",
            "https://earthsciences.uoregon.edu/people/graduate-students",
            "https://earthsciences.uoregon.edu/people/",
            "https://earthsciences.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Biology",
        "urls": [
            "https://biology.uoregon.edu/people/graduate-students/",
            "https://biology.uoregon.edu/people/graduate-students",
            "https://biology.uoregon.edu/people/",
            "https://biology.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Anthropology",
        "urls": [
            "https://anthropology.uoregon.edu/people/graduate-students/",
            "https://anthropology.uoregon.edu/people/graduate-students",
            "https://anthropology.uoregon.edu/people/",
            "https://anthropology.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Geography",
        "urls": [
            "https://geography.uoregon.edu/people/graduate-students/",
            "https://geography.uoregon.edu/people/graduate-students",
            "https://geography.uoregon.edu/people/",
            "https://geography.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Music",
        "urls": [
            "https://music.uoregon.edu/people/graduate-students/",
            "https://music.uoregon.edu/people/graduate-students",
            "https://music.uoregon.edu/people/",
            "https://music.uoregon.edu/directory/",
        ],
    },
]

# Professional Schools
PROFESSIONAL = [
    {
        "department": "Lundquist College of Business (PhD)",
        "urls": [
            "https://business.uoregon.edu/phd",
            "https://business.uoregon.edu/phd/",
            "https://business.uoregon.edu/phd/students",
            "https://business.uoregon.edu/phd/current-students",
            "https://business.uoregon.edu/faculty",
            "https://business.uoregon.edu/people/students",
            "https://business.uoregon.edu/people/graduate-students",
            "https://business.uoregon.edu/directory",
        ],
    },
    {
        "department": "School of Law",
        "urls": [
            "https://law.uoregon.edu/explore/student-organizations",
            "https://law.uoregon.edu/explore/studentorgs",
            "https://law.uoregon.edu/people/students",
            "https://law.uoregon.edu/people",
            "https://law.uoregon.edu/",
        ],
    },
    {
        "department": "College of Education",
        "urls": [
            "https://education.uoregon.edu/people/students",
            "https://education.uoregon.edu/people/students/",
            "https://education.uoregon.edu/people/graduate-students",
            "https://education.uoregon.edu/people/",
            "https://education.uoregon.edu/directory/",
        ],
    },
    {
        "department": "School of Journalism & Communication (SOJC)",
        "urls": [
            "https://journalism.uoregon.edu/people/students",
            "https://journalism.uoregon.edu/people/students/",
            "https://journalism.uoregon.edu/people/graduate-students",
            "https://journalism.uoregon.edu/people/",
            "https://journalism.uoregon.edu/directory/",
        ],
    },
    {
        "department": "College of Design (Architecture)",
        "urls": [
            "https://architecture.uoregon.edu/people/students",
            "https://architecture.uoregon.edu/people/students/",
            "https://architecture.uoregon.edu/people/graduate-students",
            "https://architecture.uoregon.edu/people/",
            "https://design.uoregon.edu/people/students",
            "https://design.uoregon.edu/people/graduate-students",
            "https://design.uoregon.edu/people/",
        ],
    },
]

# Athletics
ATHLETICS = [
    {
        "department": "Athletics (Oregon Ducks Staff)",
        "urls": [
            "https://goducks.com/staff-directory",
            "https://goducks.com/staff-directory/",
            "https://goducks.com/sports/2017/6/16/staff-html.aspx",
        ],
    },
]

# Student Orgs
STUDENT_ORGS = [
    {
        "department": "ASUO (Student Government)",
        "urls": [
            "https://asuo.uoregon.edu/",
            "https://asuo.uoregon.edu/about/",
            "https://asuo.uoregon.edu/leadership/",
            "https://asuo.uoregon.edu/officers/",
            "https://asuo.uoregon.edu/executive/",
            "https://asuo.uoregon.edu/contact/",
            "https://asuo.uoregon.edu/directory/",
        ],
    },
    {
        "department": "Oregon Daily Emerald (Student Newspaper)",
        "urls": [
            "https://www.dailyemerald.com/staff/",
            "https://www.dailyemerald.com/contact/",
            "https://www.dailyemerald.com/about/",
            "https://dailyemerald.com/staff/",
            "https://dailyemerald.com/contact/",
        ],
    },
]


# ============================================================
# EMAIL EXTRACTION
# ============================================================

def extract_uoregon_emails(text):
    """Extract all @uoregon.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*uoregon\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number-prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*uoregon\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract uoregon.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uoregon\.edu)',
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
        'uocomm@', 'uonews@', 'provost@', 'president@',
        'commencement@', 'diversity@', 'equity@', 'titleix@',
        'gradschool@', 'gradprog@', 'gradstudies@',
        'depthead@', 'frontdesk@', 'mainoffice@',
        'reserve@', 'uoregon@', 'asuo@', 'emerald@',
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
    """Extract people from card/grid-based layouts (Drupal-style UO sites)."""
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
        '.people-grid__item',
        '.people-list__item',
        '.directory-listing',
        '.staff-member',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_uoregon_emails(text)
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

        # Match profile-like URLs on uoregon.edu
        if re.search(r'/(people|directory|profiles?)/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
            # Skip non-profile URLs
            if any(x in full_url.lower() for x in [
                'graduate-students', 'faculty', '/people/$', 'directory/$',
                'all-people', 'staff', 'emeriti'
            ]):
                continue
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
    """Scrape an individual profile page for uoregon.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_uoregon_emails(text)
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
        text_emails = extract_uoregon_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*uoregon\.edu)',
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
                script_emails = extract_uoregon_emails(script.string)
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
                pg_text_emails = extract_uoregon_emails(pg_text)
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
# ATHLETICS SCRAPER (goducks.com)
# ============================================================

def scrape_athletics(session):
    """Scrape Oregon Ducks athletics staff directory for @uoregon.edu emails."""
    results = []
    seen_emails = set()
    department = "Athletics (Oregon Ducks Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://goducks.com/staff-directory",
        "https://goducks.com/staff-directory/",
        "https://goducks.com/sports/2017/6/16/staff-html.aspx",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_uoregon_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} UO emails on page")

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

        # Check for staff listing sub-links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if 'staff' in href.lower() and full_url != url and 'goducks' in full_url:
                if full_url in [u for u in urls]:
                    continue
                log(f"    Following staff link: {full_url}")
                sub_soup, sub_url = get_soup(full_url, session)
                if sub_soup:
                    sub_text = sub_soup.get_text(separator=' ', strip=True)
                    sub_emails = extract_uoregon_emails(sub_text)
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

        # Follow pagination for athletics
        pagination_urls = find_pagination_urls(soup, final_url or url)
        for page_url in pagination_urls:
            log(f"    Paginated athletics: {page_url}")
            page_soup, _ = get_soup(page_url, session)
            if page_soup:
                pg_text = page_soup.get_text(separator=' ', strip=True)
                pg_emails = extract_uoregon_emails(pg_text)
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

        if results:
            break
        time.sleep(1)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# SUPPLEMENTAL: Additional UO directory pages
# ============================================================

def scrape_supplemental_pages(session, global_seen_emails):
    """Scrape additional UO directory pages that may yield more emails."""
    results = []
    seen_emails = set(global_seen_emails)

    supplemental_urls = [
        # Graduate school general pages
        ("Graduate School", "https://gradschool.uoregon.edu/"),
        ("Graduate School", "https://gradschool.uoregon.edu/people"),

        # ========== CAS Faculty Directories (new structure) ==========
        # These live at cas.uoregon.edu/directory/{dept}-faculty with mailto links
        ("Economics (Faculty)", "https://cas.uoregon.edu/directory/economics-faculty"),
        ("Political Science (Faculty)", "https://cas.uoregon.edu/directory/political-science-faculty"),
        ("Sociology (Faculty)", "https://cas.uoregon.edu/directory/sociology-faculty"),
        ("Psychology (Faculty)", "https://cas.uoregon.edu/directory/psychology-faculty"),
        ("History (Faculty)", "https://cas.uoregon.edu/directory/history-faculty"),
        ("English (Faculty)", "https://cas.uoregon.edu/directory/english-faculty"),
        ("Philosophy (Faculty)", "https://cas.uoregon.edu/directory/philosophy-faculty"),
        ("Linguistics (Faculty)", "https://cas.uoregon.edu/directory/linguistics-faculty"),
        ("Mathematics (Faculty)", "https://cas.uoregon.edu/directory/mathematics-faculty"),
        ("Physics (Faculty)", "https://cas.uoregon.edu/directory/physics-faculty"),
        ("Chemistry & Biochemistry (Faculty)", "https://cas.uoregon.edu/directory/chemistry-biochemistry-faculty"),
        ("Earth Sciences (Faculty)", "https://cas.uoregon.edu/directory/earth-sciences-faculty"),
        ("Biology (Faculty)", "https://cas.uoregon.edu/directory/biology-faculty"),
        ("Anthropology (Faculty)", "https://cas.uoregon.edu/directory/anthropology-faculty"),
        ("Geography (Faculty)", "https://cas.uoregon.edu/directory/geography-faculty"),

        # CAS Staff Directories
        ("Economics (Staff)", "https://cas.uoregon.edu/directory/economics-staff"),
        ("Political Science (Staff)", "https://cas.uoregon.edu/directory/political-science-staff"),
        ("Sociology (Staff)", "https://cas.uoregon.edu/directory/sociology-staff"),
        ("Psychology (Staff)", "https://cas.uoregon.edu/directory/psychology-staff"),
        ("History (Staff)", "https://cas.uoregon.edu/directory/history-staff"),
        ("English (Staff)", "https://cas.uoregon.edu/directory/english-staff"),
        ("Philosophy (Staff)", "https://cas.uoregon.edu/directory/philosophy-staff"),
        ("Linguistics (Staff)", "https://cas.uoregon.edu/directory/linguistics-staff"),
        ("Mathematics (Staff)", "https://cas.uoregon.edu/directory/mathematics-staff"),
        ("Physics (Staff)", "https://cas.uoregon.edu/directory/physics-staff"),
        ("Chemistry & Biochemistry (Staff)", "https://cas.uoregon.edu/directory/chemistry-biochemistry-staff"),
        ("Earth Sciences (Staff)", "https://cas.uoregon.edu/directory/earth-sciences-staff"),
        ("Biology (Staff)", "https://cas.uoregon.edu/directory/biology-staff"),
        ("Anthropology (Staff)", "https://cas.uoregon.edu/directory/anthropology-staff"),
        ("Geography (Staff)", "https://cas.uoregon.edu/directory/geography-staff"),

        # ========== Social Sciences contact pages ==========
        ("Economics", "https://socialsciences.uoregon.edu/economics/contact-us"),
        ("Political Science", "https://socialsciences.uoregon.edu/political-science/contact-us"),
        ("Sociology", "https://socialsciences.uoregon.edu/sociology/contact-us"),
        ("History", "https://socialsciences.uoregon.edu/history/contact-us"),
        ("Anthropology", "https://socialsciences.uoregon.edu/anthropology/contact-us"),
        ("Geography", "https://socialsciences.uoregon.edu/geography/contact-us"),

        # ========== Natural Sciences contact pages ==========
        ("Psychology", "https://naturalsciences.uoregon.edu/psychology/contact-us"),
        ("Mathematics", "https://naturalsciences.uoregon.edu/mathematics/contact-us"),
        ("Physics", "https://naturalsciences.uoregon.edu/physics/contact-us"),
        ("Chemistry & Biochemistry", "https://naturalsciences.uoregon.edu/chemistry-biochemistry/contact-us"),
        ("Earth Sciences", "https://naturalsciences.uoregon.edu/earth-sciences/contact-us"),
        ("Biology", "https://naturalsciences.uoregon.edu/biology/contact-us"),

        # ========== Humanities contact pages ==========
        ("English", "https://humanities.uoregon.edu/english/contact-us"),
        ("Philosophy", "https://humanities.uoregon.edu/philosophy/contact-us"),
        ("Linguistics", "https://humanities.uoregon.edu/linguistics/contact-us"),

        # ========== Additional CAS departments ==========
        ("Computer & Information Science (Faculty)", "https://cas.uoregon.edu/directory/computer-information-science-faculty"),
        ("Computer & Information Science (Staff)", "https://cas.uoregon.edu/directory/computer-information-science-staff"),
        ("Data Science (Faculty)", "https://cas.uoregon.edu/directory/data-science-faculty"),
        ("Human Physiology (Faculty)", "https://cas.uoregon.edu/directory/human-physiology-faculty"),
        ("Human Physiology (Staff)", "https://cas.uoregon.edu/directory/human-physiology-staff"),

        # ========== Additional departments ==========
        ("Romance Languages (Faculty)", "https://cas.uoregon.edu/directory/romance-languages-faculty"),
        ("East Asian Languages (Faculty)", "https://cas.uoregon.edu/directory/east-asian-languages-literatures-faculty"),
        ("German & Scandinavian (Faculty)", "https://cas.uoregon.edu/directory/german-scandinavian-faculty"),
        ("Comparative Literature (Faculty)", "https://cas.uoregon.edu/directory/comparative-literature-faculty"),
        ("Theatre Arts (Faculty)", "https://cas.uoregon.edu/directory/theatre-arts-faculty"),
        ("Art (Faculty)", "https://cas.uoregon.edu/directory/art-faculty"),
        ("Art History (Faculty)", "https://cas.uoregon.edu/directory/art-history-faculty"),
        ("Religious Studies (Faculty)", "https://cas.uoregon.edu/directory/religious-studies-faculty"),
        ("Classics (Faculty)", "https://cas.uoregon.edu/directory/classics-faculty"),
        ("Environmental Studies (Faculty)", "https://cas.uoregon.edu/directory/environmental-studies-faculty"),

        # Computer & Information Science old URLs
        ("Computer & Information Science", "https://cs.uoregon.edu/people/graduate-students"),
        ("Computer & Information Science", "https://cs.uoregon.edu/people/"),

        # Data Science
        ("Data Science", "https://datascience.uoregon.edu/people"),
        # Human Physiology
        ("Human Physiology", "https://humanphysiology.uoregon.edu/people/graduate-students"),
        ("Human Physiology", "https://humanphysiology.uoregon.edu/people/"),
        # Romance Languages
        ("Romance Languages", "https://rl.uoregon.edu/people/graduate-students"),
        ("Romance Languages", "https://rl.uoregon.edu/people/"),
        # East Asian Languages & Literatures
        ("East Asian Languages", "https://eall.uoregon.edu/people/graduate-students"),
        ("East Asian Languages", "https://eall.uoregon.edu/people/"),
        # German & Scandinavian
        ("German & Scandinavian", "https://german.uoregon.edu/people/graduate-students"),
        ("German & Scandinavian", "https://german.uoregon.edu/people/"),
        # Comparative Literature
        ("Comparative Literature", "https://complit.uoregon.edu/people/graduate-students"),
        ("Comparative Literature", "https://complit.uoregon.edu/people/"),
        # Theater Arts
        ("Theater Arts", "https://theatre.uoregon.edu/people/graduate-students"),
        ("Theater Arts", "https://theatre.uoregon.edu/people/"),

        # ========== Education, Journalism, Architecture ==========
        ("College of Education (Faculty)", "https://education.uoregon.edu/directory"),
        ("College of Education", "https://education.uoregon.edu/people"),
        ("College of Education", "https://education.uoregon.edu/contact"),
        ("SOJC (Faculty)", "https://journalism.uoregon.edu/directory"),
        ("SOJC", "https://journalism.uoregon.edu/people"),
        ("SOJC", "https://journalism.uoregon.edu/contact"),
        ("College of Design", "https://design.uoregon.edu/directory"),
        ("College of Design", "https://design.uoregon.edu/people"),
        ("College of Design", "https://design.uoregon.edu/contact"),

        # ========== Research institutes ==========
        ("Institute of Neuroscience", "https://ion.uoregon.edu/people"),
        ("Institute of Neuroscience", "https://ion.uoregon.edu/directory"),
        ("Materials Science Institute", "https://msi.uoregon.edu/people"),
        ("Institute of Molecular Biology", "https://molbio.uoregon.edu/people"),
        ("Oregon Institute of Marine Biology", "https://oimb.uoregon.edu/people"),
    ]

    log(f"\n{'=' * 60}")
    log(f"SUPPLEMENTAL: Additional departments / directories")
    log(f"{'=' * 60}")

    for department, url in supplemental_urls:
        log(f"  Trying: {url} ({department})")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_uoregon_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

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

        # Follow pagination
        pagination_urls = find_pagination_urls(soup, final_url or url)
        for page_url in pagination_urls:
            page_soup, page_final = get_soup(page_url, session)
            if page_soup is None:
                continue
            pg_text = page_soup.get_text(separator=' ', strip=True)
            pg_emails = extract_uoregon_emails(pg_text)
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

        # Try profile links if few results
        profiles = find_profile_links(soup, final_url or url)
        dept_results_count = sum(1 for r in results if r['department'] == department)
        if dept_results_count < 3 and profiles:
            log(f"    -> Found {len(profiles)} profiles, visiting...")
            for profile in profiles[:40]:
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

        if results:
            new_count = sum(1 for r in results if r['department'] == department)
            if new_count > 0:
                log(f"    -> {new_count} emails from {department}")

        time.sleep(0.5)

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
    log("UNIVERSITY OF OREGON EMAIL SCRAPER")
    log("Domain: @uoregon.edu")
    log("=" * 70)

    # ---- Phase 1: Arts & Sciences ----
    log("\n\nPHASE 1: ARTS & SCIENCES DEPARTMENTS")
    log("=" * 70)

    for config in ARTS_SCIENCES:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: Professional Schools ----
    log("\n\nPHASE 2: PROFESSIONAL SCHOOLS")
    log("=" * 70)

    for config in PROFESSIONAL:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 3: Athletics ----
    log("\n\nPHASE 3: ATHLETICS")
    log("=" * 70)

    try:
        results = scrape_athletics(session)
        n = add_results(results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 4: Student Organizations ----
    log("\n\nPHASE 4: STUDENT ORGANIZATIONS")
    log("=" * 70)

    for config in STUDENT_ORGS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 5: Supplemental ----
    log("\n\nPHASE 5: SUPPLEMENTAL DEPARTMENTS")
    log("=" * 70)

    try:
        supp_results = scrape_supplemental_pages(session, global_seen_emails)
        n = add_results(supp_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR in supplemental scraping: {e}")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @uoregon.edu emails: {len(all_results)}")

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
        [c['department'] for c in ARTS_SCIENCES] +
        [c['department'] for c in PROFESSIONAL] +
        [c['department'] for c in ATHLETICS] +
        [c['department'] for c in STUDENT_ORGS]
    )
    depts_with_zero = [d for d in all_depts if d not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in depts_with_zero:
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
