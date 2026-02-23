#!/usr/bin/env python3
"""
Ohio State University Email Scraper
Scrapes @osu.edu and @buckeyemail.osu.edu emails from:
- Arts & Sciences department graduate student directories
- Engineering department directories
- Professional school directories
- Athletics staff directory
- Student organizations
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/osu_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/osu_dept_emails.json'


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_osu_emails(text):
    """Extract all @osu.edu and @buckeyemail.osu.edu emails from text."""
    pattern = r'[\w.+-]+@(?:buckeyemail\.)?osu\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:buckeyemail\.)?osu\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract OSU emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:buckeyemail\.)?osu\.edu)',
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
        'askhr@', 'ocio@', 'buckeyelink@', 'registrar@',
        'asc-deansoffice@', 'asc@', 'engineering@', 'fisher@',
        'law@', 'pharmacy@', 'nursing@', 'cph@', 'csw@',
        'askit@', 'noreply@', 'do-not-reply@', 'donotreply@',
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
    """Extract people from card/grid-based layouts (Drupal-style OSU sites)."""
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
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_osu_emails(text)
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

        # Match profile-like URLs on osu.edu
        if re.search(r'/people/[a-z][\w-]+$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department'
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
    """Scrape an individual profile page for osu.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_osu_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

ARTS_SCIENCES_DEPARTMENTS = [
    {
        "department": "Economics",
        "urls": [
            "https://economics.osu.edu/people/graduate-students",
            "https://economics.osu.edu/people/students",
            "https://economics.osu.edu/people",
        ],
    },
    {
        "department": "Political Science",
        "urls": [
            "https://polisci.osu.edu/people/graduate-students",
            "https://polisci.osu.edu/people/students",
            "https://polisci.osu.edu/people",
        ],
    },
    {
        "department": "Sociology",
        "urls": [
            "https://sociology.osu.edu/people/graduate-students",
            "https://sociology.osu.edu/people/students",
            "https://sociology.osu.edu/people",
        ],
    },
    {
        "department": "Psychology",
        "urls": [
            "https://psychology.osu.edu/people/graduate-students",
            "https://psychology.osu.edu/people/students",
            "https://psychology.osu.edu/people",
        ],
    },
    {
        "department": "History",
        "urls": [
            "https://history.osu.edu/people/graduate-students",
            "https://history.osu.edu/people/students",
            "https://history.osu.edu/people",
        ],
    },
    {
        "department": "English",
        "urls": [
            "https://english.osu.edu/people/graduate-students",
            "https://english.osu.edu/people/students",
            "https://english.osu.edu/people",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.osu.edu/people/graduate-students",
            "https://philosophy.osu.edu/people/students",
            "https://philosophy.osu.edu/people",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://linguistics.osu.edu/people/graduate-students",
            "https://linguistics.osu.edu/people/students",
            "https://linguistics.osu.edu/people",
        ],
    },
    {
        "department": "Mathematics",
        "urls": [
            "https://math.osu.edu/people/graduate-students",
            "https://math.osu.edu/people/students",
            "https://math.osu.edu/people",
        ],
    },
    {
        "department": "Statistics",
        "urls": [
            "https://stat.osu.edu/people/graduate-students",
            "https://stat.osu.edu/people/students",
            "https://stat.osu.edu/people",
        ],
    },
    {
        "department": "Physics",
        "urls": [
            "https://physics.osu.edu/people/graduate-students",
            "https://physics.osu.edu/people/students",
            "https://physics.osu.edu/people",
        ],
    },
    {
        "department": "Chemistry & Biochemistry",
        "urls": [
            "https://chemistry.osu.edu/people/graduate-students",
            "https://chemistry.osu.edu/people/students",
            "https://chemistry.osu.edu/people",
        ],
    },
    {
        "department": "Earth Sciences",
        "urls": [
            "https://earthsciences.osu.edu/people/graduate-students",
            "https://earthsciences.osu.edu/people/students",
            "https://earthsciences.osu.edu/people",
        ],
    },
    {
        "department": "Biology / EEOB",
        "urls": [
            "https://biology.osu.edu/people/graduate-students",
            "https://eeob.osu.edu/people/graduate-students",
            "https://biology.osu.edu/people/students",
            "https://eeob.osu.edu/people/students",
            "https://eeob.osu.edu/people",
        ],
    },
    {
        "department": "Anthropology",
        "urls": [
            "https://anthropology.osu.edu/people/graduate-students",
            "https://anthropology.osu.edu/people/students",
            "https://anthropology.osu.edu/people",
        ],
    },
    {
        "department": "Classics",
        "urls": [
            "https://classics.osu.edu/people/graduate-students",
            "https://classics.osu.edu/people/students",
            "https://classics.osu.edu/people",
        ],
    },
    {
        "department": "Communication",
        "urls": [
            "https://comm.osu.edu/people/graduate-students",
            "https://comm.osu.edu/people/students",
            "https://comm.osu.edu/people",
        ],
    },
    {
        "department": "Geography",
        "urls": [
            "https://geography.osu.edu/people/graduate-students",
            "https://geography.osu.edu/people/students",
            "https://geography.osu.edu/people",
        ],
    },
]

ENGINEERING_DEPARTMENTS = [
    {
        "department": "Computer Science & Engineering",
        "urls": [
            "https://cse.osu.edu/people/graduate-students",
            "https://cse.osu.edu/people/students",
            "https://cse.osu.edu/people",
        ],
    },
    {
        "department": "Electrical & Computer Engineering",
        "urls": [
            "https://ece.osu.edu/people/graduate-students",
            "https://ece.osu.edu/people/students",
            "https://ece.osu.edu/people",
        ],
    },
    {
        "department": "Mechanical & Aerospace Engineering",
        "urls": [
            "https://mae.osu.edu/people/graduate-students",
            "https://mae.osu.edu/people/students",
            "https://mae.osu.edu/people",
        ],
    },
    {
        "department": "Chemical & Biomolecular Engineering",
        "urls": [
            "https://cbe.osu.edu/people/graduate-students",
            "https://cbe.osu.edu/people/students",
            "https://cbe.osu.edu/people",
        ],
    },
    {
        "department": "Civil, Environmental & Geodetic Engineering",
        "urls": [
            "https://ceg.osu.edu/people/graduate-students",
            "https://ceg.osu.edu/people/students",
            "https://ceg.osu.edu/people",
        ],
    },
    {
        "department": "Biomedical Engineering",
        "urls": [
            "https://bme.osu.edu/people/graduate-students",
            "https://bme.osu.edu/people/students",
            "https://bme.osu.edu/people",
        ],
    },
    {
        "department": "Materials Science & Engineering",
        "urls": [
            "https://mse.osu.edu/people/graduate-students",
            "https://mse.osu.edu/people/students",
            "https://mse.osu.edu/people",
        ],
    },
    {
        "department": "Integrated Systems Engineering",
        "urls": [
            "https://ise.osu.edu/people/graduate-students",
            "https://ise.osu.edu/people/students",
            "https://ise.osu.edu/people",
        ],
    },
]

PROFESSIONAL_SCHOOLS = [
    {
        "department": "Fisher College of Business (PhD)",
        "urls": [
            "https://fisher.osu.edu/phd",
            "https://fisher.osu.edu/graduate/phd/students",
            "https://fisher.osu.edu/people/phd-students",
            "https://fisher.osu.edu/people/students",
        ],
    },
    {
        "department": "Moritz College of Law",
        "urls": [
            "https://moritzlaw.osu.edu/student-organizations",
            "https://moritzlaw.osu.edu/students",
            "https://moritzlaw.osu.edu/people/students",
            "https://moritzlaw.osu.edu/",
        ],
    },
    {
        "department": "College of Public Health",
        "urls": [
            "https://cph.osu.edu/people",
            "https://cph.osu.edu/people/students",
            "https://cph.osu.edu/people/graduate-students",
        ],
    },
    {
        "department": "College of Education & Human Ecology",
        "urls": [
            "https://education.osu.edu/people/students",
            "https://education.osu.edu/people/graduate-students",
            "https://education.osu.edu/people",
            "https://ehe.osu.edu/people/students",
            "https://ehe.osu.edu/people",
        ],
    },
    {
        "department": "College of Social Work",
        "urls": [
            "https://csw.osu.edu/people/students",
            "https://csw.osu.edu/people/graduate-students",
            "https://csw.osu.edu/people",
        ],
    },
    {
        "department": "Knowlton School of Architecture",
        "urls": [
            "https://knowlton.osu.edu/people/students",
            "https://knowlton.osu.edu/people/graduate-students",
            "https://knowlton.osu.edu/people",
        ],
    },
    {
        "department": "College of Pharmacy",
        "urls": [
            "https://pharmacy.osu.edu/people/students",
            "https://pharmacy.osu.edu/people/graduate-students",
            "https://pharmacy.osu.edu/people",
        ],
    },
    {
        "department": "College of Nursing",
        "urls": [
            "https://nursing.osu.edu/people/students",
            "https://nursing.osu.edu/people/graduate-students",
            "https://nursing.osu.edu/people",
        ],
    },
]

ATHLETICS_AND_ORGS = [
    {
        "department": "Athletics (Staff)",
        "urls": [
            "https://ohiostatebuckeyes.com/staff-directory/",
            "https://ohiostatebuckeyes.com/staff/",
        ],
    },
    {
        "department": "The Lantern (Student Newspaper)",
        "urls": [
            "https://www.thelantern.com/staff/",
            "https://www.thelantern.com/about/",
            "https://www.thelantern.com/contact/",
        ],
    },
    {
        "department": "Undergraduate Student Government",
        "urls": [
            "https://usg.osu.edu/about",
            "https://usg.osu.edu/leadership",
            "https://usg.osu.edu/contact",
            "https://usg.osu.edu/",
        ],
    },
    {
        "department": "Graduate Student Organizations",
        "urls": [
            "https://gss.osu.edu/",
            "https://gss.osu.edu/about",
            "https://gss.osu.edu/leadership",
            "https://activities.osu.edu/involvement/student_organizations/",
        ],
    },
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

        # Strategy 1: Extract from structured cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_osu_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:buckeyemail\.)?osu\.edu)',
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
                script_emails = extract_osu_emails(script.string)
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
                pg_text_emails = extract_osu_emails(pg_text)
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
# ATHLETICS SCRAPER (ohiostatebuckeyes.com)
# ============================================================

def scrape_athletics(session):
    """Scrape Ohio State athletics staff directory for emails."""
    results = []
    seen_emails = set()
    department = "Athletics (Staff)"

    urls = [
        "https://ohiostatebuckeyes.com/staff-directory/",
        "https://ohiostatebuckeyes.com/staff/",
    ]

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_osu_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} OSU emails on page")

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

        # Check for staff listing links and follow them
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if 'staff' in href.lower() and full_url != url and 'osu' in full_url:
                log(f"    Following staff link: {full_url}")
                sub_soup, sub_url = get_soup(full_url, session)
                if sub_soup:
                    sub_text = sub_soup.get_text(separator=' ', strip=True)
                    sub_emails = extract_osu_emails(sub_text)
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
    log("OHIO STATE UNIVERSITY EMAIL SCRAPER")
    log("Domains: @osu.edu, @buckeyemail.osu.edu")
    log("=" * 70)

    # ---- Phase 1: Arts & Sciences ----
    log("\n\nPHASE 1: ARTS & SCIENCES DEPARTMENTS")
    log("=" * 70)

    for config in ARTS_SCIENCES_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: Engineering ----
    log("\n\nPHASE 2: ENGINEERING DEPARTMENTS")
    log("=" * 70)

    for config in ENGINEERING_DEPARTMENTS:
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

    for config in PROFESSIONAL_SCHOOLS:
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
        results = scrape_athletics(session)
        n = add_results(results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 5: Student Organizations ----
    log("\n\nPHASE 5: STUDENT ORGANIZATIONS")
    log("=" * 70)

    for config in ATHLETICS_AND_ORGS[1:]:  # Skip athletics (already done)
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
    log(f"Total unique OSU emails: {len(all_results)}")

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
        [c['department'] for c in ARTS_SCIENCES_DEPARTMENTS] +
        [c['department'] for c in ENGINEERING_DEPARTMENTS] +
        [c['department'] for c in PROFESSIONAL_SCHOOLS] +
        [c['department'] for c in ATHLETICS_AND_ORGS]
    )
    depts_with_zero = [d for d in all_depts if d not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in depts_with_zero:
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
