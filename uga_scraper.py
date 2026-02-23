#!/usr/bin/env python3
"""
University of Georgia (UGA) Email Scraper
Scrapes @uga.edu emails from department directories, professional schools,
athletics, and student organizations.

UGA departments typically use a Drupal-based CMS with /directory/graduate-students paths.
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

# Arts & Sciences (Franklin College) - /directory/graduate-students pattern
FRANKLIN_COLLEGE = [
    {"department": "Economics", "urls": [
        "https://economics.uga.edu/directory/graduate-students",
        "https://economics.uga.edu/people/graduate-students",
        "https://economics.uga.edu/directory/people",
        "https://economics.uga.edu/directory",
    ]},
    {"department": "Political Science / Public Affairs (SPIA)", "urls": [
        "https://spia.uga.edu/directory/graduate-students",
        "https://spia.uga.edu/people/graduate-students",
        "https://spia.uga.edu/directory/students",
        "https://spia.uga.edu/directory",
    ]},
    {"department": "Sociology", "urls": [
        "https://sociology.uga.edu/directory/graduate-students",
        "https://sociology.uga.edu/people/graduate-students",
        "https://sociology.uga.edu/directory/students",
        "https://sociology.uga.edu/directory",
    ]},
    {"department": "Psychology", "urls": [
        "https://psychology.uga.edu/directory/graduate-students",
        "https://psychology.uga.edu/people/graduate-students",
        "https://psychology.uga.edu/directory/students",
        "https://psychology.uga.edu/directory",
    ]},
    {"department": "History", "urls": [
        "https://history.uga.edu/directory/graduate-students",
        "https://history.uga.edu/people/graduate-students",
        "https://history.uga.edu/directory/students",
        "https://history.uga.edu/directory",
    ]},
    {"department": "English", "urls": [
        "https://english.uga.edu/directory/graduate-students",
        "https://english.uga.edu/people/graduate-students",
        "https://english.uga.edu/directory/students",
        "https://english.uga.edu/directory",
    ]},
    {"department": "Philosophy", "urls": [
        "https://philosophy.uga.edu/directory/graduate-students",
        "https://philosophy.uga.edu/people/graduate-students",
        "https://philosophy.uga.edu/directory/students",
        "https://philosophy.uga.edu/directory",
    ]},
    {"department": "Linguistics", "urls": [
        "https://linguistics.uga.edu/directory/graduate-students",
        "https://linguistics.uga.edu/people/graduate-students",
        "https://linguistics.uga.edu/directory/students",
        "https://linguistics.uga.edu/directory",
    ]},
    {"department": "Mathematics", "urls": [
        "https://math.uga.edu/directory/graduate-students",
        "https://math.uga.edu/people/graduate-students",
        "https://math.uga.edu/directory/students",
        "https://math.uga.edu/directory",
    ]},
    {"department": "Statistics", "urls": [
        "https://stat.uga.edu/directory/graduate-students",
        "https://stat.uga.edu/people/graduate-students",
        "https://stat.uga.edu/directory/students",
        "https://stat.uga.edu/directory",
    ]},
    {"department": "Physics & Astronomy", "urls": [
        "https://physast.uga.edu/directory/graduate-students",
        "https://physast.uga.edu/people/graduate-students",
        "https://physast.uga.edu/directory/students",
        "https://physast.uga.edu/directory",
    ]},
    {"department": "Chemistry", "urls": [
        "https://chem.uga.edu/directory/graduate-students",
        "https://chem.uga.edu/people/graduate-students",
        "https://chem.uga.edu/directory/students",
        "https://chem.uga.edu/directory",
    ]},
    {"department": "Geology", "urls": [
        "https://geology.uga.edu/directory/graduate-students",
        "https://geology.uga.edu/people/graduate-students",
        "https://geology.uga.edu/directory/students",
        "https://geology.uga.edu/directory",
    ]},
    {"department": "Cellular Biology", "urls": [
        "https://cellbio.uga.edu/directory/graduate-students",
        "https://cellbio.uga.edu/people/graduate-students",
        "https://cellbio.uga.edu/directory/students",
        "https://cellbio.uga.edu/directory",
    ]},
    {"department": "Genetics", "urls": [
        "https://genetics.uga.edu/directory/graduate-students",
        "https://genetics.uga.edu/people/graduate-students",
        "https://genetics.uga.edu/directory/students",
        "https://genetics.uga.edu/directory",
    ]},
    {"department": "Plant Biology", "urls": [
        "https://plantbio.uga.edu/directory/graduate-students",
        "https://plantbio.uga.edu/people/graduate-students",
        "https://plantbio.uga.edu/directory/students",
        "https://plantbio.uga.edu/directory",
    ]},
    {"department": "Ecology", "urls": [
        "https://ecology.uga.edu/directory/graduate-students",
        "https://ecology.uga.edu/people/graduate-students",
        "https://ecology.uga.edu/directory/students",
        "https://ecology.uga.edu/directory",
    ]},
    {"department": "Anthropology", "urls": [
        "https://anthropology.uga.edu/directory/graduate-students",
        "https://anthropology.uga.edu/people/graduate-students",
        "https://anthropology.uga.edu/directory/students",
        "https://anthropology.uga.edu/directory",
    ]},
    {"department": "Geography", "urls": [
        "https://geography.uga.edu/directory/graduate-students",
        "https://geography.uga.edu/people/graduate-students",
        "https://geography.uga.edu/directory/students",
        "https://geography.uga.edu/directory",
    ]},
    {"department": "Religion", "urls": [
        "https://religion.uga.edu/directory/graduate-students",
        "https://religion.uga.edu/people/graduate-students",
        "https://religion.uga.edu/directory/students",
        "https://religion.uga.edu/directory",
    ]},
]

# Engineering
ENGINEERING = [
    {"department": "College of Engineering", "urls": [
        "https://engineering.uga.edu/directory/graduate-students",
        "https://engineering.uga.edu/people/graduate-students",
        "https://engineering.uga.edu/directory/students",
        "https://engineering.uga.edu/directory",
        "https://engineering.uga.edu/people",
    ]},
]

# Professional Schools
PROFESSIONAL = [
    {"department": "Terry College of Business (PhD)", "urls": [
        "https://terry.uga.edu/phd/",
        "https://terry.uga.edu/phd/students/",
        "https://terry.uga.edu/phd/current-students/",
        "https://terry.uga.edu/directory/graduate-students",
        "https://terry.uga.edu/directory/phd-students",
        "https://terry.uga.edu/directory",
    ]},
    {"department": "College of Education", "urls": [
        "https://coe.uga.edu/directory/graduate-students",
        "https://coe.uga.edu/people/graduate-students",
        "https://coe.uga.edu/directory/students",
        "https://coe.uga.edu/directory",
    ]},
    {"department": "College of Public Health", "urls": [
        "https://publichealth.uga.edu/directory/graduate-students",
        "https://publichealth.uga.edu/people/graduate-students",
        "https://publichealth.uga.edu/directory/students",
        "https://publichealth.uga.edu/directory",
    ]},
    {"department": "Grady College of Journalism", "urls": [
        "https://journalism.uga.edu/directory/graduate-students",
        "https://journalism.uga.edu/people/graduate-students",
        "https://journalism.uga.edu/directory/students",
        "https://journalism.uga.edu/directory",
    ]},
    {"department": "School of Social Work", "urls": [
        "https://socialwork.uga.edu/directory/graduate-students",
        "https://socialwork.uga.edu/people/graduate-students",
        "https://socialwork.uga.edu/directory/students",
        "https://socialwork.uga.edu/directory",
    ]},
    {"department": "College of Pharmacy", "urls": [
        "https://pharmacy.uga.edu/directory/graduate-students",
        "https://pharmacy.uga.edu/people/graduate-students",
        "https://pharmacy.uga.edu/directory/students",
        "https://pharmacy.uga.edu/directory",
    ]},
    {"department": "College of Veterinary Medicine", "urls": [
        "https://vet.uga.edu/directory/graduate-students",
        "https://vet.uga.edu/people/graduate-students",
        "https://vet.uga.edu/education/academics/graduate-studies/",
        "https://vet.uga.edu/directory",
    ]},
    {"department": "School of Law", "urls": [
        "https://law.uga.edu/student-organizations",
        "https://law.uga.edu/directory/students",
        "https://law.uga.edu/directory",
    ]},
]

# Athletics
ATHLETICS = [
    {"department": "Athletics (Staff)", "urls": [
        "https://georgiadogs.com/staff-directory",
        "https://georgiadogs.com/sports/2017/6/16/staff-html.aspx",
    ]},
]

# Student Orgs
STUDENT_ORGS = [
    {"department": "Student Government Association", "urls": [
        "https://sga.uga.edu/",
        "https://sga.uga.edu/executive-branch/",
        "https://sga.uga.edu/about/",
        "https://sga.uga.edu/leadership/",
        "https://sga.uga.edu/officers/",
        "https://sga.uga.edu/directory/",
    ]},
    {"department": "The Red & Black (Student Newspaper)", "urls": [
        "https://www.redandblack.com/staff/",
        "https://www.redandblack.com/contact/",
        "https://www.redandblack.com/about/",
    ]},
]


# ============================================================
# EMAIL EXTRACTION
# ============================================================

def extract_uga_emails(text):
    """Extract all @uga.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*uga\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup):
    """Extract uga.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uga\.edu)',
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
        'police@', 'records@', 'bursar@', 'payroll@', 'ugagrad@',
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
            return None, None
    except Exception as e:
        log(f"    Error fetching {url}: {e}")
        return None, None


# ============================================================
# NAME EXTRACTION STRATEGIES
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
    """Extract people from card/grid-based layouts (common in UGA Drupal sites)."""
    results = []
    seen_emails = set()

    # Selectors common in UGA department sites (Drupal-based)
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
        '.directory-item',
        '.people-listing',
        '.staff-listing',
        '.view-people',
        '.view-directory',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_uga_emails(text)
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

        # Match profile-like URLs: /directory/people/john-doe, /people/john-doe
        if re.search(r'/(directory|people)/(people/)?[a-z][\w-]+/?$', full_url, re.IGNORECASE):
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
    """Scrape an individual profile page for uga.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_uga_emails(text)
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

        # Check if page has relevant content
        has_people = any(kw in page_text.lower() for kw in [
            'graduate student', 'grad student', 'phd student',
            'doctoral student', '@uga.edu', 'email',
            'graduate assistant', 'teaching assistant',
        ])

        if not has_people and len(extract_uga_emails(page_text)) == 0:
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
        text_emails = extract_uga_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*uga\.edu)',
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
                script_emails = extract_uga_emails(script.string)
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
                pg_text_emails = extract_uga_emails(pg_text)
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
        log(f"    {r['email']:<40} | {r['name']}")

    return results


# ============================================================
# ATHLETICS-SPECIFIC SCRAPER
# ============================================================

def scrape_athletics(session):
    """Scrape Georgia Bulldogs athletics staff directory for @uga.edu emails."""
    results = []
    seen_emails = set()
    department = "Athletics (Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://georgiadogs.com/staff-directory",
        "https://georgiadogs.com/sports/2017/6/16/staff-html.aspx",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_uga_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} raw emails")

        # Also check for staff cards
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
                pg_emails = extract_uga_emails(pg_text)
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

        time.sleep(1)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# TERRY BUSINESS SCHOOL - SPECIAL HANDLING
# ============================================================

def scrape_terry_phd(session):
    """Scrape Terry College of Business PhD students page - may have different layout."""
    results = []
    seen_emails = set()
    department = "Terry College of Business (PhD)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    # Try the main PhD page and any sub-program pages
    urls = [
        "https://terry.uga.edu/phd/",
        "https://terry.uga.edu/phd/students/",
        "https://terry.uga.edu/phd/current-students/",
        "https://terry.uga.edu/phd/student-directory/",
        "https://terry.uga.edu/directory/?role=phd",
        "https://terry.uga.edu/directory/graduate-students",
    ]

    # Also try individual department PhD pages
    terry_depts = [
        "https://terry.uga.edu/management/phd/students/",
        "https://terry.uga.edu/management/phd/",
        "https://terry.uga.edu/finance/phd/students/",
        "https://terry.uga.edu/finance/phd/",
        "https://terry.uga.edu/marketing/phd/students/",
        "https://terry.uga.edu/marketing/phd/",
        "https://terry.uga.edu/mis/phd/students/",
        "https://terry.uga.edu/mis/phd/",
        "https://terry.uga.edu/accounting/phd/students/",
        "https://terry.uga.edu/accounting/phd/",
        "https://terry.uga.edu/economics/phd/students/",
        "https://terry.uga.edu/risk-management/phd/students/",
        "https://terry.uga.edu/realestate/phd/students/",
    ]

    all_urls = urls + terry_depts

    for url in all_urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_uga_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        if not all_emails:
            log(f"    -> No emails found")
            # Look for profile links
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
    log("UNIVERSITY OF GEORGIA EMAIL SCRAPER")
    log("=" * 70)

    # ---- Phase 1: Franklin College (Arts & Sciences) ----
    log("\n\n" + "=" * 70)
    log("PHASE 1: Franklin College of Arts & Sciences")
    log("=" * 70)

    for config in FRANKLIN_COLLEGE:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Phase 2: Engineering ----
    log("\n\n" + "=" * 70)
    log("PHASE 2: College of Engineering")
    log("=" * 70)

    for config in ENGINEERING:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Phase 3: Professional Schools ----
    log("\n\n" + "=" * 70)
    log("PHASE 3: Professional Schools")
    log("=" * 70)

    # Terry Business - special handling
    try:
        terry_results = scrape_terry_phd(session)
        n = add_results(terry_results)
        log(f"  => {n} new unique emails added")
    except Exception as e:
        log(f"  ERROR scraping Terry: {e}")

    # Other professional schools
    for config in PROFESSIONAL:
        if 'Terry' in config['department']:
            continue  # Already handled above
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Phase 4: Athletics ----
    log("\n\n" + "=" * 70)
    log("PHASE 4: Athletics")
    log("=" * 70)

    try:
        athletics_results = scrape_athletics(session)
        n = add_results(athletics_results)
        log(f"  => {n} new unique emails added")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 5: Student Orgs ----
    log("\n\n" + "=" * 70)
    log("PHASE 5: Student Organizations")
    log("=" * 70)

    for config in STUDENT_ORGS:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @uga.edu emails: {len(all_results)}")

    # Save CSV
    output_csv = '/Users/jaiashar/Documents/VoraBusinessFinder/uga_dept_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")

    # Save JSON
    output_json = '/Users/jaiashar/Documents/VoraBusinessFinder/uga_dept_emails.json'
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

    all_depts = set(c['department'] for group in [FRANKLIN_COLLEGE, ENGINEERING, PROFESSIONAL, ATHLETICS, STUDENT_ORGS] for c in group)
    all_depts.add("Terry College of Business (PhD)")
    depts_with_zero = all_depts - set(dept_counts.keys())
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in sorted(depts_with_zero):
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
