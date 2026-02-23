#!/usr/bin/env python3
"""
University of Alabama Email Scraper
Scrapes @ua.edu and @crimson.ua.edu emails from:
- Arts & Sciences department graduate student directories
- Engineering department directories
- Professional school directories (Culverhouse, Law, Education, Nursing, Social Work)
- Athletics staff directory (rolltide.com)
- Student organizations (SGA, Crimson White)

UA departments typically use a WordPress-based CMS with /people/graduate-students/ paths.
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


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# Arts & Sciences departments -- /people/graduate-students/ pattern
ARTS_SCIENCES = [
    {"department": "Economics", "urls": [
        "https://economics.ua.edu/people/graduate-students/",
        "https://economics.ua.edu/people/students/",
        "https://economics.ua.edu/people/",
        "https://economics.ua.edu/directory/",
    ]},
    {"department": "Political Science", "urls": [
        "https://psc.ua.edu/people/graduate-students/",
        "https://psc.ua.edu/people/students/",
        "https://psc.ua.edu/people/",
        "https://psc.ua.edu/directory/",
    ]},
    {"department": "Sociology", "urls": [
        "https://sociology.ua.edu/people/graduate-students/",
        "https://sociology.ua.edu/people/students/",
        "https://sociology.ua.edu/people/",
        "https://sociology.ua.edu/directory/",
    ]},
    {"department": "Psychology", "urls": [
        "https://psychology.ua.edu/people/graduate-students/",
        "https://psychology.ua.edu/people/students/",
        "https://psychology.ua.edu/people/",
        "https://psychology.ua.edu/directory/",
    ]},
    {"department": "History", "urls": [
        "https://history.ua.edu/people/graduate-students/",
        "https://history.ua.edu/people/students/",
        "https://history.ua.edu/people/",
        "https://history.ua.edu/directory/",
    ]},
    {"department": "English", "urls": [
        "https://english.ua.edu/people/graduate-students/",
        "https://english.ua.edu/people/students/",
        "https://english.ua.edu/people/",
        "https://english.ua.edu/directory/",
    ]},
    {"department": "Philosophy", "urls": [
        "https://philosophy.ua.edu/people/graduate-students/",
        "https://philosophy.ua.edu/people/students/",
        "https://philosophy.ua.edu/people/",
        "https://philosophy.ua.edu/directory/",
    ]},
    {"department": "Mathematics", "urls": [
        "https://math.ua.edu/people/graduate-students/",
        "https://math.ua.edu/people/students/",
        "https://math.ua.edu/people/",
        "https://math.ua.edu/directory/",
    ]},
    {"department": "Physics", "urls": [
        "https://physics.ua.edu/people/graduate-students/",
        "https://physics.ua.edu/people/students/",
        "https://physics.ua.edu/people/",
        "https://physics.ua.edu/directory/",
    ]},
    {"department": "Chemistry", "urls": [
        "https://chemistry.ua.edu/people/graduate-students/",
        "https://chemistry.ua.edu/people/students/",
        "https://chemistry.ua.edu/people/",
        "https://chemistry.ua.edu/directory/",
    ]},
    {"department": "Geological Sciences", "urls": [
        "https://geo.ua.edu/people/graduate-students/",
        "https://geo.ua.edu/people/students/",
        "https://geo.ua.edu/people/",
        "https://geo.ua.edu/directory/",
    ]},
    {"department": "Biological Sciences", "urls": [
        "https://biology.ua.edu/people/graduate-students/",
        "https://biology.ua.edu/people/students/",
        "https://biology.ua.edu/people/",
        "https://biology.ua.edu/directory/",
    ]},
    {"department": "Anthropology", "urls": [
        "https://anthropology.ua.edu/people/graduate-students/",
        "https://anthropology.ua.edu/people/students/",
        "https://anthropology.ua.edu/people/",
        "https://anthropology.ua.edu/directory/",
    ]},
    {"department": "Computer Science", "urls": [
        "https://cs.ua.edu/people/graduate-students/",
        "https://cs.ua.edu/people/students/",
        "https://cs.ua.edu/people/",
        "https://cs.ua.edu/directory/",
    ]},
]

# Engineering departments
ENGINEERING = [
    {"department": "Electrical & Computer Engineering", "urls": [
        "https://ece.ua.edu/people/graduate-students/",
        "https://ece.ua.edu/people/students/",
        "https://ece.ua.edu/people/",
        "https://ece.ua.edu/directory/",
    ]},
    {"department": "Mechanical Engineering", "urls": [
        "https://me.ua.edu/people/graduate-students/",
        "https://me.ua.edu/people/students/",
        "https://me.ua.edu/people/",
        "https://me.ua.edu/directory/",
    ]},
    {"department": "Civil Engineering", "urls": [
        "https://ce.ua.edu/people/graduate-students/",
        "https://ce.ua.edu/people/students/",
        "https://ce.ua.edu/people/",
        "https://ce.ua.edu/directory/",
    ]},
    {"department": "Chemical Engineering", "urls": [
        "https://che.ua.edu/people/graduate-students/",
        "https://che.ua.edu/people/students/",
        "https://che.ua.edu/people/",
        "https://che.ua.edu/directory/",
    ]},
    {"department": "Aerospace Engineering", "urls": [
        "https://aem.ua.edu/people/graduate-students/",
        "https://aem.ua.edu/people/students/",
        "https://aem.ua.edu/people/",
        "https://aem.ua.edu/directory/",
    ]},
    {"department": "Metallurgical & Materials Engineering", "urls": [
        "https://mte.ua.edu/people/graduate-students/",
        "https://mte.ua.edu/people/students/",
        "https://mte.ua.edu/people/",
        "https://mte.ua.edu/directory/",
    ]},
]

# Professional Schools
PROFESSIONAL = [
    {"department": "Culverhouse College of Business", "urls": [
        "https://culverhouse.ua.edu/people/graduate-students/",
        "https://culverhouse.ua.edu/people/students/",
        "https://culverhouse.ua.edu/people/phd-students/",
        "https://culverhouse.ua.edu/phd/",
        "https://culverhouse.ua.edu/phd/students/",
        "https://culverhouse.ua.edu/directory/",
        "https://culverhouse.ua.edu/",
    ]},
    {"department": "School of Law", "urls": [
        "https://law.ua.edu/student-organizations/",
        "https://law.ua.edu/students/student-organizations/",
        "https://law.ua.edu/students/",
        "https://law.ua.edu/people/students/",
        "https://law.ua.edu/directory/",
        "https://law.ua.edu/",
    ]},
    {"department": "College of Education", "urls": [
        "https://education.ua.edu/people/students/",
        "https://education.ua.edu/people/graduate-students/",
        "https://education.ua.edu/people/",
        "https://education.ua.edu/directory/",
    ]},
    {"department": "Capstone College of Nursing", "urls": [
        "https://nursing.ua.edu/people/students/",
        "https://nursing.ua.edu/people/graduate-students/",
        "https://nursing.ua.edu/people/",
        "https://nursing.ua.edu/directory/",
    ]},
    {"department": "School of Social Work", "urls": [
        "https://socialwork.ua.edu/people/students/",
        "https://socialwork.ua.edu/people/graduate-students/",
        "https://socialwork.ua.edu/people/",
        "https://socialwork.ua.edu/directory/",
    ]},
    {"department": "College of Communication & Information Sciences", "urls": [
        "https://cis.ua.edu/people/graduate-students/",
        "https://cis.ua.edu/people/students/",
        "https://cis.ua.edu/people/",
    ]},
    {"department": "School of Music", "urls": [
        "https://music.ua.edu/people/graduate-students/",
        "https://music.ua.edu/people/students/",
        "https://music.ua.edu/people/",
    ]},
]

# Athletics
ATHLETICS = [
    {"department": "Crimson Tide Athletics (Staff)", "urls": [
        "https://rolltide.com/staff-directory",
        "https://rolltide.com/staff-directory/",
        "https://www.rolltide.com/staff-directory",
        "https://www.rolltide.com/staff-directory/",
    ]},
]

# Student Orgs
STUDENT_ORGS = [
    {"department": "Student Government Association", "urls": [
        "https://sga.ua.edu/",
        "https://sga.ua.edu/about/",
        "https://sga.ua.edu/executive-branch/",
        "https://sga.ua.edu/leadership/",
        "https://sga.ua.edu/officers/",
        "https://sga.ua.edu/directory/",
        "https://sga.ua.edu/contact/",
    ]},
    {"department": "The Crimson White (Student Newspaper)", "urls": [
        "https://cw.ua.edu/staff/",
        "https://cw.ua.edu/contact/",
        "https://cw.ua.edu/about/",
        "https://cw.ua.edu/",
        "https://thecrimsonwhite.com/staff/",
        "https://thecrimsonwhite.com/contact/",
        "https://thecrimsonwhite.com/about/",
        "https://thecrimsonwhite.com/",
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
        # Fix phone-number prefixed artifacts
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
# STRUCTURED EXTRACTION: Card/grid-based people listings
# ============================================================

def extract_from_person_cards(soup, url, department):
    """Extract people from card/grid-based layouts (common in UA WordPress sites)."""
    results = []
    seen_emails = set()

    # Selectors common in UA department sites (WordPress-based)
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
        '.wp-block-column',
        '.entry-content .wp-block-group',
        '[class*="grid-item"]',
        '[class*="team-member"]',
        '[class*="faculty"]',
        '[class*="staff-listing"]',
        '.ua-people-listing',
        '.ua-directory-item',
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

        # Match profile-like URLs on ua.edu
        if re.search(r'/people/[\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'staff', 'administration',
                    'directory', 'about', 'contact',
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
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

        # Strategy 3: Check for obfuscated emails
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

        # Strategy 4: Check JavaScript/script tags for emails
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

        # Strategy 6: If few emails found, try profile links
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
# ATHLETICS SCRAPER (rolltide.com)
# ============================================================

def scrape_athletics(session):
    """Scrape rolltide.com athletics staff directory for UA emails."""
    results = []
    seen_emails = set()
    department = "Crimson Tide Athletics (Staff)"

    urls_to_try = [
        "https://rolltide.com/staff-directory",
        "https://rolltide.com/staff-directory/",
        "https://www.rolltide.com/staff-directory",
        "https://www.rolltide.com/staff-directory/",
        "https://rolltide.com/sports/2018/6/11/staff-directory.aspx",
    ]

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        log(f"    -> Page loaded (final URL: {final_url})")

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_ua_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} raw UA emails")

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

        # Also look for staff cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Check script tags
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

        if results:
            break
        time.sleep(1)

    # Also try individual sport staff pages
    sport_pages = [
        "https://rolltide.com/sports/football/coaches",
        "https://rolltide.com/sports/mens-basketball/coaches",
        "https://rolltide.com/sports/womens-basketball/coaches",
        "https://rolltide.com/sports/baseball/coaches",
        "https://rolltide.com/sports/softball/coaches",
        "https://rolltide.com/sports/mens-tennis/coaches",
        "https://rolltide.com/sports/gymnastics/coaches",
        "https://rolltide.com/sports/swimming-and-diving/coaches",
        "https://rolltide.com/sports/track-and-field/coaches",
        "https://rolltide.com/sports/womens-soccer/coaches",
        "https://rolltide.com/sports/volleyball/coaches",
        "https://rolltide.com/sports/golf/coaches",
    ]

    for url in sport_pages:
        log(f"  Trying sport page: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_ua_emails(page_text)
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
# CULVERHOUSE BUSINESS SCHOOL - SPECIAL HANDLING
# ============================================================

def scrape_culverhouse(session):
    """Scrape Culverhouse College of Business - may have different layout."""
    results = []
    seen_emails = set()
    department = "Culverhouse College of Business"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    # Try the main directory and sub-department pages
    urls = [
        "https://culverhouse.ua.edu/people/graduate-students/",
        "https://culverhouse.ua.edu/people/students/",
        "https://culverhouse.ua.edu/people/phd-students/",
        "https://culverhouse.ua.edu/phd/",
        "https://culverhouse.ua.edu/phd/students/",
        "https://culverhouse.ua.edu/directory/",
        "https://culverhouse.ua.edu/",
    ]

    # Also try sub-departments
    culverhouse_subs = [
        "https://culverhouse.ua.edu/management/people/",
        "https://culverhouse.ua.edu/finance/people/",
        "https://culverhouse.ua.edu/marketing/people/",
        "https://culverhouse.ua.edu/accounting/people/",
        "https://culverhouse.ua.edu/economics/people/",
        "https://culverhouse.ua.edu/information-systems/people/",
    ]

    all_urls = urls + culverhouse_subs

    for url in all_urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_ua_emails(page_text)
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
    log("UNIVERSITY OF ALABAMA EMAIL SCRAPER")
    log("Domains: @ua.edu, @crimson.ua.edu")
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

    # ---- Phase 2: Engineering ----
    log("\n\nPHASE 2: ENGINEERING DEPARTMENTS")
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

    # Culverhouse Business - special handling
    try:
        culverhouse_results = scrape_culverhouse(session)
        n = add_results(culverhouse_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping Culverhouse: {e}")

    # Other professional schools
    for config in PROFESSIONAL:
        if 'Culverhouse' in config['department']:
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

    all_dept_names = (
        [c['department'] for c in ARTS_SCIENCES] +
        [c['department'] for c in ENGINEERING] +
        [c['department'] for c in PROFESSIONAL] +
        ["Crimson Tide Athletics (Staff)"] +
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
