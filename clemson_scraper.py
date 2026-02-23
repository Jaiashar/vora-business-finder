#!/usr/bin/env python3
"""
Clemson University Email Scraper
Scrapes @clemson.edu and @g.clemson.edu emails from:
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/clemson_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/clemson_dept_emails.json'


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_clemson_emails(text):
    """Extract all @clemson.edu and @g.clemson.edu emails from text."""
    pattern = r'[\w.+-]+@(?:g\.)?clemson\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:g\.)?clemson\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract Clemson emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:g\.)?clemson\.edu)',
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
        'ccit@', 'oit@', 'itshelp@', 'gradschool@',
        'studentaffairs@', 'provost@', 'president@',
        'cusg@', 'sga@', 'cufunds@', 'cuathletics@',
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
                            'research', 'clemson',
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
                        'website', 'clemson',
                    ]):
                        return name
            parent = parent.parent

    return ""


# ============================================================
# STRUCTURED EXTRACTION: Person cards / grid layouts
# ============================================================

def extract_from_person_cards(soup, url, department):
    """Extract people from card/grid-based layouts (Clemson sites use various CMS layouts)."""
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
        '.cu-person',
        '.cu-people-card',
        '.cu-listing-item',
        '.person-info',
        '.bio-card',
        '.people-grid-item',
        '.staff-member',
        '.grad-student',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_clemson_emails(text)
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

        # Match profile-like URLs on clemson.edu
        if re.search(r'/people/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
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
    """Scrape an individual profile page for clemson.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_clemson_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# Arts & Sciences departments
# NOTE: Clemson restructured their site. Science depts -> /science/academics/departments/
# CAAH became CAH at /cah/academics/
# CBSHS departments remain at /cbshs/departments/
ARTS_SCIENCES_DEPARTMENTS = [
    {
        "department": "Chemistry",
        "urls": [
            "https://www.clemson.edu/science/academics/departments/chemistry/about/people.html",
            "https://www.clemson.edu/science/academics/departments/chemistry/",
        ],
    },
    {
        "department": "Physics & Astronomy",
        "urls": [
            "https://www.clemson.edu/science/academics/departments/physics/about/people.html",
            "https://www.clemson.edu/science/academics/departments/physics/",
        ],
    },
    {
        "department": "Mathematical & Statistical Sciences",
        "urls": [
            "https://www.clemson.edu/science/academics/departments/mathstat/about/people.html",
            "https://www.clemson.edu/science/academics/departments/mathstat/",
        ],
    },
    {
        "department": "Biological Sciences",
        "urls": [
            "https://www.clemson.edu/science/academics/departments/biosci/about/people.html",
            "https://www.clemson.edu/science/academics/departments/biosci/",
        ],
    },
    {
        "department": "Genetics & Biochemistry",
        "urls": [
            "https://www.clemson.edu/science/academics/departments/genbio/about/people.html",
            "https://www.clemson.edu/science/academics/departments/genbio/",
        ],
    },
    {
        "department": "Environmental Engineering & Earth Sciences",
        "urls": [
            "https://www.clemson.edu/cecas/departments/ese/people/",
            "https://www.clemson.edu/cecas/departments/ese/",
        ],
    },
    # NOTE: English, History, Languages, Philosophy are all on the CAH directory page.
    # They are scraped by the special scrape_cah_directory() function below.
    # Placeholder entries so they show in the department list:
    {
        "department": "English",
        "urls": [],
    },
    {
        "department": "History & Geography",
        "urls": [],
    },
    {
        "department": "Languages",
        "urls": [],
    },
    {
        "department": "Philosophy & Religion",
        "urls": [],
    },
    {
        "department": "Communication",
        "urls": [
            "https://www.clemson.edu/cbshs/departments/communication/people/",
            "https://www.clemson.edu/cbshs/departments/communication/",
        ],
    },
    {
        "department": "Psychology",
        "urls": [
            "https://www.clemson.edu/cbshs/departments/psychology/people.html",
            "https://www.clemson.edu/cbshs/departments/psychology/",
        ],
    },
    {
        "department": "Sociology, Anthropology & Criminal Justice",
        "urls": [
            "https://www.clemson.edu/cbshs/departments/sociology/people/",
            "https://www.clemson.edu/cbshs/departments/sociology/",
        ],
    },
    {
        "department": "Parks, Recreation & Tourism Management",
        "urls": [
            "https://www.clemson.edu/cbshs/departments/prtm/people/",
            "https://www.clemson.edu/cbshs/departments/prtm/",
        ],
    },
]

# Engineering departments
ENGINEERING_DEPARTMENTS = [
    {
        "department": "Electrical & Computer Engineering",
        "urls": [
            "https://www.clemson.edu/cecas/departments/ece/people/",
        ],
    },
    {
        "department": "Mechanical Engineering",
        "urls": [
            "https://www.clemson.edu/cecas/departments/me/people/",
            "https://www.clemson.edu/cecas/departments/me/people/grad-students.html",
        ],
    },
    {
        "department": "Civil Engineering",
        "urls": [
            "https://www.clemson.edu/cecas/departments/ce/people/",
            "https://www.clemson.edu/cecas/departments/ce/people/grad-students.html",
        ],
    },
    {
        "department": "Chemical & Biomolecular Engineering",
        "urls": [
            "https://www.clemson.edu/cecas/departments/chbe/people/",
            "https://www.clemson.edu/cecas/departments/chbe/people/grad-students.html",
        ],
    },
    {
        "department": "School of Computing",
        "urls": [
            "https://www.clemson.edu/cecas/departments/computing/people/grad-students.html",
            "https://www.clemson.edu/cecas/departments/computing/people/",
        ],
    },
    {
        "department": "Environmental Engineering & Earth Sciences (CECAS)",
        "urls": [
            "https://www.clemson.edu/cecas/departments/ese/people/",
            "https://www.clemson.edu/cecas/departments/ese/people/grad-students.html",
        ],
    },
    {
        "department": "Materials Science & Engineering",
        "urls": [
            "https://www.clemson.edu/cecas/departments/mse/people/",
            "https://www.clemson.edu/cecas/departments/mse/people/grad-students.html",
        ],
    },
    {
        "department": "Bioengineering",
        "urls": [
            "https://www.clemson.edu/cecas/departments/bio-eng/people/",
            "https://www.clemson.edu/cecas/departments/bio-eng/people/grad-students.html",
        ],
    },
]

# Professional Schools
PROFESSIONAL_SCHOOLS = [
    {
        "department": "College of Business (PhD)",
        "urls": [
            "https://www.clemson.edu/business/phd/",
            "https://www.clemson.edu/business/phd/students.html",
            "https://www.clemson.edu/business/people/phd-students.html",
            "https://www.clemson.edu/business/people/",
        ],
    },
    {
        "department": "College of Education",
        "urls": [
            "https://www.clemson.edu/education/people/students",
            "https://www.clemson.edu/education/people/students.html",
            "https://www.clemson.edu/education/people/",
            "https://www.clemson.edu/education/faculty-staff/",
            "https://www.clemson.edu/education/faculty-staff/directory.html",
        ],
    },
    {
        "department": "Agriculture, Forestry & Life Sciences",
        "urls": [
            "https://www.clemson.edu/cafls/people/students",
            "https://www.clemson.edu/cafls/people/students.html",
            "https://www.clemson.edu/cafls/people/",
            "https://www.clemson.edu/cafls/faculty-staff/",
            "https://www.clemson.edu/cafls/faculty-staff/directory.html",
        ],
    },
    {
        "department": "College of Architecture, Arts & Humanities",
        "urls": [
            "https://www.clemson.edu/cah/",
        ],
    },
]

# Athletics
ATHLETICS = [
    {
        "department": "Clemson Athletics (Staff)",
        "urls": [
            "https://clemsontigers.com/staff-directory/",
            "https://clemsontigers.com/sports/2017/6/16/staff-html.aspx",
            "https://clemsontigers.com/staff/",
        ],
    },
]

# Student Organizations
STUDENT_ORGS = [
    {
        "department": "Clemson Undergraduate Student Government (CUSG)",
        "urls": [
            "https://www.clemson.edu/studentaffairs/get-involved/cusg/",
            "https://www.clemson.edu/centers-institutes/cusg/",
            "https://custudengovernment.com/",
            "https://www.clemson.edu/studentaffairs/",
        ],
    },
    {
        "department": "The Tiger (Student Newspaper)",
        "urls": [
            "https://thetigercu.com/staff/",
            "https://thetigercu.com/about/",
            "https://thetigercu.com/contact/",
            "https://www.thetigernews.com/about-our-team/",
            "https://www.thetigernews.com/contact-us/",
        ],
    },
    {
        "department": "Graduate Student Government",
        "urls": [
            "https://www.clemson.edu/graduate/students/gsg/",
            "https://www.clemson.edu/graduate/students/organizations.html",
            "https://www.clemson.edu/graduate/",
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
        text_emails = extract_clemson_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:g\.)?clemson\.edu)',
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
                script_emails = extract_clemson_emails(script.string)
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
                pg_text_emails = extract_clemson_emails(pg_text)
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
# CAH (College of Arts & Humanities) DIRECTORY SCRAPER
# ============================================================

def scrape_cah_directory(session):
    """
    Scrape the CAH faculty-staff directory which has all departments in one page.
    Parses department sections to assign the correct department to each person.
    """
    url = "https://www.clemson.edu/cah/about/faculty-staff-directory.html"
    results = []
    seen_emails = set()

    log(f"\n{'=' * 60}")
    log(f"Scraping: CAH Faculty & Staff Directory")
    log(f"{'=' * 60}")
    log(f"  Trying: {url}")

    soup, final_url = get_soup(url, session)
    if soup is None:
        log("  -> Failed to load CAH directory")
        return results

    log(f"  -> Page loaded")

    # The CAH directory has department headers as <li> with anchors like:
    #   <li><a href="#0523">Department of English</a></li>
    # followed by person entries in subsequent <li> items.
    # Map section text patterns to our department names
    dept_mapping = [
        ('department of english', 'English'),
        ('department of history', 'History & Geography'),
        ('department of languages', 'Languages'),
        ('department of philosophy', 'Philosophy & Religion'),
        ('performing arts', 'Performing Arts'),
        ('interdisciplinary', 'Interdisciplinary Studies'),
        ('cah office of the dean', 'CAH Office of the Dean'),
        ('cah student services', 'CAH Student Services'),
        ('cah business support', 'CAH Business Support Services'),
    ]

    # Parse the page structure - iterate through all <li> elements
    current_dept = "College of Arts & Humanities"

    # Get all top-level list items from the directory
    all_lis = soup.find_all('li')

    for li in all_lis:
        text = li.get_text(separator=' ', strip=True)
        text_lower = text.lower()

        # Check if this is a section header - these contain anchor links with #
        # and typically just a department name without an email
        section_link = li.find('a', href=True)
        if section_link:
            href = section_link.get('href', '')
            # Section headers have href like "#0523" (fragment links)
            if href.startswith('#') and href[1:].isdigit():
                for pattern, dept_name in dept_mapping:
                    if pattern in text_lower:
                        current_dept = dept_name
                        break
                continue

        # Extract emails from this list item
        li_emails = extract_clemson_emails(text)
        li_mailto = extract_mailto_emails(li)
        all_emails = list(set(li_emails + li_mailto))

        if not all_emails:
            continue

        for email in all_emails:
            if email in seen_emails or is_admin_email(email):
                continue
            seen_emails.add(email)

            # Try to find name from profile link or bold text
            name = ""
            for tag in li.find_all(['a', 'strong', 'b']):
                tag_text = tag.get_text(strip=True)
                href = tag.get('href', '') if tag.name == 'a' else ''
                if 'mailto:' in href:
                    continue
                if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                    if not any(x in tag_text.lower() for x in [
                        'email', 'contact', 'phone', 'http', 'department',
                        'faculty', 'staff', 'update profile',
                    ]):
                        name = tag_text
                        break

            results.append({
                'email': email,
                'name': name,
                'department': current_dept,
                'source_url': final_url or url,
            })

    # Also do a full-page sweep for any emails missed by the structured parse
    page_text = soup.get_text(separator=' ', strip=True)
    all_page_emails = extract_clemson_emails(page_text)
    all_mailto = extract_mailto_emails(soup)
    for email in list(set(all_page_emails + all_mailto)):
        if email not in seen_emails and not is_admin_email(email):
            seen_emails.add(email)
            name = try_get_name_for_email(soup, email)
            results.append({
                'email': email,
                'name': name,
                'department': 'College of Arts & Humanities',
                'source_url': final_url or url,
            })

    log(f"  TOTAL from CAH directory: {len(results)} emails")

    # Print summary by department
    dept_counts = {}
    for r in results:
        dept_counts[r['department']] = dept_counts.get(r['department'], 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"    {dept}: {count}")

    return results


# ============================================================
# ATHLETICS SCRAPER (clemsontigers.com)
# ============================================================

def scrape_athletics(session):
    """Scrape Clemson Tigers athletics staff directory for emails."""
    results = []
    seen_emails = set()
    department = "Clemson Athletics (Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://clemsontigers.com/staff-directory/",
        "https://clemsontigers.com/sports/2017/6/16/staff-html.aspx",
        "https://clemsontigers.com/staff/",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_clemson_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} Clemson emails on page")

        # Try to associate names with emails from the staff directory
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if 'mailto:' in href.lower():
                match = re.search(r'mailto:\s*([\w.+-]+@(?:g\.)?clemson\.edu)', href, re.IGNORECASE)
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

        # Check for staff listing sub-pages
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if ('staff' in href.lower() or 'coach' in href.lower()) and full_url != url:
                if 'clemson' in full_url.lower():
                    log(f"    Following staff link: {full_url}")
                    sub_soup, sub_url = get_soup(full_url, session)
                    if sub_soup:
                        sub_text = sub_soup.get_text(separator=' ', strip=True)
                        sub_emails = extract_clemson_emails(sub_text)
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
    log("CLEMSON UNIVERSITY EMAIL SCRAPER")
    log("Domains: @clemson.edu, @g.clemson.edu")
    log("=" * 70)

    # ---- Phase 0: CAH Directory (English, History, Languages, Philosophy) ----
    log("\n\nPHASE 0: COLLEGE OF ARTS & HUMANITIES DIRECTORY")
    log("=" * 70)

    try:
        cah_results = scrape_cah_directory(session)
        n = add_results(cah_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping CAH directory: {e}")

    # ---- Phase 1: Arts & Sciences ----
    log("\n\nPHASE 1: ARTS & SCIENCES DEPARTMENTS")
    log("=" * 70)

    for config in ARTS_SCIENCES_DEPARTMENTS:
        if not config['urls']:
            log(f"\n  Skipping {config['department']} (handled by CAH directory)")
            continue
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
    log(f"Total unique Clemson emails: {len(all_results)}")

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
