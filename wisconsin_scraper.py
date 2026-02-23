#!/usr/bin/env python3
"""
University of Wisconsin-Madison Email Scraper
Scrapes @wisc.edu emails from department graduate student directories,
engineering people pages, professional schools, athletics, and student organizations.

Key strategies:
- L&S departments: listing page → extract /staff/<name>/ profile links → scrape each profile for mailto: email
- Engineering (engineering.wisc.edu): WP REST API /wp-json/wp/v2/person with department taxonomy filter
- Professional schools, athletics, student orgs: direct page scraping + profile scraping
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
import warnings
from urllib.parse import urljoin
import urllib3

# Suppress SSL warnings for sites with cert issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')


def log(msg):
    print(msg, flush=True)


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# ============================================================
# ARTS & SCIENCES (L&S) DEPARTMENTS
# ============================================================

ARTS_SCIENCES_DEPARTMENTS = [
    {"department": "Economics", "urls": [
        "https://econ.wisc.edu/people/graduate-students/",
        "https://econ.wisc.edu/people/phd-student-directory/",
        "https://econ.wisc.edu/people/",
    ]},
    {"department": "Political Science", "urls": [
        "https://polisci.wisc.edu/graduate-students/",
        "https://polisci.wisc.edu/people/graduate-students/",
    ]},
    {"department": "Sociology", "urls": [
        "https://sociology.wisc.edu/people/graduate-students/",
        "https://sociology.wisc.edu/graduate-students/",
        "https://sociology.wisc.edu/people/students/",
        "https://sociology.wisc.edu/people/",
    ]},
    {"department": "Psychology", "urls": [
        "https://psych.wisc.edu/people/graduate-students/",
        "https://psych.wisc.edu/people/students/",
        "https://psych.wisc.edu/people/",
    ]},
    {"department": "History", "urls": [
        "https://history.wisc.edu/people/graduate-students/",
        "https://history.wisc.edu/people-main/graduate-students/",
    ]},
    {"department": "English", "urls": [
        "https://english.wisc.edu/people/graduate-students/",
        "https://english.wisc.edu/graduate-students/",
        "https://english.wisc.edu/people/students/",
        "https://english.wisc.edu/people/",
    ]},
    {"department": "Philosophy", "urls": [
        "https://philosophy.wisc.edu/people/graduate-students/",
        "https://philosophy.wisc.edu/graduate-students/",
        "https://philosophy.wisc.edu/people/students/",
        "https://philosophy.wisc.edu/people/",
    ]},
    {"department": "Linguistics", "urls": [
        "https://linguistics.wisc.edu/people/graduate-students/",
        "https://langsci.wisc.edu/graduate-students/",
        "https://langsci.wisc.edu/people/graduate-students/",
        "https://langsci.wisc.edu/people/",
    ]},
    {"department": "Mathematics", "urls": [
        "https://math.wisc.edu/people/graduate-students/",
        "https://math.wisc.edu/people/students/",
        "https://math.wisc.edu/people/",
    ]},
    {"department": "Statistics", "urls": [
        "https://stat.wisc.edu/people/graduate-students/",
        "https://stat.wisc.edu/ms-and-phd-graduate-student-directory/",
        "https://stat.wisc.edu/people/",
    ]},
    {"department": "Physics", "urls": [
        "https://physics.wisc.edu/people/graduate-students/",
        "https://www.physics.wisc.edu/people/phd-students/",
        "https://www.physics.wisc.edu/people/mspqc-students/",
    ]},
    {"department": "Chemistry", "urls": [
        "https://chem.wisc.edu/people/graduate-students/",
        "https://chem.wisc.edu/people/students/",
        "https://chem.wisc.edu/people/",
    ]},
    {"department": "Geoscience", "urls": [
        "https://geoscience.wisc.edu/people/graduate-students/",
        "https://geoscience.wisc.edu/people/students/",
        "https://geoscience.wisc.edu/people/",
    ]},
    {"department": "Botany", "urls": [
        "https://botany.wisc.edu/people/graduate-students/",
        "https://botany.wisc.edu/department-directory/",
        "https://botany.wisc.edu/people/",
    ]},
    {"department": "Integrative Biology (Zoology)", "urls": [
        "https://zoology.wisc.edu/people/graduate-students/",
        "https://integrativebiology.wisc.edu/people/graduate-students/",
        "https://zoology.wisc.edu/people/students/",
        "https://zoology.wisc.edu/people/",
        "https://integrativebiology.wisc.edu/people/",
    ]},
    {"department": "Anthropology", "urls": [
        "https://anthropology.wisc.edu/people/graduate-students/",
        "https://anthropology.wisc.edu/graduate-students/",
        "https://anthropology.wisc.edu/people/students/",
        "https://anthropology.wisc.edu/people/",
    ]},
    {"department": "Geography", "urls": [
        "https://geography.wisc.edu/people/graduate-students/",
        "https://geography.wisc.edu/graduate-students/",
        "https://geography.wisc.edu/people/students/",
        "https://geography.wisc.edu/people/",
    ]},
    {"department": "Journalism & Mass Communication", "urls": [
        "https://journalism.wisc.edu/people/graduate-students/",
        "https://journalism.wisc.edu/graduate-students/",
        "https://journalism.wisc.edu/people/students/",
        "https://journalism.wisc.edu/people/",
    ]},
    {"department": "Music", "urls": [
        "https://music.wisc.edu/people/graduate-students/",
        "https://music.wisc.edu/graduate-students/",
        "https://music.wisc.edu/people/students/",
        "https://music.wisc.edu/people/",
    ]},
]

# ============================================================
# ENGINEERING DEPARTMENTS (via WP REST API)
# Department taxonomy IDs on engineering.wisc.edu
# ============================================================

ENGINEERING_API_DEPARTMENTS = [
    {"department": "Computer Sciences", "dept_id": None, "urls": [
        "https://www.cs.wisc.edu/people/graduate-students/",
        "https://www.cs.wisc.edu/people/students/",
        "https://www.cs.wisc.edu/people/phd-students/",
        "https://www.cs.wisc.edu/people/",
    ]},
    {"department": "Electrical & Computer Engineering", "dept_id": 2388},
    {"department": "Mechanical Engineering", "dept_id": 2392},
    {"department": "Civil & Environmental Engineering", "dept_id": 2387},
    {"department": "Biomedical Engineering", "dept_id": 2371},
    {"department": "Chemical & Biological Engineering", "dept_id": 2386},
    {"department": "Materials Science & Engineering", "dept_id": 2391},
]

# ============================================================
# PROFESSIONAL SCHOOLS
# ============================================================

PROFESSIONAL_SCHOOLS = [
    {"department": "Wisconsin School of Business (PhD)", "urls": [
        "https://business.wisc.edu/phd/",
        "https://business.wisc.edu/phd/students/",
        "https://business.wisc.edu/phd/current-students/",
        "https://business.wisc.edu/phd/student-directory/",
        "https://business.wisc.edu/directory/",
        "https://business.wisc.edu/faculty-staff/",
    ]},
    {"department": "UW Law School", "urls": [
        "https://law.wisc.edu/current/orgs.html",
        "https://law.wisc.edu/student-organizations/",
        "https://law.wisc.edu/student-life/student-organizations/",
        "https://law.wisc.edu/about/directory/",
        "https://law.wisc.edu/about/people/",
        "https://law.wisc.edu/people/",
        "https://law.wisc.edu/",
    ]},
    {"department": "School of Education", "urls": [
        "https://education.wisc.edu/people/students",
        "https://education.wisc.edu/people/students/",
        "https://education.wisc.edu/people/graduate-students/",
        "https://education.wisc.edu/people/",
    ]},
    {"department": "School of Public Health", "urls": [
        "https://sph.wisc.edu/people/",
        "https://sph.wisc.edu/people/students/",
        "https://sph.wisc.edu/people/graduate-students/",
        "https://pophealth.wisc.edu/people/",
        "https://pophealth.wisc.edu/people/graduate-students/",
    ]},
    {"department": "Information School (iSchool)", "urls": [
        "https://ischool.wisc.edu/people/students",
        "https://ischool.wisc.edu/people/students/",
        "https://ischool.wisc.edu/people/graduate-students/",
        "https://ischool.wisc.edu/people/",
    ]},
    {"department": "School of Pharmacy", "urls": [
        "https://pharmacy.wisc.edu/people/students",
        "https://pharmacy.wisc.edu/people/students/",
        "https://pharmacy.wisc.edu/people/graduate-students/",
        "https://pharmacy.wisc.edu/people/",
    ]},
]

# ============================================================
# STUDENT ORGANIZATIONS
# ============================================================

STUDENT_ORG_URLS = [
    {"department": "Associated Students of Madison (ASM)", "urls": [
        "https://asm.wisc.edu/",
        "https://asm.wisc.edu/about/",
        "https://asm.wisc.edu/officers/",
        "https://asm.wisc.edu/leadership/",
        "https://asm.wisc.edu/executive-cabinet/",
        "https://asm.wisc.edu/student-council/",
        "https://asm.wisc.edu/contact/",
    ]},
    {"department": "The Daily Cardinal (Student Newspaper)", "urls": [
        "https://www.dailycardinal.com/staff",
        "https://www.dailycardinal.com/page/staff",
        "https://www.dailycardinal.com/about",
        "https://www.dailycardinal.com/page/about",
        "https://www.dailycardinal.com/contact",
    ]},
    {"department": "The Badger Herald (Student Newspaper)", "urls": [
        "https://badgerherald.com/staff/",
        "https://badgerherald.com/about/",
        "https://badgerherald.com/contact/",
        "https://www.badgerherald.com/staff/",
        "https://www.badgerherald.com/about/",
    ]},
]


# ============================================================
# EMAIL EXTRACTION
# ============================================================

def extract_wisc_emails(text):
    """Extract all @wisc.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*wisc\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup):
    """Extract wisc.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*wisc\.edu)',
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
        'police@', 'records@', 'bursar@', 'payroll@', 'uwgrad@',
        'grad-info@', 'deptinfo@', 'uwhelp@', 'askbucky@',
        'undergraduate@', 'commencement@', 'chancellor@', 'provost@',
        'noreply@', 'donotreply@', 'listserv@', 'list@',
        'it-staff@', 'historydept@', 'webadmin@', 'lab@',
        'contact-us@', 'webmanager@',
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
    except requests.exceptions.SSLError:
        try:
            log(f"    SSL error, retrying without verification...")
            resp = session.get(url, headers=HEADERS, timeout=20,
                               allow_redirects=True, verify=False)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, 'html.parser'), resp.url
        except Exception:
            pass
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
    """Extract people from card/grid-based layouts."""
    results = []
    seen_emails = set()

    person_selectors = [
        '.views-row', '.view-content .views-row',
        '[class*="person"]', '[class*="profile"]',
        '[class*="people"]', '[class*="member"]',
        '[class*="student"]', '[class*="card"]',
        '[class*="directory"]', '.field-content',
        'article', '.node--type-person', '.person-row',
        'tr', 'li.leaf', '.vcard',
        '.wp-block-column', '.entry-content .wp-block-group',
        '[class*="grid-item"]', '[class*="team-member"]',
        '[class*="staff-item"]', '[class*="faculty-item"]',
        '.uw-person', '.uw-card', '.uw-directory-item',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_wisc_emails(text)
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
# PROFILE LINK SCRAPING (CRITICAL for UW-Madison L&S depts)
# ============================================================

def find_staff_profile_links(soup, base_url):
    """Find links to individual /staff/<name-slug>/ profile pages.
    This is the primary pattern for UW-Madison L&S departments.
    """
    profiles = []
    seen_urls = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)

        if full_url in seen_urls:
            continue

        # Match UW-Madison profile URLs: /staff/<name>/ or /people/<name>/
        if re.search(r'/(staff|people|directory)/[\w-]+/?$', full_url, re.IGNORECASE):
            # Skip navigation/category links
            if re.search(r'/(staff|people|directory)/(graduate-students|students|faculty|phd-students|all|emeriti|administration-staff|affiliates|board|visitors|undergrad|awards|alumni|faculty-staff-recruitment|faculty-2|staff|phd-student-directory|ms-and-phd-graduate-student-directory)/?$', full_url, re.IGNORECASE):
                continue

            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'staff', 'administration',
                    'emeriti', 'affiliates', 'board', 'visitors',
                    'undergrad', 'major', 'certificate', 'program',
                    'contact', 'courses', 'advising', 'expand', 'collapse',
                    'instructional', 'post-doc', 'research scientist',
                    'directory', 'awards', 'alumni', 'recruitment',
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session):
    """Scrape an individual profile page for wisc.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    # First try mailto links (most reliable)
    emails = extract_mailto_emails(soup)
    personal = [e for e in emails if not is_admin_email(e)]
    if personal:
        return personal[0]

    # Then try page text
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_wisc_emails(text)
    personal = [e for e in text_emails if not is_admin_email(e)]
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
# MAIN DEPARTMENT SCRAPER (L&S + Professional Schools)
# ============================================================

def scrape_department(config, session):
    """Scrape a single department using multi-strategy approach.
    Primary strategy: find /staff/ profile links → scrape each for email.
    Fallback: direct email extraction from page text.
    """
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
        successful_url = final_url or url

        log(f"    -> Page loaded (final URL: {final_url})")

        # ---- Strategy 1: Find /staff/ profile links and scrape each ----
        profiles = find_staff_profile_links(soup, successful_url)
        if profiles:
            log(f"    -> Found {len(profiles)} profile links to scrape")
            scraped_count = 0
            for profile in profiles:
                pname = profile['name']
                purl = profile['profile_url']
                if purl in all_pages_scraped:
                    continue
                all_pages_scraped.add(purl)
                email = scrape_profile_page(purl, session)
                if email and email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': pname,
                        'department': department,
                        'source_url': purl,
                    })
                    scraped_count += 1
                time.sleep(0.2)
            log(f"    -> Got {scraped_count} emails from profile pages")

        # ---- Strategy 2: Extract from structured cards ----
        card_results = extract_from_person_cards(soup, successful_url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # ---- Strategy 3: Extract all emails from full page text ----
        text_emails = extract_wisc_emails(page_text)
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

        # ---- Strategy 4: Check for obfuscated emails ----
        obfuscated = re.findall(
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*wisc\.edu)',
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

        # ---- Strategy 5: Check JavaScript/script tags for emails ----
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_wisc_emails(script.string)
                for email in script_emails:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': department,
                            'source_url': successful_url,
                        })

        # ---- Strategy 6: Follow pagination ----
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

                # Scrape profile links on paginated page
                pg_profiles = find_staff_profile_links(page_soup, page_url)
                for profile in pg_profiles:
                    purl = profile['profile_url']
                    if purl in all_pages_scraped:
                        continue
                    all_pages_scraped.add(purl)
                    email = scrape_profile_page(purl, session)
                    if email and email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': profile['name'],
                            'department': department,
                            'source_url': purl,
                        })
                    time.sleep(0.2)

                pg_text = page_soup.get_text(separator=' ', strip=True)
                pg_text_emails = extract_wisc_emails(pg_text)
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

        if len(results) > 0:
            log(f"    -> Found {len(results)} emails so far")

        # If we got a substantial number of personal emails, skip remaining URLs
        if len(results) >= 10:
            break

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    for r in results[:10]:
        log(f"    {r['email']:<40} | {r['name']}")
    if len(results) > 10:
        log(f"    ... and {len(results) - 10} more")

    return results


# ============================================================
# ENGINEERING SCRAPER (WP REST API)
# ============================================================

def scrape_engineering_api(config, session):
    """Scrape engineering department via WP REST API.
    engineering.wisc.edu exposes person data at /wp-json/wp/v2/person
    with ACF fields containing email, name, etc.
    """
    department = config['department']
    dept_id = config.get('dept_id')

    if dept_id is None:
        # CS is not on engineering.wisc.edu, use regular scraping
        return scrape_department(config, session)

    log(f"\n{'=' * 60}")
    log(f"Department: {department} (API dept_id={dept_id})")
    log(f"{'=' * 60}")

    results = []
    seen_emails = set()
    page = 1
    per_page = 100

    while True:
        api_url = f"https://engineering.wisc.edu/wp-json/wp/v2/person?department={dept_id}&per_page={per_page}&page={page}"
        log(f"  API page {page}: {api_url}")

        try:
            resp = session.get(api_url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                log(f"    -> HTTP {resp.status_code}")
                break

            data = resp.json()
            if not data:
                break

            total = resp.headers.get('X-WP-Total', '?')
            total_pages = int(resp.headers.get('X-WP-TotalPages', 1))

            log(f"    -> Got {len(data)} people (total: {total})")

            for person in data:
                acf = person.get('acf', {})
                email = acf.get('email', '')
                if not email or not email.endswith('wisc.edu'):
                    continue
                email = email.lower().strip()

                if email in seen_emails or is_admin_email(email):
                    continue
                seen_emails.add(email)

                first = acf.get('preferred_first_name') or acf.get('first_name', '')
                last = acf.get('last_name', '')
                name = f"{first} {last}".strip()

                profile_url = person.get('link', '')

                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': profile_url or api_url,
                })

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.3)

        except Exception as e:
            log(f"    -> Error: {e}")
            break

    log(f"  TOTAL for {department}: {len(results)} emails")
    for r in results[:10]:
        log(f"    {r['email']:<40} | {r['name']}")
    if len(results) > 10:
        log(f"    ... and {len(results) - 10} more")

    return results


# ============================================================
# ATHLETICS SCRAPER (uwbadgers.com)
# ============================================================

def scrape_athletics(session):
    """Scrape uwbadgers.com staff directory for wisc.edu emails."""
    results = []
    seen_emails = set()
    department = "Badgers Athletics (Staff)"

    urls_to_try = [
        "https://uwbadgers.com/staff-directory",
        "https://uwbadgers.com/staff-directory/",
        "https://www.uwbadgers.com/staff-directory",
        "https://www.uwbadgers.com/staff-directory/",
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
        text_emails = extract_wisc_emails(page_text)
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

        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_wisc_emails(script.string)
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

    # Also try individual sport coaching pages
    sport_staff_pages = [
        "https://uwbadgers.com/sports/mens-basketball/coaches",
        "https://uwbadgers.com/sports/football/coaches",
        "https://uwbadgers.com/sports/mens-hockey/coaches",
        "https://uwbadgers.com/sports/womens-basketball/coaches",
        "https://uwbadgers.com/sports/baseball/coaches",
        "https://uwbadgers.com/sports/womens-soccer/coaches",
        "https://uwbadgers.com/sports/womens-volleyball/coaches",
        "https://uwbadgers.com/sports/mens-soccer/coaches",
        "https://uwbadgers.com/sports/wrestling/coaches",
        "https://uwbadgers.com/sports/softball/coaches",
        "https://uwbadgers.com/sports/rowing/coaches",
        "https://uwbadgers.com/sports/track-and-field/coaches",
        "https://uwbadgers.com/sports/swimming-and-diving/coaches",
        "https://uwbadgers.com/sports/cross-country/coaches",
        "https://uwbadgers.com/sports/tennis/coaches",
        "https://uwbadgers.com/sports/golf/coaches",
    ]

    for url in sport_staff_pages:
        log(f"  Trying sport page: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_wisc_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                sport_name = url.split('/sports/')[1].split('/')[0].replace('-', ' ').title() if '/sports/' in url else ''
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
    log("UNIVERSITY OF WISCONSIN-MADISON EMAIL SCRAPER")
    log("=" * 70)

    # ---- Phase 1: Arts & Sciences (L&S) ----
    log("\n" + "=" * 70)
    log("PHASE 1: ARTS & SCIENCES (L&S) DEPARTMENTS")
    log("=" * 70)

    for config in ARTS_SCIENCES_DEPARTMENTS:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: Engineering ----
    log("\n\n" + "=" * 70)
    log("PHASE 2: ENGINEERING DEPARTMENTS")
    log("=" * 70)

    for config in ENGINEERING_API_DEPARTMENTS:
        try:
            dept_results = scrape_engineering_api(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 3: Professional Schools ----
    log("\n\n" + "=" * 70)
    log("PHASE 3: PROFESSIONAL SCHOOLS")
    log("=" * 70)

    for config in PROFESSIONAL_SCHOOLS:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 4: Athletics ----
    log("\n\n" + "=" * 70)
    log("PHASE 4: ATHLETICS")
    log("=" * 70)

    try:
        athletics_results = scrape_athletics(session)
        n = add_results(athletics_results)
        log(f"  => {n} new unique emails added")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 5: Student Organizations ----
    log("\n\n" + "=" * 70)
    log("PHASE 5: STUDENT ORGANIZATIONS")
    log("=" * 70)

    for config in STUDENT_ORG_URLS:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @wisc.edu emails: {len(all_results)}")

    # Save CSV
    output_csv = '/Users/jaiashar/Documents/VoraBusinessFinder/wisconsin_dept_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")

    # Save JSON
    output_json = '/Users/jaiashar/Documents/VoraBusinessFinder/wisconsin_dept_emails.json'
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

    all_dept_names = (
        [c['department'] for c in ARTS_SCIENCES_DEPARTMENTS] +
        [c['department'] for c in ENGINEERING_API_DEPARTMENTS] +
        [c['department'] for c in PROFESSIONAL_SCHOOLS] +
        ["Badgers Athletics (Staff)"] +
        [c['department'] for c in STUDENT_ORG_URLS]
    )
    depts_with_zero = [d for d in all_dept_names if d not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in depts_with_zero:
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
