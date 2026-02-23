#!/usr/bin/env python3
"""
UNC Chapel Hill Graduate Student & Staff Email Scraper
Scrapes @unc.edu and @email.unc.edu emails from department people/graduate-student pages,
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
# ARTS & SCIENCES DEPARTMENTS
# ============================================================

ARTS_SCIENCES_DEPARTMENTS = [
    {
        "department": "Economics",
        "urls": [
            "https://econ.unc.edu/people/graduate-students/",
            "https://econ.unc.edu/people/students/",
            "https://econ.unc.edu/people/phd-students/",
            "https://econ.unc.edu/people/",
        ],
    },
    {
        "department": "Political Science",
        "urls": [
            "https://politicalscience.unc.edu/people/graduate-students/",
            "https://politicalscience.unc.edu/people/students/",
            "https://politicalscience.unc.edu/people/phd-students/",
            "https://politicalscience.unc.edu/people/",
        ],
    },
    {
        "department": "Sociology",
        "urls": [
            "https://sociology.unc.edu/people/graduate-students/",
            "https://sociology.unc.edu/people/students/",
            "https://sociology.unc.edu/people/",
        ],
    },
    {
        "department": "Psychology - Quantitative",
        "urls": [
            "https://quantpsych.unc.edu/graduate-students/",
        ],
    },
    {
        "department": "Psychology - Social",
        "urls": [
            "https://socialpsych.unc.edu/graduate-students/",
        ],
    },
    {
        "department": "Psychology - Developmental",
        "urls": [
            "https://devpsych.unc.edu/graduate-students/",
        ],
    },
    {
        "department": "Psychology - Cognitive",
        "urls": [
            "https://cogpsych.unc.edu/graduate-students/",
        ],
    },
    {
        "department": "Psychology - Clinical",
        "urls": [
            "https://clinicalpsych.unc.edu/graduate-students/",
        ],
    },
    {
        "department": "Psychology - Behavioral & Integrative Neuroscience",
        "urls": [
            "https://bnpsych.unc.edu/graduate-students/",
        ],
    },
    {
        "department": "History",
        "urls": [
            "https://history.unc.edu/people/graduate-students/",
            "https://history.unc.edu/people/students/",
            "https://history.unc.edu/people/",
        ],
    },
    {
        "department": "English",
        "urls": [
            "https://english.unc.edu/people/graduate-students/",
            "https://english.unc.edu/people/students/",
            "https://english.unc.edu/graduate/current-students/",
            "https://english.unc.edu/people/",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.unc.edu/people/graduate-students/",
            "https://philosophy.unc.edu/people/students/",
            "https://philosophy.unc.edu/people/",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://linguistics.unc.edu/people/graduate-students/",
            "https://linguistics.unc.edu/people/students/",
            "https://linguistics.unc.edu/people/",
        ],
    },
    {
        "department": "Mathematics",
        "urls": [
            "https://math.unc.edu/people/graduate-students/",
            "https://math.unc.edu/people/students/",
            "https://math.unc.edu/people/",
        ],
    },
    {
        "department": "Statistics",
        "urls": [
            "https://stat.unc.edu/people/graduate-students/",
            "https://stat.unc.edu/people/students/",
            "https://stat.unc.edu/people/",
            "https://stor.unc.edu/people/graduate-students/",
            "https://stor.unc.edu/people/students/",
        ],
    },
    {
        "department": "Physics",
        "urls": [
            "https://physics.unc.edu/people/graduate-students/",
            "https://physics.unc.edu/people/students/",
            "https://physics.unc.edu/people/",
        ],
    },
    {
        "department": "Chemistry",
        "urls": [
            "https://chem.unc.edu/people/graduate-students/",
            "https://chem.unc.edu/people/students/",
            "https://chem.unc.edu/people/",
        ],
    },
    {
        "department": "Geological Sciences",
        "urls": [
            "https://geos.unc.edu/people/graduate-students/",
            "https://geos.unc.edu/people/students/",
            "https://geos.unc.edu/graduate/current-students/",
            "https://geos.unc.edu/people/",
        ],
    },
    {
        "department": "Biology",
        "urls": [
            "https://bio.unc.edu/people/graduate-students/",
            "https://bio.unc.edu/people/students/",
            "https://bio.unc.edu/people/",
        ],
    },
    {
        "department": "Anthropology",
        "urls": [
            "https://anthro.unc.edu/people/graduate-students/",
            "https://anthro.unc.edu/people/students/",
            "https://anthro.unc.edu/graduate/current-students/",
            "https://anthro.unc.edu/people/",
        ],
    },
    {
        "department": "Classics",
        "urls": [
            "https://classics.unc.edu/people/graduate-students/",
            "https://classics.unc.edu/people/students/",
            "https://classics.unc.edu/people/",
        ],
    },
    {
        "department": "Communication",
        "urls": [
            "https://comm.unc.edu/people/graduate-students/",
            "https://comm.unc.edu/people/students/",
            "https://comm.unc.edu/people/",
        ],
    },
    {
        "department": "Geography",
        "urls": [
            "https://geography.unc.edu/people/graduate-students/",
            "https://geography.unc.edu/people/students/",
            "https://geography.unc.edu/people/",
        ],
    },
    {
        "department": "Music",
        "urls": [
            "https://music.unc.edu/people/graduate-students/",
            "https://music.unc.edu/people/students/",
            "https://music.unc.edu/people/",
        ],
    },
    {
        "department": "Art & Art History",
        "urls": [
            "https://art.unc.edu/people/graduate-students/",
            "https://art.unc.edu/people/students/",
            "https://art.unc.edu/people/",
        ],
    },
]

# ============================================================
# ENGINEERING & CS DEPARTMENTS
# ============================================================

ENGINEERING_DEPARTMENTS = [
    {
        "department": "Computer Science",
        "urls": [
            "https://cs.unc.edu/people/graduate-students/",
            "https://cs.unc.edu/people/students/",
            "https://cs.unc.edu/people/phd-students/",
            "https://cs.unc.edu/people/",
        ],
    },
    {
        "department": "Biomedical Engineering",
        "urls": [
            "https://bme.unc.edu/people/graduate-students/",
            "https://bme.unc.edu/people/students/",
            "https://bme.unc.edu/people/",
        ],
    },
    {
        "department": "Electrical & Computer Engineering",
        "urls": [
            "https://ece.unc.edu/people/graduate-students/",
            "https://ece.unc.edu/people/students/",
            "https://ece.unc.edu/people/phd-students/",
            "https://ece.unc.edu/people/",
        ],
    },
]

# ============================================================
# PROFESSIONAL SCHOOLS
# ============================================================

PROFESSIONAL_SCHOOLS = [
    {
        "department": "Kenan-Flagler Business School (PhD)",
        "urls": [
            "https://www.kenan-flagler.unc.edu/phd/",
            "https://www.kenan-flagler.unc.edu/faculty-and-research/phd/",
            "https://www.kenan-flagler.unc.edu/phd/students/",
            "https://www.kenan-flagler.unc.edu/phd/current-students/",
            "https://www.kenan-flagler.unc.edu/research/phd-students/",
            "https://www.kenan-flagler.unc.edu/faculty/",
        ],
    },
    {
        "department": "UNC School of Law",
        "urls": [
            "https://law.unc.edu/people/students/",
            "https://law.unc.edu/people/",
            "https://law.unc.edu/academics/student-organizations/",
            "https://law.unc.edu/student-life/student-organizations/",
        ],
    },
    {
        "department": "Gillings School of Public Health",
        "urls": [
            "https://sph.unc.edu/people/",
            "https://sph.unc.edu/students/",
            "https://sph.unc.edu/people/students/",
            "https://sph.unc.edu/bios/students/",
        ],
    },
    {
        "department": "School of Government",
        "urls": [
            "https://sog.unc.edu/people/",
            "https://sog.unc.edu/about/people/",
            "https://sog.unc.edu/about/staff/",
            "https://www.sog.unc.edu/resources/microsites/mpa/people",
            "https://www.sog.unc.edu/about/faculty-and-staff",
        ],
    },
    {
        "department": "School of Social Work",
        "urls": [
            "https://ssw.unc.edu/people/students",
            "https://ssw.unc.edu/people/students/",
            "https://ssw.unc.edu/people/",
        ],
    },
    {
        "department": "Hussman School of Journalism",
        "urls": [
            "https://hussman.unc.edu/people/students",
            "https://hussman.unc.edu/people/students/",
            "https://hussman.unc.edu/people/",
            "https://hussman.unc.edu/people/graduate-students/",
        ],
    },
    {
        "department": "School of Education",
        "urls": [
            "https://soe.unc.edu/people/students",
            "https://soe.unc.edu/people/students/",
            "https://soe.unc.edu/people/",
            "https://soe.unc.edu/people/graduate-students/",
        ],
    },
    {
        "department": "School of Nursing",
        "urls": [
            "https://nursing.unc.edu/people/students",
            "https://nursing.unc.edu/people/students/",
            "https://nursing.unc.edu/people/",
        ],
    },
    {
        "department": "Eshelman School of Pharmacy",
        "urls": [
            "https://pharmacy.unc.edu/people/students",
            "https://pharmacy.unc.edu/people/students/",
            "https://pharmacy.unc.edu/people/",
            "https://pharmacy.unc.edu/directory/",
            "https://pharmacy.unc.edu/faculty-and-research/faculty/",
        ],
    },
]

# ============================================================
# ATHLETICS & STUDENT ORGS
# ============================================================

ATHLETICS_URLS = [
    {
        "department": "Tar Heels Athletics (Staff)",
        "urls": [
            "https://goheels.com/staff-directory",
            "https://goheels.com/staff-directory/",
            "https://www.goheels.com/staff-directory",
        ],
    },
]

STUDENT_ORG_URLS = [
    {
        "department": "Student Government",
        "urls": [
            "https://studentgovernment.unc.edu/",
            "https://studentgovernment.unc.edu/about/",
            "https://studentgovernment.unc.edu/executive-branch/",
            "https://studentgovernment.unc.edu/about/officers/",
            "https://studentgovernment.unc.edu/about/executive-branch/",
        ],
    },
    {
        "department": "Daily Tar Heel (Student Newspaper)",
        "urls": [
            "https://www.dailytarheel.com/page/staff",
            "https://www.dailytarheel.com/staff",
            "https://www.dailytarheel.com/page/about",
            "https://www.dailytarheel.com/about",
        ],
    },
]


# ============================================================
# EMAIL EXTRACTION
# ============================================================

def extract_unc_emails(text):
    """Extract all UNC email addresses from text.
    Captures: @unc.edu, @email.unc.edu, @live.unc.edu, @ad.unc.edu, @med.unc.edu, @cidd.unc.edu, etc.
    """
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*unc\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup):
    """Extract UNC emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*unc\.edu)',
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
        'admissions@', 'accessibility@', 'diversity@', 'commencement@',
        'chancellor@', 'provost@', 'unc_', 'uncnews@', 'uncpress@',
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
    """Extract people from card/grid-based layouts (common in UNC WordPress sites)."""
    results = []
    seen_emails = set()

    # Selectors common in UNC department sites (WordPress-based)
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
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_unc_emails(text)
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

        # Match profile-like URLs for UNC
        if re.search(r'/people/[\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'staff', 'administration'
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
    """Scrape an individual profile page for UNC email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_unc_emails(text)
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
            'doctoral student', '@unc.edu', '@email.unc.edu',
            '@live.unc.edu', '@ad.unc.edu', '@med.unc.edu',
            'email', 'advisor', 'faculty', 'staff', 'people'
        ])

        if not has_people and len(extract_unc_emails(page_text)) == 0:
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
        text_emails = extract_unc_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*unc\.edu)',
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

        # Strategy 4: Check JavaScript/script tags for emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_unc_emails(script.string)
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
                pg_text_emails = extract_unc_emails(pg_text)
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
# ATHLETICS SCRAPER (goheels.com)
# ============================================================

def scrape_athletics(session):
    """Scrape goheels.com staff directory for UNC emails."""
    results = []
    seen_emails = set()
    department = "Tar Heels Athletics (Staff)"

    urls_to_try = [
        "https://goheels.com/staff-directory",
        "https://goheels.com/staff-directory/",
        "https://www.goheels.com/staff-directory",
        "https://www.goheels.com/staff-directory/",
        "https://goheels.com/sports/2020/3/18/staff-directory.aspx",
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
        text_emails = extract_unc_emails(page_text)
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

        # Also look for staff cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Try roster pages for athlete-related staff
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_unc_emails(script.string)
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
    sport_staff_pages = [
        "https://goheels.com/sports/mens-basketball/coaches",
        "https://goheels.com/sports/football/coaches",
        "https://goheels.com/sports/mens-soccer/coaches",
        "https://goheels.com/sports/womens-basketball/coaches",
        "https://goheels.com/sports/baseball/coaches",
        "https://goheels.com/sports/womens-soccer/coaches",
        "https://goheels.com/sports/mens-lacrosse/coaches",
        "https://goheels.com/sports/womens-lacrosse/coaches",
        "https://goheels.com/sports/field-hockey/coaches",
        "https://goheels.com/sports/mens-tennis/coaches",
    ]

    for url in sport_staff_pages:
        log(f"  Trying sport page: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_unc_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': "Athletics - " + url.split('/sports/')[1].split('/')[0].replace('-', ' ').title() if '/sports/' in url else department,
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
    log("UNC CHAPEL HILL EMAIL SCRAPER")
    log("=" * 70)

    # ---- Phase 1: Arts & Sciences ----
    log("\n" + "=" * 70)
    log("PHASE 1: ARTS & SCIENCES DEPARTMENTS")
    log("=" * 70)

    for config in ARTS_SCIENCES_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: Engineering & CS ----
    log("\n\n" + "=" * 70)
    log("PHASE 2: ENGINEERING & CS DEPARTMENTS")
    log("=" * 70)

    for config in ENGINEERING_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
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
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 4: Athletics ----
    log("\n\n" + "=" * 70)
    log("PHASE 4: ATHLETICS")
    log("=" * 70)

    try:
        results = scrape_athletics(session)
        n = add_results(results)
        log(f"  => {n} new unique emails added")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 5: Student Organizations ----
    log("\n\n" + "=" * 70)
    log("PHASE 5: STUDENT ORGANIZATIONS")
    log("=" * 70)

    for config in STUDENT_ORG_URLS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @unc.edu / @email.unc.edu emails: {len(all_results)}")

    # Save CSV
    output_csv = '/Users/jaiashar/Documents/VoraBusinessFinder/unc_dept_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")

    # Save JSON
    output_json = '/Users/jaiashar/Documents/VoraBusinessFinder/unc_dept_emails.json'
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
        [c['department'] for c in ENGINEERING_DEPARTMENTS] +
        [c['department'] for c in PROFESSIONAL_SCHOOLS] +
        ["Tar Heels Athletics (Staff)"] +
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
