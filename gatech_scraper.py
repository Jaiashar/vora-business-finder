#!/usr/bin/env python3
"""
Georgia Institute of Technology (Georgia Tech) Email Scraper
Scrapes @gatech.edu emails from:
- College of Computing (CS, IC, CSE)
- College of Engineering (ECE, ME, CEE, ChBE, BME, MSE, AE, ISyE)
- College of Sciences (Physics, Chemistry, Math, Biology, EAS)
- Ivan Allen College of Liberal Arts (Econ, HTS, Psychology, LMC, INTA, PublicPolicy)
- Scheller College of Business
- Athletics (ramblinwreck.com)
- Research labs and student organizations
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/gatech_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/gatech_dept_emails.json'


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_gatech_emails(text):
    """Extract all @gatech.edu emails from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*gatech\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*gatech\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract gatech.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*gatech\.edu)',
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
        'noreply@', 'do-not-reply@', 'donotreply@', 'ask@',
        'ece@', 'me@', 'ce@', 'chbe@', 'bme@', 'mse@', 'ae@', 'isye@',
        'cs@', 'cc@', 'coc@', 'coe@', 'cos@', 'iac@', 'spp@',
        'scheller@', 'gtri@', 'provost@', 'president@',
        'undergradadvising@', 'gradadvising@', 'graduate@',
        'academics@', 'research@', 'compliance@', 'oit@',
        'hr-help@', 'buzz@', 'strap@', 'sga@',
        'ece-', 'me-', 'ae-', 'isye-', 'cs-', 'chbe-',
        'coc-', 'coe-', 'cos-',
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
                for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a', 'span']):
                    tag_text = tag.get_text(strip=True)
                    if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                        if not any(x in tag_text.lower() for x in [
                            'email', 'contact', 'phone', 'http', 'department',
                            'graduate', 'student', 'people', 'faculty', 'office',
                            'read more', 'view profile', 'website', 'lab',
                            'research', 'google scholar', 'orcid', 'personal',
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
                        'website', 'personal', 'google scholar', 'orcid',
                    ]):
                        return name
            parent = parent.parent

    return ""


# ============================================================
# STRUCTURED EXTRACTION: Person cards / grid layouts
# ============================================================

def extract_from_person_cards(soup, url, department):
    """Extract people from card/grid-based layouts."""
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
        'li',
        '.vcard',
        '.grid-item',
        '.teaser',
        '.faculty-staff',
        '.person-listing',
        '.people-listing__person',
        '.hentry',
        '.team-member',
        '.staff-member',
        '.bio',
        '[class*="faculty"]',
        '[class*="staff"]',
        '.row',
        'dl',
        'dd',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_gatech_emails(text)
                mailto_emails = extract_mailto_emails(card)
                all_emails = list(set(emails + mailto_emails))

                for email in all_emails:
                    if email in seen_emails or is_admin_email(email):
                        continue
                    seen_emails.add(email)

                    name = ""
                    for tag in card.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a', 'span']):
                        tag_text = tag.get_text(strip=True)
                        href = tag.get('href', '') if tag.name == 'a' else ''
                        if 'mailto:' in href:
                            continue
                        if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                            if not any(x in tag_text.lower() for x in [
                                'email', 'contact', 'phone', 'http', 'department',
                                'graduate', 'student', 'people', 'faculty', 'office',
                                'read more', 'view profile',
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

        # Match profile-like URLs on gatech.edu
        if (re.search(r'/people/[a-z][\w-]+$', full_url, re.IGNORECASE) or
                re.search(r'/directory/[a-z][\w-]+$', full_url, re.IGNORECASE) or
                re.search(r'/person/[a-z][\w-]+$', full_url, re.IGNORECASE) or
                re.search(r'/faculty/[a-z][\w-]+$', full_url, re.IGNORECASE) or
                re.search(r'/staff/[a-z][\w-]+$', full_url, re.IGNORECASE)):
            if 'gatech.edu' in full_url:
                name = a_tag.get_text(strip=True)
                if name and '@' not in name and len(name) > 2 and len(name) < 80:
                    if not any(x in name.lower() for x in [
                        'graduate', 'student', 'people', 'faculty', 'all',
                        'home', 'search', 'more', 'view', 'page', 'next',
                        'previous', 'department', 'directory', 'staff',
                    ]):
                        seen_urls.add(full_url)
                        profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
    """Scrape an individual profile page for gatech.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_gatech_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# College of Computing
COMPUTING_DEPARTMENTS = [
    {
        "department": "School of Computer Science",
        "urls": [
            "https://www.scs.gatech.edu/people/phd-students",
            "https://www.scs.gatech.edu/people/graduate-students",
            "https://scs.gatech.edu/people/phd-students",
            "https://scs.gatech.edu/people/graduate-students",
            "https://scs.gatech.edu/people",
            "https://www.cc.gatech.edu/people/phd-students",
            "https://www.cc.gatech.edu/people?field_person_type_tid=3964",
        ],
    },
    {
        "department": "School of Interactive Computing",
        "urls": [
            "https://www.ic.gatech.edu/people/phd-students",
            "https://ic.gatech.edu/people/phd-students",
            "https://ic.gatech.edu/people/graduate-students",
            "https://ic.gatech.edu/people",
            "https://www.ic.gatech.edu/people",
        ],
    },
    {
        "department": "School of Computational Science & Engineering",
        "urls": [
            "https://cse.gatech.edu/people/phd-students",
            "https://www.cse.gatech.edu/people/phd-students",
            "https://cse.gatech.edu/people/graduate-students",
            "https://cse.gatech.edu/people",
        ],
    },
    {
        "department": "School of Cybersecurity and Privacy",
        "urls": [
            "https://scp.cc.gatech.edu/people",
            "https://scp.cc.gatech.edu/people/phd-students",
            "https://www.scp.gatech.edu/people",
        ],
    },
    {
        "department": "College of Computing (General)",
        "urls": [
            "https://www.cc.gatech.edu/people",
            "https://cc.gatech.edu/people",
            "https://www.cc.gatech.edu/people/graduate-students",
            "https://www.cc.gatech.edu/people/students",
        ],
    },
]

# College of Engineering
ENGINEERING_DEPARTMENTS = [
    {
        "department": "Electrical & Computer Engineering (ECE)",
        "urls": [
            "https://ece.gatech.edu/directory/all/graduate-student",
            "https://ece.gatech.edu/people/graduate-students",
            "https://ece.gatech.edu/people/phd-students",
            "https://ece.gatech.edu/directory",
            "https://ece.gatech.edu/people",
            "https://www.ece.gatech.edu/people",
        ],
    },
    {
        "department": "Mechanical Engineering (ME)",
        "urls": [
            "https://me.gatech.edu/graduate-students",
            "https://me.gatech.edu/people/graduate-students",
            "https://me.gatech.edu/people/phd-students",
            "https://me.gatech.edu/directory",
            "https://me.gatech.edu/people",
            "https://www.me.gatech.edu/people",
        ],
    },
    {
        "department": "Civil & Environmental Engineering (CEE)",
        "urls": [
            "https://ce.gatech.edu/people/graduate-students",
            "https://ce.gatech.edu/people/phd-students",
            "https://ce.gatech.edu/directory",
            "https://ce.gatech.edu/people",
            "https://www.ce.gatech.edu/people",
        ],
    },
    {
        "department": "Chemical & Biomolecular Engineering (ChBE)",
        "urls": [
            "https://chbe.gatech.edu/people/graduate-students",
            "https://chbe.gatech.edu/people/phd-students",
            "https://chbe.gatech.edu/directory",
            "https://chbe.gatech.edu/people",
            "https://www.chbe.gatech.edu/people",
        ],
    },
    {
        "department": "Biomedical Engineering (BME)",
        "urls": [
            "https://bme.gatech.edu/bme/people/graduate-students",
            "https://bme.gatech.edu/bme/people/phd-students",
            "https://bme.gatech.edu/bme/directory",
            "https://bme.gatech.edu/bme/people",
            "https://bme.gatech.edu/people",
            "https://www.bme.gatech.edu/bme/people",
        ],
    },
    {
        "department": "Materials Science & Engineering (MSE)",
        "urls": [
            "https://mse.gatech.edu/people/graduate-students",
            "https://mse.gatech.edu/people/phd-students",
            "https://mse.gatech.edu/directory",
            "https://mse.gatech.edu/people",
            "https://www.mse.gatech.edu/people",
        ],
    },
    {
        "department": "Aerospace Engineering (AE)",
        "urls": [
            "https://ae.gatech.edu/people/graduate-students",
            "https://ae.gatech.edu/people/phd-students",
            "https://ae.gatech.edu/directory",
            "https://ae.gatech.edu/people",
            "https://www.ae.gatech.edu/people",
        ],
    },
    {
        "department": "Industrial & Systems Engineering (ISyE)",
        "urls": [
            "https://www.isye.gatech.edu/people/phd-students",
            "https://www.isye.gatech.edu/people/graduate-students",
            "https://isye.gatech.edu/people/phd-students",
            "https://isye.gatech.edu/people/graduate-students",
            "https://isye.gatech.edu/directory",
            "https://isye.gatech.edu/people",
        ],
    },
    {
        "department": "Nuclear & Radiological Engineering (NRE)",
        "urls": [
            "https://nre.gatech.edu/people/graduate-students",
            "https://nre.gatech.edu/people",
        ],
    },
]

# College of Sciences
SCIENCES_DEPARTMENTS = [
    {
        "department": "School of Physics",
        "urls": [
            "https://physics.gatech.edu/people/graduate-students",
            "https://physics.gatech.edu/people/phd-students",
            "https://physics.gatech.edu/directory",
            "https://physics.gatech.edu/people",
            "https://www.physics.gatech.edu/people",
        ],
    },
    {
        "department": "School of Chemistry & Biochemistry",
        "urls": [
            "https://chemistry.gatech.edu/people/graduate-students",
            "https://chemistry.gatech.edu/people/phd-students",
            "https://chemistry.gatech.edu/directory",
            "https://chemistry.gatech.edu/people",
            "https://www.chemistry.gatech.edu/people",
        ],
    },
    {
        "department": "School of Mathematics",
        "urls": [
            "https://math.gatech.edu/people/graduate-students",
            "https://math.gatech.edu/people/phd-students",
            "https://math.gatech.edu/people",
            "https://www.math.gatech.edu/people",
        ],
    },
    {
        "department": "School of Biological Sciences",
        "urls": [
            "https://biosciences.gatech.edu/people/graduate-students",
            "https://biosciences.gatech.edu/people/phd-students",
            "https://biosciences.gatech.edu/people",
            "https://biology.gatech.edu/people/graduate-students",
            "https://biology.gatech.edu/people",
        ],
    },
    {
        "department": "School of Earth & Atmospheric Sciences (EAS)",
        "urls": [
            "https://eas.gatech.edu/people/graduate-students",
            "https://eas.gatech.edu/people/phd-students",
            "https://eas.gatech.edu/people",
            "https://www.eas.gatech.edu/people",
        ],
    },
    {
        "department": "School of Psychology",
        "urls": [
            "https://psychology.gatech.edu/people/graduate-students",
            "https://psychology.gatech.edu/people/phd-students",
            "https://psychology.gatech.edu/people",
            "https://www.psychology.gatech.edu/people",
        ],
    },
]

# Ivan Allen College of Liberal Arts
LIBERAL_ARTS_DEPARTMENTS = [
    {
        "department": "School of Economics",
        "urls": [
            "https://econ.gatech.edu/people/graduate-students",
            "https://econ.gatech.edu/people/phd-students",
            "https://econ.gatech.edu/people",
            "https://www.econ.gatech.edu/people",
        ],
    },
    {
        "department": "School of History & Sociology",
        "urls": [
            "https://hts.gatech.edu/people/graduate-students",
            "https://hts.gatech.edu/people/phd-students",
            "https://hts.gatech.edu/people",
            "https://www.hts.gatech.edu/people",
        ],
    },
    {
        "department": "School of Literature, Media & Communication (LMC)",
        "urls": [
            "https://lmc.gatech.edu/people/graduate-students",
            "https://lmc.gatech.edu/people/phd-students",
            "https://lmc.gatech.edu/people",
            "https://www.lmc.gatech.edu/people",
        ],
    },
    {
        "department": "Sam Nunn School of International Affairs (INTA)",
        "urls": [
            "https://inta.gatech.edu/people/graduate-students",
            "https://inta.gatech.edu/people/phd-students",
            "https://inta.gatech.edu/people",
            "https://www.inta.gatech.edu/people",
        ],
    },
    {
        "department": "School of Public Policy",
        "urls": [
            "https://spp.gatech.edu/people/graduate-students",
            "https://spp.gatech.edu/people/phd-students",
            "https://spp.gatech.edu/people",
            "https://www.spp.gatech.edu/people",
        ],
    },
]

# Professional Schools
PROFESSIONAL_SCHOOLS = [
    {
        "department": "Scheller College of Business",
        "urls": [
            "https://scheller.gatech.edu/people/phd-students",
            "https://scheller.gatech.edu/people/graduate-students",
            "https://scheller.gatech.edu/people",
            "https://www.scheller.gatech.edu/people",
            "https://scheller.gatech.edu/directory",
            "https://scheller.gatech.edu/doctoral/current-doctoral-students.html",
        ],
    },
    {
        "department": "College of Design",
        "urls": [
            "https://design.gatech.edu/people/graduate-students",
            "https://design.gatech.edu/people/phd-students",
            "https://design.gatech.edu/people",
            "https://arch.gatech.edu/people",
        ],
    },
    {
        "department": "School of Music",
        "urls": [
            "https://music.gatech.edu/people",
            "https://music.gatech.edu/people/graduate-students",
        ],
    },
]

# Athletics & Student Life
ATHLETICS_AND_ORGS = [
    {
        "department": "GT Athletics",
        "urls": [
            "https://ramblinwreck.com/staff-directory/",
            "https://ramblinwreck.com/athletic-directory/",
        ],
    },
    {
        "department": "Student Government Association",
        "urls": [
            "https://sga.gatech.edu/about",
            "https://sga.gatech.edu/",
            "https://sga.gatech.edu/leadership",
        ],
    },
    {
        "department": "Graduate Student Government",
        "urls": [
            "https://grad.gatech.edu/student-organizations",
            "https://grad.gatech.edu/",
        ],
    },
]

# Research Centers & Interdisciplinary
RESEARCH_CENTERS = [
    {
        "department": "GT Research Institute (GTRI)",
        "urls": [
            "https://gtri.gatech.edu/people",
            "https://www.gtri.gatech.edu/people",
        ],
    },
    {
        "department": "Institute for Robotics & Intelligent Machines (IRIM)",
        "urls": [
            "https://research.gatech.edu/robotics/people",
            "https://irim.gatech.edu/people",
            "https://www.irim.gatech.edu/people",
        ],
    },
    {
        "department": "Machine Learning Center",
        "urls": [
            "https://ml.gatech.edu/people",
            "https://www.ml.gatech.edu/people",
            "https://ml.gatech.edu/people/phd-students",
        ],
    },
    {
        "department": "Institute for Data Engineering & Science (IDEaS)",
        "urls": [
            "https://ideas.gatech.edu/people",
            "https://www.ideas.gatech.edu/people",
        ],
    },
    {
        "department": "Parker H. Petit Institute for Bioengineering",
        "urls": [
            "https://petitinstitute.gatech.edu/people",
            "https://www.petitinstitute.gatech.edu/people",
        ],
    },
]


# ============================================================
# MAIN DEPARTMENT SCRAPER
# ============================================================

def scrape_department(config, session, global_seen_emails):
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
        effective_url = final_url or url
        log(f"    -> Page loaded ({len(page_text)} chars)")

        # Strategy 1: Extract from structured cards
        card_results = extract_from_person_cards(soup, effective_url, department)
        for r in card_results:
            if r['email'] not in seen_emails and r['email'] not in global_seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_gatech_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        for email in all_emails:
            if email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': effective_url,
                })

        # Strategy 3: Obfuscated emails
        obfuscated = re.findall(
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*gatech\.edu)',
            page_text, re.IGNORECASE
        )
        for prefix, domain in obfuscated:
            email = f"{prefix}@{domain}".lower()
            if email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': '',
                    'department': department,
                    'source_url': effective_url,
                })

        # Strategy 4: JavaScript-embedded emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_gatech_emails(script.string)
                for email in script_emails:
                    if email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': department,
                            'source_url': effective_url,
                        })

        # Strategy 5: Follow pagination
        pagination_urls = find_pagination_urls(soup, effective_url)
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
                pg_text_emails = extract_gatech_emails(pg_text)
                pg_mailto = extract_mailto_emails(page_soup)
                pg_all = list(set(pg_text_emails + pg_mailto))

                pg_cards = extract_from_person_cards(page_soup, page_url, department)
                for r in pg_cards:
                    if r['email'] not in seen_emails and r['email'] not in global_seen_emails:
                        seen_emails.add(r['email'])
                        results.append(r)

                for email in pg_all:
                    if email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
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
            profiles = find_profile_links(soup, effective_url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting for emails...")
                for profile in profiles[:60]:
                    pname = profile['name']
                    purl = profile['profile_url']
                    email = scrape_profile_page(purl, session, department)
                    if email and email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
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
    for r in results[:10]:
        log(f"    {r['email']:<45} | {r['name']}")
    if len(results) > 10:
        log(f"    ... and {len(results) - 10} more")

    return results


# ============================================================
# ATHLETICS SCRAPER (ramblinwreck.com)
# ============================================================

def scrape_athletics(session, global_seen_emails):
    """Scrape GT athletics staff directory for emails."""
    results = []
    seen_emails = set()
    department = "GT Athletics"

    log(f"\n{'=' * 60}")
    log(f"Scraping: GT Athletics (ramblinwreck.com)")
    log(f"{'=' * 60}")

    urls = [
        "https://ramblinwreck.com/staff-directory/",
        "https://ramblinwreck.com/athletic-directory/",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_gatech_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} gatech.edu emails on page")

        for email in all_emails:
            if email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': final_url or url,
                })

        # Follow staff profile links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if ('staff' in href.lower() or 'coaches' in href.lower()) and full_url != url:
                if 'ramblinwreck.com' in full_url or 'gatech.edu' in full_url:
                    if full_url not in urls:
                        log(f"    Following staff link: {full_url}")
                        sub_soup, sub_url = get_soup(full_url, session)
                        if sub_soup:
                            sub_text = sub_soup.get_text(separator=' ', strip=True)
                            sub_emails = extract_gatech_emails(sub_text)
                            sub_mailto = extract_mailto_emails(sub_soup)
                            for email in list(set(sub_emails + sub_mailto)):
                                if email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
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
# GATECH DIRECTORY API - Some GT departments use JSON APIs
# ============================================================

def try_gatech_directory_api(session, global_seen_emails):
    """Try to access the GT directory/people API endpoints that return JSON."""
    results = []
    seen_emails = set()

    log(f"\n{'=' * 60}")
    log(f"Trying GT directory API endpoints...")
    log(f"{'=' * 60}")

    # Some GT sites use Drupal views with JSON export
    api_urls = [
        ("School of Computer Science (API)", "https://scs.gatech.edu/views/ajax"),
        ("College of Computing (API)", "https://www.cc.gatech.edu/views/ajax"),
    ]

    for dept_name, url in api_urls:
        log(f"  Trying API: {url} for {dept_name}")
        try:
            # Try standard JSON endpoint
            resp = session.get(
                url.replace('/views/ajax', '/people/graduate-students?_format=json'),
                headers=HEADERS, timeout=15
            )
            if resp.status_code == 200 and resp.text.strip().startswith(('[', '{')):
                data = resp.json()
                log(f"    -> Got JSON response ({len(str(data))} chars)")
                text = json.dumps(data)
                emails = extract_gatech_emails(text)
                for email in emails:
                    if email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': dept_name,
                            'source_url': url,
                        })
        except Exception as e:
            log(f"    -> API failed: {e}")

        time.sleep(0.5)

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
    log("GEORGIA INSTITUTE OF TECHNOLOGY (GEORGIA TECH) EMAIL SCRAPER")
    log("Domain: @gatech.edu")
    log("=" * 70)

    # ---- Phase 1: College of Computing ----
    log("\n\nPHASE 1: COLLEGE OF COMPUTING")
    log("=" * 70)
    for config in COMPUTING_DEPARTMENTS:
        try:
            results = scrape_department(config, session, global_seen_emails)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: College of Engineering ----
    log("\n\nPHASE 2: COLLEGE OF ENGINEERING")
    log("=" * 70)
    for config in ENGINEERING_DEPARTMENTS:
        try:
            results = scrape_department(config, session, global_seen_emails)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 3: College of Sciences ----
    log("\n\nPHASE 3: COLLEGE OF SCIENCES")
    log("=" * 70)
    for config in SCIENCES_DEPARTMENTS:
        try:
            results = scrape_department(config, session, global_seen_emails)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 4: Liberal Arts ----
    log("\n\nPHASE 4: IVAN ALLEN COLLEGE OF LIBERAL ARTS")
    log("=" * 70)
    for config in LIBERAL_ARTS_DEPARTMENTS:
        try:
            results = scrape_department(config, session, global_seen_emails)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 5: Professional Schools ----
    log("\n\nPHASE 5: PROFESSIONAL SCHOOLS")
    log("=" * 70)
    for config in PROFESSIONAL_SCHOOLS:
        try:
            results = scrape_department(config, session, global_seen_emails)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 6: Athletics ----
    log("\n\nPHASE 6: ATHLETICS")
    log("=" * 70)
    try:
        results = scrape_athletics(session, global_seen_emails)
        n = add_results(results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 7: Research Centers ----
    log("\n\nPHASE 7: RESEARCH CENTERS & INTERDISCIPLINARY")
    log("=" * 70)
    for config in RESEARCH_CENTERS:
        try:
            results = scrape_department(config, session, global_seen_emails)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 8: Student Organizations ----
    log("\n\nPHASE 8: STUDENT ORGANIZATIONS")
    log("=" * 70)
    for config in ATHLETICS_AND_ORGS[1:]:  # Skip athletics (already done)
        try:
            results = scrape_department(config, session, global_seen_emails)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 9: Directory API endpoints ----
    log("\n\nPHASE 9: DIRECTORY API ENDPOINTS")
    log("=" * 70)
    try:
        api_results = try_gatech_directory_api(session, global_seen_emails)
        n = add_results(api_results)
        log(f"  => {n} new unique emails from APIs (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR with API endpoints: {e}")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @gatech.edu emails: {len(all_results)}")

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
        [c['department'] for c in COMPUTING_DEPARTMENTS] +
        [c['department'] for c in ENGINEERING_DEPARTMENTS] +
        [c['department'] for c in SCIENCES_DEPARTMENTS] +
        [c['department'] for c in LIBERAL_ARTS_DEPARTMENTS] +
        [c['department'] for c in PROFESSIONAL_SCHOOLS] +
        [c['department'] for c in ATHLETICS_AND_ORGS] +
        [c['department'] for c in RESEARCH_CENTERS]
    )
    depts_with_zero = [d for d in all_depts if d not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in depts_with_zero:
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
