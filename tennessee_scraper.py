#!/usr/bin/env python3
"""
University of Tennessee Knoxville (UTK) Email Scraper
Scrapes @utk.edu emails from department directories, graduate student pages,
research labs, athletics, and student organizations.

UTK departments typically use WordPress or custom CMS with /people/ or /directory/ paths.
Engineering departments are under the Tickle College of Engineering.
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/tennessee_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/tennessee_dept_emails.json'


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_utk_emails(text):
    """Extract all @utk.edu and @vols.utk.edu emails from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*utk\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number prefixed artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*utk\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract utk.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*utk\.edu)',
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
        'oit@', 'itshelp@', 'gradschool@', 'utfi@',
        'studentaffairs@', 'provost@', 'president@',
        'sga@', 'volmail@', 'utk@', 'utkcal@',
        'ask-a-nurse@', 'utcomp@', 'thesis@', 'research@',
        'compliance@', 'audit@', 'procurement@', 'diversity@',
        'titleix@', 'eeo@', 'honors@', 'chancellor@',
        'graduate@', 'commencement@', 'schedule@', 'testing@',
        'counseling@', 'studenthealth@', 'wellness@',
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
                            'research', 'tennessee', 'utk.edu',
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
                        'website', 'tennessee', 'utk.edu',
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
        'li.leaf',
        '.vcard',
        '.grid-item',
        '.teaser',
        '.faculty-staff',
        '.person-listing',
        '.people-listing',
        '.staff-listing',
        '.grad-student',
        '.bio-card',
        '.people-grid-item',
        '.staff-member',
        '.wp-block-column',
        '.wp-block-group',
        '.entry-content li',
        '.personnel-item',
        '.wp-block-media-text',
        '.team-member',
        '.faculty-card',
        '.utk-person',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_utk_emails(text)
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

        # Match profile-like URLs on utk.edu
        if re.search(r'/(people|directory|faculty|staff|graduate-students)/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'directory', 'about',
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
    """Scrape an individual profile page for utk.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_utk_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# Arts & Sciences departments
# UTK uses pattern: https://[dept].utk.edu/people/ or /directory/
ARTS_SCIENCES = [
    {"department": "Economics", "urls": [
        "https://economics.utk.edu/people/",
        "https://economics.utk.edu/people/graduate-students/",
        "https://economics.utk.edu/directory/",
        "https://economics.utk.edu/graduate/current-students/",
        "https://economics.utk.edu/people/phd-students/",
    ]},
    {"department": "Political Science", "urls": [
        "https://polisci.utk.edu/people/",
        "https://polisci.utk.edu/people/graduate-students/",
        "https://polisci.utk.edu/directory/",
        "https://polisci.utk.edu/graduate/current-students/",
        "https://polisci.utk.edu/people/phd-students/",
    ]},
    {"department": "Sociology", "urls": [
        "https://sociology.utk.edu/people/",
        "https://sociology.utk.edu/people/graduate-students/",
        "https://sociology.utk.edu/directory/",
        "https://sociology.utk.edu/graduate/current-students/",
        "https://sociology.utk.edu/people/phd-students/",
    ]},
    {"department": "Psychology", "urls": [
        "https://psychology.utk.edu/people/",
        "https://psychology.utk.edu/people/graduate-students/",
        "https://psychology.utk.edu/directory/",
        "https://psychology.utk.edu/graduate/current-students/",
        "https://psychology.utk.edu/people/phd-students/",
    ]},
    {"department": "History", "urls": [
        "https://history.utk.edu/people/",
        "https://history.utk.edu/people/graduate-students/",
        "https://history.utk.edu/directory/",
        "https://history.utk.edu/graduate/current-students/",
        "https://history.utk.edu/people/phd-students/",
    ]},
    {"department": "English", "urls": [
        "https://english.utk.edu/people/",
        "https://english.utk.edu/people/graduate-students/",
        "https://english.utk.edu/directory/",
        "https://english.utk.edu/graduate/current-students/",
        "https://english.utk.edu/people/phd-students/",
        "https://english.utk.edu/graduate-teaching-associates/",
    ]},
    {"department": "Philosophy", "urls": [
        "https://philosophy.utk.edu/people/",
        "https://philosophy.utk.edu/people/graduate-students/",
        "https://philosophy.utk.edu/directory/",
        "https://philosophy.utk.edu/graduate/current-students/",
    ]},
    {"department": "Mathematics", "urls": [
        "https://math.utk.edu/people/",
        "https://math.utk.edu/people/graduate-students/",
        "https://math.utk.edu/directory/",
        "https://math.utk.edu/graduate/current-students/",
        "https://math.utk.edu/people/graduate-teaching-associates/",
    ]},
    {"department": "Physics & Astronomy", "urls": [
        "https://physics.utk.edu/people/",
        "https://physics.utk.edu/people/graduate-students/",
        "https://physics.utk.edu/directory/",
        "https://physics.utk.edu/graduate/current-students/",
    ]},
    {"department": "Chemistry", "urls": [
        "https://chemistry.utk.edu/people/",
        "https://chemistry.utk.edu/people/graduate-students/",
        "https://chemistry.utk.edu/directory/",
        "https://chemistry.utk.edu/graduate/current-students/",
    ]},
    {"department": "Ecology & Evolutionary Biology (EEB)", "urls": [
        "https://eeb.utk.edu/people/",
        "https://eeb.utk.edu/people/graduate-students/",
        "https://eeb.utk.edu/directory/",
        "https://eeb.utk.edu/graduate/current-students/",
    ]},
    {"department": "Geography", "urls": [
        "https://geography.utk.edu/people/",
        "https://geography.utk.edu/people/graduate-students/",
        "https://geography.utk.edu/directory/",
        "https://geography.utk.edu/graduate/current-students/",
    ]},
    {"department": "Anthropology", "urls": [
        "https://anthropology.utk.edu/people/",
        "https://anthropology.utk.edu/people/graduate-students/",
        "https://anthropology.utk.edu/directory/",
    ]},
    {"department": "Biochemistry & Cellular/Molecular Biology", "urls": [
        "https://bcmb.utk.edu/people/",
        "https://bcmb.utk.edu/people/graduate-students/",
        "https://bcmb.utk.edu/directory/",
    ]},
    {"department": "Microbiology", "urls": [
        "https://micro.utk.edu/people/",
        "https://micro.utk.edu/people/graduate-students/",
        "https://micro.utk.edu/directory/",
    ]},
    {"department": "Earth & Planetary Sciences", "urls": [
        "https://eps.utk.edu/people/",
        "https://eps.utk.edu/people/graduate-students/",
        "https://eps.utk.edu/directory/",
    ]},
    {"department": "Music", "urls": [
        "https://music.utk.edu/people/",
        "https://music.utk.edu/people/graduate-students/",
        "https://music.utk.edu/directory/",
    ]},
    {"department": "Theatre", "urls": [
        "https://theatre.utk.edu/people/",
        "https://theatre.utk.edu/people/graduate-students/",
        "https://theatre.utk.edu/directory/",
    ]},
    {"department": "Religious Studies", "urls": [
        "https://religion.utk.edu/people/",
        "https://religion.utk.edu/people/graduate-students/",
        "https://religion.utk.edu/directory/",
    ]},
    {"department": "Classics", "urls": [
        "https://classics.utk.edu/people/",
        "https://classics.utk.edu/people/graduate-students/",
        "https://classics.utk.edu/directory/",
    ]},
]

# Engineering departments (Tickle College of Engineering)
# Some use their own subdomains, some under tickle.utk.edu
ENGINEERING = [
    {"department": "Electrical Engineering & Computer Science (EECS)", "urls": [
        "https://eecs.utk.edu/people/",
        "https://eecs.utk.edu/people/graduate-students/",
        "https://eecs.utk.edu/directory/",
        "https://eecs.utk.edu/graduate/current-students/",
    ]},
    {"department": "Mechanical, Aerospace & Biomedical Engineering (MABE)", "urls": [
        "https://mabe.utk.edu/people/",
        "https://mabe.utk.edu/people/graduate-students/",
        "https://mabe.utk.edu/directory/",
        "https://me.utk.edu/people/",
        "https://me.utk.edu/people/graduate-students/",
    ]},
    {"department": "Civil & Environmental Engineering (CEE)", "urls": [
        "https://cee.utk.edu/people/",
        "https://cee.utk.edu/people/graduate-students/",
        "https://cee.utk.edu/directory/",
        "https://civil.utk.edu/people/",
        "https://civil.utk.edu/people/graduate-students/",
    ]},
    {"department": "Chemical & Biomolecular Engineering (CBE)", "urls": [
        "https://cbe.utk.edu/people/",
        "https://cbe.utk.edu/people/graduate-students/",
        "https://cbe.utk.edu/directory/",
    ]},
    {"department": "Materials Science & Engineering (MSE)", "urls": [
        "https://mse.utk.edu/people/",
        "https://mse.utk.edu/people/graduate-students/",
        "https://mse.utk.edu/directory/",
    ]},
    {"department": "Nuclear Engineering", "urls": [
        "https://ne.utk.edu/people/",
        "https://ne.utk.edu/people/graduate-students/",
        "https://ne.utk.edu/directory/",
        "https://nuclear.utk.edu/people/",
        "https://nuclear.utk.edu/people/graduate-students/",
    ]},
    {"department": "Biomedical Engineering", "urls": [
        "https://bme.utk.edu/people/",
        "https://bme.utk.edu/people/graduate-students/",
        "https://bme.utk.edu/directory/",
    ]},
    {"department": "Industrial & Systems Engineering (ISE)", "urls": [
        "https://ise.utk.edu/people/",
        "https://ise.utk.edu/people/graduate-students/",
        "https://ise.utk.edu/directory/",
    ]},
    {"department": "Tickle College of Engineering (General)", "urls": [
        "https://tickle.utk.edu/people/",
        "https://tickle.utk.edu/directory/",
        "https://tickle.utk.edu/graduate/current-students/",
    ]},
]

# Professional Schools
PROFESSIONAL = [
    {"department": "Haslam College of Business", "urls": [
        "https://haslam.utk.edu/people/",
        "https://haslam.utk.edu/directory/",
        "https://haslam.utk.edu/phd/current-students/",
        "https://haslam.utk.edu/phd/students/",
        "https://haslam.utk.edu/people/graduate-students/",
        "https://haslam.utk.edu/people/phd-students/",
        "https://haslam.utk.edu/graduate/current-students/",
        "https://haslam.utk.edu/phd/",
    ]},
    {"department": "College of Law", "urls": [
        "https://law.utk.edu/people/",
        "https://law.utk.edu/directory/",
        "https://law.utk.edu/student-organizations/",
        "https://law.utk.edu/people/students/",
        "https://law.utk.edu/students/",
    ]},
    {"department": "College of Nursing", "urls": [
        "https://nursing.utk.edu/people/",
        "https://nursing.utk.edu/directory/",
        "https://nursing.utk.edu/people/graduate-students/",
        "https://nursing.utk.edu/graduate/current-students/",
    ]},
    {"department": "College of Education, Health & Human Sciences (CEHHS)", "urls": [
        "https://cehhs.utk.edu/people/",
        "https://cehhs.utk.edu/directory/",
        "https://cehhs.utk.edu/people/graduate-students/",
        "https://cehhs.utk.edu/graduate/current-students/",
        "https://cehhs.utk.edu/students/",
    ]},
    {"department": "School of Information Sciences (SIS)", "urls": [
        "https://sis.utk.edu/people/",
        "https://sis.utk.edu/directory/",
        "https://sis.utk.edu/people/graduate-students/",
        "https://sis.utk.edu/graduate/current-students/",
        "https://sis.utk.edu/people/phd-students/",
        "https://sis.utk.edu/students/",
    ]},
    {"department": "College of Social Work", "urls": [
        "https://socialwork.utk.edu/people/",
        "https://socialwork.utk.edu/directory/",
        "https://socialwork.utk.edu/people/graduate-students/",
    ]},
    {"department": "College of Architecture & Design", "urls": [
        "https://archdesign.utk.edu/people/",
        "https://archdesign.utk.edu/directory/",
        "https://archdesign.utk.edu/people/graduate-students/",
    ]},
    {"department": "College of Communication & Information", "urls": [
        "https://cci.utk.edu/people/",
        "https://cci.utk.edu/directory/",
        "https://cci.utk.edu/people/graduate-students/",
    ]},
    {"department": "Herbert College of Agriculture", "urls": [
        "https://ag.utk.edu/people/",
        "https://ag.utk.edu/directory/",
        "https://ag.utk.edu/people/graduate-students/",
        "https://herbert.utk.edu/people/",
        "https://herbert.utk.edu/directory/",
    ]},
    {"department": "Baker School of Public Policy & Public Affairs", "urls": [
        "https://bakerschool.utk.edu/people/",
        "https://bakerschool.utk.edu/directory/",
        "https://bakerschool.utk.edu/people/graduate-students/",
        "https://bakerschool.utk.edu/people/phd-students/",
    ]},
]

# Research Labs / Centers
RESEARCH_LABS = [
    {"department": "National Institute for Mathematical & Biological Synthesis (NIMBioS)", "urls": [
        "https://nimbios.utk.edu/people/",
        "https://nimbios.utk.edu/directory/",
        "https://nimbios.utk.edu/graduate-students/",
    ]},
    {"department": "Bredesen Center (Energy Science & Engineering)", "urls": [
        "https://bredesen.utk.edu/people/",
        "https://bredesen.utk.edu/directory/",
        "https://bredesen.utk.edu/students/",
        "https://bredesencenter.utk.edu/people/",
        "https://bredesencenter.utk.edu/students/",
    ]},
    {"department": "Institute for Advanced Materials & Manufacturing (IAMM)", "urls": [
        "https://iamm.utk.edu/people/",
        "https://iamm.utk.edu/directory/",
    ]},
    {"department": "Joint Institutes for Biological Sciences", "urls": [
        "https://jibs.utk.edu/people/",
        "https://jibs.utk.edu/directory/",
    ]},
]

# Athletics
ATHLETICS = [
    {"department": "UT Athletics (Staff)", "urls": [
        "https://utsports.com/staff-directory",
        "https://utsports.com/sports/2017/6/16/staff-html.aspx",
    ]},
]

# Student Organizations
STUDENT_ORGS = [
    {"department": "Student Government Association (SGA)", "urls": [
        "https://sga.utk.edu/",
        "https://sga.utk.edu/about/",
        "https://sga.utk.edu/leadership/",
        "https://sga.utk.edu/officers/",
        "https://sga.utk.edu/executive-branch/",
        "https://sga.utk.edu/directory/",
        "https://sga.utk.edu/senate/",
    ]},
    {"department": "The Daily Beacon (Student Newspaper)", "urls": [
        "https://www.utdailybeacon.com/staff/",
        "https://www.utdailybeacon.com/contact/",
        "https://www.utdailybeacon.com/about/",
        "https://utdailybeacon.com/staff/",
        "https://utdailybeacon.com/about/",
    ]},
    {"department": "Graduate Student Senate", "urls": [
        "https://gss.utk.edu/",
        "https://gss.utk.edu/about/",
        "https://gss.utk.edu/leadership/",
        "https://gss.utk.edu/officers/",
        "https://gss.utk.edu/directory/",
        "https://gss.utk.edu/senate/",
    ]},
    {"department": "Center for Student Engagement", "urls": [
        "https://studentengagement.utk.edu/",
        "https://studentengagement.utk.edu/organizations/",
        "https://getinvolved.utk.edu/",
        "https://getinvolved.utk.edu/organizations/",
    ]},
    {"department": "Vol Network / WUTK (Student Radio)", "urls": [
        "https://wutkradio.com/about/",
        "https://wutkradio.com/staff/",
        "https://wutkradio.com/contact/",
    ]},
]

# Additional graduate directories and pages to scrape
GRAD_DIRECTORIES = [
    {"department": "Graduate School (General)", "urls": [
        "https://gradschool.utk.edu/graduate-student-life/",
        "https://gradschool.utk.edu/about/people/",
        "https://gradschool.utk.edu/about/directory/",
    ]},
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
        successful_url = final_url or url

        # Strategy 1: Extract from structured cards
        card_results = extract_from_person_cards(soup, successful_url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_utk_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*utk\.edu)',
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
                script_emails = extract_utk_emails(script.string)
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
                pg_text_emails = extract_utk_emails(pg_text)
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
# ATHLETICS SCRAPER (utsports.com)
# ============================================================

def scrape_athletics(session):
    """Scrape UT Vols athletics staff directory for @utk.edu emails."""
    results = []
    seen_emails = set()
    department = "UT Athletics (Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://utsports.com/staff-directory",
        "https://utsports.com/sports/2017/6/16/staff-html.aspx",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_utk_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} UTK emails on page")

        # Try to associate names with emails from the staff directory
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if 'mailto:' in href.lower():
                match = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*utk\.edu)', href, re.IGNORECASE)
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
        for page_url in pagination_urls:
            log(f"    Paginated athletics: {page_url}")
            page_soup, _ = get_soup(page_url, session)
            if page_soup:
                pg_text = page_soup.get_text(separator=' ', strip=True)
                pg_emails = extract_utk_emails(pg_text)
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
    log("UNIVERSITY OF TENNESSEE KNOXVILLE EMAIL SCRAPER")
    log("Domain: @utk.edu (including subdomains)")
    log("=" * 70)

    # ---- Phase 1: Arts & Sciences ----
    log("\n\n" + "=" * 70)
    log("PHASE 1: Arts & Sciences Departments")
    log("=" * 70)

    for config in ARTS_SCIENCES:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Phase 2: Engineering ----
    log("\n\n" + "=" * 70)
    log("PHASE 2: Tickle College of Engineering")
    log("=" * 70)

    for config in ENGINEERING:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Phase 3: Professional Schools ----
    log("\n\n" + "=" * 70)
    log("PHASE 3: Professional Schools")
    log("=" * 70)

    for config in PROFESSIONAL:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Phase 4: Research Labs ----
    log("\n\n" + "=" * 70)
    log("PHASE 4: Research Labs & Centers")
    log("=" * 70)

    for config in RESEARCH_LABS:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Phase 5: Graduate Directories ----
    log("\n\n" + "=" * 70)
    log("PHASE 5: Graduate Directories")
    log("=" * 70)

    for config in GRAD_DIRECTORIES:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Phase 6: Athletics ----
    log("\n\n" + "=" * 70)
    log("PHASE 6: Athletics")
    log("=" * 70)

    try:
        athletics_results = scrape_athletics(session)
        n = add_results(athletics_results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 7: Student Organizations ----
    log("\n\n" + "=" * 70)
    log("PHASE 7: Student Organizations")
    log("=" * 70)

    for config in STUDENT_ORGS:
        try:
            dept_results = scrape_department(config, session)
            n = add_results(dept_results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @utk.edu emails: {len(all_results)}")

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

    all_dept_names = set()
    for group in [ARTS_SCIENCES, ENGINEERING, PROFESSIONAL, RESEARCH_LABS, GRAD_DIRECTORIES, ATHLETICS, STUDENT_ORGS]:
        for c in group:
            all_dept_names.add(c['department'])

    depts_with_zero = all_dept_names - set(dept_counts.keys())
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in sorted(depts_with_zero):
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
