#!/usr/bin/env python3
"""
University of Oklahoma (OU) Email Scraper
Scrapes @ou.edu emails from department directories, engineering,
professional schools, athletics, research labs, and student organizations.

OU departments typically use:
  - https://www.ou.edu/cas/[dept]/people  (Arts & Sciences)
  - https://www.ou.edu/coe/[dept]/people  (Engineering)
  - Subdomains for professional schools: price.ou.edu, law.ou.edu, etc.
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/oklahoma_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/oklahoma_dept_emails.json'


# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# College of Arts & Sciences - https://www.ou.edu/cas/[dept]/people
CAS_DEPARTMENTS = [
    {"department": "Economics", "urls": [
        "https://www.ou.edu/cas/economics/people",
        "https://www.ou.edu/cas/economics/people/graduate-students",
        "https://www.ou.edu/cas/economics/people/students",
        "https://www.ou.edu/cas/economics/people/phd-students",
        "https://www.ou.edu/cas/economics/directory",
    ]},
    {"department": "Political Science", "urls": [
        "https://www.ou.edu/cas/political-science/people",
        "https://www.ou.edu/cas/political-science/people/graduate-students",
        "https://www.ou.edu/cas/political-science/people/students",
        "https://www.ou.edu/cas/polisci/people",
        "https://www.ou.edu/cas/polisci/people/graduate-students",
    ]},
    {"department": "Sociology", "urls": [
        "https://www.ou.edu/cas/sociology/people",
        "https://www.ou.edu/cas/sociology/people/graduate-students",
        "https://www.ou.edu/cas/sociology/people/students",
        "https://www.ou.edu/cas/sociology/directory",
    ]},
    {"department": "Psychology", "urls": [
        "https://www.ou.edu/cas/psychology/people",
        "https://www.ou.edu/cas/psychology/people/graduate-students",
        "https://www.ou.edu/cas/psychology/people/students",
        "https://www.ou.edu/cas/psychology/directory",
    ]},
    {"department": "History", "urls": [
        "https://www.ou.edu/cas/history/people",
        "https://www.ou.edu/cas/history/people/graduate-students",
        "https://www.ou.edu/cas/history/people/students",
        "https://www.ou.edu/cas/history/directory",
    ]},
    {"department": "English", "urls": [
        "https://www.ou.edu/cas/english/people",
        "https://www.ou.edu/cas/english/people/graduate-students",
        "https://www.ou.edu/cas/english/people/students",
        "https://www.ou.edu/cas/english/directory",
    ]},
    {"department": "Philosophy", "urls": [
        "https://www.ou.edu/cas/philosophy/people",
        "https://www.ou.edu/cas/philosophy/people/graduate-students",
        "https://www.ou.edu/cas/philosophy/people/students",
        "https://www.ou.edu/cas/philosophy/directory",
    ]},
    {"department": "Mathematics", "urls": [
        "https://www.ou.edu/cas/math/people",
        "https://www.ou.edu/cas/math/people/graduate-students",
        "https://www.ou.edu/cas/math/people/students",
        "https://www.ou.edu/cas/mathematics/people",
        "https://www.ou.edu/cas/mathematics/people/graduate-students",
    ]},
    {"department": "Physics & Astronomy", "urls": [
        "https://www.ou.edu/cas/physics-astronomy/people",
        "https://www.ou.edu/cas/physics-astronomy/people/graduate-students",
        "https://www.ou.edu/cas/physics-astronomy/people/students",
        "https://www.ou.edu/cas/physics/people",
        "https://www.ou.edu/cas/physics/people/graduate-students",
    ]},
    {"department": "Chemistry & Biochemistry", "urls": [
        "https://www.ou.edu/cas/chemistry/people",
        "https://www.ou.edu/cas/chemistry/people/graduate-students",
        "https://www.ou.edu/cas/chemistry/people/students",
        "https://www.ou.edu/cas/chemistry-biochemistry/people",
        "https://www.ou.edu/cas/chemistry-biochemistry/people/graduate-students",
    ]},
    {"department": "Biology", "urls": [
        "https://www.ou.edu/cas/biology/people",
        "https://www.ou.edu/cas/biology/people/graduate-students",
        "https://www.ou.edu/cas/biology/people/students",
        "https://www.ou.edu/cas/biology/directory",
    ]},
    {"department": "Geosciences", "urls": [
        "https://www.ou.edu/cas/geosciences/people",
        "https://www.ou.edu/cas/geosciences/people/graduate-students",
        "https://www.ou.edu/cas/geosciences/people/students",
        "https://www.ou.edu/cas/geology/people",
        "https://www.ou.edu/cas/geology/people/graduate-students",
    ]},
    {"department": "Anthropology", "urls": [
        "https://www.ou.edu/cas/anthropology/people",
        "https://www.ou.edu/cas/anthropology/people/graduate-students",
        "https://www.ou.edu/cas/anthropology/people/students",
        "https://www.ou.edu/cas/anthropology/directory",
    ]},
    {"department": "Geography & Environmental Sustainability", "urls": [
        "https://www.ou.edu/cas/geography/people",
        "https://www.ou.edu/cas/geography/people/graduate-students",
        "https://www.ou.edu/cas/geography/people/students",
        "https://www.ou.edu/cas/ges/people",
        "https://www.ou.edu/cas/ges/people/graduate-students",
    ]},
    {"department": "Communication", "urls": [
        "https://www.ou.edu/cas/comm/people",
        "https://www.ou.edu/cas/comm/people/graduate-students",
        "https://www.ou.edu/cas/communication/people",
        "https://www.ou.edu/cas/communication/people/graduate-students",
    ]},
    {"department": "Modern Languages, Literatures & Linguistics", "urls": [
        "https://www.ou.edu/cas/modlang/people",
        "https://www.ou.edu/cas/modlang/people/graduate-students",
        "https://www.ou.edu/cas/modlang/people/students",
    ]},
    {"department": "Microbiology & Plant Biology", "urls": [
        "https://www.ou.edu/cas/mpbio/people",
        "https://www.ou.edu/cas/mpbio/people/graduate-students",
        "https://www.ou.edu/cas/microbiology-plant-biology/people",
        "https://www.ou.edu/cas/microbiology-plant-biology/people/graduate-students",
    ]},
    {"department": "Social Work", "urls": [
        "https://www.ou.edu/cas/socialwork/people",
        "https://www.ou.edu/cas/socialwork/people/graduate-students",
        "https://www.ou.edu/cas/social-work/people",
        "https://www.ou.edu/cas/social-work/people/graduate-students",
    ]},
    {"department": "Letters", "urls": [
        "https://www.ou.edu/cas/letters/people",
        "https://www.ou.edu/cas/letters/people/graduate-students",
    ]},
    {"department": "Religious Studies", "urls": [
        "https://www.ou.edu/cas/religiousstudies/people",
        "https://www.ou.edu/cas/religiousstudies/people/graduate-students",
        "https://www.ou.edu/cas/religious-studies/people",
    ]},
    {"department": "Classics & Letters", "urls": [
        "https://www.ou.edu/cas/classicsandletters/people",
        "https://www.ou.edu/cas/classicsandletters/people/graduate-students",
    ]},
    {"department": "Human Relations", "urls": [
        "https://www.ou.edu/cas/hr/people",
        "https://www.ou.edu/cas/hr/people/graduate-students",
        "https://www.ou.edu/cas/human-relations/people",
        "https://www.ou.edu/cas/human-relations/people/graduate-students",
    ]},
]

# Gallogly College of Engineering - https://www.ou.edu/coe/[dept]/people
ENGINEERING_DEPARTMENTS = [
    {"department": "Computer Science", "urls": [
        "https://www.ou.edu/coe/cs/people",
        "https://www.ou.edu/coe/cs/people/graduate-students",
        "https://www.ou.edu/coe/cs/people/students",
        "https://www.ou.edu/coe/cs/directory",
    ]},
    {"department": "Electrical & Computer Engineering", "urls": [
        "https://www.ou.edu/coe/ece/people",
        "https://www.ou.edu/coe/ece/people/graduate-students",
        "https://www.ou.edu/coe/ece/people/students",
        "https://www.ou.edu/coe/ece/directory",
    ]},
    {"department": "Mechanical & Aerospace Engineering", "urls": [
        "https://www.ou.edu/coe/mae/people",
        "https://www.ou.edu/coe/mae/people/graduate-students",
        "https://www.ou.edu/coe/mae/people/students",
        "https://www.ou.edu/coe/ame/people",
        "https://www.ou.edu/coe/ame/people/graduate-students",
    ]},
    {"department": "Civil Engineering & Environmental Science", "urls": [
        "https://www.ou.edu/coe/cees/people",
        "https://www.ou.edu/coe/cees/people/graduate-students",
        "https://www.ou.edu/coe/cees/people/students",
        "https://www.ou.edu/coe/cees/directory",
    ]},
    {"department": "Chemical, Biological & Materials Engineering", "urls": [
        "https://www.ou.edu/coe/cbme/people",
        "https://www.ou.edu/coe/cbme/people/graduate-students",
        "https://www.ou.edu/coe/cbme/people/students",
        "https://www.ou.edu/coe/cbme/directory",
    ]},
    {"department": "Aerospace & Mechanical Engineering", "urls": [
        "https://www.ou.edu/coe/ame/people",
        "https://www.ou.edu/coe/ame/people/graduate-students",
        "https://www.ou.edu/coe/ame/people/students",
    ]},
    {"department": "Industrial & Systems Engineering", "urls": [
        "https://www.ou.edu/coe/ise/people",
        "https://www.ou.edu/coe/ise/people/graduate-students",
        "https://www.ou.edu/coe/ise/people/students",
        "https://www.ou.edu/coe/ise/directory",
    ]},
    {"department": "Petroleum & Geological Engineering", "urls": [
        "https://www.ou.edu/coe/petroleum/people",
        "https://www.ou.edu/coe/petroleum/people/graduate-students",
        "https://www.ou.edu/coe/petroleum/people/students",
        "https://www.ou.edu/coe/pge/people",
        "https://www.ou.edu/coe/pge/people/graduate-students",
    ]},
    {"department": "Biomedical Engineering", "urls": [
        "https://www.ou.edu/coe/bme/people",
        "https://www.ou.edu/coe/bme/people/graduate-students",
        "https://www.ou.edu/coe/bme/people/students",
    ]},
    {"department": "College of Engineering (General)", "urls": [
        "https://www.ou.edu/coe/people",
        "https://www.ou.edu/coe/directory",
    ]},
]

# Professional Schools
PROFESSIONAL_SCHOOLS = [
    {"department": "Price College of Business", "urls": [
        "https://price.ou.edu/people/",
        "https://price.ou.edu/directory/",
        "https://price.ou.edu/people/graduate-students/",
        "https://price.ou.edu/people/phd-students/",
        "https://price.ou.edu/phd/students/",
        "https://price.ou.edu/phd/",
        "https://price.ou.edu/mba/",
        "https://www.ou.edu/price/people",
        "https://www.ou.edu/price/directory",
    ]},
    {"department": "College of Law", "urls": [
        "https://law.ou.edu/directory",
        "https://law.ou.edu/directory/students",
        "https://law.ou.edu/student-organizations",
        "https://law.ou.edu/students",
        "https://law.ou.edu/people",
        "https://www.law.ou.edu/directory",
        "https://www.law.ou.edu/people",
    ]},
    {"department": "College of Medicine", "urls": [
        "https://medicine.ou.edu/directory",
        "https://medicine.ou.edu/people",
        "https://medicine.ou.edu/departments",
        "https://www.ou.edu/medicine/directory",
        "https://www.ou.edu/medicine/people",
    ]},
    {"department": "College of Pharmacy", "urls": [
        "https://pharmacy.ou.edu/directory",
        "https://pharmacy.ou.edu/people",
        "https://pharmacy.ou.edu/people/graduate-students",
        "https://pharmacy.ou.edu/people/students",
        "https://www.ou.edu/pharmacy/people",
        "https://www.ou.edu/pharmacy/directory",
    ]},
    {"department": "College of Education", "urls": [
        "https://education.ou.edu/directory",
        "https://education.ou.edu/people",
        "https://education.ou.edu/people/graduate-students",
        "https://education.ou.edu/people/students",
        "https://www.ou.edu/education/directory",
        "https://www.ou.edu/education/people",
        "https://www.ou.edu/education/people/graduate-students",
    ]},
    {"department": "Gaylord College of Journalism", "urls": [
        "https://gaylord.ou.edu/directory",
        "https://gaylord.ou.edu/people",
        "https://gaylord.ou.edu/people/graduate-students",
        "https://gaylord.ou.edu/people/students",
        "https://www.ou.edu/gaylord/people",
        "https://www.ou.edu/gaylord/people/graduate-students",
        "https://www.ou.edu/gaylord/directory",
    ]},
    {"department": "Weitzenhoffer School of Musical Theatre", "urls": [
        "https://www.ou.edu/finearts/music/people",
        "https://www.ou.edu/finearts/music/people/graduate-students",
        "https://www.ou.edu/finearts/drama/people",
        "https://www.ou.edu/finearts/drama/people/graduate-students",
    ]},
    {"department": "Dodge Family College of Arts & Sciences (Dean)", "urls": [
        "https://www.ou.edu/cas/people",
        "https://www.ou.edu/cas/directory",
    ]},
    {"department": "Graduate College", "urls": [
        "https://www.ou.edu/gradcollege/people",
        "https://www.ou.edu/gradcollege/directory",
        "https://www.ou.edu/gradcollege/about/people",
    ]},
    {"department": "College of Atmospheric & Geographic Sciences", "urls": [
        "https://www.ou.edu/cags/people",
        "https://www.ou.edu/cags/people/graduate-students",
        "https://www.ou.edu/cags/directory",
    ]},
    {"department": "Gibbs College of Architecture", "urls": [
        "https://www.ou.edu/architecture/people",
        "https://www.ou.edu/architecture/people/graduate-students",
        "https://www.ou.edu/architecture/directory",
    ]},
    {"department": "College of Professional & Continuing Studies", "urls": [
        "https://pacs.ou.edu/people",
        "https://pacs.ou.edu/directory",
        "https://www.ou.edu/pacs/people",
    ]},
]

# Research Centers & Labs
RESEARCH_LABS = [
    {"department": "National Weather Center / School of Meteorology", "urls": [
        "https://www.ou.edu/cas/meteorology/people",
        "https://www.ou.edu/cas/meteorology/people/graduate-students",
        "https://www.ou.edu/cas/meteorology/people/students",
        "https://www.ou.edu/nwc/people",
        "https://www.ou.edu/nwc/directory",
    ]},
    {"department": "Stephenson Research & Technology Center", "urls": [
        "https://www.ou.edu/srtc/people",
        "https://www.ou.edu/srtc/directory",
    ]},
    {"department": "Institute for Quality Communities", "urls": [
        "https://iqc.ou.edu/people",
        "https://iqc.ou.edu/directory",
    ]},
    {"department": "Sam Noble Museum", "urls": [
        "https://samnoblemuseum.ou.edu/people",
        "https://samnoblemuseum.ou.edu/staff",
        "https://samnoblemuseum.ou.edu/about-the-museum/staff/",
        "https://www.ou.edu/snomnh/people",
    ]},
    {"department": "OU Biological Station", "urls": [
        "https://www.ou.edu/ubs/people",
        "https://www.ou.edu/ubs/directory",
        "https://biostation.ou.edu/people",
    ]},
    {"department": "Oklahoma Geological Survey", "urls": [
        "https://www.ou.edu/ogs/people",
        "https://www.ou.edu/ogs/directory",
    ]},
]

# Athletics
ATHLETICS = [
    {"department": "Athletics (Staff)", "urls": [
        "https://soonersports.com/staff-directory",
        "https://soonersports.com/sports/2019/12/11/staff-directory.aspx",
    ]},
]

# Student Organizations
STUDENT_ORGS = [
    {"department": "Student Government Association", "urls": [
        "https://www.ou.edu/sga",
        "https://www.ou.edu/sga/about",
        "https://www.ou.edu/sga/leadership",
        "https://www.ou.edu/sga/officers",
        "https://www.ou.edu/sga/executive",
        "https://www.ou.edu/sga/directory",
    ]},
    {"department": "OU Daily (Student Newspaper)", "urls": [
        "https://www.oudaily.com/staff/",
        "https://www.oudaily.com/contact/",
        "https://www.oudaily.com/about/",
    ]},
    {"department": "Graduate Student Senate", "urls": [
        "https://www.ou.edu/gss",
        "https://www.ou.edu/gss/about",
        "https://www.ou.edu/gss/leadership",
        "https://www.ou.edu/gss/officers",
        "https://www.ou.edu/gss/directory",
    ]},
    {"department": "Student Organizations", "urls": [
        "https://www.ou.edu/getinvolved/student_organizations",
        "https://getinvolved.ou.edu/organizations",
        "https://www.ou.edu/studentlife/organizations",
    ]},
    {"department": "Honors College", "urls": [
        "https://www.ou.edu/honors/people",
        "https://www.ou.edu/honors/people/students",
        "https://www.ou.edu/honors/directory",
    ]},
]


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_ou_emails(text):
    """Extract all @ou.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*ou\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Skip obviously wrong patterns (phone numbers glued to emails)
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*ou\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract ou.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*ou\.edu)',
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
        'noreply@', 'do-not-reply@', 'donotreply@', 'testing@',
        'counseling@', 'transit@', 'payroll@', 'gradschool@',
        'oupress@', 'oudaily@', 'webcomm@', 'ouit@', 'soonercard@',
        'soonerathletics@', 'compliance@', 'ticket@', 'tickets@',
        'copyright@', 'accessibility@', 'title-ix@', 'titleix@',
        'provost@', 'president@', 'vicepresident@', 'chancellor@',
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
                            'read more', 'view profile', 'website', 'lab',
                            'research', 'curriculum', 'course',
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
    """Extract people from card/grid-based layouts (OU WordPress/Drupal sites)."""
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
        '.people-listing__person',
        '.ou-people-card',
        '.ou-card',
        '.wp-block-column',
        '.entry-content',
        '.type-people',
        '.people-grid-item',
        '[class*="bio"]',
        '[class*="staff"]',
        '[class*="faculty"]',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_ou_emails(text)
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

        # Match OU profile-like URLs
        if re.search(r'/people/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'directory', 'about',
                    'phd', 'staff', 'adjunct',
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session, department):
    """Scrape an individual profile page for ou.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_ou_emails(text)
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
        text_emails = extract_ou_emails(page_text)
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*ou\.edu)',
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
                script_emails = extract_ou_emails(script.string)
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
                pg_text_emails = extract_ou_emails(pg_text)
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
# ATHLETICS SCRAPER (soonersports.com)
# ============================================================

def scrape_athletics(session):
    """Scrape OU Sooners athletics staff directory for @ou.edu emails."""
    results = []
    seen_emails = set()
    department = "Athletics (Staff)"

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    urls = [
        "https://soonersports.com/staff-directory",
        "https://soonersports.com/sports/2019/12/11/staff-directory.aspx",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_ou_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} OU emails on page")

        # Check for staff cards
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
                pg_emails = extract_ou_emails(pg_text)
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

        # Follow individual staff profile links (limit to avoid crawling hundreds)
        followed = set()
        follow_count = 0
        max_follow = 80
        for a_tag in soup.find_all('a', href=True):
            if follow_count >= max_follow:
                break
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if full_url in followed or full_url == url:
                continue
            if '/staff-directory/' in href.lower() and ('ou.edu' in full_url or 'soonersports' in full_url):
                followed.add(full_url)
                follow_count += 1
                log(f"    Following staff link [{follow_count}/{max_follow}]: {full_url}")
                sub_soup, sub_url = get_soup(full_url, session)
                if sub_soup:
                    sub_text = sub_soup.get_text(separator=' ', strip=True)
                    sub_emails = extract_ou_emails(sub_text)
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
    log("UNIVERSITY OF OKLAHOMA EMAIL SCRAPER")
    log("Domain: @ou.edu")
    log("=" * 70)

    # ---- Phase 1: College of Arts & Sciences ----
    log("\n\nPHASE 1: COLLEGE OF ARTS & SCIENCES (CAS)")
    log("=" * 70)

    for config in CAS_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 2: Engineering ----
    log("\n\nPHASE 2: GALLOGLY COLLEGE OF ENGINEERING")
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

    # ---- Phase 4: Research Labs ----
    log("\n\nPHASE 4: RESEARCH CENTERS & LABS")
    log("=" * 70)

    for config in RESEARCH_LABS:
        try:
            results = scrape_department(config, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added (running total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    # ---- Phase 5: Athletics ----
    log("\n\nPHASE 5: ATHLETICS")
    log("=" * 70)

    try:
        results = scrape_athletics(session)
        n = add_results(results)
        log(f"  => {n} new unique emails added (running total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    # ---- Phase 6: Student Organizations ----
    log("\n\nPHASE 6: STUDENT ORGANIZATIONS")
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
    log(f"Total unique @ou.edu emails: {len(all_results)}")

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
        [c['department'] for c in CAS_DEPARTMENTS] +
        [c['department'] for c in ENGINEERING_DEPARTMENTS] +
        [c['department'] for c in PROFESSIONAL_SCHOOLS] +
        [c['department'] for c in RESEARCH_LABS] +
        [c['department'] for c in ATHLETICS] +
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
