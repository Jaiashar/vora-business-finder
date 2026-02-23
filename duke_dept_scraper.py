#!/usr/bin/env python3
"""
Duke University Email Scraper
Scrapes @duke.edu emails from department people pages, professional schools,
athletics, student orgs, and research centers.
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

# Arts & Sciences (Trinity College)
ARTS_SCIENCES_DEPTS = [
    {
        "department": "Economics",
        "urls": [
            "https://econ.duke.edu/people/phd-students",
            "https://econ.duke.edu/people/graduate-students",
            "https://econ.duke.edu/people/students/phd-students",
            "https://econ.duke.edu/people/students",
        ],
    },
    {
        "department": "Political Science",
        "urls": [
            "https://polisci.duke.edu/people/students/phd-students",
            "https://polisci.duke.edu/people/students/masters-students",
            "https://polisci.duke.edu/people/students/mape-students",
            "https://polisci.duke.edu/people/graduate-students",
        ],
    },
    {
        "department": "Sociology",
        "urls": [
            "https://sociology.duke.edu/people/graduate-students",
            "https://sociology.duke.edu/people/students/phd-students",
            "https://sociology.duke.edu/people/students",
        ],
    },
    {
        "department": "Psychology & Neuroscience",
        "urls": [
            "https://psychandneuro.duke.edu/people/graduate-students",
            "https://psychandneuro.duke.edu/people/students/phd-students",
            "https://psychandneuro.duke.edu/people/students",
        ],
    },
    {
        "department": "History",
        "urls": [
            "https://history.duke.edu/people/graduate-students",
            "https://history.duke.edu/people/students/phd-students",
            "https://history.duke.edu/people/students",
        ],
    },
    {
        "department": "English",
        "urls": [
            "https://english.duke.edu/people/graduate-students",
            "https://english.duke.edu/people/students/phd-students",
            "https://english.duke.edu/people/students",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.duke.edu/people/graduate-students",
            "https://philosophy.duke.edu/people/students/phd-students",
            "https://philosophy.duke.edu/people/students",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://linguisticsprogram.duke.edu/people/",
            "https://linguisticsprogram.duke.edu/people/graduate-students",
            "https://linguisticsprogram.duke.edu/people/students",
        ],
    },
    {
        "department": "Mathematics",
        "urls": [
            "https://math.duke.edu/people/graduate-students",
            "https://math.duke.edu/people/students/phd-students",
        ],
    },
    {
        "department": "Statistical Science",
        "urls": [
            "https://stat.duke.edu/people/graduate-students",
            "https://stat.duke.edu/people/students/phd-students",
            "https://stat.duke.edu/people/students",
        ],
    },
    {
        "department": "Physics",
        "urls": [
            "https://phy.duke.edu/people/graduate-students",
            "https://phy.duke.edu/people/students/phd-students",
            "https://phy.duke.edu/people/students",
            "https://physics.duke.edu/people/graduate-students",
        ],
    },
    {
        "department": "Chemistry",
        "urls": [
            "https://chem.duke.edu/people/graduate-students",
            "https://chem.duke.edu/people/students/phd-students",
            "https://chem.duke.edu/people/students",
        ],
    },
    {
        "department": "Biology",
        "urls": [
            "https://biology.duke.edu/people/graduate-students",
            "https://biology.duke.edu/people/students/phd-students",
            "https://biology.duke.edu/people/students",
        ],
    },
    {
        "department": "Nicholas School of the Environment",
        "urls": [
            "https://nicholas.duke.edu/people/students",
            "https://nicholas.duke.edu/people/graduate-students",
        ],
    },
    {
        "department": "Cultural Anthropology",
        "urls": [
            "https://culturalanthropology.duke.edu/people/graduate-students",
            "https://culturalanthropology.duke.edu/people/students/phd-students",
            "https://culturalanthropology.duke.edu/people/students",
        ],
    },
    {
        "department": "Romance Studies",
        "urls": [
            "https://romancestudies.duke.edu/people/graduate-students",
            "https://romancestudies.duke.edu/people/students/phd-students",
            "https://romancestudies.duke.edu/people/students",
        ],
    },
    {
        "department": "German Studies",
        "urls": [
            "https://german.duke.edu/people/graduate-students",
            "https://german.duke.edu/people/students/phd-students",
            "https://german.duke.edu/people/students",
        ],
    },
    {
        "department": "Asian & Middle Eastern Studies",
        "urls": [
            "https://asianandmiddleeastern.duke.edu/people/graduate-students",
            "https://asianandmiddleeastern.duke.edu/people/students/phd-students",
            "https://asianandmiddleeastern.duke.edu/people/students",
        ],
    },
    {
        "department": "Classical Studies",
        "urls": [
            "https://classicalstudies.duke.edu/people/graduate-students",
            "https://classicalstudies.duke.edu/people/students/phd-students",
            "https://classicalstudies.duke.edu/people/students",
        ],
    },
    {
        "department": "Religion",
        "urls": [
            "https://religion.duke.edu/people/graduate-students",
            "https://religion.duke.edu/people/students/phd-students",
            "https://religion.duke.edu/people/students",
        ],
    },
]

# Pratt School of Engineering
ENGINEERING_DEPTS = [
    {
        "department": "Electrical & Computer Engineering (ECE)",
        "urls": [
            "https://ece.duke.edu/people/graduate-students",
            "https://ece.duke.edu/people/students/phd-students",
            "https://ece.duke.edu/people/phd-students",
        ],
    },
    {
        "department": "Mechanical Engineering & Materials Science (MEMS)",
        "urls": [
            "https://mems.duke.edu/people/graduate-students",
            "https://mems.duke.edu/people/students/phd-students",
            "https://mems.duke.edu/people/phd-students",
        ],
    },
    {
        "department": "Biomedical Engineering (BME)",
        "urls": [
            "https://bme.duke.edu/people/graduate-students",
            "https://bme.duke.edu/people/students/phd-students",
            "https://bme.duke.edu/people/phd-students",
        ],
    },
    {
        "department": "Civil & Environmental Engineering (CEE)",
        "urls": [
            "https://cee.duke.edu/people/graduate-students",
            "https://cee.duke.edu/people/students/phd-students",
            "https://cee.duke.edu/people/phd-students",
        ],
    },
    {
        "department": "Computer Science",
        "urls": [
            "https://cs.duke.edu/phd-program",
            "https://cs.duke.edu/people/graduate-students",
            "https://cs.duke.edu/masters-computer-science",
            "https://cs.duke.edu/masters-economics-and-computation",
        ],
    },
]

# Professional Schools
PROFESSIONAL_SCHOOLS = [
    {
        "department": "Fuqua School of Business (PhD)",
        "urls": [
            "https://fuqua.duke.edu/phd/",
            "https://fuqua.duke.edu/faculty-and-research/phd-students",
            "https://fuqua.duke.edu/programs/phd/students",
        ],
    },
    {
        "department": "Sanford School of Public Policy",
        "urls": [
            "https://sanford.duke.edu/people/students",
            "https://sanford.duke.edu/people/graduate-students",
            "https://sanford.duke.edu/people/students/phd-students",
        ],
    },
    {
        "department": "School of Nursing",
        "urls": [
            "https://nursing.duke.edu/people/students",
            "https://nursing.duke.edu/people/graduate-students",
        ],
    },
    {
        "department": "Divinity School",
        "urls": [
            "https://divinity.duke.edu/people/students",
            "https://divinity.duke.edu/people/graduate-students",
        ],
    },
    {
        "department": "Duke Law School",
        "urls": [
            "https://law.duke.edu/students/orgs/",
            "https://law.duke.edu/students/",
        ],
    },
    {
        "department": "Duke School of Medicine",
        "urls": [
            "https://medschool.duke.edu/education/student-services/student-organizations",
            "https://medschool.duke.edu/about-us/student-organizations",
        ],
    },
]

# Athletics
ATHLETICS_URLS = [
    {
        "department": "Duke Athletics",
        "urls": [
            "https://goduke.com/staff-directory",
        ],
    },
]

# Research Centers
RESEARCH_CENTERS = [
    {
        "department": "Duke Institute for Health Innovation (DIHI)",
        "urls": [
            "https://dihi.org/people/",
            "https://dihi.org/team/",
        ],
    },
    {
        "department": "Bass Connections",
        "urls": [
            "https://bass.duke.edu/people/",
            "https://bassconnections.duke.edu/people/",
        ],
    },
]

# Additional pages to try for more coverage
SUPPLEMENTAL_PAGES = [
    {
        "department": "Computer Science (MS/ECE)",
        "urls": [
            "https://cs.duke.edu/affiliated-graduate-students",
        ],
    },
    {
        "department": "Biostatistics & Bioinformatics",
        "urls": [
            "https://biostat.duke.edu/people/graduate-students",
            "https://biostat.duke.edu/people/students",
        ],
    },
    {
        "department": "Computational Biology & Bioinformatics",
        "urls": [
            "https://genome.duke.edu/education/students",
            "https://cbb.duke.edu/people/graduate-students",
        ],
    },
    {
        "department": "Public Policy (PhD)",
        "urls": [
            "https://sanford.duke.edu/people/students/phd-students",
        ],
    },
    {
        "department": "Earth & Climate Sciences",
        "urls": [
            "https://earthclimate.duke.edu/people/graduate-students",
            "https://earthclimate.duke.edu/people/students",
        ],
    },
    {
        "department": "Art, Art History & Visual Studies",
        "urls": [
            "https://aahvs.duke.edu/people/graduate-students",
            "https://aahvs.duke.edu/people/students",
        ],
    },
    {
        "department": "Music",
        "urls": [
            "https://music.duke.edu/people/graduate-students",
            "https://music.duke.edu/people/students",
        ],
    },
    {
        "department": "Gender, Sexuality & Feminist Studies",
        "urls": [
            "https://gendersexualityfeminist.duke.edu/people/graduate-students",
            "https://gendersexualityfeminist.duke.edu/people/students",
        ],
    },
    {
        "department": "African & African American Studies",
        "urls": [
            "https://aaas.duke.edu/people/graduate-students",
            "https://aaas.duke.edu/people/students",
        ],
    },
]


# ============================================================
# EMAIL EXTRACTION
# ============================================================

def extract_duke_emails(text):
    """Extract all @duke.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*duke\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup, domain='duke.edu'):
    """Extract duke.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*duke\.edu)',
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
        'admissions@', 'math-us@', 'math-phd', 'math-dgsa@',
        'polisci@', 'econ@', 'english@', 'history@',
        'phd-program@', 'dgs@', 'dus@', 'biology@',
        'chem@', 'physics@', 'cs-grad@', 'compsci@',
        'pratt@', 'nursing@', 'divinity@', 'sanford@',
        'fuqua@', 'law-admissions@', 'lawschool@',
        'medadmiss@', 'accessibility@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) or p in email_lower for p in admin_patterns)


def is_likely_faculty_title(text):
    """Check if text contains faculty/staff titles."""
    faculty_indicators = [
        'professor', 'chair', 'director', 'dean', 'associate professor',
        'assistant professor', 'lecturer', 'instructor', 'coordinator',
        'manager', 'administrator', 'business manager', 'program coord',
        'program dir', 'supervisor', 'emerit', 'fellow', 'postdoc',
    ]
    text_lower = text.lower()
    return any(f in text_lower for f in faculty_indicators)


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
                            'read more', 'view profile', 'website', 'personal',
                            'google scholar', 'orcid', 'dblp',
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
                        'read more', 'department', 'faculty', 'office', 'http',
                        'website', 'personal', 'google scholar', 'orcid',
                    ]):
                        return name
            parent = parent.parent

    return ""


# ============================================================
# STRUCTURED EXTRACTION: Duke Drupal people listings
# ============================================================

def extract_from_duke_listing(soup, url, department):
    """
    Extract people from Duke's standard Drupal people listing pages.
    These pages use <li> items with name as a link and email as mailto.
    """
    results = []
    seen_emails = set()

    # ----- Strategy A: Extract from list items (most Duke dept pages) -----
    # Duke pages typically have: <li><a href="scholars.duke.edu/...">Name</a> \n <a href="mailto:...">email</a></li>
    for li in soup.find_all('li'):
        text = li.get_text(separator=' ', strip=True)
        emails_in_li = extract_duke_emails(text)
        mailto_in_li = extract_mailto_emails(li, 'duke.edu')
        all_emails = list(set(emails_in_li + mailto_in_li))

        for email in all_emails:
            if email in seen_emails or is_admin_email(email):
                continue

            # Try to get name from links/headings in this li
            name = ""
            for tag in li.find_all(['a', 'h2', 'h3', 'h4', 'h5', 'strong', 'b']):
                tag_text = tag.get_text(strip=True)
                href = tag.get('href', '') if tag.name == 'a' else ''
                # Skip mailto links and external links for name detection
                if 'mailto:' in href:
                    continue
                if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                    if not any(x in tag_text.lower() for x in [
                        'email', 'contact', 'phone', 'http', 'department',
                        'graduate', 'student', 'people', 'faculty', 'office',
                        'website', 'personal', 'google scholar', 'academic',
                    ]):
                        # Check if this looks like a name (has scholars.duke.edu link or short text)
                        if 'scholars.duke.edu' in href or len(tag_text.split()) <= 6:
                            name = tag_text
                            break

            # Skip if this looks like a faculty/staff entry
            if is_likely_faculty_title(text) and 'student' not in text.lower():
                continue

            seen_emails.add(email)
            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': url,
            })

    # ----- Strategy B: Extract from card/grid/div-based layouts -----
    person_selectors = [
        '.views-row',
        '[class*="person"]',
        '[class*="profile"]',
        '[class*="people"]',
        '[class*="member"]',
        '[class*="student"]',
        '[class*="card"]',
        '[class*="directory"]',
        'article',
        '.node--type-person',
        'tr',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails_in_card = extract_duke_emails(text)
                mailto_in_card = extract_mailto_emails(card, 'duke.edu')
                all_emails = list(set(emails_in_card + mailto_in_card))

                for email in all_emails:
                    if email in seen_emails or is_admin_email(email):
                        continue
                    seen_emails.add(email)

                    name = ""
                    for tag in card.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a']):
                        tag_text = tag.get_text(strip=True)
                        href = tag.get('href', '') if tag.name == 'a' else ''
                        if 'mailto:' in href:
                            continue
                        if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                            if not any(x in tag_text.lower() for x in [
                                'email', 'contact', 'phone', 'http', 'department',
                                'graduate', 'student', 'people', 'faculty', 'office',
                                'read more', 'view profile', 'website',
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
# PROFILE LINK SCRAPING (for pages that don't list emails directly)
# ============================================================

def find_profile_links(soup, base_url):
    """Find links to individual profile pages."""
    profiles = []
    seen_urls = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)

        if full_url in seen_urls:
            continue
        if '#' in href and not href.startswith('http'):
            continue

        # Match scholars.duke.edu profiles or dept profile URLs
        if ('scholars.duke.edu/person/' in full_url or
                re.search(r'/people/[a-z][\w-]+$', full_url, re.IGNORECASE)):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'primary', 'secondary',
                    'emerit', 'postdoc', 'staff',
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session):
    """Scrape an individual profile page for duke.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup, 'duke.edu')
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_duke_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


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
            continue

        all_pages_scraped.add(url)
        if final_url:
            all_pages_scraped.add(final_url)

        page_text = soup.get_text(separator=' ', strip=True)
        effective_url = final_url or url

        # Check if page has duke.edu content
        duke_emails_on_page = extract_duke_emails(page_text)
        if not duke_emails_on_page:
            log(f"    -> No @duke.edu emails found on page, trying next URL")
            time.sleep(0.5)
            continue

        log(f"    -> Page loaded ({len(duke_emails_on_page)} raw emails detected)")

        # Strategy 1: Extract from structured listing
        listing_results = extract_from_duke_listing(soup, effective_url, department)
        for r in listing_results:
            if r['email'] not in seen_emails and r['email'] not in global_seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Strategy 2: Extract all emails from full page text
        text_emails = extract_duke_emails(page_text)
        mailto_emails = extract_mailto_emails(soup, 'duke.edu')
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

        # Strategy 3: Check for obfuscated emails
        obfuscated = re.findall(
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*duke\.edu)',
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

        # Strategy 4: Check JavaScript for emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_duke_emails(script.string)
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
                pg_text_emails = extract_duke_emails(pg_text)
                pg_mailto = extract_mailto_emails(page_soup, 'duke.edu')
                pg_all = list(set(pg_text_emails + pg_mailto))

                pg_listing = extract_from_duke_listing(page_soup, page_url, department)
                for r in pg_listing:
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

        # Strategy 6: If few emails, try profile links
        if len(results) < 3:
            profiles = find_profile_links(soup, effective_url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting for emails...")
                for i, profile in enumerate(profiles[:50]):
                    pname = profile['name']
                    purl = profile['profile_url']
                    email = scrape_profile_page(purl, session)
                    if email and email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': pname,
                            'department': department,
                            'source_url': purl,
                        })
                    time.sleep(0.3)

        if results:
            log(f"    -> Found {len(results)} emails so far")

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    for r in results[:10]:
        log(f"    {r['email']:<40} | {r['name']}")
    if len(results) > 10:
        log(f"    ... and {len(results) - 10} more")

    return results


# ============================================================
# ATHLETICS SCRAPER (GoDuke.com specific)
# ============================================================

def scrape_athletics(session, global_seen_emails):
    """Scrape Duke Athletics staff directory from goduke.com."""
    results = []
    seen_emails = set()
    department = "Duke Athletics"

    log(f"\n{'=' * 60}")
    log(f"Scraping: Duke Athletics (goduke.com)")
    log(f"{'=' * 60}")

    url = "https://goduke.com/staff-directory"
    soup, final_url = get_soup(url, session)

    if soup is None:
        log("  -> Failed to load athletics page")
        return results

    page_text = soup.get_text(separator=' ', strip=True)

    # Extract all duke.edu emails
    text_emails = extract_duke_emails(page_text)
    mailto_emails = extract_mailto_emails(soup, 'duke.edu')
    all_emails = list(set(text_emails + mailto_emails))

    log(f"  -> Found {len(all_emails)} raw duke.edu emails on athletics page")

    # Try to associate names with emails from the staff directory table structure
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*duke\.edu)', href, re.IGNORECASE)
            if match:
                email = match.group(1).lower().strip()
                if email in seen_emails or email in global_seen_emails or is_admin_email(email):
                    continue

                # Try to find name - look for staff-directory link nearby
                name = ""
                parent = a_tag.parent
                for _ in range(5):
                    if parent is None:
                        break
                    # Look for staff directory links (name links)
                    for link in parent.find_all('a', href=True):
                        if 'staff-directory/' in link.get('href', '') and link.get_text(strip=True):
                            name_text = link.get_text(strip=True)
                            if '@' not in name_text and len(name_text) > 2:
                                name = name_text
                                break
                    if name:
                        break
                    parent = parent.parent

                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': url,
                })

    # Catch any remaining emails
    for email in all_emails:
        if email not in seen_emails and email not in global_seen_emails and not is_admin_email(email):
            seen_emails.add(email)
            name = try_get_name_for_email(soup, email)
            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': url,
            })

    log(f"  TOTAL for Athletics: {len(results)} emails")
    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_results = []
    global_seen_emails = set()

    log("=" * 70)
    log("DUKE UNIVERSITY EMAIL SCRAPER")
    log("=" * 70)

    all_dept_configs = []
    all_dept_configs.extend(ARTS_SCIENCES_DEPTS)
    all_dept_configs.extend(ENGINEERING_DEPTS)
    all_dept_configs.extend(PROFESSIONAL_SCHOOLS)
    all_dept_configs.extend(RESEARCH_CENTERS)
    all_dept_configs.extend(SUPPLEMENTAL_PAGES)

    log(f"Scraping {len(all_dept_configs)} department configurations + athletics...")

    # ---- Phase 1: Department pages ----
    log("\n\nPHASE 1: DEPARTMENT PEOPLE PAGES")
    log("=" * 70)

    for config in all_dept_configs:
        try:
            dept_results = scrape_department(config, session, global_seen_emails)
            for r in dept_results:
                email = r['email'].lower().strip()
                if email and email not in global_seen_emails:
                    global_seen_emails.add(email)
                    all_results.append(r)
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    log(f"\nAfter Phase 1: {len(all_results)} unique emails")

    # ---- Phase 2: Athletics ----
    log("\n\nPHASE 2: ATHLETICS")
    log("=" * 70)

    try:
        athletics_results = scrape_athletics(session, global_seen_emails)
        for r in athletics_results:
            email = r['email'].lower().strip()
            if email and email not in global_seen_emails:
                global_seen_emails.add(email)
                all_results.append(r)
    except Exception as e:
        log(f"  ERROR scraping athletics: {e}")

    log(f"\nAfter Phase 2: {len(all_results)} unique emails")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @duke.edu emails: {len(all_results)}")

    # Save CSV
    output_csv = 'duke_dept_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: (x['department'], x['email'])):
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")

    # Save JSON
    output_json = 'duke_dept_emails.json'
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

    depts_with_zero = [c['department'] for c in all_dept_configs if c['department'] not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in depts_with_zero:
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
