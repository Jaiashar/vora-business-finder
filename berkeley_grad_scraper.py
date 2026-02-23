#!/usr/bin/env python3
"""
UC Berkeley Graduate Student Email Scraper
Scrapes @berkeley.edu emails from department people/graduate-student pages.
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
# Each department has a list of URLs to try (primary + fallbacks)
# ============================================================

DEPARTMENTS = [
    # --- Social Sciences & Humanities ---
    {
        "department": "Economics",
        "urls": [
            "https://www.econ.berkeley.edu/people/graduate-students",
            "https://www.econ.berkeley.edu/grad/current-students",
            "https://econ.berkeley.edu/people/graduate-students",
            "https://econ.berkeley.edu/grad/current-students",
            "https://www.econ.berkeley.edu/people/students",
            "https://www.econ.berkeley.edu/people/phd-students",
            "https://www.econ.berkeley.edu/people",
        ],
    },
    {
        "department": "Political Science",
        "urls": [
            "https://polisci.berkeley.edu/people/graduate-students",
            "https://www.polisci.berkeley.edu/people/graduate-students",
            "https://polisci.berkeley.edu/people/students",
            "https://polisci.berkeley.edu/people/phd-students",
            "https://polisci.berkeley.edu/people",
        ],
    },
    {
        "department": "Sociology",
        "urls": [
            "https://sociology.berkeley.edu/people/graduate-students",
            "https://www.sociology.berkeley.edu/people/graduate-students",
            "https://sociology.berkeley.edu/people/students",
            "https://sociology.berkeley.edu/people/phd-students",
            "https://sociology.berkeley.edu/people",
        ],
    },
    {
        "department": "Psychology",
        "urls": [
            "https://psychology.berkeley.edu/people/graduate-students",
            "https://www.psychology.berkeley.edu/people/graduate-students",
            "https://psychology.berkeley.edu/people/students",
            "https://psychology.berkeley.edu/people/phd-students",
            "https://psychology.berkeley.edu/people",
        ],
    },
    {
        "department": "History",
        "urls": [
            "https://history.berkeley.edu/people/graduate-students",
            "https://www.history.berkeley.edu/people/graduate-students",
            "https://history.berkeley.edu/people/students",
            "https://history.berkeley.edu/people/phd-students",
            "https://history.berkeley.edu/people",
        ],
    },
    {
        "department": "English",
        "urls": [
            "https://english.berkeley.edu/people/graduate-students",
            "https://www.english.berkeley.edu/people/graduate-students",
            "https://english.berkeley.edu/people/students",
            "https://english.berkeley.edu/people/phd-students",
            "https://english.berkeley.edu/people",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.berkeley.edu/people/graduate-students",
            "https://www.philosophy.berkeley.edu/people/graduate-students",
            "https://philosophy.berkeley.edu/people/students",
            "https://philosophy.berkeley.edu/people/phd-students",
            "https://philosophy.berkeley.edu/people",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://linguistics.berkeley.edu/people/graduate-students",
            "https://www.linguistics.berkeley.edu/people/graduate-students",
            "https://lx.berkeley.edu/people/graduate-students",
            "https://linguistics.berkeley.edu/people/students",
            "https://linguistics.berkeley.edu/people/phd-students",
            "https://linguistics.berkeley.edu/people",
        ],
    },
    {
        "department": "Rhetoric",
        "urls": [
            "https://rhetoric.berkeley.edu/people/graduate-students",
            "https://www.rhetoric.berkeley.edu/people/graduate-students",
            "https://rhetoric.berkeley.edu/people/students",
            "https://rhetoric.berkeley.edu/people/phd-students",
            "https://rhetoric.berkeley.edu/people",
        ],
    },
    {
        "department": "Comparative Literature",
        "urls": [
            "https://complit.berkeley.edu/people/graduate-students",
            "https://www.complit.berkeley.edu/people/graduate-students",
            "https://complit.berkeley.edu/people/students",
            "https://complit.berkeley.edu/people/phd-students",
            "https://complit.berkeley.edu/people",
        ],
    },
    {
        "department": "Music",
        "urls": [
            "https://music.berkeley.edu/people/graduate-students",
            "https://www.music.berkeley.edu/people/graduate-students",
            "https://music.berkeley.edu/people/students",
            "https://music.berkeley.edu/people/phd-students",
            "https://music.berkeley.edu/people",
        ],
    },
    {
        "department": "Art History",
        "urls": [
            "https://arthistory.berkeley.edu/people/graduate-students",
            "https://www.arthistory.berkeley.edu/people/graduate-students",
            "https://arthistory.berkeley.edu/people/students",
            "https://arthistory.berkeley.edu/people/phd-students",
            "https://arthistory.berkeley.edu/people",
        ],
    },

    # --- STEM: Math / Stats / Physics / Chemistry ---
    {
        "department": "Mathematics",
        "urls": [
            "https://math.berkeley.edu/people/grad-students",
            "https://math.berkeley.edu/people/graduate-students",
            "https://www.math.berkeley.edu/people/grad-students",
            "https://math.berkeley.edu/people/students",
            "https://math.berkeley.edu/people",
        ],
    },
    {
        "department": "Statistics",
        "urls": [
            "https://statistics.berkeley.edu/people/graduate-students",
            "https://www.statistics.berkeley.edu/people/graduate-students",
            "https://statistics.berkeley.edu/people/students",
            "https://statistics.berkeley.edu/people/phd-students",
            "https://statistics.berkeley.edu/people",
            "https://stat.berkeley.edu/people/graduate-students",
        ],
    },
    {
        "department": "Physics",
        "urls": [
            "https://physics.berkeley.edu/people/graduate-students",
            "https://www.physics.berkeley.edu/people/graduate-students",
            "https://physics.berkeley.edu/people/students",
            "https://physics.berkeley.edu/people/phd-students",
            "https://physics.berkeley.edu/people",
        ],
    },
    {
        "department": "Chemistry",
        "urls": [
            "https://chemistry.berkeley.edu/people/graduate-students",
            "https://www.chemistry.berkeley.edu/people/graduate-students",
            "https://chemistry.berkeley.edu/people/students",
            "https://chemistry.berkeley.edu/people/phd-students",
            "https://chemistry.berkeley.edu/people",
        ],
    },

    # --- Life & Earth Sciences ---
    {
        "department": "Earth & Planetary Science",
        "urls": [
            "https://eps.berkeley.edu/people/graduate-students",
            "https://www.eps.berkeley.edu/people/graduate-students",
            "https://eps.berkeley.edu/people/students",
            "https://eps.berkeley.edu/people/phd-students",
            "https://eps.berkeley.edu/people",
        ],
    },
    {
        "department": "Molecular & Cell Biology",
        "urls": [
            "https://mcb.berkeley.edu/people/graduate-students",
            "https://www.mcb.berkeley.edu/people/graduate-students",
            "https://mcb.berkeley.edu/people/students",
            "https://mcb.berkeley.edu/people/phd-students",
            "https://mcb.berkeley.edu/people",
        ],
    },
    {
        "department": "Integrative Biology",
        "urls": [
            "https://ib.berkeley.edu/people/graduate-students",
            "https://www.ib.berkeley.edu/people/graduate-students",
            "https://ib.berkeley.edu/people/students",
            "https://ib.berkeley.edu/people/phd-students",
            "https://ib.berkeley.edu/people",
        ],
    },
    {
        "department": "Agricultural & Resource Economics",
        "urls": [
            "https://are.berkeley.edu/people/graduate-students",
            "https://www.are.berkeley.edu/people/graduate-students",
            "https://are.berkeley.edu/people/students",
            "https://are.berkeley.edu/people/phd-students",
            "https://are.berkeley.edu/people",
        ],
    },

    # --- Engineering ---
    {
        "department": "EECS (Electrical Engineering & Computer Science)",
        "urls": [
            "https://eecs.berkeley.edu/people/graduate-students/",
            "https://www2.eecs.berkeley.edu/Pubs/Grads/",
            "https://eecs.berkeley.edu/people/students",
            "https://www2.eecs.berkeley.edu/Grads/",
            "https://eecs.berkeley.edu/people",
        ],
    },
    {
        "department": "Mechanical Engineering",
        "urls": [
            "https://me.berkeley.edu/people/graduate-students",
            "https://www.me.berkeley.edu/people/graduate-students",
            "https://me.berkeley.edu/people/students",
            "https://me.berkeley.edu/people/phd-students",
            "https://me.berkeley.edu/people",
        ],
    },
    {
        "department": "Civil & Environmental Engineering",
        "urls": [
            "https://ce.berkeley.edu/people/graduate-students",
            "https://www.ce.berkeley.edu/people/graduate-students",
            "https://ce.berkeley.edu/people/students",
            "https://ce.berkeley.edu/people/phd-students",
            "https://ce.berkeley.edu/people",
        ],
    },
    {
        "department": "Materials Science & Engineering",
        "urls": [
            "https://mse.berkeley.edu/people/graduate-students",
            "https://www.mse.berkeley.edu/people/graduate-students",
            "https://mse.berkeley.edu/people/students",
            "https://mse.berkeley.edu/people/phd-students",
            "https://mse.berkeley.edu/people",
        ],
    },
    {
        "department": "Bioengineering",
        "urls": [
            "https://bioeng.berkeley.edu/people/graduate-students",
            "https://www.bioeng.berkeley.edu/people/graduate-students",
            "https://bioeng.berkeley.edu/people/students",
            "https://bioeng.berkeley.edu/people/phd-students",
            "https://bioeng.berkeley.edu/people",
        ],
    },
    {
        "department": "Industrial Engineering & Operations Research",
        "urls": [
            "https://ieor.berkeley.edu/people/graduate-students",
            "https://www.ieor.berkeley.edu/people/graduate-students",
            "https://ieor.berkeley.edu/people/students",
            "https://ieor.berkeley.edu/people/phd-students",
            "https://ieor.berkeley.edu/people",
        ],
    },
    {
        "department": "Nuclear Engineering",
        "urls": [
            "https://nuc.berkeley.edu/people/graduate-students",
            "https://www.nuc.berkeley.edu/people/graduate-students",
            "https://nuc.berkeley.edu/people/students",
            "https://nuc.berkeley.edu/people/phd-students",
            "https://nuc.berkeley.edu/people",
        ],
    },
    {
        "department": "Chemical & Biomolecular Engineering",
        "urls": [
            "https://chemistry.berkeley.edu/cbe/people",
            "https://chemistry.berkeley.edu/cbe/people/graduate-students",
            "https://cbe.berkeley.edu/people/graduate-students",
            "https://cbe.berkeley.edu/people",
        ],
    },
]


# ============================================================
# EMAIL EXTRACTION
# ============================================================

def extract_berkeley_emails(text):
    """Extract all @berkeley.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*berkeley\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup, domain='berkeley.edu'):
    """Extract berkeley.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*berkeley\.edu)',
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
    """Extract people from card/grid-based layouts (common in Drupal/Open Berkeley)."""
    results = []
    seen_emails = set()

    # Selectors common in Berkeley department sites (Open Berkeley / Drupal)
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
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_berkeley_emails(text)
                mailto_emails = extract_mailto_emails(card, 'berkeley.edu')
                all_emails = list(set(emails + mailto_emails))

                for email in all_emails:
                    if email in seen_emails or is_admin_email(email):
                        continue
                    seen_emails.add(email)

                    name = ""
                    # Try headings within the card
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

    # Typical Berkeley profile URL patterns
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)

        # Skip if already seen or is an anchor/external link
        if full_url in seen_urls:
            continue
        if '#' in href and not href.startswith('http'):
            continue

        # Match profile-like URLs
        # e.g., /people/john-doe, /person/john-doe, /grad/john-doe
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
    """Scrape an individual profile page for berkeley.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    # Try mailto links first
    emails = extract_mailto_emails(soup, 'berkeley.edu')

    # Try text extraction
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_berkeley_emails(text)

    all_emails = list(set(emails + text_emails))

    # Filter admin emails
    personal = [e for e in all_emails if not is_admin_email(e)]

    return personal[0] if personal else None


# ============================================================
# PAGINATION HANDLING
# ============================================================

def find_pagination_urls(soup, base_url):
    """Find paginated page URLs (common: ?page=1, ?page=2, etc.)."""
    pages = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)
        # Match pagination patterns
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
            'doctoral student', '@berkeley.edu', 'email'
        ])

        if not has_people and len(extract_berkeley_emails(page_text)) == 0:
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
        text_emails = extract_berkeley_emails(page_text)
        mailto_emails = extract_mailto_emails(soup, 'berkeley.edu')
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
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*berkeley\.edu)',
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
                script_emails = extract_berkeley_emails(script.string)
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

                # Extract from paginated page
                pg_text = page_soup.get_text(separator=' ', strip=True)
                pg_text_emails = extract_berkeley_emails(pg_text)
                pg_mailto = extract_mailto_emails(page_soup, 'berkeley.edu')
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
                for i, profile in enumerate(profiles[:50]):  # Cap at 50 profiles
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

        # If we found emails, no need to try more URL variants
        if len(results) > 0:
            log(f"    -> Found {len(results)} emails so far, continuing to check fallback URLs for more")
            # But still continue to other URLs to get more results

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    for r in results:
        log(f"    {r['email']:<40} | {r['name']}")

    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_results = []
    global_seen_emails = set()

    log("=" * 70)
    log("UC BERKELEY GRADUATE STUDENT EMAIL SCRAPER")
    log("=" * 70)
    log(f"Scraping {len(DEPARTMENTS)} departments...")

    for config in DEPARTMENTS:
        try:
            dept_results = scrape_department(config, session)
            for r in dept_results:
                email = r['email'].lower().strip()
                if email and email not in global_seen_emails:
                    global_seen_emails.add(email)
                    all_results.append(r)
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")
            continue

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @berkeley.edu emails: {len(all_results)}")

    # Save CSV
    output_csv = 'berkeley_dept_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")

    # Save JSON
    output_json = 'berkeley_dept_emails.json'
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

    depts_with_zero = [c['department'] for c in DEPARTMENTS if c['department'] not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in depts_with_zero:
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
