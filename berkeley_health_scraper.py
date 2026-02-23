#!/usr/bin/env python3
"""
UC Berkeley Health Sciences, STEM Labs, and Remaining Departments Email Scraper
Scrapes @berkeley.edu emails from department people pages, research labs, and directories.
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

DEPARTMENTS = [
    # --- Health & Life Sciences ---
    {
        "department": "Public Health",
        "urls": [
            "https://publichealth.berkeley.edu/people/",
            "https://publichealth.berkeley.edu/people",
            "https://publichealth.berkeley.edu/people/faculty",
            "https://publichealth.berkeley.edu/people/students",
            "https://publichealth.berkeley.edu/people/staff",
            "https://publichealth.berkeley.edu/people/researchers",
            "https://publichealth.berkeley.edu/people/?page=1",
            "https://publichealth.berkeley.edu/people/?page=2",
            "https://publichealth.berkeley.edu/people/?page=3",
        ],
    },
    {
        "department": "School of Public Health",
        "urls": [
            "https://sph.berkeley.edu/people/",
            "https://sph.berkeley.edu/people",
            "https://sph.berkeley.edu/people/faculty",
            "https://sph.berkeley.edu/people/students",
            "https://sph.berkeley.edu/people/staff",
            "https://sph.berkeley.edu/directory",
            "https://sph.berkeley.edu/people/doctoral-students",
        ],
    },
    {
        "department": "QB3 Institute (Quantitative Biosciences)",
        "urls": [
            "https://qb3.berkeley.edu/people/",
            "https://qb3.berkeley.edu/people",
            "https://qb3.berkeley.edu/people/faculty",
            "https://qb3.berkeley.edu/people/staff",
            "https://qb3.berkeley.edu/people/researchers",
            "https://qb3.berkeley.edu/about/people/",
            "https://qb3.berkeley.edu/directory/",
        ],
    },
    {
        "department": "Innovative Genomics Institute",
        "urls": [
            "https://igb.berkeley.edu/people/",
            "https://igb.berkeley.edu/people",
            "https://innovativegenomics.org/people/",
            "https://innovativegenomics.org/people",
            "https://innovativegenomics.org/team/",
            "https://innovativegenomics.org/about/people/",
        ],
    },
    {
        "department": "Neuroscience (Barker Hall)",
        "urls": [
            "https://bnl.berkeley.edu/people/",
            "https://bnl.berkeley.edu/people",
            "https://neuroscience.berkeley.edu/people/",
            "https://neuroscience.berkeley.edu/people",
            "https://neuroscience.berkeley.edu/people/graduate-students",
            "https://neuroscience.berkeley.edu/people/faculty",
            "https://neuroscience.berkeley.edu/people/postdocs",
        ],
    },

    # --- STEM Research Labs ---
    {
        "department": "ML@Berkeley",
        "urls": [
            "https://ml.berkeley.edu/people/",
            "https://ml.berkeley.edu/people",
            "https://ml.berkeley.edu/about/",
            "https://ml.berkeley.edu/about",
            "https://ml.berkeley.edu/team/",
            "https://ml.berkeley.edu/members/",
        ],
    },
    {
        "department": "Berkeley Deep Drive (BDD)",
        "urls": [
            "https://bdd.berkeley.edu/people.html",
            "https://bdd.berkeley.edu/people",
            "https://bdd.berkeley.edu/members.html",
            "https://bdd.berkeley.edu/team.html",
            "https://bdd.berkeley.edu/",
        ],
    },
    {
        "department": "Sky Computing Lab",
        "urls": [
            "https://sky.cs.berkeley.edu/people/",
            "https://sky.cs.berkeley.edu/people",
            "https://sky.cs.berkeley.edu/",
            "https://sky.cs.berkeley.edu/members/",
            "https://sky.cs.berkeley.edu/team/",
        ],
    },
    {
        "department": "EECS People Directory",
        "urls": [
            "https://people.eecs.berkeley.edu/",
            "https://people.eecs.berkeley.edu",
            "https://www2.eecs.berkeley.edu/Faculty/Lists/list.html",
        ],
    },
    {
        "department": "Nanolab",
        "urls": [
            "https://nanolab.berkeley.edu/people/",
            "https://nanolab.berkeley.edu/people",
            "https://nanolab.berkeley.edu/",
            "https://nanolab.berkeley.edu/members/",
            "https://nanolab.berkeley.edu/team/",
        ],
    },
    {
        "department": "Biomechanics",
        "urls": [
            "https://biomechanics.berkeley.edu/people/",
            "https://biomechanics.berkeley.edu/people",
            "https://biomechanics.berkeley.edu/",
            "https://biomechanics.berkeley.edu/members/",
            "https://biomechanics.berkeley.edu/team/",
        ],
    },

    # --- Remaining Departments ---
    {
        "department": "Demography",
        "urls": [
            "https://demography.berkeley.edu/people/",
            "https://demography.berkeley.edu/people",
            "https://demography.berkeley.edu/people/faculty",
            "https://demography.berkeley.edu/people/graduate-students",
            "https://demography.berkeley.edu/people/students",
            "https://demography.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "Geography",
        "urls": [
            "https://geography.berkeley.edu/people/",
            "https://geography.berkeley.edu/people",
            "https://geography.berkeley.edu/people/faculty",
            "https://geography.berkeley.edu/people/graduate-students",
            "https://geography.berkeley.edu/people/students",
            "https://geography.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "Italian Studies",
        "urls": [
            "https://iastp.berkeley.edu/people/",
            "https://iastp.berkeley.edu/people",
            "https://italian.berkeley.edu/people/",
            "https://italian.berkeley.edu/people",
            "https://iastp.berkeley.edu/people/faculty",
            "https://iastp.berkeley.edu/people/graduate-students",
        ],
    },
    {
        "department": "German",
        "urls": [
            "https://german.berkeley.edu/people/",
            "https://german.berkeley.edu/people",
            "https://german.berkeley.edu/people/faculty",
            "https://german.berkeley.edu/people/graduate-students",
            "https://german.berkeley.edu/people/students",
            "https://german.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "French",
        "urls": [
            "https://french.berkeley.edu/people/",
            "https://french.berkeley.edu/people",
            "https://french.berkeley.edu/people/faculty",
            "https://french.berkeley.edu/people/graduate-students",
            "https://french.berkeley.edu/people/students",
            "https://french.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "South & Southeast Asian Studies",
        "urls": [
            "https://sseas.berkeley.edu/people/",
            "https://sseas.berkeley.edu/people",
            "https://sseas.berkeley.edu/people/faculty",
            "https://sseas.berkeley.edu/people/graduate-students",
            "https://sseas.berkeley.edu/people/students",
            "https://sseas.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "East Asian Languages & Cultures",
        "urls": [
            "https://ealc.berkeley.edu/people/",
            "https://ealc.berkeley.edu/people",
            "https://ealc.berkeley.edu/people/faculty",
            "https://ealc.berkeley.edu/people/graduate-students",
            "https://ealc.berkeley.edu/people/students",
            "https://ealc.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "Scandinavian",
        "urls": [
            "https://scandinavian.berkeley.edu/people/",
            "https://scandinavian.berkeley.edu/people",
            "https://scandinavian.berkeley.edu/people/faculty",
            "https://scandinavian.berkeley.edu/people/graduate-students",
            "https://scandinavian.berkeley.edu/people/students",
            "https://scandinavian.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "Near Eastern Studies",
        "urls": [
            "https://neareastern.berkeley.edu/people/",
            "https://neareastern.berkeley.edu/people",
            "https://neareastern.berkeley.edu/people/faculty",
            "https://neareastern.berkeley.edu/people/graduate-students",
            "https://neareastern.berkeley.edu/people/students",
            "https://neareastern.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "Slavic Languages",
        "urls": [
            "https://slavic.berkeley.edu/people/",
            "https://slavic.berkeley.edu/people",
            "https://slavic.berkeley.edu/people/faculty",
            "https://slavic.berkeley.edu/people/graduate-students",
            "https://slavic.berkeley.edu/people/students",
            "https://slavic.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "Spanish & Portuguese",
        "urls": [
            "https://spanish-portuguese.berkeley.edu/people/",
            "https://spanish-portuguese.berkeley.edu/people",
            "https://spanish-portuguese.berkeley.edu/people/faculty",
            "https://spanish-portuguese.berkeley.edu/people/graduate-students",
            "https://spanish-portuguese.berkeley.edu/people/students",
            "https://spanish-portuguese.berkeley.edu/people/staff",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.berkeley.edu/people/",
            "https://philosophy.berkeley.edu/people",
            "https://philosophy.berkeley.edu/people/faculty",
            "https://philosophy.berkeley.edu/people/graduate-students",
            "https://philosophy.berkeley.edu/people/students",
            "https://philosophy.berkeley.edu/people/staff",
            "https://philosophy.berkeley.edu/people/emeriti",
        ],
    },
    {
        "department": "Anthropology",
        "urls": [
            "https://anthropology.berkeley.edu/people/",
            "https://anthropology.berkeley.edu/people",
            "https://anthropology.berkeley.edu/people/faculty",
            "https://anthropology.berkeley.edu/people/graduate-students",
            "https://anthropology.berkeley.edu/people/students",
            "https://anthropology.berkeley.edu/people/staff",
        ],
    },

    # --- Additional CS/EECS labs from search queries ---
    {
        "department": "BAIR (Berkeley AI Research)",
        "urls": [
            "https://bair.berkeley.edu/students.html",
            "https://bair.berkeley.edu/faculty.html",
            "https://bair.berkeley.edu/members.html",
            "https://bair.berkeley.edu/people.html",
            "https://bair.berkeley.edu/",
        ],
    },
    {
        "department": "Berkeley RISE Lab",
        "urls": [
            "https://rise.cs.berkeley.edu/people/",
            "https://rise.cs.berkeley.edu/people",
            "https://rise.cs.berkeley.edu/",
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


def extract_mailto_emails(soup):
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
        'ugrad@', 'econ@', 'polisci@', 'bef@', 'physics@', 'chem@',
        'sph@', 'publichealth@', 'anthro@', 'anthdesk@', 'philosophy@',
        'philos@', 'french@', 'german@', 'slavic@', 'ealc@',
        'scandinavian@', 'sseas@', 'neareastern@', 'spanish@',
        'demography@', 'geography@', 'italian@', 'iastp@',
        'noreply@', 'no-reply@', 'donotreply@', 'do-not-reply@',
        'postmaster@', 'root@', 'abuse@', 'spam@', 'mailer@',
        'www@', 'ftp@', 'sysadmin@',
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
                            'graduate', 'student', 'people', 'faculty', 'office',
                            'research', 'lab ', 'institute'
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
                        'view profile', 'research', 'lab ', 'institute'
                    ]):
                        return name
            parent = parent.parent

    return ""


def name_from_email(email):
    """Derive a best-guess name from an email prefix."""
    prefix = email.split('@')[0]
    if '.' in prefix:
        parts = prefix.split('.')
        return ' '.join(p.capitalize() for p in parts if p.isalpha())
    elif '_' in prefix:
        parts = prefix.split('_')
        return ' '.join(p.capitalize() for p in parts if p.isalpha())
    elif '-' in prefix:
        parts = prefix.split('-')
        return ' '.join(p.capitalize() for p in parts if p.isalpha())
    return ""


# ============================================================
# STRUCTURED EXTRACTION: Card/grid-based people listings
# ============================================================

def extract_from_person_cards(soup, url, department):
    """Extract people from card/grid-based layouts (common in Drupal/Open Berkeley)."""
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
        '[class*="team"]',
        '[class*="faculty"]',
        '[class*="researcher"]',
        '[class*="staff"]',
        '.grid-item',
        '.entry',
        '.post',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_berkeley_emails(text)
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
                                'read more', 'view profile', 'research area',
                                'more info', 'lab ', 'institute'
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

        # Match profile-like URLs
        if re.search(r'/people/[a-z][\w-]+$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'staff', 'postdoc', 'emerit',
                    'researcher', 'visitors', 'skip to'
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

        # Also match /person/ pattern (used by some departments)
        elif re.search(r'/person/[a-z][\w-]+$', full_url, re.IGNORECASE):
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


def scrape_profile_page(url, session):
    """Scrape an individual profile page for berkeley.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_berkeley_emails(text)
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

                pg_text = page_soup.get_text(separator=' ', strip=True)
                pg_text_emails = extract_berkeley_emails(pg_text)
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
        if len(results) < 5:
            profiles = find_profile_links(soup, successful_url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting for emails...")
                for i, profile in enumerate(profiles[:80]):  # Cap at 80 profiles
                    pname = profile['name']
                    purl = profile['profile_url']
                    email = scrape_profile_page(purl, session)
                    if email and email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': pname,
                            'department': department,
                            'source_url': purl,
                        })
                    if i % 20 == 0 and i > 0:
                        log(f"      [{i}/{len(profiles[:80])}] profiles processed...")
                    time.sleep(0.3)

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    for r in results[:10]:
        log(f"    {r['email']:<40} | {r['name']}")
    if len(results) > 10:
        log(f"    ... and {len(results) - 10} more")

    return results


# ============================================================
# GOOGLE SEARCH SCRAPING
# ============================================================

def google_search_for_emails(query, session, department, max_results=50):
    """Search Google and scrape result pages for berkeley.edu emails."""
    log(f"\n{'=' * 60}")
    log(f"Google search: {query}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    results = []
    seen_emails = set()

    # Use Google search
    search_url = "https://www.google.com/search"
    params = {'q': query, 'num': 20}

    try:
        resp = session.get(search_url, headers=HEADERS, params=params, timeout=15)
        if resp.status_code != 200:
            log(f"  Google search returned {resp.status_code}")
            return results

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract result URLs
        result_urls = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            # Google wraps URLs in /url?q=...
            if '/url?q=' in href:
                actual_url = href.split('/url?q=')[1].split('&')[0]
                if 'berkeley.edu' in actual_url:
                    result_urls.append(actual_url)
            elif 'berkeley.edu' in href and href.startswith('http'):
                result_urls.append(href)

        # Deduplicate
        result_urls = list(dict.fromkeys(result_urls))[:10]
        log(f"  Found {len(result_urls)} Berkeley URLs from search")

        # Also extract emails directly from search results page
        search_text = soup.get_text()
        search_emails = extract_berkeley_emails(search_text)
        for email in search_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': '',
                    'department': department,
                    'source_url': f'google:{query}',
                })

        # Visit each result URL
        for url in result_urls:
            log(f"  Visiting: {url}")
            page_soup, final_url = get_soup(url, session)
            if page_soup is None:
                continue

            page_text = page_soup.get_text(separator=' ', strip=True)
            page_emails = extract_berkeley_emails(page_text)
            page_mailto = extract_mailto_emails(page_soup)
            all_page_emails = list(set(page_emails + page_mailto))

            for email in all_page_emails:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    name = try_get_name_for_email(page_soup, email)
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': final_url or url,
                    })

            time.sleep(1)

    except Exception as e:
        log(f"  Google search error: {e}")

    log(f"  Found {len(results)} emails from search")
    return results


# ============================================================
# EECS DIRECTORY SPECIAL SCRAPER
# ============================================================

def scrape_eecs_directory(session):
    """Special scraper for people.eecs.berkeley.edu which has a unique format."""
    log(f"\n{'=' * 60}")
    log(f"EECS People Directory - Special scraper")
    log(f"{'=' * 60}")

    results = []
    seen_emails = set()

    # The EECS directory may list faculty/students with emails
    urls_to_try = [
        "https://people.eecs.berkeley.edu/",
        "https://www2.eecs.berkeley.edu/Faculty/Lists/list.html",
        "https://www2.eecs.berkeley.edu/Pubs/Grads/",
    ]

    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue

        log(f"    -> Loaded")

        # Extract emails from page
        text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_berkeley_emails(text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(text_emails + mailto_emails))

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'EECS People Directory',
                    'source_url': final_url or url,
                })

        # Look for links to individual faculty/student pages
        profile_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full = urljoin(final_url or url, href)
            name = a_tag.get_text(strip=True)

            if name and len(name) > 3 and len(name) < 80 and '@' not in name:
                # EECS home pages are typically ~username
                if re.search(r'people\.eecs\.berkeley\.edu/~[\w]+/?$', full):
                    if not any(x in name.lower() for x in [
                        'home', 'search', 'people', 'department', 'faculty',
                        'staff', 'about', 'contact', 'eecs', 'back',
                    ]):
                        profile_links.append({'name': name, 'url': full})

        if profile_links:
            log(f"    -> Found {len(profile_links)} EECS home pages, visiting up to 100...")
            for i, prof in enumerate(profile_links[:100]):
                psoup, _ = get_soup(prof['url'], session)
                if psoup is None:
                    continue
                pmailto = extract_mailto_emails(psoup)
                ptext = extract_berkeley_emails(psoup.get_text())
                pemails = list(set(pmailto + ptext))
                ppersonal = [e for e in pemails if not is_admin_email(e)]
                if ppersonal:
                    for em in ppersonal:
                        if em not in seen_emails:
                            seen_emails.add(em)
                            results.append({
                                'email': em,
                                'name': prof['name'],
                                'department': 'EECS People Directory',
                                'source_url': prof['url'],
                            })
                if i % 20 == 0 and i > 0:
                    log(f"      [{i}/{len(profile_links[:100])}] profiles processed...")
                time.sleep(0.3)

        time.sleep(0.5)

    log(f"  TOTAL EECS Directory: {len(results)} emails")
    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_results = []
    global_seen_emails = set()

    log("=" * 70)
    log("UC BERKELEY HEALTH SCIENCES, STEM LABS & REMAINING DEPTS SCRAPER")
    log("=" * 70)
    log(f"Scraping {len(DEPARTMENTS)} department configurations + Google searches...")

    # --- Phase 1: Scrape all department URLs ---
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

    # --- Phase 2: Google search queries ---
    search_queries = [
        ('site:publichealth.berkeley.edu email @berkeley.edu', 'Public Health (Search)'),
        ('site:cs.berkeley.edu "members" OR "people" email @berkeley.edu', 'CS Labs (Search)'),
        ('UC Berkeley machine learning lab members email @berkeley.edu', 'ML Labs (Search)'),
        ('site:berkeley.edu "postdoc" OR "phd student" email @berkeley.edu lab', 'Berkeley Labs (Search)'),
        ('site:eecs.berkeley.edu "graduate student" email @berkeley.edu', 'EECS (Search)'),
        ('site:berkeley.edu nanolab people email @berkeley.edu', 'Nanolab (Search)'),
        ('site:berkeley.edu biomechanics lab people @berkeley.edu', 'Biomechanics (Search)'),
    ]

    for query, dept in search_queries:
        try:
            search_results = google_search_for_emails(query, session, dept)
            for r in search_results:
                email = r['email'].lower().strip()
                if email and email not in global_seen_emails:
                    global_seen_emails.add(email)
                    all_results.append(r)
            time.sleep(3)  # Be very polite with Google
        except Exception as e:
            log(f"  ERROR with search '{query}': {e}")
            continue

    # --- Phase 3: Special EECS directory scraper ---
    try:
        eecs_results = scrape_eecs_directory(session)
        for r in eecs_results:
            email = r['email'].lower().strip()
            if email and email not in global_seen_emails:
                global_seen_emails.add(email)
                all_results.append(r)
    except Exception as e:
        log(f"  ERROR with EECS directory: {e}")

    # --- Clean up names ---
    for r in all_results:
        if not r['name'] or r['name'].strip() == '':
            r['name'] = name_from_email(r['email'])

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @berkeley.edu emails: {len(all_results)}")

    # Save CSV
    output_csv = '/Users/jaiashar/Documents/VoraBusinessFinder/berkeley_health_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")

    # Save JSON too
    output_json = '/Users/jaiashar/Documents/VoraBusinessFinder/berkeley_health_emails.json'
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

    log(f"\n  GRAND TOTAL: {len(all_results)}")

    return all_results


if __name__ == '__main__':
    main()
