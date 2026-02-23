#!/usr/bin/env python3
"""
University of Michigan Graduate Student Email Scraper
Scrapes @umich.edu emails from LSA department pages, Engineering department pages,
and research lab/people pages.

Strategy:
- LSA departments: cloudscraper + Cloudflare email decoding (.directory.html pages)
- Engineering MSE: Regular requests (Plone-based, no CF)
- Engineering CSE/ECE: cloudscraper with mailto extraction
- Other engineering: Try multiple approaches
"""

import cloudscraper
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
# CLOUDFLARE EMAIL DECODING
# ============================================================

def decode_cf_email(encoded):
    """Decode Cloudflare email protection hex string."""
    try:
        key = int(encoded[:2], 16)
        email = ''
        for n in range(2, len(encoded), 2):
            i = int(encoded[n:n+2], 16) ^ key
            email += chr(i)
        return email.lower().strip()
    except Exception:
        return ''


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_umich_emails(text):
    """Extract all @umich.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*umich\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup, domain='umich.edu'):
    """Extract umich.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*umich\.edu)',
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
        'lsa-', 'lsaadvising@', 'lsa@', 'engin@', 'eecs@',
        'admissions@', 'gradcoord@', 'uofmphysics@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


# ============================================================
# LSA DEPARTMENTS (Cloudflare-protected, .directory.html pages)
# ============================================================

LSA_DEPARTMENTS = [
    {"department": "Economics", "path": "econ", "slug": "phd-students"},
    {"department": "Political Science", "path": "polisci", "slug": "graduate-students"},
    {"department": "Sociology", "path": "soc", "slug": "graduate-students"},
    {"department": "Psychology", "path": "psych", "slug": "graduate-students"},
    {"department": "History", "path": "history", "slug": "graduate-students"},
    {"department": "English", "path": "english", "slug": "graduate-students"},
    {"department": "Philosophy", "path": "philosophy", "slug": "graduate-students"},
    {"department": "Linguistics", "path": "linguistics", "slug": "graduate-students"},
    {"department": "Mathematics", "path": "math", "slug": "phd-students"},
    {"department": "Statistics", "path": "stats", "slug": "phd-students"},
    {"department": "Physics", "path": "physics", "slug": "graduate-students"},
    {"department": "Chemistry", "path": "chem", "slug": "graduate-students"},
    {"department": "Earth & Environmental Sciences", "path": "earth", "slug": "graduate-students"},
    {"department": "Ecology & Evolutionary Biology", "path": "biology", "slug": "graduate-students"},
    {"department": "Molecular, Cellular & Developmental Biology", "path": "mcdb", "slug": "graduate-students"},
    {"department": "Anthropology", "path": "anthro", "slug": "graduate-students"},
    {"department": "Communication & Media", "path": "comm", "slug": "graduate-students"},
    {"department": "Classics", "path": "classics", "slug": "graduate-students"},
    {"department": "Asian Languages & Cultures", "path": "asian", "slug": "graduate-students"},
    {"department": "German Studies", "path": "german", "slug": "graduate-students"},
    {"department": "Romance Languages & Literatures", "path": "romance", "slug": "graduate-students"},
    {"department": "Slavic Languages & Literatures", "path": "slavic", "slug": "graduate-students"},
    {"department": "Afroamerican & African Studies", "path": "afroam", "slug": "graduate-students"},
    {"department": "Women's & Gender Studies", "path": "women", "slug": "graduate-students"},
]


def scrape_lsa_department(config, scraper):
    """
    Scrape an LSA department using the .directory.html page.
    Uses cloudscraper to bypass Cloudflare, then decodes CF-protected emails.
    """
    department = config['department']
    path = config['path']
    slug = config['slug']
    results = []
    seen_emails = set()

    # Try the .directory.html variant (which shows all students in one page)
    url = f"https://lsa.umich.edu/{path}/people/{slug}.directory.html"
    log(f"  Fetching: {url}")

    try:
        r = scraper.get(url, timeout=30)
        if r.status_code != 200:
            log(f"    -> HTTP {r.status_code}")
            # Try without .directory
            url = f"https://lsa.umich.edu/{path}/people/{slug}.html"
            log(f"  Fallback: {url}")
            r = scraper.get(url, timeout=30)
            if r.status_code != 200:
                log(f"    -> HTTP {r.status_code}")
                return results

        soup = BeautifulSoup(r.text, 'html.parser')

        # Strategy 1: Decode Cloudflare-protected emails from person cards
        person_divs = soup.find_all('div', class_='person')
        log(f"    -> Found {len(person_divs)} person entries")

        for div in person_divs:
            # Get name from profile link
            name_link = div.find('a', class_='profileLink')
            if not name_link:
                name_link = div.find('a', href=re.compile(r'/people/'))
            name = name_link.get_text(strip=True) if name_link else ''

            # Get email from data-cfemail
            cf_span = div.find('span', class_='__cf_email__')
            email = ''
            if cf_span:
                encoded = cf_span.get('data-cfemail', '')
                if encoded:
                    email = decode_cf_email(encoded)

            # Fallback: try mailto links
            if not email:
                mailto = extract_mailto_emails(div, 'umich.edu')
                if mailto:
                    email = mailto[0]

            # Fallback: construct from profile URL
            if not email and name_link:
                href = name_link.get('href', '')
                match = re.search(r'/people/[^/]+/([\w-]+)\.html', href)
                if match:
                    uniqname = match.group(1)
                    email = f"{uniqname}@umich.edu"

            if email and email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': url,
                })

        # Strategy 2: If no person divs found, try extracting all CF emails from page
        if not person_divs:
            cf_spans = soup.find_all('span', class_='__cf_email__')
            log(f"    -> Found {len(cf_spans)} CF-protected email spans")
            for span in cf_spans:
                encoded = span.get('data-cfemail', '')
                if encoded:
                    email = decode_cf_email(encoded)
                    if email and email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        # Try to find associated name
                        name = ''
                        parent = span.parent
                        for _ in range(6):
                            if parent is None:
                                break
                            for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a']):
                                tag_text = tag.get_text(strip=True)
                                if (tag_text and '@' not in tag_text and
                                        len(tag_text) > 2 and len(tag_text) < 80 and
                                        'cf_email' not in str(tag.get('class', ''))):
                                    if not any(x in tag_text.lower() for x in [
                                        'email', 'contact', 'phone', 'department',
                                        'graduate', 'student', 'people', 'faculty'
                                    ]):
                                        name = tag_text
                                        break
                            if name:
                                break
                            parent = parent.parent

                        results.append({
                            'email': email,
                            'name': name,
                            'department': department,
                            'source_url': url,
                        })

        # Strategy 3: Extract any remaining umich.edu emails from page text
        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_umich_emails(page_text)
        for email in text_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': '',
                    'department': department,
                    'source_url': url,
                })

        # Strategy 4: Check for obfuscated emails
        cfemail_from_html = re.findall(r'data-cfemail="([a-f0-9]+)"', r.text)
        for encoded in cfemail_from_html:
            email = decode_cf_email(encoded)
            if email and email not in seen_emails and not is_admin_email(email):
                if '@umich.edu' in email or '@' in email:
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': '',
                        'department': department,
                        'source_url': url,
                    })

    except Exception as e:
        log(f"    -> Error: {e}")

    log(f"    -> {len(results)} emails extracted")
    return results


# ============================================================
# ENGINEERING DEPARTMENTS
# ============================================================

ENGINEERING_DEPARTMENTS = [
    {
        "department": "Computer Science & Engineering",
        "urls": ["https://cse.engin.umich.edu/people/phd-students/"],
        "type": "wordpress_mailto",
    },
    {
        "department": "Electrical & Computer Engineering",
        "urls": [
            "https://ece.engin.umich.edu/people/phd-students/",
            "https://ece.engin.umich.edu/people/graduate-students/",
        ],
        "type": "wordpress_mailto",
    },
    {
        "department": "Mechanical Engineering",
        "urls": [
            "https://me.engin.umich.edu/people/phd-students/",
            "https://me.engin.umich.edu/people/graduate-students/",
            "https://me.engin.umich.edu/people/students/",
        ],
        "type": "wordpress_mailto",
    },
    {
        "department": "Civil & Environmental Engineering",
        "urls": [
            "https://cee.engin.umich.edu/people/phd-students/",
            "https://cee.engin.umich.edu/people/graduate-students/",
        ],
        "type": "wordpress_mailto",
    },
    {
        "department": "Biomedical Engineering",
        "urls": [
            "https://bme.umich.edu/people/phd-students/",
            "https://bme.umich.edu/people/graduate-students/",
        ],
        "type": "wordpress_mailto",
    },
    {
        "department": "Aerospace Engineering",
        "urls": [
            "https://aero.engin.umich.edu/people/phd-students/",
            "https://aero.engin.umich.edu/people/graduate-students/",
        ],
        "type": "wordpress_mailto",
    },
    {
        "department": "Industrial & Operations Engineering",
        "urls": [
            "https://ioe.engin.umich.edu/people/phd-students/",
            "https://ioe.engin.umich.edu/people/graduate-students/",
        ],
        "type": "wordpress_mailto",
    },
    {
        "department": "Materials Science & Engineering",
        "urls": ["https://mse.engin.umich.edu/people/graduate-students/"],
        "type": "plone",  # Uses Plone, no Cloudflare
    },
    {
        "department": "Naval Architecture & Marine Engineering",
        "urls": [
            "https://name.engin.umich.edu/people/phd-students/",
            "https://name.engin.umich.edu/people/graduate-students/",
        ],
        "type": "wordpress_mailto",
    },
    {
        "department": "Chemical Engineering",
        "urls": [
            "https://cheme.engin.umich.edu/people/phd-students/",
            "https://cheme.engin.umich.edu/people/graduate-students/",
        ],
        "type": "wordpress_mailto",
    },
]


def scrape_engineering_plone(config, session):
    """Scrape Plone-based engineering pages (MSE) using regular requests."""
    department = config['department']
    results = []
    seen_emails = set()

    for url in config['urls']:
        log(f"  Fetching (Plone): {url}")
        try:
            r = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
            if r.status_code != 200:
                log(f"    -> HTTP {r.status_code}")
                continue

            soup = BeautifulSoup(r.text, 'html.parser')
            page_text = soup.get_text(separator=' ', strip=True)
            text_emails = extract_umich_emails(page_text)
            mailto_emails = extract_mailto_emails(soup, 'umich.edu')
            all_emails = list(set(text_emails + mailto_emails))

            log(f"    -> Found {len(all_emails)} emails")

            for email in all_emails:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    name = try_get_name_for_email(soup, email)
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': url,
                    })

        except Exception as e:
            log(f"    -> Error: {e}")

    return results


def scrape_engineering_wordpress(config, scraper):
    """Scrape WordPress-based engineering pages using cloudscraper."""
    department = config['department']
    results = []
    seen_emails = set()

    for url in config['urls']:
        log(f"  Fetching (WP): {url}")
        try:
            r = scraper.get(url, timeout=30)
            if r.status_code == 403 and 'Just a moment' in r.text[:500]:
                log(f"    -> Cloudflare challenge (403)")
                continue
            if r.status_code == 404:
                log(f"    -> 404 Not Found")
                continue
            if r.status_code != 200:
                log(f"    -> HTTP {r.status_code}")
                continue

            soup = BeautifulSoup(r.text, 'html.parser')

            # Extract mailto emails
            mailto_emails = extract_mailto_emails(soup, 'umich.edu')
            log(f"    -> Found {len(mailto_emails)} mailto emails")

            for email in mailto_emails:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    name = try_get_name_for_email_wp(soup, email)
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': url,
                    })

            # Also check for CF-protected emails
            cfemail_from_html = re.findall(r'data-cfemail="([a-f0-9]+)"', r.text)
            for encoded in cfemail_from_html:
                email = decode_cf_email(encoded)
                if email and '@umich.edu' in email and email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': '',
                        'department': department,
                        'source_url': url,
                    })

            # Text-based extraction
            text_emails = extract_umich_emails(soup.get_text(separator=' ', strip=True))
            for email in text_emails:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': '',
                        'department': department,
                        'source_url': url,
                    })

            if results:
                break  # Found emails, no need for fallback URLs

        except Exception as e:
            log(f"    -> Error: {e}")

    return results


def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email (general approach)."""
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
                            'email', 'contact', 'phone', 'department',
                            'graduate', 'student', 'people', 'faculty'
                        ]):
                            return tag_text
                parent = parent.parent
    return ""


def try_get_name_for_email_wp(soup, email):
    """Try to find name for email in WordPress-style pages."""
    # Find the mailto link
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if f'mailto:{email}' in href.lower():
            # Look for name in parent/sibling elements
            parent = a_tag.parent
            for _ in range(8):
                if parent is None:
                    break
                # Check for name-like text content
                texts = parent.find_all(string=True)
                for text in texts:
                    t = text.strip()
                    if (t and '@' not in t and 'mailto' not in t and
                            len(t) > 2 and len(t) < 80 and
                            not any(x in t.lower() for x in [
                                'email', 'website', 'phone', 'http',
                                'graduate', 'student', 'phd'
                            ])):
                        # Check if it looks like a name (has at least one space or is capitalized)
                        if ' ' in t or (t[0].isupper() and len(t) > 3):
                            return t
                parent = parent.parent
    return ""


# ============================================================
# ADDITIONAL SCHOOLS
# ============================================================

ADDITIONAL_DEPARTMENTS = [
    {
        "department": "School of Information",
        "urls": [
            "https://www.si.umich.edu/people/phd-students",
            "https://www.si.umich.edu/people/directory/students",
        ],
    },
    {
        "department": "School of Public Health - Epidemiology",
        "urls": ["https://sph.umich.edu/epid/phd-students.html"],
    },
    {
        "department": "School of Public Health - Biostatistics",
        "urls": ["https://sph.umich.edu/biostat/phd-students.html"],
    },
    {
        "department": "Ford School of Public Policy",
        "urls": [
            "https://fordschool.umich.edu/phd-students",
            "https://fordschool.umich.edu/people/phd-students",
        ],
    },
]


def scrape_additional_dept(config, scraper):
    """Scrape additional departments with generic approach."""
    department = config['department']
    results = []
    seen_emails = set()

    for url in config['urls']:
        log(f"  Fetching: {url}")
        try:
            r = scraper.get(url, timeout=30)
            if r.status_code != 200:
                log(f"    -> HTTP {r.status_code}")
                continue

            soup = BeautifulSoup(r.text, 'html.parser')

            # CF email decoding
            cfemail_from_html = re.findall(r'data-cfemail="([a-f0-9]+)"', r.text)
            for encoded in cfemail_from_html:
                email = decode_cf_email(encoded)
                if email and '@umich.edu' in email and email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': '',
                        'department': department,
                        'source_url': url,
                    })

            # Mailto extraction
            mailto_emails = extract_mailto_emails(soup, 'umich.edu')
            for email in mailto_emails:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    name = try_get_name_for_email(soup, email)
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': url,
                    })

            # Text extraction
            page_text = soup.get_text(separator=' ', strip=True)
            text_emails = extract_umich_emails(page_text)
            for email in text_emails:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': '',
                        'department': department,
                        'source_url': url,
                    })

            # Person divs (LSA-style)
            person_divs = soup.find_all('div', class_='person')
            for div in person_divs:
                name_link = div.find('a', class_='profileLink')
                if not name_link:
                    name_link = div.find('a', href=re.compile(r'/people/'))
                name = name_link.get_text(strip=True) if name_link else ''

                cf_span = div.find('span', class_='__cf_email__')
                email = ''
                if cf_span:
                    encoded = cf_span.get('data-cfemail', '')
                    if encoded:
                        email = decode_cf_email(encoded)

                if not email:
                    mailto = extract_mailto_emails(div, 'umich.edu')
                    if mailto:
                        email = mailto[0]

                if email and email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': url,
                    })

            if results:
                break

        except Exception as e:
            log(f"    -> Error: {e}")

    return results


# ============================================================
# MAIN
# ============================================================

def main():
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'darwin', 'desktop': True}
    )
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

    # ---- Phase 1: LSA Departments ----
    log("=" * 70)
    log("PHASE 1: LSA DEPARTMENTS (Cloudflare-protected)")
    log("=" * 70)

    for config in LSA_DEPARTMENTS:
        log(f"\n{'=' * 50}")
        log(f"Department: {config['department']}")
        log(f"{'=' * 50}")
        try:
            results = scrape_lsa_department(config, scraper)
            n = add_results(results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR: {e}")

    # ---- Phase 2: Engineering Departments ----
    log("\n\n" + "=" * 70)
    log("PHASE 2: ENGINEERING DEPARTMENTS")
    log("=" * 70)

    for config in ENGINEERING_DEPARTMENTS:
        log(f"\n{'=' * 50}")
        log(f"Department: {config['department']}")
        log(f"{'=' * 50}")
        try:
            if config['type'] == 'plone':
                results = scrape_engineering_plone(config, session)
            else:
                results = scrape_engineering_wordpress(config, scraper)
            n = add_results(results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR: {e}")

    # ---- Phase 3: Additional Schools ----
    log("\n\n" + "=" * 70)
    log("PHASE 3: ADDITIONAL SCHOOLS")
    log("=" * 70)

    for config in ADDITIONAL_DEPARTMENTS:
        log(f"\n{'=' * 50}")
        log(f"Department: {config['department']}")
        log(f"{'=' * 50}")
        try:
            results = scrape_additional_dept(config, scraper)
            n = add_results(results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR: {e}")

    # ---- Save results ----
    log(f"\n\n{'=' * 70}")
    log(f"RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique @umich.edu emails: {len(all_results)}")

    # Save CSV
    output_csv = 'michigan_dept_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")

    # Save JSON
    output_json = 'michigan_dept_emails.json'
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

    all_depts = [c['department'] for c in LSA_DEPARTMENTS] + \
                [c['department'] for c in ENGINEERING_DEPARTMENTS] + \
                [c['department'] for c in ADDITIONAL_DEPARTMENTS]
    depts_with_zero = [d for d in all_depts if d not in dept_counts]
    if depts_with_zero:
        log(f"\nDepartments with 0 emails found:")
        for d in depts_with_zero:
            log(f"  - {d}")

    return all_results


if __name__ == '__main__':
    main()
