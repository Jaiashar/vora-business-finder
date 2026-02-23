#!/usr/bin/env python3
"""
Georgia Tech Supplement Scraper
Targets departments that returned 0 or few emails in the initial scrape.
Visits individual profile pages for ECE, ISyE, BME, Physics, etc.
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


def extract_gatech_emails(text):
    """Extract all @gatech.edu emails from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*gatech\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
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
        'noreply@', 'do-not-reply@', 'donotreply@', 'ask@',
        'ece-', 'me-', 'ae-', 'isye-', 'cs-', 'chbe-',
        'coc-', 'coe-', 'cos-',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


def get_soup(url, session, timeout=20):
    """Fetch a page and return BeautifulSoup object."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        else:
            return None, None
    except Exception as e:
        log(f"    Error fetching {url}: {e}")
        return None, None


def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email on the page."""
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
                        ]):
                            return tag_text
                parent = parent.parent
    return ""


# ============================================================
# LOAD EXISTING RESULTS
# ============================================================

def load_existing_emails():
    """Load already scraped emails to avoid duplicates."""
    existing = set()
    try:
        with open(OUTPUT_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing.add(row['email'].lower().strip())
    except FileNotFoundError:
        pass
    return existing


def load_existing_results():
    """Load all existing results."""
    results = []
    try:
        with open(OUTPUT_JSON, 'r') as f:
            results = json.load(f)
    except FileNotFoundError:
        pass
    return results


# ============================================================
# BME: Direct scrape of faculty page (has mailto links)
# ============================================================

def scrape_bme_faculty(session, seen_emails):
    """Scrape BME faculty page which has direct mailto links."""
    results = []
    department = "Biomedical Engineering (BME)"

    log(f"\n{'=' * 60}")
    log(f"Scraping: BME Faculty Page")
    log(f"{'=' * 60}")

    urls = [
        "https://bme.gatech.edu/bme/faculty",
        "https://www.bme.gatech.edu/bme/faculty",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        # Extract all mailto emails
        mailto_emails = extract_mailto_emails(soup)
        text_emails = extract_gatech_emails(soup.get_text(separator=' ', strip=True))
        all_emails = list(set(mailto_emails + text_emails))

        log(f"    -> Found {len(all_emails)} gatech.edu emails")

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

        if results:
            break
        time.sleep(0.5)

    log(f"  TOTAL for BME: {len(results)} new emails")
    return results


# ============================================================
# ECE: Visit alphabetical directory pages + individual profiles
# ============================================================

def scrape_ece_directory(session, seen_emails):
    """Scrape ECE directory by visiting alphabetical pages and individual profiles."""
    results = []
    department = "Electrical & Computer Engineering (ECE)"

    log(f"\n{'=' * 60}")
    log(f"Scraping: ECE Directory (alphabetical)")
    log(f"{'=' * 60}")

    # Visit each letter page
    for letter in 'abcdefghijklmnopqrstuvwxyz':
        url = f"https://ece.gatech.edu/directory/{letter}"
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        # The listing page may not have emails, but let's check
        text = soup.get_text(separator=' ', strip=True)
        emails = extract_gatech_emails(text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto_emails))

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

        # Find profile links and visit them
        profile_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if re.search(r'ece\.gatech\.edu/directory/[\w-]+$', full_url):
                if full_url != url and '/directory/' + letter != href:
                    name = a_tag.get_text(strip=True)
                    if name and 'Learn more' not in name and '@' not in name and len(name) > 2:
                        profile_links.append((name, full_url))

        # Visit each profile
        seen_profiles = set()
        for name, profile_url in profile_links:
            if profile_url in seen_profiles:
                continue
            seen_profiles.add(profile_url)

            p_soup, p_url = get_soup(profile_url, session)
            if p_soup is None:
                continue

            p_text = p_soup.get_text(separator=' ', strip=True)
            p_emails = extract_gatech_emails(p_text)
            p_mailto = extract_mailto_emails(p_soup)
            p_all = list(set(p_emails + p_mailto))

            for email in p_all:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': name if 'Learn more' not in name else '',
                        'department': department,
                        'source_url': profile_url,
                    })

            time.sleep(0.2)

        time.sleep(0.3)

    log(f"  TOTAL for ECE: {len(results)} new emails")
    return results


# ============================================================
# ISyE: Visit individual PhD student profiles
# ============================================================

def scrape_isye_profiles(session, seen_emails):
    """Scrape ISyE PhD student profiles for emails."""
    results = []
    department = "Industrial & Systems Engineering (ISyE)"

    log(f"\n{'=' * 60}")
    log(f"Scraping: ISyE PhD Student Profiles")
    log(f"{'=' * 60}")

    # First, get the PhD students listing page
    url = "https://www.isye.gatech.edu/people/phd-students"
    log(f"  Getting listing: {url}")
    soup, final_url = get_soup(url, session)
    if soup is None:
        log("  -> Failed to load listing page")
        return results

    # Find all profile links
    profile_links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(url, href)
        if 'isye.gatech.edu/users/' in full_url:
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                profile_links.append((name, full_url))

    log(f"  -> Found {len(profile_links)} student profiles")

    # Visit each profile
    seen_urls = set()
    for name, profile_url in profile_links:
        if profile_url in seen_urls:
            continue
        seen_urls.add(profile_url)

        p_soup, p_url = get_soup(profile_url, session)
        if p_soup is None:
            continue

        p_text = p_soup.get_text(separator=' ', strip=True)
        p_emails = extract_gatech_emails(p_text)
        p_mailto = extract_mailto_emails(p_soup)
        p_all = list(set(p_emails + p_mailto))

        for email in p_all:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': profile_url,
                })

        time.sleep(0.2)

    # Also try faculty/staff pages
    staff_urls = [
        "https://www.isye.gatech.edu/people/faculty",
        "https://www.isye.gatech.edu/people/staff",
    ]
    for url in staff_urls:
        log(f"  Trying staff page: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue
        
        # Check for direct emails on the page
        text = soup.get_text(separator=' ', strip=True)
        emails = extract_gatech_emails(text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto_emails))
        
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

        # Visit profile links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(url, href)
            if 'isye.gatech.edu/users/' in full_url and full_url not in seen_urls:
                seen_urls.add(full_url)
                name = a_tag.get_text(strip=True)
                if not name or '@' in name or len(name) < 2:
                    continue
                p_soup, p_url = get_soup(full_url, session)
                if p_soup is None:
                    continue
                p_text = p_soup.get_text(separator=' ', strip=True)
                p_emails = extract_gatech_emails(p_text)
                p_mailto = extract_mailto_emails(p_soup)
                for email in list(set(p_emails + p_mailto)):
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': name,
                            'department': department,
                            'source_url': full_url,
                        })
                time.sleep(0.2)

        time.sleep(0.5)

    log(f"  TOTAL for ISyE: {len(results)} new emails")
    return results


# ============================================================
# Additional department pages
# ============================================================

def scrape_additional_pages(session, seen_emails):
    """Scrape additional pages for departments that had 0 or low results."""
    results = []

    additional_configs = [
        {
            "department": "School of Physics",
            "urls": [
                "https://physics.gatech.edu/directory",
                "https://physics.gatech.edu/people/all",
            ],
        },
        {
            "department": "School of History & Sociology",
            "urls": [
                "https://hts.gatech.edu/people/faculty",
                "https://hts.gatech.edu/directory",
            ],
        },
        {
            "department": "School of Literature, Media & Communication (LMC)",
            "urls": [
                "https://lmc.gatech.edu/people/faculty",
                "https://lmc.gatech.edu/directory",
            ],
        },
        {
            "department": "Sam Nunn School of International Affairs (INTA)",
            "urls": [
                "https://inta.gatech.edu/people/faculty",
                "https://inta.gatech.edu/directory",
            ],
        },
        {
            "department": "Scheller College of Business",
            "urls": [
                "https://scheller.gatech.edu/directory/index.html",
                "https://scheller.gatech.edu/directory/faculty-and-research/index.html",
                "https://scheller.gatech.edu/directory/faculty.html",
            ],
        },
        {
            "department": "College of Design",
            "urls": [
                "https://design.gatech.edu/people/faculty",
                "https://design.gatech.edu/directory",
                "https://arch.gatech.edu/people/faculty",
            ],
        },
        {
            "department": "Nuclear & Radiological Engineering (NRE)",
            "urls": [
                "https://nre.gatech.edu/directory",
                "https://me.gatech.edu/nuclear-radiological-engineering",
            ],
        },
    ]

    for config in additional_configs:
        department = config['department']
        log(f"\n{'=' * 60}")
        log(f"Additional: {department}")
        log(f"{'=' * 60}")

        dept_results = []
        for url in config['urls']:
            log(f"  Trying: {url}")
            soup, final_url = get_soup(url, session)
            if soup is None:
                continue

            text = soup.get_text(separator=' ', strip=True)
            emails = extract_gatech_emails(text)
            mailto_emails = extract_mailto_emails(soup)
            all_emails = list(set(emails + mailto_emails))

            log(f"    -> Found {len(all_emails)} raw emails")

            for email in all_emails:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    name = try_get_name_for_email(soup, email)
                    dept_results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': final_url or url,
                    })

            # Follow profile links
            profile_links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                full_url = urljoin(url, href)
                if 'gatech.edu' in full_url and (
                    '/directory/' in full_url or '/people/' in full_url or '/users/' in full_url
                ):
                    name = a_tag.get_text(strip=True)
                    if name and '@' not in name and len(name) > 2 and len(name) < 80:
                        if not any(x in name.lower() for x in [
                            'faculty', 'staff', 'student', 'all', 'directory', 'people',
                            'learn more', 'read more', 'view', 'home', 'back',
                        ]):
                            profile_links.append((name, full_url))

            if len(dept_results) < 3 and profile_links:
                log(f"    -> Visiting {len(profile_links[:40])} profiles...")
                seen_profiles = set()
                for name, purl in profile_links[:40]:
                    if purl in seen_profiles:
                        continue
                    seen_profiles.add(purl)
                    p_soup, p_final = get_soup(purl, session)
                    if p_soup is None:
                        continue
                    p_text = p_soup.get_text(separator=' ', strip=True)
                    p_emails = extract_gatech_emails(p_text)
                    p_mailto = extract_mailto_emails(p_soup)
                    for email in list(set(p_emails + p_mailto)):
                        if email not in seen_emails and not is_admin_email(email):
                            seen_emails.add(email)
                            dept_results.append({
                                'email': email,
                                'name': name,
                                'department': department,
                                'source_url': purl,
                            })
                    time.sleep(0.2)

            time.sleep(0.5)

        log(f"  TOTAL for {department}: {len(dept_results)} new emails")
        results.extend(dept_results)

    return results


# ============================================================
# Scheller Business School (different site structure)
# ============================================================

def scrape_scheller(session, seen_emails):
    """Scrape Scheller College of Business."""
    results = []
    department = "Scheller College of Business"

    log(f"\n{'=' * 60}")
    log(f"Scraping: Scheller College of Business")
    log(f"{'=' * 60}")

    urls = [
        "https://www.scheller.gatech.edu/directory/faculty/index.html",
        "https://www.scheller.gatech.edu/directory/index.html",
        "https://www.scheller.gatech.edu/doctoral/current-doctoral-students.html",
        "https://www.scheller.gatech.edu/people",
        "https://scheller.gatech.edu/doctoral/current-doctoral-students.html",
    ]

    for url in urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        text = soup.get_text(separator=' ', strip=True)
        emails = extract_gatech_emails(text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto_emails))

        log(f"    -> Found {len(all_emails)} raw emails")

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

        time.sleep(0.5)

    log(f"  TOTAL for Scheller: {len(results)} new emails")
    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()

    # Load existing data
    existing_emails = load_existing_emails()
    all_results = load_existing_results()
    global_seen = set(existing_emails)

    log("=" * 70)
    log("GEORGIA TECH SUPPLEMENT SCRAPER")
    log(f"Loaded {len(existing_emails)} existing emails")
    log("=" * 70)

    # Phase 1: BME Faculty (had 0 results)
    log("\n\nPHASE 1: BIOMEDICAL ENGINEERING FACULTY")
    log("=" * 70)
    bme_results = scrape_bme_faculty(session, global_seen)
    all_results.extend(bme_results)
    log(f"Running total: {len(all_results)} emails")

    # Phase 2: ECE Directory (had 0 results)
    log("\n\nPHASE 2: ECE DIRECTORY")
    log("=" * 70)
    ece_results = scrape_ece_directory(session, global_seen)
    all_results.extend(ece_results)
    log(f"Running total: {len(all_results)} emails")

    # Phase 3: ISyE Profiles (had 0 results)
    log("\n\nPHASE 3: ISyE PROFILES")
    log("=" * 70)
    isye_results = scrape_isye_profiles(session, global_seen)
    all_results.extend(isye_results)
    log(f"Running total: {len(all_results)} emails")

    # Phase 4: Scheller Business
    log("\n\nPHASE 4: SCHELLER COLLEGE OF BUSINESS")
    log("=" * 70)
    scheller_results = scrape_scheller(session, global_seen)
    all_results.extend(scheller_results)
    log(f"Running total: {len(all_results)} emails")

    # Phase 5: Additional departments
    log("\n\nPHASE 5: ADDITIONAL DEPARTMENTS")
    log("=" * 70)
    additional_results = scrape_additional_pages(session, global_seen)
    all_results.extend(additional_results)
    log(f"Running total: {len(all_results)} emails")

    # Deduplicate
    deduped = {}
    for r in all_results:
        email = r['email'].lower().strip()
        if email not in deduped:
            deduped[email] = r
    all_results = list(deduped.values())

    # Save CSV
    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: (x['department'], x['email'])):
            writer.writerow(r)
    log(f"\nSaved {len(all_results)} total emails to {OUTPUT_CSV}")

    # Save JSON
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(all_results, f, indent=2)
    log(f"Saved to {OUTPUT_JSON}")

    # Summary
    log(f"\n{'=' * 70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")

    log(f"\nTotal unique @gatech.edu emails: {len(all_results)}")


if __name__ == '__main__':
    main()
