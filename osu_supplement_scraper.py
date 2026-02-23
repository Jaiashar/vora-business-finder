#!/usr/bin/env python3
"""
Ohio State University - Supplemental Scraper
Covers Engineering directories (paginated), Professional Schools, 
and other sources that the main scraper missed.
Merges results into the main osu_dept_emails.csv.
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/osu_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/osu_dept_emails.json'


def extract_osu_emails(text):
    """Extract @osu.edu and @buckeyemail.osu.edu emails."""
    pattern = r'[\w.+-]+@(?:buckeyemail\.)?osu\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:buckeyemail\.)?osu\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract OSU emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:buckeyemail\.)?osu\.edu)',
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
        'askhr@', 'ocio@', 'buckeyelink@',
        'asc-deansoffice@', 'asc@', 'engineering@', 'fisher@',
        'law@', 'pharmacy@', 'nursing@', 'cph@', 'csw@',
        'askit@', 'noreply@', 'do-not-reply@', 'donotreply@',
        'email@', 'accessibility@', 'sl-accessibility@',
        'usg@', 'studentorganizations@', 'con-webmaster@',
        'cop-digitalaccessibility@', 'ehe-accessibility@',
        'csw-accessibility@', 'cbe@', 'ece-', 'cse-',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


def get_soup(url, session):
    """Fetch a page and return BeautifulSoup."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        else:
            log(f"    HTTP {resp.status_code}")
            return None, None
    except Exception as e:
        log(f"    Error: {e}")
        return None, None


def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email."""
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
                            'read more', 'view profile', 'website', 'directory'
                        ]):
                            return tag_text
                parent = parent.parent

    # Strategy 2: Look for nearby heading
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
                        'read more', 'department', 'faculty', 'office', 'http', 'directory'
                    ]):
                        return name
            parent = parent.parent

    return ""


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
        'article',
        'tr',
        '.teaser',
        '.person-listing',
        '.people-listing__person',
        '.osu-kinetic-people-result',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_osu_emails(text)
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
                                'read more', 'view profile', 'directory'
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


def scrape_paginated_directory(base_url, department, session, max_pages=30):
    """Scrape a paginated OSU directory (e.g., /directory?page=0, ?page=1, ...)."""
    results = []
    seen_emails = set()

    log(f"\n{'=' * 60}")
    log(f"Department: {department}")
    log(f"{'=' * 60}")

    for page_num in range(max_pages):
        url = f"{base_url}?page={page_num}" if page_num > 0 else base_url
        log(f"  Page {page_num}: {url}")

        soup, final_url = get_soup(url, session)
        if soup is None:
            break

        page_text = soup.get_text(separator=' ', strip=True)

        # Extract from cards
        card_results = extract_from_person_cards(soup, final_url or url, department)
        new_on_page = 0
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)
                new_on_page += 1

        # Extract all emails from text
        text_emails = extract_osu_emails(page_text)
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
                new_on_page += 1

        # Check for JS-embedded emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_osu_emails(script.string)
                for email in script_emails:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': department,
                            'source_url': final_url or url,
                        })
                        new_on_page += 1

        log(f"    -> {new_on_page} new emails on this page")

        # Check if there's a next page
        has_next = False
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if f'page={page_num + 1}' in href:
                has_next = True
                break

        if not has_next:
            log(f"    -> No more pages")
            break

        if new_on_page == 0:
            log(f"    -> No new emails, stopping pagination")
            break

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


def scrape_simple_page(url, department, session):
    """Scrape a single page for emails."""
    results = []
    seen_emails = set()

    log(f"  Trying: {url}")
    soup, final_url = get_soup(url, session)
    if soup is None:
        return results

    page_text = soup.get_text(separator=' ', strip=True)

    # Cards
    card_results = extract_from_person_cards(soup, final_url or url, department)
    for r in card_results:
        if r['email'] not in seen_emails:
            seen_emails.add(r['email'])
            results.append(r)

    # Text + mailto
    text_emails = extract_osu_emails(page_text)
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

    # Scripts
    for script in soup.find_all('script'):
        if script.string:
            script_emails = extract_osu_emails(script.string)
            for email in script_emails:
                if email not in seen_emails and not is_admin_email(email):
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': '',
                        'department': department,
                        'source_url': final_url or url,
                    })

    return results


# ============================================================
# CONFIGURATIONS
# ============================================================

# Engineering departments with paginated /directory pages
ENGINEERING_DIRECTORIES = [
    ("https://cse.osu.edu/directory", "Computer Science & Engineering"),
    ("https://ece.osu.edu/directory", "Electrical & Computer Engineering"),
    ("https://mae.osu.edu/directory", "Mechanical & Aerospace Engineering"),
    ("https://cbe.osu.edu/directory", "Chemical & Biomolecular Engineering"),
    ("https://mse.osu.edu/directory", "Materials Science & Engineering"),
    ("https://ise.osu.edu/directory", "Integrated Systems Engineering"),
    ("https://engineering.osu.edu/directory", "College of Engineering (General)"),
]

# Additional single-page sources
ADDITIONAL_SINGLE_PAGES = [
    ("https://cbe.osu.edu/graduate-students", "Chemical & Biomolecular Engineering"),
    ("https://cse.osu.edu/people", "Computer Science & Engineering"),
    ("https://cse.osu.edu/research", "Computer Science & Engineering (Research)"),
]

# Professional schools with directory pages
PROFESSIONAL_DIRECTORIES = [
    ("https://knowlton.osu.edu/directory", "Knowlton School of Architecture"),
    ("https://pharmacy.osu.edu/directory", "College of Pharmacy"),
    ("https://ehe.osu.edu/directory", "College of Education & Human Ecology"),
]

# Professional schools - single pages
PROFESSIONAL_SINGLE = [
    ("https://fisher.osu.edu/", "Fisher College of Business"),
    ("https://fisher.osu.edu/academic-programs/phd", "Fisher College of Business (PhD)"),
    ("https://fisher.osu.edu/faculty-research/departments", "Fisher College of Business (Depts)"),
    ("https://moritzlaw.osu.edu/student-organizations", "Moritz College of Law"),
    ("https://moritzlaw.osu.edu/", "Moritz College of Law"),
    ("https://csw.osu.edu/", "College of Social Work"),
    ("https://csw.osu.edu/about/faculty-staff", "College of Social Work"),
    ("https://csw.osu.edu/about", "College of Social Work"),
    ("https://nursing.osu.edu/", "College of Nursing"),
    ("https://nursing.osu.edu/faculty-staff", "College of Nursing"),
    ("https://nursing.osu.edu/about/faculty-staff", "College of Nursing"),
    ("https://www.thelantern.com/contact-us/", "The Lantern (Student Newspaper)"),
]


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    new_results = []
    seen_emails = set()

    # Load existing results to avoid duplicates
    try:
        with open(OUTPUT_CSV, 'r') as f:
            reader = csv.DictReader(f)
            existing = list(reader)
            for r in existing:
                seen_emails.add(r['email'].lower().strip())
        log(f"Loaded {len(existing)} existing emails from {OUTPUT_CSV}")
    except FileNotFoundError:
        existing = []
        log("No existing file found, starting fresh")

    def add_results(results):
        count = 0
        for r in results:
            email = r['email'].lower().strip()
            if email and email not in seen_emails:
                seen_emails.add(email)
                new_results.append(r)
                count += 1
        return count

    log("=" * 70)
    log("OHIO STATE UNIVERSITY - SUPPLEMENTAL SCRAPER")
    log("=" * 70)

    # ---- Phase 1: Engineering Directories (paginated) ----
    log("\n\nPHASE 1: ENGINEERING DIRECTORIES (paginated)")
    log("=" * 70)

    for base_url, department in ENGINEERING_DIRECTORIES:
        try:
            results = scrape_paginated_directory(base_url, department, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {department}: {e}")

    # ---- Phase 2: Additional single-page engineering sources ----
    log("\n\nPHASE 2: ADDITIONAL ENGINEERING PAGES")
    log("=" * 70)

    for url, department in ADDITIONAL_SINGLE_PAGES:
        try:
            results = scrape_simple_page(url, department, session)
            n = add_results(results)
            log(f"  => {n} new unique emails from {url}")
            time.sleep(0.5)
        except Exception as e:
            log(f"  ERROR: {e}")

    # ---- Phase 3: Professional School Directories (paginated) ----
    log("\n\nPHASE 3: PROFESSIONAL SCHOOL DIRECTORIES (paginated)")
    log("=" * 70)

    for base_url, department in PROFESSIONAL_DIRECTORIES:
        try:
            results = scrape_paginated_directory(base_url, department, session)
            n = add_results(results)
            log(f"  => {n} new unique emails added")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {department}: {e}")

    # ---- Phase 4: Professional School Single Pages ----
    log("\n\nPHASE 4: PROFESSIONAL SCHOOL SINGLE PAGES")
    log("=" * 70)

    for url, department in PROFESSIONAL_SINGLE:
        try:
            results = scrape_simple_page(url, department, session)
            n = add_results(results)
            log(f"  => {n} new unique emails from {url}")
            time.sleep(0.5)
        except Exception as e:
            log(f"  ERROR: {e}")

    # ---- Merge and save ----
    log(f"\n\n{'=' * 70}")
    log(f"SUPPLEMENT RESULTS")
    log(f"{'=' * 70}")
    log(f"New emails found: {len(new_results)}")
    log(f"Previously existing: {len(existing)}")

    all_results = existing + new_results
    log(f"Total combined: {len(all_results)}")

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

    # Summary of new additions
    log(f"\n{'=' * 70}")
    log("NEW EMAILS BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in new_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} new emails")

    # Full summary
    log(f"\n{'=' * 70}")
    log("FULL SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    full_counts = {}
    for r in all_results:
        dept = r['department']
        full_counts[dept] = full_counts.get(dept, 0) + 1

    for dept, count in sorted(full_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")

    return all_results


if __name__ == '__main__':
    main()
