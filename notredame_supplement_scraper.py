#!/usr/bin/env python3
"""
University of Notre Dame - Supplemental Email Scraper
Adds emails from sources that the main scraper missed:
- The Observer (student newspaper) staff
- Engineering faculty pages (for grad student emails)
- Law school pages
- Additional department pages with alternate URL patterns
- Student Government
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/notredame_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/notredame_dept_emails.json'


def extract_nd_emails(text):
    """Extract all @nd.edu email addresses from text."""
    pattern = r'[\w.+-]+@nd\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup):
    """Extract nd.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(r'mailto:\s*([\w.+-]+@nd\.edu)', href, re.IGNORECASE)
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
        'provost@', 'president@', 'commencement@', 'oit@',
        'gradschool@', 'askhr@', 'ndworks@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


def get_soup(url, session):
    """Fetch a page and return BeautifulSoup object."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=25, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        else:
            log(f"    HTTP {resp.status_code} for {url}")
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
                for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a']):
                    tag_text = tag.get_text(strip=True)
                    if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                        if not any(x in tag_text.lower() for x in [
                            'email', 'contact', 'phone', 'http', 'department',
                            'graduate', 'student', 'people', 'faculty', 'office',
                            'read more', 'view profile', 'website', 'lab'
                        ]):
                            return tag_text
                parent = parent.parent

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


def scrape_page_for_emails(url, department, session):
    """Generic page scraper that returns all found @nd.edu emails."""
    results = []
    soup, final_url = get_soup(url, session)
    if soup is None:
        return results

    page_text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_nd_emails(page_text)
    mailto_emails = extract_mailto_emails(soup)
    all_emails = list(set(text_emails + mailto_emails))

    for email in all_emails:
        if not is_admin_email(email):
            name = try_get_name_for_email(soup, email)
            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': final_url or url,
            })

    # Also check scripts
    for script in soup.find_all('script'):
        if script.string:
            script_emails = extract_nd_emails(script.string)
            for email in script_emails:
                if not is_admin_email(email) and not any(r['email'] == email for r in results):
                    results.append({
                        'email': email,
                        'name': '',
                        'department': department,
                        'source_url': final_url or url,
                    })

    return results


def scrape_profile_page(url, session):
    """Scrape an individual profile page for nd.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None
    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_nd_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


def find_and_visit_profile_links(soup, base_url, department, session, seen_emails, max_profiles=60):
    """Find profile links on a listing page and visit them for emails."""
    results = []
    seen_urls = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)

        if full_url in seen_urls:
            continue
        if '#' in href and not href.startswith('http'):
            continue

        if re.search(r'/(?:people|faculty|staff)/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'college', 'engineering',
                    'research', 'about', 'news'
                ]):
                    seen_urls.add(full_url)

    profile_urls = list(seen_urls)[:max_profiles]
    log(f"    -> Found {len(profile_urls)} profile links to visit")

    for i, purl in enumerate(profile_urls):
        email = scrape_profile_page(purl, session)
        if email and email not in seen_emails:
            seen_emails.add(email)
            # Try to get name from the link text
            name = ""
            for a_tag in soup.find_all('a', href=True):
                if urljoin(base_url, a_tag.get('href', '')) == purl:
                    name = a_tag.get_text(strip=True)
                    break
            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': purl,
            })
        if (i + 1) % 10 == 0:
            log(f"    Visited {i+1}/{len(profile_urls)} profiles, found {len(results)} emails")
        time.sleep(0.3)

    return results


def main():
    session = requests.Session()

    # Load existing results
    existing_emails = set()
    existing_results = []
    try:
        with open(OUTPUT_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_results.append(row)
                if row['email']:
                    existing_emails.add(row['email'].lower().strip())
        log(f"Loaded {len(existing_results)} existing results ({len(existing_emails)} unique emails)")
    except FileNotFoundError:
        log("No existing results found, starting fresh")

    new_results = []
    seen_emails = set(existing_emails)

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
    log("NOTRE DAME SUPPLEMENTAL SCRAPER")
    log("=" * 70)

    # =========================================================
    # 1. THE OBSERVER (Student Newspaper) - manual extraction
    # =========================================================
    log("\n\n1. THE OBSERVER STAFF")
    log("=" * 70)

    # These emails were found on the Observer contact page
    observer_staff = [
        {"email": "lkelly8@nd.edu", "name": "Liam Kelly (Editor-in-Chief)"},
        {"email": "gnocjar@nd.edu", "name": "Gray Nocjar (Managing Editor)"},
        {"email": "hjagodzi@nd.edu", "name": "Henry Jagodzinski (Asst. Managing Editor)"},
        {"email": "gtadajwe@nd.edu", "name": "Grace Tadajweski (Asst. Managing Editor)"},
        {"email": "ahernan@nd.edu", "name": "Abby Hernan (Asst. Managing Editor)"},
    ]
    for entry in observer_staff:
        entry['department'] = 'The Observer (Student Newspaper)'
        entry['source_url'] = 'https://www.ndsmcobserver.com/page/contact'

    n = add_results(observer_staff)
    log(f"  => {n} new Observer emails added")

    # Also scrape the Observer contact/about pages for any we missed
    for url in ['https://www.ndsmcobserver.com/page/contact', 'https://www.ndsmcobserver.com/page/about']:
        results = scrape_page_for_emails(url, 'The Observer (Student Newspaper)', session)
        n = add_results(results)
        log(f"  => {n} new emails from {url}")
        time.sleep(0.5)

    # =========================================================
    # 2. ENGINEERING - Try faculty pages for any grad student emails
    # =========================================================
    log("\n\n2. ENGINEERING DEPARTMENTS (Faculty/Research pages)")
    log("=" * 70)

    eng_urls = [
        # CSE
        ("Computer Science & Engineering", [
            "https://cse.nd.edu/faculty/",
            "https://cse.nd.edu/graduate/phd-in-computer-science-and-engineering/",
            "https://cse.nd.edu/graduate/",
            "https://cse.nd.edu/research/",
        ]),
        # EE
        ("Electrical Engineering", [
            "https://ee.nd.edu/faculty/",
            "https://ee.nd.edu/graduate/",
            "https://ee.nd.edu/research/",
            "https://ee.nd.edu/people/",
        ]),
        # AME
        ("Aerospace & Mechanical Engineering", [
            "https://ame.nd.edu/faculty/",
            "https://ame.nd.edu/graduate/",
            "https://ame.nd.edu/research/",
            "https://ame.nd.edu/people/",
        ]),
        # CEEES
        ("Civil & Environmental Engineering", [
            "https://ceees.nd.edu/faculty/",
            "https://ceees.nd.edu/graduate/",
            "https://ceees.nd.edu/research/",
            "https://ceees.nd.edu/people/",
        ]),
        # CBE
        ("Chemical & Biomolecular Engineering", [
            "https://cbe.nd.edu/faculty/",
            "https://cbe.nd.edu/graduate/",
            "https://cbe.nd.edu/research/",
            "https://cbe.nd.edu/people/",
        ]),
    ]

    for dept, urls in eng_urls:
        log(f"\n  {dept}")
        for url in urls:
            log(f"    Trying: {url}")
            soup, final_url = get_soup(url, session)
            if soup is None:
                continue

            # Extract emails directly from page
            results = scrape_page_for_emails(url, dept, session)
            n = add_results(results)
            if n > 0:
                log(f"      => {n} new emails from page text")

            # Visit profile links
            profile_results = find_and_visit_profile_links(
                soup, final_url or url, dept, session, seen_emails, max_profiles=50
            )
            n = add_results(profile_results)
            if n > 0:
                log(f"      => {n} new emails from profiles")

            time.sleep(0.5)

    # =========================================================
    # 3. LAW SCHOOL - try more pages
    # =========================================================
    log("\n\n3. LAW SCHOOL")
    log("=" * 70)

    law_urls = [
        "https://law.nd.edu/student-life/student-organizations/",
        "https://law.nd.edu/faculty-and-research/faculty-profiles/",
        "https://law.nd.edu/about/contact/",
        "https://law.nd.edu/faculty-and-research/",
        "https://law.nd.edu/people/",
    ]

    for url in law_urls:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        results = scrape_page_for_emails(url, 'Law School', session)
        n = add_results(results)
        log(f"    => {n} new emails")

        # Also visit faculty profile links
        if 'faculty' in url.lower() or 'people' in url.lower():
            profile_results = find_and_visit_profile_links(
                soup, final_url or url, 'Law School', session, seen_emails, max_profiles=40
            )
            n = add_results(profile_results)
            if n > 0:
                log(f"    => {n} new emails from profiles")

        time.sleep(0.5)

    # =========================================================
    # 4. LINGUISTICS (try alternate domain)
    # =========================================================
    log("\n\n4. LINGUISTICS (alternate URLs)")
    log("=" * 70)

    ling_urls = [
        "https://al.nd.edu/departments-programs/linguistics/",
        "https://al.nd.edu/departments-programs/linguistics/people/",
        "https://romancelanguages.nd.edu/linguistics/",
    ]

    for url in ling_urls:
        log(f"  Trying: {url}")
        results = scrape_page_for_emails(url, 'Linguistics', session)
        n = add_results(results)
        log(f"    => {n} new emails")
        time.sleep(0.5)

    # =========================================================
    # 5. GERMAN & RUSSIAN (alternate URLs)
    # =========================================================
    log("\n\n5. GERMAN & RUSSIAN (alternate URLs)")
    log("=" * 70)

    german_urls = [
        "https://al.nd.edu/departments-programs/german-and-russian-languages-and-literatures/",
        "https://al.nd.edu/departments-programs/german-and-russian-languages-and-literatures/people/",
        "https://germanandrussian.nd.edu/people/graduate-students/",
        "https://germanandrussian.nd.edu/people/",
    ]

    for url in german_urls:
        log(f"  Trying: {url}")
        results = scrape_page_for_emails(url, 'German & Russian', session)
        n = add_results(results)
        log(f"    => {n} new emails")
        time.sleep(0.5)

    # =========================================================
    # 6. STUDENT GOVERNMENT (alternate URLs)
    # =========================================================
    log("\n\n6. STUDENT GOVERNMENT (alternate URLs)")
    log("=" * 70)

    sg_urls = [
        "https://studentgovernment.nd.edu/",
        "https://studentgovernment.nd.edu/about/",
        "https://studentgovernment.nd.edu/leadership/",
        "https://studentgovernment.nd.edu/executive-cabinet/",
    ]

    for url in sg_urls:
        log(f"  Trying: {url}")
        results = scrape_page_for_emails(url, 'Student Government', session)
        n = add_results(results)
        log(f"    => {n} new emails")
        time.sleep(0.5)

    # =========================================================
    # 7. ADDITIONAL DEPARTMENT PAGES - try visiting individual profiles
    #    for departments with few results
    # =========================================================
    log("\n\n7. ADDITIONAL PROFILE SCRAPING")
    log("=" * 70)

    additional_profile_pages = [
        ("History", [
            "https://history.nd.edu/people/",
            "https://history.nd.edu/people/faculty/",
        ]),
        ("Classics", [
            "https://classics.nd.edu/people/",
            "https://classics.nd.edu/people/faculty/",
        ]),
        ("Anthropology", [
            "https://anthropology.nd.edu/people/",
            "https://anthropology.nd.edu/people/faculty/",
        ]),
        ("Keough School of Global Affairs", [
            "https://keough.nd.edu/people/",
            "https://keough.nd.edu/people/faculty/",
        ]),
    ]

    for dept, urls in additional_profile_pages:
        log(f"\n  {dept}")
        for url in urls:
            log(f"    Trying: {url}")
            soup, final_url = get_soup(url, session)
            if soup is None:
                continue

            results = scrape_page_for_emails(url, dept, session)
            n = add_results(results)
            if n > 0:
                log(f"      => {n} new emails from page text")

            profile_results = find_and_visit_profile_links(
                soup, final_url or url, dept, session, seen_emails, max_profiles=40
            )
            n = add_results(profile_results)
            if n > 0:
                log(f"      => {n} new emails from profiles")

            time.sleep(0.5)

    # =========================================================
    # SAVE COMBINED RESULTS
    # =========================================================
    all_results = existing_results + new_results

    log(f"\n\n{'=' * 70}")
    log(f"SUPPLEMENT RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Previously had: {len(existing_emails)} unique emails")
    log(f"New emails added: {len(new_results)}")
    log(f"Total unique @nd.edu emails: {len(seen_emails)}")

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

    # Summary of new results by department
    if new_results:
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
    dept_counts = {}
    for r in all_results:
        if r.get('email'):
            dept = r['department']
            dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")


if __name__ == '__main__':
    main()
