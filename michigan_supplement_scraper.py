#!/usr/bin/env python3
"""
Supplementary scraper for University of Michigan departments that
the main scraper missed. Handles:
1. LSA departments with different URL patterns
2. MSE uniqname extraction from profile URLs
3. CSE/ECE extraction from page source
4. General /people.directory.html pages filtered for grad students
"""

import cloudscraper
import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time


def log(msg):
    print(msg, flush=True)


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}


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


def scrape_lsa_directory(scraper, url, department, grad_only=True):
    """Scrape an LSA .directory.html page, optionally filtering for grad students."""
    results = []
    seen_emails = set()

    log(f"  Fetching: {url}")
    try:
        r = scraper.get(url, timeout=30)
        if r.status_code != 200:
            log(f"    -> HTTP {r.status_code}")
            return results

        soup = BeautifulSoup(r.text, 'html.parser')
        person_divs = soup.find_all('div', class_='person')
        log(f"    -> Found {len(person_divs)} person entries")

        for div in person_divs:
            # Check if this is a grad student
            if grad_only:
                text = div.get_text(separator='|', strip=True).lower()
                is_grad = any(kw in text for kw in [
                    'graduate student', 'phd', 'doctoral', 'grad student',
                    'ph.d.', 'candidate', 'graduate student - phd',
                ])
                if not is_grad:
                    continue

            # Get name
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

            # Fallback: profile URL uniqname
            if not email and name_link:
                href = name_link.get('href', '')
                match = re.search(r'/people/[^/]+/([\w-]+)\.html', href)
                if match:
                    email = f"{match.group(1)}@umich.edu"

            if email and email not in seen_emails and not is_admin_email(email):
                if '@umich.edu' in email:
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': url,
                    })

        # Also get CF emails from raw HTML that might not be in person divs
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

    except Exception as e:
        log(f"    -> Error: {e}")

    log(f"    -> {len(results)} grad student emails")
    return results


def scrape_mse_from_page(scraper):
    """Scrape MSE grad students by extracting uniqnames from profile URLs/images."""
    results = []
    seen_emails = set()
    url = "https://mse.engin.umich.edu/people/graduate-students/"
    department = "Materials Science & Engineering"

    log(f"  Fetching MSE: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            log(f"    -> HTTP {r.status_code}")
            return results

        soup = BeautifulSoup(r.text, 'html.parser')

        # Find all student entries - they have h6 headings with names
        # and image paths containing uniqnames
        h6_tags = soup.find_all('h6')

        for h6 in h6_tags:
            name = h6.get_text(strip=True)
            if not name or len(name) < 2:
                continue

            # Look for a profile link
            link = h6.find('a', href=True)
            email = ''

            if link:
                href = link.get('href', '')
                match = re.search(r'/people/([\w]+)/?', href)
                if match:
                    uniqname = match.group(1)
                    if uniqname not in ['graduate-students', 'fac', 'staff',
                                         'emeritus-faculty', 'lecturers',
                                         'research-scientists', 'research-fellows',
                                         'courtesy-faculty', 'adjunct-faculty']:
                        email = f"{uniqname}@umich.edu"

            # If no link, try finding uniqname from nearby image URL
            if not email:
                parent = h6.parent
                if parent:
                    img = parent.find('img')
                    if img:
                        src = img.get('src', '')
                        match = re.search(r'/people/([\w]+)/@@images/', src)
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
            elif not email and name:
                # We have a name but no email - skip (no reliable email)
                pass

    except Exception as e:
        log(f"    -> Error: {e}")

    log(f"    -> {len(results)} MSE emails")
    return results


def main():
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'darwin', 'desktop': True}
    )

    # Load existing results
    existing_emails = set()
    existing_results = []
    try:
        with open('michigan_dept_emails.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_results.append(row)
                existing_emails.add(row['email'].lower().strip())
        log(f"Loaded {len(existing_results)} existing results ({len(existing_emails)} unique emails)")
    except FileNotFoundError:
        log("No existing results file found, starting fresh")

    new_results = []
    global_seen = set(existing_emails)

    def add_results(results):
        count = 0
        for r in results:
            email = r['email'].lower().strip()
            if email and email not in global_seen:
                global_seen.add(email)
                new_results.append(r)
                count += 1
        return count

    # ---- 1. Fix LSA departments with different URL patterns ----
    log("\n" + "=" * 70)
    log("FIXING LSA DEPARTMENTS WITH DIFFERENT URLS")
    log("=" * 70)

    lsa_fixes = [
        # Sociology uses /current-graduate-students
        ("Sociology", "https://lsa.umich.edu/soc/people/current-graduate-students.directory.html", False),
        # EEB uses /eeb/ not /biology/
        ("Ecology & Evolutionary Biology", "https://lsa.umich.edu/eeb/people/graduate-students.directory.html", False),
        # Romance Languages uses /rll/
        ("Romance Languages & Literatures", "https://lsa.umich.edu/rll/people/graduate-students.directory.html", False),
        # Chemistry - use people directory and filter for grad students
        ("Chemistry", "https://lsa.umich.edu/chem/people.directory.html", True),
        # Asian Languages - use people directory and filter
        ("Asian Languages & Cultures", "https://lsa.umich.edu/asian/people.directory.html", True),
        # German - use people directory and filter
        ("German Studies", "https://lsa.umich.edu/german/people.directory.html", True),
        # Women's & Gender Studies uses /wgs/
        ("Women's & Gender Studies", "https://lsa.umich.edu/wgs/people.directory.html", True),
        # DAAS uses /daas/
        ("Afroamerican & African Studies", "https://lsa.umich.edu/daas/people.directory.html", True),
    ]

    for dept, url, need_filter in lsa_fixes:
        log(f"\n{'=' * 50}")
        log(f"Department: {dept}")
        log(f"{'=' * 50}")
        results = scrape_lsa_directory(scraper, url, dept, grad_only=need_filter)
        n = add_results(results)
        log(f"  => {n} new unique emails added")
        time.sleep(1)

    # ---- 2. MSE from profile URLs ----
    log("\n" + "=" * 70)
    log("SCRAPING MSE GRADUATE STUDENTS")
    log("=" * 70)

    mse_results = scrape_mse_from_page(scraper)
    n = add_results(mse_results)
    log(f"  => {n} new unique MSE emails added")

    # ---- 3. Summary and merge ----
    log(f"\n{'=' * 70}")
    log(f"SUPPLEMENT RESULTS")
    log(f"{'=' * 70}")
    log(f"New emails found: {len(new_results)}")

    # Merge with existing results
    all_results = existing_results + new_results
    log(f"Total emails after merge: {len(all_results)}")

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

    log(f"\nTotal: {len(all_results)} emails across {len(dept_counts)} departments")


if __name__ == '__main__':
    main()
