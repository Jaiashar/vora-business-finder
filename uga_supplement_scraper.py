#!/usr/bin/env python3
"""
UGA Supplemental Scraper
Handles departments that failed in the main scraper:
1. Engineering - decodes Cloudflare email protection
2. Terry College of Business - visits individual PhD student profiles
3. Athletics - visits individual staff bio pages
4. SPIA (Political Science) - tries alternate URL patterns
5. Other failing departments - tries www. prefix and alternate paths
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


def get_soup(url, session):
    """Fetch a page and return BeautifulSoup object."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=25, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        else:
            return None, None
    except Exception as e:
        log(f"    Error fetching {url}: {e}")
        return None, None


def extract_uga_emails(text):
    """Extract all @uga.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*uga\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup):
    """Extract uga.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uga\.edu)',
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
        'admission@', 'admissions@', 'gradschool@', 'finaid@',
        'ugasga@', 'soci-web@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


# ============================================================
# CLOUDFLARE EMAIL PROTECTION DECODER
# ============================================================

def decode_cloudflare_email(encoded_string):
    """
    Decode Cloudflare's email protection encoding.
    The first two hex chars are the XOR key, the rest are XOR'd email bytes.
    """
    try:
        key = int(encoded_string[:2], 16)
        decoded = ''
        for i in range(2, len(encoded_string), 2):
            byte_val = int(encoded_string[i:i+2], 16) ^ key
            decoded += chr(byte_val)
        return decoded
    except Exception:
        return None


def extract_cloudflare_emails(soup, domain='uga.edu'):
    """Extract emails from Cloudflare email-protection encoded links."""
    results = []

    # Method 1: href links to /cdn-cgi/l/email-protection#HEXSTRING
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        match = re.search(r'email-protection#([0-9a-f]+)', href, re.IGNORECASE)
        if match:
            decoded = decode_cloudflare_email(match.group(1))
            if decoded and domain in decoded.lower():
                results.append(decoded.lower().strip())

    # Method 2: <span class="__cf_email__" data-cfemail="HEXSTRING">
    for span in soup.find_all('span', class_='__cf_email__'):
        encoded = span.get('data-cfemail', '')
        if encoded:
            decoded = decode_cloudflare_email(encoded)
            if decoded and domain in decoded.lower():
                results.append(decoded.lower().strip())

    # Method 3: data-cfemail attribute on any element
    for elem in soup.find_all(attrs={'data-cfemail': True}):
        encoded = elem.get('data-cfemail', '')
        if encoded:
            decoded = decode_cloudflare_email(encoded)
            if decoded and domain in decoded.lower():
                results.append(decoded.lower().strip())

    return list(set(results))


# ============================================================
# ENGINEERING SCRAPER (Cloudflare protected)
# ============================================================

def scrape_engineering(session):
    """Scrape UGA Engineering directory with Cloudflare email decoding."""
    results = []
    seen_emails = set()
    department = "College of Engineering"

    log(f"\n{'=' * 60}")
    log(f"Scraping: {department}")
    log(f"{'=' * 60}")

    # The directory page lists all people - we need to filter for grad students
    # The page has type filters: Research Assistant, Teaching Assistant, etc.
    base_url = "https://engineering.uga.edu/people/"
    alt_url = "https://engineering.uga.edu/directory/"

    for url in [base_url, alt_url]:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        # Decode Cloudflare-protected emails
        cf_emails = extract_cloudflare_emails(soup, 'uga.edu')
        log(f"    -> Decoded {len(cf_emails)} Cloudflare-protected emails")

        # Also get regular emails
        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_uga_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)

        all_emails = list(set(cf_emails + text_emails + mailto_emails))
        log(f"    -> Total emails found: {len(all_emails)}")

        # Now try to match emails with names by finding person cards
        # Engineering uses team_member cards with h6 names
        cards = soup.find_all('a', href=True)
        person_map = {}  # email -> name

        for card in soup.select('[class*="team"], [class*="person"], [class*="member"]'):
            name_tag = card.find(['h6', 'h5', 'h4', 'h3', 'strong'])
            if name_tag:
                name = name_tag.get_text(strip=True)
            else:
                name = ""

            # Get email from cloudflare-protected link within card
            card_emails = extract_cloudflare_emails(card, 'uga.edu')
            for email in card_emails:
                person_map[email] = name

        # Also look at individual profile-linked entries
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if '/team_member/' in href or '/people/' in href:
                name = a_tag.get_text(strip=True)
                if name and '@' not in name and len(name) > 2 and len(name) < 80:
                    # Check for a sibling/nearby email link
                    parent = a_tag.parent
                    if parent:
                        p_cf_emails = extract_cloudflare_emails(parent, 'uga.edu')
                        for email in p_cf_emails:
                            if email not in person_map:
                                person_map[email] = name

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = person_map.get(email, '')
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': final_url or url,
                })

        if results:
            break
        time.sleep(1)

    # If we got the directory but names are sparse, try visiting individual profile pages
    # for entries without names
    nameless = [r for r in results if not r['name']]
    if nameless and len(nameless) > len(results) * 0.5:
        log(f"  -> Many entries without names, trying to scrape profile pages...")
        # Find all profile links
        for url in [base_url, alt_url]:
            soup, final_url = get_soup(url, session)
            if soup is None:
                continue

            profile_links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                full_url = urljoin(final_url or url, href)
                if '/team_member/' in full_url:
                    name = a_tag.get_text(strip=True)
                    if name and '@' not in name and len(name) > 2:
                        profile_links.append({'name': name, 'url': full_url})

            log(f"    -> Found {len(profile_links)} profile links")

            for i, pl in enumerate(profile_links[:100]):
                profile_soup, _ = get_soup(pl['url'], session)
                if profile_soup is None:
                    continue
                pf_cf = extract_cloudflare_emails(profile_soup, 'uga.edu')
                pf_text = extract_uga_emails(profile_soup.get_text(separator=' ', strip=True))
                pf_mailto = extract_mailto_emails(profile_soup)
                pf_all = list(set(pf_cf + pf_text + pf_mailto))
                pf_personal = [e for e in pf_all if not is_admin_email(e)]

                # Check if this person is a graduate student
                page_text = profile_soup.get_text(separator=' ', strip=True).lower()
                is_grad = any(kw in page_text for kw in [
                    'graduate', 'doctoral', 'phd', 'ph.d', 'research assistant',
                    'teaching assistant', 'grad student'
                ])

                if pf_personal and is_grad:
                    for email in pf_personal:
                        if email not in seen_emails:
                            seen_emails.add(email)
                            results.append({
                                'email': email,
                                'name': pl['name'],
                                'department': department,
                                'source_url': pl['url'],
                            })
                time.sleep(0.2)
            break

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# TERRY COLLEGE OF BUSINESS PhD STUDENTS
# ============================================================

def scrape_terry_phd(session):
    """Scrape Terry College PhD student profiles."""
    results = []
    seen_emails = set()
    department = "Terry College of Business (PhD)"

    log(f"\n{'=' * 60}")
    log(f"Scraping: {department}")
    log(f"{'=' * 60}")

    # From the management page, we know student names and profile URLs
    # Let's get them from each department's PhD page
    terry_phd_pages = [
        ("Accounting PhD", "https://www.terry.uga.edu/phd/accounting/"),
        ("Economics PhD", "https://www.terry.uga.edu/phd/economics/"),
        ("Finance PhD", "https://www.terry.uga.edu/phd/finance/"),
        ("Management PhD", "https://www.terry.uga.edu/phd/management/"),
        ("MIS PhD", "https://www.terry.uga.edu/phd/management-information-systems/"),
        ("Marketing PhD", "https://www.terry.uga.edu/phd/marketing/"),
        ("Real Estate PhD", "https://www.terry.uga.edu/phd/real-estate/"),
        ("RMI PhD", "https://www.terry.uga.edu/phd/risk-management-and-insurance/"),
    ]

    # Also try the directory pages directly
    terry_directory_pages = [
        "https://www.terry.uga.edu/directory/?group=accounting&type[]=phd",
        "https://www.terry.uga.edu/directory/?group=economics&type[]=phd",
        "https://www.terry.uga.edu/directory/?group=finance&type[]=phd",
        "https://www.terry.uga.edu/directory/?group=management&type[]=phd",
        "https://www.terry.uga.edu/directory/?group=mis&type[]=phd",
        "https://www.terry.uga.edu/directory/?group=marketing&type[]=phd",
        "https://www.terry.uga.edu/directory/?group=real-estate&type[]=phd",
        "https://www.terry.uga.edu/directory/?group=rmi&type[]=phd",
    ]

    # Collect all profile URLs from PhD pages
    all_profile_urls = []

    for area, url in terry_phd_pages:
        log(f"  Fetching {area}: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue

        # Find links to terry.uga.edu/directory/ pages (student profiles)
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(final_url or url, href)
            name = a_tag.get_text(strip=True)

            if '/directory/' in full_url and name and len(name) > 2 and len(name) < 80:
                if '@' not in name and not any(kw in name.lower() for kw in [
                    'faculty', 'research', 'phd program', 'apply', 'current phd',
                    'handbook', 'contact', 'financial', 'graduate school',
                    'department', 'office', 'terry college'
                ]):
                    # Check if this looks like a student name (not a faculty member)
                    # Faculty usually have "Ph.D." or professor titles on phd pages
                    all_profile_urls.append({
                        'name': name,
                        'url': full_url,
                        'area': area,
                    })

        # Also extract emails directly from the page
        page_text = soup.get_text(separator=' ', strip=True)
        direct_emails = extract_uga_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        for email in list(set(direct_emails + mailto_emails)):
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                name = ''
                # Try to find name near this email
                for a_tag in soup.find_all('a', href=True):
                    if email in a_tag.get('href', '').lower():
                        parent = a_tag.parent
                        if parent:
                            for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong']):
                                tag_text = tag.get_text(strip=True)
                                if tag_text and '@' not in tag_text and len(tag_text) > 2:
                                    name = tag_text
                                    break
                results.append({
                    'email': email,
                    'name': name,
                    'department': f"{department} - {area}",
                    'source_url': final_url or url,
                })

        time.sleep(0.5)

    # Also try directory pages
    for dir_url in terry_directory_pages:
        log(f"  Trying directory: {dir_url}")
        soup, final_url = get_soup(dir_url, session)
        if soup is None:
            continue

        # Find profile links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(final_url or dir_url, href)
            name = a_tag.get_text(strip=True)
            if '/directory/' in full_url and name and len(name) > 2 and len(name) < 80:
                if '@' not in name:
                    all_profile_urls.append({
                        'name': name,
                        'url': full_url,
                        'area': 'Terry PhD',
                    })

        # Extract emails from directory page
        page_text = soup.get_text(separator=' ', strip=True)
        for email in extract_uga_emails(page_text) + extract_mailto_emails(soup):
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': '',
                    'department': department,
                    'source_url': final_url or dir_url,
                })

        time.sleep(0.5)

    # Deduplicate profile URLs
    seen_profile_urls = set()
    unique_profiles = []
    for p in all_profile_urls:
        if p['url'] not in seen_profile_urls:
            seen_profile_urls.add(p['url'])
            unique_profiles.append(p)

    log(f"  Found {len(unique_profiles)} unique profile URLs to visit")

    # Visit each profile page for email
    for i, profile in enumerate(unique_profiles):
        log(f"    [{i+1}/{len(unique_profiles)}] {profile['name']}")
        soup, final_url = get_soup(profile['url'], session)
        if soup is None:
            continue

        page_text = soup.get_text(separator=' ', strip=True)
        emails = extract_uga_emails(page_text) + extract_mailto_emails(soup)
        cf_emails = extract_cloudflare_emails(soup, 'uga.edu')
        all_emails = list(set(emails + cf_emails))

        personal = [e for e in all_emails if not is_admin_email(e)]
        if personal:
            for email in personal:
                if email not in seen_emails:
                    seen_emails.add(email)
                    results.append({
                        'email': email,
                        'name': profile['name'],
                        'department': f"{department} - {profile['area']}",
                        'source_url': profile['url'],
                    })

        time.sleep(0.3)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# ATHLETICS - Visit individual staff bios
# ============================================================

def scrape_athletics(session):
    """Scrape Georgia Bulldogs staff bios for @uga.edu emails."""
    results = []
    seen_emails = set()
    department = "Athletics (Staff)"

    log(f"\n{'=' * 60}")
    log(f"Scraping: {department}")
    log(f"{'=' * 60}")

    url = "https://georgiadogs.com/staff-directory"
    log(f"  Fetching: {url}")
    soup, final_url = get_soup(url, session)
    if soup is None:
        log("  -> Failed to load staff directory")
        return results

    # Collect staff bio URLs
    staff_links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if '/staff-directory/' in href and href != '/staff-directory':
            full_url = urljoin('https://georgiadogs.com', href)
            name = a_tag.get_text(strip=True)
            if name and len(name) > 2 and 'Full Bio' not in name:
                staff_links.append({'name': name, 'url': full_url})

    # Deduplicate
    seen_urls = set()
    unique_staff = []
    for sl in staff_links:
        if sl['url'] not in seen_urls:
            seen_urls.add(sl['url'])
            unique_staff.append(sl)

    log(f"  Found {len(unique_staff)} staff members")

    # Visit first 50 staff bio pages to find @uga.edu emails
    for i, staff in enumerate(unique_staff[:50]):
        bio_soup, _ = get_soup(staff['url'], session)
        if bio_soup is None:
            continue

        page_text = bio_soup.get_text(separator=' ', strip=True)
        emails = extract_uga_emails(page_text) + extract_mailto_emails(bio_soup)
        cf_emails = extract_cloudflare_emails(bio_soup, 'uga.edu')
        all_emails = list(set(emails + cf_emails))

        for email in all_emails:
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': staff['name'],
                    'department': department,
                    'source_url': staff['url'],
                })

        time.sleep(0.3)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# ALTERNATE URL PATTERNS FOR FAILING DEPARTMENTS
# ============================================================

def scrape_alternate_departments(session):
    """Try www. prefix and other alternate URL patterns."""
    results = []
    seen_emails = set()

    # Departments that failed due to DNS or other issues
    alternate_configs = [
        {
            "department": "Economics (Franklin)",
            "urls": [
                "https://www.economics.uga.edu/directory/graduate-students",
                "https://www.economics.uga.edu/directory",
                "https://www.economics.uga.edu/people",
                "https://franklin.uga.edu/departments/economics",
            ],
        },
        {
            "department": "Philosophy",
            "urls": [
                "https://www.philosophy.uga.edu/directory/graduate-students",
                "https://www.philosophy.uga.edu/directory",
                "https://www.philosophy.uga.edu/people",
                "https://franklin.uga.edu/departments/philosophy",
            ],
        },
        {
            "department": "SPIA (Political Science)",
            "urls": [
                "https://spia.uga.edu/departments-centers/department-of-political-science/",
                "https://spia.uga.edu/departments-centers/",
                "https://spia.uga.edu/faculty-and-staff/",
                "https://spia.uga.edu/people/",
            ],
        },
        {
            "department": "Grady College of Journalism",
            "urls": [
                "https://www.grady.uga.edu/directory/graduate-students",
                "https://www.grady.uga.edu/directory",
                "https://www.grady.uga.edu/people",
                "https://grady.uga.edu/directory/graduate-students",
                "https://grady.uga.edu/directory",
                "https://grady.uga.edu/people",
            ],
        },
        {
            "department": "School of Social Work",
            "urls": [
                "https://ssw.uga.edu/directory/graduate-students",
                "https://ssw.uga.edu/directory",
                "https://ssw.uga.edu/people",
                "https://www.ssw.uga.edu/directory",
            ],
        },
        {
            "department": "College of Pharmacy",
            "urls": [
                "https://www.rx.uga.edu/directory",
                "https://rx.uga.edu/directory",
                "https://www.pharmacy.uga.edu/directory",
            ],
        },
        {
            "department": "College of Veterinary Medicine",
            "urls": [
                "https://www.vet.uga.edu/education/academics/graduate-studies/",
                "https://www.vet.uga.edu/directory",
                "https://vet.uga.edu/education/",
            ],
        },
        {
            "department": "School of Law",
            "urls": [
                "https://www.law.uga.edu/student-organizations",
                "https://www.law.uga.edu/directory",
                "https://law.uga.edu/student-organizations",
            ],
        },
        {
            "department": "College of Education",
            "urls": [
                "https://www.coe.uga.edu/directory",
                "https://coe.uga.edu/directory/graduate-students",
                "https://coe.uga.edu/people",
            ],
        },
        {
            "department": "College of Public Health",
            "urls": [
                "https://www.publichealth.uga.edu/directory",
                "https://publichealth.uga.edu/directory/",
                "https://publichealth.uga.edu/people/",
            ],
        },
        {
            "department": "The Red & Black (Student Newspaper)",
            "urls": [
                "https://www.redandblack.com/staff/",
                "https://www.redandblack.com/contact/",
            ],
        },
    ]

    for config in alternate_configs:
        dept = config['department']
        log(f"\n{'=' * 60}")
        log(f"Trying alternate URLs for: {dept}")
        log(f"{'=' * 60}")

        for url in config['urls']:
            log(f"  Trying: {url}")
            soup, final_url = get_soup(url, session)
            if soup is None:
                log(f"    -> Failed")
                continue

            page_text = soup.get_text(separator=' ', strip=True)
            text_emails = extract_uga_emails(page_text)
            mailto_emails = extract_mailto_emails(soup)
            cf_emails = extract_cloudflare_emails(soup, 'uga.edu')
            all_emails = list(set(text_emails + mailto_emails + cf_emails))

            if all_emails:
                log(f"    -> Found {len(all_emails)} emails")
                for email in all_emails:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        # Try to find name
                        name = ''
                        for a_tag in soup.find_all('a', href=True):
                            if email in a_tag.get('href', '').lower():
                                parent = a_tag.parent
                                if parent:
                                    for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b']):
                                        tag_text = tag.get_text(strip=True)
                                        if tag_text and '@' not in tag_text and len(tag_text) > 2:
                                            name = tag_text
                                            break
                        results.append({
                            'email': email,
                            'name': name,
                            'department': dept,
                            'source_url': final_url or url,
                        })

            # Also check pagination
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                full_url = urljoin(final_url or url, href)
                if re.search(r'[?&]page=\d+', full_url):
                    log(f"    Pagination: {full_url}")
                    pg_soup, _ = get_soup(full_url, session)
                    if pg_soup:
                        pg_text = pg_soup.get_text(separator=' ', strip=True)
                        pg_emails = extract_uga_emails(pg_text)
                        pg_mailto = extract_mailto_emails(pg_soup)
                        pg_cf = extract_cloudflare_emails(pg_soup, 'uga.edu')
                        for email in list(set(pg_emails + pg_mailto + pg_cf)):
                            if email not in seen_emails and not is_admin_email(email):
                                seen_emails.add(email)
                                results.append({
                                    'email': email,
                                    'name': '',
                                    'department': dept,
                                    'source_url': full_url,
                                })
                    time.sleep(0.3)

            time.sleep(0.5)

        dept_count = len([r for r in results if r['department'] == dept])
        log(f"  TOTAL for {dept}: {dept_count} emails")

    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_new_results = []
    global_seen = set()

    # Load existing results to avoid duplicates
    existing_csv = '/Users/jaiashar/Documents/VoraBusinessFinder/uga_dept_emails.csv'
    try:
        with open(existing_csv, 'r') as f:
            reader = csv.DictReader(f)
            existing_results = list(reader)
            for r in existing_results:
                if r.get('email'):
                    global_seen.add(r['email'].lower().strip())
        log(f"Loaded {len(existing_results)} existing results ({len(global_seen)} unique emails)")
    except FileNotFoundError:
        existing_results = []
        log("No existing results file found, starting fresh")

    def add_results(new_results):
        count = 0
        for r in new_results:
            email = r['email'].lower().strip()
            if email and email not in global_seen:
                global_seen.add(email)
                all_new_results.append(r)
                count += 1
        return count

    log("\n" + "=" * 70)
    log("UGA SUPPLEMENTAL SCRAPER")
    log("=" * 70)

    # Phase 1: Engineering (Cloudflare decode)
    eng_results = scrape_engineering(session)
    n = add_results(eng_results)
    log(f"  => {n} new Engineering emails")

    # Phase 2: Terry Business PhD
    terry_results = scrape_terry_phd(session)
    n = add_results(terry_results)
    log(f"  => {n} new Terry PhD emails")

    # Phase 3: Athletics
    athletics_results = scrape_athletics(session)
    n = add_results(athletics_results)
    log(f"  => {n} new Athletics emails")

    # Phase 4: Alternate department URLs
    alt_results = scrape_alternate_departments(session)
    n = add_results(alt_results)
    log(f"  => {n} new emails from alternate URLs")

    # Merge with existing
    combined = existing_results + all_new_results
    # Deduplicate
    final_seen = set()
    final_results = []
    for r in combined:
        email = r.get('email', '').lower().strip()
        if email and email not in final_seen:
            final_seen.add(email)
            final_results.append(r)

    log(f"\n\n{'=' * 70}")
    log(f"SUPPLEMENTAL RESULTS")
    log(f"{'=' * 70}")
    log(f"New emails found: {len(all_new_results)}")
    log(f"Total unique emails (combined): {len(final_results)}")

    # Save combined CSV
    with open(existing_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in final_results:
            writer.writerow(r)
    log(f"\nSaved combined results to {existing_csv}")

    # Save combined JSON
    output_json = '/Users/jaiashar/Documents/VoraBusinessFinder/uga_dept_emails.json'
    with open(output_json, 'w') as f:
        json.dump(final_results, f, indent=2)
    log(f"Saved combined results to {output_json}")

    # Print summary by department
    log(f"\n{'=' * 70}")
    log("UPDATED SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in final_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")

    # Print new emails
    if all_new_results:
        log(f"\n{'=' * 70}")
        log("NEW EMAILS FOUND:")
        log(f"{'=' * 70}")
        for r in all_new_results:
            log(f"  {r['email']:<40} | {r['name']:<30} | {r['department']}")

    return final_results


if __name__ == '__main__':
    main()
