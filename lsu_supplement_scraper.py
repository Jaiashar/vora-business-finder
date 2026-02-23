#!/usr/bin/env python3
"""
LSU Supplemental Scraper - targets correct URLs for departments that returned 0 in first pass.
Many LSU departments restructured their websites and use /faculty/, /directory/, etc.
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/lsu_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/lsu_dept_emails.json'


def extract_lsu_emails(text):
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*lsu\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*lsu\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*lsu\.edu)',
                href, re.IGNORECASE
            )
            if match:
                emails.append(match.group(1).lower().strip())
    return list(set(emails))


def is_admin_email(email):
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
        'testing@', 'counseling@', 'housing@', 'parking@', 'transit@',
        'police@', 'records@', 'bursar@', 'payroll@', 'noreply@',
        'do-not-reply@', 'donotreply@', 'enrollment@',
        'dining@', 'athletics@', 'compliance@', 'sports@',
        'gradstudies@', 'psychology@', 'chemistry@', 'physics@',
        'math@', 'biology@', 'geology@', 'english@', 'history@',
        'sociology@', 'communication@', 'philosophy@', 'polisci@',
        'geography@', 'manship@', 'education@', 'business@',
        'sg@', 'greeks@', 'lsusga@', 'lsumanship@', 'lsulaw@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


def get_soup(url, session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        else:
            return None, None
    except Exception as e:
        log(f"    Error: {e}")
        return None, None


def try_get_name_for_email(soup, email):
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
                            'skip to', 'toggle', 'search', 'menu',
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
                        'read more', 'department', 'faculty', 'office', 'http',
                        'skip to', 'toggle', 'search', 'menu',
                    ]):
                        return name
            parent = parent.parent

    return ""


def extract_from_person_cards(soup, url, department):
    results = []
    seen_emails = set()

    person_selectors = [
        '.views-row', '[class*="person"]', '[class*="profile"]',
        '[class*="people"]', '[class*="member"]', '[class*="student"]',
        '[class*="card"]', '[class*="directory"]', '.field-content',
        'article', 'tr', 'li', '.vcard', '.grid-item', '.teaser',
        '.faculty-staff', '.people-listing', '.staff-listing',
        '.ppl-lister__card', '.ppl-card', '.people-grid__item',
        '.listing-item', '.faculty-item', '.person-entry',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_lsu_emails(text)
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
                                'read more', 'view profile', 'skip to',
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


def find_pagination_urls(soup, base_url):
    pages = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        full_url = urljoin(base_url, href)
        if re.search(r'[?&]page=\d+', full_url):
            pages.add(full_url)
        elif re.search(r'/page/\d+/?$', full_url):
            pages.add(full_url)
    return sorted(pages)


def find_profile_links(soup, base_url, domain_filter='lsu.edu'):
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
        if domain_filter not in full_url:
            continue

        # Match profile-like URLs
        profile_patterns = [
            r'/faculty/faculty/[\w-]+\.php$',
            r'/faculty/instructors/[\w-]+\.php$',
            r'/people/[\w-]+\.php$',
            r'/directory/[\w-]+\.php$',
            r'/faculty/[\w-]+\.php$',
            r'/grad[_-]?students?/[\w-]+\.php$',
        ]

        for pattern in profile_patterns:
            if re.search(pattern, full_url, re.IGNORECASE):
                name = a_tag.get_text(strip=True)
                if name and '@' not in name and len(name) > 2 and len(name) < 80:
                    if not any(x in name.lower() for x in [
                        'graduate', 'student', 'people', 'faculty', 'all',
                        'home', 'search', 'more', 'view', 'page', 'next',
                        'previous', 'department', 'directory', 'about',
                        'index', 'back', 'skip', 'toggle', 'menu',
                    ]):
                        if full_url not in seen_urls:
                            seen_urls.add(full_url)
                            profiles.append({'name': name, 'profile_url': full_url})
                break

    return profiles


def scrape_profile_page(url, session):
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None
    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_lsu_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


def scrape_department(config, session):
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
            time.sleep(0.3)
            continue

        all_pages_scraped.add(url)
        if final_url:
            all_pages_scraped.add(final_url)

        successful_url = final_url or url
        log(f"    -> Loaded ({successful_url})")

        # Card extraction
        card_results = extract_from_person_cards(soup, successful_url, department)
        for r in card_results:
            if r['email'] not in seen_emails:
                seen_emails.add(r['email'])
                results.append(r)

        # Full text extraction
        page_text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_lsu_emails(page_text)
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

        # JavaScript emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_lsu_emails(script.string)
                for email in script_emails:
                    if email not in seen_emails and not is_admin_email(email):
                        seen_emails.add(email)
                        results.append({
                            'email': email,
                            'name': '',
                            'department': department,
                            'source_url': successful_url,
                        })

        # Pagination
        pagination_urls = find_pagination_urls(soup, successful_url)
        if pagination_urls:
            log(f"    -> Found {len(pagination_urls)} additional pages")
            for page_url in pagination_urls:
                if page_url in all_pages_scraped:
                    continue
                all_pages_scraped.add(page_url)
                page_soup, page_final = get_soup(page_url, session)
                if page_soup is None:
                    continue
                pg_text = page_soup.get_text(separator=' ', strip=True)
                pg_emails = extract_lsu_emails(pg_text)
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

        # Profile links (if few emails found on current page)
        if len(results) < 5:
            profiles = find_profile_links(soup, successful_url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting...")
                for profile in profiles[:60]:
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
                    time.sleep(0.3)

        if results:
            log(f"    -> {len(results)} emails so far")

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    for r in results:
        log(f"    {r['email']:<45} | {r['name']}")

    return results


# ============================================================
# SUPPLEMENTAL DEPARTMENT CONFIGURATIONS
# These use the correct URL patterns found by inspection
# ============================================================

SUPPLEMENT_DEPARTMENTS = [
    # English - has /faculty/index.php with all faculty + instructor emails
    {"department": "English", "urls": [
        "https://www.lsu.edu/hss/english/faculty/index.php",
        "https://www.lsu.edu/hss/english/graduate_program/current_students.php",
        "https://www.lsu.edu/hss/english/graduate_program/graduate_students.php",
    ]},

    # Psychology - has /faculty/index.php but no individual email on listing
    {"department": "Psychology", "urls": [
        "https://www.lsu.edu/hss/psychology/faculty/index.php",
        "https://www.lsu.edu/hss/psychology/faculty/clinical/index.php",
        "https://www.lsu.edu/hss/psychology/faculty/cbs/index.php",
        "https://www.lsu.edu/hss/psychology/faculty/ba/index.php",
        "https://www.lsu.edu/hss/psychology/faculty/io/index.php",
        "https://www.lsu.edu/hss/psychology/faculty/school/index.php",
        "https://www.lsu.edu/hss/psychology/grad/clinical/current-students.php",
        "https://www.lsu.edu/hss/psychology/grad/cbs/current-students.php",
        "https://www.lsu.edu/hss/psychology/grad/io/current-students.php",
        "https://www.lsu.edu/hss/psychology/grad/school/current-students.php",
        "https://www.lsu.edu/hss/psychology/grad/ba/current-students.php",
    ]},

    # Political Science
    {"department": "Political Science", "urls": [
        "https://www.lsu.edu/hss/polisci/faculty_staff/index.php",
        "https://www.lsu.edu/hss/polisci/student_resources/graduate_programs/index.php",
        "https://www.lsu.edu/hss/polisci/student_resources/graduate_programs/graduate_students.php",
        "https://www.lsu.edu/hss/polisci/faculty_staff/faculty.php",
    ]},

    # Economics - main dept page is 404, may have moved
    {"department": "Economics", "urls": [
        "https://www.lsu.edu/business/economics/",
        "https://www.lsu.edu/business/economics/people/",
        "https://www.lsu.edu/business/economics/faculty/",
        "https://business.lsu.edu/economics/",
        "https://business.lsu.edu/economics/people/",
        "https://business.lsu.edu/academics/economics/",
    ]},

    # Philosophy
    {"department": "Philosophy", "urls": [
        "https://www.lsu.edu/hss/phil/",
        "https://www.lsu.edu/hss/phil/people/",
        "https://www.lsu.edu/hss/phil/faculty/",
        "https://www.lsu.edu/hss/philosophy/faculty/index.php",
        "https://www.lsu.edu/hss/philosophy/faculty/",
    ]},

    # Geography & Anthropology (often combined at LSU)
    {"department": "Geography & Anthropology", "urls": [
        "https://www.lsu.edu/hss/geography/faculty_staff/index.php",
        "https://www.lsu.edu/hss/ga/",
        "https://www.lsu.edu/hss/ga/people/",
        "https://www.lsu.edu/hss/ga/faculty/",
        "https://www.lsu.edu/hss/geography/faculty/index.php",
    ]},

    # Communication Studies
    {"department": "Communication Studies", "urls": [
        "https://www.lsu.edu/hss/cmst/",
        "https://www.lsu.edu/hss/cmst/people/",
        "https://www.lsu.edu/hss/cmst/faculty/",
        "https://www.lsu.edu/hss/communication/faculty/index.php",
    ]},

    # Mathematics
    {"department": "Mathematics", "urls": [
        "https://www.math.lsu.edu/",
        "https://www.math.lsu.edu/grad/grad_students",
        "https://www.math.lsu.edu/dept/grad_students",
        "https://www.math.lsu.edu/faculty",
        "https://www.math.lsu.edu/dept/faculty",
        "https://www.math.lsu.edu/people",
    ]},

    # Physics & Astronomy
    {"department": "Physics & Astronomy", "urls": [
        "https://www.lsu.edu/physics/",
        "https://www.lsu.edu/physics/people/",
        "https://www.lsu.edu/physics/people/graduate-students.php",
        "https://www.lsu.edu/physics/people/grad-students.php",
        "https://www.lsu.edu/physics/faculty/",
        "https://www.lsu.edu/physics/directory/",
        "https://www.phys.lsu.edu/",
        "https://www.phys.lsu.edu/people/",
        "https://www.phys.lsu.edu/faculty/",
    ]},

    # Chemistry
    {"department": "Chemistry", "urls": [
        "https://www.lsu.edu/science/chemistry/people/",
        "https://www.lsu.edu/science/chemistry/people/index.php",
        "https://www.lsu.edu/science/chemistry/people/faculty/index.php",
        "https://www.lsu.edu/science/chemistry/people/grad_students.php",
        "https://www.lsu.edu/science/chemistry/people/grad-students.php",
        "https://www.lsu.edu/science/chemistry/people/graduate_students.php",
        "https://www.lsu.edu/science/chemistry/people/graduate-students/index.php",
        "https://www.lsu.edu/science/chemistry/academics/graduate/current-students.php",
    ]},

    # Biological Sciences
    {"department": "Biological Sciences", "urls": [
        "https://www.lsu.edu/science/biosci/",
        "https://www.lsu.edu/science/biosci/people/",
        "https://www.lsu.edu/science/biosci/people/index.php",
        "https://www.lsu.edu/science/biosci/faculty/",
        "https://www.lsu.edu/science/biosci/people/faculty.php",
        "https://www.lsu.edu/science/biosci/people/grad-students.php",
    ]},

    # Geology & Geophysics (got 2 emails, let's expand)
    {"department": "Geology & Geophysics", "urls": [
        "https://www.lsu.edu/science/geology/people/faculty.php",
        "https://www.lsu.edu/science/geology/people/staff.php",
        "https://www.lsu.edu/science/geology/people/graduate-students.php",
    ]},

    # Computer Science & Engineering
    {"department": "Computer Science & Engineering", "urls": [
        "https://www.lsu.edu/eng/cse/people/",
        "https://www.lsu.edu/eng/cse/people/index.php",
        "https://www.lsu.edu/eng/cse/people/faculty.php",
        "https://www.lsu.edu/eng/cse/people/grad-students.php",
        "https://www.lsu.edu/eng/cse/faculty/",
    ]},

    # Civil & Environmental Engineering
    {"department": "Civil & Environmental Engineering", "urls": [
        "https://www.lsu.edu/eng/cee/people/",
        "https://www.lsu.edu/eng/cee/people/index.php",
        "https://www.lsu.edu/eng/cee/people/faculty.php",
        "https://www.lsu.edu/eng/cee/people/grad-students.php",
        "https://www.lsu.edu/eng/cee/faculty/",
        "https://www.lsu.edu/eng/civil/people/grad-students.php",
    ]},

    # Chemical Engineering
    {"department": "Chemical Engineering", "urls": [
        "https://www.lsu.edu/eng/che/people/",
        "https://www.lsu.edu/eng/che/people/index.php",
        "https://www.lsu.edu/eng/che/people/faculty.php",
        "https://www.lsu.edu/eng/che/people/grad-students.php",
        "https://www.lsu.edu/eng/che/faculty/",
    ]},

    # College of Education (trying alternate paths)
    {"department": "College of Education", "urls": [
        "https://www.lsu.edu/chse/",
        "https://www.lsu.edu/chse/people/",
        "https://www.lsu.edu/chse/directory/",
        "https://www.lsu.edu/chse/education/",
        "https://www.lsu.edu/education/",
        "https://www.lsu.edu/education/people/",
        "https://education.lsu.edu/",
        "https://education.lsu.edu/people/",
        "https://education.lsu.edu/people/students/",
    ]},

    # Manship School of Mass Communication (expand)
    {"department": "Manship School of Mass Communication", "urls": [
        "https://www.lsu.edu/manship/people/index.php",
        "https://www.lsu.edu/manship/people/faculty.php",
        "https://www.lsu.edu/manship/people/faculty/index.php",
        "https://www.lsu.edu/manship/people/staff.php",
        "https://www.lsu.edu/manship/people/grad-students.php",
        "https://www.lsu.edu/manship/people/graduate_students.php",
        "https://www.lsu.edu/manship/directory/",
    ]},

    # The Reveille - try alternate URLs
    {"department": "The Reveille (Student Newspaper)", "urls": [
        "https://www.lsureveille.com/site/about.html",
        "https://lsureveille.com/site/about.html",
    ]},
]


def main():
    session = requests.Session()

    # Load existing results
    existing_results = []
    existing_emails = set()
    try:
        with open(OUTPUT_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_results.append(row)
                existing_emails.add(row['email'].lower().strip())
        log(f"Loaded {len(existing_results)} existing emails from {OUTPUT_CSV}")
    except FileNotFoundError:
        log(f"No existing file found at {OUTPUT_CSV}")

    new_count = 0

    for config in SUPPLEMENT_DEPARTMENTS:
        try:
            results = scrape_department(config, session)
            for r in results:
                email = r['email'].lower().strip()
                if email and email not in existing_emails:
                    existing_emails.add(email)
                    existing_results.append(r)
                    new_count += 1
            log(f"  => Running new total: {new_count} new emails")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR: {e}")

    log(f"\n{'=' * 70}")
    log(f"SUPPLEMENT RESULTS")
    log(f"{'=' * 70}")
    log(f"New unique emails added: {new_count}")
    log(f"Total unique emails now: {len(existing_results)}")

    # Save merged CSV
    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(existing_results, key=lambda x: (x['department'], x['email'])):
            writer.writerow(r)
    log(f"\nSaved to {OUTPUT_CSV}")

    # Save merged JSON
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(existing_results, f, indent=2)
    log(f"Saved to {OUTPUT_JSON}")

    # Summary by department
    log(f"\n{'=' * 70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in existing_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")


if __name__ == '__main__':
    main()
