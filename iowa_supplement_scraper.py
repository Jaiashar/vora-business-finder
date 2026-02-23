#!/usr/bin/env python3
"""
Iowa Supplement Scraper - Fixes departments that failed in the first pass
due to incorrect subdomain URLs.

Correct URLs discovered:
- Political Science: politicalscience.uiowa.edu (not polisci)
- Earth & Environmental: sees.uiowa.edu (not geoscience)
- Communication Studies: communicationstudies.uiowa.edu (not communication-studies)
- Engineering depts: engineering.uiowa.edu/ece-people, /me-people, /cee/cee-people, /bme/people, /cbe/people
- Education: education.uiowa.edu/directory
- Pharmacy: pharmacy.uiowa.edu/people
- UISG: usg.uiowa.edu/people (not uisg)
- Daily Iowan: dailyiowan.com/staff_name/di-staff/ and /contact/
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

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/iowa_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/iowa_dept_emails.json'


# Supplement departments with corrected URLs
SUPPLEMENT_DEPARTMENTS = [
    {
        "department": "Political Science",
        "urls": [
            "https://politicalscience.uiowa.edu/people",
            "https://politicalscience.uiowa.edu/people/graduate-students",
            "https://politicalscience.uiowa.edu/people/faculty",
        ],
    },
    {
        "department": "Earth & Environmental Sciences",
        "urls": [
            "https://sees.uiowa.edu/people/graduate-students",
            "https://sees.uiowa.edu/people",
            "https://ees.uiowa.edu/people/graduate-students",
            "https://ees.uiowa.edu/people",
        ],
    },
    {
        "department": "Communication Studies",
        "urls": [
            "https://communicationstudies.uiowa.edu/people",
            "https://communicationstudies.uiowa.edu/people/graduate-students",
            "https://communicationstudies.uiowa.edu/people/faculty",
        ],
    },
    {
        "department": "Economics",
        "urls": [
            "https://tippie.uiowa.edu/people/economics",
            "https://tippie.uiowa.edu/economics",
            "https://tippie.uiowa.edu/phd/phd-economics",
        ],
    },
    {
        "department": "Electrical & Computer Engineering",
        "urls": [
            "https://engineering.uiowa.edu/ece-people",
            "https://engineering.uiowa.edu/ece",
        ],
    },
    {
        "department": "Mechanical Engineering",
        "urls": [
            "https://engineering.uiowa.edu/me-people",
            "https://engineering.uiowa.edu/me",
        ],
    },
    {
        "department": "Civil & Environmental Engineering",
        "urls": [
            "https://engineering.uiowa.edu/cee/cee-people",
            "https://engineering.uiowa.edu/cee",
        ],
    },
    {
        "department": "Biomedical Engineering",
        "urls": [
            "https://engineering.uiowa.edu/bme/people/bme-graduate-students",
            "https://engineering.uiowa.edu/bme/people",
            "https://engineering.uiowa.edu/bme",
        ],
    },
    {
        "department": "Chemical & Biochemical Engineering",
        "urls": [
            "https://engineering.uiowa.edu/cbe/people",
            "https://engineering.uiowa.edu/cbe",
        ],
    },
    {
        "department": "College of Education",
        "urls": [
            "https://education.uiowa.edu/directory",
            "https://education.uiowa.edu/directory?cohort=519554",
        ],
    },
    {
        "department": "College of Pharmacy",
        "urls": [
            "https://pharmacy.uiowa.edu/people",
        ],
    },
    {
        "department": "University of Iowa Student Government (USG)",
        "urls": [
            "https://usg.uiowa.edu/people",
            "https://usg.uiowa.edu/people?page=1",
            "https://usg.uiowa.edu/about-us/contact-usg",
        ],
    },
    {
        "department": "The Daily Iowan (Student Newspaper)",
        "urls": [
            "https://dailyiowan.com/staff_name/di-staff/",
            "https://dailyiowan.com/contact/",
        ],
    },
]


def extract_uiowa_emails(text):
    """Extract all @uiowa.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*uiowa\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*uiowa\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract @uiowa.edu emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(
                r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uiowa\.edu)',
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
        'noreply@', 'do-not-reply@', 'donotreply@',
        'president@', 'provost@', 'gradschool@', 'gradstudies@',
        'law-admissions@', 'law-career@',
        'athletics@', 'compliance@', 'titleix@', 'title-ix@',
        'conduct@', 'counseling@', 'disability@', 'veterans@',
        'international@', 'multicultural@', 'equity@',
        'clas@', 'coe@', 'cph@',
        'tippie@', 'pharmacy@', 'nursing@',
        'ask-', 'its-helpdesk@', 'its@',
        'hawkeyesports@', 'uifoundation@',
        'ui-usg@', 'usg-', 'usg@',
        'con-', 'nursing-',
        'sociology@', 'sociology-',
        'actuarial-science@',
        'writers-workshop@',
        'uss-sociology@',
        'tippie-phd@',
        'pharmacy-',
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
            log(f"    HTTP {resp.status_code}")
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
                            'research', 'iowa', 'uiowa',
                        ]):
                            return tag_text
                parent = parent.parent

    email_elements = soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE))
    for elem in email_elements:
        parent = elem.parent
        for _ in range(6):
            if parent is None:
                break
            name_tags = parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a', 'span'])
            for tag in name_tags:
                name = tag.get_text(strip=True)
                if name and '@' not in name and len(name) > 2 and len(name) < 80:
                    if not any(x in name.lower() for x in [
                        'email', 'contact', '@', 'student', 'people', 'phone',
                        'read more', 'department', 'faculty', 'office', 'http',
                        'website', 'iowa', 'uiowa',
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
        '.field-content',
        'article',
        '.node--type-person',
        '.person-row',
        'tr',
        'li.leaf',
        '.vcard',
        '.grid-item',
        '.teaser',
        '.faculty-staff',
        '.person-listing',
        '.bio-block',
        '.staff-member',
        '.team-member',
        '.people-item',
        '.faculty-card',
        '.person-card',
        '.people-card',
        '.people-listing__person',
        '.view-people .view-content > div',
        '.view-directory .view-content > div',
    ]

    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_uiowa_emails(text)
                mailto_emails = extract_mailto_emails(card)
                all_emails = list(set(emails + mailto_emails))

                for email in all_emails:
                    if email in seen_emails or is_admin_email(email):
                        continue
                    seen_emails.add(email)

                    name = ""
                    for tag in card.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a', 'span']):
                        tag_text = tag.get_text(strip=True)
                        if tag_text and '@' not in tag_text and len(tag_text) > 2 and len(tag_text) < 80:
                            if not any(x in tag_text.lower() for x in [
                                'email', 'contact', 'phone', 'http', 'department',
                                'graduate', 'student', 'people', 'faculty', 'office',
                                'read more', 'view profile', 'research', 'website',
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

        if re.search(r'/(people|directory)/[a-z][\w-]+/?$', full_url, re.IGNORECASE):
            name = a_tag.get_text(strip=True)
            if name and '@' not in name and len(name) > 2 and len(name) < 80:
                if not any(x in name.lower() for x in [
                    'graduate', 'student', 'people', 'faculty', 'all',
                    'home', 'search', 'more', 'view', 'page', 'next',
                    'previous', 'department', 'staff', 'back'
                ]):
                    seen_urls.add(full_url)
                    profiles.append({'name': name, 'profile_url': full_url})

    return profiles


def scrape_profile_page(url, session):
    """Scrape an individual profile page for uiowa.edu email."""
    soup, final_url = get_soup(url, session)
    if soup is None:
        return None

    emails = extract_mailto_emails(soup)
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_uiowa_emails(text)
    all_emails = list(set(emails + text_emails))
    personal = [e for e in all_emails if not is_admin_email(e)]
    return personal[0] if personal else None


def scrape_department(config, session):
    """Scrape a single department."""
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
            time.sleep(0.5)
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
        text_emails = extract_uiowa_emails(page_text)
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

        # Strategy 3: Obfuscated emails
        obfuscated = re.findall(
            r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*uiowa\.edu)',
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

        # Strategy 4: JavaScript-embedded emails
        for script in soup.find_all('script'):
            if script.string:
                script_emails = extract_uiowa_emails(script.string)
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
                pg_text_emails = extract_uiowa_emails(pg_text)
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

        # Strategy 6: If few emails, try visiting profile links
        if len(results) < 3:
            profiles = find_profile_links(soup, successful_url)
            if profiles:
                log(f"    -> Found {len(profiles)} profile links, visiting for emails...")
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

        if len(results) > 0:
            log(f"    -> Found {len(results)} emails so far")

        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    for r in results:
        log(f"    {r['email']:<45} | {r['name']}")

    return results


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
        log(f"Loaded {len(existing_results)} existing results ({len(existing_emails)} unique emails)")
    except FileNotFoundError:
        log("No existing results found, starting fresh")

    log("=" * 70)
    log("UNIVERSITY OF IOWA - SUPPLEMENT SCRAPER")
    log("Fixing departments with incorrect URLs")
    log("=" * 70)

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
            log(f"  => Running total of new emails: {new_count}")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR scraping {config['department']}: {e}")

    log(f"\n\n{'=' * 70}")
    log(f"SUPPLEMENT RESULTS")
    log(f"{'=' * 70}")
    log(f"New unique emails added: {new_count}")
    log(f"Total unique @uiowa.edu emails: {len(existing_results)}")

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

    return existing_results


if __name__ == '__main__':
    main()
