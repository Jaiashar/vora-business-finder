#!/usr/bin/env python3
"""
Stanford School of Engineering Email Scraper
Scrapes @stanford.edu emails from:
1. profiles.stanford.edu/browse (graduate students by department)
2. Research lab people pages
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
from urllib.parse import urljoin

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def extract_stanford_emails_from_text(text):
    """Extract all @stanford.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*stanford\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*stanford\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def is_admin_email(email):
    """Check if email is an administrative/generic email."""
    admin_patterns = [
        'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
        'support@', 'contact@', 'registrar@', 'admissions@',
        'advising@', 'dean@', 'chair@', 'reception@', 'enroll@',
        'communications@', 'department@', 'services@', 'affairs@',
        'news@', 'events@', 'postmaster@', 'root@', 'abuse@',
        'noreply@', 'no-reply@', 'www@', 'feedback@', 'security@',
        'privacy@', 'computing@', 'helpsu@', 'helpdesk@', 'action@',
        'affiliates@', 'careers@', 'titleix@', 'ombuds@', 'council@',
        'undergrad@', 'future@', 'summer@', 'apply@',
        'contact-', 'neuro_', 'human_neuro@',
    ]
    for p in admin_patterns:
        if p in email:
            return True
    # Filter list/group emails
    if '@lists.' in email or '@cs-' in email:
        return True
    return False


def scrape_profiles_browse(session):
    """
    Scrape profiles.stanford.edu/browse for School of Engineering graduate students.
    This is the main source - has structured data with names, departments, and emails.
    """
    results = []
    seen_emails = set()

    base_url = 'https://profiles.stanford.edu/browse/school-of-engineering'

    # Graduate students filter
    affiliations = 'capMdStudent,capMsStudent,capPhdStudent'

    # First, get total count
    url = f'{base_url}?affiliations={affiliations}&p=1&ps=100'
    r = session.get(url, headers=HEADERS, timeout=20)
    soup = BeautifulSoup(r.text, 'html.parser')
    total_text = soup.get_text()
    count_match = re.search(r'(\d+)\s*Results?', total_text)
    total = int(count_match.group(1)) if count_match else 0
    print(f"Total graduate students in School of Engineering: {total}")

    total_pages = (total // 100) + 1 if total > 0 else 1

    for page in range(1, total_pages + 2):  # +2 for safety
        url = f'{base_url}?affiliations={affiliations}&p={page}&ps=100'
        print(f"\n  Page {page}/{total_pages}: {url}")

        try:
            r = session.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                print(f"    HTTP {r.status_code}")
                continue

            soup = BeautifulSoup(r.text, 'html.parser')

            # Extract structured data from mini-profile-holder divs
            profiles = soup.select('.mini-profile-holder')
            page_count = 0

            for profile in profiles:
                text = profile.get_text(separator='\n', strip=True)

                # Extract email
                emails = extract_stanford_emails_from_text(text)
                if not emails:
                    # Also check mailto links
                    for a_tag in profile.find_all('a', href=True):
                        href = a_tag.get('href', '')
                        if 'mailto:' in href:
                            em = re.search(r'mailto:([\w.+-]+@[\w.-]*stanford\.edu)', href, re.IGNORECASE)
                            if em:
                                emails.append(em.group(1).lower())

                if not emails:
                    continue

                email = emails[0]
                if email in seen_emails or is_admin_email(email):
                    continue

                # Extract name - first heading or strong text
                name = ""
                for tag in profile.find_all(['h2', 'h3', 'h4', 'strong', 'b', 'a']):
                    tag_text = tag.get_text(strip=True)
                    if tag_text and '@' not in tag_text and len(tag_text) > 2:
                        words = tag_text.split()
                        if 1 <= len(words) <= 6:
                            if not any(x in tag_text.lower() for x in
                                       ['email', 'contact', 'phone', 'edit', 'view',
                                        'sign in', 'search', 'browse', 'profile']):
                                name = tag_text
                                break

                # Extract department from text
                department = "School of Engineering"
                dept_patterns = [
                    (r'(?:Ph\.?D\.?|Doctoral)\s+Student\s+in\s+([^,\n]+)', 'PhD'),
                    (r'Masters?\s+Student\s+in\s+([^,\n]+)', 'MS'),
                    (r'Student\s+in\s+([^,\n]+)', 'Student'),
                ]
                for pattern, _ in dept_patterns:
                    m = re.search(pattern, text, re.IGNORECASE)
                    if m:
                        department = m.group(1).strip()
                        # Clean up department name
                        department = re.sub(r',?\s*admitted\s+.*$', '', department, flags=re.IGNORECASE)
                        break

                seen_emails.add(email)
                page_count += 1
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': url
                })

            print(f"    Extracted {page_count} emails (total so far: {len(results)})")

            if page_count == 0 and page > 1:
                print("    No more results, stopping pagination.")
                break

            time.sleep(0.7)

        except Exception as e:
            print(f"    Error: {type(e).__name__}: {e}")
            continue

    # Also scrape by department for completeness (some may only appear in dept views)
    dept_orgs = [
        ('school-of-engineering/computer-science', 'Computer Science'),
        ('school-of-engineering/electrical-engineering', 'Electrical Engineering'),
        ('school-of-engineering/mechanical-engineering', 'Mechanical Engineering'),
        ('school-of-engineering/civil-and-environmental-engineering', 'Civil & Environmental Engineering'),
        ('school-of-engineering/materials-science-and-engineering', 'Materials Science & Engineering'),
        ('school-of-engineering/chemical-engineering', 'Chemical Engineering'),
        ('school-of-engineering/aeronautics-and-astronautics', 'Aeronautics & Astronautics'),
        ('school-of-engineering/bioengineering', 'Bioengineering'),
        ('school-of-engineering/management-science-and-engineering', 'Management Science & Engineering'),
        ('school-of-engineering/programs-centers-and-institutes', 'Programs, Centers & Institutes'),
    ]

    print(f"\n\n  Scraping by department...")
    for org_path, dept_name in dept_orgs:
        for page in range(1, 20):
            url = f'{base_url}?org={org_path}&affiliations={affiliations}&p={page}&ps=100'

            try:
                r = session.get(url, headers=HEADERS, timeout=20)
                if r.status_code != 200:
                    break

                soup = BeautifulSoup(r.text, 'html.parser')
                profiles = soup.select('.mini-profile-holder')
                page_new = 0

                for profile in profiles:
                    text = profile.get_text(separator='\n', strip=True)
                    emails = extract_stanford_emails_from_text(text)
                    if not emails:
                        for a_tag in profile.find_all('a', href=True):
                            href = a_tag.get('href', '')
                            if 'mailto:' in href:
                                em = re.search(r'mailto:([\w.+-]+@[\w.-]*stanford\.edu)', href, re.IGNORECASE)
                                if em:
                                    emails.append(em.group(1).lower())

                    if not emails:
                        continue

                    email = emails[0]
                    if email in seen_emails or is_admin_email(email):
                        continue

                    name = ""
                    for tag in profile.find_all(['h2', 'h3', 'h4', 'strong', 'b', 'a']):
                        tag_text = tag.get_text(strip=True)
                        if tag_text and '@' not in tag_text and len(tag_text) > 2:
                            words = tag_text.split()
                            if 1 <= len(words) <= 6:
                                if not any(x in tag_text.lower() for x in
                                           ['email', 'contact', 'phone', 'edit', 'view',
                                            'sign in', 'search', 'browse', 'profile']):
                                    name = tag_text
                                    break

                    # Use the department from the org filter
                    department = dept_name

                    # Try to refine from profile text
                    for pattern in [r'(?:Ph\.?D\.?|Doctoral)\s+Student\s+in\s+([^,\n]+)',
                                    r'Masters?\s+Student\s+in\s+([^,\n]+)']:
                        m = re.search(pattern, text, re.IGNORECASE)
                        if m:
                            dept_from_text = m.group(1).strip()
                            dept_from_text = re.sub(r',?\s*admitted\s+.*$', '', dept_from_text, flags=re.IGNORECASE)
                            if dept_from_text:
                                department = dept_from_text
                            break

                    seen_emails.add(email)
                    page_new += 1
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': url
                    })

                print(f"    {dept_name} page {page}: {page_new} new emails")

                if len(profiles) == 0:
                    break

                time.sleep(0.5)

            except Exception as e:
                print(f"    Error on {dept_name} page {page}: {e}")
                break

    return results


def scrape_lab_page(url, department, session, visited_urls):
    """Scrape a research lab page for Stanford emails."""
    if url in visited_urls:
        return []
    visited_urls.add(url)

    results = []
    try:
        print(f"  Scraping: {url}")
        r = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            print(f"    HTTP {r.status_code}")
            return results

        soup = BeautifulSoup(r.text, 'html.parser')
        page_text = soup.get_text()

        # Extract emails from all sources
        text_emails = extract_stanford_emails_from_text(page_text)

        # Mailto links
        mailto_emails = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if 'mailto:' in href.lower():
                em = re.search(r'mailto:([\w.+-]+@[\w.-]*stanford\.edu)', href, re.IGNORECASE)
                if em:
                    mailto_emails.append(em.group(1).lower())

        # Script tags
        script_emails = []
        for script in soup.find_all('script'):
            if script.string:
                script_emails.extend(extract_stanford_emails_from_text(script.string))

        # Obfuscated
        for pattern_str in [r'([\w.+-]+)\s*\[at\]\s*([\w.-]*stanford\.edu)',
                            r'([\w.+-]+)\s*\(at\)\s*([\w.-]*stanford\.edu)']:
            for m in re.finditer(pattern_str, page_text, re.IGNORECASE):
                mailto_emails.append(f"{m.group(1)}@{m.group(2)}".lower())

        all_emails = list(set(text_emails + mailto_emails + script_emails))
        filtered = [e for e in all_emails if not is_admin_email(e)]

        for email in filtered:
            name = ""
            # Try mailto link text
            for a_tag in soup.find_all('a', href=True):
                if email in a_tag.get('href', '').lower():
                    link_text = a_tag.get_text(strip=True)
                    if link_text and '@' not in link_text and 2 < len(link_text) < 50:
                        words = link_text.split()
                        if 1 <= len(words) <= 5:
                            name = link_text
                            break

            if not name:
                # Try nearby headings
                for elem in soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE)):
                    parent = elem.parent
                    for _ in range(6):
                        if parent is None:
                            break
                        for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b']):
                            tag_text = tag.get_text(strip=True)
                            if tag_text and '@' not in tag_text:
                                words = tag_text.split()
                                if 1 <= len(words) <= 5:
                                    name = tag_text
                                    break
                        if name:
                            break
                        parent = parent.parent

            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': url
            })

        print(f"    Found {len(results)} emails")

    except requests.exceptions.ConnectionError:
        print(f"    Connection error")
    except requests.exceptions.Timeout:
        print(f"    Timeout")
    except Exception as e:
        print(f"    Error: {type(e).__name__}: {e}")

    return results


def scrape_research_labs(session, seen_emails):
    """Scrape known Stanford research lab pages."""
    results = []
    visited_urls = set()

    LAB_URLS = [
        # AI / ML Labs
        ("https://ai.stanford.edu/people/", "Stanford AI Lab (SAIL)"),
        ("https://nlp.stanford.edu/people/", "NLP Group"),
        ("https://nlp.stanford.edu/", "NLP Group"),
        ("https://svl.stanford.edu/people", "Stanford Vision Lab"),
        ("https://svl.stanford.edu/", "Stanford Vision Lab"),
        ("https://graphics.stanford.edu/", "Computer Graphics Lab"),
        ("https://graphics.stanford.edu/people/", "Computer Graphics Lab"),
        ("https://hci.stanford.edu/people/", "HCI Group"),
        ("https://hci.stanford.edu/", "HCI Group"),
        ("https://robotics.stanford.edu/", "Robotics Lab"),
        ("https://robotics.stanford.edu/people/", "Robotics Lab"),
        ("https://crfm.stanford.edu/about/people", "CRFM (Foundation Models)"),
        ("https://crfm.stanford.edu/", "CRFM (Foundation Models)"),
        ("https://stanfordmlgroup.github.io/", "ML Group"),
        ("https://stanfordmlgroup.github.io/people/", "ML Group"),
        ("https://hazyresearch.stanford.edu/", "Hazy Research"),
        ("https://dawn.cs.stanford.edu/", "DAWN Lab"),
        ("https://snap.stanford.edu/", "SNAP Group"),
        ("https://snap.stanford.edu/people.html", "SNAP Group"),
        ("https://theory.stanford.edu/", "Theory Group"),
        ("https://iliad.stanford.edu/", "ILIAD Lab"),
        ("https://asl.stanford.edu/", "Autonomous Systems Lab"),
        ("https://asl.stanford.edu/people/", "Autonomous Systems Lab"),
        ("https://seclab.stanford.edu/", "Security Lab"),
        ("https://seclab.stanford.edu/people/", "Security Lab"),

        # Faculty group pages (often list students)
        ("https://web.stanford.edu/~jurafsky/", "Jurafsky NLP Group"),
        ("https://cs.stanford.edu/~pliang/", "Percy Liang Group"),
        ("https://web.stanford.edu/~bohg/group.html", "Bohg Lab"),
        ("https://cs.stanford.edu/people/eroberts/", "Eric Roberts"),

        # Stanford web groups
        ("https://web.stanford.edu/group/pdplab/", "PDP Lab"),
        ("https://web.stanford.edu/group/pdplab/people.html", "PDP Lab"),
        ("https://web.stanford.edu/group/SOL/", "Systems Optimization Lab"),
        ("https://web.stanford.edu/group/sisl/", "SISL"),
        ("https://web.stanford.edu/group/ctr/", "Center for Turbulence"),
        ("https://web.stanford.edu/group/ctr/people.html", "Center for Turbulence"),
        ("https://web.stanford.edu/group/flow/", "Flow Physics Lab"),
        ("https://web.stanford.edu/group/nptl/", "Neural Prosthetics Lab"),

        # Other research centers
        ("https://energy.stanford.edu/people/", "Precourt Energy"),
        ("https://biox.stanford.edu/people/", "Bio-X"),
        ("https://datascience.stanford.edu/people/", "Stanford Data Science"),
        ("https://neuroscience.stanford.edu/people/", "Wu Tsai Neurosciences"),

        # Department research pages
        ("https://ee.stanford.edu/research/", "EE Research"),
        ("https://me.stanford.edu/research/", "ME Research"),
        ("https://cee.stanford.edu/research/", "CEE Research"),
        ("https://aa.stanford.edu/research/", "AA Research"),

        # Additional well-known labs
        ("https://stanfordnlp.github.io/", "Stanford NLP (GitHub)"),
        ("https://purl.stanford.edu/", "Stanford Digital Repository"),

        # Engineering department people pages (some render server-side)
        ("https://ee.stanford.edu/people", "Electrical Engineering"),
        ("https://me.stanford.edu/people", "Mechanical Engineering"),
        ("https://cee.stanford.edu/people", "Civil & Environmental Engineering"),
        ("https://aa.stanford.edu/people", "Aeronautics & Astronautics"),
        ("https://bioengineering.stanford.edu/people", "Bioengineering"),
        ("https://bioengineering.stanford.edu/people/phd-students", "Bioengineering"),
        ("https://bioengineering.stanford.edu/people/masters-students", "Bioengineering"),
        ("https://msande.stanford.edu/people", "Management Science & Engineering"),
        ("https://msande.stanford.edu/people/phd-students", "Management Science & Engineering"),

        # ICME
        ("https://icme.stanford.edu/", "ICME"),

        # Profiles browse for specific institutes
        ("https://profiles.stanford.edu/browse/school-of-engineering?org=school-of-engineering/programs-centers-and-institutes", "Programs & Institutes"),
    ]

    for url, department in LAB_URLS:
        lab_results = scrape_lab_page(url, department, session, visited_urls)
        for r in lab_results:
            email = r['email'].lower().strip()
            if email not in seen_emails:
                seen_emails.add(email)
                results.append(r)
        time.sleep(0.5)

    return results


def scrape_profiles_alpha(session, seen_emails):
    """
    Scrape profiles.stanford.edu by alphabetical letters for School of Engineering.
    This catches people who might not show up in the affiliation filter.
    """
    results = []
    base_url = 'https://profiles.stanford.edu/browse/school-of-engineering'

    print("\n  Scraping alphabetical listings...")
    for letter in 'abcdefghijklmnopqrstuvwxyz':
        for page in range(1, 15):
            url = f'{base_url}?name={letter}&p={page}&ps=100'
            try:
                r = session.get(url, headers=HEADERS, timeout=20)
                if r.status_code != 200:
                    break

                soup = BeautifulSoup(r.text, 'html.parser')
                profiles = soup.select('.mini-profile-holder')

                if not profiles:
                    break

                page_new = 0
                for profile in profiles:
                    text = profile.get_text(separator='\n', strip=True)

                    # Only interested in students
                    if not any(kw in text.lower() for kw in ['student', 'ph.d.', 'phd', 'masters', 'doctoral']):
                        continue

                    emails = extract_stanford_emails_from_text(text)
                    if not emails:
                        for a_tag in profile.find_all('a', href=True):
                            href = a_tag.get('href', '')
                            if 'mailto:' in href:
                                em = re.search(r'mailto:([\w.+-]+@[\w.-]*stanford\.edu)', href, re.IGNORECASE)
                                if em:
                                    emails.append(em.group(1).lower())

                    if not emails:
                        continue

                    email = emails[0]
                    if email in seen_emails or is_admin_email(email):
                        continue

                    name = ""
                    for tag in profile.find_all(['h2', 'h3', 'h4', 'strong', 'b', 'a']):
                        tag_text = tag.get_text(strip=True)
                        if tag_text and '@' not in tag_text and len(tag_text) > 2:
                            words = tag_text.split()
                            if 1 <= len(words) <= 6:
                                if not any(x in tag_text.lower() for x in
                                           ['email', 'contact', 'phone', 'edit', 'view',
                                            'sign in', 'search', 'browse', 'profile']):
                                    name = tag_text
                                    break

                    department = "School of Engineering"
                    for pattern in [r'(?:Ph\.?D\.?|Doctoral)\s+Student\s+in\s+([^,\n]+)',
                                    r'Masters?\s+Student\s+in\s+([^,\n]+)',
                                    r'Student\s+in\s+([^,\n]+)']:
                        m = re.search(pattern, text, re.IGNORECASE)
                        if m:
                            department = m.group(1).strip()
                            department = re.sub(r',?\s*admitted\s+.*$', '', department, flags=re.IGNORECASE)
                            break

                    seen_emails.add(email)
                    page_new += 1
                    results.append({
                        'email': email,
                        'name': name,
                        'department': department,
                        'source_url': url
                    })

                print(f"    Letter '{letter}' page {page}: {page_new} new (total new: {len(results)})")

                if page_new == 0 and page > 1:
                    break

                time.sleep(0.4)

            except Exception as e:
                print(f"    Error on letter '{letter}' page {page}: {e}")
                break

    return results


def main():
    all_results = []
    seen_emails = set()
    session = requests.Session()

    print("=" * 70)
    print("STANFORD SCHOOL OF ENGINEERING - EMAIL SCRAPER")
    print("=" * 70)

    # ==================== PHASE 1: Profiles Browse (Graduate Students) ====================
    print("\n\nPHASE 1: SCRAPING STANFORD PROFILES - GRADUATE STUDENTS")
    print("=" * 70)

    profiles_results = scrape_profiles_browse(session)
    for r in profiles_results:
        email = r['email'].lower().strip()
        if email not in seen_emails:
            seen_emails.add(email)
            all_results.append(r)

    print(f"\nAfter Phase 1: {len(all_results)} unique emails")

    # ==================== PHASE 2: Research Lab Pages ====================
    print("\n\nPHASE 2: SCRAPING RESEARCH LAB PAGES")
    print("=" * 70)

    lab_results = scrape_research_labs(session, seen_emails)
    all_results.extend(lab_results)

    print(f"\nAfter Phase 2: {len(all_results)} unique emails")

    # ==================== PHASE 3: Alphabetical Browse (catch stragglers) ====================
    print("\n\nPHASE 3: ALPHABETICAL BROWSE FOR ADDITIONAL STUDENTS")
    print("=" * 70)

    alpha_results = scrape_profiles_alpha(session, seen_emails)
    all_results.extend(alpha_results)

    print(f"\nAfter Phase 3: {len(all_results)} unique emails")

    # ==================== SAVE RESULTS ====================
    print(f"\n\n{'='*70}")
    print(f"TOTAL UNIQUE STANFORD EMAILS FOUND: {len(all_results)}")
    print(f"{'='*70}")

    # Save to CSV
    output_csv = 'stanford_eng_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: (x['department'], x['email'])):
            writer.writerow(r)
    print(f"\nSaved to {output_csv}")

    # Save as JSON
    output_json = 'stanford_eng_emails.json'
    with open(output_json, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"Also saved to {output_json}")

    # Summary by department
    print(f"\n{'='*70}")
    print("SUMMARY BY DEPARTMENT:")
    print(f"{'='*70}")
    dept_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f"  {dept}: {count} emails")

    # Print sample emails
    print(f"\n{'='*70}")
    print("SAMPLE EMAILS (first 50):")
    print(f"{'='*70}")
    for r in sorted(all_results, key=lambda x: x['email'])[:50]:
        name_str = f" ({r['name']})" if r['name'] else ""
        print(f"  {r['email']}{name_str} - {r['department']}")

    return all_results


if __name__ == '__main__':
    main()
