#!/usr/bin/env python3
"""
USC Viterbi School of Engineering Graduate Student Email Scraper
Scrapes student/researcher emails from department directories and research lab pages.
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
from urllib.parse import urljoin, urlparse

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# All URLs to try - each only once
ALL_URLS = [
    # ===== DEPARTMENT GRADUATE STUDENT PAGES =====
    # Viterbi main
    ("https://viterbi.usc.edu/directory/", "Viterbi (Directory)"),
    
    # Computer Science
    ("https://www.cs.usc.edu/people/phd-students/", "Computer Science"),
    ("https://www.cs.usc.edu/people/graduate-students/", "Computer Science"),
    ("https://www.cs.usc.edu/people/students/", "Computer Science"),
    ("https://www.cs.usc.edu/academic-programs/phd/current-students/", "Computer Science"),
    
    # Electrical & Computer Engineering (Ming Hsieh)
    ("https://minghsiehece.usc.edu/people/phd-students/", "Electrical & Computer Engineering"),
    ("https://minghsiehece.usc.edu/people/graduate-students/", "Electrical & Computer Engineering"),
    ("https://minghsiehece.usc.edu/staff/", "Electrical & Computer Engineering (Staff)"),
    ("https://minghsiehece.usc.edu/students/", "Electrical & Computer Engineering"),
    
    # Aerospace & Mechanical Engineering
    ("https://ame.usc.edu/people/graduate-students/", "Aerospace & Mechanical Engineering"),
    ("https://ame.usc.edu/directory/", "Aerospace & Mechanical Engineering"),
    ("https://ame.usc.edu/people/phd-students/", "Aerospace & Mechanical Engineering"),
    ("https://ame.usc.edu/people/students/", "Aerospace & Mechanical Engineering"),
    
    # Chemical Engineering & Materials Science
    ("https://che.usc.edu/people/graduate-students/", "Chemical Engineering"),
    ("https://che.usc.edu/people/phd-students/", "Chemical Engineering"),
    ("https://che.usc.edu/people/students/", "Chemical Engineering"),
    
    # Civil & Environmental Engineering
    ("https://cee.usc.edu/people/graduate-students/", "Civil & Environmental Engineering"),
    ("https://cee.usc.edu/people/phd-students/", "Civil & Environmental Engineering"),
    ("https://cee.usc.edu/people/students/", "Civil & Environmental Engineering"),
    
    # Biomedical Engineering
    ("https://bme.usc.edu/people/graduate-students/", "Biomedical Engineering"),
    ("https://bme.usc.edu/people/phd-students/", "Biomedical Engineering"),
    ("https://bme.usc.edu/people/students/", "Biomedical Engineering"),
    
    # Industrial & Systems Engineering
    ("https://ise.usc.edu/people/graduate-students/", "Industrial & Systems Engineering"),
    ("https://ise.usc.edu/people/phd-students/", "Industrial & Systems Engineering"),
    ("https://ise.usc.edu/people/students/", "Industrial & Systems Engineering"),
    
    # Materials Science
    ("https://mse.usc.edu/people/graduate-students/", "Materials Science"),
    ("https://mse.usc.edu/people/phd-students/", "Materials Science"),
    
    # ISI
    ("https://www.isi.edu/people/", "ISI"),
    ("https://www.isi.edu/directory/", "ISI"),
    
    # ===== RESEARCH LAB PAGES =====
    # CS Labs
    ("https://nlg.isi.edu/people/", "NLG Lab (ISI)"),
    ("https://sail.usc.edu/people.html", "SAIL (Signal Analysis & Interpretation Lab)"),
    ("https://sail.usc.edu/people/", "SAIL"),
    ("https://melady.usc.edu/people/", "Melady Lab"),
    ("https://robotics.usc.edu/interaction/people.html", "Interaction Lab"),
    ("https://glamor.usc.edu/people/", "GLAMOR Lab"),
    ("https://icaros.usc.edu/people", "ICAROS Lab"),
    ("https://loni.usc.edu/about/people", "LONI Lab"),
    ("https://clvrai.com/people/", "CLVR Lab"),
    ("https://nsl.usc.edu/people/", "Networked Systems Lab"),
    ("https://hal.usc.edu/people.html", "HAL Lab"),
    
    # EE Labs
    ("https://sipi.usc.edu/people/", "SIPI Lab"),
    ("https://anrg.usc.edu/www/people/", "ANRG Lab"),
    
    # Other labs
    ("https://sites.usc.edu/eessc/people/", "EESSC Lab"),
    ("https://sites.usc.edu/dmml/people/", "DMML Lab"),
    ("https://sites.usc.edu/atomicmol/people/", "Atomic & Molecular Physics Lab"),
    ("https://sites.usc.edu/multiscale/people/", "Multiscale Lab"),
    ("https://sites.usc.edu/rocketlab/people/", "Rocket Lab"),
    ("https://sites.usc.edu/bhattlab/people/", "Bhatt Lab (BME)"),
    ("https://sites.usc.edu/npnl/people/", "NPNL Lab (BME)"),
    
    # CS research pages for discovering labs
    ("https://www.cs.usc.edu/research/", "CS Research"),
    ("https://www.cs.usc.edu/research/research-labs/", "CS Research Labs"),
    
    # Additional known USC CS labs
    ("https://viterbi-web.usc.edu/~csci570/", "CS 570"),
    ("https://nlp.usc.edu/people/", "NLP Lab"),
    
    # More USC labs (common patterns)
    ("https://teamcore.usc.edu/people/", "TeamCore Lab"),
    ("https://www.isi.edu/people/students/", "ISI Students"),
    ("https://viterbi.usc.edu/research/labs/", "Viterbi Labs"),
    ("https://viterbi.usc.edu/research/centers/", "Viterbi Centers"),
    
    # Department research pages
    ("https://minghsiehece.usc.edu/research/", "ECE Research"),
    ("https://ame.usc.edu/research/", "AME Research"),
    ("https://bme.usc.edu/research/", "BME Research"),
    ("https://che.usc.edu/research/", "ChemE Research"),
    ("https://cee.usc.edu/research/", "CEE Research"),
    ("https://ise.usc.edu/research/", "ISE Research"),
]


def extract_usc_emails_from_text(text):
    """Extract all USC email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*usc\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    # Clean and deduplicate
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix common scraping artifacts: phone numbers prefixed to emails
        # e.g., "740-4447gabby.garcia@usc.edu"
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*usc\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_emails_from_mailto(soup):
    """Extract USC emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            email_match = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', href, re.IGNORECASE)
            if email_match:
                emails.append(email_match.group(1).lower())
    return emails


def is_admin_email(email):
    """Check if email is an administrative/generic email."""
    admin_patterns = [
        'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
        'support@', 'contact@', 'registrar@', 'admissions@',
        'advising@', 'dean@', 'chair@', 'reception@', 'enroll@',
        'gradadm@', 'viterbi@', 'communications@', 'department@',
        'ece.student', 'ece.faculty', 'eceadmin@', 'eepadmin@',
        'vasephd@', 'vasems@', 'sure@vase',
        '.department@', 'services@', 'affairs@',
    ]
    for p in admin_patterns:
        if p in email:
            return True
    # Filter emails that start with "email" prefix (artifact)
    if email.startswith('email'):
        return True
    return False


def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email on the page."""
    # Strategy 1: Look for mailto link
    for a_tag in soup.find_all('a', href=True):
        if email in a_tag.get('href', '').lower():
            parent = a_tag.parent
            for _ in range(5):
                if parent is None:
                    break
                text = parent.get_text(separator=' ', strip=True)
                parts = text.split(email)
                for part in parts:
                    part = part.strip(' ,|-•·\n\t')
                    words = part.split()
                    if 2 <= len(words) <= 5:
                        name_candidate = ' '.join(words[:4])
                        if not any(x in name_candidate.lower() for x in
                                   ['student', 'professor', 'phd', 'email', 'phone',
                                    'address', 'http', 'www', 'lab', 'research',
                                    'department', 'office', 'contact']):
                            return name_candidate
                parent = parent.parent

    # Strategy 2: Check nearby headings
    email_text_elements = soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE))
    for elem in email_text_elements:
        parent = elem.parent
        for _ in range(5):
            if parent is None:
                break
            name_tags = parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b'])
            for tag in name_tags:
                name = tag.get_text(strip=True)
                if name and not any(x in name.lower() for x in
                                    ['email', 'contact', '@', 'student', 'people', 'phone',
                                     'department', 'office']):
                    return name
            parent = parent.parent

    return ""


def scrape_page(url, department, session):
    """Scrape a single page for USC emails."""
    results = []
    try:
        print(f"\nScraping: {url}")
        print(f"  Department: {department}")

        response = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)

        if response.status_code != 200:
            print(f"  HTTP {response.status_code} - Skipping")
            return results

        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text()

        # Extract all USC emails from various sources
        text_emails = extract_usc_emails_from_text(page_text)
        mailto_emails = extract_emails_from_mailto(soup)

        # Check for obfuscated emails
        obfuscated = re.findall(r'([\w.+-]+)\s*\[at\]\s*((?:[\w-]+\.)*usc\.edu)', page_text, re.IGNORECASE)
        obfuscated_emails = [f"{m[0]}@{m[1]}".lower() for m in obfuscated]

        # Check script tags
        script_emails = []
        for script in soup.find_all('script'):
            if script.string:
                script_emails.extend(extract_usc_emails_from_text(script.string))

        all_emails = list(set(text_emails + mailto_emails + obfuscated_emails + script_emails))

        # Filter out admin emails
        filtered_emails = [e for e in all_emails if not is_admin_email(e)]

        print(f"  Found {len(filtered_emails)} non-admin emails")

        for email in filtered_emails:
            name = try_get_name_for_email(soup, email)
            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': url
            })
            print(f"    {email} | {name}")

    except requests.exceptions.ConnectionError:
        print(f"  Connection error")
    except requests.exceptions.Timeout:
        print(f"  Timeout")
    except Exception as e:
        print(f"  Error: {type(e).__name__}: {e}")

    return results


def discover_lab_links(url, session):
    """Find links to lab pages from a research/directory page."""
    links = []
    try:
        response = session.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '').strip()
                text = a_tag.get_text(strip=True).lower()
                full_url = urljoin(url, href)

                if 'usc.edu' in full_url or full_url.startswith('/'):
                    full_url = urljoin(url, href)
                    keywords = ['lab', 'research', 'group', 'people', 'member', 'team']
                    if any(kw in href.lower() or kw in text for kw in keywords):
                        if full_url != url and '#' not in full_url:
                            links.append(full_url)
    except Exception as e:
        print(f"  Error discovering links from {url}: {e}")

    return list(set(links))


def main():
    all_results = []
    seen_emails = set()
    visited_urls = set()
    session = requests.Session()

    print("=" * 70)
    print("USC VITERBI SCHOOL OF ENGINEERING - EMAIL SCRAPER")
    print("=" * 70)

    # Phase 1: Scrape all known URLs
    print("\n\nPHASE 1: SCRAPING ALL KNOWN URLS")
    print("=" * 70)

    for url, department in ALL_URLS:
        if url in visited_urls:
            continue
        visited_urls.add(url)
        
        results = scrape_page(url, department, session)
        for r in results:
            email = r['email'].lower().strip()
            if email not in seen_emails:
                seen_emails.add(email)
                all_results.append(r)
        time.sleep(0.5)

    print(f"\nAfter Phase 1: {len(all_results)} unique emails")

    # Phase 2: Discover and scrape lab pages from research pages
    print("\n\nPHASE 2: DISCOVERING LAB PAGES")
    print("=" * 70)

    research_pages = [
        "https://www.cs.usc.edu/research/",
        "https://www.cs.usc.edu/research/research-labs/",
        "https://minghsiehece.usc.edu/research/",
        "https://ame.usc.edu/research/",
        "https://bme.usc.edu/research/",
        "https://viterbi.usc.edu/research/labs/",
    ]

    discovered_lab_urls = []
    for page_url in research_pages:
        links = discover_lab_links(page_url, session)
        discovered_lab_urls.extend(links)
        time.sleep(0.5)

    discovered_lab_urls = list(set(discovered_lab_urls))
    print(f"\n  Discovered {len(discovered_lab_urls)} potential lab pages")

    for url in discovered_lab_urls[:40]:
        if url in visited_urls:
            continue
        visited_urls.add(url)
        
        # Try /people/ sub-path if the URL is a lab homepage
        urls_to_try = [url]
        if not url.endswith('/people/') and not url.endswith('/people'):
            urls_to_try.append(urljoin(url + '/', 'people/'))
            urls_to_try.append(urljoin(url + '/', 'team/'))
            urls_to_try.append(urljoin(url + '/', 'members/'))

        for try_url in urls_to_try:
            if try_url in visited_urls:
                continue
            visited_urls.add(try_url)
            
            results = scrape_page(try_url, "Discovered Lab", session)
            for r in results:
                email = r['email'].lower().strip()
                if email not in seen_emails:
                    seen_emails.add(email)
                    all_results.append(r)
            time.sleep(0.3)

    print(f"\nAfter Phase 2: {len(all_results)} unique emails")

    # Save results
    print(f"\n\n{'='*70}")
    print(f"TOTAL UNIQUE USC EMAILS FOUND: {len(all_results)}")
    print(f"{'='*70}")

    # Save to CSV
    output_csv = 'usc_viterbi_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: x['department']):
            writer.writerow(r)
    print(f"\nSaved to {output_csv}")

    # Save as JSON
    output_json = 'usc_viterbi_emails.json'
    with open(output_json, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"Also saved to {output_json}")

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY BY DEPARTMENT/SOURCE:")
    print(f"{'='*70}")
    dept_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f"  {dept}: {count} emails")

    # Print all emails
    print(f"\n{'='*70}")
    print("ALL EMAILS:")
    print(f"{'='*70}")
    for r in sorted(all_results, key=lambda x: x['email']):
        name_str = f" ({r['name']})" if r['name'] else ""
        print(f"  {r['email']}{name_str} - {r['department']}")

    return all_results


if __name__ == '__main__':
    main()
