#!/usr/bin/env python3
"""
UC Berkeley Graduate Student Email Scraper - Pass 2
1. Scrapes profile-linked departments (Polisci, Physics, etc.)
2. Tries alternative URLs for missing departments (Stats, EECS, ME, MSE, NucE, CBE)
3. Cleans up and merges with pass 1 data
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


def extract_berkeley_emails(text):
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*berkeley\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup):
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*berkeley\.edu)', href, re.IGNORECASE)
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
        'ugrad@', 'gradapp@', 'apply@', 'econ@', 'polisci@',
        'ieor-student', 'bef@', 'physics@', 'chem@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


def get_soup(url, session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        return None, None
    except Exception as e:
        return None, None


# ============================================================
# PROFILE-LINKED DEPARTMENT SCRAPERS
# ============================================================

def scrape_polisci_profiles(session):
    """Scrape Political Science - listing pages have names, profile pages have emails."""
    log("\n" + "=" * 60)
    log("POLITICAL SCIENCE - Scraping individual profiles")
    log("=" * 60)

    results = []
    base_url = "https://polisci.berkeley.edu/people/graduate-students"
    
    # Collect all profile links from paginated listing pages
    profiles = []
    pages_to_try = [
        base_url,
        base_url + "?page=1",
        base_url + "?page=2",
        base_url + "?page=3",
        base_url + "?page=4",
        base_url + "?page=5",
    ]
    
    seen_profile_urls = set()
    
    for page_url in pages_to_try:
        log(f"  Listing page: {page_url}")
        soup, final_url = get_soup(page_url, session)
        if soup is None:
            continue
        
        # Find profile links - pattern: /people/person/name-slug
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if '/people/person/' in href:
                full_url = urljoin("https://polisci.berkeley.edu", href)
                if full_url in seen_profile_urls:
                    continue
                seen_profile_urls.add(full_url)
                
                name = a_tag.get_text(strip=True)
                # Skip non-name links (image alt text, etc.)
                if not name or len(name) < 2 or name.startswith('Profile') or name.startswith('People'):
                    continue
                if any(x in name.lower() for x in ['graduate', 'faculty', 'search', 'people at', 'skip']):
                    continue
                
                profiles.append({'name': name, 'url': full_url})
        
        time.sleep(0.5)
    
    # Deduplicate by URL
    seen = set()
    unique_profiles = []
    for p in profiles:
        if p['url'] not in seen:
            seen.add(p['url'])
            unique_profiles.append(p)
    profiles = unique_profiles
    
    log(f"  Found {len(profiles)} unique student profiles")
    
    # Visit each profile page
    for i, profile in enumerate(profiles):
        name = profile['name']
        url = profile['url']
        
        if i % 20 == 0:
            log(f"  [{i+1}/{len(profiles)}] Processing profiles...")
        
        soup, _ = get_soup(url, session)
        if soup is None:
            continue
        
        # Extract email from profile
        mailto = extract_mailto_emails(soup)
        text_emails = extract_berkeley_emails(soup.get_text())
        all_emails = list(set(mailto + text_emails))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            results.append({
                'email': personal[0],
                'name': name,
                'department': 'Political Science',
                'source_url': url,
            })
        
        time.sleep(0.3)
    
    log(f"  TOTAL Political Science: {len(results)} emails")
    return results


def scrape_physics_profiles(session):
    """Scrape Physics - listing page has table with profile links."""
    log("\n" + "=" * 60)
    log("PHYSICS - Scraping individual profiles")
    log("=" * 60)

    results = []
    base_url = "https://physics.berkeley.edu/people/graduate-students"
    
    # Collect all grad student profile links from paginated pages
    profiles = []
    seen_urls = set()
    
    # Physics has many pages (up to page=25 seen)
    pages_to_try = [base_url] + [f"{base_url}?page={i}" for i in range(1, 26)]
    
    for page_url in pages_to_try:
        log(f"  Listing page: {page_url}")
        soup, final_url = get_soup(page_url, session)
        if soup is None:
            continue
        
        # Find grad student profile links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if '/people/graduate-student/' in href:
                full_url = urljoin("https://physics.berkeley.edu", href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)
                
                name = a_tag.get_text(strip=True)
                if not name or len(name) < 2:
                    continue
                
                profiles.append({'name': name, 'url': full_url})
        
        time.sleep(0.3)
    
    # Deduplicate
    seen = set()
    unique_profiles = []
    for p in profiles:
        if p['url'] not in seen:
            seen.add(p['url'])
            unique_profiles.append(p)
    profiles = unique_profiles
    
    log(f"  Found {len(profiles)} unique physics grad student profiles")
    
    # Visit each profile page for email
    for i, profile in enumerate(profiles):
        name = profile['name']
        url = profile['url']
        
        if i % 20 == 0:
            log(f"  [{i+1}/{len(profiles)}] Processing profiles...")
        
        soup, _ = get_soup(url, session)
        if soup is None:
            continue
        
        mailto = extract_mailto_emails(soup)
        text_emails = extract_berkeley_emails(soup.get_text())
        all_emails = list(set(mailto + text_emails))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            results.append({
                'email': personal[0],
                'name': name,
                'department': 'Physics',
                'source_url': url,
            })
        
        time.sleep(0.3)
    
    log(f"  TOTAL Physics: {len(results)} emails")
    return results


def scrape_statistics(session):
    """Scrape Statistics - try various URL patterns."""
    log("\n" + "=" * 60)
    log("STATISTICS - Trying alternative URLs")
    log("=" * 60)

    results = []
    
    urls_to_try = [
        "https://statistics.berkeley.edu/people/phd-students",
        "https://statistics.berkeley.edu/people/grad-students",
        "https://statistics.berkeley.edu/people/students",
        "https://statistics.berkeley.edu/people/masters-students",
        "https://statistics.berkeley.edu/graduate-program/students",
        "https://statistics.berkeley.edu/people",
        "https://stat.berkeley.edu/people",
        "https://stat.berkeley.edu/people/graduate-students",
    ]
    
    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue
        
        log(f"    -> Loaded (final: {final_url})")
        
        # Look for email-containing content
        text = soup.get_text()
        emails = extract_berkeley_emails(text)
        mailto = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            log(f"    -> Found {len(personal)} emails")
            for email in personal:
                results.append({
                    'email': email,
                    'name': '',
                    'department': 'Statistics',
                    'source_url': final_url or url,
                })
        
        # Also try profile links
        profile_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full_url = urljoin(final_url or url, href)
            name = a_tag.get_text(strip=True)
            if ('/people/' in href or '/person/' in href) and name and len(name) > 2 and '@' not in name:
                if not any(x in name.lower() for x in ['faculty', 'staff', 'people', 'home', 'search', 'phd students', 'masters', 'visitors', 'current', 'past', 'emerit', 'researcher']):
                    profile_links.append({'name': name, 'url': full_url})
        
        if profile_links and len(results) < 5:
            log(f"    -> Found {len(profile_links)} profile links, visiting...")
            for prof in profile_links[:100]:
                psoup, _ = get_soup(prof['url'], session)
                if psoup is None:
                    continue
                pmailto = extract_mailto_emails(psoup)
                ptext = extract_berkeley_emails(psoup.get_text())
                pemails = list(set(pmailto + ptext))
                ppersonal = [e for e in pemails if not is_admin_email(e)]
                if ppersonal:
                    results.append({
                        'email': ppersonal[0],
                        'name': prof['name'],
                        'department': 'Statistics',
                        'source_url': prof['url'],
                    })
                time.sleep(0.3)
        
        if results:
            break
        
        time.sleep(0.5)
    
    log(f"  TOTAL Statistics: {len(results)} emails")
    return results


def scrape_eecs(session):
    """Scrape EECS - try various URL patterns."""
    log("\n" + "=" * 60)
    log("EECS - Trying alternative URLs")
    log("=" * 60)

    results = []
    
    urls_to_try = [
        "https://www2.eecs.berkeley.edu/Pubs/Grads/",
        "https://eecs.berkeley.edu/people/graduate-students",
        "https://eecs.berkeley.edu/people/students",
        "https://eecs.berkeley.edu/people",
        "https://www2.eecs.berkeley.edu/Grads/",
        "https://www.eecs.berkeley.edu/people/graduate-students",
        "https://eecs.berkeley.edu/academics/graduate",
    ]
    
    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue
        
        log(f"    -> Loaded (final: {final_url})")
        text = soup.get_text()
        emails = extract_berkeley_emails(text)
        mailto = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            log(f"    -> Found {len(personal)} emails")
            for email in personal:
                results.append({
                    'email': email,
                    'name': '',
                    'department': 'EECS',
                    'source_url': final_url or url,
                })
        
        # Look for profile links
        profile_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full = urljoin(final_url or url, href)
            name = a_tag.get_text(strip=True)
            if ('/people/' in href or '/Grads/' in href) and name and len(name) > 2 and '@' not in name:
                if not any(x in name.lower() for x in ['faculty', 'staff', 'people', 'home', 'search', 'graduate', 'student']):
                    profile_links.append({'name': name, 'url': full})
        
        if profile_links and len(results) < 5:
            log(f"    -> Found {len(profile_links)} profile links, visiting up to 50...")
            for prof in profile_links[:50]:
                psoup, _ = get_soup(prof['url'], session)
                if psoup is None:
                    continue
                pmailto = extract_mailto_emails(psoup)
                ptext = extract_berkeley_emails(psoup.get_text())
                pemails = list(set(pmailto + ptext))
                ppersonal = [e for e in pemails if not is_admin_email(e)]
                if ppersonal:
                    results.append({
                        'email': ppersonal[0],
                        'name': prof['name'],
                        'department': 'EECS',
                        'source_url': prof['url'],
                    })
                time.sleep(0.3)
        
        if results:
            break
        
        time.sleep(0.5)
    
    log(f"  TOTAL EECS: {len(results)} emails")
    return results


def scrape_me(session):
    """Scrape Mechanical Engineering."""
    log("\n" + "=" * 60)
    log("MECHANICAL ENGINEERING - Trying alternative URLs")
    log("=" * 60)

    results = []
    urls_to_try = [
        "https://me.berkeley.edu/people/graduate-students/",
        "https://me.berkeley.edu/people/students/",
        "https://me.berkeley.edu/people/",
        "https://me.berkeley.edu/people/phd-students/",
        "https://me.berkeley.edu/people",
        "https://www.me.berkeley.edu/people",
    ]
    
    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue
        
        log(f"    -> Loaded (final: {final_url})")
        text = soup.get_text()
        emails = extract_berkeley_emails(text)
        mailto = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            log(f"    -> Found {len(personal)} emails")
            for email in personal:
                results.append({
                    'email': email,
                    'name': '',
                    'department': 'Mechanical Engineering',
                    'source_url': final_url or url,
                })
            break
        
        time.sleep(0.5)
    
    log(f"  TOTAL ME: {len(results)} emails")
    return results


def scrape_nuceng(session):
    """Scrape Nuclear Engineering."""
    log("\n" + "=" * 60)
    log("NUCLEAR ENGINEERING - Trying alternative URLs")
    log("=" * 60)

    results = []
    urls_to_try = [
        "https://nuc.berkeley.edu/people/graduate-students",
        "https://nuc.berkeley.edu/people/students",
        "https://nuc.berkeley.edu/people",
        "https://nuc.berkeley.edu/people/",
        "https://www.nuc.berkeley.edu/people",
        "https://nuc.berkeley.edu/graduate/",
        "https://nuc.berkeley.edu/",
    ]
    
    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue
        
        log(f"    -> Loaded (final: {final_url})")
        text = soup.get_text()
        emails = extract_berkeley_emails(text)
        mailto = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            log(f"    -> Found {len(personal)} emails")
            for email in personal:
                results.append({
                    'email': email,
                    'name': '',
                    'department': 'Nuclear Engineering',
                    'source_url': final_url or url,
                })
            break
        
        time.sleep(0.5)
    
    log(f"  TOTAL NucE: {len(results)} emails")
    return results


def scrape_mse(session):
    """Scrape Materials Science & Engineering."""
    log("\n" + "=" * 60)
    log("MATERIALS SCIENCE - Trying alternative URLs")
    log("=" * 60)

    results = []
    urls_to_try = [
        "https://mse.berkeley.edu/people/graduate-students",
        "https://mse.berkeley.edu/people/students",
        "https://mse.berkeley.edu/people",
        "https://mse.berkeley.edu/people/",
        "https://www.mse.berkeley.edu/people",
        "https://mse.berkeley.edu/",
    ]
    
    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue
        
        log(f"    -> Loaded (final: {final_url})")
        text = soup.get_text()
        emails = extract_berkeley_emails(text)
        mailto = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            log(f"    -> Found {len(personal)} emails")
            for email in personal:
                results.append({
                    'email': email,
                    'name': '',
                    'department': 'Materials Science & Engineering',
                    'source_url': final_url or url,
                })
            break
        
        time.sleep(0.5)
    
    log(f"  TOTAL MSE: {len(results)} emails")
    return results


def scrape_cbe(session):
    """Scrape Chemical & Biomolecular Engineering."""
    log("\n" + "=" * 60)
    log("CHEMICAL & BIOMOLECULAR ENG - Trying alternative URLs")
    log("=" * 60)

    results = []
    urls_to_try = [
        "https://chemistry.berkeley.edu/cbe/people",
        "https://chemistry.berkeley.edu/cbe/people/graduate-students",
        "https://cbe.berkeley.edu/people/graduate-students",
        "https://cbe.berkeley.edu/people",
        "https://cbe.berkeley.edu/",
        "https://chemistry.berkeley.edu/cbe",
    ]
    
    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final_url = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue
        
        log(f"    -> Loaded (final: {final_url})")
        text = soup.get_text()
        emails = extract_berkeley_emails(text)
        mailto = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            log(f"    -> Found {len(personal)} emails")
            for email in personal:
                results.append({
                    'email': email,
                    'name': '',
                    'department': 'Chemical & Biomolecular Engineering',
                    'source_url': final_url or url,
                })
            break
        
        time.sleep(0.5)
    
    log(f"  TOTAL CBE: {len(results)} emails")
    return results


# ============================================================
# NAME CLEANUP
# ============================================================

def name_from_email(email):
    """Derive a best-guess name from an email prefix."""
    prefix = email.split('@')[0]
    
    # Common patterns:
    # firstname.lastname -> First Last
    # firstname_lastname -> First Last
    # firstlast -> can't easily split
    # flastname -> can't easily split
    
    if '.' in prefix:
        parts = prefix.split('.')
        return ' '.join(p.capitalize() for p in parts if p.isalpha())
    elif '_' in prefix:
        parts = prefix.split('_')
        return ' '.join(p.capitalize() for p in parts if p.isalpha())
    elif '-' in prefix:
        parts = prefix.split('-')
        return ' '.join(p.capitalize() for p in parts if p.isalpha())
    
    return ""


def clean_name(name, email):
    """Clean up a name - return cleaned name or derive from email."""
    bad_indicators = [
        'skip to main', 'www.', 'directions', 'alumni', 'staff',
        'lecturer', 'recruitment', 'people', 'faculty', 'http',
        'content', 'search', 'berkeley', 'home', 'department',
        'menu', 'navigation', 'footer', 'header', 'sidebar',
        'main content', 'toggle', 'close', 'open', 'submit',
    ]
    
    if not name or any(x in name.lower() for x in bad_indicators):
        return name_from_email(email)
    
    # Clean up trailing department/role info that got concatenated
    # e.g. "Jason Bircea18th-Century British19th-Century BritishEarly American"
    # Try to find where the name ends and extra info starts
    clean = re.split(r'(?<=[a-z])(?=[A-Z][a-z].*(?:Century|American|British|Theory|Poetry|Studies|African|Asian|Caribbean|Atlantic|Cultural|Film|Drama|Scottish|Disability|Narrative|Novel|Renaissance|Modern|Medieval|Middle|English))', name)
    if clean:
        name = clean[0].strip()
    
    # Remove "PhD Candidate", "PhD Student" etc suffixes
    name = re.sub(r'\s*(?:PhD?\.?\s*(?:Candidate|Student|candidate|student))', '', name)
    name = re.sub(r'\s*(?:Ph\.D\.?\s*(?:Candidate|Student|candidate|student))', '', name)
    
    return name.strip()


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    
    # Load pass 1 results
    pass1_results = []
    try:
        with open('berkeley_dept_emails.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pass1_results.append(row)
        log(f"Loaded {len(pass1_results)} results from pass 1")
    except FileNotFoundError:
        log("No pass 1 results found")
    
    # Clean pass 1 results
    cleaned_pass1 = []
    for r in pass1_results:
        email = r['email'].lower().strip()
        
        # Skip admin emails
        if is_admin_email(email):
            continue
        
        # Clean name
        name = clean_name(r['name'], email)
        
        cleaned_pass1.append({
            'email': email,
            'name': name,
            'department': r['department'],
            'source_url': r['source_url'],
        })
    
    log(f"After cleaning pass 1: {len(cleaned_pass1)} entries")
    
    # Build global seen set
    global_seen = set(r['email'] for r in cleaned_pass1)
    
    # Departments that need profile-level scraping
    all_new_results = []
    
    # Political Science (profile pages)
    polisci_results = scrape_polisci_profiles(session)
    for r in polisci_results:
        if r['email'] not in global_seen:
            global_seen.add(r['email'])
            all_new_results.append(r)
    
    # Physics (profile pages)
    physics_results = scrape_physics_profiles(session)
    for r in physics_results:
        if r['email'] not in global_seen:
            global_seen.add(r['email'])
            all_new_results.append(r)
    
    # Statistics
    stats_results = scrape_statistics(session)
    for r in stats_results:
        if r['email'] not in global_seen:
            global_seen.add(r['email'])
            all_new_results.append(r)
    
    # EECS
    eecs_results = scrape_eecs(session)
    for r in eecs_results:
        if r['email'] not in global_seen:
            global_seen.add(r['email'])
            all_new_results.append(r)
    
    # Mechanical Engineering
    me_results = scrape_me(session)
    for r in me_results:
        if r['email'] not in global_seen:
            global_seen.add(r['email'])
            all_new_results.append(r)
    
    # Materials Science
    mse_results = scrape_mse(session)
    for r in mse_results:
        if r['email'] not in global_seen:
            global_seen.add(r['email'])
            all_new_results.append(r)
    
    # Nuclear Engineering
    nuc_results = scrape_nuceng(session)
    for r in nuc_results:
        if r['email'] not in global_seen:
            global_seen.add(r['email'])
            all_new_results.append(r)
    
    # Chemical & Biomolecular Engineering
    cbe_results = scrape_cbe(session)
    for r in cbe_results:
        if r['email'] not in global_seen:
            global_seen.add(r['email'])
            all_new_results.append(r)
    
    log(f"\nNew emails from pass 2: {len(all_new_results)}")
    
    # Merge all results
    final_results = cleaned_pass1 + all_new_results
    
    # Remove duplicates, keep first occurrence
    seen = set()
    deduplicated = []
    for r in final_results:
        if r['email'] not in seen:
            seen.add(r['email'])
            deduplicated.append(r)
    
    final_results = deduplicated
    
    log(f"\n{'=' * 70}")
    log(f"FINAL RESULTS")
    log(f"{'=' * 70}")
    log(f"Total unique @berkeley.edu emails: {len(final_results)}")
    
    # Save CSV
    output_csv = 'berkeley_dept_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in final_results:
            writer.writerow(r)
    log(f"Saved to {output_csv}")
    
    # Save JSON
    output_json = 'berkeley_dept_emails.json'
    with open(output_json, 'w') as f:
        json.dump(final_results, f, indent=2)
    log(f"Saved to {output_json}")
    
    # Summary by department
    log(f"\n{'=' * 70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in final_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")
    
    log(f"\n  GRAND TOTAL: {len(final_results)}")
    
    return final_results


if __name__ == '__main__':
    main()
