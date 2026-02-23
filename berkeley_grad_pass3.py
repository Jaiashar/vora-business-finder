#!/usr/bin/env python3
"""
UC Berkeley Graduate Student Email Scraper - Pass 3
Focused scraping of profile-linked departments where pass 2 failed.
Also handles departments that need JS rendering via direct URL construction.
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
        'ieor-student', 'bef@', 'physics@', 'chem@', 'hkansa@',
        'caocampo@', 'jallen@', 'phildept@', 'nuceng@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) for p in admin_patterns)


def get_soup(url, session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        return None, None
    except Exception:
        return None, None


def name_from_email(email):
    """Derive a best-guess name from an email prefix."""
    prefix = email.split('@')[0]
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


# ============================================================
# POLITICAL SCIENCE - Fixed profile extraction
# ============================================================

def scrape_polisci(session):
    """Scrape Polisci by properly collecting profile URLs then visiting each."""
    log("\n" + "=" * 60)
    log("POLITICAL SCIENCE - Profile scraping (fixed)")
    log("=" * 60)

    results = []
    base = "https://polisci.berkeley.edu"
    listing_url = f"{base}/people/graduate-students"
    
    # Collect profile URLs from ALL paginated listing pages
    # The polisci page has 50 per page, typically 3-5 pages
    profile_map = {}  # url -> name
    
    for page_num in range(0, 6):
        url = listing_url if page_num == 0 else f"{listing_url}?page={page_num}"
        log(f"  Listing page: {url}")
        soup, _ = get_soup(url, session)
        if soup is None:
            continue
        
        # Find ALL links with /people/person/ - collect name+url pairs
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if '/people/person/' not in href:
                continue
            
            full_url = urljoin(base, href)
            name = a_tag.get_text(strip=True)
            
            # Only update if we have a valid name (skip image links)
            if not name or len(name) < 3:
                continue
            if name.startswith('Profile') or name.startswith('People'):
                continue
            if any(x in name.lower() for x in ['graduate', 'faculty', 'search', 'people at', 'skip']):
                continue
            
            # Store or update (prefer longest name for same URL)
            if full_url not in profile_map or len(name) > len(profile_map[full_url]):
                profile_map[full_url] = name
        
        time.sleep(0.5)
    
    profiles = [{'name': name, 'url': url} for url, name in profile_map.items()]
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


# ============================================================
# STATISTICS - Try the PhD Students sub-page via link exploration
# ============================================================

def scrape_statistics(session):
    """Scrape Statistics by finding and visiting all sub-pages."""
    log("\n" + "=" * 60)
    log("STATISTICS - Deep page exploration")
    log("=" * 60)

    results = []
    seen_urls = set()
    
    # Start from the people page and follow sub-links
    people_url = "https://statistics.berkeley.edu/people"
    soup, _ = get_soup(people_url, session)
    if soup is None:
        log("  Failed to load main people page")
        return results
    
    # Find sub-page links (PhD Students, Masters Students, etc.)
    sub_links = {}
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        text = a_tag.get_text(strip=True).lower()
        full_url = urljoin(people_url, href)
        if 'student' in text or 'phd' in text or 'master' in text or 'grad' in text:
            sub_links[full_url] = a_tag.get_text(strip=True)
    
    log(f"  Found sub-links: {sub_links}")
    
    for sub_url, label in sub_links.items():
        if sub_url in seen_urls:
            continue
        seen_urls.add(sub_url)
        
        log(f"  Visiting: {sub_url} ({label})")
        sub_soup, final = get_soup(sub_url, session)
        if sub_soup is None:
            log(f"    -> Failed")
            continue
        
        log(f"    -> Loaded (final: {final})")
        
        # Extract emails directly
        text = sub_soup.get_text()
        emails = extract_berkeley_emails(text)
        mailto = extract_mailto_emails(sub_soup)
        all_emails = list(set(emails + mailto))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            log(f"    -> Found {len(personal)} emails directly")
            for email in personal:
                name = name_from_email(email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'Statistics',
                    'source_url': final or sub_url,
                })
        
        # Also look for individual profile links
        profile_map = {}
        for a_tag in sub_soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full = urljoin(final or sub_url, href)
            name = a_tag.get_text(strip=True)
            if ('/people/' in href and name and len(name) > 2 and '@' not in name
                and not any(x in name.lower() for x in ['faculty', 'staff', 'people', 'home', 'search',
                    'phd students', 'masters', 'visitors', 'current', 'past', 'emerit', 'researcher',
                    'back', 'menu', 'skip', 'navigation', 'department'])):
                if full not in profile_map or len(name) > len(profile_map[full]):
                    profile_map[full] = name
        
        if profile_map:
            log(f"    -> Found {len(profile_map)} profile links, visiting...")
            for prof_url, prof_name in list(profile_map.items())[:150]:
                psoup, _ = get_soup(prof_url, session)
                if psoup is None:
                    continue
                pmailto = extract_mailto_emails(psoup)
                ptext = extract_berkeley_emails(psoup.get_text())
                pemails = list(set(pmailto + ptext))
                ppersonal = [e for e in pemails if not is_admin_email(e)]
                if ppersonal:
                    results.append({
                        'email': ppersonal[0],
                        'name': prof_name,
                        'department': 'Statistics',
                        'source_url': prof_url,
                    })
                time.sleep(0.3)
        
        time.sleep(0.5)
    
    log(f"  TOTAL Statistics: {len(results)} emails")
    return results


# ============================================================
# EECS - More thorough exploration
# ============================================================

def scrape_eecs_deep(session):
    """Deep EECS scraping."""
    log("\n" + "=" * 60)
    log("EECS - Deep exploration")
    log("=" * 60)

    results = []
    
    # Try the EECS yearbook/people pages
    urls_to_try = [
        "https://eecs.berkeley.edu/people/students",
        "https://eecs.berkeley.edu/people",
        "https://www2.eecs.berkeley.edu/Pubs/Grads/",
        "https://www2.eecs.berkeley.edu/Grads/",
    ]
    
    for url in urls_to_try:
        log(f"  Trying: {url}")
        soup, final = get_soup(url, session)
        if soup is None:
            log(f"    -> Failed")
            continue
        
        log(f"    -> Loaded (final: {final})")
        text = soup.get_text()
        emails = extract_berkeley_emails(text)
        mailto = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto))
        personal = [e for e in all_emails if not is_admin_email(e)]
        
        if personal:
            log(f"    -> Found {len(personal)} emails")
            for email in personal:
                name = name_from_email(email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'EECS',
                    'source_url': final or url,
                })
        
        # Follow profile links
        profile_map = {}
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            full = urljoin(final or url, href)
            name = a_tag.get_text(strip=True)
            if (name and len(name) > 2 and '@' not in name
                and not any(x in name.lower() for x in ['faculty', 'staff', 'people', 'home', 'search',
                    'graduate', 'student', 'menu', 'skip', 'navigation', 'department', 'back',
                    'about', 'research', 'academics', 'news', 'events'])):
                # Only follow person-like links
                if re.search(r'/people/\w', href) or re.search(r'/Grads/\w', href):
                    if full not in profile_map or len(name) > len(profile_map[full]):
                        profile_map[full] = name
        
        if profile_map:
            log(f"    -> Found {len(profile_map)} profile links, visiting...")
            for prof_url, prof_name in list(profile_map.items())[:100]:
                psoup, _ = get_soup(prof_url, session)
                if psoup is None:
                    continue
                pmailto = extract_mailto_emails(psoup)
                ptext = extract_berkeley_emails(psoup.get_text())
                pemails = list(set(pmailto + ptext))
                ppersonal = [e for e in pemails if not is_admin_email(e)]
                if ppersonal:
                    results.append({
                        'email': ppersonal[0],
                        'name': prof_name,
                        'department': 'EECS',
                        'source_url': prof_url,
                    })
                time.sleep(0.3)
        
        time.sleep(0.5)
    
    log(f"  TOTAL EECS: {len(results)} emails")
    return results


# ============================================================
# Additional Physics profile scraping (augment pass 2)
# ============================================================

def scrape_more_physics(session, existing_emails):
    """Scrape remaining Physics profiles not covered in pass 2."""
    log("\n" + "=" * 60)
    log("PHYSICS - Additional profile scraping")
    log("=" * 60)

    results = []
    base = "https://physics.berkeley.edu"
    
    # Collect grad student profile URLs from ALL pages
    profile_map = {}
    
    for page_num in range(0, 26):
        url = f"{base}/people/graduate-students" if page_num == 0 else f"{base}/people/graduate-students?page={page_num}"
        soup, _ = get_soup(url, session)
        if soup is None:
            continue
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if '/people/graduate-student/' in href:
                full_url = urljoin(base, href)
                name = a_tag.get_text(strip=True)
                if name and len(name) > 2:
                    if full_url not in profile_map or len(name) > len(profile_map[full_url]):
                        profile_map[full_url] = name
        
        time.sleep(0.3)
    
    log(f"  Found {len(profile_map)} total physics grad profiles")
    
    # Visit each for email
    for i, (url, name) in enumerate(profile_map.items()):
        if i % 30 == 0:
            log(f"  [{i+1}/{len(profile_map)}] Processing...")
        
        soup, _ = get_soup(url, session)
        if soup is None:
            continue
        
        mailto = extract_mailto_emails(soup)
        text_emails = extract_berkeley_emails(soup.get_text())
        all_emails = list(set(mailto + text_emails))
        personal = [e for e in all_emails if not is_admin_email(e) and e not in existing_emails]
        
        if personal:
            results.append({
                'email': personal[0],
                'name': name,
                'department': 'Physics',
                'source_url': url,
            })
        
        time.sleep(0.25)
    
    log(f"  NEW Physics emails: {len(results)}")
    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    
    # Load current results
    current_results = []
    try:
        with open('berkeley_dept_emails.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                current_results.append(row)
        log(f"Loaded {len(current_results)} current results")
    except FileNotFoundError:
        log("No current results found")
    
    existing_emails = set(r['email'].lower().strip() for r in current_results)
    
    # Scrape missing departments
    all_new = []
    
    # Political Science (was 0 due to bug)
    polisci = scrape_polisci(session)
    for r in polisci:
        if r['email'] not in existing_emails:
            existing_emails.add(r['email'])
            all_new.append(r)
    
    # Statistics (was 0)
    stats = scrape_statistics(session)
    for r in stats:
        if r['email'] not in existing_emails:
            existing_emails.add(r['email'])
            all_new.append(r)
    
    # EECS (was only 6)
    eecs = scrape_eecs_deep(session)
    for r in eecs:
        if r['email'] not in existing_emails:
            existing_emails.add(r['email'])
            all_new.append(r)
    
    # More Physics profiles
    physics = scrape_more_physics(session, existing_emails)
    for r in physics:
        if r['email'] not in existing_emails:
            existing_emails.add(r['email'])
            all_new.append(r)
    
    log(f"\nNew emails from pass 3: {len(all_new)}")
    
    # Merge with existing
    final = current_results + all_new
    
    # Deduplicate
    seen = set()
    deduped = []
    for r in final:
        email = r['email'].lower().strip()
        if email not in seen:
            seen.add(email)
            deduped.append(r)
    final = deduped
    
    log(f"\n{'=' * 70}")
    log(f"FINAL MERGED RESULTS")
    log(f"{'=' * 70}")
    log(f"Total unique @berkeley.edu emails: {len(final)}")
    
    # Save
    with open('berkeley_dept_emails.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in final:
            writer.writerow(r)
    log(f"Saved to berkeley_dept_emails.csv")
    
    with open('berkeley_dept_emails.json', 'w') as f:
        json.dump(final, f, indent=2)
    log(f"Saved to berkeley_dept_emails.json")
    
    # Summary
    log(f"\n{'=' * 70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in final:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")
    
    log(f"\n  GRAND TOTAL: {len(final)}")


if __name__ == '__main__':
    main()
