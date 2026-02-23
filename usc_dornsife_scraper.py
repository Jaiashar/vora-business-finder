#!/usr/bin/env python3
"""
USC Dornsife College Graduate Student Email Scraper
Scrapes graduate student names and @usc.edu emails from Dornsife department pages.

Handles two types of pages:
1. Direct email pages (Economics, Math, Psychology) - emails right on listing page
2. Profile-linked pages (Philosophy, Earth Sciences, English, History, POIR, Sociology) - 
   names on listing page, emails on individual profile pages
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
import sys
from urllib.parse import urljoin


def log(msg):
    """Print with immediate flush."""
    print(msg, flush=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# ============================================================
# DEPARTMENT CONFIGURATIONS
# ============================================================

# Type 1: Pages where emails appear directly on listing pages
DIRECT_EMAIL_PAGES = [
    {
        "department": "Economics",
        "urls": [
            "https://dornsife.usc.edu/econ/doctoral/student-directory/",
        ],
        "paginated": False,
    },
    {
        "department": "Mathematics",
        "urls": [
            "https://dornsife.usc.edu/mathematics/graduate-list/",
        ],
        "paginated": False,  # All on one page
    },
    {
        "department": "Psychology - Clinical Science",
        "urls": [
            "https://dornsife.usc.edu/psyc/graduate-students/",
        ],
        "paginated": False,
    },
    {
        "department": "Psychology - Social Psychology",
        "urls": [
            "https://dornsife.usc.edu/psyc/social-psychology-graduate-students/",
        ],
        "paginated": False,
    },
    {
        "department": "Psychology - Brain & Cognitive Science",
        "urls": [
            "https://dornsife.usc.edu/psyc/graduate-students-and-postdocs/",
        ],
        "paginated": False,
    },
]

# Type 2: Pages where we need to click into individual profiles to get emails
PROFILE_LINKED_PAGES = [
    {
        "department": "Philosophy",
        "base_url": "https://dornsife.usc.edu/phil/graduate-students/",
        "max_pages": 4,
        "profile_base": "https://dornsife.usc.edu",
    },
    {
        "department": "Earth Sciences",
        "base_url": "https://dornsife.usc.edu/earth/people/gradstudents/",
        "max_pages": 2,
        "profile_base": "https://dornsife.usc.edu",
    },
    {
        "department": "English",
        "base_url": "https://dornsife.usc.edu/engl/graduate/students/",
        "max_pages": 4,
        "profile_base": "https://dornsife.usc.edu",
    },
    {
        "department": "History",
        "base_url": "https://dornsife.usc.edu/hist/graduate-studies/current-graduate-students/",
        "max_pages": 3,
        "profile_base": "https://dornsife.usc.edu",
    },
    {
        "department": "Political Science & International Relations",
        "base_url": "https://dornsife.usc.edu/poir/graduate-students/",
        "max_pages": 4,
        "profile_base": "https://dornsife.usc.edu",
    },
    {
        "department": "Sociology",
        "base_url": "https://dornsife.usc.edu/soci/profile_type/graduate/",
        "max_pages": 1,
        "profile_base": "https://dornsife.usc.edu",
    },
]

# Type 3: Additional search pages (MCB, Biological Sciences, etc.)
ADDITIONAL_PAGES = [
    {
        "department": "Molecular & Computational Biology (MCB)",
        "urls": [
            "https://dornsife.usc.edu/mcb/mcb-graduate-students/",
        ],
    },
    {
        "department": "Biological Sciences",
        "urls": [
            "https://dornsife.usc.edu/bisc/graduate/",
            "https://dornsife.usc.edu/meb/people/graduate-students/",
            "https://dornsife.usc.edu/meb/current-students/",
        ],
    },
    {
        "department": "Neuroscience",
        "urls": [
            "https://dornsife.usc.edu/psyc/graduate-students-and-postdocs/",  # already covered
        ],
    },
    {
        "department": "Chemistry",
        "urls": [
            "https://dornsife.usc.edu/chemistry/chemistry-instrumentation-resources/chemistry-resource-personnel/",
            "https://dornsife.usc.edu/chemistry/graduate-program/graduate-school-path/",
        ],
    },
    {
        "department": "Physics & Astronomy",
        "urls": [
            "https://dornsife.usc.edu/physics/graduate-students/",
            "https://dornsife.usc.edu/physics/graduate/",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://dornsife.usc.edu/ling/people/",
        ],
    },
]


def extract_usc_emails(text):
    """Extract all @usc.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*usc\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(e.lower().strip() for e in emails))


def extract_mailto_emails(soup):
    """Extract USC emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', href, re.IGNORECASE)
            if match:
                emails.append(match.group(1).lower().strip())
    return list(set(emails))


def is_admin_email(email):
    """Filter out department/admin emails."""
    admin_patterns = [
        'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@', 'support@',
        'contact@', 'registrar@', 'chemmail@', 'chemgrad@', 'english@', 'philosophy@',
        'physics@', 'poir@', 'dornsife', 'graduate@', 'burleson@', 'danajohn@',
        'sanfordb@', 'ashleylc@',
    ]
    email_lower = email.lower()
    return any(email_lower.startswith(p) or p in email_lower for p in admin_patterns)


def get_soup(url, session):
    """Fetch a page and return BeautifulSoup object."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser')
        else:
            log(f"  HTTP {resp.status_code} for {url}")
            return None
    except Exception as e:
        log(f"  Error fetching {url}: {e}")
        return None


# ============================================================
# SCRAPER: Direct email pages
# ============================================================

def scrape_economics(soup, url):
    """Parse the Economics student directory - structured with h3 names and mailto links."""
    results = []
    # Find all h3 tags (student names) and their adjacent mailto links
    for h3 in soup.find_all('h3'):
        name = h3.get_text(strip=True)
        if not name or '@' in name:
            continue
        
        # Look for mailto link near this h3
        parent = h3.parent
        if parent is None:
            continue
        
        emails = extract_mailto_emails(parent)
        if not emails:
            # Check siblings
            next_sib = h3.find_next_sibling()
            if next_sib:
                emails = extract_mailto_emails(next_sib)
            if not emails:
                # Check the a tag within h3
                a_tag = h3.find_next('a', href=True)
                if a_tag and 'mailto:' in a_tag.get('href', ''):
                    match = re.search(r'mailto:\s*([\w.+-]+@usc\.edu)', a_tag['href'], re.IGNORECASE)
                    if match:
                        emails = [match.group(1).lower()]
        
        for email in emails:
            if not is_admin_email(email):
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'Economics',
                    'source_url': url,
                })
    return results


def scrape_math(soup, url):
    """Parse the Mathematics doctoral students page - 'Email: xxx@usc.edu' format."""
    results = []
    
    for h3 in soup.find_all('h3'):
        name = h3.get_text(strip=True)
        if not name or '@' in name or 'Doctoral' in name:
            continue
        
        # Look for "Email:" text after this h3
        parent = h3.parent
        if parent is None:
            continue
        
        text = parent.get_text(separator='\n', strip=True)
        email_match = re.search(r'Email:\s*([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', text, re.IGNORECASE)
        if email_match:
            email = email_match.group(1).lower().strip()
            if not is_admin_email(email):
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'Mathematics',
                    'source_url': url,
                })
    
    return results


def scrape_psychology_clinical(soup, url):
    """Parse Psychology clinical science grad students - names and advisors, no direct emails."""
    results = []
    
    for h3 in soup.find_all('h3'):
        name = h3.get_text(strip=True)
        if not name or '@' in name or len(name) < 3:
            continue
        # Skip non-name headers
        if any(kw in name.lower() for kw in ['home', 'faculty', 'graduate', 'research', 'major', 'training', 'document', 'student', 'back', 'area']):
            continue
        
        # Try to find an email nearby
        parent = h3.parent
        if parent:
            text = parent.get_text(separator=' ', strip=True)
            emails = extract_usc_emails(text)
            mailto = extract_mailto_emails(parent)
            all_emails = list(set(emails + mailto))
            
            if all_emails:
                for email in all_emails:
                    if not is_admin_email(email):
                        results.append({
                            'email': email,
                            'name': name,
                            'department': 'Psychology - Clinical Science',
                            'source_url': url,
                        })
            else:
                # No email found - try to construct from name
                # USC emails are typically: firstinitial+lastname@usc.edu or similar
                results.append({
                    'email': '',
                    'name': name,
                    'department': 'Psychology - Clinical Science',
                    'source_url': url,
                })
    
    return results


def scrape_psychology_social(soup, url):
    """Parse Psychology social grad students - 'Contact X at email' format.
    
    The page has h3 for names and following paragraphs with bios + 'Contact X at email@usc.edu'
    """
    results = []
    
    # Strategy: get the full text and parse it section by section
    # Each student section starts with an h3 name and ends before the next h3
    h3_tags = soup.find_all('h3')
    student_names = []
    
    for h3 in h3_tags:
        name = h3.get_text(strip=True)
        if not name or '@' in name or len(name) < 3:
            continue
        if any(kw in name.lower() for kw in ['home', 'faculty', 'graduate', 'research', 'back', 'area', 'social psych']):
            continue
        student_names.append((name, h3))
    
    for idx, (name, h3) in enumerate(student_names):
        all_emails = []
        
        # Collect all elements between this h3 and the next
        current = h3.next_sibling
        block_html = []
        while current:
            if hasattr(current, 'name') and current.name == 'h3':
                break
            block_html.append(current)
            current = current.next_sibling
        
        # Also check the parent container and siblings at parent level
        parent = h3.parent
        if parent:
            # Get all siblings after this h3's parent block
            text_block = parent.get_text(separator=' ', strip=True) if parent else ''
            # Check for mailto links in parent
            mailto_e = extract_mailto_emails(parent)
            all_emails.extend(mailto_e)
            text_e = extract_usc_emails(text_block)
            all_emails.extend(text_e)
        
        # Also search the next sibling elements at the same level
        next_el = h3.find_next_sibling()
        while next_el:
            if next_el.name == 'h3':
                break
            el_text = next_el.get_text(separator=' ', strip=True) if hasattr(next_el, 'get_text') else str(next_el)
            el_emails = extract_usc_emails(el_text)
            all_emails.extend(el_emails)
            if hasattr(next_el, 'find_all'):
                mailto_e = extract_mailto_emails(next_el)
                all_emails.extend(mailto_e)
            next_el = next_el.find_next_sibling()
        
        # Broader search: look in the full page text near this name
        page_text = soup.get_text(separator='\n')
        name_idx = page_text.find(name)
        if name_idx >= 0:
            # Get text from name to the next ~2000 chars
            nearby_text = page_text[name_idx:name_idx + 2000]
            # Stop at next student name if found
            for next_name, _ in student_names[idx + 1:idx + 2]:
                stop_idx = nearby_text.find(next_name)
                if stop_idx > 0:
                    nearby_text = nearby_text[:stop_idx]
                    break
            nearby_emails = extract_usc_emails(nearby_text)
            all_emails.extend(nearby_emails)
        
        all_emails = list(set(all_emails))
        
        for email in all_emails:
            if not is_admin_email(email):
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'Psychology - Social Psychology',
                    'source_url': url,
                })
    
    return results


def scrape_psychology_bcs(soup, url):
    """Parse Psychology BCS grad students - 'Email: xxx@usc.edu' format."""
    results = []
    
    for h3 in soup.find_all('h3'):
        name = h3.get_text(strip=True)
        if not name or '@' in name or len(name) < 3:
            continue
        if any(kw in name.lower() for kw in ['home', 'bcs', 'faculty', 'graduate', 'back', 'area']):
            continue
        
        # Look for the email block after this h3
        parent = h3.parent
        if parent is None:
            continue
        
        # Get text from the container around this h3
        container = parent
        all_emails = []
        
        # Check parent and sibling elements
        for el in [parent, h3.find_next_sibling()]:
            if el:
                text = el.get_text(separator=' ', strip=True)
                text_emails = extract_usc_emails(text)
                mailto_e = extract_mailto_emails(el)
                all_emails.extend(text_emails + mailto_e)
        
        # Also look in the broader container
        grandparent = parent.parent if parent else None
        if grandparent:
            gp_text = grandparent.get_text(separator=' ', strip=True)
            # Find email near this name
            name_pos = gp_text.find(name)
            if name_pos >= 0:
                nearby = gp_text[name_pos:name_pos + 500]
                nearby_emails = extract_usc_emails(nearby)
                all_emails.extend(nearby_emails)
        
        all_emails = list(set(all_emails))
        
        for email in all_emails:
            if not is_admin_email(email):
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'Psychology - Brain & Cognitive Science',
                    'source_url': url,
                })
    
    return results


def scrape_direct_email_page(config, session):
    """Scrape a direct-email page using appropriate parser."""
    results = []
    department = config['department']
    
    for url in config['urls']:
        log(f"\n  Fetching: {url}")
        soup = get_soup(url, session)
        if soup is None:
            continue
        
        if department == 'Economics':
            results.extend(scrape_economics(soup, url))
        elif department == 'Mathematics':
            results.extend(scrape_math(soup, url))
        elif department == 'Psychology - Clinical Science':
            results.extend(scrape_psychology_clinical(soup, url))
        elif department == 'Psychology - Social Psychology':
            results.extend(scrape_psychology_social(soup, url))
        elif department == 'Psychology - Brain & Cognitive Science':
            results.extend(scrape_psychology_bcs(soup, url))
        else:
            # Generic extraction
            results.extend(scrape_generic_emails(soup, url, department))
        
        time.sleep(1)
    
    return results


# ============================================================
# SCRAPER: Profile-linked pages  
# ============================================================

def get_paginated_urls(base_url, max_pages):
    """Generate paginated URLs."""
    urls = [base_url]
    for page in range(2, max_pages + 1):
        urls.append(f"{base_url}page/{page}/")
    return urls


def extract_profile_links(soup, profile_base):
    """Extract profile page links and names from a listing page."""
    profiles = []
    
    for h3 in soup.find_all('h3'):
        name = h3.get_text(strip=True)
        if not name or len(name) < 2:
            continue
        if any(kw in name.lower() for kw in ['graduate', 'search', 'people', 'contact', 'current', 'department', 'student']):
            continue
        
        # Look for profile link
        link = h3.find('a', href=True)
        if link:
            href = link['href']
            if href.startswith('/'):
                href = profile_base + href
            elif not href.startswith('http'):
                href = profile_base + '/' + href
            profiles.append({'name': name, 'profile_url': href})
        else:
            # Check parent for link
            parent = h3.parent
            if parent:
                link = parent.find('a', href=True)
                if link and 'profile' in link.get('href', ''):
                    href = link['href']
                    if href.startswith('/'):
                        href = profile_base + href
                    profiles.append({'name': name, 'profile_url': href})
                else:
                    profiles.append({'name': name, 'profile_url': None})
    
    return profiles


def scrape_profile_page(url, session):
    """Scrape an individual profile page for email."""
    soup = get_soup(url, session)
    if soup is None:
        return None
    
    # Try mailto links first
    emails = extract_mailto_emails(soup)
    
    # Try text extraction
    text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_usc_emails(text)
    
    all_emails = list(set(emails + text_emails))
    
    # Filter admin emails
    personal_emails = [e for e in all_emails if not is_admin_email(e)]
    
    return personal_emails[0] if personal_emails else None


def scrape_profile_linked_pages(config, session):
    """Scrape profile-linked department pages."""
    results = []
    department = config['department']
    base_url = config['base_url']
    max_pages = config['max_pages']
    profile_base = config['profile_base']
    
    urls = get_paginated_urls(base_url, max_pages)
    all_profiles = []
    
    for url in urls:
        log(f"\n  Fetching listing: {url}")
        soup = get_soup(url, session)
        if soup is None:
            continue
        
        profiles = extract_profile_links(soup, profile_base)
        all_profiles.extend(profiles)
        log(f"    Found {len(profiles)} students on this page")
        time.sleep(0.5)
    
    log(f"  Total students found for {department}: {len(all_profiles)}")
    
    # Now visit each profile page to get emails
    for i, profile in enumerate(all_profiles):
        name = profile['name']
        profile_url = profile['profile_url']
        
        if profile_url:
            log(f"    [{i+1}/{len(all_profiles)}] Scraping profile: {name}")
            email = scrape_profile_page(profile_url, session)
            
            if email:
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': profile_url,
                })
            else:
                results.append({
                    'email': '',
                    'name': name,
                    'department': department,
                    'source_url': profile_url or base_url,
                })
            
            time.sleep(0.5)  # Be polite
        else:
            results.append({
                'email': '',
                'name': name,
                'department': department,
                'source_url': base_url,
            })
    
    return results


# ============================================================
# SCRAPER: Generic email extraction
# ============================================================

def scrape_generic_emails(soup, url, department):
    """Generic email extraction from any page."""
    results = []
    
    page_text = soup.get_text(separator=' ', strip=True)
    text_emails = extract_usc_emails(page_text)
    mailto_emails = extract_mailto_emails(soup)
    
    all_emails = list(set(text_emails + mailto_emails))
    
    for email in all_emails:
        if not is_admin_email(email):
            name = try_get_name_for_email(soup, email)
            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': url,
            })
    
    return results


def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email."""
    # Check mailto links
    for a_tag in soup.find_all('a', href=True):
        if email in a_tag.get('href', '').lower():
            parent = a_tag.parent
            for _ in range(5):
                if parent is None:
                    break
                for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b']):
                    tag_text = tag.get_text(strip=True)
                    if tag_text and '@' not in tag_text and len(tag_text) > 3:
                        if not any(x in tag_text.lower() for x in ['email', 'contact', 'phone', 'http', 'department']):
                            return tag_text
                parent = parent.parent
    return ""


def scrape_additional_pages(config, session):
    """Scrape additional pages for emails."""
    results = []
    department = config['department']
    
    for url in config['urls']:
        log(f"\n  Fetching: {url}")
        soup = get_soup(url, session)
        if soup is None:
            continue
        
        results.extend(scrape_generic_emails(soup, url, department))
        time.sleep(1)
    
    return results


# ============================================================
# Also try to find Physics grad students
# ============================================================

def scrape_physics(session):
    """Try to scrape Physics & Astronomy graduate students."""
    results = []
    urls_to_try = [
        "https://dornsife.usc.edu/physics/graduate-students/",
        "https://dornsife.usc.edu/physics/graduate-students/page/2/",
        "https://dornsife.usc.edu/physics/graduate-students/page/3/",
    ]
    
    all_profiles = []
    
    for url in urls_to_try:
        log(f"\n  Fetching Physics: {url}")
        soup = get_soup(url, session)
        if soup is None:
            continue
        
        # First try generic email extraction
        gen_results = scrape_generic_emails(soup, url, "Physics & Astronomy")
        if gen_results:
            results.extend(gen_results)
        
        # Also try to get profile links
        profiles = extract_profile_links(soup, "https://dornsife.usc.edu")
        all_profiles.extend(profiles)
        time.sleep(0.5)
    
    # Visit profile pages
    for i, profile in enumerate(all_profiles):
        name = profile['name']
        profile_url = profile['profile_url']
        
        if profile_url:
            log(f"    [{i+1}/{len(all_profiles)}] Physics profile: {name}")
            email = scrape_profile_page(profile_url, session)
            
            if email:
                results.append({
                    'email': email,
                    'name': name,
                    'department': 'Physics & Astronomy',
                    'source_url': profile_url,
                })
            else:
                results.append({
                    'email': '',
                    'name': name,
                    'department': 'Physics & Astronomy',
                    'source_url': profile_url,
                })
            time.sleep(0.5)
    
    return results


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_results = []
    seen_emails = set()
    
    def add_results(results):
        count = 0
        for r in results:
            email = r['email'].lower().strip() if r['email'] else ''
            if email and email not in seen_emails:
                seen_emails.add(email)
                all_results.append(r)
                count += 1
            elif not email:
                # Keep entries without email (name only)
                all_results.append(r)
                count += 1
        return count
    
    # ---- Phase 1: Direct email pages ----
    log("=" * 70)
    log("PHASE 1: Scraping direct-email pages")
    log("=" * 70)
    
    for config in DIRECT_EMAIL_PAGES:
        log(f"\n{'='*50}")
        log(f"Department: {config['department']}")
        log(f"{'='*50}")
        results = scrape_direct_email_page(config, session)
        n = add_results(results)
        log(f"  => {n} new entries added ({len([r for r in results if r.get('email')])} with emails)")
    
    # ---- Phase 2: Profile-linked pages ----
    log("\n\n" + "=" * 70)
    log("PHASE 2: Scraping profile-linked pages (visiting individual profiles)")
    log("=" * 70)
    
    for config in PROFILE_LINKED_PAGES:
        log(f"\n{'='*50}")
        log(f"Department: {config['department']}")
        log(f"{'='*50}")
        results = scrape_profile_linked_pages(config, session)
        n = add_results(results)
        log(f"  => {n} new entries added ({len([r for r in results if r.get('email')])} with emails)")
    
    # ---- Phase 3: Physics (special handling) ----
    log("\n\n" + "=" * 70)
    log("PHASE 3: Scraping Physics & Astronomy")
    log("=" * 70)
    results = scrape_physics(session)
    n = add_results(results)
    log(f"  => {n} new entries added")
    
    # ---- Phase 4: Additional pages ----
    log("\n\n" + "=" * 70)
    log("PHASE 4: Scraping additional/supplementary pages")
    log("=" * 70)
    
    for config in ADDITIONAL_PAGES:
        log(f"\n{'='*50}")
        log(f"Department: {config['department']}")
        log(f"{'='*50}")
        results = scrape_additional_pages(config, session)
        n = add_results(results)
        log(f"  => {n} new entries added")
    
    # ---- Save results ----
    # Separate results with emails vs without
    with_email = [r for r in all_results if r.get('email')]
    without_email = [r for r in all_results if not r.get('email')]
    
    log(f"\n\n{'='*70}")
    log(f"RESULTS SUMMARY")
    log(f"{'='*70}")
    log(f"Total entries with @usc.edu emails: {len(with_email)}")
    log(f"Total entries without emails (name only): {len(without_email)}")
    log(f"Total unique emails: {len(seen_emails)}")
    
    # Save CSV with all entries
    output_csv = 'usc_dornsife_grad_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    log(f"\nSaved all entries to {output_csv}")
    
    # Save JSON
    output_json = 'usc_dornsife_grad_emails.json'
    with open(output_json, 'w') as f:
        json.dump(all_results, f, indent=2)
    log(f"Saved all entries to {output_json}")
    
    # Print summary by department
    log(f"\n{'='*70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'='*70}")
    dept_counts = {}
    dept_email_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
        if r.get('email'):
            dept_email_counts[dept] = dept_email_counts.get(dept, 0) + 1
    
    for dept in sorted(dept_counts.keys()):
        total = dept_counts[dept]
        emails = dept_email_counts.get(dept, 0)
        log(f"  {dept}: {total} total ({emails} with emails)")
    
    # Print all email entries
    log(f"\n{'='*70}")
    log("ALL EMAILS FOUND:")
    log(f"{'='*70}")
    for r in sorted(with_email, key=lambda x: x['department']):
        log(f"  {r['email']:<35} | {r['name']:<30} | {r['department']}")
    
    return all_results


if __name__ == '__main__':
    main()
