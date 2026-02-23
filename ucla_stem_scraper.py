#!/usr/bin/env python3
"""
UCLA STEM Research Lab Email Scraper
Scrapes student/researcher emails from various UCLA lab and department pages.
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
from urllib.parse import urljoin

# Headers to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# All URLs to scrape
URLS = [
    # Bio/Math/Stats departments
    ("https://www.biostat.ucla.edu/people/students", "Biostatistics"),
    ("https://biomath.ucla.edu/people/students", "Biomathematics"),
    ("https://www.cmbi.ucla.edu/people", "CMBI (Computational Medicine)"),
    ("https://qcb.ucla.edu/people/students/", "QCB (Quantitative & Computational Biology)"),
    ("https://www.mbi.ucla.edu/people/", "MBI (Molecular Biology Institute)"),
    ("https://www.ipam.ucla.edu/people/", "IPAM (Institute for Pure & Applied Math)"),
    ("https://compmed.ucla.edu/people", "Computational Medicine"),
    
    # Engineering labs
    ("https://structures.seas.ucla.edu/people/", "Structures Lab / SEAS"),
    ("https://aerospacerobotics.seas.ucla.edu/people", "Aerospace Robotics / SEAS"),
    ("https://www.biomechatronics.seas.ucla.edu/people", "Biomechatronics / SEAS"),
    ("https://www.picoelectronics.ee.ucla.edu/people/", "Pico Electronics Lab / EE"),
    ("https://www.rarl.ee.ucla.edu/people/", "RARL (Reconfigurable Antenna) / EE"),
    ("https://tanglab.seas.ucla.edu/people", "Tang Lab / SEAS"),
    ("https://xialab.seas.ucla.edu/people", "Xia Lab / SEAS"),
    ("https://zhanglab.seas.ucla.edu/people", "Zhang Lab / SEAS"),
    
    # Math department
    ("https://ww3.math.ucla.edu/people/graduate-students/", "Mathematics"),
]

# Additional URL variations to try
ADDITIONAL_URLS = [
    ("https://tanglab.seas.ucla.edu/people/", "Tang Lab / SEAS"),
    ("https://xialab.seas.ucla.edu/people/", "Xia Lab / SEAS"),
    ("https://zhanglab.seas.ucla.edu/people/", "Zhang Lab / SEAS"),
    ("https://aerospacerobotics.seas.ucla.edu/people/", "Aerospace Robotics / SEAS"),
    ("https://www.biomechatronics.seas.ucla.edu/people/", "Biomechatronics / SEAS"),
    ("https://structures.seas.ucla.edu/people", "Structures Lab / SEAS"),
    ("https://compmed.ucla.edu/people/", "Computational Medicine"),
    ("https://www.cmbi.ucla.edu/people/", "CMBI (Computational Medicine)"),
    ("https://biomath.ucla.edu/people/students/", "Biomathematics"),
    ("https://www.biostat.ucla.edu/people/students/", "Biostatistics"),
    # Sub-pages that might have students
    ("https://qcb.ucla.edu/people/", "QCB (Quantitative & Computational Biology)"),
    ("https://www.mbi.ucla.edu/people/graduate-students/", "MBI (Molecular Biology Institute)"),
    ("https://www.ipam.ucla.edu/people/staff/", "IPAM (Institute for Pure & Applied Math)"),
    ("https://tanglab.seas.ucla.edu/team", "Tang Lab / SEAS"),
    ("https://tanglab.seas.ucla.edu/team/", "Tang Lab / SEAS"),
    ("https://xialab.seas.ucla.edu/team", "Xia Lab / SEAS"),
    ("https://xialab.seas.ucla.edu/team/", "Xia Lab / SEAS"),
    ("https://zhanglab.seas.ucla.edu/team", "Zhang Lab / SEAS"),
    ("https://zhanglab.seas.ucla.edu/team/", "Zhang Lab / SEAS"),
    ("https://tanglab.seas.ucla.edu/members", "Tang Lab / SEAS"),
    ("https://xialab.seas.ucla.edu/members", "Xia Lab / SEAS"),
    ("https://zhanglab.seas.ucla.edu/members", "Zhang Lab / SEAS"),
    # More SEAS labs
    ("https://www.rarl.ee.ucla.edu/people", "RARL / EE"),
    ("https://www.picoelectronics.ee.ucla.edu/people", "Pico Electronics Lab / EE"),
    # Biostat sub-pages
    ("https://www.biostat.ucla.edu/people/phd-students", "Biostatistics"),
    ("https://www.biostat.ucla.edu/people/ms-students", "Biostatistics"),
    ("https://biostat.ucla.edu/people/students", "Biostatistics"),
    ("https://biostat.ucla.edu/people/phd-students", "Biostatistics"),
    # Math extra pages
    ("https://ww3.math.ucla.edu/people/graduate-students", "Mathematics"),
    ("https://www.math.ucla.edu/people/graduate-students/", "Mathematics"),
    ("https://www.math.ucla.edu/people/grad/", "Mathematics"),
]


def extract_emails_from_text(text):
    """Extract all UCLA email addresses from text."""
    # Match emails ending in @g.ucla.edu, @ucla.edu, @cs.ucla.edu, @ee.ucla.edu, etc.
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*ucla\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    # Clean up and deduplicate
    emails = list(set(e.lower().strip() for e in emails))
    return emails


def extract_emails_from_mailto(soup):
    """Extract emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            email_match = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*ucla\.edu)', href, re.IGNORECASE)
            if email_match:
                emails.append(email_match.group(1).lower())
    return emails


def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email on the page."""
    # Strategy 1: Look for mailto link and get surrounding text
    for a_tag in soup.find_all('a', href=True):
        if email in a_tag.get('href', '').lower():
            # Check parent elements for name
            parent = a_tag.parent
            for _ in range(5):  # Go up 5 levels
                if parent is None:
                    break
                text = parent.get_text(separator=' ', strip=True)
                # Try to find a name-like string (not the email itself)
                parts = text.split(email)
                for part in parts:
                    part = part.strip(' ,|-•·')
                    # Check if it looks like a name (2-4 words, no special chars)
                    words = part.split()
                    if 2 <= len(words) <= 5:
                        name_candidate = ' '.join(words[:4])
                        # Filter out common non-name strings
                        if not any(x in name_candidate.lower() for x in ['student', 'professor', 'phd', 'email', 'phone', 'address', 'http', 'www']):
                            return name_candidate
                parent = parent.parent
    
    # Strategy 2: Look for the email as text and check nearby elements
    email_text_elements = soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE))
    for elem in email_text_elements:
        parent = elem.parent
        for _ in range(5):
            if parent is None:
                break
            # Look for headings or strong tags nearby
            name_tags = parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b'])
            for tag in name_tags:
                name = tag.get_text(strip=True)
                if name and not any(x in name.lower() for x in ['email', 'contact', '@', 'student', 'people']):
                    return name
            parent = parent.parent
    
    # Strategy 3: Try to derive from the email prefix
    prefix = email.split('@')[0]
    # Common patterns: firstname.lastname, firstlast, flastname
    return ""


def extract_people_structured(soup, url, department):
    """Try to extract structured people data from common page layouts."""
    results = []
    
    # Common patterns for people listings
    # Pattern 1: Cards/divs with class containing 'person', 'member', 'people', 'profile'
    person_selectors = [
        '[class*="person"]', '[class*="member"]', '[class*="people"]',
        '[class*="profile"]', '[class*="team"]', '[class*="card"]',
        '[class*="faculty"]', '[class*="student"]', '[class*="staff"]',
        '.view-content .views-row', '.person-item', '.team-member',
        '.grid-item', '.person-card', '.staff-member',
        'article', '.entry', '.post'
    ]
    
    for selector in person_selectors:
        try:
            cards = soup.select(selector)
            for card in cards:
                text = card.get_text(separator=' ', strip=True)
                emails = extract_emails_from_text(text)
                mailto_emails = extract_emails_from_mailto(card)
                all_emails = list(set(emails + mailto_emails))
                
                for email in all_emails:
                    name = ""
                    role = ""
                    
                    # Try to get name from headings within the card
                    for tag in card.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a']):
                        tag_text = tag.get_text(strip=True)
                        if tag_text and '@' not in tag_text and len(tag_text) > 3:
                            if not any(x in tag_text.lower() for x in ['email', 'contact', 'phone', 'http', 'read more', 'lab', 'department']):
                                name = tag_text
                                break
                    
                    # Try to determine role
                    text_lower = text.lower()
                    if 'phd' in text_lower or 'doctoral' in text_lower:
                        role = "PhD Student"
                    elif 'graduate' in text_lower or 'grad student' in text_lower:
                        role = "Graduate Student"
                    elif 'postdoc' in text_lower:
                        role = "Postdoc"
                    elif 'master' in text_lower or 'ms student' in text_lower:
                        role = "MS Student"
                    elif 'undergrad' in text_lower:
                        role = "Undergraduate"
                    elif 'research assistant' in text_lower:
                        role = "Research Assistant"
                    elif 'student' in text_lower:
                        role = "Student"
                    else:
                        role = "Researcher"
                    
                    results.append({
                        'email': email,
                        'name': name,
                        'role': role,
                        'department': department,
                        'source_url': url
                    })
        except Exception:
            continue
    
    return results


def scrape_page(url, department, session):
    """Scrape a single page for emails."""
    results = []
    
    try:
        print(f"\n{'='*60}")
        print(f"Scraping: {url}")
        print(f"Department: {department}")
        
        response = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        
        if response.status_code != 200:
            print(f"  HTTP {response.status_code} - Skipping")
            return results
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text()
        
        # Extract all emails from the page
        text_emails = extract_emails_from_text(page_text)
        mailto_emails = extract_emails_from_mailto(soup)
        
        # Also check for obfuscated emails (common pattern: name [at] ucla.edu)
        obfuscated = re.findall(r'([\w.+-]+)\s*\[at\]\s*((?:[\w-]+\.)*ucla\.edu)', page_text, re.IGNORECASE)
        obfuscated_emails = [f"{m[0]}@{m[1]}".lower() for m in obfuscated]
        
        # Also check for JavaScript-obfuscated emails
        script_emails = []
        for script in soup.find_all('script'):
            if script.string:
                script_emails.extend(extract_emails_from_text(script.string))
        
        all_emails = list(set(text_emails + mailto_emails + obfuscated_emails + script_emails))
        
        # Filter to student/researcher emails (exclude department/admin emails)
        filtered_emails = []
        admin_patterns = ['info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@', 'support@', 'contact@', 'grad@', 'registrar@']
        for email in all_emails:
            if not any(email.startswith(p) for p in admin_patterns):
                filtered_emails.append(email)
        
        print(f"  Found {len(filtered_emails)} emails")
        
        # Try structured extraction first
        structured = extract_people_structured(soup, url, department)
        structured_emails = {r['email'] for r in structured}
        
        # For emails not found in structured extraction, do basic extraction
        for email in filtered_emails:
            if email not in structured_emails:
                name = try_get_name_for_email(soup, email)
                
                # Determine role from page context
                role = "Student"  # Default
                
                results.append({
                    'email': email,
                    'name': name,
                    'role': role,
                    'department': department,
                    'source_url': url
                })
        
        results.extend(structured)
        
        # Also look for links to sub-pages (like individual profiles)
        # that might contain more emails
        
        for r in results:
            print(f"  -> {r['email']} | {r['name']} | {r['role']}")
        
    except requests.exceptions.ConnectionError:
        print(f"  Connection error - site may be down")
    except requests.exceptions.Timeout:
        print(f"  Timeout - site too slow")
    except Exception as e:
        print(f"  Error: {type(e).__name__}: {e}")
    
    return results


def scrape_math_grad_students(session):
    """Special scraper for math department which may have a different structure."""
    url = "https://ww3.math.ucla.edu/people/graduate-students/"
    department = "Mathematics"
    results = []
    
    try:
        print(f"\n{'='*60}")
        print(f"Scraping (special): {url}")
        
        response = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if response.status_code != 200:
            print(f"  HTTP {response.status_code}")
            # Try alternate URL
            alt_url = "https://www.math.ucla.edu/people/graduate-students/"
            response = session.get(alt_url, headers=HEADERS, timeout=15, allow_redirects=True)
            if response.status_code != 200:
                print(f"  Alternate also failed: HTTP {response.status_code}")
                return results
            url = alt_url
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Math dept often lists students in tables or lists
        # Try tables first
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                row_text = row.get_text(separator=' ', strip=True)
                emails = extract_emails_from_text(row_text)
                mailto_emails = extract_emails_from_mailto(row)
                all_emails = list(set(emails + mailto_emails))
                
                for email in all_emails:
                    name = ""
                    if cells:
                        name = cells[0].get_text(strip=True)
                        if '@' in name:
                            name = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    
                    results.append({
                        'email': email,
                        'name': name,
                        'role': 'Graduate Student',
                        'department': department,
                        'source_url': url
                    })
        
        # Also try general extraction
        page_text = soup.get_text()
        all_emails_page = extract_emails_from_text(page_text)
        existing = {r['email'] for r in results}
        
        for email in all_emails_page:
            if email not in existing:
                name = try_get_name_for_email(soup, email)
                results.append({
                    'email': email,
                    'name': name,
                    'role': 'Graduate Student',
                    'department': department,
                    'source_url': url
                })
        
        print(f"  Found {len(results)} math grad student emails")
        for r in results:
            print(f"  -> {r['email']} | {r['name']}")
            
    except Exception as e:
        print(f"  Error: {type(e).__name__}: {e}")
    
    return results


def load_existing_emails():
    """Load already-scraped emails to avoid duplicates."""
    existing = set()
    
    for csv_file in ['ucla_psych_lab_emails.csv', 'ucla_seas_lab_emails.csv']:
        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing.add(row['email'].lower().strip())
        except FileNotFoundError:
            pass
    
    print(f"Loaded {len(existing)} existing emails to avoid duplicates")
    return existing


def main():
    existing_emails = load_existing_emails()
    all_results = []
    seen_emails = set()
    session = requests.Session()
    
    # Scrape main URLs
    for url, department in URLS:
        results = scrape_page(url, department, session)
        for r in results:
            email = r['email'].lower().strip()
            if email not in seen_emails and email not in existing_emails:
                seen_emails.add(email)
                all_results.append(r)
        time.sleep(1)  # Be polite
    
    # Try additional URL variations
    print("\n\n" + "="*60)
    print("TRYING ADDITIONAL URL VARIATIONS...")
    print("="*60)
    
    for url, department in ADDITIONAL_URLS:
        # Skip if we've already scraped this URL base
        results = scrape_page(url, department, session)
        for r in results:
            email = r['email'].lower().strip()
            if email not in seen_emails and email not in existing_emails:
                seen_emails.add(email)
                all_results.append(r)
        time.sleep(0.5)
    
    # Special math scraper
    math_results = scrape_math_grad_students(session)
    for r in math_results:
        email = r['email'].lower().strip()
        if email not in seen_emails and email not in existing_emails:
            seen_emails.add(email)
            all_results.append(r)
    
    # Save results
    print(f"\n\n{'='*60}")
    print(f"TOTAL NEW EMAILS FOUND: {len(all_results)}")
    print(f"{'='*60}")
    
    # Save to CSV
    output_file = 'ucla_stem_lab_emails.csv'
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'role', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    
    print(f"\nSaved to {output_file}")
    
    # Also save as JSON for easy inspection
    with open('ucla_stem_lab_emails.json', 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print(f"Also saved to ucla_stem_lab_emails.json")
    
    # Print summary by department
    print(f"\n{'='*60}")
    print("SUMMARY BY DEPARTMENT:")
    print(f"{'='*60}")
    dept_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f"  {dept}: {count} emails")
    
    return all_results


if __name__ == '__main__':
    main()
