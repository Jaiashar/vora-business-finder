#!/usr/bin/env python3
"""
Scrape USC professional school student directories for @usc.edu emails.
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import time
import json

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

all_contacts = []

def extract_usc_emails(text):
    """Extract @usc.edu emails from text."""
    pattern = r'[a-zA-Z0-9._%+-]+@(?:[a-zA-Z0-9-]+\.)?usc\.edu'
    return list(set(re.findall(pattern, text)))

def scrape_annenberg():
    """Scrape Annenberg PhD student profiles for emails."""
    print("\n=== ANNENBERG SCHOOL (Communication/Journalism) ===")
    
    # Get the main directory page to find student names and profile links
    url = "https://annenberg.usc.edu/phd-students"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Find all student profile links with their names from the directory page
    students = []  # list of (name, url) tuples
    seen_urls = set()
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '/phd-students/' in href and href != '/phd-students' and href != '/phd-students/':
            full_url = href if href.startswith('http') else f"https://annenberg.usc.edu{href}"
            if full_url not in seen_urls:
                # Get the link text as name
                link_text = link.get_text(strip=True)
                if link_text and link_text not in ['Phd Students', 'PhD Students', '']:
                    seen_urls.add(full_url)
                    students.append((link_text, full_url))
    
    print(f"Found {len(students)} student profile links")
    
    contacts = []
    for i, (dir_name, profile_url) in enumerate(students):
        try:
            time.sleep(0.5)
            resp = requests.get(profile_url, headers=HEADERS, timeout=30)
            psoup = BeautifulSoup(resp.text, 'html.parser')
            
            # Get name: prefer from directory listing, fallback to h1 on profile page
            name = dir_name
            h1 = psoup.find('h1')
            if h1:
                h1_text = h1.get_text(strip=True)
                # Only use h1 if it looks like a real name (not site title)
                if h1_text and 'looking for' not in h1_text.lower() and len(h1_text) < 60:
                    name = h1_text
            
            # Get email from mailto links
            email = ""
            for mailto in psoup.find_all('a', href=True):
                if 'mailto:' in mailto['href'] and 'usc.edu' in mailto['href']:
                    email = mailto['href'].replace('mailto:', '').strip()
                    break
            
            if not email:
                # Try extracting from page text
                emails = extract_usc_emails(resp.text)
                # Filter out gradasc@ and other admin emails
                emails = [e for e in emails if not any(x in e for x in ['gradasc', 'admin', 'info', 'contact'])]
                if emails:
                    email = emails[0]
            
            if email and name:
                contacts.append({
                    'email': email,
                    'name': name,
                    'school': 'USC Annenberg School for Communication and Journalism',
                    'program': 'Communication PhD'
                })
                print(f"  [{i+1}/{len(students)}] {name}: {email}")
            else:
                print(f"  [{i+1}/{len(students)}] {name}: NO EMAIL FOUND")
                
        except Exception as e:
            print(f"  Error fetching {profile_url}: {e}")
    
    print(f"\nAnnenberg total: {len(contacts)} contacts with emails")
    return contacts


def scrape_rossier():
    """Scrape Rossier School of Education PhD student directory."""
    print("\n=== ROSSIER SCHOOL OF EDUCATION ===")
    
    # Rossier directory is paginated - collect all pages
    all_html = ""
    for page in range(0, 5):  # pages 0-4 should cover all students
        page_url = f"https://rossier.usc.edu/programs/doctoral-degree-programs/directory?page={page}"
        try:
            resp = requests.get(page_url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                all_html += resp.text
                print(f"  Fetched page {page}")
            else:
                break
        except Exception:
            break
        time.sleep(0.3)
    
    soup = BeautifulSoup(all_html, 'html.parser')
    
    contacts = []
    seen_emails = set()
    
    # Find all student entries - each student is in a list item or article
    # Look for all mailto links with usc.edu
    for mailto in soup.find_all('a', href=True):
        href = mailto['href']
        if 'mailto:' in href and 'usc.edu' in href:
            email = href.replace('mailto:', '').strip()
            if email in seen_emails:
                continue
            seen_emails.add(email)
            
            # Find the associated name - traverse up to find h2
            name = ""
            concentration = ""
            
            # Go up through parents to find the student container
            parent = mailto
            for _ in range(25):
                parent = parent.parent
                if parent is None:
                    break
                
                # Look for h2 with name
                h2 = parent.find('h2')
                if h2:
                    name_link = h2.find('a')
                    name = name_link.get_text(strip=True) if name_link else h2.get_text(strip=True)
                    
                    # Now look for research concentration in same container
                    text = parent.get_text()
                    for conc in ['Higher Education', 'K-12 Education Policy', 'Educational Psychology', 'Teacher Education']:
                        if conc in text:
                            concentration = conc
                            break
                    break
            
            if email and name:
                contacts.append({
                    'email': email,
                    'name': name,
                    'school': 'USC Rossier School of Education',
                    'program': f'PhD - {concentration}' if concentration else 'PhD in Education'
                })
                print(f"  {name}: {email} ({concentration})")
    
    # Also check individual student profile pages linked from directory
    profile_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '/directory/' in href and href != '/programs/doctoral-degree-programs/directory':
            full_url = href if href.startswith('http') else f"https://rossier.usc.edu{href}"
            if full_url not in profile_links and '/directory/' in full_url:
                profile_links.append(full_url)
    
    print(f"  Found {len(profile_links)} individual profile links to check")
    
    for profile_url in profile_links:
        try:
            time.sleep(0.3)
            resp2 = requests.get(profile_url, headers=HEADERS, timeout=15)
            psoup = BeautifulSoup(resp2.text, 'html.parser')
            
            for mailto in psoup.find_all('a', href=True):
                if 'mailto:' in mailto['href'] and 'usc.edu' in mailto['href']:
                    email = mailto['href'].replace('mailto:', '').strip()
                    if email in seen_emails:
                        continue
                    seen_emails.add(email)
                    
                    h1 = psoup.find('h1')
                    name = h1.get_text(strip=True) if h1 else ""
                    
                    if email and name:
                        contacts.append({
                            'email': email,
                            'name': name,
                            'school': 'USC Rossier School of Education',
                            'program': 'PhD in Education'
                        })
                        print(f"  [profile] {name}: {email}")
        except Exception:
            pass
    
    print(f"\nRossier total: {len(contacts)} contacts with emails")
    return contacts


def find_closest_h2_before(element, soup):
    """Find the closest h2 that appears before this element in the document."""
    # Get all h2 elements
    all_h2s = soup.find_all('h2')
    
    # Strategy: find which h2 this email "belongs to" by checking document order
    # Get the position of the element in the document
    all_cf = soup.find_all(attrs={'data-cfemail': True})
    
    # Find index of current element among all cf elements
    cf_index = -1
    for i, cf in enumerate(all_cf):
        if cf.get('data-cfemail') == element.get('data-cfemail') and cf is element:
            cf_index = i
            break
    
    # The h2s and cf-emails should roughly alternate (h2 then email, h2 then email)
    # Let's pair them up by position in the HTML string
    element_str = str(element)
    element_pos = str(soup).find(element_str)
    
    closest_h2 = None
    closest_dist = float('inf')
    for h2 in all_h2s:
        h2_pos = str(soup).find(str(h2))
        if h2_pos < element_pos:
            dist = element_pos - h2_pos
            if dist < closest_dist:
                closest_dist = dist
                closest_h2 = h2
    
    return closest_h2.get_text(strip=True) if closest_h2 else ""


def scrape_price_generic(url, program_name):
    """Scrape Price School PhD students from a given URL."""
    print(f"\n=== PRICE SCHOOL - {program_name} ===")
    
    resp = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    contacts = []
    seen_emails = set()
    
    # Strategy: find all h2 elements (student names) and the CF emails under each
    # Build ordered list of (element_type, element) tuples based on document position
    html_str = str(soup)
    
    # Get all h2s and cf-email elements with their positions
    items = []
    for h2 in soup.find_all('h2'):
        h2_str = str(h2)
        pos = html_str.find(h2_str)
        items.append(('h2', h2.get_text(strip=True), pos))
    
    for cf_el in soup.find_all(attrs={'data-cfemail': True}):
        encoded = cf_el.get('data-cfemail', '')
        email = decode_cf_email(encoded) if encoded else ''
        cf_str = str(cf_el)
        pos = html_str.find(cf_str)
        items.append(('email', email, pos))
    
    # Sort by position
    items.sort(key=lambda x: x[2])
    
    # Now pair each email with the most recent h2 before it
    current_name = ""
    for item_type, value, pos in items:
        if item_type == 'h2':
            current_name = value
        elif item_type == 'email' and value and 'usc.edu' in value:
            if value not in seen_emails:
                seen_emails.add(value)
                contacts.append({
                    'email': value,
                    'name': current_name,
                    'school': 'USC Price School of Public Policy',
                    'program': program_name
                })
                print(f"  {current_name}: {value}")
    
    print(f"\nPrice {program_name} total: {len(contacts)} contacts with emails")
    return contacts


def scrape_price_ppm():
    return scrape_price_generic(
        "https://priceschool.usc.edu/academics/doctoral/ph-d-in-public-policy-and-management-ppm/public-policy-and-management-ph-d-students/",
        "PhD in Public Policy and Management"
    )


def scrape_price_upd():
    return scrape_price_generic(
        "https://priceschool.usc.edu/academics/doctoral/ph-d-in-urban-planning-and-development-dupd/urban-planning-and-development-phd-students/",
        "PhD in Urban Planning and Development"
    )


def decode_cf_email(encoded_string):
    """Decode CloudFlare email protection."""
    try:
        r = int(encoded_string[:2], 16)
        email = ''
        for i in range(2, len(encoded_string), 2):
            email += chr(int(encoded_string[i:i+2], 16) ^ r)
        return email
    except Exception:
        return ''


def scrape_gould_orgs():
    """Scrape Gould School of Law student organization contact emails."""
    print("\n=== GOULD SCHOOL OF LAW (Student Organizations) ===")
    
    url = "https://gould.usc.edu/students/student-organizations-associations/"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    contacts = []
    seen_emails = set()
    
    # Find all elements with data-cfemail attribute
    cf_elements = soup.select('[data-cfemail]')
    print(f"  Found {len(cf_elements)} CF email elements")
    
    for cf_el in cf_elements:
        encoded = cf_el.get('data-cfemail', '')
        if not encoded:
            continue
        email = decode_cf_email(encoded)
        if not email or email in seen_emails:
            continue
        # Accept both @usc.edu and @lawmail.usc.edu
        if 'usc.edu' not in email:
            continue
        seen_emails.add(email)
        
        # Try to find org name by traversing up
        org_name = "Student Organization"
        parent = cf_el.parent
        while parent:
            # Look for preceding text that might be the org name
            prev = parent.find_previous_sibling()
            if prev:
                text = prev.get_text(strip=True)
                if text and len(text) > 5 and 'Contact' not in text and '@' not in text:
                    org_name = text[:100]
                    break
            parent = parent.parent
            if parent and parent.name in ['body', 'html']:
                break
        
        contacts.append({
            'email': email,
            'name': org_name,
            'school': 'USC Gould School of Law',
            'program': 'Student Organization'
        })
        print(f"  {org_name}: {email}")
    
    # Fallback: extract emails directly from HTML
    if not contacts:
        print("  CF approach failed, trying direct email extraction...")
        emails = extract_usc_emails(resp.text)
        admin_patterns = ['admissions@law', 'careers@law', 'dean@']
        for email in emails:
            if email not in seen_emails and not any(p in email for p in admin_patterns):
                seen_emails.add(email)
                contacts.append({
                    'email': email,
                    'name': 'Student Organization',
                    'school': 'USC Gould School of Law',
                    'program': 'Student Organization'
                })
                print(f"  Direct: {email}")
    
    print(f"\nGould total: {len(contacts)} org contacts with emails")
    return contacts


def scrape_marshall():
    """Try to find Marshall PhD student directory."""
    print("\n=== MARSHALL SCHOOL OF BUSINESS ===")
    
    # Try various URL patterns
    urls_to_try = [
        "https://www.marshall.usc.edu/programs/graduate-programs/phd-program/accounting",
        "https://www.marshall.usc.edu/programs/graduate-programs/phd-program/finance",
        "https://www.marshall.usc.edu/programs/graduate-programs/phd-program/management-organization",
        "https://www.marshall.usc.edu/programs/graduate-programs/phd-program/marketing",
        "https://www.marshall.usc.edu/programs/graduate-programs/phd-program/data-sciences-operations",
    ]
    
    contacts = []
    for url in urls_to_try:
        try:
            time.sleep(0.5)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            emails = extract_usc_emails(resp.text)
            
            # Filter out admin/faculty emails
            student_emails = [e for e in emails if 'phd@' not in e and 'marshall' not in e.split('@')[0]]
            
            if student_emails:
                print(f"  Found emails on {url}:")
                for email in student_emails:
                    contacts.append({
                        'email': email,
                        'name': '',
                        'school': 'USC Marshall School of Business',
                        'program': 'PhD'
                    })
                    print(f"    {email}")
            else:
                print(f"  No student emails on: {url.split('/')[-1]}")
        except Exception as e:
            print(f"  Error: {e}")
    
    # Try the PhD students/current students pages
    try:
        for path in ['/phd/current-students', '/phd/students', '/programs/graduate-programs/phd-program/current-students']:
            url = f"https://www.marshall.usc.edu{path}"
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                emails = extract_usc_emails(resp.text)
                if emails:
                    print(f"  Found emails on {url}: {emails}")
    except Exception:
        pass
    
    print(f"\nMarshall total: {len(contacts)} contacts with emails")
    return contacts


def scrape_social_work():
    """Try Dworak-Peck School of Social Work."""
    print("\n=== DWORAK-PECK SCHOOL OF SOCIAL WORK ===")
    
    url = "https://dworakpeck.usc.edu/academic-programs/doctor-of-philosophy/phd-current-student-bios"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    
    contacts = []
    emails = extract_usc_emails(resp.text)
    
    if emails:
        for email in emails:
            contacts.append({
                'email': email,
                'name': '',
                'school': 'USC Suzanne Dworak-Peck School of Social Work',
                'program': 'PhD in Social Work'
            })
            print(f"  {email}")
    else:
        print("  No student emails found on bios page")
    
    # Also check the staff directory for any student entries
    soup = BeautifulSoup(resp.text, 'html.parser')
    for cf_email in soup.find_all('a', class_='__cf_email__'):
        encoded = cf_email.get('data-cfemail', '')
        if encoded:
            email = decode_cf_email(encoded)
            if email and 'usc.edu' in email:
                print(f"  Decoded CF email: {email}")
    
    print(f"\nSocial Work total: {len(contacts)} contacts with emails")
    return contacts


def scrape_dornsife_economics():
    """Scrape USC Dornsife Economics PhD student directory."""
    print("\n=== USC DORNSIFE - DEPARTMENT OF ECONOMICS ===")
    
    url = "https://dornsife.usc.edu/econ/doctoral/student-directory/"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    contacts = []
    seen_emails = set()
    
    # Find all mailto links with usc.edu
    for mailto in soup.find_all('a', href=True):
        href = mailto['href']
        if 'mailto:' in href and 'usc.edu' in href:
            email = href.replace('mailto:', '').strip()
            # Clean up email (some have trailing newlines/spaces/URL encoding)
            email = email.strip().split('\n')[0].strip()
            email = email.replace('%20', '').replace(' ', '')
            
            if email in seen_emails or not email:
                continue
            seen_emails.add(email)
            
            # Find the name from the nearest h3 heading
            parent = mailto
            name = ""
            for _ in range(10):
                parent = parent.parent
                if parent is None:
                    break
                h3 = parent.find('h3')
                if h3:
                    name = h3.get_text(strip=True)
                    # Clean up HTML entities
                    name = name.replace('\u201c', '"').replace('\u201d', '"')
                    break
            
            if email and name:
                contacts.append({
                    'email': email,
                    'name': name,
                    'school': 'USC Dornsife - Department of Economics',
                    'program': 'PhD in Economics'
                })
                print(f"  {name}: {email}")
    
    print(f"\nEconomics total: {len(contacts)} contacts with emails")
    return contacts


def scrape_dornsife_math():
    """Scrape USC Dornsife Mathematics PhD student directory."""
    print("\n=== USC DORNSIFE - DEPARTMENT OF MATHEMATICS ===")
    
    url = "https://dornsife.usc.edu/mathematics/graduate-list/"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    contacts = []
    seen_emails = set()
    
    # The page has h3 for names and "Email: xxx@usc.edu" text
    for h3 in soup.find_all('h3'):
        name = h3.get_text(strip=True)
        if not name or len(name) < 3:
            continue
        
        # Find the next sibling or parent text that contains email
        parent = h3.parent
        if parent:
            text = parent.get_text()
            # Extract email from text pattern "Email: xxx@usc.edu"
            import re
            email_match = re.search(r'Email:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]*usc\.edu)', text)
            if email_match:
                email = email_match.group(1).strip()
                # Fix escaped underscores
                email = email.replace('\\_', '_')
                
                if email not in seen_emails:
                    seen_emails.add(email)
                    # Clean name (remove "Last, First" -> "First Last")
                    if ',' in name:
                        parts = name.split(',', 1)
                        name = f"{parts[1].strip()} {parts[0].strip()}"
                    
                    contacts.append({
                        'email': email,
                        'name': name,
                        'school': 'USC Dornsife - Department of Mathematics',
                        'program': 'PhD in Mathematics'
                    })
                    print(f"  {name}: {email}")
    
    print(f"\nMathematics total: {len(contacts)} contacts with emails")
    return contacts


def try_additional_searches():
    """Try additional USC school pages."""
    print("\n=== ADDITIONAL SCHOOL SEARCHES ===")
    
    contacts = []
    
    # Architecture - skip bulk scraping as the people page has faculty/staff emails, not students
    print("\n--- USC School of Architecture ---")
    print("  No public student email directory found (people page contains faculty/staff)")
    
    # Thornton Music
    print("\n--- USC Thornton School of Music ---")
    for url in ["https://music.usc.edu/students/", "https://music.usc.edu/doctoral-programs/"]:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            emails = extract_usc_emails(resp.text)
            if emails:
                for email in emails:
                    contacts.append({
                        'email': email,
                        'name': '',
                        'school': 'USC Thornton School of Music',
                        'program': 'Student'
                    })
                    print(f"  Thornton: {email}")
        except Exception:
            pass
    
    # Kaufman Dance
    print("\n--- USC Kaufman School of Dance ---")
    try:
        resp = requests.get("https://kaufman.usc.edu/all-students/", headers=HEADERS, timeout=15)
        emails = extract_usc_emails(resp.text)
        if emails:
            for email in emails:
                contacts.append({
                    'email': email,
                    'name': '',
                    'school': 'USC Kaufman School of Dance',
                    'program': 'Student'
                })
                print(f"  Kaufman: {email}")
    except Exception:
        pass
    
    # Keck Medicine
    print("\n--- USC Keck School of Medicine ---")
    for url in ["https://keck.usc.edu/md-program/student-affairs/", "https://keck.usc.edu/biomedical-and-biological-sciences-phd-program/"]:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            emails = extract_usc_emails(resp.text)
            student_emails = [e for e in emails if not any(x in e for x in ['admin', 'info', 'med.usc', 'office'])]
            if student_emails:
                for email in student_emails:
                    contacts.append({
                        'email': email,
                        'name': '',
                        'school': 'USC Keck School of Medicine',
                        'program': 'Student'
                    })
                    print(f"  Keck: {email}")
        except Exception:
            pass
    
    return contacts


def main():
    all_contacts = []
    
    # 1. Annenberg
    annenberg = scrape_annenberg()
    all_contacts.extend(annenberg)
    
    # 2. Rossier
    rossier = scrape_rossier()
    all_contacts.extend(rossier)
    
    # 3. Price PPM
    price_ppm = scrape_price_ppm()
    all_contacts.extend(price_ppm)
    
    # 4. Price UPD
    price_upd = scrape_price_upd()
    all_contacts.extend(price_upd)
    
    # 5. Gould
    gould = scrape_gould_orgs()
    all_contacts.extend(gould)
    
    # 6. Marshall
    marshall = scrape_marshall()
    all_contacts.extend(marshall)
    
    # 7. Social Work
    social_work = scrape_social_work()
    all_contacts.extend(social_work)
    
    # 8. Dornsife Economics
    economics = scrape_dornsife_economics()
    all_contacts.extend(economics)
    
    # 9. Dornsife Mathematics
    math = scrape_dornsife_math()
    all_contacts.extend(math)
    
    # 10. Additional schools
    additional = try_additional_searches()
    all_contacts.extend(additional)
    
    # Deduplicate by email and filter out generic admin/department emails
    admin_prefixes = [
        'thornton.studentaffairs', 'uscdance', 'medstuaf', 'uscmusic',
        'admissions@', 'careers@', 'dean@', 'info@', 'uschr@', 'uscnews@',
        'archdean@', 'archoff@', 'archweb@', 'archcomm@', 'archeven@',
        'archgrad@', 'archadv@', 'archadvs@', 'arcguild@', 'archguild@',
        'buildsci@', 'woodshop@', 'techenti@', 'soahsw@', 'soaipal@',
        'clennox@',  # faculty member, not student
    ]
    
    seen_emails = set()
    unique_contacts = []
    for c in all_contacts:
        email_lower = c['email'].lower()
        if email_lower not in seen_emails:
            # Skip generic admin/department emails
            if any(email_lower.startswith(p) for p in admin_prefixes):
                continue
            seen_emails.add(email_lower)
            unique_contacts.append(c)
    
    # Save to CSV
    csv_path = '/Users/jaiashar/Documents/VoraBusinessFinder/usc_pro_school_emails.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'school', 'program'])
        writer.writeheader()
        for c in unique_contacts:
            writer.writerow(c)
    
    # Save to JSON
    json_path = '/Users/jaiashar/Documents/VoraBusinessFinder/usc_pro_school_emails.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(unique_contacts, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    schools = {}
    for c in unique_contacts:
        school = c['school']
        if school not in schools:
            schools[school] = 0
        schools[school] += 1
    
    for school, count in sorted(schools.items()):
        print(f"  {school}: {count} emails")
    
    print(f"\n  TOTAL UNIQUE EMAILS: {len(unique_contacts)}")
    print(f"\nSaved to:")
    print(f"  CSV: {csv_path}")
    print(f"  JSON: {json_path}")


if __name__ == "__main__":
    main()
