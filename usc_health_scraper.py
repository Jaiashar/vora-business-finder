#!/usr/bin/env python3
"""
Scrape USC Keck School of Medicine, School of Pharmacy (Mann), and health
sciences pages for @usc.edu emails.

Strategy: Most USC health sites use individual profile pages. We:
1. Visit directory/listing pages to collect profile links
2. Visit each profile page to extract name + email
3. Deduplicate and save
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
import sys
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

session = requests.Session()
session.headers.update(HEADERS)


def extract_usc_emails(text):
    """Extract @usc.edu emails from text."""
    raw = re.findall(r'[\w.+-]+@(?:[\w-]+\.)*usc\.edu', text, re.IGNORECASE)
    cleaned = set()
    for e in raw:
        e = e.lower().strip()
        # Remove leading digits
        m = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*usc\.edu)', e)
        if m:
            cleaned.add(m.group(1))
        else:
            cleaned.add(e)
    return list(cleaned)


def decode_cf_email(encoded_string):
    """Decode CloudFlare email protection."""
    try:
        r = int(encoded_string[:2], 16)
        return ''.join(chr(int(encoded_string[i:i+2], 16) ^ r) for i in range(2, len(encoded_string), 2))
    except Exception:
        return ''


def is_admin_email(email):
    """Filter out admin/generic emails."""
    admin = [
        'info@', 'admin@', 'office@', 'webmaster@', 'help@', 'support@',
        'contact@', 'registrar@', 'admissions@', 'dean@', 'chair@',
        'reception@', 'communications@', 'services@', 'affairs@',
        'media@', 'news@', 'marketing@', 'events@', 'giving@',
        'development@', 'alumni@', 'library@', 'itshelp@', 'helpdesk@',
        'press@', 'noreply@', 'do-not-reply@', 'donotreply@',
        'pphsadmissions@', 'global.health@',
    ]
    e = email.lower()
    return any(p in e for p in admin) or e.startswith('email')


def get_page(url, timeout=15):
    """Fetch a page, return (soup, text, response) or (None, None, None)."""
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return None, None, r
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup, r.text, r
    except Exception:
        return None, None, None


def scrape_profile_emails(url):
    """Visit a single profile page and extract name + email."""
    soup, text, resp = get_page(url, timeout=10)
    if not soup:
        return []
    
    results = []
    page_text = soup.get_text()
    
    # Extract emails from text, mailto, CF obfuscation
    text_emails = extract_usc_emails(page_text)
    
    mailto_emails = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'mailto:' in href:
            m = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', href, re.IGNORECASE)
            if m:
                mailto_emails.append(m.group(1).lower())
    
    cf_emails = []
    for el in soup.find_all(attrs={'data-cfemail': True}):
        enc = el.get('data-cfemail', '')
        if enc:
            decoded = decode_cf_email(enc)
            if decoded and 'usc.edu' in decoded.lower():
                cf_emails.append(decoded.lower())
    
    all_emails = list(set(text_emails + mailto_emails + cf_emails))
    all_emails = [e for e in all_emails if not is_admin_email(e)]
    
    # Get name from h1 or title
    name = ""
    h1 = soup.find('h1')
    if h1:
        name = h1.get_text(strip=True)
        # Clean up common patterns
        for remove in ['Faculty Listing', 'Keck School', 'USC', 'People', '404', 'Page Not Found']:
            if remove.lower() in name.lower():
                name = ""
                break
    
    if not name:
        # Try h2
        for h2 in soup.find_all('h2'):
            t = h2.get_text(strip=True)
            if t and len(t) < 60 and len(t.split()) <= 5:
                name = t
                break
    
    for email in all_emails:
        results.append({'email': email, 'name': name})
    
    return results


def collect_profile_links(soup, base_url, pattern_keywords):
    """Collect profile links from a listing page."""
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        if any(kw in href for kw in pattern_keywords):
            if href.startswith('http'):
                links.add(href)
            elif href.startswith('/'):
                parsed = urlparse(base_url)
                links.add(f"{parsed.scheme}://{parsed.netloc}{href}")
            else:
                links.add(urljoin(base_url, href))
    return list(links)


def scrape_keck_departments():
    """Scrape Keck department faculty archive pages -> profile links -> emails."""
    print("\n" + "=" * 70)
    print("KECK SCHOOL OF MEDICINE - Department Faculty/People")
    print("=" * 70)
    
    # Department archive pages that link to individual faculty-search profiles
    dept_pages = [
        ("https://keck.usc.edu/pphs/faculty-archive/", "Keck Population & Public Health Sciences"),
        ("https://keck.usc.edu/pphs/faculty-archive/affiliated-faculty/", "Keck PPHS Affiliated Faculty"),
        ("https://keck.usc.edu/immunology-and-immune-therapeutics/faculty/", "Keck Immunology & Immune Therapeutics"),
        ("https://keck.usc.edu/cancer-biology/faculty/", "Keck Cancer Biology"),
        ("https://keck.usc.edu/physiology-and-neuroscience/faculty/", "Keck Physiology & Neuroscience"),
        ("https://keck.usc.edu/integrative-anatomical-sciences/faculty/", "Keck Integrative Anatomical Sciences"),
        # Try more departments
        ("https://keck.usc.edu/pathology/faculty/", "Keck Pathology"),
        ("https://keck.usc.edu/pathology-and-laboratory-medicine/faculty/", "Keck Pathology & Lab Medicine"),
        ("https://keck.usc.edu/cell-and-neurobiology/faculty/", "Keck Cell & Neurobiology"),
        ("https://keck.usc.edu/stem-cell-biology-and-regenerative-medicine/faculty/", "Keck Stem Cell Biology"),
        ("https://keck.usc.edu/translational-genomics/faculty/", "Keck Translational Genomics"),
    ]
    
    all_profile_urls = {}  # url -> department
    
    for page_url, dept in dept_pages:
        soup, text, resp = get_page(page_url)
        if not soup:
            continue
        
        # Collect faculty-search profile links
        links = collect_profile_links(soup, page_url, ['/faculty-search/'])
        # Filter to individual profiles (not the main search page)
        links = [l for l in links if l != 'https://keck.usc.edu/faculty-search/' 
                 and '/faculty-search/' in l and len(l) > len('https://keck.usc.edu/faculty-search/') + 3]
        
        for link in links:
            if link not in all_profile_urls:
                all_profile_urls[link] = dept
        
        print(f"  {dept}: {len(links)} profile links", flush=True)
        time.sleep(0.2)
    
    # Now visit each profile
    print(f"\n  Visiting {len(all_profile_urls)} Keck faculty profiles...", flush=True)
    results = []
    seen_emails = set()
    
    for i, (url, dept) in enumerate(all_profile_urls.items()):
        profile_results = scrape_profile_emails(url)
        for r in profile_results:
            email = r['email']
            if email not in seen_emails:
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': r['name'],
                    'department': dept,
                    'source_url': url
                })
        
        if (i + 1) % 25 == 0:
            print(f"    ... {i+1}/{len(all_profile_urls)} profiles, {len(results)} emails", flush=True)
        time.sleep(0.15)
    
    print(f"  Keck total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def scrape_keck_all_faculty():
    """Scrape ALL Keck faculty-search profiles for broader coverage."""
    print("\n" + "=" * 70)
    print("KECK - ALL FACULTY SEARCH PROFILES (broader sweep)")
    print("=" * 70)
    
    soup, text, resp = get_page('https://keck.usc.edu/faculty-search/')
    if not soup:
        print("  Could not fetch faculty search page", flush=True)
        return [], set()
    
    # Collect ALL profile links
    all_links = collect_profile_links(soup, 'https://keck.usc.edu', ['/faculty-search/'])
    all_links = [l for l in all_links if l != 'https://keck.usc.edu/faculty-search/'
                 and '/faculty-search/' in l and len(l) > len('https://keck.usc.edu/faculty-search/') + 3]
    
    # Deduplicate
    all_links = list(set(all_links))
    print(f"  Found {len(all_links)} faculty profile links total", flush=True)
    
    results = []
    seen_emails = set()
    
    for i, url in enumerate(all_links):
        profile_results = scrape_profile_emails(url)
        for r in profile_results:
            email = r['email']
            if email not in seen_emails:
                seen_emails.add(email)
                # Try to determine department from the profile page
                dept = "Keck School of Medicine"
                results.append({
                    'email': email,
                    'name': r['name'],
                    'department': dept,
                    'source_url': url
                })
        
        if (i + 1) % 50 == 0:
            print(f"    ... {i+1}/{len(all_links)} profiles, {len(results)} emails", flush=True)
        time.sleep(0.12)
    
    print(f"  Keck All Faculty total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def scrape_keck_profile_with_dept(url):
    """Scrape a single Keck profile and extract department info too."""
    soup, text, resp = get_page(url, timeout=10)
    if not soup:
        return None
    
    page_text = soup.get_text()
    
    # Extract emails
    text_emails = extract_usc_emails(page_text)
    mailto_emails = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'mailto:' in href:
            m = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', href, re.IGNORECASE)
            if m:
                mailto_emails.append(m.group(1).lower())
    
    cf_emails = []
    for el in soup.find_all(attrs={'data-cfemail': True}):
        enc = el.get('data-cfemail', '')
        if enc:
            decoded = decode_cf_email(enc)
            if decoded and 'usc.edu' in decoded.lower():
                cf_emails.append(decoded.lower())
    
    all_emails = list(set(text_emails + mailto_emails + cf_emails))
    all_emails = [e for e in all_emails if not is_admin_email(e)]
    
    if not all_emails:
        return None
    
    # Get name
    name = ""
    h1 = soup.find('h1')
    if h1:
        name = h1.get_text(strip=True)
        for remove in ['Faculty Listing', 'Keck School', '404', 'Page Not Found']:
            if remove.lower() in name.lower():
                name = ""
                break
    
    # Get department - look for dept info on the profile page
    department = "Keck School of Medicine"
    dept_patterns = [
        r'Department\s+of\s+([\w\s&,]+?)(?:\n|<|$)',
        r'Division\s+of\s+([\w\s&,]+?)(?:\n|<|$)',
    ]
    for pattern in dept_patterns:
        m = re.search(pattern, page_text)
        if m:
            dept_text = m.group(1).strip()
            if len(dept_text) < 80:
                department = f"Keck {dept_text}"
                break
    
    # Also look for department in specific elements
    for dt in soup.find_all(['dt', 'th', 'label', 'span']):
        if 'department' in dt.get_text(strip=True).lower():
            dd = dt.find_next_sibling(['dd', 'td', 'span', 'div', 'p'])
            if dd:
                dept_text = dd.get_text(strip=True)
                if dept_text and len(dept_text) < 80:
                    department = f"Keck {dept_text}"
                    break
    
    return {
        'emails': all_emails,
        'name': name,
        'department': department,
    }


def scrape_mann_pharmacy():
    """Scrape Mann School of Pharmacy (formerly pharmacyschool.usc.edu)."""
    print("\n" + "=" * 70)
    print("MANN SCHOOL OF PHARMACY (formerly USC School of Pharmacy)")
    print("=" * 70)
    
    # Get the faculty directory page
    soup, text, resp = get_page('https://mann.usc.edu/research-faculty/faculty-directory/')
    if not soup:
        print("  Could not fetch Mann directory", flush=True)
        return [], set()
    
    # Collect all individual faculty profile links
    profile_links = collect_profile_links(soup, 'https://mann.usc.edu', ['/faculty/'])
    profile_links = [l for l in profile_links if '/faculty/' in l and len(l) > len('https://mann.usc.edu/faculty/') + 3]
    profile_links = list(set(profile_links))
    
    print(f"  Found {len(profile_links)} faculty profile links", flush=True)
    
    results = []
    seen_emails = set()
    
    for i, url in enumerate(profile_links):
        profile_results = scrape_profile_emails(url)
        for r in profile_results:
            email = r['email']
            if email not in seen_emails:
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': r['name'],
                    'department': 'USC Mann School of Pharmacy',
                    'source_url': url
                })
        
        if (i + 1) % 25 == 0:
            print(f"    ... {i+1}/{len(profile_links)} profiles, {len(results)} emails", flush=True)
        time.sleep(0.15)
    
    # Also try PhD program page
    soup2, text2, _ = get_page('https://mann.usc.edu/program/phd-in-health-economics/')
    if soup2:
        text_emails = extract_usc_emails(soup2.get_text())
        for e in text_emails:
            if e not in seen_emails and not is_admin_email(e):
                seen_emails.add(e)
                results.append({
                    'email': e,
                    'name': '',
                    'department': 'USC Mann School of Pharmacy - PhD',
                    'source_url': 'https://mann.usc.edu/program/phd-in-health-economics/'
                })
    
    print(f"  Mann Pharmacy total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def scrape_chan_division():
    """Scrape USC Chan Division of Occupational Science & Occupational Therapy."""
    print("\n" + "=" * 70)
    print("USC CHAN DIVISION (Occupational Therapy)")
    print("=" * 70)
    
    results = []
    seen_emails = set()
    
    # Get the people listing page
    soup, text, resp = get_page('https://chan.usc.edu/msop/people')
    if not soup:
        print("  Could not fetch Chan people page", flush=True)
        return results, seen_emails
    
    # Collect PhD student profile links and other profile links
    profile_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/people/' in href:
            if href.startswith('http'):
                profile_links.append(href)
            elif href.startswith('/'):
                profile_links.append(f"https://chan.usc.edu{href}")
    
    profile_links = list(set(profile_links))
    print(f"  Found {len(profile_links)} profile links", flush=True)
    
    for i, url in enumerate(profile_links):
        profile_results = scrape_profile_emails(url)
        for r in profile_results:
            email = r['email']
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': r['name'],
                    'department': 'USC Chan Division of OT',
                    'source_url': url
                })
        time.sleep(0.2)
    
    # Also try more Chan pages
    extra_chan = [
        'https://chan.usc.edu/research',
        'https://chan.usc.edu/research/labs',
        'https://chan.usc.edu/about',
        'https://chan.usc.edu/msop/faculty',
    ]
    for url in extra_chan:
        soup, text, resp = get_page(url)
        if soup:
            emails = extract_usc_emails(soup.get_text())
            for e in emails:
                if e not in seen_emails and not is_admin_email(e):
                    seen_emails.add(e)
                    results.append({
                        'email': e,
                        'name': '',
                        'department': 'USC Chan Division of OT',
                        'source_url': url
                    })
        time.sleep(0.2)
    
    print(f"  Chan Division total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def scrape_physical_therapy():
    """Scrape USC Division of Biokinesiology and Physical Therapy."""
    print("\n" + "=" * 70)
    print("USC PHYSICAL THERAPY")
    print("=" * 70)
    
    results = []
    seen_emails = set()
    
    pt_urls = [
        'https://pt.usc.edu/',
        'https://pt.usc.edu/people/',
        'https://pt.usc.edu/faculty/',
        'https://pt.usc.edu/students/',
        'https://pt.usc.edu/research/',
        'https://pt.usc.edu/directory/',
        'https://pt.usc.edu/about/',
        'https://pt.usc.edu/bkn-pt/faculty/',
        'https://pt.usc.edu/bkn-pt/people/',
        'https://keck.usc.edu/biokinesiology-and-physical-therapy/',
        'https://keck.usc.edu/biokinesiology-and-physical-therapy/faculty/',
        'https://keck.usc.edu/biokinesiology-and-physical-therapy/people/',
    ]
    
    for url in pt_urls:
        soup, text, resp = get_page(url)
        if not soup:
            continue
        
        page_text = soup.get_text()
        emails = extract_usc_emails(page_text)
        
        # Also get from mailto
        for a in soup.find_all('a', href=True):
            if 'mailto:' in a['href']:
                m = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', a['href'], re.IGNORECASE)
                if m:
                    emails.append(m.group(1).lower())
        
        # Look for profile links to follow
        profile_links = collect_profile_links(soup, url, ['/people/', '/faculty/', '/faculty-search/'])
        profile_links = [l for l in profile_links if len(l) > 30]
        
        for e in set(emails):
            if e not in seen_emails and not is_admin_email(e):
                seen_emails.add(e)
                results.append({
                    'email': e,
                    'name': '',
                    'department': 'USC Physical Therapy',
                    'source_url': url
                })
        
        # Follow profile links
        for plink in list(set(profile_links))[:50]:
            psoup, _, _ = get_page(plink)
            if psoup:
                pemails = extract_usc_emails(psoup.get_text())
                for a in psoup.find_all('a', href=True):
                    if 'mailto:' in a['href']:
                        m = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', a['href'], re.IGNORECASE)
                        if m:
                            pemails.append(m.group(1).lower())
                
                pname = ""
                h1 = psoup.find('h1')
                if h1:
                    pname = h1.get_text(strip=True)
                    if len(pname) > 60 or '404' in pname:
                        pname = ""
                
                for e in set(pemails):
                    if e not in seen_emails and not is_admin_email(e):
                        seen_emails.add(e)
                        results.append({
                            'email': e,
                            'name': pname,
                            'department': 'USC Physical Therapy',
                            'source_url': plink
                        })
            time.sleep(0.15)
        
        time.sleep(0.2)
    
    print(f"  Physical Therapy total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def scrape_schaeffer_center():
    """Scrape Schaeffer Center (Health Policy)."""
    print("\n" + "=" * 70)
    print("USC SCHAEFFER CENTER FOR HEALTH POLICY & ECONOMICS")
    print("=" * 70)
    
    results = []
    seen_emails = set()
    
    # Get people listing
    soup, text, resp = get_page('https://schaeffer.usc.edu/people/')
    if not soup:
        print("  Could not fetch Schaeffer people page", flush=True)
        return results, seen_emails
    
    # Collect profile links
    profile_links = collect_profile_links(soup, 'https://schaeffer.usc.edu', ['/people/'])
    profile_links = [l for l in profile_links if '/people/' in l and len(l) > len('https://schaeffer.usc.edu/people/') + 3]
    profile_links = list(set(profile_links))
    
    print(f"  Found {len(profile_links)} profile links", flush=True)
    
    for i, url in enumerate(profile_links):
        profile_results = scrape_profile_emails(url)
        for r in profile_results:
            email = r['email']
            if email not in seen_emails and not is_admin_email(email):
                seen_emails.add(email)
                results.append({
                    'email': email,
                    'name': r['name'],
                    'department': 'USC Schaeffer Center Health Policy',
                    'source_url': url
                })
        
        if (i + 1) % 25 == 0:
            print(f"    ... {i+1}/{len(profile_links)} profiles, {len(results)} emails", flush=True)
        time.sleep(0.15)
    
    print(f"  Schaeffer Center total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def scrape_gerontology():
    """Scrape USC Leonard Davis School of Gerontology."""
    print("\n" + "=" * 70)
    print("USC LEONARD DAVIS SCHOOL OF GERONTOLOGY")
    print("=" * 70)
    
    results = []
    seen_emails = set()
    
    gero_urls = [
        'https://gero.usc.edu/',
        'https://gero.usc.edu/students/',
        'https://gero.usc.edu/faculty/',
        'https://gero.usc.edu/research/',
        'https://gero.usc.edu/phd/',
        'https://gero.usc.edu/phd/current-students/',
        'https://gero.usc.edu/about/',
        'https://gero.usc.edu/faculty-and-research/',
        'https://gero.usc.edu/faculty-and-research/directory/',
        'https://gero.usc.edu/directory/',
    ]
    
    all_profile_links = set()
    
    for url in gero_urls:
        soup, text, resp = get_page(url)
        if not soup:
            continue
        
        # Direct emails
        emails = extract_usc_emails(soup.get_text())
        for a in soup.find_all('a', href=True):
            if 'mailto:' in a['href']:
                m = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', a['href'], re.IGNORECASE)
                if m:
                    emails.append(m.group(1).lower())
        
        for e in set(emails):
            if e not in seen_emails and not is_admin_email(e):
                seen_emails.add(e)
                results.append({
                    'email': e,
                    'name': '',
                    'department': 'USC Gerontology',
                    'source_url': url
                })
        
        # Collect profile links
        for a in soup.find_all('a', href=True):
            href = a['href']
            if any(kw in href for kw in ['/faculty/', '/people/', '/directory/', '/student']):
                if href.startswith('http') and 'usc.edu' in href:
                    all_profile_links.add(href)
                elif href.startswith('/'):
                    all_profile_links.add(f"https://gero.usc.edu{href}")
        
        time.sleep(0.2)
    
    # Follow profile links
    for url in list(all_profile_links)[:100]:
        psoup, _, _ = get_page(url)
        if psoup:
            pemails = extract_usc_emails(psoup.get_text())
            for a in psoup.find_all('a', href=True):
                if 'mailto:' in a['href']:
                    m = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', a['href'], re.IGNORECASE)
                    if m:
                        pemails.append(m.group(1).lower())
            
            pname = ""
            h1 = psoup.find('h1')
            if h1:
                pname = h1.get_text(strip=True)
                if len(pname) > 60:
                    pname = ""
            
            for e in set(pemails):
                if e not in seen_emails and not is_admin_email(e):
                    seen_emails.add(e)
                    results.append({
                        'email': e,
                        'name': pname,
                        'department': 'USC Gerontology',
                        'source_url': url
                    })
        time.sleep(0.15)
    
    print(f"  Gerontology total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def scrape_global_health():
    """Scrape USC Institute for Global Health."""
    print("\n" + "=" * 70)
    print("USC GLOBAL HEALTH")
    print("=" * 70)
    
    results = []
    seen_emails = set()
    
    urls = [
        'https://globalhealth.usc.edu/',
        'https://globalhealth.usc.edu/about/',
        'https://globalhealth.usc.edu/about/team/',
        'https://globalhealth.usc.edu/research/',
        'https://globalhealth.usc.edu/education/',
    ]
    
    for url in urls:
        soup, text, resp = get_page(url)
        if not soup:
            continue
        
        emails = extract_usc_emails(soup.get_text())
        for a in soup.find_all('a', href=True):
            if 'mailto:' in a['href']:
                m = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', a['href'], re.IGNORECASE)
                if m:
                    emails.append(m.group(1).lower())
        
        for e in set(emails):
            if e not in seen_emails and not is_admin_email(e):
                seen_emails.add(e)
                results.append({
                    'email': e,
                    'name': '',
                    'department': 'USC Global Health',
                    'source_url': url
                })
        time.sleep(0.2)
    
    print(f"  Global Health total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def scrape_keck_labs_and_centers():
    """Scrape Keck-affiliated research labs and centers."""
    print("\n" + "=" * 70)
    print("KECK RESEARCH LABS & CENTERS")
    print("=" * 70)
    
    results = []
    seen_emails = set()
    
    lab_urls = [
        # Norris Cancer Center
        ("https://norris.usc.edu/research/", "Norris Cancer Center"),
        ("https://norris.usc.edu/", "Norris Cancer Center"),
        # Zilkha Neurogenetic Institute
        ("https://keck.usc.edu/zilkha-neurogenetic-institute/", "Zilkha Neurogenetic Institute"),
        ("https://keck.usc.edu/zilkha-neurogenetic-institute/faculty/", "Zilkha Neurogenetic Institute"),
        # Broad Center for Stem Cell
        ("https://keck.usc.edu/eli-and-edythe-broad-center-for-regenerative-medicine-and-stem-cell-research/", "Broad Center for Stem Cell Research"),
        ("https://stemcell.usc.edu/", "USC Stem Cell"),
        ("https://stemcell.usc.edu/people/", "USC Stem Cell"),
        ("https://stemcell.usc.edu/faculty/", "USC Stem Cell"),
        # Brain and Creativity Institute
        ("https://dornsife.usc.edu/bci/people/", "Brain & Creativity Institute"),
        ("https://dornsife.usc.edu/bci/", "Brain & Creativity Institute"),
        # Alzheimer's Research
        ("https://keck.usc.edu/alzheimers-disease-research-center/", "Alzheimer's Disease Research"),
        # SC CTSI
        ("https://sc-ctsi.org/about/people", "SC CTSI"),
        ("https://sc-ctsi.org/about", "SC CTSI"),
        # sites.usc.edu health labs
        ("https://sites.usc.edu/neuroimage/people/", "Neuroimaging Lab"),
        ("https://sites.usc.edu/neuroimage/team/", "Neuroimaging Lab"),
        ("https://sites.usc.edu/mhealth/people/", "mHealth Lab"),
        ("https://sites.usc.edu/mhealth/team/", "mHealth Lab"),
        ("https://sites.usc.edu/spiritlab/people/", "SPIRIT Lab"),
        ("https://sites.usc.edu/spiritlab/team/", "SPIRIT Lab"),
        ("https://sites.usc.edu/biostatistics/people/", "Biostatistics"),
        ("https://sites.usc.edu/biostatistics/team/", "Biostatistics"),
        ("https://sites.usc.edu/brainlab/people/", "Brain Lab"),
        ("https://sites.usc.edu/brainlab/team/", "Brain Lab"),
        ("https://sites.usc.edu/neuro/people/", "Neuroengineering Lab"),
        ("https://sites.usc.edu/neuro/team/", "Neuroengineering Lab"),
        # USC health research centers
        ("https://preventivemedicine.usc.edu/", "USC Preventive Medicine"),
        ("https://keck.usc.edu/surgical-research/", "Keck Surgical Research"),
        ("https://keck.usc.edu/research/", "Keck Research"),
        ("https://keck.usc.edu/research/research-centers/", "Keck Research Centers"),
    ]
    
    for url, dept in lab_urls:
        soup, text, resp = get_page(url)
        if not soup:
            continue
        
        page_text = soup.get_text()
        emails = extract_usc_emails(page_text)
        
        for a in soup.find_all('a', href=True):
            if 'mailto:' in a['href']:
                m = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', a['href'], re.IGNORECASE)
                if m:
                    emails.append(m.group(1).lower())
        
        for el in soup.find_all(attrs={'data-cfemail': True}):
            enc = el.get('data-cfemail', '')
            if enc:
                decoded = decode_cf_email(enc)
                if decoded and 'usc.edu' in decoded.lower():
                    emails.append(decoded.lower())
        
        for e in set(emails):
            if e not in seen_emails and not is_admin_email(e):
                seen_emails.add(e)
                # Try to find name near email
                name = ""
                results.append({
                    'email': e,
                    'name': name,
                    'department': dept,
                    'source_url': url
                })
        
        time.sleep(0.2)
    
    print(f"  Labs & Centers total: {len(results)} unique emails", flush=True)
    return results, seen_emails


def main():
    print("=" * 70)
    print("USC HEALTH SCIENCES EMAIL SCRAPER")
    print("Keck School of Medicine, Pharmacy, PT, OT, Gerontology, etc.")
    print("=" * 70, flush=True)
    
    all_results = []
    master_seen = set()
    
    def merge_results(new_results, new_seen):
        """Merge new results, avoiding duplicates."""
        added = 0
        for r in new_results:
            email = r['email'].lower()
            if email not in master_seen:
                master_seen.add(email)
                all_results.append(r)
                added += 1
        return added
    
    # 1. Keck department-specific faculty
    keck_dept_results, keck_dept_seen = scrape_keck_departments()
    added = merge_results(keck_dept_results, keck_dept_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # 2. Keck ALL faculty profiles (broader sweep)
    keck_all_results, keck_all_seen = scrape_keck_all_faculty()
    added = merge_results(keck_all_results, keck_all_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # 3. Mann School of Pharmacy
    mann_results, mann_seen = scrape_mann_pharmacy()
    added = merge_results(mann_results, mann_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # 4. Chan Division (OT)
    chan_results, chan_seen = scrape_chan_division()
    added = merge_results(chan_results, chan_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # 5. Physical Therapy
    pt_results, pt_seen = scrape_physical_therapy()
    added = merge_results(pt_results, pt_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # 6. Schaeffer Center (Health Policy)
    schaeffer_results, schaeffer_seen = scrape_schaeffer_center()
    added = merge_results(schaeffer_results, schaeffer_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # 7. Gerontology
    gero_results, gero_seen = scrape_gerontology()
    added = merge_results(gero_results, gero_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # 8. Global Health
    global_results, global_seen = scrape_global_health()
    added = merge_results(global_results, global_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # 9. Keck Labs & Research Centers
    lab_results, lab_seen = scrape_keck_labs_and_centers()
    added = merge_results(lab_results, lab_seen)
    print(f"  -> Added {added} new emails (total: {len(all_results)})", flush=True)
    
    # Final cleanup
    print(f"\n{'='*70}")
    print(f"FINAL CLEANUP & SAVE")
    print(f"{'='*70}", flush=True)
    
    # Sort by department, then email
    all_results.sort(key=lambda x: (x['department'], x['email']))
    
    # Save CSV
    csv_path = '/Users/jaiashar/Documents/VoraBusinessFinder/usc_health_emails.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    print(f"Saved CSV: {csv_path}")
    
    # Save JSON
    json_path = '/Users/jaiashar/Documents/VoraBusinessFinder/usc_health_emails.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"Saved JSON: {json_path}")
    
    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY BY DEPARTMENT:")
    print(f"{'='*70}")
    dept_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f"  {dept}: {count} emails")
    
    print(f"\n  TOTAL UNIQUE EMAILS: {len(all_results)}")
    
    # Show sample
    print(f"\nSample emails:")
    for r in all_results[:30]:
        name_str = f" ({r['name']})" if r['name'] else ""
        print(f"  {r['email']}{name_str} - {r['department']}")
    
    if len(all_results) > 30:
        print(f"  ... and {len(all_results) - 30} more")
    
    return all_results


if __name__ == '__main__':
    main()
