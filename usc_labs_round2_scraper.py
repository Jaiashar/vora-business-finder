#!/usr/bin/env python3
"""
USC Research Labs Round 2 - Email Scraper
Scrapes @usc.edu emails from research lab pages across ALL USC schools.
Uses curl for sites.usc.edu (which rejects certain Python requests sessions).
Uses separate sessions per domain group to avoid cookie contamination.
"""

import subprocess
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
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def extract_usc_emails(text):
    """Extract all @usc.edu email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*usc\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix phone-number prefix artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*usc\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


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
    """Filter out administrative/generic emails."""
    admin_patterns = [
        'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
        'support@', 'contact@', 'registrar@', 'admissions@',
        'advising@', 'dean@', 'chair@', 'reception@', 'enroll@',
        'gradadm@', 'viterbi@', 'communications@', 'department@',
        'ece.student', 'ece.faculty', 'eceadmin@', 'eepadmin@',
        '.department@', 'services@', 'affairs@',
        'chemmail@', 'chemgrad@', 'english@', 'philosophy@',
        'physics@', 'poir@', 'dornsife@', 'graduate@',
        'news@', 'events@', 'pr@', 'media@', 'marketing@',
        'library@', 'alumni@', 'development@', 'giving@',
        'keck@', 'research@', 'grants@', 'web@', 'gero@',
        'uscnews@', 'dornsife.communications',
        'pharmacy@', 'pharmacyschool@', 'annenberg@',
        'dworakpeck@', 'price@', 'marshall@',
        'stemcell@', 'norris@', 'cancer@',
        'usc.edu.', 'admission', 'careers@',
        'bci@', 'dni@', 'earthsci@', 'studenthealth@',
        'uschr@', 'dpsrecords@', 'pharmcom@',
        'hr@', 'jobs@', 'hiring@', 'career@',
        'safety@', 'security@', 'emergency@',
        'it@', 'tech@', 'helpdesk@',
        'provost@', 'president@', 'chancellor@',
    ]
    email_lower = email.lower()
    for p in admin_patterns:
        if p in email_lower:
            return True
    if email_lower.startswith('email'):
        return True
    # Filter if email looks like a department name (no dots, very short local part)
    local = email_lower.split('@')[0]
    if len(local) <= 3 and '.' not in local:
        return True
    return False


def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email on the page."""
    # Strategy 1: mailto link parent
    for a_tag in soup.find_all('a', href=True):
        if email in a_tag.get('href', '').lower():
            parent = a_tag.parent
            for _ in range(6):
                if parent is None:
                    break
                text = parent.get_text(separator=' ', strip=True)
                parts = text.split(email)
                for part in parts:
                    part = part.strip(' ,|-•·\n\t')
                    words = part.split()
                    if 2 <= len(words) <= 5:
                        name = ' '.join(words[:4])
                        if not any(x in name.lower() for x in
                                   ['student', 'professor', 'phd', 'email', 'phone',
                                    'address', 'http', 'www', 'lab', 'research',
                                    'department', 'office', 'contact', 'faculty',
                                    'postdoc', 'fellow', 'director']):
                            return name
                parent = parent.parent

    # Strategy 2: nearby headings
    email_elems = soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE))
    for elem in email_elems:
        parent = elem.parent
        for _ in range(6):
            if parent is None:
                break
            for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b']):
                name = tag.get_text(strip=True)
                if name and '@' not in name and len(name) > 3:
                    words = name.split()
                    if 2 <= len(words) <= 5:
                        if not any(x in name.lower() for x in
                                   ['email', 'contact', 'student', 'people', 'phone',
                                    'department', 'office', 'lab', 'member', 'research',
                                    'about', 'home', 'join', 'publication']):
                            return name
            parent = parent.parent

    return ""


def fetch_with_curl(url):
    """Fetch a URL using curl (for sites that reject Python requests)."""
    try:
        result = subprocess.run(
            ['curl', '-s', '-L', '-m', '15',
             '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
             '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
             url],
            capture_output=True, text=True, timeout=20
        )
        if result.returncode == 0 and result.stdout:
            return result.stdout
    except:
        pass
    return None


def scrape_page_curl(url, department):
    """Scrape a page using curl."""
    results = []
    log(f"  [curl] {url}")
    
    html = fetch_with_curl(url)
    if not html or len(html) < 100:
        log(f"    No content")
        return results
    
    soup = BeautifulSoup(html, 'html.parser')
    page_text = soup.get_text()
    
    text_emails = extract_usc_emails(page_text)
    mailto_emails = extract_mailto_emails(soup)
    
    # Check obfuscated
    obfuscated = re.findall(r'([\w.+-]+)\s*\[at\]\s*((?:[\w-]+\.)*usc\.edu)', page_text, re.IGNORECASE)
    obfuscated_emails = [f"{m[0]}@{m[1]}".lower() for m in obfuscated]
    
    # Check script tags
    script_emails = []
    for script in soup.find_all('script'):
        if script.string:
            script_emails.extend(extract_usc_emails(script.string))
    
    all_emails = list(set(text_emails + mailto_emails + obfuscated_emails + script_emails))
    filtered = [e for e in all_emails if not is_admin_email(e)]
    
    if filtered:
        log(f"    Found {len(filtered)} emails")
    
    for email in filtered:
        name = try_get_name_for_email(soup, email)
        results.append({
            'email': email,
            'name': name,
            'department': department,
            'source_url': url,
        })
    
    return results


def scrape_page_requests(url, department, session):
    """Scrape a page using requests."""
    results = []
    try:
        log(f"  [req] {url}")
        resp = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        
        if resp.status_code == 429:
            log(f"    Rate limited - waiting 10s...")
            time.sleep(10)
            resp = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        
        if resp.status_code != 200:
            log(f"    HTTP {resp.status_code}")
            return results
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        page_text = soup.get_text()
        
        text_emails = extract_usc_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        
        obfuscated = re.findall(r'([\w.+-]+)\s*\[at\]\s*((?:[\w-]+\.)*usc\.edu)', page_text, re.IGNORECASE)
        obfuscated_emails = [f"{m[0]}@{m[1]}".lower() for m in obfuscated]
        
        script_emails = []
        for script in soup.find_all('script'):
            if script.string:
                script_emails.extend(extract_usc_emails(script.string))
        
        all_emails = list(set(text_emails + mailto_emails + obfuscated_emails + script_emails))
        filtered = [e for e in all_emails if not is_admin_email(e)]
        
        if filtered:
            log(f"    Found {len(filtered)} emails")
        
        for email in filtered:
            name = try_get_name_for_email(soup, email)
            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': url,
            })
    
    except requests.exceptions.ConnectionError:
        log(f"    Connection error")
    except requests.exceptions.Timeout:
        log(f"    Timeout")
    except Exception as e:
        log(f"    Error: {type(e).__name__}: {e}")
    
    return results


def discover_lab_links_from_page(url, session):
    """From an index page, find subdomain lab links."""
    links = []
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return links
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '').strip()
            text = a_tag.get_text(strip=True)
            full_url = urljoin(url, href)
            
            # Only subdomain lab patterns
            m = re.match(r'https?://([a-z0-9-]+)\.usc\.edu/?$', full_url)
            if m:
                sub = m.group(1)
                skip = {'www', 'dornsife', 'keck', 'viterbi', 'annenberg', 
                        'dworakpeck', 'price', 'marshall', 'gero', 'myusc',
                        'apply', 'admission', 'library', 'catalogue',
                        'maps', 'email', 'calendar', 'uscdirectory',
                        'gradadm', 'accessibility', 'emergency', 'eeotix',
                        'mydornsife', 'studentaffairs', 'studenthealth',
                        'usccareers', 'classes', 'lifespanhealth',
                        'fightonline', 'publicexchange', 'priceschool',
                        'pharmacyschool', 'uscnorriscancer', 'stemcell',
                        'pphs', 'ict', 'dworakpeck'}
                if sub not in skip and 'lab' in sub.lower():
                    links.append((full_url, f"Discovered - {text}" if text else f"Discovered ({sub})"))
            
            # sites.usc.edu/labname pattern
            sm = re.match(r'https?://sites\.usc\.edu/([a-z0-9-]+)/?$', full_url)
            if sm:
                slug = sm.group(1)
                if 'lab' in slug.lower() or 'research' in slug.lower():
                    links.append((full_url, f"Discovered - {text}" if text else f"sites/{slug}"))
    except:
        pass
    
    return list(set(links))


def main():
    all_results = []
    seen_emails = set()
    visited_urls = set()
    
    def add_results(results):
        count = 0
        for r in results:
            email = r['email'].lower().strip()
            if email and email not in seen_emails:
                seen_emails.add(email)
                all_results.append(r)
                count += 1
        return count
    
    log("=" * 70)
    log("USC RESEARCH LABS ROUND 2 - EMAIL SCRAPER")
    log("=" * 70)
    
    # ================================================================
    # PHASE 1: sites.usc.edu labs using CURL (avoids 400 errors)
    # ================================================================
    log("\n\nPHASE 1: SITES.USC.EDU LABS (using curl)")
    log("=" * 70)
    
    sites_labs = [
        # Known lab slugs on sites.usc.edu
        ("eessc", "EESSC Lab"),
        ("dmml", "DMML Lab"),
        ("multiscale", "Multiscale Lab"),
        ("rocketlab", "Rocket Lab"),
        ("spacephysics", "Space Physics Lab"),
        ("biorobotics", "Biorobotics Lab"),
        ("npnl", "NPNL (Neuro)"),
        ("neurotheory", "Neurotheory Lab"),
        ("brainlab", "Brain Lab"),
        ("bhattlab", "Bhatt Lab (Bio)"),
        ("bergerlab", "Berger Lab (BME)"),
        ("medicaldevices", "Medical Devices Lab"),
        ("cssl", "CSSL Lab"),
        ("softrobotics", "Soft Robotics Lab"),
        ("ilab", "iLab"),
        ("arni", "ARNI Lab"),
        ("geochemlab", "Geochem Lab"),
        ("cosmolab", "Cosmo Lab"),
        ("memdyn", "Membrane Dynamics Lab"),
        ("thompsonlab", "Thompson Lab (Chem)"),
        ("warshel", "Warshel Lab (Chem)"),
        ("quantumdyn", "Quantum Dynamics Lab"),
        ("atomicmol", "Atomic & Molecular Lab"),
        ("quantuminfo", "Quantum Info Lab"),
        ("lidarlab", "Lidar Lab (Physics)"),
        ("haaslab", "Haas Lab"),
        ("cammlab", "CAMM Lab"),
        ("matherlab", "Mather Lab (Neuro)"),
        ("saxena", "Saxena Lab"),
        ("healthyaging", "Healthy Aging Lab"),
        ("teel", "Teel Lab"),
        ("leelab", "Lee Lab"),
        ("chenlab", "Chen Lab"),
        ("wanglab", "Wang Lab"),
        ("zhanglab", "Zhang Lab"),
        ("zhoulab", "Zhou Lab"),
        ("cpc", "CPC Lab"),
        ("cfl", "CFL Lab"),
        ("sailab", "SAI Lab"),
        ("maelab", "MAE Lab"),
        ("sceclab", "SCEC Lab"),
        ("cppe", "CPPE Lab"),
        # Additional lab slugs
        ("laulab", "Lau Lab"),
        ("qcblab", "QCB Lab"),
        ("songlab", "Song Lab"),
        ("yunlab", "Yun Lab"),
        ("linlab", "Lin Lab"),
        ("rosenthallab", "Rosenthal Lab"),
        ("imageryforthesciences", "Imagery for Sciences"),
        ("spirallab", "SPIRAL Lab"),
        ("comblab", "COMB Lab"),
        ("computational-neuroscience", "Computational Neuro"),
        ("cogneurolab", "Cognitive Neuro Lab"),
        ("socialcoglab", "Social Cognition Lab"),
        ("dslab", "DS Lab"),
        ("datalab", "Data Lab"),
        ("compgenomics", "Computational Genomics"),
        ("cancerlab", "Cancer Lab"),
        ("immunologylab", "Immunology Lab"),
        ("neuroimaging", "Neuroimaging Lab"),
        ("molecularbiology", "Molecular Biology Lab"),
        ("structuralbiology", "Structural Biology Lab"),
    ]
    
    for slug, dept in sites_labs:
        for suffix in ['/', '/people/', '/members/', '/team/']:
            url = f"https://sites.usc.edu/{slug}{suffix}"
            if url in visited_urls:
                continue
            visited_urls.add(url)
            
            results = scrape_page_curl(url, dept)
            n = add_results(results)
            if n > 0:
                log(f"    => {n} new emails")
            time.sleep(0.3)
    
    log(f"\nAfter Phase 1: {len(all_results)} unique emails")
    
    # ================================================================
    # PHASE 2: Subdomain lab sites (using requests with fresh sessions)
    # ================================================================
    log("\n\nPHASE 2: SUBDOMAIN LAB SITES")
    log("=" * 70)
    
    subdomain_labs = [
        # Chemistry
        ("https://kahnlab.usc.edu/", "Chemistry - Kahn Lab"),
        ("https://lorimerlab.usc.edu/", "Chemistry - Lorimer Lab"),
        ("https://williamslab.usc.edu/", "Chemistry - Williams Lab"),
        ("https://haydocklab.usc.edu/", "Chemistry - Haydock Lab"),
        ("https://el-naggar-lab.usc.edu/", "Physics/Biology - El-Naggar Lab"),
        ("https://www.mannonelabs.org/", "Chemistry - Mannone Lab"),
        
        # Other known subdomain labs
        ("https://icaros.usc.edu/", "ICAROS Lab"),
        ("https://icaros.usc.edu/people", "ICAROS Lab"),
        ("https://nsl.usc.edu/", "Networked Systems Lab"),
        ("https://nsl.usc.edu/people/", "Networked Systems Lab"),
        ("https://sail.usc.edu/", "SAIL Lab"),
        ("https://sail.usc.edu/people.html", "SAIL Lab"),
        ("https://melady.usc.edu/", "Melady Lab"),
        ("https://melady.usc.edu/people/", "Melady Lab"),
        ("https://glamor.usc.edu/", "GLAMOR Lab"),
        ("https://glamor.usc.edu/people/", "GLAMOR Lab"),
        ("https://hal.usc.edu/", "HAL Lab"),
        ("https://hal.usc.edu/people.html", "HAL Lab"),
        ("https://teamcore.usc.edu/", "TeamCore Lab"),
        ("https://teamcore.usc.edu/people/", "TeamCore Lab"),
        ("https://robotics.usc.edu/interaction/", "Interaction Lab"),
        ("https://robotics.usc.edu/interaction/people.html", "Interaction Lab"),
        ("https://sipi.usc.edu/", "SIPI Lab"),
        ("https://sipi.usc.edu/people/", "SIPI Lab"),
        ("https://anrg.usc.edu/www/people/", "ANRG Lab"),
        ("https://combio.usc.edu/", "Computational Biology Lab"),
        ("https://combio.usc.edu/people/", "Computational Biology Lab"),
        ("https://clvrai.com/", "CLVR Lab"),
        ("https://clvrai.com/people/", "CLVR Lab"),
        ("https://nlg.isi.edu/", "NLG Lab (ISI)"),
        ("https://nlg.isi.edu/people/", "NLG Lab (ISI)"),
        ("https://dslab.usc.edu/", "Data Science Lab"),
        ("https://dslab.usc.edu/people/", "Data Science Lab"),
        ("https://scec.usc.edu/", "SCEC"),
        ("https://scec.usc.edu/people/", "SCEC"),
        ("https://zanbranolab.usc.edu/", "Zambrano Lab"),
        ("https://zanbranolab.usc.edu/people/", "Zambrano Lab"),
        ("https://spacescience.usc.edu/", "Space Science Lab"),
        ("https://stemcell.usc.edu/", "Stem Cell Center"),
        ("https://stemcell.usc.edu/people/", "Stem Cell Center"),
        
        # Try some likely lab subdomains  
        ("https://loni.usc.edu/", "LONI Lab"),
        ("https://loni.usc.edu/about/people", "LONI Lab"),
        ("https://cesr.usc.edu/", "CESR"),
        ("https://cesr.usc.edu/people/", "CESR"),
    ]
    
    # Use curl for ALL subdomain labs to avoid session issues
    for url, dept in subdomain_labs:
        if url in visited_urls:
            continue
        visited_urls.add(url)
        
        results = scrape_page_curl(url, dept)
        n = add_results(results)
        if n > 0:
            log(f"    => {n} new emails")
        time.sleep(0.3)
    
    # Also try /people/ /members/ /team/ variants for base URLs
    for url, dept in subdomain_labs:
        base = url.rstrip('/')
        if base.endswith(('/people', '/members', '/team', '/people.html')):
            continue
        for suffix in ['/people/', '/members/', '/team/', '/people.html']:
            try_url = base + suffix
            if try_url in visited_urls:
                continue
            visited_urls.add(try_url)
            results = scrape_page_curl(try_url, dept)
            n = add_results(results)
            if n > 0:
                log(f"    => {n} new emails from {suffix}")
            time.sleep(0.2)
    
    log(f"\nAfter Phase 2: {len(all_results)} unique emails")
    
    # ================================================================
    # PHASE 3: Dornsife departments (requests, fresh session)
    # ================================================================
    log("\n\nPHASE 3: DORNSIFE DEPARTMENT PAGES")
    log("=" * 70)
    
    dornsife_session = requests.Session()
    
    dornsife_pages = [
        ("https://dornsife.usc.edu/chemistry/faculty/", "Chemistry Faculty"),
        ("https://dornsife.usc.edu/physics/research/", "Physics Research"),
        ("https://dornsife.usc.edu/physics/faculty/", "Physics Faculty"),
        ("https://dornsife.usc.edu/bisc/research/", "Biology Research"),
        ("https://dornsife.usc.edu/bisc/faculty/", "Biology Faculty"),
        ("https://dornsife.usc.edu/mcb/faculty/", "MCB Faculty"),
        ("https://dornsife.usc.edu/mcb/research/", "MCB Research"),
        ("https://dornsife.usc.edu/qcb/people/", "QCB"),
        ("https://dornsife.usc.edu/qcb/", "QCB"),
        ("https://dornsife.usc.edu/bci/people/", "Neuroscience - BCI Lab"),
        ("https://dornsife.usc.edu/bci/", "Neuroscience - BCI Lab"),
        ("https://dornsife.usc.edu/earth/research/", "Earth Sciences"),
        ("https://dornsife.usc.edu/earth/faculty/", "Earth Sciences"),
        ("https://dornsife.usc.edu/psyc/research/", "Psychology Research"),
        ("https://dornsife.usc.edu/ling/people/", "Linguistics"),
    ]
    
    for url, dept in dornsife_pages:
        if url in visited_urls:
            continue
        visited_urls.add(url)
        results = scrape_page_requests(url, dept, dornsife_session)
        n = add_results(results)
        if n > 0:
            log(f"    => {n} new emails")
        time.sleep(0.5)
    
    log(f"\nAfter Phase 3: {len(all_results)} unique emails")
    
    # ================================================================
    # PHASE 4: Keck School of Medicine (separate session, longer delays)
    # ================================================================
    log("\n\nPHASE 4: KECK SCHOOL OF MEDICINE")
    log("=" * 70)
    
    keck_session = requests.Session()
    
    keck_pages = [
        ("https://keck.usc.edu/research/", "Keck Research"),
        ("https://keck.usc.edu/preventive-medicine/", "Keck - Preventive Medicine"),
        ("https://keck.usc.edu/preventive-medicine/research/", "Keck - Prev Med Research"),
        ("https://keck.usc.edu/preventive-medicine/faculty/", "Keck - Prev Med Faculty"),
        ("https://keck.usc.edu/pharmacology-and-pharmaceutical-sciences/", "Keck - Pharmacology"),
        ("https://keck.usc.edu/pharmacology-and-pharmaceutical-sciences/research/", "Keck - Pharmacology Research"),
        ("https://keck.usc.edu/pharmacology-and-pharmaceutical-sciences/faculty/", "Keck - Pharmacology Faculty"),
        ("https://keck.usc.edu/pathology/", "Keck - Pathology"),
        ("https://keck.usc.edu/pathology/research/", "Keck - Pathology Research"),
        ("https://keck.usc.edu/pathology/faculty/", "Keck - Pathology Faculty"),
        ("https://keck.usc.edu/biochemistry-and-molecular-medicine/", "Keck - Biochemistry"),
        ("https://keck.usc.edu/biochemistry-and-molecular-medicine/research/", "Keck - Biochemistry Research"),
        ("https://keck.usc.edu/biochemistry-and-molecular-medicine/faculty/", "Keck - Biochemistry Faculty"),
        ("https://keck.usc.edu/microbiology/", "Keck - Microbiology"),
        ("https://keck.usc.edu/microbiology/research/", "Keck - Microbiology Research"),
        ("https://keck.usc.edu/microbiology/faculty/", "Keck - Microbiology Faculty"),
        ("https://keck.usc.edu/physiology-and-neuroscience/", "Keck - Physiology"),
        ("https://keck.usc.edu/physiology-and-neuroscience/research/", "Keck - Physiology Research"),
        ("https://keck.usc.edu/physiology-and-neuroscience/faculty/", "Keck - Physiology Faculty"),
        ("https://keck.usc.edu/molecular-and-computational-biology/", "Keck - Molecular Bio"),
        ("https://keck.usc.edu/molecular-and-computational-biology/research/", "Keck - Molecular Bio Research"),
        ("https://keck.usc.edu/ophthalmology/research/", "Keck - Ophthalmology Research"),
        ("https://keck.usc.edu/neurology/research/", "Keck - Neurology Research"),
        ("https://keck.usc.edu/psychiatry/research/", "Keck - Psychiatry Research"),
        ("https://keck.usc.edu/surgery/research/", "Keck - Surgery Research"),
    ]
    
    for url, dept in keck_pages:
        if url in visited_urls:
            continue
        visited_urls.add(url)
        results = scrape_page_requests(url, dept, keck_session)
        n = add_results(results)
        if n > 0:
            log(f"    => {n} new emails")
        time.sleep(2)  # Be gentle with Keck
    
    log(f"\nAfter Phase 4: {len(all_results)} unique emails")
    
    # ================================================================
    # PHASE 5: Other USC schools & centers
    # ================================================================
    log("\n\nPHASE 5: OTHER USC SCHOOLS & CENTERS")
    log("=" * 70)
    
    other_session = requests.Session()
    
    other_pages = [
        # Gerontology
        ("https://gero.usc.edu/research/", "Gerontology"),
        ("https://gero.usc.edu/students/doctoral/current-students/", "Gerontology - PhD Students"),
        # Pharmacy
        ("https://pharmacyschool.usc.edu/research/", "Pharmacy School"),
        # ISI
        ("https://www.isi.edu/people/", "ISI"),
        ("https://www.isi.edu/research/", "ISI Research"),
        # ICT
        ("https://ict.usc.edu/about/people/", "ICT"),
        # Price School
        ("https://priceschool.usc.edu/research/", "Price School"),
        # Annenberg
        ("https://annenberg.usc.edu/research", "Annenberg"),
        # Social Work
        ("https://dworakpeck.usc.edu/research", "Social Work"),
        # Preventive Med (PPHS)
        ("https://pphs.usc.edu/research/", "Preventive Medicine"),
        ("https://pphs.usc.edu/divisions/biostatistics/", "Biostatistics"),
        ("https://pphs.usc.edu/divisions/epidemiology/", "Epidemiology"),
        # Norris Cancer
        ("https://uscnorriscancer.usc.edu/research/", "Norris Cancer Center"),
    ]
    
    for url, dept in other_pages:
        if url in visited_urls:
            continue
        visited_urls.add(url)
        results = scrape_page_requests(url, dept, other_session)
        n = add_results(results)
        if n > 0:
            log(f"    => {n} new emails")
        time.sleep(0.5)
    
    log(f"\nAfter Phase 5: {len(all_results)} unique emails")
    
    # ================================================================
    # PHASE 6: Discover and scrape faculty lab links
    # ================================================================
    log("\n\nPHASE 6: DISCOVERING LAB LINKS FROM FACULTY PAGES")
    log("=" * 70)
    
    disc_session = requests.Session()
    discovery_pages = [
        "https://dornsife.usc.edu/chemistry/faculty/",
        "https://dornsife.usc.edu/physics/faculty/",
        "https://dornsife.usc.edu/bisc/faculty/",
        "https://dornsife.usc.edu/mcb/faculty/",
        "https://dornsife.usc.edu/earth/faculty/",
    ]
    
    discovered = []
    for page in discovery_pages:
        links = discover_lab_links_from_page(page, disc_session)
        discovered.extend(links)
        time.sleep(0.5)
    
    # Deduplicate
    unique_disc = []
    for url, dept in discovered:
        if url not in visited_urls:
            unique_disc.append((url, dept))
    
    log(f"  Discovered {len(unique_disc)} new lab URLs")
    
    for url, dept in unique_disc[:50]:
        visited_urls.add(url)
        results = scrape_page_curl(url, dept)
        n = add_results(results)
        if n > 0:
            log(f"    => {n} new emails")
        
        # Try /people/ variant
        base = url.rstrip('/')
        for suffix in ['/people/', '/members/', '/team/']:
            try_url = base + suffix
            if try_url not in visited_urls:
                visited_urls.add(try_url)
                results = scrape_page_curl(try_url, dept)
                n = add_results(results)
                if n > 0:
                    log(f"    => {n} new emails from {suffix}")
        time.sleep(0.3)
    
    log(f"\nAfter Phase 6: {len(all_results)} unique emails")
    
    # ================================================================
    # FILTERING: Remove emails already in previous CSV files
    # ================================================================
    log("\n\nFILTERING: Removing emails from previous scrapers")
    log("=" * 70)
    
    existing_emails = set()
    existing_files = [
        'usc_viterbi_emails.csv',
        'usc_dornsife_grad_emails.csv',
        'usc_pro_school_emails.csv',
        'usc_emails.csv',
    ]
    
    for fname in existing_files:
        try:
            with open(fname, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('email'):
                        existing_emails.add(row['email'].lower().strip())
            log(f"  Loaded existing emails from {fname}")
        except FileNotFoundError:
            pass
        except Exception as e:
            log(f"  Error reading {fname}: {e}")
    
    log(f"  Total existing emails to exclude: {len(existing_emails)}")
    
    new_results = [r for r in all_results if r['email'].lower().strip() not in existing_emails]
    log(f"  After filtering: {len(new_results)} truly new emails (removed {len(all_results) - len(new_results)} duplicates)")
    
    # ================================================================
    # SAVE RESULTS
    # ================================================================
    log(f"\n\n{'='*70}")
    log(f"TOTAL NEW UNIQUE USC EMAILS FOUND: {len(new_results)}")
    log(f"{'='*70}")
    
    output_csv = 'usc_labs_round2_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(new_results, key=lambda x: x['department']):
            writer.writerow(r)
    log(f"\nSaved to {output_csv}")
    
    output_json = 'usc_labs_round2_emails.json'
    with open(output_json, 'w') as f:
        json.dump(new_results, f, indent=2)
    log(f"Also saved to {output_json}")
    
    # Summary
    log(f"\n{'='*70}")
    log("SUMMARY BY DEPARTMENT/LAB:")
    log(f"{'='*70}")
    dept_counts = {}
    for r in new_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")
    
    log(f"\n{'='*70}")
    log("ALL NEW EMAILS:")
    log(f"{'='*70}")
    for r in sorted(new_results, key=lambda x: x['email']):
        name_str = f" ({r['name']})" if r['name'] else ""
        log(f"  {r['email']}{name_str} - {r['department']}")
    
    return new_results


if __name__ == '__main__':
    main()
