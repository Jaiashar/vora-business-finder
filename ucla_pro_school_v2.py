#!/usr/bin/env python3
"""
UCLA Professional School Email Scraper v2 - Extended
=====================================================
Targets specific pages from user request:
  - GSEIS/SEIS student directory
  - Education students
  - Anderson Business School PhD/doctoral
  - Luskin PhD students (urban planning, public policy)
  - IOA people
  - Chavez graduate students
  - Asian American Studies graduate students
  - African American Studies graduate students
"""

import os
import re
import csv
import sys
import time
import json
import html
import urllib.request
import urllib.error
import ssl
import signal
from datetime import datetime
from urllib.parse import urlparse, urljoin

sys.stdout.reconfigure(line_buffering=True)

# ─── Setup ───────────────────────────────────────────────────────
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Broader email regex to catch @anderson.ucla.edu, @luskin.ucla.edu, etc.
EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@(?:[a-zA-Z0-9\-]+\.)*ucla\.edu',
    re.IGNORECASE
)
MAILTO_RE = re.compile(
    r'mailto:([a-zA-Z0-9._%+\-]+@(?:[a-zA-Z0-9\-]+\.)*ucla\.edu)',
    re.IGNORECASE
)

SKIP_EMAILS = {
    'webmaster@ucla.edu', 'communications@ucla.edu', 'info@ucla.edu',
    'registrar@ucla.edu', 'accessibility@ucla.edu', 'askus@ucla.edu',
    'itservices@ucla.edu', 'feedback@ucla.edu', 'help@ucla.edu',
    'admissions@ucla.edu', 'support@ucla.edu', 'donotreply@ucla.edu',
    'noreply@ucla.edu', 'library@ucla.edu', 'security@ucla.edu',
    'abuse@ucla.edu', 'postmaster@ucla.edu', 'root@ucla.edu',
    'privacy@ucla.edu', 'records@ucla.edu', 'hr@ucla.edu',
    'copyright@ucla.edu', 'dmca@ucla.edu', 'ucladir@it.ucla.edu',
    'sao@ucla.edu', 'chancellor@ucla.edu', 'titleix@equity.ucla.edu',
    'info@anderson.ucla.edu', 'admissions@anderson.ucla.edu',
    'info@luskin.ucla.edu', 'admissions@luskin.ucla.edu',
    'communications@anderson.ucla.edu', 'giving@support.ucla.edu',
    'askgseis@gseis.ucla.edu', 'info@gseis.ucla.edu',
    'askgseis@seis.ucla.edu', 'info@seis.ucla.edu',
    'contact@seis.ucla.edu', 'fsl@ucla.edu',
    'askus@seis.ucla.edu', 'admissions@seis.ucla.edu',
    'info@education.ucla.edu', 'admissions@education.ucla.edu',
    'communications@education.ucla.edu', 'askus@education.ucla.edu',
}


def fetch_page(url, timeout=15):
    """Fetch a URL with strict timeout."""
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Fetch timed out for {url}")
    
    try:
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout + 5)
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        data = resp.read().decode('utf-8', errors='ignore')
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)
        return data
    except Exception as e:
        signal.alarm(0)
        try:
            signal.signal(signal.SIGALRM, old_handler)
        except:
            pass
        print(f"      [WARN] {url[:80]} — {str(e)[:60]}")
        return None


def decode_html_entities(text):
    """Decode HTML entities like &#97; &#64; etc."""
    if not text:
        return text
    decoded = html.unescape(text)
    decoded = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), decoded)
    decoded = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), decoded)
    return decoded


def extract_emails_from_html(raw_html):
    """Extract UCLA emails from HTML, handling obfuscation."""
    if not raw_html:
        return []
    
    decoded = decode_html_entities(raw_html)
    emails = set()
    
    for m in EMAIL_RE.finditer(decoded):
        emails.add(m.group(0).lower().strip().rstrip('.'))
    
    for m in MAILTO_RE.finditer(decoded):
        emails.add(m.group(1).lower().strip().rstrip('.'))
    
    # Also try decoding mailto links with entities in original
    mailto_pattern = re.compile(r'mailto:((?:&#?\w+;|[a-zA-Z0-9._%+\-@])+)')
    for m in mailto_pattern.finditer(raw_html):
        raw_email = decode_html_entities(m.group(1))
        if re.match(r'^[a-zA-Z0-9._%+\-]+@(?:[a-zA-Z0-9\-]+\.)*ucla\.edu$', raw_email, re.IGNORECASE):
            emails.add(raw_email.lower().strip())
    
    return [e for e in emails if e not in SKIP_EMAILS and not any(
        kw in e for kw in ['noreply', 'no-reply', 'donotreply', 'webmaster', 'wordpress']
    )]


def extract_name_near_email(text, email, window=500):
    """Find a person's name near their email in text."""
    idx = text.lower().find(email.lower())
    if idx < 0:
        return derive_name_from_email(email)
    
    start = max(0, idx - window)
    end = min(len(text), idx + window)
    context = text[start:end]
    
    patterns = [
        r'<h[1-4][^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']+?)(?:\s*</a>)?\s*</h[1-4]>',
        r'<strong[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']+?)(?:\s*</a>)?\s*</strong>',
        r'class="[^"]*name[^"]*"[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']+)',
        r'<a[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,40})\s*</a>\s*(?:<[^>]+>)*\s*(?:Graduate|Doctoral|PhD)',
        r'<div[^>]*class="[^"]*card[^"]*"[^>]*>.*?<[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,40})\s*<',
        r'(?:Dr\.\s+|Prof\.\s+)?([A-Z][a-zA-ZÀ-ÿ]+(?:\s+[A-Z][a-zA-ZÀ-ÿ]+){1,3})\s*(?:<[^>]*>)*\s*' + re.escape(email),
    ]
    
    for pat in patterns:
        m = re.search(pat, context, re.DOTALL)
        if m:
            name = m.group(1).strip()
            name = re.sub(r'<[^>]+>', '', name).strip()
            if 2 < len(name) < 60 and not re.search(r'[<>@{}\[\]]', name):
                return name
    
    return derive_name_from_email(email)


def derive_name_from_email(email):
    """Derive a likely name from email address."""
    local = email.split('@')[0]
    if '.' in local:
        parts = local.split('.')
        if all(len(p) > 1 for p in parts[:2]):
            cleaned = [re.sub(r'\d+', '', p) for p in parts[:2]]
            if all(len(p) > 1 for p in cleaned):
                return ' '.join(p.capitalize() for p in cleaned)
    elif '_' in local:
        parts = local.split('_')
        if all(len(p) > 1 for p in parts[:2]):
            cleaned = [re.sub(r'\d+', '', p) for p in parts[:2]]
            if all(len(p) > 1 for p in cleaned):
                return ' '.join(p.capitalize() for p in cleaned)
    return ""


def scrape_url(url, department):
    """Fetch URL and extract all UCLA emails."""
    print(f"    Fetching: {url[:80]}...")
    raw_html = fetch_page(url)
    if not raw_html:
        return []
    
    emails = extract_emails_from_html(raw_html)
    decoded = decode_html_entities(raw_html)
    
    results = []
    seen = set()
    for email in emails:
        if email not in seen:
            name = extract_name_near_email(decoded, email)
            results.append({
                "name": name,
                "email": email,
                "department": department,
                "source_url": url,
            })
            seen.add(email)
    
    return results


def scrape_with_subpages(url, department, link_pattern=None, max_subpages=200):
    """Scrape a page and also follow links matching a pattern for more emails."""
    all_results = []
    all_seen = set()
    
    # Scrape main page
    print(f"    Fetching: {url[:80]}...")
    raw_html = fetch_page(url)
    if not raw_html:
        return []
    
    decoded = decode_html_entities(raw_html)
    emails = extract_emails_from_html(raw_html)
    
    for email in emails:
        if email not in all_seen:
            name = extract_name_near_email(decoded, email)
            all_results.append({
                "name": name,
                "email": email,
                "department": department,
                "source_url": url,
            })
            all_seen.add(email)
    
    # Find sub-page links if pattern provided
    if link_pattern:
        base_url = urlparse(url)
        links = re.findall(r'href=["\']([^"\']+)["\']', raw_html)
        sub_urls = set()
        for link in links:
            if re.search(link_pattern, link):
                full_url = urljoin(url, link)
                if full_url != url:
                    sub_urls.add(full_url)
        
        if sub_urls:
            print(f"      Found {len(sub_urls)} sub-pages to check...")
            for i, sub_url in enumerate(sorted(sub_urls)[:max_subpages]):
                sub_results = scrape_url(sub_url, department)
                for r in sub_results:
                    if r["email"] not in all_seen:
                        all_results.append(r)
                        all_seen.add(r["email"])
                time.sleep(0.3)
    
    return all_results


# ═══════════════════════════════════════════════════════════════════
# ALL TARGETED SOURCES (from user request)
# ═══════════════════════════════════════════════════════════════════

SOURCES = [
    # --- GSEIS / SEIS ---
    ("https://gseis.ucla.edu/directory/students/", "GSEIS - Student Directory"),
    ("https://seis.ucla.edu/students", "SEIS - Students"),
    ("https://seis.ucla.edu/people", "SEIS - People"),
    ("https://seis.ucla.edu/people/students", "SEIS - Students v2"),
    ("https://seis.ucla.edu/directory/students", "SEIS - Student Directory"),
    ("https://gseis.ucla.edu/directory/", "GSEIS - Directory"),
    ("https://gseis.ucla.edu/people/students/", "GSEIS - Students"),
    
    # --- Education ---
    ("https://education.ucla.edu/people/students/", "Education - Students"),
    ("https://education.ucla.edu/people/", "Education - People"),
    ("https://education.ucla.edu/", "Education - Main"),
    ("https://education.ucla.edu/people/doctoral-students/", "Education - Doctoral Students"),
    ("https://education.ucla.edu/people/graduate-students/", "Education - Graduate Students"),
    
    # --- Anderson Business School ---
    ("https://www.anderson.ucla.edu/programs-and-admissions/doctoral-program/current-students", "Anderson - Doctoral Current Students"),
    ("https://www.anderson.ucla.edu/programs-and-admissions/phd/current-students", "Anderson - PhD Current Students"),
    ("https://www.anderson.ucla.edu/degrees/phd-program/current-students", "Anderson - PhD Students v2"),
    ("https://www.anderson.ucla.edu/faculty-and-research/doctoral/students", "Anderson - Doctoral Students"),
    ("https://www.anderson.ucla.edu/faculty-and-research/doctoral-program/students", "Anderson - Doctoral Prog Students"),
    ("https://www.anderson.ucla.edu/doctoral/current-students", "Anderson - Doctoral Current v2"),
    ("https://www.anderson.ucla.edu/academics/doctoral-program/current-students", "Anderson - Doctoral Acad"),
    ("https://www.anderson.ucla.edu/programs-and-admissions/doctoral-program", "Anderson - Doctoral Program"),
    ("https://www.anderson.ucla.edu/doctoral-program/current-students", "Anderson - Doctoral Current v3"),
    
    # --- Luskin PhD Students ---
    ("https://luskin.ucla.edu/urban-planning/people/phd-students", "Luskin - Urban Planning PhD"),
    ("https://luskin.ucla.edu/public-policy/people/phd-students", "Luskin - Public Policy PhD"),
    ("https://luskin.ucla.edu/social-welfare/people/phd-students", "Luskin - Social Welfare PhD"),
    ("https://luskin.ucla.edu/urban-planning/phd-students", "Luskin - Urban Planning PhD v2"),
    ("https://luskin.ucla.edu/public-policy/phd-students", "Luskin - Public Policy PhD v2"),
    ("https://luskin.ucla.edu/urban-planning/people/", "Luskin - Urban Planning People"),
    ("https://luskin.ucla.edu/public-policy/people/", "Luskin - Public Policy People"),
    ("https://luskin.ucla.edu/social-welfare/people/", "Luskin - Social Welfare People"),
    ("https://luskin.ucla.edu/people", "Luskin - People"),
    ("https://luskin.ucla.edu/urban-planning/people/students", "Luskin - UP Students"),
    ("https://luskin.ucla.edu/public-policy/people/students", "Luskin - PP Students"),
    ("https://luskin.ucla.edu/urban-planning/people/doctoral-students", "Luskin - UP Doctoral"),
    ("https://luskin.ucla.edu/public-policy/people/doctoral-students", "Luskin - PP Doctoral"),
    
    # --- IOA (Institute of American Cultures) ---
    ("https://www.ioa.ucla.edu/people", "IOA - People"),
    ("https://www.ioa.ucla.edu/", "IOA - Main"),
    ("https://ioa.ucla.edu/people", "IOA - People v2"),
    
    # --- Chavez (Chicana/o Studies) ---
    ("https://chavez.ucla.edu/people/graduate-students/", "Chavez - Graduate Students"),
    ("https://www.chavez.ucla.edu/people/graduate-students", "Chavez - Graduate Students v2"),
    ("https://chavez.ucla.edu/people/", "Chavez - People"),
    
    # --- Asian American Studies ---
    ("https://asianam.ucla.edu/people/graduate-students/", "Asian American Studies - Grad Students"),
    ("https://asianam.ucla.edu/people/", "Asian American Studies - People"),
    
    # --- African American Studies ---
    ("https://afam.ucla.edu/people/graduate-students/", "African American Studies - Grad Students"),
    ("https://afam.ucla.edu/people/", "African American Studies - People"),
    ("https://afam.ucla.edu/graduate/current-students/", "African American Studies - Current"),
    ("https://www.afam.ucla.edu/people/graduate-students/", "Afam - Grad Students v2"),
    
    # --- Additional professional schools ---
    # Public Affairs
    ("https://publicaffairs.ucla.edu/people/students/", "Public Affairs - Students"),
    
    # School of Dentistry
    ("https://www.dentistry.ucla.edu/people/students", "Dentistry - Students"),
    
    # Institute for Society and Genetics
    ("https://socgen.ucla.edu/people/graduate-students/", "Society & Genetics - Grad Students"),
    
    # Labor Center
    ("https://www.labor.ucla.edu/people/", "Labor Center - People"),
    
    # Ralph J. Bunche Center
    ("https://bunchecenter.ucla.edu/people/", "Bunche Center - People"),
    
    # American Indian Studies
    ("https://www.aisc.ucla.edu/people/graduate-students/", "American Indian Studies - Grad"),
    ("https://aisc.ucla.edu/people/graduate-students/", "American Indian Studies - Grad v2"),
]

# Luskin Urban Planning individual profile slugs to try
LUSKIN_UP_SLUGS = [
    # These will be discovered from the people pages
]


def main():
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║  UCLA PROFESSIONAL SCHOOL EMAIL SCRAPER v2 - Extended        ║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<62}║")
    print("╚════════════════════════════════════════════════════════════════╝")
    
    all_contacts = []
    all_seen = set()
    
    # Load existing emails to avoid duplicates
    existing_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ucla_pro_school_emails.csv")
    if os.path.exists(existing_csv):
        with open(existing_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_seen.add(row['email'].lower().strip())
        print(f"\n  Loaded {len(all_seen)} existing emails to skip duplicates")
    
    # ── Phase 1: Scrape all targeted URLs ──
    print(f"\n  Phase 1: Scraping {len(SOURCES)} targeted pages...")
    print("  " + "─"*60)
    
    luskin_profile_urls = set()
    anderson_profile_urls = set()
    education_profile_urls = set()
    
    for url, department in SOURCES:
        raw_html = fetch_page(url)
        if not raw_html:
            print(f"    [SKIP  ] {department}")
            time.sleep(0.3)
            continue
        
        decoded = decode_html_entities(raw_html)
        emails = extract_emails_from_html(raw_html)
        
        new_count = 0
        for email in emails:
            if email not in all_seen:
                name = extract_name_near_email(decoded, email)
                all_contacts.append({
                    "name": name,
                    "email": email,
                    "department": department,
                    "source_url": url,
                })
                all_seen.add(email)
                new_count += 1
        
        if new_count > 0:
            print(f"    [{new_count:>3} new] {department}")
            for c in all_contacts[-min(3, new_count):]:
                nm = c['name'][:25] if c['name'] else '—'
                print(f"             {nm:<28} {c['email']}")
        else:
            print(f"    [  0    ] {department}")
        
        # Extract profile links for follow-up
        if 'luskin.ucla.edu' in url:
            links = re.findall(r'href=["\']([^"\']*luskin\.ucla\.edu/person/[^"\']+)["\']', raw_html)
            links += [urljoin(url, l) for l in re.findall(r'href=["\'](/person/[^"\']+)["\']', raw_html)]
            luskin_profile_urls.update(links)
        
        if 'anderson.ucla.edu' in url:
            links = re.findall(r'href=["\']([^"\']*anderson\.ucla\.edu[^"\']*(?:student|phd|doctoral)[^"\']*)["\']', raw_html, re.IGNORECASE)
            anderson_profile_urls.update(links)
        
        if 'education.ucla.edu' in url or 'seis.ucla.edu' in url or 'gseis.ucla.edu' in url:
            links = re.findall(r'href=["\']([^"\']+)["\']', raw_html)
            for l in links:
                full = urljoin(url, l)
                if any(d in full for d in ['education.ucla.edu', 'seis.ucla.edu', 'gseis.ucla.edu']):
                    if any(kw in full.lower() for kw in ['student', 'people', 'directory', 'doctoral', 'phd']):
                        education_profile_urls.add(full)
        
        time.sleep(0.3)
    
    # ── Phase 2: Follow Luskin profile links ──
    new_luskin = luskin_profile_urls - {f"https://luskin.ucla.edu/person/{s}" for s in [
        "madonna-cadiz", "yesi-camacho-torres", "danielle-dunn", "samantha-eisert",
        "natalie-fensterstock", "kimberly-fuentes", "livier-gutierrez",
        "domonique-henderson", "sawyer-hogenkamp", "yeon-jae-hwang",
        "juan-c-jauregui", "sarah-kang", "sophie-koestner", "julia-lesnick",
        "jianan-li", "baiyang-li", "keri-lintz", "bethany-murray",
        "juan-j-nunez", "emanuel-nunez", "stephanie-patton",
        "hillary-peregrina", "emonie-robinson", "norma-rubio",
        "merhawi-tesfai", "irene-valdovinos", "qianyun-wang",
        "vanessa-warri", "emily-m-waters", "63912", "chaoyue-wu", "minyang-zhang",
    ]}
    
    if new_luskin:
        print(f"\n  Phase 2: Scraping {len(new_luskin)} Luskin profile pages...")
        print("  " + "─"*60)
        for profile_url in sorted(new_luskin):
            raw_html = fetch_page(profile_url)
            if not raw_html:
                continue
            decoded = decode_html_entities(raw_html)
            
            name_match = re.search(r'<h1[^>]*>([^<]+)</h1>', decoded)
            name = name_match.group(1).strip() if name_match else ""
            
            emails = extract_emails_from_html(raw_html)
            for email in emails:
                if email not in all_seen:
                    all_contacts.append({
                        "name": name or derive_name_from_email(email),
                        "email": email,
                        "department": "Luskin - Profile",
                        "source_url": profile_url,
                    })
                    all_seen.add(email)
                    nm = name[:30] if name else '—'
                    print(f"    + {nm:<30} {email}")
            time.sleep(0.3)
    
    # ── Phase 3: Follow Education/GSEIS profile links ──
    if education_profile_urls:
        # Filter to only unique, unvisited pages
        visited = {url for url, _ in SOURCES}
        new_edu = education_profile_urls - visited
        if new_edu:
            print(f"\n  Phase 3: Scraping {len(new_edu)} Education/GSEIS sub-pages...")
            print("  " + "─"*60)
            for edu_url in sorted(new_edu)[:50]:
                raw_html = fetch_page(edu_url)
                if not raw_html:
                    continue
                decoded = decode_html_entities(raw_html)
                emails = extract_emails_from_html(raw_html)
                for email in emails:
                    if email not in all_seen:
                        name = extract_name_near_email(decoded, email)
                        all_contacts.append({
                            "name": name,
                            "email": email,
                            "department": "Education/GSEIS",
                            "source_url": edu_url,
                        })
                        all_seen.add(email)
                        nm = name[:30] if name else '—'
                        print(f"    + {nm:<30} {email}")
                time.sleep(0.3)
    
    # ── Phase 4: Try Anderson profile links ──
    if anderson_profile_urls:
        print(f"\n  Phase 4: Scraping {len(anderson_profile_urls)} Anderson sub-pages...")
        print("  " + "─"*60)
        for and_url in sorted(anderson_profile_urls)[:50]:
            raw_html = fetch_page(and_url)
            if not raw_html:
                continue
            decoded = decode_html_entities(raw_html)
            emails = extract_emails_from_html(raw_html)
            for email in emails:
                if email not in all_seen:
                    name = extract_name_near_email(decoded, email)
                    all_contacts.append({
                        "name": name,
                        "email": email,
                        "department": "Anderson Business School",
                        "source_url": and_url,
                    })
                    all_seen.add(email)
                    nm = name[:30] if name else '—'
                    print(f"    + {nm:<30} {email}")
            time.sleep(0.3)
    
    # ── Classify ──
    for c in all_contacts:
        if '@g.ucla.edu' in c["email"]:
            c["type"] = "student"
        elif any(kw in c.get("department", "").lower() for kw in 
                 ['student', 'doctoral', 'phd', 'grad']):
            c["type"] = "likely_student"
        else:
            c["type"] = "unknown"
    
    # ── Results Summary ──
    print("\n" + "═"*70)
    print("  RESULTS SUMMARY")
    print("═"*70)
    
    g_ucla = [c for c in all_contacts if '@g.ucla.edu' in c['email']]
    other_ucla = [c for c in all_contacts if '@ucla.edu' in c['email'] and '@g.ucla.edu' not in c['email']]
    
    print(f"\n  NEW unique emails found: {len(all_contacts)}")
    print(f"  @g.ucla.edu (student): {len(g_ucla)}")
    print(f"  @ucla.edu (other): {len(other_ucla)}")
    
    depts = {}
    for c in all_contacts:
        d = c["department"]
        depts[d] = depts.get(d, 0) + 1
    
    print(f"\n  By department/source:")
    for d, count in sorted(depts.items(), key=lambda x: -x[1]):
        print(f"    {d:<55} {count}")
    
    # ── Save CSV ──
    base = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base, "ucla_pro_school_v2_emails.csv")
    fieldnames = ["name", "email", "department", "type", "source_url"]
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_contacts)
    
    json_path = os.path.join(base, "ucla_pro_school_v2_emails.json")
    with open(json_path, 'w') as f:
        json.dump(all_contacts, f, indent=2)
    
    print(f"\n  CSV saved:  {csv_path}")
    print(f"  JSON saved: {json_path}")
    
    # Print all
    print(f"\n  {'═'*70}")
    print(f"  ALL {len(all_contacts)} NEW EMAILS")
    print(f"  {'═'*70}")
    
    by_dept = {}
    for c in all_contacts:
        d = c["department"]
        if d not in by_dept:
            by_dept[d] = []
        by_dept[d].append(c)
    
    for dept in sorted(by_dept.keys()):
        contacts = by_dept[dept]
        print(f"\n  [{dept}] ({len(contacts)} contacts)")
        for c in contacts:
            nm = c['name'][:35] if c['name'] else '—'
            print(f"    {nm:<37} {c['email']:<45} {c.get('type','')}")
    
    print(f"\n{'═'*70}")
    print(f"  COMPLETE. {len(all_contacts)} new unique emails from extended search.")
    print(f"{'═'*70}")


if __name__ == "__main__":
    main()
