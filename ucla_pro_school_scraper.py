#!/usr/bin/env python3
"""
UCLA Professional School & Student Org Email Scraper v2
========================================================
Targeted scraper for UCLA department pages that are known to have student emails.
Uses urllib with robust timeout handling.
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

# Regex for UCLA emails
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@(?:g\.)?ucla\.edu', re.IGNORECASE)
MAILTO_RE = re.compile(r'mailto:([a-zA-Z0-9._%+\-]+@(?:g\.)?ucla\.edu)', re.IGNORECASE)

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
}


def fetch_page(url, timeout=12):
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
        print(f"      [WARN] {url[:65]} — {str(e)[:50]}")
        return None


def decode_html_entities(text):
    """Decode HTML entities like &#97; &#64; etc."""
    if not text:
        return text
    # First pass: decode named and numeric entities
    decoded = html.unescape(text)
    # Second pass: handle leftover numeric entities
    decoded = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), decoded)
    decoded = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), decoded)
    return decoded


def extract_emails_from_html(raw_html):
    """Extract UCLA emails from HTML, handling obfuscation."""
    if not raw_html:
        return []
    
    # Decode HTML entities first
    decoded = decode_html_entities(raw_html)
    
    # Find all emails in decoded text
    emails = set()
    
    # Standard email regex
    for m in EMAIL_RE.finditer(decoded):
        emails.add(m.group(0).lower().strip().rstrip('.'))
    
    # Also search in mailto: links (even in original HTML)
    for m in MAILTO_RE.finditer(decoded):
        emails.add(m.group(1).lower().strip().rstrip('.'))
    
    # Also try decoding mailto links with entities in original
    mailto_pattern = re.compile(r'mailto:((?:&#?\w+;|[a-zA-Z0-9._%+\-@])+)')
    for m in mailto_pattern.finditer(raw_html):
        raw_email = decode_html_entities(m.group(1))
        if re.match(r'^[a-zA-Z0-9._%+\-]+@(?:g\.)?ucla\.edu$', raw_email, re.IGNORECASE):
            emails.add(raw_email.lower().strip())
    
    # Filter out skip emails
    return [e for e in emails if e not in SKIP_EMAILS and not any(
        kw in e for kw in ['noreply', 'no-reply', 'donotreply', 'webmaster']
    )]


def extract_name_near_email(text, email, window=400):
    """Find a person's name near their email in text."""
    idx = text.lower().find(email.lower())
    if idx < 0:
        return derive_name_from_email(email)
    
    start = max(0, idx - window)
    end = min(len(text), idx + window)
    context = text[start:end]
    
    # Look for name in heading tags
    patterns = [
        r'<h[2-4][^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']+?)(?:\s*</a>)?\s*</h[2-4]>',
        r'<strong[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']+?)(?:\s*</a>)?\s*</strong>',
        r'class="[^"]*name[^"]*"[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']+)',
        r'<a[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,40})\s*</a>\s*(?:<[^>]+>)*\s*(?:Graduate|Doctoral|PhD)',
    ]
    
    for pat in patterns:
        m = re.search(pat, context)
        if m:
            name = m.group(1).strip()
            if 2 < len(name) < 60 and not re.search(r'[<>@{}\[\]]', name):
                return name
    
    return derive_name_from_email(email)


def derive_name_from_email(email):
    """Derive a likely name from email address."""
    local = email.split('@')[0]
    # Common patterns: firstname.lastname, firstlast, first_last
    if '.' in local:
        parts = local.split('.')
        if all(len(p) > 1 for p in parts[:2]):
            # Filter out numbers
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


# ═══════════════════════════════════════════════════════════════════
# ALL TARGETED SOURCES
# ═══════════════════════════════════════════════════════════════════

SOURCES = [
    # --- Professional Schools (from user request) ---
    ("https://law.ucla.edu/student-life/student-organizations", "UCLA Law - Student Orgs"),
    ("https://law.ucla.edu/academics/journals", "UCLA Law - Journals"),
    ("https://law.ucla.edu/academics/journals/ucla-law-review", "UCLA Law - Law Review"),
    ("https://law.ucla.edu/academics/journals/pacific-basin-law-journal", "UCLA Law - Pacific Basin Journal"),
    ("https://law.ucla.edu/academics/journals/national-black-law-journal", "UCLA Law - Black Law Journal"),
    
    # Public Health
    ("https://ph.ucla.edu/students", "Public Health - Students"),
    ("https://ph.ucla.edu/about/contact-us", "Public Health - Contact"),
    
    # Luskin
    ("https://luskin.ucla.edu/social-welfare/phd-social-welfare/social-welfare-student-profiles", "Luskin - Social Welfare PhD"),
    ("https://luskin.ucla.edu/urban-planning/students", "Luskin - Urban Planning Students"),
    ("https://luskin.ucla.edu/social-welfare/students", "Luskin - Social Welfare Students"),
    ("https://luskin.ucla.edu/public-policy/students", "Luskin - Public Policy Students"),
    ("https://luskin.ucla.edu/people", "Luskin - People"),
    ("https://luskin.ucla.edu/contact", "Luskin - Contact"),
    
    # Education
    ("https://education.ucla.edu/people/students/", "Education - Students"),
    ("https://education.ucla.edu/people/", "Education - People"),
    
    # Social Welfare
    ("https://socialwelfare.ucla.edu/people/doctoral-students/", "Social Welfare - Doctoral Students"),
    ("https://socialwelfare.ucla.edu/people/", "Social Welfare - People"),
    
    # Information Studies
    ("https://is.gseis.ucla.edu/people/doctoral-students/", "Info Studies - Doctoral Students"),
    ("https://is.gseis.ucla.edu/people/", "Info Studies - People"),
    
    # Nursing
    ("https://nursing.ucla.edu/about/our-students/doctoral-student-directories/phd-student-directory", "Nursing - PhD Students"),
    ("https://nursing.ucla.edu/about/our-students/doctoral-student-directories/dnp-student-directory", "Nursing - DNP Students"),
    
    # --- Departments known to have email directories ---
    # Communication (CONFIRMED RICH SOURCE)
    ("https://comm.ucla.edu/people/graduate-students/", "Communication - Grad Students"),
    
    # Anthropology (CONFIRMED RICH SOURCE)
    ("https://www.anthro.ucla.edu/people/graduate-students", "Anthropology - Grad Students"),
    
    # Linguistics (CONFIRMED RICH SOURCE - HTML entity encoded)
    ("https://linguistics.ucla.edu/grads/", "Linguistics - Grad Students"),
    
    # Classics
    ("https://classics.ucla.edu/graduate-students/", "Classics - Grad Students"),
    
    # World Arts & Cultures/Dance
    ("https://wacd.ucla.edu/people/graduate-students", "World Arts & Cultures - Grad Students"),
    
    # Political Science
    ("https://polisci.ucla.edu/people/graduate-students/", "Political Science - Grad Students"),
    
    # Sociology
    ("https://soc.ucla.edu/people/graduate-students/", "Sociology - Grad Students"),
    ("https://soc.ucla.edu/graduate/current-students", "Sociology - Current Students"),
    
    # Economics
    ("https://economics.ucla.edu/graduate/current-students/", "Economics - Grad Students"),
    ("https://www.econ.ucla.edu/graduate/current-students/", "Economics - Grad Students v2"),
    
    # Psychology
    ("https://www.psych.ucla.edu/graduate/current-students", "Psychology - Grad Students"),
    ("https://www.psych.ucla.edu/people/", "Psychology - People"),
    
    # History
    ("https://www.history.ucla.edu/people/graduate-students", "History - Grad Students"),
    ("https://history.ucla.edu/people/graduate-students", "History - Grad Students v2"),
    
    # Philosophy
    ("https://philosophy.ucla.edu/people/graduate-students/", "Philosophy - Grad Students"),
    ("https://www.philosophy.ucla.edu/people/graduate-students", "Philosophy - Grad Students v2"),
    
    # English
    ("https://english.ucla.edu/people/graduate-students/", "English - Grad Students"),
    
    # Geography
    ("https://geog.ucla.edu/people/graduate-students/", "Geography - Grad Students"),
    ("https://www.geog.ucla.edu/people/graduate-students/", "Geography - Grad Students v2"),
    
    # Gender Studies
    ("https://www.genderstudies.ucla.edu/people/graduate-students", "Gender Studies - Grad Students"),
    
    # Asian American Studies
    ("https://asianam.ucla.edu/people/graduate-students/", "Asian American Studies - Grad Students"),
    
    # Chicana/o Studies
    ("https://www.chavez.ucla.edu/people/graduate-students", "Chicana/o Studies - Grad Students"),
    
    # --- STEM Departments ---
    # Chemistry
    ("https://www.chemistry.ucla.edu/people/graduate-students", "Chemistry - Grad Students"),
    
    # Physics
    ("https://www.physics.ucla.edu/people/graduate-students/", "Physics - Grad Students"),
    
    # Math
    ("https://www.math.ucla.edu/people/grad", "Math - Grad Students"),
    
    # Statistics
    ("https://www.stat.ucla.edu/people/graduate-students/", "Statistics - Grad Students"),
    
    # CS
    ("https://www.cs.ucla.edu/people/graduate-students/", "CS - Grad Students"),
    
    # EE
    ("https://www.ee.ucla.edu/people/graduate-students/", "ECE - Grad Students"),
    
    # Bioengineering
    ("https://www.bioeng.ucla.edu/people/graduate-students/", "Bioengineering - Grad Students"),
    
    # --- Student Media (from user request) ---
    ("https://dailybruin.com/staff", "Daily Bruin - Staff"),
    ("https://dailybruin.com/contact", "Daily Bruin - Contact"),
    ("https://www.uclaradio.com/about", "UCLA Radio - About"),
    ("https://usac.ucla.edu/", "USAC Student Gov"),
    ("https://usac.ucla.edu/about/officers/", "USAC Officers"),
    
    # --- Greek Life (from user request) ---
    ("https://www.greeklife.ucla.edu/", "Greek Life - Main"),
    ("https://www.greeklife.ucla.edu/councils/", "Greek Life - Councils"),
    ("https://www.greeklife.ucla.edu/ifc/", "Greek Life - IFC"),
    ("https://www.greeklife.ucla.edu/cpc/", "Greek Life - CPC"),
    
    # --- Additional high-value pages ---
    # Anderson MBA
    ("https://www.anderson.ucla.edu/degrees/phd-program/current-students", "Anderson - PhD Students"),
    
    # TFT
    ("https://www.tft.ucla.edu/people/students/", "Theater Film TV - Students"),
    
    # EEB
    ("https://www.eeb.ucla.edu/people/graduate-students/", "EEB - Grad Students"),
    
    # MCDB
    ("https://www.mcdb.ucla.edu/people/graduate-students/", "MCDB - Grad Students"),
    
    # Architecture
    ("https://www.aud.ucla.edu/people/students", "Architecture - Students"),
    
    # Music
    ("https://schoolofmusic.ucla.edu/people/graduate-students/", "Music - Grad Students"),
]

# Luskin individual profile pages (known to have emails)
LUSKIN_STUDENTS = [
    "madonna-cadiz", "yesi-camacho-torres", "danielle-dunn", "samantha-eisert",
    "natalie-fensterstock", "kimberly-fuentes", "livier-gutierrez",
    "domonique-henderson", "sawyer-hogenkamp", "yeon-jae-hwang",
    "juan-c-jauregui", "sarah-kang", "sophie-koestner", "julia-lesnick",
    "jianan-li", "baiyang-li", "keri-lintz", "bethany-murray",
    "juan-j-nunez", "emanuel-nunez", "stephanie-patton",
    "hillary-peregrina", "emonie-robinson", "norma-rubio",
    "merhawi-tesfai", "irene-valdovinos", "qianyun-wang",
    "vanessa-warri", "emily-m-waters", "63912", "chaoyue-wu", "minyang-zhang",
]


def main():
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║  UCLA PROFESSIONAL SCHOOL & STUDENT ORG EMAIL SCRAPER v2     ║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<62}║")
    print("╚════════════════════════════════════════════════════════════════╝")
    
    all_contacts = []
    all_seen = set()
    
    # ── Phase 1: Scrape all known department URLs ──
    print(f"\n  Phase 1: Scraping {len(SOURCES)} department pages...")
    print("  " + "─"*60)
    
    for url, department in SOURCES:
        results = scrape_url(url, department)
        new_count = 0
        for r in results:
            if r["email"] not in all_seen:
                all_contacts.append(r)
                all_seen.add(r["email"])
                new_count += 1
        if new_count > 0:
            print(f"    [{new_count:>3} new] {department}")
            for r in results[:2]:
                if r["email"] in all_seen:
                    nm = r['name'][:25] if r['name'] else '—'
                    print(f"             {nm:<28} {r['email']}")
        else:
            print(f"    [  0    ] {department}")
        time.sleep(0.3)
    
    # ── Phase 2: Scrape Luskin individual profiles ──
    print(f"\n  Phase 2: Scraping {len(LUSKIN_STUDENTS)} Luskin student profiles...")
    print("  " + "─"*60)
    
    for slug in LUSKIN_STUDENTS:
        url = f"https://luskin.ucla.edu/person/{slug}"
        raw_html = fetch_page(url)
        if not raw_html:
            continue
        
        decoded = decode_html_entities(raw_html)
        
        # Extract name from page title
        name_match = re.search(r'<h1[^>]*>([^<]+)</h1>', decoded)
        name = name_match.group(1).strip() if name_match else slug.replace('-', ' ').title()
        
        # Extract ALL emails (including gmail for some students)
        all_emails = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', decoded)
        ucla_emails = [e.lower() for e in all_emails if 'ucla.edu' in e.lower()]
        
        # If no UCLA email, also grab gmail/other
        if not ucla_emails:
            personal_emails = [e.lower() for e in all_emails 
                             if not any(skip in e.lower() for skip in ['noreply', 'wordpress', 'siteground', 'cpanel'])]
            for pe in personal_emails[:1]:
                if pe not in all_seen:
                    all_contacts.append({
                        "name": name,
                        "email": pe,
                        "department": "Luskin - Social Welfare PhD",
                        "source_url": url,
                    })
                    all_seen.add(pe)
                    print(f"    + {name:<30} {pe}")
        
        for email in ucla_emails:
            if email not in all_seen and email not in SKIP_EMAILS:
                all_contacts.append({
                    "name": name,
                    "email": email,
                    "department": "Luskin - Social Welfare PhD",
                    "source_url": url,
                })
                all_seen.add(email)
                print(f"    + {name:<30} {email}")
        
        time.sleep(0.3)
    
    # ── Phase 3: Try to visit Nursing individual profiles for emails ──
    print(f"\n  Phase 3: Scraping Nursing PhD student profiles...")
    print("  " + "─"*60)
    
    nursing_slugs = [
        "emily-bloom-rn", "paul-boy-msn-mph-agacnp-bc-agnp-c-pccn-rn-phn",
        "charlotte-bryant-bsn-rn", "cristina-cabrera-mino-rn",
        "lisa-diaz-rn-msn-cdces", "lauren-furtick-rn",
        "marianne-gutierrez-bsn-rn", "elizabeth-kohout-msn-rn",
        "yuriko-matsuo-msn-pmhnp-bc-aprn", "sarah-moreau-msn-ma-rn-bcba-phn",
        "rey-paolo-ernesto-j-roca-iii-msn-rn-phn-cne-pmh-bc",
        "dave-tan-msn-rn", "chunyu-wang-rn-msn", "yi-ping-wen-msn-rn-fnp-c",
    ]
    
    for slug in nursing_slugs:
        url = f"https://nursing.ucla.edu/people/{slug}"
        results = scrape_url(url, "Nursing - PhD Student")
        for r in results:
            if r["email"] not in all_seen:
                all_contacts.append(r)
                all_seen.add(r["email"])
                print(f"    + {r['name'][:30]:<30} {r['email']}")
        time.sleep(0.3)
    
    # ── Classify student vs staff ──
    for c in all_contacts:
        if '@g.ucla.edu' in c["email"]:
            c["type"] = "student"
        elif any(kw in c.get("department", "").lower() for kw in 
                 ['student', 'doctoral', 'phd', 'grad']):
            c["type"] = "likely_student"
        elif any(kw in c.get("name", "").lower() for kw in ['student']):
            c["type"] = "likely_student"
        else:
            c["type"] = "unknown"
    
    # ── Results Summary ──
    print("\n" + "═"*70)
    print("  RESULTS SUMMARY")
    print("═"*70)
    
    g_ucla = [c for c in all_contacts if '@g.ucla.edu' in c['email']]
    other_ucla = [c for c in all_contacts if '@ucla.edu' in c['email'] and '@g.ucla.edu' not in c['email']]
    non_ucla = [c for c in all_contacts if '@ucla.edu' not in c['email']]
    
    print(f"\n  Total unique emails: {len(all_contacts)}")
    print(f"  @g.ucla.edu (student): {len(g_ucla)}")
    print(f"  @ucla.edu (other): {len(other_ucla)}")
    print(f"  Non-UCLA: {len(non_ucla)}")
    
    # By department
    depts = {}
    for c in all_contacts:
        d = c["department"]
        depts[d] = depts.get(d, 0) + 1
    
    print(f"\n  By department/source:")
    for d, count in sorted(depts.items(), key=lambda x: -x[1]):
        print(f"    {d:<50} {count}")
    
    # ── Save CSV ──
    base = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base, "ucla_pro_school_emails.csv")
    fieldnames = ["name", "email", "department", "type", "source_url"]
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_contacts)
    
    json_path = os.path.join(base, "ucla_pro_school_emails.json")
    with open(json_path, 'w') as f:
        json.dump(all_contacts, f, indent=2)
    
    print(f"\n  CSV saved:  {csv_path}")
    print(f"  JSON saved: {json_path}")
    
    # ── Print ALL emails grouped by department ──
    print(f"\n  {'═'*70}")
    print(f"  ALL {len(all_contacts)} EMAILS")
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
    print(f"  COMPLETE. {len(all_contacts)} unique emails scraped from {len(SOURCES)} pages + profiles.")
    print(f"{'═'*70}")


if __name__ == "__main__":
    main()
